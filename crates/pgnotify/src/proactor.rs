//! Caller-owned readiness and deadline dispatch on a shared io_uring ring.

use std::{
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    rc::Rc,
    time::{Duration, Instant},
};

use uarch::{
    bits::Bitmap64,
    prelude::{cold_sink, hot_inline},
};

use liburing::{
    Completion, CompletionDispatcher, OperationId, OperationPayload, Proactor, SubmissionContext,
    SubsystemId, UringError, ops::Timeout,
};

use crate::{
    error::{ListenerPhase, PgNotifyError, Result},
    fsm::PgListener,
    hook::NotifyHook,
    types::{ListenerEvent, SocketInterest},
};

const OPERATION_CAPACITY: usize = 64;
const _: () = assert!(OPERATION_CAPACITY == u64::BITS as usize);

struct SocketLease {
    fd: i32,
    generation: u64,
    socket: Rc<OwnedFd>,
}

enum Operand {
    Poll {
        fd: i32,
        generation: u64,
        interest: SocketInterest,
    },
    Timer {
        generation: u64,
        timeout: Timeout,
    },
    Cancel,
}

struct Operation {
    id: OperationId,
    operand: Operand,
    canceling: bool,
}

/// Adapts a listener to one caller-owned ring and a unique subsystem identifier.
///
/// Call `flush` after changing the listener, then route completions to this driver.
/// Shutdown on a shared ring uses `begin_shutdown`, `flush`, and the caller's
/// dispatcher until `has_pending_io` becomes false. Target and cancel completions
/// retire independently. Dropping early retains unretired timeout operands.
/// A driver must remain associated with the same ring for its entire lifetime.
pub struct ProactorPgListener<H: NotifyHook> {
    /// The thread-confined listener driven by this adapter.
    pub listener: PgListener<H>,
    subsystem: SubsystemId,
    operations: [Option<Operation>; OPERATION_CAPACITY],
    occupied: Bitmap64,
    owner_id: Option<u64>,
    socket: Option<SocketLease>,
    sequence: u64,
    shutting_down: bool,
    failure: Option<PgNotifyError>,
}

impl<H: NotifyHook> ProactorPgListener<H> {
    /// Wraps a listener without submitting work or taking ownership of the ring.
    ///
    /// # Errors
    /// Rejects subsystem zero, reserved for the ring's internal operations.
    pub fn new(subsystem: SubsystemId, listener: PgListener<H>) -> Result<Self> {
        if subsystem == SubsystemId::INTERNAL {
            return Err(PgNotifyError::InvalidConfig {
                reason: "PostgreSQL subsystem must be nonzero",
            });
        }
        Ok(Self {
            listener,
            subsystem,
            operations: std::array::from_fn(|_| None),
            occupied: Bitmap64::EMPTY,
            owner_id: None,
            socket: None,
            sequence: 1,
            shutting_down: false,
            failure: None,
        })
    }

    /// Stages current interests, deferring work when bounded ring credit is full.
    ///
    /// Call again after peers release credit. Accepted operations remain owned
    /// even if a later preparation fails. This method never waits for I/O.
    /// # Errors
    /// Returns descriptor duplication, identity exhaustion, or submission errors.
    #[inline]
    pub fn flush(&mut self, ring: &mut Proactor) -> Result<()> {
        self.bind_owner(ring.owner_id()).map_err(map_ring_error)?;
        if self.occupied.is_empty()
            && self.socket.is_none()
            && matches!(
                self.listener.phase(),
                ListenerPhase::Disconnected | ListenerPhase::Terminated
            )
        {
            return Ok(());
        }
        self.flush_active(ring)
    }

    #[inline(never)]
    fn flush_active(&mut self, ring: &mut Proactor) -> Result<()> {
        self.flush_to(ring).map_err(map_ring_error)
    }

    /// Returns the first listener error observed during completion dispatch.
    #[must_use]
    pub fn failure(&self) -> Option<PgNotifyError> {
        self.failure
    }

    /// Counts driver-owned targets and explicit cancellation acknowledgments separately.
    #[must_use]
    #[inline]
    pub fn inflight_count(&self) -> usize {
        self.occupied.count_ones() as usize
    }

    /// Includes deferred driver work and live target CQEs. Internal fallback cancellation
    /// acknowledgments belong to the ring and retain no driver operands.
    #[must_use]
    pub fn has_pending_io(&self) -> bool {
        !self.occupied.is_empty()
            || (!self.shutting_down
                && (self.listener.poll_interest().is_some()
                    || self.listener.timer_deadline().is_some()))
    }

    /// Closes the listener and stages cancellation, without canceling peer I/O.
    ///
    /// Continue flushing and routing this subsystem until no I/O remains pending.
    /// # Errors
    /// Returns submission errors; pending operands remain retained for retry.
    pub fn begin_shutdown(&mut self, ring: &mut Proactor) -> Result<()> {
        self.bind_owner(ring.owner_id()).map_err(map_ring_error)?;
        if !self.shutting_down {
            self.shutting_down = true;
            self.listener.begin_shutdown();
            self.socket = None;
        }
        self.flush(ring)
    }

    /// Drains a listener-only ring dispatch loop within five seconds.
    ///
    /// Shared-ring owners must use `begin_shutdown` and their own dispatcher so
    /// peer completions are delivered to their rightful owners. The ring remains
    /// open after success. Never drop pending owners merely because a budget ran out.
    /// # Errors
    /// Returns cancellation, dispatch, wait, or shutdown-budget exhaustion errors.
    pub fn shutdown(&mut self, ring: &mut Proactor) -> Result<()> {
        self.shutdown_within(ring, Duration::from_secs(5))
    }

    /// Drains a listener-only dispatch loop within a caller-supplied budget.
    ///
    /// # Errors
    /// Returns cancellation, dispatch, wait, or budget errors. On failure retain
    /// this driver and its ring and resume draining rather than releasing operands.
    pub fn shutdown_within(&mut self, ring: &mut Proactor, budget: Duration) -> Result<()> {
        let deadline = Instant::now()
            .checked_add(budget)
            .ok_or(PgNotifyError::InvalidConfig {
                reason: "PostgreSQL shutdown budget overflows",
            })?;
        self.begin_shutdown(ring)?;
        while self.has_pending_io() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(PgNotifyError::TimedOut {
                    state: self.listener.state(),
                });
            }
            self.flush(ring)?;
            if ring.mode().allows_blocking_wait() {
                ring.wait_completions(1, Some(remaining.min(Duration::from_millis(20))), self)
                    .map_err(map_ring_error)?;
            } else {
                ring.tick(self).map_err(map_ring_error)?;
                std::hint::spin_loop();
            }
        }
        Ok(())
    }

    fn bind_owner(&mut self, owner_id: u64) -> liburing::Result<()> {
        if let Some(owner) = self.owner_id {
            if owner != owner_id {
                return Err(UringError::InvalidConfig(
                    "PostgreSQL proactor owner mismatch",
                ));
            }
        } else {
            self.owner_id = Some(owner_id);
        }
        Ok(())
    }

    fn next_id(&mut self, slot: usize) -> liburing::Result<OperationId> {
        let payload = self
            .sequence
            .checked_mul(OPERATION_CAPACITY as u64)
            .and_then(|generation| OperationPayload::new(generation | slot as u64))
            .ok_or(UringError::PayloadOverflow(self.sequence))?;
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(UringError::PayloadOverflow(self.sequence))?;
        Ok(OperationId::pack(self.subsystem, payload))
    }

    #[hot_inline]
    fn vacant(&self) -> Option<usize> {
        self.occupied.lowest_zero_index().map(|slot| slot as usize)
    }

    fn insert_operation(&mut self, slot: usize, id: OperationId, operand: Operand) {
        debug_assert!(self.operations[slot].is_none());
        self.operations[slot] = Some(Operation {
            id,
            operand,
            canceling: false,
        });
        self.occupied = self.occupied.set(slot as u32);
    }

    #[inline]
    fn take_operation(&mut self, index: usize) -> liburing::Result<Operation> {
        let operation = self.operations[index]
            .take()
            .ok_or(UringError::InvalidConfig("missing completed operation"))?;
        self.occupied = self.occupied.clear(index as u32);
        Ok(operation)
    }

    fn active_operations(&self) -> impl Iterator<Item = &Operation> {
        self.occupied
            .iter_ones()
            .filter_map(|slot| self.operations[slot as usize].as_ref())
    }

    fn flush_to(&mut self, submit: &mut impl Submit) -> liburing::Result<()> {
        let interest = if self.shutting_down {
            None
        } else {
            self.listener
                .poll_interest()
                .filter(|(_, _, value)| value.readable || value.writable)
        };
        let deadlines = if self.shutting_down {
            [None, None]
        } else {
            [
                self.listener.armed_deadline(),
                self.listener.continuation_deadline(),
            ]
        };
        let mut pending = self.occupied;
        while let Some(slot) = pending.pop_lowest_one() {
            let index = slot as usize;
            let stale = self.operations[index].as_ref().is_some_and(|operation| {
                if operation.canceling {
                    return false;
                }
                match operation.operand {
                    Operand::Poll {
                        fd,
                        generation,
                        interest: submitted,
                    } => interest != Some((fd, generation, submitted)),
                    Operand::Timer { generation, .. } => !deadlines
                        .iter()
                        .flatten()
                        .any(|value| value.generation == generation),
                    Operand::Cancel => false,
                }
            });
            if !stale {
                continue;
            }
            let target = self.operations[index]
                .as_ref()
                .map(|operation| operation.id)
                .ok_or(UringError::InvalidConfig("missing cancellation target"))?;
            let prepared = if let Some(slot) = self.vacant() {
                match self.next_id(slot) {
                    Ok(id) => submit.cancel(id, target).map(|tracked| {
                        if tracked {
                            self.insert_operation(slot, id, Operand::Cancel);
                        }
                    }),
                    // Target retirement must remain possible after identity exhaustion.
                    Err(UringError::PayloadOverflow(_)) => submit.cancel_untracked(target),
                    Err(error) => return Err(error),
                }
            } else {
                submit.cancel_untracked(target)
            };
            match prepared {
                Ok(()) => {
                    if let Some(target) = self.operations[index].as_mut() {
                        target.canceling = true;
                    }
                }
                Err(UringError::SqRingFull) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
        if self.shutting_down {
            return Ok(());
        }
        for deadline in deadlines.into_iter().flatten() {
            let present = self.active_operations().any(|operation| {
                matches!(operation.operand, Operand::Timer { generation, .. } if generation == deadline.generation)
            });
            if !present {
                let Some(slot) = self.vacant() else {
                    return Ok(());
                };
                let timeout = Timeout::new(deadline.duration)?;
                let id = self.next_id(slot)?;
                match submit.timeout(id, &timeout) {
                    Ok(()) => self.insert_operation(
                        slot,
                        id,
                        Operand::Timer {
                            generation: deadline.generation,
                            timeout,
                        },
                    ),
                    Err(UringError::SqRingFull) => return Ok(()),
                    Err(error) => return Err(error),
                }
            }
        }
        let Some((fd, generation, desired)) = interest else {
            self.socket = None;
            return Ok(());
        };
        let present = self.active_operations().any(|operation| {
            matches!(operation.operand, Operand::Poll { fd: active_fd, generation: active_generation, .. }
                if active_fd == fd && active_generation == generation)
        });
        if present {
            return Ok(());
        }
        let Some(slot) = self.vacant() else {
            return Ok(());
        };
        if self
            .socket
            .as_ref()
            .is_none_or(|lease| (lease.fd, lease.generation) != (fd, generation))
        {
            // SAFETY: PgListener owns this live descriptor on this same thread.
            // Duplication preserves its open file description across reconnect.
            let duplicate = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
            if duplicate < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            // SAFETY: fcntl returned a fresh descriptor uniquely owned by this call.
            let socket = Rc::new(unsafe { OwnedFd::from_raw_fd(duplicate) });
            self.socket = Some(SocketLease {
                fd,
                generation,
                socket,
            });
        }
        let socket = Rc::clone(
            &self
                .socket
                .as_ref()
                .ok_or(UringError::InvalidConfig("missing socket lease"))?
                .socket,
        );
        let mask = if desired.readable {
            libc::POLLIN as u32
        } else {
            0
        } | if desired.writable {
            libc::POLLOUT as u32
        } else {
            0
        };
        let id = self.next_id(slot)?;
        match submit.poll(id, socket, mask) {
            Ok(()) => self.insert_operation(
                slot,
                id,
                Operand::Poll {
                    fd,
                    generation,
                    interest: desired,
                },
            ),
            Err(UringError::SqRingFull) => (),
            Err(error) => return Err(error),
        }
        Ok(())
    }

    fn deliver(&mut self, event: ListenerEvent) -> liburing::Result<()> {
        self.listener.on_event(event).map(|_| ()).map_err(|error| {
            self.failure.get_or_insert(error);
            UringError::InvalidConfig("PostgreSQL listener event failed")
        })
    }
}

impl<H: NotifyHook> CompletionDispatcher for ProactorPgListener<H> {
    fn dispatch(
        &mut self,
        completion: Completion,
        submissions: &mut SubmissionContext<'_>,
    ) -> liburing::Result<()> {
        // Ring-local operation IDs do not identify the owner of retained operands.
        if self.owner_id != Some(submissions.owner_id()) {
            return Err(UringError::InvalidConfig(
                "PostgreSQL proactor owner mismatch",
            ));
        }
        if completion.operation_id.subsystem_id() != self.subsystem {
            return Err(UringError::InvalidSubsystemId(
                completion.operation_id.subsystem_id().raw(),
            ));
        }
        // Low bits select the slot; the full identity rejects a retired generation.
        let index = completion.operation_id.payload().raw() as usize & (OPERATION_CAPACITY - 1);
        let slot = &mut self.operations[index];
        if slot
            .as_ref()
            .is_none_or(|operation| operation.id != completion.operation_id)
        {
            return Err(UringError::InvalidPayload(
                completion.operation_id.payload().raw(),
            ));
        }
        submissions.claim_completion(completion)?;
        let operation = self.take_operation(index)?;
        let delivered = match operation.operand {
            Operand::Cancel => {
                if [0, -libc::ENOENT, -libc::EALREADY, -libc::ECANCELED]
                    .contains(&completion.result)
                {
                    Ok(())
                } else {
                    Err(UringError::syscalls("PostgreSQL cancel", completion.result))
                }
            }
            Operand::Timer { generation, .. } => {
                if completion.result == -libc::ETIME
                    && !self.shutting_down
                    && self
                        .listener
                        .armed_deadline()
                        .into_iter()
                        .chain(self.listener.continuation_deadline())
                        .any(|deadline| deadline.generation == generation)
                {
                    self.deliver(ListenerEvent::Timer { generation })
                } else if [-libc::ETIME, -libc::ECANCELED].contains(&completion.result) {
                    Ok(())
                } else {
                    Err(UringError::syscalls(
                        "PostgreSQL timeout",
                        completion.result,
                    ))
                }
            }
            Operand::Poll { fd, generation, .. } => {
                if self.shutting_down
                    || !self.listener.owns_socket(fd, generation)
                    || completion.result == -libc::ECANCELED
                {
                    Ok(())
                } else {
                    let flags = completion.result;
                    let error = flags < 0
                        || flags & i32::from(libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0;
                    self.deliver(ListenerEvent::Socket {
                        fd,
                        readable: flags >= 0 && flags & i32::from(libc::POLLIN) != 0,
                        writable: flags >= 0 && flags & i32::from(libc::POLLOUT) != 0,
                        error,
                    })
                }
            }
        };
        let flushed = self.flush_to(submissions);
        delivered.and(flushed)
    }
}

impl<H: NotifyHook> Drop for ProactorPgListener<H> {
    fn drop(&mut self) {
        for operation in &mut self.operations {
            if let Some(Operation {
                operand: Operand::Timer { timeout, .. },
                ..
            }) = operation.take()
            {
                // A cancel acknowledgment does not retire its target. The kernel
                // may still reference this operand even when the driver is gone.
                std::mem::forget(timeout);
            }
        }
    }
}

#[cold_sink]
fn map_ring_error(error: UringError) -> PgNotifyError {
    let errno = match error {
        UringError::RingInit(errno)
        | UringError::Submit(errno)
        | UringError::Wait(errno)
        | UringError::Syscall { errno, .. } => errno,
        UringError::SqRingFull => libc::EAGAIN,
        UringError::CompletionsDropped => libc::EOVERFLOW,
        UringError::SqThreadGone => libc::EPIPE,
        UringError::OpcodeNotAdmitted { .. }
        | UringError::BlockingWaitUnavailable { .. }
        | UringError::CancellationUnavailable { .. } => libc::EOPNOTSUPP,
        _ => libc::EINVAL,
    };
    PgNotifyError::IoError {
        op: "PostgreSQL proactor",
        errno,
    }
}

trait Submit {
    fn poll(&mut self, id: OperationId, socket: Rc<OwnedFd>, mask: u32) -> liburing::Result<()>;
    fn timeout(&mut self, id: OperationId, timeout: &Timeout) -> liburing::Result<()>;
    fn cancel(&mut self, id: OperationId, target: OperationId) -> liburing::Result<bool>;
    fn cancel_untracked(&mut self, target: OperationId) -> liburing::Result<()>;
}

macro_rules! impl_submit {
    ($target:ty) => {
        impl Submit for $target {
            fn poll(
                &mut self,
                id: OperationId,
                socket: Rc<OwnedFd>,
                mask: u32,
            ) -> liburing::Result<()> {
                let fd = socket.as_raw_fd();
                self.prepare_owned_sqe_with(id, socket, None, |sqe| {
                    // SAFETY: The ring retains this descriptor through the target
                    // final CQE. POLL_ADD references no userspace memory operands.
                    unsafe { liburing::ops::net::prep_poll_add(sqe, fd, mask) };
                })
            }
            fn timeout(&mut self, id: OperationId, timeout: &Timeout) -> liburing::Result<()> {
                self.prepare_sqe_with(id, |sqe| {
                    // SAFETY: The driver retains this stable operand until the
                    // target CQE. Early Drop deliberately retains pending operands.
                    unsafe { timeout.prepare(sqe) };
                })
            }
            fn cancel(&mut self, id: OperationId, target: OperationId) -> liburing::Result<bool> {
                match self.prepare_sqe_with(id, |sqe| {
                    // SAFETY: The ring resolves this target to its live kernel
                    // generation. No target resources are freed by this request.
                    unsafe { liburing::ops::net::prep_cancel(sqe, target, 0) };
                }) {
                    Ok(()) => Ok(true),
                    // Target retirement must remain possible when all application slots are held.
                    Err(UringError::SqRingFull) => self.cancel_untracked(target).map(|()| false),
                    Err(error) => Err(error),
                }
            }
            fn cancel_untracked(&mut self, target: OperationId) -> liburing::Result<()> {
                self.cancel(target).map(|_| ())
            }
        }
    };
}
impl_submit!(Proactor);
impl_submit!(SubmissionContext<'_>);

#[cfg(test)]
mod tests {
    use super::*;

    struct Hook;
    impl NotifyHook for Hook {
        fn on_notify(&mut self, _: crate::NotificationRef<'_>) {}
    }

    #[test]
    fn bitmap_slots_preserve_full_capacity_and_high_slot_reuse() -> Result<()> {
        let listener = PgListener::new(Default::default(), Hook)?;
        let mut driver = ProactorPgListener::new(SubsystemId::new_unchecked(1), listener)?;
        let _: uarch::bits::Bitmap64 = driver.occupied;
        for slot in 0..OPERATION_CAPACITY {
            assert_eq!(driver.vacant(), Some(slot));
            let id = OperationId::pack(driver.subsystem, OperationPayload(slot as u64));
            driver.insert_operation(slot, id, Operand::Cancel);
        }
        assert_eq!(driver.vacant(), None);
        assert_eq!(driver.inflight_count(), 64);
        assert_eq!(driver.active_operations().count(), 64);
        let removed = driver.take_operation(63).map_err(map_ring_error)?;
        assert_eq!(removed.id.payload().raw(), 63);
        assert_eq!(driver.vacant(), Some(63));
        assert_eq!(driver.inflight_count(), 63);
        assert!(driver.take_operation(63).is_err());
        assert_eq!(driver.inflight_count(), 63);
        driver.insert_operation(63, removed.id, Operand::Cancel);
        assert_eq!(driver.vacant(), None);
        assert_eq!(driver.active_operations().count(), 64);
        Ok(())
    }

    #[test]
    fn ring_errors_keep_specific_errno() {
        for (error, expected) in [
            (UringError::CompletionsDropped, libc::EOVERFLOW),
            (UringError::SqThreadGone, libc::EPIPE),
            (
                UringError::OpcodeNotAdmitted {
                    opcode: 0,
                    mode: "io-poll",
                },
                libc::EOPNOTSUPP,
            ),
            (
                UringError::BlockingWaitUnavailable { mode: "sq-poll" },
                libc::EOPNOTSUPP,
            ),
            (
                UringError::CancellationUnavailable { mode: "io-poll" },
                libc::EOPNOTSUPP,
            ),
        ] {
            assert!(
                matches!(map_ring_error(error), PgNotifyError::IoError {
                op: "PostgreSQL proactor", errno,
            } if errno == expected),
                "incorrect mapping for {error}"
            );
        }
    }
}

#[cfg(test)]
#[path = "../tests/proactor/boundaries.rs"]
mod boundary_tests;
