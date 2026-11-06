//! Deterministic, caller-driven listener lifecycle and subscription acknowledgement.
use crate::{
    channel_table::ChannelTable,
    error::{ListenerPhase, ListenerState, PgNotifyError, Result},
    hook::NotifyHook,
    metrics::PgNotifyMetricsSnapshot,
    options::{ListenerOptions, reconnect_delay},
    raw_conn::{QueryProgress, RawPgConn},
    types::{ChannelId, FixedString, ListenCommand, ListenerEvent, SocketInterest, TimerDeadline},
};
use std::{cell::Cell, ffi::CString, rc::Rc, time::Duration};
use uarch::prelude::{cold_sink, hot_inline, ilp_all, patch, pattern, unlikely};
mod state;
use state::FsmStateWord;

/// Callback-safe admission check before attempting to borrow and drive the listener again.
/// Cloning shares the owner-thread flag and never grants access to the connection or hook.
#[derive(Clone)]
pub struct DriveGuard {
    active: Rc<Cell<bool>>,
}
struct DriveScope {
    active: Rc<Cell<bool>>,
    previous: bool,
}
impl Drop for DriveScope {
    fn drop(&mut self) {
        self.active.set(self.previous);
    }
}
impl DriveGuard {
    #[inline]
    fn enter(&self) -> DriveScope {
        DriveScope {
            active: Rc::clone(&self.active),
            previous: self.active.replace(true),
        }
    }
    /// # Errors
    /// Returns ReentrantDriveForbidden while a drive or failure callback is active.
    #[hot_inline]
    pub fn check(&self) -> Result<()> {
        if unlikely(self.active.get()) {
            Err(PgNotifyError::ReentrantDriveForbidden)
        } else {
            Ok(())
        }
    }
}

pub struct PgListener<H: NotifyHook> {
    options: ListenerOptions,
    hook: H,
    metrics: PgNotifyMetricsSnapshot,
    channels: ChannelTable,
    raw: Option<RawPgConn>,
    control: FsmStateWord,
    epoch: u64,
    socket_generation: u64,
    timer_generation: u64,
    timer: TimerDeadline,
    active: Vec<Option<FixedString<64>>>,
    sent: Vec<Option<FixedString<64>>>,
    driving: DriveGuard,
    continuation_generation: u64,
}
impl<H: NotifyHook> PgListener<H> {
    /// # Errors
    /// Rejects zero budgets, invalid capacities, or invalid deadline/keepalive settings.
    pub fn new(options: ListenerOptions, hook: H) -> Result<Self> {
        options.validate()?;
        let channels = ChannelTable::with_capacity(options.channel_capacity)?;
        let count = options.channel_capacity;
        Ok(Self {
            options,
            hook,
            channels,
            metrics: PgNotifyMetricsSnapshot::default(),
            raw: None,
            control: FsmStateWord::disconnected(),
            epoch: 0,
            socket_generation: 0,
            timer_generation: 0,
            timer: TimerDeadline {
                generation: 0,
                duration: Duration::ZERO,
            },
            active: vec![None; count],
            sent: vec![None; count],
            driving: DriveGuard {
                active: Rc::new(Cell::new(false)),
            },
            continuation_generation: 0,
        })
    }
    /// # Errors
    /// Rejects a second connection, invalid URLs or libpq initialization failures.
    pub fn connect(&mut self, database_url: &str) -> Result<()> {
        self.ensure_mutable()?;
        let _scope = self.driving.enter();
        if self.state() != ListenerState::Disconnected {
            return Err(PgNotifyError::invalid_config("already connected"));
        }
        self.raw = Some(RawPgConn::connect(database_url, &self.options)?);
        self.socket_generation += 1;
        self.transition(ListenerPhase::ConnectingWriting);
        self.arm(self.options.connect_timeout);
        self.poll_connect(false).or_else(|error| {
            self.fail(error);
            Ok(())
        })
    }
    #[hot_inline]
    pub const fn state(&self) -> ListenerState {
        self.phase().state()
    }
    #[hot_inline]
    pub const fn phase(&self) -> ListenerPhase {
        self.control.phase()
    }
    #[inline]
    fn set_phase(&mut self, phase: ListenerPhase) {
        self.control = self.control.with_phase(phase);
    }
    #[inline]
    fn transition(&mut self, phase: ListenerPhase) {
        self.set_phase(self.phase().transition(phase));
    }
    #[hot_inline]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }
    #[hot_inline]
    pub fn hook(&self) -> &H {
        &self.hook
    }
    #[hot_inline]
    pub fn hook_mut(&mut self) -> &mut H {
        &mut self.hook
    }
    /// Returns the same admission guard used by drive methods, safe to retain inside callbacks.
    pub fn drive_guard(&self) -> DriveGuard {
        self.driving.clone()
    }
    #[hot_inline]
    pub fn metrics(&self) -> &PgNotifyMetricsSnapshot {
        &self.metrics
    }
    /// # Errors
    /// Rejects invalid names, full tables, or a terminating listener.
    pub fn subscribe(&mut self, channel: &str) -> Result<ChannelId> {
        self.ensure_mutable()?;
        if let Some(id) = self.channels.lookup(channel) {
            return Ok(id);
        }
        let id = self.channels.subscribe(channel)?;
        if self.state() == ListenerState::Listening {
            self.sync_or_fail()?;
        }
        Ok(id)
    }
    /// # Errors
    /// Rejects unknown channels or a terminating listener.
    pub fn unsubscribe(&mut self, channel: &str) -> Result<()> {
        self.ensure_mutable()?;
        let id = self
            .channels
            .lookup(channel)
            .ok_or_else(|| PgNotifyError::invalid_config("unknown channel"))?;
        self.channels.remove(id)?;
        if self.state() == ListenerState::Listening {
            self.sync_or_fail()?;
        }
        Ok(())
    }
    /// # Errors
    /// Returns command validation errors; no more than budget items are consumed.
    pub fn drain_commands<I>(&mut self, budget: usize, commands: I) -> Result<usize>
    where
        I: IntoIterator<Item = ListenCommand>,
    {
        self.ensure_mutable()?;
        let mut count = 0;
        for command in commands.into_iter().take(budget) {
            match command {
                ListenCommand::Subscribe {
                    channel_id,
                    channel,
                } => {
                    self.channels.register_at(channel, channel_id)?;
                    if self.state() == ListenerState::Listening && self.desired_changed() {
                        self.sync_or_fail()?;
                    }
                }
                ListenCommand::Unsubscribe { channel_id } => {
                    let name = *self
                        .channels
                        .channel(channel_id)
                        .ok_or(PgNotifyError::UnknownChannelId(channel_id))?;
                    self.unsubscribe(name.as_str())?;
                }
                ListenCommand::UnsubscribeByName { channel } => {
                    self.unsubscribe(channel.as_str())?
                }
                ListenCommand::Ping => {
                    if self.state() == ListenerState::Listening {
                        let _scope = self.driving.enter();
                        if let Err(error) = self.start_ping() {
                            self.fail(error);
                            return Err(error);
                        }
                    }
                }
                ListenCommand::Reconnect => {
                    if self.raw.is_none() {
                        return Err(PgNotifyError::invalid_config("not connected"));
                    }
                    self.fail(PgNotifyError::connection_failed("requested reconnect"));
                }
                ListenCommand::Shutdown => self.begin_shutdown(),
            }
            count += 1;
            if self.is_terminated() {
                break;
            }
        }
        Ok(count)
    }
    #[inline]
    pub fn poll_interest(&self) -> Option<(i32, u64, SocketInterest)> {
        let interest = SocketInterest {
            readable: self.control.test(FsmStateWord::READ_INTEREST),
            writable: self.control.test(FsmStateWord::WRITE_INTEREST),
        };
        if !(interest.readable | interest.writable) {
            return None;
        }
        self.raw
            .as_ref()
            .map(RawPgConn::socket)
            .filter(|fd| *fd >= 0)
            .map(|fd| (fd, self.socket_generation, interest))
    }
    #[inline]
    pub fn owns_socket(&self, fd: i32, generation: u64) -> bool {
        generation == self.socket_generation
            && self
                .poll_interest()
                .is_some_and(|(current, _, _)| current == fd)
    }
    #[inline]
    pub const fn timer_deadline(&self) -> Option<TimerDeadline> {
        match self.continuation_deadline() {
            Some(deadline) => Some(deadline),
            None => self.armed_deadline(),
        }
    }
    pub(crate) const fn continuation_deadline(&self) -> Option<TimerDeadline> {
        if self.phase().has_continuation() {
            Some(TimerDeadline {
                generation: self.continuation_generation,
                duration: Duration::ZERO,
            })
        } else {
            None
        }
    }
    pub(crate) const fn armed_deadline(&self) -> Option<TimerDeadline> {
        if self.control.matches(pattern!(FsmStateWord { TIMER_ARMED })) {
            Some(self.timer)
        } else {
            None
        }
    }
    /// # Errors
    /// Rejects reentrant drive and events on terminated listeners; transport faults initiate backoff.
    /// A timer event represents expiry, not a clock tick. Stale generations are ignored.
    #[inline]
    pub fn on_event(&mut self, event: ListenerEvent) -> Result<usize> {
        self.ensure_mutable()?;
        let _scope = self.driving.enter();
        match self.drive(event) {
            Ok(count) => Ok(count),
            Err(error) => {
                self.fail(error);
                Ok(0)
            }
        }
    }
    pub fn begin_shutdown(&mut self) {
        self.transition(ListenerPhase::ShuttingDown);
        self.socket_generation += 1;
        self.cancel_timer();
        self.raw = None;
        self.transition(ListenerPhase::Terminated);
    }
    pub const fn is_terminated(&self) -> bool {
        matches!(self.state(), ListenerState::Terminated)
    }
    fn ensure_mutable(&self) -> Result<()> {
        self.driving.check()?;
        if unlikely(self.control.matches(pattern!(FsmStateWord { DRAINING }))) {
            return Err(PgNotifyError::ShuttingDown);
        }
        Ok(())
    }
    fn raw_mut(&mut self) -> Result<&mut RawPgConn> {
        self.raw
            .as_mut()
            .ok_or_else(|| PgNotifyError::connection_failed("missing connection"))
    }
    fn sync_or_fail(&mut self) -> Result<()> {
        let _scope = self.driving.enter();
        self.sync_subscriptions(false)
            .inspect_err(|error| self.fail(*error))
    }
    #[inline]
    fn drive(&mut self, event: ListenerEvent) -> Result<usize> {
        match event {
            ListenerEvent::Timer { generation } => {
                if self.phase().has_continuation() && self.continuation_generation == generation {
                    self.set_phase(self.phase().without_continuation());
                    return self.continue_buffered();
                }
                if !ilp_all!(
                    self.control.test(FsmStateWord::TIMER_ARMED),
                    self.timer.generation == generation
                ) {
                    return Ok(0);
                }
                self.cancel_timer();
                if self.phase().is_offline() {
                    if self.state() == ListenerState::BackoffWait {
                        self.raw_mut()?.reset_start()?;
                        self.socket_generation += 1;
                        self.transition(ListenerPhase::ResettingWriting);
                        self.arm(self.options.connect_timeout);
                        self.poll_connect(true)?;
                    }
                } else if self.phase().is_active_session()
                    && self.state() == ListenerState::Listening
                {
                    self.start_ping()?;
                } else if !self.phase().is_terminating() {
                    return Err(PgNotifyError::timed_out(self.state()));
                }
                Ok(0)
            }
            ListenerEvent::Socket {
                fd,
                readable,
                writable,
                error,
            } => {
                if self.poll_interest().is_none_or(|(owned, _, _)| owned != fd) {
                    return Ok(0);
                }
                if unlikely(error) {
                    return Err(PgNotifyError::connection_failed("socket readiness"));
                }
                if self.phase().is_handshaking() {
                    let reset = self
                        .control
                        .matches(pattern!(FsmStateWord { RECONNECTING }));
                    self.poll_connect(reset)?;
                    return Ok(0);
                }
                match self.state() {
                    ListenerState::FlushingSubscriptions | ListenerState::IdlePingFlushing => {
                        if readable {
                            self.raw_mut()?.consume()?;
                        }
                        if writable || readable {
                            self.flush_query()?;
                        }
                        self.drain_notifications()
                    }
                    ListenerState::AwaitingCommandAck | ListenerState::IdlePingAwaiting => {
                        if readable {
                            self.raw_mut()?.consume()?;
                            self.acknowledge()?;
                        }
                        self.drain_notifications()
                    }
                    ListenerState::Listening => {
                        if readable {
                            self.raw_mut()?.consume()?;
                        }
                        self.drain_notifications()
                    }
                    ListenerState::Disconnected
                    | ListenerState::BackoffWait
                    | ListenerState::ConnectingReading
                    | ListenerState::ConnectingWriting
                    | ListenerState::ResettingReading
                    | ListenerState::ResettingWriting
                    | ListenerState::ShuttingDown
                    | ListenerState::Terminated => Ok(0),
                }
            }
        }
    }
    #[inline]
    fn poll_connect(&mut self, reset: bool) -> Result<()> {
        use pq_sys::PostgresPollingStatusType as Poll;
        let fd_before = self.raw_mut()?.socket();
        let before_socket = (fd_before >= 0).then_some(fd_before);
        let before_identity = socket_identity(fd_before);
        let status = self.raw_mut()?.poll(reset);
        let fd_after = self.raw_mut()?.socket();
        let after_socket = (fd_after >= 0).then_some(fd_after);
        // libpq can replace the open file description without changing the descriptor number.
        // A missing identity matters only while a descriptor is open; fd -1 on both sides is not a replacement.
        let identity_replaced = before_socket.is_some()
            && (before_identity.is_none() || before_identity != socket_identity(fd_after));
        if before_socket != after_socket || identity_replaced {
            self.socket_generation += 1;
        }
        match status {
            Poll::PGRES_POLLING_READING => {
                self.transition(if reset {
                    ListenerPhase::ResettingReading
                } else {
                    ListenerPhase::ConnectingReading
                });
            }
            Poll::PGRES_POLLING_WRITING | Poll::PGRES_POLLING_ACTIVE => {
                self.transition(if reset {
                    ListenerPhase::ResettingWriting
                } else {
                    ListenerPhase::ConnectingWriting
                });
            }
            Poll::PGRES_POLLING_FAILED => {
                return Err(PgNotifyError::connection_failed("handshake"));
            }
            Poll::PGRES_POLLING_OK => {
                self.cancel_timer();
                self.active.fill(None);
                self.metrics.record_connect();
                self.sync_subscriptions(true)?;
            }
        }
        Ok(())
    }
    fn sync_subscriptions(&mut self, replay: bool) -> Result<()> {
        // UNLISTEN * makes removal and slot reuse atomic with a single acknowledged desired snapshot.
        let mut sql = String::from("UNLISTEN *;");
        for (_, name) in self.channels.iter() {
            sql.push_str("LISTEN \"");
            for ch in name.as_str().chars() {
                if ch == '"' {
                    sql.push('"');
                }
                sql.push(ch);
            }
            sql.push_str("\";");
        }
        self.sent.fill(None);
        for (id, name) in self.channels.iter() {
            self.sent[usize::from(id.0)] = Some(*name);
        }
        let sql = CString::new(sql)
            .map_err(|_| PgNotifyError::invalid_config("channel SQL contains NUL"))?;
        self.raw_mut()?.send_query(&sql)?;
        self.transition(if replay {
            ListenerPhase::FlushingReplay
        } else {
            ListenerPhase::FlushingSubscriptions
        });
        self.arm(self.options.command_timeout);
        self.flush_query()
    }
    #[inline]
    fn flush_query(&mut self) -> Result<()> {
        if self.raw_mut()?.flush()? {
            self.set_phase(self.phase().after_flush());
            self.acknowledge()?;
        }
        Ok(())
    }
    #[inline]
    fn acknowledge(&mut self) -> Result<()> {
        for _ in 0..self.options.notification_budget {
            match self.raw_mut()?.result()? {
                QueryProgress::Busy => return Ok(()),
                QueryProgress::Result => (),
                QueryProgress::Done => {
                    let ping = self.state() == ListenerState::IdlePingAwaiting;
                    let replay = self.phase().is_subscription_replay();
                    if !ping {
                        // Retain replay identity and a wake while subscription callbacks can unwind.
                        self.request_continuation();
                        self.apply_subscriptions();
                    }
                    self.transition(ListenerPhase::Listening);
                    self.cancel_timer();
                    if self.desired_changed() {
                        self.sync_subscriptions(replay)?;
                        return Ok(());
                    }
                    if let Some(interval) = self.options.idle_ping_interval {
                        self.arm(interval);
                    }
                    if replay {
                        self.request_continuation();
                    }
                    self.complete_replay(replay);
                    if replay {
                        // The readiness callback returned; its unwind-only wake is no longer needed.
                        self.set_phase(self.phase().without_continuation());
                    }
                    return Ok(());
                }
            }
        }
        // Completed results can remain solely in libpq's buffer; a caller timer drives the next batch.
        self.request_continuation();
        Ok(())
    }
    fn desired_changed(&self) -> bool {
        (0..self.active.len()).any(|index| {
            self.active[index].as_ref() != self.channels.channel(ChannelId(index as u16))
        })
    }
    fn apply_subscriptions(&mut self) {
        for index in 0..self.active.len() {
            if self.active[index] == self.sent[index] {
                continue;
            }
            let id = ChannelId(index as u16);
            if let Some(name) = self.active[index].take() {
                self.hook.on_unsubscribed(id, name.as_str());
            }
            if let Some(name) = self.sent[index] {
                self.active[index] = Some(name);
                self.hook.on_subscribed(id, name.as_str());
            }
        }
    }
    #[inline]
    fn request_continuation(&mut self) {
        if !self.phase().has_continuation() {
            self.timer_generation += 1;
            self.continuation_generation = self.timer_generation;
            self.set_phase(self.phase().with_continuation());
        }
    }
    #[inline]
    fn continue_buffered(&mut self) -> Result<usize> {
        if self
            .control
            .matches(pattern!(FsmStateWord { HAS_PENDING_CMDS }))
        {
            if self.control.test(FsmStateWord::WRITE_INTEREST) {
                self.flush_query()?;
            } else {
                self.acknowledge()?;
            }
        } else if self.state() != ListenerState::Listening {
            return Ok(0);
        }
        self.drain_notifications()
    }
    fn complete_replay(&mut self, replay: bool) {
        if replay {
            self.epoch += 1;
            self.control = self.control.patch(patch!(FsmStateWord { RETRIES: 0 }));
            let pid = self.raw.as_ref().map_or(0, RawPgConn::backend_pid);
            if self.epoch == 1 {
                self.hook.on_connected(pid);
            } else {
                self.metrics.record_reconnect();
                self.hook.on_reconnected(self.epoch, pid);
            }
        }
    }
    #[inline]
    fn drain_notifications(&mut self) -> Result<usize> {
        if self.state() != ListenerState::Listening {
            return Ok(0);
        }
        let had_continuation = self.phase().has_continuation();
        let mut count = 0;
        for _ in 0..self.options.notification_budget {
            let Some(guard) = self.raw_mut()?.notification() else {
                break;
            };
            let mut notify = guard.notification()?;
            notify.channel_id = self.channels.lookup(notify.channel);
            if count == 0 {
                // A caught callback panic must leave a wake for libpq-buffered notifications.
                self.request_continuation();
            }
            self.hook.on_notify(notify);
            self.metrics.record_notification();
            count += 1;
        }
        if count > 0
            && let Some(interval) = self.options.idle_ping_interval
        {
            self.arm(interval);
        }
        // libpq may have read more than one budget; a zero-delay wake avoids stranding buffered data.
        if count == self.options.notification_budget {
            self.request_continuation();
        } else if !had_continuation {
            self.set_phase(self.phase().without_continuation());
        }
        Ok(count)
    }
    fn start_ping(&mut self) -> Result<()> {
        self.raw_mut()?.send_query(c"SELECT 1;")?;
        self.metrics.record_ping();
        self.transition(ListenerPhase::IdlePingFlushing);
        self.arm(self.options.command_timeout);
        self.flush_query()
    }
    #[cold_sink]
    fn fail(&mut self, error: PgNotifyError) {
        let _scope = self.driving.enter();
        self.set_phase(self.phase().without_continuation());
        self.metrics.record_error();
        self.metrics.record_disconnect();
        self.socket_generation += 1;
        self.transition(ListenerPhase::BackoffWait);
        self.arm(reconnect_delay(
            self.options.reconnect_initial,
            self.options.reconnect_max,
            self.control.get(FsmStateWord::RETRIES),
        ));
        self.control = self.control.next_retry();
        // Publish recoverable ownership and deadlines before invoking fallible user code.
        self.hook.on_error(error);
        self.hook.on_disconnect(self.epoch, error);
    }
    #[inline]
    fn arm(&mut self, duration: Duration) {
        self.timer_generation += 1;
        self.timer = TimerDeadline {
            generation: self.timer_generation,
            duration,
        };
        self.control = self
            .control
            .patch(patch!(FsmStateWord { TIMER_ARMED: true }));
    }
    #[inline]
    fn cancel_timer(&mut self) {
        self.set_phase(self.phase().without_continuation());
        self.timer_generation += 1;
        self.control = self
            .control
            .patch(patch!(FsmStateWord { TIMER_ARMED: false }));
    }
}

fn socket_identity(fd: i32) -> Option<(libc::dev_t, libc::ino_t)> {
    if fd < 0 {
        return None;
    }
    let mut stat = core::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: stat points to writable storage; fstat does not retain it or take ownership of fd.
    if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: a successful fstat initialized every field read below.
    let stat = unsafe { stat.assume_init() };
    Some((stat.st_dev, stat.st_ino))
}

#[cfg(test)]
mod tests;
