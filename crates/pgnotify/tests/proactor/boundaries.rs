use super::*;
struct Hook;
impl NotifyHook for Hook {
    fn on_notify(&mut self, _: crate::NotificationRef<'_>) {}
}
#[test]
fn exhausted_ids_still_allow_cancellation() -> Result<()> {
    struct Probe {
        canceled: Vec<OperationId>,
        full: bool,
    }
    impl Submit for Probe {
        fn poll(&mut self, _: OperationId, _: Rc<OwnedFd>, _: u32) -> liburing::Result<()> {
            panic!("shutdown must not stage a poll")
        }
        fn timeout(&mut self, _: OperationId, _: &Timeout) -> liburing::Result<()> {
            panic!("shutdown must not stage a timer")
        }
        fn cancel(&mut self, _: OperationId, _: OperationId) -> liburing::Result<bool> {
            panic!("exhausted identity must use untracked cancellation")
        }
        fn cancel_untracked(&mut self, target: OperationId) -> liburing::Result<()> {
            if self.full {
                return Err(UringError::SqRingFull);
            }
            self.canceled.push(target);
            Ok(())
        }
    }
    let listener = PgListener::new(Default::default(), Hook)?;
    let mut driver = ProactorPgListener::new(SubsystemId(1), listener)?;
    let target = driver.next_id(0).map_err(map_ring_error)?;
    driver.insert_operation(
        0,
        target,
        Operand::Poll {
            fd: -1,
            generation: 1,
            interest: SocketInterest {
                readable: true,
                writable: false,
            },
        },
    );
    // No kernel operation or invalid fd is used: Probe records cancellation only.
    driver.sequence = (OperationPayload::MAX_RAW >> 6) + 1;
    driver.shutting_down = true;
    let mut probe = Probe {
        canceled: Vec::new(),
        full: true,
    };
    driver.flush_to(&mut probe).map_err(map_ring_error)?;
    assert!(probe.canceled.is_empty());
    assert!(
        driver.operations[0]
            .as_ref()
            .is_some_and(|operation| !operation.canceling)
    );
    probe.full = false;
    driver.flush_to(&mut probe).map_err(map_ring_error)?;
    assert_eq!(probe.canceled, [target]);
    assert_eq!(driver.inflight_count(), 1);
    assert!(
        driver.operations[0]
            .as_ref()
            .is_some_and(|operation| operation.canceling)
    );
    Ok(())
}

#[test]
fn last_identity_preserves_slot_bits() -> Result<()> {
    for slot in [0, OPERATION_CAPACITY - 1] {
        let listener = PgListener::new(Default::default(), Hook)?;
        let mut driver = ProactorPgListener::new(SubsystemId(1), listener)?;
        let last_sequence = OperationPayload::MAX_RAW >> 6;
        driver.sequence = last_sequence;
        let last = driver.next_id(slot).map_err(map_ring_error)?;
        assert_eq!(last.payload().raw(), (last_sequence << 6) | slot as u64);
        assert!(matches!(
            driver.next_id(slot),
            Err(UringError::PayloadOverflow(_))
        ));
        assert_eq!(driver.sequence, last_sequence + 1);
        assert_eq!(driver.inflight_count(), 0);
    }
    Ok(())
}

#[test]
fn foreign_context_preserves_local_slot() -> Result<()> {
    let listener = PgListener::new(Default::default(), Hook)?;
    let mut driver = ProactorPgListener::new(SubsystemId(1), listener)?;
    let mut owner = Proactor::builder()
        .entries(8)
        .build()
        .map_err(map_ring_error)?;
    let mut foreign = Proactor::builder()
        .entries(8)
        .build()
        .map_err(map_ring_error)?;
    driver.flush(&mut owner)?;
    let id = driver.next_id(0).map_err(map_ring_error)?;
    // The local slot holds no kernel operand; the foreign ring executes only NOP.
    driver.insert_operation(0, id, Operand::Cancel);
    foreign.alloc_sqe(id).map_err(map_ring_error)?;
    let mut seen = false;
    foreign
        .wait_completions(
            1,
            Some(Duration::from_secs(1)),
            &mut |completion, context: &mut SubmissionContext<'_>| {
                assert!(driver.dispatch(completion, context).is_err());
                assert_eq!(driver.inflight_count(), 1);
                assert!(
                    driver.operations[0]
                        .as_ref()
                        .is_some_and(|operation| operation.id == id)
                );
                context.claim_completion(completion)?;
                seen = true;
                Ok(())
            },
        )
        .map_err(map_ring_error)?;
    assert!(seen);
    driver.take_operation(0).map_err(map_ring_error)?;
    owner.shutdown().map_err(map_ring_error)?;
    foreign.shutdown().map_err(map_ring_error)?;
    Ok(())
}

use crate::test_support as support;

#[test]
fn continuation_retains_armed_timeout() -> std::result::Result<(), Box<dyn std::error::Error>> {
    struct Probe {
        canceled: Vec<OperationId>,
    }
    impl Submit for Probe {
        fn poll(&mut self, _: OperationId, _: Rc<OwnedFd>, _: u32) -> liburing::Result<()> {
            Ok(())
        }
        fn timeout(&mut self, _: OperationId, _: &Timeout) -> liburing::Result<()> {
            Ok(())
        }
        fn cancel(&mut self, _: OperationId, target: OperationId) -> liburing::Result<bool> {
            self.canceled.push(target);
            Ok(false)
        }
        fn cancel_untracked(&mut self, target: OperationId) -> liburing::Result<()> {
            self.canceled.push(target);
            Ok(())
        }
    }
    let mut server = support::MockServer::new()?;
    server.hold_ack = true;
    let mut listener = PgListener::new(
        crate::ListenerOptions {
            notification_budget: 1,
            idle_ping_interval: None,
            ..Default::default()
        },
        Hook,
    )?;
    listener.subscribe("orders")?;
    listener.connect(&server.url()?)?;
    support::until(&mut server, &mut listener, |listener, _| {
        listener.state() == crate::ListenerState::AwaitingCommandAck
    })?;
    let armed = listener.timer_deadline().ok_or("command deadline")?;
    let mut driver = ProactorPgListener::new(SubsystemId(1), listener)?;
    let mut probe = Probe {
        canceled: Vec::new(),
    };
    driver.flush_to(&mut probe).map_err(map_ring_error)?;
    let timer_id = driver.active_operations().find_map(|operation| {
        matches!(operation.operand, Operand::Timer { generation, .. } if generation == armed.generation).then_some(operation.id)
    }).ok_or("staged command timeout")?;
    server.release();
    support::until(&mut server, &mut driver.listener, |listener, _| {
        listener.phase().has_continuation()
    })?;
    let immediate = driver
        .listener
        .timer_deadline()
        .ok_or("continuation deadline")?;
    assert!(immediate.duration.is_zero());
    driver.flush_to(&mut probe).map_err(map_ring_error)?;
    assert!(
        !probe.canceled.contains(&timer_id),
        "a continuation must not cancel or restart the armed timeout"
    );
    assert!(driver.active_operations().any(|operation| {
        matches!(operation.operand, Operand::Timer { generation, .. } if generation == immediate.generation)
    }));
    // Probe never exposed an operand to the kernel, so all local allocations can be reclaimed.
    for operation in &mut driver.operations {
        drop(operation.take());
    }
    driver.occupied = Bitmap64::EMPTY;
    driver.listener.begin_shutdown();
    Ok(())
}
