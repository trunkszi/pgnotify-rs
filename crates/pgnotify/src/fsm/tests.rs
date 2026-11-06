use super::*;
struct Hook;
impl NotifyHook for Hook {
    fn on_notify(&mut self, _: crate::NotificationRef<'_>) {}
}
fn listener() -> Result<PgListener<Hook>> {
    PgListener::new(ListenerOptions::default(), Hook)
}
#[test]
fn full_width_generations_and_armed_deadline_survive_continuation() -> Result<()> {
    let mut listener = listener()?;
    listener.set_phase(ListenerPhase::Listening);
    listener.timer_generation = u64::MAX - 3;
    listener.socket_generation = (1_u64 << 61) + 1;
    listener.arm(Duration::from_secs(15));
    let armed = listener
        .timer_deadline()
        .ok_or(PgNotifyError::ShuttingDown)?;
    assert_eq!(armed.generation, u64::MAX - 2);
    assert_eq!(armed.duration, Duration::from_secs(15));
    listener.request_continuation();
    let immediate = listener
        .timer_deadline()
        .ok_or(PgNotifyError::ShuttingDown)?;
    assert_eq!(immediate.generation, u64::MAX - 1);
    assert_eq!(immediate.duration, Duration::ZERO);
    listener.set_phase(listener.phase().without_continuation());
    assert_eq!(listener.timer_deadline(), Some(armed));
    listener.cancel_timer();
    assert_eq!(listener.timer_generation, u64::MAX);
    assert_eq!(listener.socket_generation, (1_u64 << 61) + 1);
    assert!(listener.timer_deadline().is_none());
    assert!(!listener.control.test(FsmStateWord::TIMER_ARMED));
    Ok(())
}
#[test]
fn retry_word_controls_backoff_and_replay_resets_the_exponent() -> Result<()> {
    let mut listener = listener()?;
    for millis in [100, 200, 400, 800, 1600, 3200, 6400] {
        listener.fail(PgNotifyError::connection_failed("test"));
        assert_eq!(listener.state(), ListenerState::BackoffWait);
        assert_eq!(
            listener.timer_deadline().map(|timer| timer.duration),
            Some(Duration::from_millis(millis))
        );
    }
    assert_eq!(listener.control.get(FsmStateWord::RETRIES), 7);
    listener.set_phase(ListenerPhase::Listening);
    listener.complete_replay(true);
    assert_eq!(listener.control.get(FsmStateWord::RETRIES), 0);
    listener.fail(PgNotifyError::connection_failed("test"));
    assert_eq!(
        listener.timer_deadline().map(|timer| timer.duration),
        Some(Duration::from_millis(100))
    );
    Ok(())
}

use crate::test_support as support;

#[test]
fn command_errors_arm_reconnect() -> std::result::Result<(), Box<dyn std::error::Error>> {
    for subscribe in [false, true] {
        let mut server = support::MockServer::new()?;
        let mut listener = PgListener::new(
            ListenerOptions {
                idle_ping_interval: None,
                ..Default::default()
            },
            Hook,
        )?;
        listener.subscribe("base")?;
        listener.connect(&server.url()?)?;
        support::until(&mut server, &mut listener, |listener, _| {
            listener.epoch() == 1 && listener.timer_deadline().is_none()
        })?;
        server.disconnect()?;
        let mut broken = false;
        for _ in 0..100_000 {
            if listener.raw_mut()?.consume().is_err() {
                broken = true;
                break;
            }
        }
        assert!(
            broken,
            "native connection must observe EOF before the command"
        );
        assert!(listener.poll_interest().is_none());
        let command = if subscribe {
            ListenCommand::Subscribe {
                channel_id: ChannelId(1),
                channel: FixedString::try_from_str("orders")?,
            }
        } else {
            ListenCommand::Ping
        };
        assert!(listener.drain_commands(1, [command]).is_err());
        assert_eq!(listener.state(), ListenerState::BackoffWait);
        assert_eq!(listener.metrics().errors, 1);
        assert!(listener.timer_deadline().is_some());
        listener.begin_shutdown();
    }
    Ok(())
}
