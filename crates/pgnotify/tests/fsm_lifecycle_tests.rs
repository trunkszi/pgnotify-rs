mod support;
use pgnotify::{
    ChannelId, ListenerEvent, ListenerOptions, ListenerState, NotificationRef, NotifyHook,
    PgListener, PgNotifyError,
};
use support::{MockServer, pump, until};

#[derive(Default)]
struct Hook {
    events: Vec<String>,
    frames: Vec<pgnotify::NotifyFrame<32>>,
    errors: Vec<PgNotifyError>,
}
impl NotifyHook for Hook {
    fn on_connected(&mut self, pid: i32) {
        self.events.push(format!("connected:{pid}"));
    }
    fn on_reconnected(&mut self, epoch: u64, _: i32) {
        self.events.push(format!("reconnected:{epoch}"));
    }
    fn on_subscribed(&mut self, _: ChannelId, channel: &str) {
        self.events.push(format!("sub:{channel}"));
    }
    fn on_unsubscribed(&mut self, _: ChannelId, channel: &str) {
        self.events.push(format!("unsub:{channel}"));
    }
    fn on_notify(&mut self, notification: NotificationRef<'_>) {
        if let Ok(frame) = notification.to_frame() {
            self.frames.push(frame);
        }
    }
    fn on_error(&mut self, error: PgNotifyError) {
        self.errors.push(error);
    }
}

#[test]
fn nonblocking_handshake_listen_notify_unlisten_and_shutdown()
-> Result<(), Box<dyn std::error::Error>> {
    let mut server = MockServer::new()?;
    let mut listener = PgListener::new(ListenerOptions::default(), Hook::default())?;
    let id = listener.subscribe("Orders\"live")?;
    listener.connect(&server.url()?)?;
    until(&mut server, &mut listener, |listener, _| {
        listener.epoch() == 1
    })?;
    assert_eq!(listener.state(), ListenerState::Listening);
    assert!(server.queries[0].contains("LISTEN \"Orders\"\"live\";"));
    assert!(listener.hook().events.contains(&"connected:1234".into()));
    server.notify("Orders\"live", "filled");
    until(&mut server, &mut listener, |listener, _| {
        !listener.hook().frames.is_empty()
    })?;
    assert_eq!(listener.hook().frames[0].channel_id, id);
    assert_eq!(&listener.hook().frames[0].payload[..6], b"filled");
    listener.unsubscribe("Orders\"live")?;
    until(&mut server, &mut listener, |listener, _| {
        listener.state() == ListenerState::Listening
    })?;
    assert!(
        listener
            .hook()
            .events
            .contains(&"unsub:Orders\"live".into())
    );
    listener.begin_shutdown();
    assert!(listener.is_terminated());
    assert!(listener.poll_interest().is_none());
    assert!(listener.timer_deadline().is_none());
    Ok(())
}

#[test]
fn reconnect_replays_before_epoch_callback_and_rejects_stale_deadlines()
-> Result<(), Box<dyn std::error::Error>> {
    let mut server = MockServer::new()?;
    let mut listener = PgListener::new(ListenerOptions::default(), Hook::default())?;
    listener.subscribe("orders")?;
    listener.connect(&server.url()?)?;
    until(&mut server, &mut listener, |listener, _| {
        listener.epoch() == 1
    })?;
    let (old_fd, old_generation, _) = listener.poll_interest().ok_or("missing socket")?;
    server.disconnect()?;
    until(&mut server, &mut listener, |listener, _| {
        listener.state() == ListenerState::BackoffWait
    })?;
    assert!(!listener.owns_socket(old_fd, old_generation));
    let retry = listener.timer_deadline().ok_or("missing retry")?;
    listener.on_event(ListenerEvent::Timer {
        generation: retry.generation - 1,
    })?;
    assert_eq!(listener.state(), ListenerState::BackoffWait);
    server.hold_ack = true;
    listener.on_event(ListenerEvent::Timer {
        generation: retry.generation,
    })?;
    until(&mut server, &mut listener, |listener, server| {
        server.sessions == 2
            && listener.state() == ListenerState::AwaitingCommandAck
            && server.queries.len() == 2
    })?;
    assert_eq!(listener.epoch(), 1);
    assert!(!listener.hook().events.contains(&"reconnected:2".into()));
    assert!(server.queries[1].contains("LISTEN \"orders\";"));
    server.release();
    until(&mut server, &mut listener, |listener, _| {
        listener.epoch() == 2
    })?;
    let events = &listener.hook().events;
    assert_eq!(events.last().map(String::as_str), Some("reconnected:2"));
    assert_eq!(events[events.len() - 2], "sub:orders");
    Ok(())
}

#[test]
fn stalled_idle_ping_enters_bounded_reconnect_backoff() -> Result<(), Box<dyn std::error::Error>> {
    let mut server = MockServer::new()?;
    let mut listener = PgListener::new(ListenerOptions::default(), Hook::default())?;
    listener.connect(&server.url()?)?;
    until(&mut server, &mut listener, |listener, _| {
        listener.epoch() == 1
    })?;
    server.stall_ping = true;
    let idle = listener.timer_deadline().ok_or("idle timer missing")?;
    listener.on_event(ListenerEvent::Timer {
        generation: idle.generation,
    })?;
    until(&mut server, &mut listener, |listener, server| {
        listener.state() == ListenerState::IdlePingAwaiting
            && server.queries.iter().any(|q| q == "SELECT 1;")
    })?;
    let probe = listener.timer_deadline().ok_or("probe timer missing")?;
    listener.on_event(ListenerEvent::Timer {
        generation: probe.generation,
    })?;
    assert_eq!(listener.state(), ListenerState::BackoffWait);
    assert!(matches!(
        listener.hook().errors.last(),
        Some(PgNotifyError::TimedOut {
            state: ListenerState::IdlePingAwaiting
        })
    ));
    Ok(())
}

#[test]
fn buffered_notifications_continue_in_bounded_batches() -> Result<(), Box<dyn std::error::Error>> {
    let mut server = MockServer::new()?;
    let options = ListenerOptions {
        notification_budget: 2,
        ..ListenerOptions::default()
    };
    let mut listener = PgListener::new(options, Hook::default())?;
    listener.subscribe("orders")?;
    listener.connect(&server.url()?)?;
    until(&mut server, &mut listener, |listener, _| {
        listener.epoch() == 1
    })?;
    for _ in 0..7 {
        server.notify("orders", "batch");
    }
    until(&mut server, &mut listener, |listener, _| {
        listener.hook().frames.len() == 7
    })?;
    assert_eq!(listener.metrics().notifications, 7);
    assert_eq!(listener.epoch(), 1);
    for _ in 0..3 {
        pump(&mut server, &mut listener)?;
    }
    Ok(())
}

#[test]
fn desired_subscriptions_changed_during_replay_are_acknowledged_before_connected()
-> Result<(), Box<dyn std::error::Error>> {
    let mut server = MockServer::new()?;
    server.hold_ack = true;
    let mut listener = PgListener::new(ListenerOptions::default(), Hook::default())?;
    let old = listener.subscribe("old")?;
    listener.connect(&server.url()?)?;
    until(&mut server, &mut listener, |listener, server| {
        listener.state() == ListenerState::AwaitingCommandAck && !server.queries.is_empty()
    })?;
    listener.unsubscribe("old")?;
    assert_eq!(listener.subscribe("new")?, old);
    assert_eq!(listener.epoch(), 0);
    server.release();
    until(&mut server, &mut listener, |listener, _| {
        listener.epoch() == 1
    })?;
    assert_eq!(server.queries.len(), 2);
    assert_eq!(server.queries[1], "UNLISTEN *;LISTEN \"new\";");
    assert_eq!(
        listener.hook().events.last().map(String::as_str),
        Some("connected:1234")
    );
    assert!(listener.hook().events.contains(&"sub:new".into()));
    Ok(())
}

#[test]
fn command_budget_and_explicit_channel_id_are_respected() -> Result<(), Box<dyn std::error::Error>>
{
    use pgnotify::{FixedString, ListenCommand};
    let mut listener = PgListener::new(ListenerOptions::default(), Hook::default())?;
    let commands = [
        ListenCommand::Subscribe {
            channel_id: ChannelId(20),
            channel: FixedString::try_from_str("explicit")?,
        },
        ListenCommand::Shutdown,
    ];
    let mut commands = commands.into_iter();
    assert_eq!(listener.drain_commands(1, &mut commands)?, 1);
    assert!(!listener.is_terminated());
    assert_eq!(listener.subscribe("explicit")?, ChannelId(20));
    assert_eq!(listener.drain_commands(2, &mut commands)?, 1);
    assert!(listener.is_terminated());
    assert!(matches!(
        listener.subscribe("late"),
        Err(PgNotifyError::ShuttingDown)
    ));
    Ok(())
}

#[test]
fn connect_timeout_uses_injected_timer_and_backoff_is_exponential()
-> Result<(), Box<dyn std::error::Error>> {
    let mut server = MockServer::new()?;
    let options = ListenerOptions {
        reconnect_initial: std::time::Duration::from_millis(5),
        reconnect_max: std::time::Duration::from_millis(12),
        ..ListenerOptions::default()
    };
    let mut listener = PgListener::new(options, Hook::default())?;
    listener.connect(&server.url()?)?;
    let deadline = listener
        .timer_deadline()
        .ok_or("missing connect deadline")?;
    listener.on_event(ListenerEvent::Timer {
        generation: deadline.generation,
    })?;
    assert_eq!(listener.state(), ListenerState::BackoffWait);
    assert_eq!(
        listener.timer_deadline().ok_or("retry")?.duration,
        std::time::Duration::from_millis(5)
    );
    for expected in [10, 12, 12] {
        let timer = listener.timer_deadline().ok_or("retry timer")?;
        listener.on_event(ListenerEvent::Timer {
            generation: timer.generation,
        })?;
        let connect = listener.timer_deadline().ok_or("connect timer")?;
        listener.on_event(ListenerEvent::Timer {
            generation: connect.generation,
        })?;
        assert_eq!(
            listener.timer_deadline().ok_or("new retry")?.duration,
            std::time::Duration::from_millis(expected)
        );
    }
    listener.begin_shutdown();
    server.step()?;
    Ok(())
}

#[test]
fn subscription_changes_queued_during_ping_are_flushed_after_ping_ack()
-> Result<(), Box<dyn std::error::Error>> {
    let mut server = MockServer::new()?;
    let mut listener = PgListener::new(ListenerOptions::default(), Hook::default())?;
    listener.connect(&server.url()?)?;
    until(&mut server, &mut listener, |listener, _| {
        listener.epoch() == 1
    })?;
    server.hold_ack = true;
    let timer = listener.timer_deadline().ok_or("idle timer")?;
    listener.on_event(ListenerEvent::Timer {
        generation: timer.generation,
    })?;
    until(&mut server, &mut listener, |listener, server| {
        listener.state() == ListenerState::IdlePingAwaiting && server.queries.len() == 2
    })?;
    listener.subscribe("during_ping")?;
    server.release();
    until(&mut server, &mut listener, |listener, server| {
        listener.state() == ListenerState::Listening && server.queries.len() == 3
    })?;
    assert!(server.queries[2].contains("LISTEN \"during_ping\""));
    assert!(listener.hook().events.contains(&"sub:during_ping".into()));
    Ok(())
}

#[test]
fn configuration_rejects_invalid_budgets_and_blocking_name_resolution()
-> Result<(), Box<dyn std::error::Error>> {
    let options = ListenerOptions {
        channel_capacity: 0,
        ..ListenerOptions::default()
    };
    assert!(matches!(
        PgListener::new(options, Hook::default()),
        Err(PgNotifyError::InvalidConfig {
            reason: "channel_capacity"
        })
    ));
    let options = ListenerOptions {
        notification_budget: 0,
        ..ListenerOptions::default()
    };
    assert!(PgListener::new(options, Hook::default()).is_err());
    let options = ListenerOptions {
        keepalives_count: 0,
        ..ListenerOptions::default()
    };
    assert!(PgListener::new(options, Hook::default()).is_err());
    let mut listener = PgListener::new(ListenerOptions::default(), Hook::default())?;
    assert!(matches!(
        listener.connect("postgresql://localhost/test"),
        Err(PgNotifyError::InvalidConfig { .. })
    ));
    assert!(
        listener
            .connect("postgresql://test@127.0.0.1/test\0")
            .is_err()
    );
    assert_eq!(listener.state(), ListenerState::Disconnected);
    Ok(())
}

#[test]
fn endpoint_lists_validate_every_host_and_numeric_override()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::new()?;
    let port = server
        .url()?
        .split(':')
        .nth(2)
        .ok_or("missing mock port")?
        .split('/')
        .next()
        .ok_or("missing port separator")?
        .to_owned();
    let options = ListenerOptions::default();
    for host_spec in [
        "host=/tmp,localhost",
        "host=@socket,localhost",
        "host=localhost",
        "host=/tmp,,127.0.0.1",
        "host=,127.0.0.1",
        "host=127.0.0.1,",
        "host=''",
        "hostaddr=127.0.0.1,localhost",
        "hostaddr=127.0.0.1,",
        "hostaddr=,127.0.0.1",
        "host=localhost,second hostaddr=127.0.0.1",
        "host=localhost hostaddr=127.0.0.1,127.0.0.1",
        "host=/tmp,localhost hostaddr=127.0.0.1,",
    ] {
        let conninfo = format!("{host_spec} port={port} user=test dbname=test sslmode=disable");
        assert!(
            matches!(
                pgnotify::raw_conn::RawPgConn::connect(&conninfo, &options),
                Err(PgNotifyError::InvalidConfig { .. })
            ),
            "accepted endpoint: {host_spec}"
        );
    }
    for host_spec in [
        "host=/tmp,127.0.0.1",
        "host=127.0.0.1,::1",
        "hostaddr=127.0.0.1,::1",
        "host=primary,secondary hostaddr=127.0.0.1,::1",
        "host=/tmp,localhost hostaddr=127.0.0.1,127.0.0.1",
    ] {
        let conninfo = format!("{host_spec} port={port} user=test dbname=test sslmode=disable");
        assert!(
            pgnotify::raw_conn::RawPgConn::connect(&conninfo, &options).is_ok(),
            "rejected endpoint: {host_spec}"
        );
    }
    Ok(())
}

#[test]
fn error_and_disconnect_callbacks_keep_the_reentry_admission_guard_active()
-> Result<(), Box<dyn std::error::Error>> {
    #[derive(Default)]
    struct ReentryHook {
        guard: Option<pgnotify::fsm::DriveGuard>,
        results: Vec<pgnotify::Result<()>>,
    }
    impl NotifyHook for ReentryHook {
        fn on_notify(&mut self, _: NotificationRef<'_>) {}
        fn on_error(&mut self, _: PgNotifyError) {
            if let Some(guard) = &self.guard {
                self.results.push(guard.check());
            }
        }
        fn on_disconnect(&mut self, _: u64, _: PgNotifyError) {
            if let Some(guard) = &self.guard {
                self.results.push(guard.check());
            }
        }
    }
    let server = MockServer::new()?;
    let mut listener = PgListener::new(ListenerOptions::default(), ReentryHook::default())?;
    let guard = listener.drive_guard();
    listener.hook_mut().guard = Some(guard.clone());
    listener.connect(&server.url()?)?;
    let timer = listener.timer_deadline().ok_or("missing connect timeout")?;
    listener.on_event(ListenerEvent::Timer {
        generation: timer.generation,
    })?;
    assert_eq!(
        listener.hook().results,
        vec![Err(PgNotifyError::ReentrantDriveForbidden); 2]
    );
    assert_eq!(guard.check(), Ok(()));
    assert_eq!(listener.state(), ListenerState::BackoffWait);
    listener.on_event(ListenerEvent::Timer {
        generation: timer.generation,
    })?;
    assert_eq!(listener.hook().results.len(), 2);
    Ok(())
}
