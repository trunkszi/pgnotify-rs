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
fn handshake_subscription_notify_shutdown() -> Result<(), Box<dyn std::error::Error>> {
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
fn reconnect_replay_rejects_stale_timers() -> Result<(), Box<dyn std::error::Error>> {
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
fn stalled_ping_enters_backoff() -> Result<(), Box<dyn std::error::Error>> {
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
fn buffered_notifications_respect_budget() -> Result<(), Box<dyn std::error::Error>> {
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
fn replay_waits_for_subscription_changes() -> Result<(), Box<dyn std::error::Error>> {
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
fn command_budget_respects_explicit_id() -> Result<(), Box<dyn std::error::Error>> {
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
fn connect_timeout_uses_backoff() -> Result<(), Box<dyn std::error::Error>> {
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
fn subscriptions_wait_for_ping_ack() -> Result<(), Box<dyn std::error::Error>> {
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
fn invalid_config_and_dns_rejected() -> Result<(), Box<dyn std::error::Error>> {
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
fn host_lists_validate_numeric_overrides() -> Result<(), Box<dyn std::error::Error>> {
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
fn callbacks_hold_reentry_guard() -> Result<(), Box<dyn std::error::Error>> {
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

#[test]
fn replay_survives_continuation_and_resync() -> Result<(), Box<dyn std::error::Error>> {
    let mut server = MockServer::new()?;
    server.hold_ack = true;
    let options = ListenerOptions {
        notification_budget: 1,
        ..ListenerOptions::default()
    };
    let mut listener = PgListener::new(options, Hook::default())?;
    assert!(listener.phase().is_offline());
    listener.subscribe("orders.alpha")?;
    listener.subscribe("orders.beta")?;
    listener.connect(&server.url()?)?;
    until(&mut server, &mut listener, |listener, _| {
        listener.state() == ListenerState::AwaitingCommandAck
    })?;
    assert!(listener.phase().is_active_session());
    assert!(listener.phase().is_subscription_replay());
    listener.subscribe("orders.gamma")?;
    server.release();
    until(&mut server, &mut listener, |listener, _| {
        listener.epoch() == 1
    })?;
    assert_eq!(server.queries.len(), 2);
    assert_eq!(
        listener.hook().events.last().map(String::as_str),
        Some("connected:1234")
    );
    assert!(!listener.phase().is_subscription_replay());
    server.notify("orders.alpha", "one");
    server.notify("orders.alpha", "two");
    server.notify("orders.alpha", "three");
    until(&mut server, &mut listener, |listener, _| {
        listener.hook().frames.len() == 1
    })?;
    let wake = listener.timer_deadline().ok_or("continuation missing")?;
    assert_eq!(wake.duration, std::time::Duration::ZERO);
    // Another socket batch advances the idle deadline without consuming the queued wake.
    let (fd, _, _) = listener.poll_interest().ok_or("socket missing")?;
    listener.on_event(ListenerEvent::Socket {
        fd,
        readable: false,
        writable: false,
        error: false,
    })?;
    assert_eq!(listener.hook().frames.len(), 2);
    assert_eq!(
        listener
            .timer_deadline()
            .ok_or("queued wake missing")?
            .generation,
        wake.generation
    );
    listener.on_event(ListenerEvent::Timer {
        generation: wake.generation,
    })?;
    assert_eq!(listener.hook().frames.len(), 3);
    assert_eq!(listener.epoch(), 1);
    listener.on_event(ListenerEvent::Timer {
        generation: wake.generation,
    })?;
    assert_eq!(listener.hook().frames.len(), 3);
    listener.begin_shutdown();
    assert!(listener.phase().is_terminating());
    assert!(!listener.phase().has_continuation());
    Ok(())
}

#[test]
fn collision_routing_preserves_recycled_ids() -> Result<(), Box<dyn std::error::Error>> {
    let mut server = MockServer::new()?;
    let options = ListenerOptions {
        channel_capacity: 4,
        ..ListenerOptions::default()
    };
    let mut listener = PgListener::new(options, Hook::default())?;
    // Equal-length names share both edge words, forcing collision fallback to the trie.
    let names = [
        "prefix00.alpha.suffix00",
        "prefix00.bravo.suffix00",
        "prefix00.cafe.suffix00",
        "prefix00.a",
    ];
    let ids = names
        .map(|name| listener.subscribe(name))
        .into_iter()
        .collect::<pgnotify::Result<Vec<_>>>()?;
    listener.connect(&server.url()?)?;
    until(&mut server, &mut listener, |listener, _| {
        listener.epoch() == 1
    })?;
    for name in names {
        server.notify(name, "first");
    }
    until(&mut server, &mut listener, |listener, _| {
        listener.hook().frames.len() == 4
    })?;
    assert_eq!(
        listener
            .hook()
            .frames
            .iter()
            .map(|f| f.channel_id)
            .collect::<Vec<_>>(),
        ids
    );
    listener.unsubscribe(names[0])?;
    let recycled = listener.subscribe("prefix00.delta.suffix00")?;
    assert_eq!(recycled, ids[0]);
    until(&mut server, &mut listener, |listener, _| {
        listener.state() == ListenerState::Listening
    })?;
    for name in [names[1], "prefix00.delta.suffix00", names[0], names[2]] {
        server.notify(name, "second");
    }
    until(&mut server, &mut listener, |listener, _| {
        listener.metrics().notifications == 8
    })?;
    let frames = &listener.hook().frames;
    assert_eq!(frames.len(), 7);
    assert_eq!(frames[4].channel_id, ids[1]);
    assert_eq!(frames[5].channel_id, recycled);
    assert_eq!(frames[5].channel.as_str(), "prefix00.delta.suffix00");
    assert_eq!(frames[6].channel_id, ids[2]);
    listener.unsubscribe("prefix00.delta.suffix00")?;
    until(&mut server, &mut listener, |listener, _| {
        listener.state() == ListenerState::Listening
    })?;
    server.notify(names[1], "survivor");
    until(&mut server, &mut listener, |listener, _| {
        listener.hook().frames.len() == 8
    })?;
    assert_eq!(listener.hook().frames[7].channel_id, ids[1]);
    Ok(())
}

#[test]
fn callback_unwind_preserves_backoff_and_guard() -> Result<(), Box<dyn std::error::Error>> {
    struct PanicHook {
        panic_once: bool,
    }
    impl NotifyHook for PanicHook {
        fn on_notify(&mut self, _: NotificationRef<'_>) {}
        fn on_error(&mut self, _: PgNotifyError) {
            if self.panic_once {
                self.panic_once = false;
                panic!("callback fault");
            }
        }
    }
    let mut server = MockServer::new()?;
    let mut listener = PgListener::new(ListenerOptions::default(), PanicHook { panic_once: true })?;
    let guard = listener.drive_guard();
    listener.connect(&server.url()?)?;
    server.step()?;
    let connect_timer = listener.timer_deadline().ok_or("missing connect timer")?;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        listener.on_event(ListenerEvent::Timer {
            generation: connect_timer.generation,
        })
    }));
    assert!(result.is_err());
    assert_eq!(guard.check(), Ok(()));
    assert_eq!(listener.state(), ListenerState::BackoffWait);
    let retry = listener
        .timer_deadline()
        .ok_or("backoff deadline lost after unwind")?;
    assert_ne!(retry.generation, connect_timer.generation);
    assert_eq!(retry.duration, ListenerOptions::default().reconnect_initial);
    server.disconnect()?;
    listener.on_event(ListenerEvent::Timer {
        generation: retry.generation,
    })?;
    until(&mut server, &mut listener, |listener, _| {
        listener.epoch() == 1
    })?;
    assert_eq!(listener.state(), ListenerState::Listening);
    listener.begin_shutdown();
    Ok(())
}
