#[path = "support/cluster_peer.rs"]
mod support;
use pgnotify::{
    ChannelId, ListenerOptions, ListenerState, NotificationRef, NotifyHook, PgListener,
};
use support::{MockServer, pump, until};
#[derive(Default)]
struct Hook {
    panic_once: bool,
    items: Vec<(String, Option<ChannelId>, String)>,
}
impl NotifyHook for Hook {
    fn on_notify(&mut self, n: NotificationRef<'_>) {
        if self.panic_once {
            self.panic_once = false;
            panic!("notification callback");
        }
        self.items
            .push((n.channel.into(), n.channel_id, n.payload.into()));
    }
}
#[test]
fn queued_notice_does_not_alias_reuse() -> Result<(), Box<dyn std::error::Error>> {
    let mut server = MockServer::new()?;
    let mut listener = PgListener::new(
        ListenerOptions {
            channel_capacity: 1,
            notification_budget: 1,
            idle_ping_interval: None,
            ..Default::default()
        },
        Hook::default(),
    )?;
    let old = listener.subscribe("old")?;
    assert!(listener.subscribe("bad\0name").is_err());
    listener.connect(&server.url()?)?;
    until(&mut server, &mut listener, |l, _| {
        l.state() == ListenerState::Listening
    })?;
    server.notify("old", "one");
    server.notify("old", "two");
    until(&mut server, &mut listener, |l, _| l.hook().items.len() == 1)?;
    listener.unsubscribe("old")?;
    assert_eq!(listener.subscribe("new")?, old);
    until(&mut server, &mut listener, |l, _| {
        l.state() == ListenerState::Listening && l.hook().items.len() == 2
    })?;
    assert_eq!(listener.hook().items[0].1, Some(old));
    assert_eq!(listener.hook().items[1].1, None);
    server.notify("new", "three");
    until(&mut server, &mut listener, |l, _| l.hook().items.len() == 3)?;
    assert_eq!(listener.hook().items[2].1, Some(old));
    listener.begin_shutdown();
    Ok(())
}
#[test]
fn callback_unwind_keeps_buffered_wakeup() -> Result<(), Box<dyn std::error::Error>> {
    let mut server = MockServer::new()?;
    let mut listener = PgListener::new(
        ListenerOptions {
            idle_ping_interval: None,
            ..Default::default()
        },
        Hook {
            panic_once: true,
            ..Hook::default()
        },
    )?;
    listener.subscribe("queue")?;
    listener.connect(&server.url()?)?;
    until(&mut server, &mut listener, |l, _| {
        l.state() == ListenerState::Listening
    })?;
    server.notify("queue", "one");
    server.notify("queue", "two");
    let mut panicked = false;
    for _ in 0..1000 {
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pump(&mut server, &mut listener)
        }))
        .is_err()
        {
            panicked = true;
            break;
        }
    }
    assert!(panicked);
    assert_eq!(listener.state(), ListenerState::Listening);
    let has_wakeup = listener
        .timer_deadline()
        .is_some_and(|d| d.duration.is_zero());
    if !has_wakeup {
        for _ in 0..100 {
            pump(&mut server, &mut listener)?;
        }
        assert!(
            listener.hook().items.is_empty(),
            "no real wake reached the buffered notice"
        );
        let (fd, _, _) = listener.poll_interest().ok_or("socket")?;
        listener.on_event(pgnotify::ListenerEvent::Socket {
            fd,
            readable: false,
            writable: false,
            error: false,
        })?;
        assert_eq!(listener.hook().items[0].2, "two");
        eprintln!(
            "STRANDED: no timer/socket wake; synthetic event recovered libpq-buffered payload two"
        );
    }
    assert!(
        has_wakeup,
        "buffered notification needs a continuation after callback unwind"
    );

    until(&mut server, &mut listener, |l, _| {
        !l.hook().items.is_empty()
    })?;
    assert_eq!(listener.hook().items[0].2, "two");
    listener.begin_shutdown();
    Ok(())
}

#[test]
fn partial_drain_clears_deadline() -> Result<(), Box<dyn std::error::Error>> {
    let mut server = MockServer::new()?;
    let mut listener = PgListener::new(
        ListenerOptions {
            notification_budget: 4,
            idle_ping_interval: None,
            ..Default::default()
        },
        Hook::default(),
    )?;
    listener.subscribe("queue")?;
    listener.connect(&server.url()?)?;
    until(&mut server, &mut listener, |l, _| {
        l.state() == ListenerState::Listening
    })?;
    server.notify("queue", "one");
    until(&mut server, &mut listener, |l, _| l.hook().items.len() == 1)?;
    assert_eq!(listener.hook().items[0].2, "one");
    assert!(
        listener.timer_deadline().is_none(),
        "partial drain must clear the pre-armed continuation"
    );
    listener.begin_shutdown();
    Ok(())
}
#[test]
fn backoff_saturates_at_signed_timespec_limit() -> Result<(), Box<dyn std::error::Error>> {
    use std::time::Duration;
    let server = MockServer::new()?;
    let initial = Duration::from_secs(1u64 << 62);
    let maximum = Duration::from_secs(i64::MAX as u64);
    let mut listener = PgListener::new(
        ListenerOptions {
            reconnect_initial: initial,
            reconnect_max: maximum,
            idle_ping_interval: None,
            ..Default::default()
        },
        Hook::default(),
    )?;
    listener.connect(&server.url()?)?;
    for n in 0..64 {
        listener.drain_commands(1, [pgnotify::ListenCommand::Reconnect])?;
        let deadline = listener.timer_deadline().ok_or("backoff timer")?;
        assert_eq!(deadline.duration, if n == 0 { initial } else { maximum });
        assert_eq!(listener.state(), ListenerState::BackoffWait);
    }
    listener.begin_shutdown();
    for too_large in [Duration::MAX, Duration::from_secs(1u64 << 63)] {
        assert!(
            PgListener::new(
                ListenerOptions {
                    reconnect_max: too_large,
                    ..Default::default()
                },
                Hook::default()
            )
            .is_err()
        );
    }
    Ok(())
}
