#[path = "support/cluster_peer.rs"]
mod support;
use pgnotify::{
    ChannelId, ListenerOptions, ListenerState, NotificationRef, NotifyHook, PgListener,
};
use support::{MockServer, pump};

#[derive(Default)]
struct Hook {
    panic_once: bool,
    subscribed: usize,
    connected: usize,
    unsubscribed: usize,
    panic_connected: bool,
    panic_unsubscribed: bool,
}
impl NotifyHook for Hook {
    fn on_notify(&mut self, _: NotificationRef<'_>) {}
    fn on_subscribed(&mut self, _: ChannelId, _: &str) {
        self.subscribed += 1;
        if self.panic_once {
            self.panic_once = false;
            panic!("subscription callback");
        }
    }
    fn on_connected(&mut self, _: i32) {
        self.connected += 1;
        if self.panic_connected {
            self.panic_connected = false;
            panic!("connected callback");
        }
    }
    fn on_unsubscribed(&mut self, _: ChannelId, _: &str) {
        self.unsubscribed += 1;
        if self.panic_unsubscribed {
            self.panic_unsubscribed = false;
            panic!("unsubscribe callback");
        }
    }
}
#[test]
fn subscription_unwind_resumes_replay() -> Result<(), Box<dyn std::error::Error>> {
    let mut server = MockServer::new()?;
    let mut listener = PgListener::new(
        ListenerOptions {
            idle_ping_interval: None,
            ..Default::default()
        },
        Hook {
            panic_once: true,
            ..Default::default()
        },
    )?;
    listener.subscribe("orders")?;
    listener.connect(&server.url()?)?;
    let mut panicked = false;
    for _ in 0..100_000 {
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pump(&mut server, &mut listener)
        }));
        match outcome {
            Err(_) => {
                panicked = true;
                break;
            }
            Ok(result) => result?,
        }
    }
    assert!(panicked, "subscription callback was never reached");
    assert_eq!(listener.drive_guard().check(), Ok(()));
    // Either all internal bookkeeping already committed before the callback,
    // or a retained zero-delay continuation must finish it without new input.
    assert!(
        listener.epoch() == 1
            || listener
                .timer_deadline()
                .is_some_and(|d| d.duration.is_zero()),
        "caught subscription panic lost replay progress and its wakeup"
    );
    for _ in 0..100_000 {
        if listener.epoch() == 1
            && listener.hook().connected == 1
            && listener.timer_deadline().is_none()
        {
            break;
        }
        pump(&mut server, &mut listener)?;
    }
    assert_eq!(listener.state(), ListenerState::Listening);
    assert_eq!(listener.epoch(), 1);
    assert_eq!(
        listener.hook().subscribed,
        1,
        "ack callback must not be redelivered"
    );
    assert_eq!(
        listener.hook().connected,
        1,
        "readiness callback must follow subscription callback"
    );
    assert!(listener.timer_deadline().is_none());
    listener.begin_shutdown();
    Ok(())
}

#[test]
fn ready_unwind_retains_idle_deadline() -> Result<(), Box<dyn std::error::Error>> {
    let mut server = MockServer::new()?;
    let idle = std::time::Duration::from_secs(15);
    let mut listener = PgListener::new(
        ListenerOptions {
            idle_ping_interval: Some(idle),
            ..Default::default()
        },
        Hook {
            panic_connected: true,
            ..Default::default()
        },
    )?;
    listener.connect(&server.url()?)?;
    let mut panicked = false;
    for _ in 0..100_000 {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pump(&mut server, &mut listener)
        })) {
            Err(_) => {
                panicked = true;
                break;
            }
            Ok(result) => result?,
        }
    }
    assert!(panicked);
    assert_eq!(listener.epoch(), 1);
    assert_eq!(listener.hook().connected, 1);
    assert!(
        listener
            .timer_deadline()
            .is_some_and(|timer| timer.duration.is_zero())
    );
    pump(&mut server, &mut listener)?;
    assert_eq!(
        listener.timer_deadline().map(|timer| timer.duration),
        Some(idle)
    );
    listener.begin_shutdown();
    Ok(())
}

#[test]
fn unsubscribe_unwind_commits_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    let mut server = MockServer::new()?;
    let mut listener = PgListener::new(
        ListenerOptions {
            idle_ping_interval: None,
            ..Default::default()
        },
        Hook::default(),
    )?;
    listener.subscribe("orders")?;
    listener.connect(&server.url()?)?;
    support::until(&mut server, &mut listener, |listener, _| {
        listener.epoch() == 1 && listener.timer_deadline().is_none()
    })?;
    listener.hook_mut().panic_unsubscribed = true;
    listener.unsubscribe("orders")?;
    let mut panicked = false;
    for _ in 0..100_000 {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pump(&mut server, &mut listener)
        })) {
            Err(_) => {
                panicked = true;
                break;
            }
            Ok(result) => result?,
        }
    }
    assert!(panicked);
    assert!(
        listener
            .timer_deadline()
            .is_some_and(|timer| timer.duration.is_zero())
    );
    support::until(&mut server, &mut listener, |listener, _| {
        listener.state() == ListenerState::Listening && listener.timer_deadline().is_none()
    })?;
    assert_eq!(listener.hook().unsubscribed, 1);
    listener.subscribe("orders")?;
    support::until(&mut server, &mut listener, |listener, _| {
        listener.state() == ListenerState::Listening && listener.timer_deadline().is_none()
    })?;
    assert_eq!(listener.hook().subscribed, 2);
    assert_eq!(listener.hook().unsubscribed, 1);
    listener.begin_shutdown();
    Ok(())
}
