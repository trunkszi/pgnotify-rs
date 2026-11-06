//! Drive LISTEN/NOTIFY on a caller-owned ring without an executor runtime.
use liburing::{Proactor, SubsystemId};
use pgnotify::proactor::ProactorPgListener;
use pgnotify::{NotificationRef, NotifyHook, PgListener};
use std::{
    error::Error,
    time::{Duration, Instant},
};

struct PrintNotifications;
impl NotifyHook for PrintNotifications {
    fn on_notify(&mut self, notification: NotificationRef<'_>) {
        println!("{}: {}", notification.channel, notification.payload);
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let url = std::env::var("DATABASE_URL")?;
    let mut listener = PgListener::new(Default::default(), PrintNotifications)?;
    listener.subscribe("events")?;
    listener.connect(&url)?;
    let mut driver = ProactorPgListener::new(SubsystemId(1), listener)?;
    let mut ring = Proactor::builder().entries(64).build()?;
    let deadline = Instant::now() + Duration::from_secs(30);
    let outcome = (|| -> Result<(), Box<dyn Error>> {
        while Instant::now() < deadline {
            driver.flush(&mut ring)?;
            ring.wait_completions(1, Some(Duration::from_millis(100)), &mut driver)?;
        }
        Ok(())
    })();
    driver.shutdown(&mut ring)?;
    ring.shutdown()?;
    outcome
}
