#![cfg(feature = "uring")]
use liburing::{Proactor, SubsystemId};
use pgnotify::{ListenerOptions, NotificationRef, NotifyHook, PgListener, ProactorPgListener};
use std::net::{Ipv4Addr, TcpListener};

struct Hook;
impl NotifyHook for Hook {
    fn on_notify(&mut self, _: NotificationRef<'_>) {}
}

#[test]
fn driver_rejects_a_different_ring() -> Result<(), Box<dyn std::error::Error>> {
    let server = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let mut listener = PgListener::new(ListenerOptions::default(), Hook)?;
    listener.connect(&format!(
        "host=127.0.0.1 port={} user=test dbname=test sslmode=disable",
        server.local_addr()?.port()
    ))?;
    let mut owner = Proactor::builder().entries(8).build()?;
    let mut other = Proactor::builder().entries(8).build()?;
    let mut driver = ProactorPgListener::new(SubsystemId(1), listener)?;
    driver.flush(&mut owner)?;
    let pending = driver.inflight_count();
    let phase = driver.listener.phase();
    let timer = driver.listener.timer_deadline();
    assert!(driver.flush(&mut other).is_err());
    assert!(driver.begin_shutdown(&mut other).is_err());
    assert_eq!(driver.inflight_count(), pending);
    assert_eq!(driver.listener.phase(), phase);
    assert_eq!(driver.listener.timer_deadline(), timer);
    assert_eq!(other.operation_count(), 0);
    driver.shutdown(&mut owner)?;
    assert!(!driver.has_pending_io());
    assert_eq!(owner.operation_count(), 0);
    owner.shutdown()?;
    other.shutdown()?;
    Ok(())
}
