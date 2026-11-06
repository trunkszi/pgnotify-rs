#![cfg(feature = "uring")]
#[path = "support/cluster_peer.rs"]
mod support;
use liburing::{Proactor, ProactorConfig, SubsystemId};
use pgnotify::{ListenerOptions, ListenerState, NotificationRef, NotifyHook, PgListener};
use std::{
    mem::MaybeUninit,
    time::{Duration, Instant},
};
use support::MockServer;
struct Hook;
impl NotifyHook for Hook {
    fn on_notify(&mut self, _: NotificationRef<'_>) {}
}
fn inode(fd: i32) -> Result<u64, Box<dyn std::error::Error>> {
    let mut stat = MaybeUninit::<libc::stat>::uninit();
    // SAFETY: fstat initializes stat on success; fd is a borrowed live listener socket.
    if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: successful fstat initialized every field read here.
    Ok(unsafe { stat.assume_init() }.st_ino)
}
fn port(server: &MockServer) -> Result<u16, Box<dyn std::error::Error>> {
    Ok(server
        .url()?
        .split('@')
        .nth(1)
        .ok_or("authority")?
        .split(':')
        .nth(1)
        .ok_or("port")?
        .split('/')
        .next()
        .ok_or("port value")?
        .parse()?)
}
#[test]
fn reset_host_fallback_rejects_fd_aba() -> Result<(), Box<dyn std::error::Error>> {
    let mut first = MockServer::new()?;
    let mut second = MockServer::new()?;
    let mut listener = PgListener::new(
        ListenerOptions {
            reconnect_initial: Duration::from_millis(1),
            reconnect_max: Duration::from_millis(2),
            idle_ping_interval: None,
            ..Default::default()
        },
        Hook,
    )?;
    listener.connect(&format!("host=127.0.0.1,127.0.0.1 port={},{} user=test dbname=test sslmode=disable target_session_attrs=read-write",port(&first)?,port(&second)?))?;
    let mut driver = pgnotify::proactor::ProactorPgListener::new(SubsystemId(77), listener)?;
    let mut ring = Proactor::new(ProactorConfig::default())?;
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut disconnected = false;
    let mut previous = None;
    let mut observed_replacement = false;
    while Instant::now() < deadline {
        first.step()?;
        second.step()?;
        if driver.listener.state() == ListenerState::Listening && !disconnected {
            first.read_only = true;
            first.disconnect()?;
            disconnected = true;
        }
        driver.flush(&mut ring)?;
        ring.wait_completions(1, Some(Duration::from_millis(2)), &mut driver)?;
        if disconnected && let Some((fd, generation, _)) = driver.listener.poll_interest() {
            let current_inode = inode(fd)?;
            if let Some((old_fd, old_generation, old_inode)) = previous
                && old_fd == fd
                && old_inode != current_inode
            {
                eprintln!(
                    "SAME-FD REPLACEMENT fd={fd} old_inode={old_inode} new_inode={current_inode} old_generation={old_generation} new_generation={generation} phase={:?}",
                    driver.listener.phase()
                );
                assert_ne!(
                    old_generation, generation,
                    "fd reuse must invalidate the duplicated socket lease"
                );
                observed_replacement = true;
            }
            previous = Some((fd, generation, current_inode));
        }
        if observed_replacement && driver.listener.state() == ListenerState::Listening {
            break;
        }
    }
    assert!(
        observed_replacement,
        "fixture must force same-fd libpq host fallback"
    );
    assert_eq!(driver.listener.state(), ListenerState::Listening);
    driver.shutdown(&mut ring)?;
    ring.shutdown()?;
    Ok(())
}
