//! Real-ring deadline, socket-generation, and early-drop regressions.
#![cfg(feature = "uring")]
use liburing::{Completion, Proactor, SubsystemId};
use pgnotify::proactor::ProactorPgListener;
use pgnotify::{ListenerOptions, ListenerState, NotificationRef, NotifyHook, PgListener};
use std::{
    cell::RefCell,
    io::{Read, Write},
    net::{Ipv4Addr, Shutdown, TcpListener, TcpStream},
    rc::Rc,
    time::{Duration, Instant},
};

struct Hook(Rc<RefCell<Vec<String>>>);
impl NotifyHook for Hook {
    fn on_notify(&mut self, notification: NotificationRef<'_>) {
        self.0.borrow_mut().push(notification.payload.to_owned());
    }
}

struct Peer {
    stream: TcpStream,
    input: Vec<u8>,
    output: Vec<u8>,
    offset: usize,
    startup: bool,
}

struct MockPg {
    listener: TcpListener,
    peer: Option<Peer>,
    queries: Vec<String>,
}

impl MockPg {
    fn new() -> std::io::Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            listener,
            peer: None,
            queries: Vec::new(),
        })
    }
    fn url(&self) -> std::io::Result<String> {
        Ok(format!(
            "postgresql://test@127.0.0.1:{}/test?sslmode=disable&gssencmode=disable",
            self.listener.local_addr()?.port()
        ))
    }
    fn packet(output: &mut Vec<u8>, tag: u8, body: &[u8]) {
        output.push(tag);
        output.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        output.extend_from_slice(body);
    }
    fn notify(&mut self, payload: &str) -> Result<(), Box<dyn std::error::Error>> {
        let peer = self.peer.as_mut().ok_or("missing PG peer")?;
        let mut body = 1234_i32.to_be_bytes().to_vec();
        body.extend_from_slice(b"events\0");
        body.extend_from_slice(payload.as_bytes());
        body.push(0);
        Self::packet(&mut peer.output, b'A', &body);
        Ok(())
    }
    fn pump(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.peer.is_none() {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(true)?;
                    self.peer = Some(Peer {
                        stream,
                        input: Vec::new(),
                        output: Vec::new(),
                        offset: 0,
                        startup: true,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) => return Err(error.into()),
            }
        }
        let peer = self.peer.as_mut().ok_or("PG peer missing after accept")?;
        let mut input = [0; 8192];
        for _ in 0..16 {
            match peer.stream.read(&mut input) {
                Ok(0) => break,
                Ok(count) => peer.input.extend_from_slice(&input[..count]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => return Err(error.into()),
            }
        }
        for _ in 0..16 {
            let prefix = usize::from(!peer.startup);
            if peer.input.len() < prefix + 4 {
                break;
            }
            let length = u32::from_be_bytes(peer.input[prefix..prefix + 4].try_into()?) as usize;
            if !(4..=65536).contains(&length) {
                return Err("invalid PG frame size".into());
            }
            if peer.input.len() < prefix + length {
                break;
            }
            let frame: Vec<_> = peer.input.drain(..prefix + length).collect();
            if peer.startup {
                if length == 8 {
                    peer.output.push(b'N');
                    continue;
                }
                peer.startup = false;
                Self::packet(&mut peer.output, b'R', &0_u32.to_be_bytes());
                Self::packet(&mut peer.output, b'S', b"server_version\x0017.0\0");
                Self::packet(&mut peer.output, b'S', b"client_encoding\0UTF8\0");
                Self::packet(&mut peer.output, b'K', &[0, 0, 4, 210, 0, 0, 0, 1]);
                Self::packet(&mut peer.output, b'Z', b"I");
            } else if frame[0] == b'Q' {
                let query = std::str::from_utf8(&frame[5..frame.len() - 1])?.to_owned();
                let tag: &[u8] = if query.trim_start().starts_with("LISTEN") {
                    b"LISTEN\0"
                } else {
                    b"SELECT 1\0"
                };
                self.queries.push(query);
                Self::packet(&mut peer.output, b'C', tag);
                Self::packet(&mut peer.output, b'Z', b"I");
            }
        }
        while peer.offset < peer.output.len() {
            match peer.stream.write(&peer.output[peer.offset..]) {
                Ok(0) => return Err("PG peer write returned zero".into()),
                Ok(count) => peer.offset += count,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => return Err(error.into()),
            }
        }
        if peer.offset == peer.output.len() {
            peer.output.clear();
            peer.offset = 0;
        }
        Ok(())
    }
}

#[test]
fn timeout_sqe_drives_backoff_and_replaces_the_socket_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let server = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let options = ListenerOptions {
        connect_timeout: Duration::from_millis(5),
        reconnect_initial: Duration::from_millis(2),
        reconnect_max: Duration::from_millis(10),
        ..Default::default()
    };
    let mut listener = PgListener::new(options, Hook(Rc::new(RefCell::new(Vec::new()))))?;
    listener.connect(&format!(
        "postgresql://test@127.0.0.1:{}/test?sslmode=disable&gssencmode=disable",
        server.local_addr()?.port()
    ))?;
    let initial = listener.poll_interest().ok_or("initial socket")?;
    let mut driver = ProactorPgListener::new(SubsystemId(1), listener)?;
    let mut ring = Proactor::builder().entries(16).build()?;
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut backoff_seen = false;
    let mut replacement = false;
    while !replacement && Instant::now() < deadline {
        driver.flush(&mut ring)?;
        ring.wait_completions(1, Some(Duration::from_millis(5)), &mut driver)?;
        backoff_seen |= driver.listener.state() == ListenerState::BackoffWait;
        replacement = driver
            .listener
            .poll_interest()
            .is_some_and(|(_, generation, _)| generation != initial.1);
    }
    assert!(backoff_seen);
    assert!(replacement);
    driver.shutdown(&mut ring)?;
    assert_eq!(driver.inflight_count(), 0);
    ring.shutdown()?;
    Ok(())
}

#[test]
fn reconnect_switches_fd_generation_replays_listen_and_delivers_again()
-> Result<(), Box<dyn std::error::Error>> {
    let mut server = MockPg::new()?;
    let messages = Rc::new(RefCell::new(Vec::new()));
    let options = ListenerOptions {
        idle_ping_interval: None,
        reconnect_initial: Duration::from_millis(2),
        reconnect_max: Duration::from_millis(10),
        ..Default::default()
    };
    let mut listener = PgListener::new(options, Hook(Rc::clone(&messages)))?;
    listener.subscribe("events")?;
    listener.connect(&server.url()?)?;
    let mut driver = ProactorPgListener::new(SubsystemId(1), listener)?;
    let mut ring = Proactor::builder().entries(16).build()?;
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut original = None;
    let mut closed = false;
    let mut notified = false;
    while messages.borrow().is_empty() && Instant::now() < deadline {
        server.pump()?;
        if driver.listener.state() == ListenerState::Listening {
            if driver.listener.epoch() == 1 && !closed {
                original = driver.listener.poll_interest();
                let peer = server.peer.take().ok_or("peer before disconnect")?;
                peer.stream.shutdown(Shutdown::Both)?;
                closed = true;
            } else if driver.listener.epoch() > 1 && !notified {
                let old = original.ok_or("original socket identity")?;
                let new = driver
                    .listener
                    .poll_interest()
                    .ok_or("replacement socket identity")?;
                assert_ne!((old.0, old.1), (new.0, new.1));
                assert!(!driver.listener.owns_socket(old.0, old.1));
                assert!(
                    server
                        .queries
                        .iter()
                        .filter(|query| query.contains("LISTEN"))
                        .count()
                        >= 2
                );
                server.notify("after reconnect")?;
                notified = true;
            }
        }
        driver.flush(&mut ring)?;
        ring.wait_completions(1, Some(Duration::from_millis(2)), &mut driver)?;
    }
    assert!(closed && notified);
    assert_eq!(&*messages.borrow(), &["after reconnect"]);
    assert!(driver.listener.epoch() >= 2);
    driver.shutdown(&mut ring)?;
    ring.shutdown()?;
    Ok(())
}

#[test]
fn early_drop_preserves_staged_timeout_and_ring_owned_duplicate()
-> Result<(), Box<dyn std::error::Error>> {
    let server = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let options = ListenerOptions {
        connect_timeout: Duration::from_millis(5),
        ..Default::default()
    };
    let mut listener = PgListener::new(options, Hook(Rc::new(RefCell::new(Vec::new()))))?;
    listener.connect(&format!(
        "postgresql://test@127.0.0.1:{}/test?sslmode=disable&gssencmode=disable",
        server.local_addr()?.port()
    ))?;
    let mut driver = ProactorPgListener::new(SubsystemId(1), listener)?;
    let mut ring = Proactor::builder().entries(8).build()?;
    driver.flush(&mut ring)?;
    assert_eq!(driver.inflight_count(), 2);
    drop(driver);
    let mut expired = false;
    let deadline = Instant::now() + Duration::from_secs(1);
    while !expired && Instant::now() < deadline {
        ring.wait_completions(
            1,
            Some(Duration::from_millis(10)),
            &mut |completion: Completion, _: &mut liburing::SubmissionContext<'_>| {
                expired |= completion.result == -libc::ETIME;
                Ok(())
            },
        )?;
    }
    assert!(expired);
    ring.shutdown()?;
    Ok(())
}
