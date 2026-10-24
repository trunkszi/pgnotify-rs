#![cfg(feature = "uring")]
use liburing::SubsystemId;
use pgnotify::proactor::ProactorPgListener;
use pgnotify::{NotifyHook, PgListener};

struct Hook;
impl NotifyHook for Hook {
    fn on_notify(&mut self, _: pgnotify::NotificationRef<'_>) {}
}

#[test]
fn empty_shutdown_leaves_the_ring_usable() -> Result<(), Box<dyn std::error::Error>> {
    let listener = PgListener::new(Default::default(), Hook)?;
    let mut driver = ProactorPgListener::new(SubsystemId::new_unchecked(1), listener)?;
    let mut ring = liburing::Proactor::builder().entries(8).build()?;
    driver.shutdown(&mut ring)?;
    assert_eq!(driver.inflight_count(), 0);
    assert!(!driver.has_pending_io());
    ring.alloc_sqe(liburing::OperationId::pack(
        SubsystemId::new_unchecked(2),
        liburing::OperationPayload(1),
    ))?;
    let mut seen = 0;
    ring.wait_completions(
        1,
        Some(std::time::Duration::from_secs(1)),
        &mut |_, _: &mut liburing::SubmissionContext<'_>| {
            seen += 1;
            Ok(())
        },
    )?;
    assert_eq!(seen, 1);
    ring.shutdown()?;
    Ok(())
}

use liburing::{
    Completion, CompletionDispatcher, OperationId, OperationPayload, Proactor, ProactorConfig,
    SubmissionContext,
};
use pgnotify::{ListenerOptions, ListenerState, NotificationRef};
use std::{
    cell::RefCell,
    io::{Read, Write},
    net::{Ipv4Addr, TcpListener, TcpStream},
    rc::Rc,
    time::{Duration, Instant},
};

struct Captured(Rc<RefCell<Vec<String>>>);
impl NotifyHook for Captured {
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

type Fixture = (
    MockPg,
    ProactorPgListener<Captured>,
    Rc<RefCell<Vec<String>>>,
);

fn configured() -> Result<Fixture, Box<dyn std::error::Error>> {
    let server = MockPg::new()?;
    let notifications = Rc::new(RefCell::new(Vec::new()));
    let options = ListenerOptions {
        idle_ping_interval: None,
        ..Default::default()
    };
    let mut listener = PgListener::new(options, Captured(Rc::clone(&notifications)))?;
    listener.subscribe("events")?;
    listener.connect(&server.url()?)?;
    Ok((
        server,
        ProactorPgListener::new(SubsystemId(1), listener)?,
        notifications,
    ))
}

#[test]
fn notification_and_peer_share_one_ring_and_reject_forged_completions()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut server, mut driver, notifications) = configured()?;
    let mut ring = Proactor::builder().entries(16).build()?;
    let peer = OperationId::pack(SubsystemId(2), OperationPayload(1));
    ring.alloc_sqe(peer)?;
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut peers = 0;
    let mut forged_checked = false;
    let mut sent = false;
    while notifications.borrow().is_empty() && Instant::now() < deadline {
        server.pump()?;
        if driver.listener.state() == ListenerState::Listening && !sent {
            server.notify("delivered")?;
            sent = true;
        }
        driver.flush(&mut ring)?;
        let mut dispatch = |completion: Completion, submissions: &mut SubmissionContext<'_>| {
            if completion.operation_id.subsystem_id() == SubsystemId(1) {
                if !forged_checked {
                    let before = driver.inflight_count();
                    assert!(
                        driver
                            .dispatch(
                                Completion {
                                    flags: completion.flags ^ 1,
                                    ..completion
                                },
                                submissions
                            )
                            .is_err()
                    );
                    assert_eq!(driver.inflight_count(), before);
                    forged_checked = true;
                }
                driver.dispatch(completion, submissions)
            } else {
                peers += 1;
                Ok(())
            }
        };
        ring.wait_completions(1, Some(Duration::from_millis(2)), &mut dispatch)?;
    }
    assert_eq!(&*notifications.borrow(), &["delivered"]);
    assert_eq!(peers, 1);
    assert!(forged_checked);
    driver.shutdown(&mut ring)?;
    assert_eq!(driver.inflight_count(), 0);
    ring.shutdown()?;
    Ok(())
}

#[test]
fn staged_and_submitted_shutdown_count_cancel_and_target_cqes_independently()
-> Result<(), Box<dyn std::error::Error>> {
    for submitted in [false, true] {
        let (_server, mut driver, _) = configured()?;
        let mut ring = Proactor::new(ProactorConfig {
            entries: 8,
            operation_capacity: 16,
            staged_capacity: 8,
            ..Default::default()
        })?;
        driver.flush(&mut ring)?;
        assert_eq!(driver.inflight_count(), 2);
        if submitted {
            ring.submit()?;
        }
        let peer = OperationId::pack(SubsystemId(2), OperationPayload(1));
        ring.alloc_sqe(peer)?;
        driver.begin_shutdown(&mut ring)?;
        assert_eq!(driver.inflight_count(), 4);
        let mut peers = 0;
        let mut targets = 0;
        let deadline = Instant::now() + Duration::from_secs(3);
        while (driver.has_pending_io() || peers == 0) && Instant::now() < deadline {
            driver.flush(&mut ring)?;
            let mut dispatch = |completion: Completion, submissions: &mut SubmissionContext<'_>| {
                if completion.operation_id.subsystem_id() == SubsystemId(1) {
                    let before = driver.inflight_count();
                    driver.dispatch(completion, submissions)?;
                    assert_eq!(driver.inflight_count() + 1, before);
                    targets += 1;
                } else {
                    peers += 1;
                }
                Ok(())
            };
            ring.wait_completions(1, Some(Duration::from_millis(5)), &mut dispatch)?;
        }
        assert!(!driver.has_pending_io());
        assert_eq!(targets, 4);
        assert_eq!(peers, 1);
        assert_eq!(ring.operation_count(), 0);
        ring.shutdown()?;
    }
    Ok(())
}

#[test]
fn zero_budget_retains_pending_io_until_ring_credit_is_released()
-> Result<(), Box<dyn std::error::Error>> {
    let (_server, mut driver, _) = configured()?;
    let mut ring = Proactor::new(ProactorConfig {
        entries: 2,
        operation_capacity: 3,
        staged_capacity: 2,
        ..Default::default()
    })?;
    driver.flush(&mut ring)?;
    ring.submit()?;
    ring.alloc_sqe(OperationId::pack(SubsystemId(2), OperationPayload(1)))?;
    assert!(driver.shutdown_within(&mut ring, Duration::ZERO).is_err());
    assert!(driver.has_pending_io());
    let deadline = Instant::now() + Duration::from_secs(3);
    while driver.has_pending_io() && Instant::now() < deadline {
        driver.flush(&mut ring)?;
        ring.wait_completions(
            1,
            Some(Duration::from_millis(5)),
            &mut |completion: Completion, submissions: &mut SubmissionContext<'_>| {
                if completion.operation_id.subsystem_id() == SubsystemId(1) {
                    driver.dispatch(completion, submissions)
                } else {
                    Ok(())
                }
            },
        )?;
    }
    assert!(!driver.has_pending_io());
    assert_eq!(driver.inflight_count(), 0);
    ring.shutdown()?;
    Ok(())
}

#[test]
fn reserved_subsystem_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let listener = PgListener::new(Default::default(), Hook)?;
    assert!(ProactorPgListener::new(SubsystemId::INTERNAL, listener).is_err());
    Ok(())
}
