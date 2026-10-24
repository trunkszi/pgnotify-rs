use pgnotify::{ListenerEvent, NotifyHook, PgListener};
use std::{
    io::{self, Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
};

pub(crate) struct MockServer {
    listener: TcpListener,
    peer: Option<TcpStream>,
    input: Vec<u8>,
    output: Vec<u8>,
    startup: bool,
    pub(crate) queries: Vec<String>,
    pub(crate) hold_ack: bool,
    pub(crate) stall_ping: bool,
    pending: Vec<Vec<u8>>,
    pub(crate) sessions: usize,
}
impl MockServer {
    pub(crate) fn new() -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            listener,
            peer: None,
            input: Vec::new(),
            output: Vec::new(),
            startup: true,
            queries: Vec::new(),
            hold_ack: false,
            stall_ping: false,
            pending: Vec::new(),
            sessions: 0,
        })
    }
    pub(crate) fn url(&self) -> io::Result<String> {
        Ok(format!(
            "postgresql://test@127.0.0.1:{}/test?sslmode=disable",
            self.listener.local_addr()?.port()
        ))
    }
    pub(crate) fn disconnect(&mut self) -> io::Result<()> {
        if let Some(peer) = self.peer.take() {
            peer.shutdown(Shutdown::Both)?;
        }
        self.input.clear();
        self.output.clear();
        self.pending.clear();
        self.startup = true;
        Ok(())
    }
    pub(crate) fn notify(&mut self, channel: &str, payload: &str) {
        let mut body = 4321_i32.to_be_bytes().to_vec();
        body.extend_from_slice(channel.as_bytes());
        body.push(0);
        body.extend_from_slice(payload.as_bytes());
        body.push(0);
        self.output.extend(packet(b'A', &body));
    }
    pub(crate) fn release(&mut self) {
        self.hold_ack = false;
        for response in self.pending.drain(..) {
            self.output.extend(response);
        }
    }
    pub(crate) fn step(&mut self) -> io::Result<()> {
        if self.peer.is_none() {
            match self.listener.accept() {
                Ok((peer, _)) => {
                    peer.set_nonblocking(true)?;
                    peer.set_nodelay(true)?;
                    self.peer = Some(peer);
                    self.sessions += 1;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) => return Err(e),
            }
        }
        if let Some(peer) = self.peer.as_mut() {
            let mut data = [0; 4096];
            loop {
                match peer.read(&mut data) {
                    Ok(0) => break,
                    Ok(n) => self.input.extend_from_slice(&data[..n]),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) => return Err(e),
                }
            }
        }
        self.parse()?;
        if let Some(peer) = self.peer.as_mut() {
            while !self.output.is_empty() {
                match peer.write(&self.output) {
                    Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                    Ok(n) => {
                        self.output.drain(..n);
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(())
    }
    fn parse(&mut self) -> io::Result<()> {
        loop {
            let offset = usize::from(!self.startup);
            if self.input.len() < offset + 4 {
                return Ok(());
            }
            let mut length = [0; 4];
            length.copy_from_slice(&self.input[offset..offset + 4]);
            let size = u32::from_be_bytes(length) as usize + offset;
            if size < offset + 4 || size > 1024 * 1024 {
                return Err(io::ErrorKind::InvalidData.into());
            }
            if self.input.len() < size {
                return Ok(());
            }
            let message: Vec<_> = self.input.drain(..size).collect();
            if self.startup {
                if size == 8 {
                    self.output.push(b'N');
                    continue;
                }
                self.startup = false;
                self.output.extend(packet(b'R', &0_u32.to_be_bytes()));
                self.output.extend(packet(b'S', b"client_encoding\0UTF8\0"));
                self.output
                    .extend(packet(b'S', b"server_version\x0018.6\0"));
                let mut key = 1234_i32.to_be_bytes().to_vec();
                key.extend(5678_i32.to_be_bytes());
                self.output.extend(packet(b'K', &key));
                self.output.extend(packet(b'Z', b"I"));
            } else if message[0] == b'Q' {
                let sql = core::str::from_utf8(&message[5..size - 1])
                    .map_err(|_| io::ErrorKind::InvalidData)?
                    .to_owned();
                let mut reply = Vec::new();
                for statement in sql.split(';').filter(|s| !s.is_empty()) {
                    reply.extend(packet(
                        b'C',
                        if statement == "SELECT 1" {
                            b"SELECT 1\0"
                        } else if statement.starts_with("UNLISTEN") {
                            b"UNLISTEN\0"
                        } else {
                            b"LISTEN\0"
                        },
                    ));
                }
                reply.extend(packet(b'Z', b"I"));
                let stalled = self.stall_ping && sql == "SELECT 1;";
                self.queries.push(sql);
                if !stalled {
                    if self.hold_ack {
                        self.pending.push(reply);
                    } else {
                        self.output.extend(reply);
                    }
                }
            }
        }
    }
}
fn packet(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut bytes = vec![kind];
    bytes.extend(((payload.len() + 4) as u32).to_be_bytes());
    bytes.extend(payload);
    bytes
}

pub(crate) fn pump<H: NotifyHook>(
    server: &mut MockServer,
    listener: &mut PgListener<H>,
) -> Result<(), Box<dyn std::error::Error>> {
    server.step()?;
    if let Some(timer) = listener
        .timer_deadline()
        .filter(|timer| timer.duration.is_zero())
    {
        listener.on_event(ListenerEvent::Timer {
            generation: timer.generation,
        })?;
    }
    if let Some((fd, _, interest)) = listener.poll_interest() {
        let mut pollfd = libc::pollfd {
            fd,
            events: if interest.readable { libc::POLLIN } else { 0 }
                | if interest.writable { libc::POLLOUT } else { 0 },
            revents: 0,
        };
        // SAFETY: pollfd is a live one-element pollfd array; timeout zero never blocks.
        let result = unsafe { libc::poll(&mut pollfd, 1, 0) };
        if result < 0 {
            return Err(io::Error::last_os_error().into());
        }
        if result > 0 {
            listener.on_event(ListenerEvent::Socket {
                fd,
                readable: pollfd.revents & libc::POLLIN != 0,
                writable: pollfd.revents & libc::POLLOUT != 0,
                error: pollfd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0,
            })?;
        }
    }
    Ok(())
}

pub(crate) fn until<H: NotifyHook>(
    server: &mut MockServer,
    listener: &mut PgListener<H>,
    done: impl Fn(&PgListener<H>, &MockServer) -> bool,
) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..100_000 {
        if done(listener, server) {
            return Ok(());
        }
        pump(server, listener)?;
    }
    Err(format!(
        "mock failed to reach target; state={:?}, queries={:?}",
        listener.state(),
        server.queries
    )
    .into())
}
