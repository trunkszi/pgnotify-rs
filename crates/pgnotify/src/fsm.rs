//! Deterministic, caller-driven listener lifecycle and subscription acknowledgement.
use crate::{
    channel_table::ChannelTable,
    error::{ListenerState, PgNotifyError, Result},
    hook::NotifyHook,
    metrics::PgNotifyMetricsSnapshot,
    options::ListenerOptions,
    raw_conn::{QueryProgress, RawPgConn},
    types::{ChannelId, FixedString, ListenCommand, ListenerEvent, SocketInterest, TimerDeadline},
};
use std::{cell::Cell, ffi::CString, rc::Rc, time::Duration};

/// Callback-safe admission check before attempting to borrow and drive the listener again.
/// Cloning shares the owner-thread flag and never grants access to the connection or hook.
#[derive(Clone)]
pub struct DriveGuard {
    active: Rc<Cell<bool>>,
}
impl DriveGuard {
    /// # Errors
    /// Returns ReentrantDriveForbidden while a drive or failure callback is active.
    pub fn check(&self) -> Result<()> {
        if self.active.get() {
            Err(PgNotifyError::ReentrantDriveForbidden)
        } else {
            Ok(())
        }
    }
}

pub struct PgListener<H: NotifyHook> {
    options: ListenerOptions,
    hook: H,
    metrics: PgNotifyMetricsSnapshot,
    channels: ChannelTable,
    raw: Option<RawPgConn>,
    state: ListenerState,
    epoch: u64,
    socket_generation: u64,
    timer_generation: u64,
    timer: Option<TimerDeadline>,
    backoff: Duration,
    active: Vec<Option<FixedString<64>>>,
    sent: Vec<Option<FixedString<64>>>,
    driving: DriveGuard,
    replay: bool,
    continuation: Option<u64>,
}
impl<H: NotifyHook> PgListener<H> {
    /// # Errors
    /// Rejects zero budgets, invalid capacities, or invalid deadline/keepalive settings.
    pub fn new(options: ListenerOptions, hook: H) -> Result<Self> {
        options.validate()?;
        let channels = ChannelTable::with_capacity(options.channel_capacity)?;
        let count = options.channel_capacity;
        let backoff = options.reconnect_initial;
        Ok(Self {
            options,
            hook,
            channels,
            metrics: PgNotifyMetricsSnapshot::default(),
            raw: None,
            state: ListenerState::Disconnected,
            epoch: 0,
            socket_generation: 0,
            timer_generation: 0,
            timer: None,
            backoff,
            active: vec![None; count],
            sent: vec![None; count],
            driving: DriveGuard {
                active: Rc::new(Cell::new(false)),
            },
            replay: false,
            continuation: None,
        })
    }
    /// # Errors
    /// Rejects a second connection, invalid URLs or libpq initialization failures.
    pub fn connect(&mut self, database_url: &str) -> Result<()> {
        self.ensure_mutable()?;
        if self.state != ListenerState::Disconnected {
            return Err(PgNotifyError::InvalidConfig {
                reason: "already connected",
            });
        }
        self.raw = Some(RawPgConn::connect(database_url, &self.options)?);
        self.socket_generation += 1;
        self.state = ListenerState::ConnectingWriting;
        self.arm(self.options.connect_timeout);
        self.poll_connect(false).or_else(|error| {
            self.fail(error);
            Ok(())
        })
    }
    pub const fn state(&self) -> ListenerState {
        self.state
    }
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }
    pub fn hook(&self) -> &H {
        &self.hook
    }
    pub fn hook_mut(&mut self) -> &mut H {
        &mut self.hook
    }
    /// Returns the same admission guard used by drive methods, safe to retain inside callbacks.
    pub fn drive_guard(&self) -> DriveGuard {
        self.driving.clone()
    }
    pub fn metrics(&self) -> &PgNotifyMetricsSnapshot {
        &self.metrics
    }
    /// # Errors
    /// Rejects invalid names, full tables, or a terminating listener.
    pub fn subscribe(&mut self, channel: &str) -> Result<ChannelId> {
        self.ensure_mutable()?;
        if let Some(id) = self.channels.lookup(channel) {
            return Ok(id);
        }
        let id = self.channels.subscribe(channel)?;
        if self.state == ListenerState::Listening
            && let Err(error) = self.sync_subscriptions()
        {
            self.fail(error);
            return Err(error);
        }
        Ok(id)
    }
    /// # Errors
    /// Rejects unknown channels or a terminating listener.
    pub fn unsubscribe(&mut self, channel: &str) -> Result<()> {
        self.ensure_mutable()?;
        let id = self
            .channels
            .lookup(channel)
            .ok_or(PgNotifyError::InvalidConfig {
                reason: "unknown channel",
            })?;
        self.channels.remove(id)?;
        if self.state == ListenerState::Listening
            && let Err(error) = self.sync_subscriptions()
        {
            self.fail(error);
            return Err(error);
        }
        Ok(())
    }
    /// # Errors
    /// Returns command validation errors; no more than budget items are consumed.
    pub fn drain_commands<I>(&mut self, budget: usize, commands: I) -> Result<usize>
    where
        I: IntoIterator<Item = ListenCommand>,
    {
        self.ensure_mutable()?;
        let mut count = 0;
        for command in commands.into_iter().take(budget) {
            match command {
                ListenCommand::Subscribe {
                    channel_id,
                    channel,
                } => {
                    self.channels.register_at(channel, channel_id)?;
                    if self.state == ListenerState::Listening && self.desired_changed() {
                        self.sync_subscriptions()?;
                    }
                }
                ListenCommand::Unsubscribe { channel_id } => {
                    let name = *self
                        .channels
                        .channel(channel_id)
                        .ok_or(PgNotifyError::UnknownChannelId(channel_id))?;
                    self.unsubscribe(name.as_str())?;
                }
                ListenCommand::UnsubscribeByName { channel } => {
                    self.unsubscribe(channel.as_str())?
                }
                ListenCommand::Ping => {
                    if self.state == ListenerState::Listening {
                        self.start_ping()?;
                    }
                }
                ListenCommand::Reconnect => {
                    if self.raw.is_none() {
                        return Err(PgNotifyError::InvalidConfig {
                            reason: "not connected",
                        });
                    }
                    self.fail(PgNotifyError::ConnectionFailed {
                        phase: "requested reconnect",
                    });
                }
                ListenCommand::Shutdown => self.begin_shutdown(),
            }
            count += 1;
            if self.is_terminated() {
                break;
            }
        }
        Ok(count)
    }
    pub fn poll_interest(&self) -> Option<(i32, u64, SocketInterest)> {
        let interest = match self.state {
            ListenerState::ConnectingReading | ListenerState::ResettingReading => SocketInterest {
                readable: true,
                writable: false,
            },
            ListenerState::ConnectingWriting | ListenerState::ResettingWriting => SocketInterest {
                readable: false,
                writable: true,
            },
            ListenerState::FlushingSubscriptions | ListenerState::IdlePingFlushing => {
                SocketInterest {
                    readable: true,
                    writable: true,
                }
            }
            ListenerState::AwaitingCommandAck
            | ListenerState::Listening
            | ListenerState::IdlePingAwaiting => SocketInterest {
                readable: true,
                writable: false,
            },
            ListenerState::Disconnected
            | ListenerState::BackoffWait
            | ListenerState::ShuttingDown
            | ListenerState::Terminated => return None,
        };
        self.raw
            .as_ref()
            .map(RawPgConn::socket)
            .filter(|fd| *fd >= 0)
            .map(|fd| (fd, self.socket_generation, interest))
    }
    pub fn owns_socket(&self, fd: i32, generation: u64) -> bool {
        generation == self.socket_generation
            && self
                .poll_interest()
                .is_some_and(|(current, _, _)| current == fd)
    }
    pub const fn timer_deadline(&self) -> Option<TimerDeadline> {
        match self.continuation {
            Some(generation) => Some(TimerDeadline {
                generation,
                duration: Duration::ZERO,
            }),
            None => self.timer,
        }
    }
    /// # Errors
    /// Rejects reentrant drive and events on terminated listeners; transport faults initiate backoff.
    /// A timer event represents expiry, not a clock tick. Stale generations are ignored.
    pub fn on_event(&mut self, event: ListenerEvent) -> Result<usize> {
        self.ensure_mutable()?;
        self.driving.active.set(true);
        let result = self.drive(event);
        let outcome = match result {
            Ok(count) => Ok(count),
            Err(error) => {
                self.fail(error);
                Ok(0)
            }
        };
        self.driving.active.set(false);
        outcome
    }
    pub fn begin_shutdown(&mut self) {
        self.state = ListenerState::ShuttingDown;
        self.socket_generation += 1;
        self.cancel_timer();
        self.raw = None;
        self.state = ListenerState::Terminated;
    }
    pub const fn is_terminated(&self) -> bool {
        matches!(self.state, ListenerState::Terminated)
    }
    fn ensure_mutable(&self) -> Result<()> {
        self.driving.check()?;
        if matches!(
            self.state,
            ListenerState::ShuttingDown | ListenerState::Terminated
        ) {
            return Err(PgNotifyError::ShuttingDown);
        }
        Ok(())
    }
    fn raw_mut(&mut self) -> Result<&mut RawPgConn> {
        self.raw.as_mut().ok_or(PgNotifyError::ConnectionFailed {
            phase: "missing connection",
        })
    }
    fn drive(&mut self, event: ListenerEvent) -> Result<usize> {
        match event {
            ListenerEvent::Timer { generation } => {
                if self.continuation == Some(generation) {
                    self.continuation = None;
                    return self.continue_buffered();
                }
                if self
                    .timer
                    .is_none_or(|timer| timer.generation != generation)
                {
                    return Ok(0);
                }
                self.cancel_timer();
                match self.state {
                    ListenerState::BackoffWait => {
                        self.raw_mut()?.reset_start()?;
                        self.socket_generation += 1;
                        self.state = ListenerState::ResettingWriting;
                        self.arm(self.options.connect_timeout);
                        self.poll_connect(true)?;
                    }
                    ListenerState::Listening => self.start_ping()?,
                    ListenerState::ConnectingReading
                    | ListenerState::ConnectingWriting
                    | ListenerState::FlushingSubscriptions
                    | ListenerState::AwaitingCommandAck
                    | ListenerState::IdlePingFlushing
                    | ListenerState::IdlePingAwaiting
                    | ListenerState::ResettingReading
                    | ListenerState::ResettingWriting => {
                        return Err(PgNotifyError::TimedOut { state: self.state });
                    }
                    ListenerState::Disconnected
                    | ListenerState::ShuttingDown
                    | ListenerState::Terminated => (),
                }
                Ok(0)
            }
            ListenerEvent::Socket {
                fd,
                readable,
                writable,
                error,
            } => {
                if self.poll_interest().is_none_or(|(owned, _, _)| owned != fd) {
                    return Ok(0);
                }
                if error {
                    return Err(PgNotifyError::ConnectionFailed {
                        phase: "socket readiness",
                    });
                }
                match self.state {
                    ListenerState::ConnectingReading | ListenerState::ConnectingWriting => {
                        self.poll_connect(false)?;
                        Ok(0)
                    }
                    ListenerState::ResettingReading | ListenerState::ResettingWriting => {
                        self.poll_connect(true)?;
                        Ok(0)
                    }
                    ListenerState::FlushingSubscriptions | ListenerState::IdlePingFlushing => {
                        if readable {
                            self.raw_mut()?.consume()?;
                        }
                        if writable || readable {
                            self.flush_query()?;
                        }
                        self.drain_notifications()
                    }
                    ListenerState::AwaitingCommandAck | ListenerState::IdlePingAwaiting => {
                        if readable {
                            self.raw_mut()?.consume()?;
                            self.acknowledge()?;
                        }
                        self.drain_notifications()
                    }
                    ListenerState::Listening => {
                        if readable {
                            self.raw_mut()?.consume()?;
                        }
                        self.drain_notifications()
                    }
                    ListenerState::Disconnected
                    | ListenerState::BackoffWait
                    | ListenerState::ShuttingDown
                    | ListenerState::Terminated => Ok(0),
                }
            }
        }
    }
    fn poll_connect(&mut self, reset: bool) -> Result<()> {
        use pq_sys::PostgresPollingStatusType as Poll;
        let fd_before = self.raw_mut()?.socket();
        let status = self.raw_mut()?.poll(reset);
        if self.raw_mut()?.socket() != fd_before {
            self.socket_generation += 1;
        }
        match status {
            Poll::PGRES_POLLING_READING => {
                self.state = if reset {
                    ListenerState::ResettingReading
                } else {
                    ListenerState::ConnectingReading
                }
            }
            Poll::PGRES_POLLING_WRITING | Poll::PGRES_POLLING_ACTIVE => {
                self.state = if reset {
                    ListenerState::ResettingWriting
                } else {
                    ListenerState::ConnectingWriting
                }
            }
            Poll::PGRES_POLLING_FAILED => {
                return Err(PgNotifyError::ConnectionFailed { phase: "handshake" });
            }
            Poll::PGRES_POLLING_OK => {
                self.cancel_timer();
                self.active.fill(None);
                self.replay = true;
                self.metrics.connects += 1;
                self.state = ListenerState::Listening;
                self.sync_subscriptions()?;
            }
        }
        Ok(())
    }
    fn sync_subscriptions(&mut self) -> Result<()> {
        // UNLISTEN * makes removal and slot reuse atomic with a single acknowledged desired snapshot.
        let mut sql = String::from("UNLISTEN *;");
        for (_, name) in self.channels.iter() {
            sql.push_str("LISTEN \"");
            for ch in name.as_str().chars() {
                if ch == '"' {
                    sql.push('"');
                }
                sql.push(ch);
            }
            sql.push_str("\";");
        }
        self.sent.fill(None);
        for (id, name) in self.channels.iter() {
            self.sent[usize::from(id.0)] = Some(*name);
        }
        let sql = CString::new(sql).map_err(|_| PgNotifyError::InvalidConfig {
            reason: "channel SQL contains NUL",
        })?;
        self.raw_mut()?.send_query(&sql)?;
        self.state = ListenerState::FlushingSubscriptions;
        self.arm(self.options.command_timeout);
        self.flush_query()
    }
    fn flush_query(&mut self) -> Result<()> {
        if self.raw_mut()?.flush()? {
            self.state = if self.state == ListenerState::IdlePingFlushing {
                ListenerState::IdlePingAwaiting
            } else {
                ListenerState::AwaitingCommandAck
            };
            self.acknowledge()?;
        }
        Ok(())
    }
    fn acknowledge(&mut self) -> Result<()> {
        for _ in 0..self.options.notification_budget {
            match self.raw_mut()?.result()? {
                QueryProgress::Busy => return Ok(()),
                QueryProgress::Result => (),
                QueryProgress::Done => {
                    let ping = self.state == ListenerState::IdlePingAwaiting;
                    self.state = ListenerState::Listening;
                    self.cancel_timer();
                    if !ping {
                        self.apply_subscriptions();
                    }
                    if self.desired_changed() {
                        self.sync_subscriptions()?;
                        return Ok(());
                    }
                    self.complete_replay();
                    if let Some(interval) = self.options.idle_ping_interval {
                        self.arm(interval);
                    }
                    return Ok(());
                }
            }
        }
        // Completed results can remain solely in libpq's buffer; a caller timer drives the next batch.
        self.request_continuation();
        Ok(())
    }
    fn desired_changed(&self) -> bool {
        (0..self.active.len()).any(|index| {
            self.active[index].as_ref() != self.channels.channel(ChannelId(index as u16))
        })
    }
    fn apply_subscriptions(&mut self) {
        for index in 0..self.active.len() {
            if self.active[index] == self.sent[index] {
                continue;
            }
            let id = ChannelId(index as u16);
            if let Some(name) = self.active[index] {
                self.hook.on_unsubscribed(id, name.as_str());
            }
            if let Some(name) = self.sent[index] {
                self.hook.on_subscribed(id, name.as_str());
            }
        }
        self.active.copy_from_slice(&self.sent);
    }
    fn request_continuation(&mut self) {
        if self.continuation.is_none() {
            self.timer_generation += 1;
            self.continuation = Some(self.timer_generation);
        }
    }
    fn continue_buffered(&mut self) -> Result<usize> {
        match self.state {
            ListenerState::AwaitingCommandAck | ListenerState::IdlePingAwaiting => {
                self.acknowledge()?
            }
            ListenerState::FlushingSubscriptions | ListenerState::IdlePingFlushing => {
                self.flush_query()?
            }
            ListenerState::Listening => (),
            ListenerState::Disconnected
            | ListenerState::ConnectingReading
            | ListenerState::ConnectingWriting
            | ListenerState::BackoffWait
            | ListenerState::ResettingReading
            | ListenerState::ResettingWriting
            | ListenerState::ShuttingDown
            | ListenerState::Terminated => return Ok(0),
        }
        self.drain_notifications()
    }
    fn complete_replay(&mut self) {
        if self.replay {
            self.epoch += 1;
            self.backoff = self.options.reconnect_initial;
            self.replay = false;
            let pid = self.raw.as_ref().map_or(0, RawPgConn::backend_pid);
            if self.epoch == 1 {
                self.hook.on_connected(pid);
            } else {
                self.metrics.reconnects += 1;
                self.hook.on_reconnected(self.epoch, pid);
            }
        }
    }
    fn drain_notifications(&mut self) -> Result<usize> {
        if self.state != ListenerState::Listening {
            return Ok(0);
        }
        let mut count = 0;
        for _ in 0..self.options.notification_budget {
            let Some(guard) = self.raw_mut()?.notification() else {
                break;
            };
            let mut notify = guard.notification()?;
            notify.channel_id = self.channels.lookup(notify.channel);
            self.hook.on_notify(notify);
            self.metrics.notifications += 1;
            count += 1;
        }
        if count > 0
            && let Some(interval) = self.options.idle_ping_interval
        {
            self.arm(interval);
        }
        // libpq may have read more than one budget; a zero-delay wake avoids stranding buffered data.
        if count == self.options.notification_budget {
            self.request_continuation();
        }
        Ok(count)
    }
    fn start_ping(&mut self) -> Result<()> {
        self.raw_mut()?.send_query(c"SELECT 1;")?;
        self.metrics.pings += 1;
        self.state = ListenerState::IdlePingFlushing;
        self.arm(self.options.command_timeout);
        self.flush_query()
    }
    fn fail(&mut self, error: PgNotifyError) {
        let already_driving = self.driving.active.replace(true);
        self.continuation = None;
        self.metrics.errors += 1;
        self.metrics.disconnects += 1;
        self.hook.on_error(error);
        self.hook.on_disconnect(self.epoch, error);
        self.socket_generation += 1;
        self.state = ListenerState::BackoffWait;
        self.arm(self.backoff);
        self.backoff = self
            .backoff
            .saturating_mul(2)
            .min(self.options.reconnect_max);
        self.driving.active.set(already_driving);
    }
    fn arm(&mut self, duration: Duration) {
        self.timer_generation += 1;
        self.timer = Some(TimerDeadline {
            generation: self.timer_generation,
            duration,
        });
    }
    fn cancel_timer(&mut self) {
        self.continuation = None;
        self.timer_generation += 1;
        self.timer = None;
    }
}
