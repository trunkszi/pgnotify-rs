//! Allocation-free listener errors and lifecycle states.
use core::fmt;

pub type Result<T> = core::result::Result<T, PgNotifyError>;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SqlState(pub [u8; 5]);
impl fmt::Display for SqlState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{}", char::from(byte))?;
        }
        Ok(())
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ListenerState {
    Disconnected = 0,
    ConnectingReading = 1,
    ConnectingWriting = 2,
    FlushingSubscriptions = 3,
    AwaitingCommandAck = 4,
    Listening = 5,
    IdlePingFlushing = 6,
    IdlePingAwaiting = 7,
    BackoffWait = 8,
    ResettingReading = 9,
    ResettingWriting = 10,
    ShuttingDown = 11,
    Terminated = 12,
}

/// The high nibble groups phases with the same connection ownership.
/// ActiveSession uses zero so its hot-path guard is a single masked test.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Superstate {
    Offline = 0x10,
    Handshaking = 0x20,
    ActiveSession = 0x00,
    Terminating = 0x30,
}

/// One-byte lifecycle: high nibble is the superstate, low bit 3 queues a continuation.
/// Replay phases cover both initial subscription synchronization and reconnect replay.
/// The continuation generation is owned by PgListener and is valid only while this bit is set.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ListenerPhase {
    Disconnected = 0x10,
    BackoffWait = 0x11,
    ConnectingReading = 0x20,
    ConnectingWriting = 0x21,
    ResettingReading = 0x22,
    ResettingWriting = 0x23,
    FlushingSubscriptions = 0x00,
    AwaitingCommandAck = 0x01,
    Listening = 0x02,
    IdlePingFlushing = 0x03,
    IdlePingAwaiting = 0x04,
    FlushingReplay = 0x05,
    AwaitingReplayAck = 0x06,
    FlushingSubscriptionsContinuation = 0x08,
    AwaitingCommandAckContinuation = 0x09,
    ListeningContinuation = 0x0a,
    IdlePingFlushingContinuation = 0x0b,
    IdlePingAwaitingContinuation = 0x0c,
    FlushingReplayContinuation = 0x0d,
    AwaitingReplayAckContinuation = 0x0e,
    ShuttingDown = 0x30,
    Terminated = 0x31,
}
impl ListenerPhase {
    #[inline]
    pub const fn is_offline(self) -> bool {
        self as u8 & 0xf0 == Superstate::Offline as u8
    }
    #[inline]
    pub const fn is_handshaking(self) -> bool {
        self as u8 & 0xf0 == Superstate::Handshaking as u8
    }
    #[inline]
    pub const fn is_active_session(self) -> bool {
        self as u8 & 0xf0 == Superstate::ActiveSession as u8
    }
    #[inline]
    pub const fn is_terminating(self) -> bool {
        self as u8 & 0xf0 == Superstate::Terminating as u8
    }
    #[inline]
    pub const fn superstate(self) -> Superstate {
        if self.is_offline() {
            Superstate::Offline
        } else if self.is_handshaking() {
            Superstate::Handshaking
        } else if self.is_active_session() {
            Superstate::ActiveSession
        } else {
            Superstate::Terminating
        }
    }
    #[inline]
    pub const fn has_continuation(self) -> bool {
        self as u8 & 8 != 0
    }
    #[inline]
    pub const fn without_continuation(self) -> Self {
        match self {
            Self::FlushingSubscriptionsContinuation => Self::FlushingSubscriptions,
            Self::AwaitingCommandAckContinuation => Self::AwaitingCommandAck,
            Self::ListeningContinuation => Self::Listening,
            Self::IdlePingFlushingContinuation => Self::IdlePingFlushing,
            Self::IdlePingAwaitingContinuation => Self::IdlePingAwaiting,
            Self::FlushingReplayContinuation => Self::FlushingReplay,
            Self::AwaitingReplayAckContinuation => Self::AwaitingReplayAck,
            phase => phase,
        }
    }
    /// Queue a wake in active-session phases; other superstates remain unchanged.
    #[inline]
    pub const fn with_continuation(self) -> Self {
        match self {
            Self::FlushingSubscriptions => Self::FlushingSubscriptionsContinuation,
            Self::AwaitingCommandAck => Self::AwaitingCommandAckContinuation,
            Self::Listening => Self::ListeningContinuation,
            Self::IdlePingFlushing => Self::IdlePingFlushingContinuation,
            Self::IdlePingAwaiting => Self::IdlePingAwaitingContinuation,
            Self::FlushingReplay => Self::FlushingReplayContinuation,
            Self::AwaitingReplayAck => Self::AwaitingReplayAckContinuation,
            phase => phase,
        }
    }
    #[inline]
    pub const fn is_subscription_replay(self) -> bool {
        matches!(
            self.without_continuation(),
            Self::FlushingReplay | Self::AwaitingReplayAck
        )
    }
    /// Preserve a queued continuation across active-session transitions only.
    #[inline]
    pub const fn transition(self, next: Self) -> Self {
        if self.has_continuation() && next.is_active_session() {
            next.with_continuation()
        } else {
            next
        }
    }
    /// Advance a flushing phase after libpq has accepted all outbound query bytes.
    ///
    /// # Panics
    /// Panics unless this is a subscription or ping flushing phase.
    #[inline]
    pub const fn after_flush(self) -> Self {
        let next = match self.without_continuation() {
            Self::FlushingSubscriptions => Self::AwaitingCommandAck,
            Self::FlushingReplay => Self::AwaitingReplayAck,
            Self::IdlePingFlushing => Self::IdlePingAwaiting,
            _ => panic!("query flush requires a flushing phase"),
        };
        self.transition(next)
    }
    /// Project the packed lifecycle onto the stable public state vocabulary.
    #[inline]
    pub const fn state(self) -> ListenerState {
        match self {
            Self::Disconnected => ListenerState::Disconnected,
            Self::BackoffWait => ListenerState::BackoffWait,
            Self::ConnectingReading => ListenerState::ConnectingReading,
            Self::ConnectingWriting => ListenerState::ConnectingWriting,
            Self::ResettingReading => ListenerState::ResettingReading,
            Self::ResettingWriting => ListenerState::ResettingWriting,
            Self::FlushingSubscriptions | Self::FlushingSubscriptionsContinuation => {
                ListenerState::FlushingSubscriptions
            }
            Self::AwaitingCommandAck | Self::AwaitingCommandAckContinuation => {
                ListenerState::AwaitingCommandAck
            }
            Self::Listening | Self::ListeningContinuation => ListenerState::Listening,
            Self::IdlePingFlushing | Self::IdlePingFlushingContinuation => {
                ListenerState::IdlePingFlushing
            }
            Self::IdlePingAwaiting | Self::IdlePingAwaitingContinuation => {
                ListenerState::IdlePingAwaiting
            }
            Self::FlushingReplay | Self::FlushingReplayContinuation => {
                ListenerState::FlushingSubscriptions
            }
            Self::AwaitingReplayAck | Self::AwaitingReplayAckContinuation => {
                ListenerState::AwaitingCommandAck
            }
            Self::ShuttingDown => ListenerState::ShuttingDown,
            Self::Terminated => ListenerState::Terminated,
        }
    }
}
const _: () = assert!(core::mem::size_of::<ListenerPhase>() == 1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PgNotifyError {
    #[error("[pgnotify] invalid channel name (len={len}, must be 1..=63 ASCII/UTF-8 without NUL)")]
    InvalidChannelName { len: usize },
    #[error("[pgnotify] payload length {len} exceeds PostgreSQL {max}-byte limit")]
    PayloadTooLarge { len: usize, max: usize },
    #[error("[pgnotify] channel table full (capacity={capacity})")]
    ChannelTableFull { capacity: usize },
    #[error("[pgnotify] unknown channel id {0:?}")]
    UnknownChannelId(crate::types::ChannelId),
    #[error("[pgnotify] invalid configuration: {reason}")]
    InvalidConfig { reason: &'static str },
    #[error("[pgnotify] connection failed in phase {phase}")]
    ConnectionFailed { phase: &'static str },
    #[error("[pgnotify] postgres protocol error (sqlstate={sqlstate})")]
    ProtocolError { sqlstate: SqlState },
    #[error("[pgnotify] system I/O error (op={op}, errno={errno})")]
    IoError { op: &'static str, errno: i32 },
    #[error("[pgnotify] payload codec error: {reason}")]
    CodecError { reason: &'static str },
    #[error("[pgnotify] operation timed out in state {state:?}")]
    TimedOut { state: ListenerState },
    #[error("[pgnotify] re-entrant drive forbidden while callback is active")]
    ReentrantDriveForbidden,
    #[error("[pgnotify] listener is shutting down or terminated")]
    ShuttingDown,
}
