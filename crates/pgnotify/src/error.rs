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
