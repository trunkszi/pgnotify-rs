//! Caller-driven PostgreSQL notification listener and bounded value contracts.
#[cfg(test)]
extern crate self as pgnotify;
#[cfg(test)]
#[path = "../tests/support/cluster_peer.rs"]
mod test_support;

pub mod channel_table;
pub mod error;
pub mod fsm;
pub mod hook;
pub mod metrics;
pub mod notify_ref;
pub mod options;
pub mod raw_conn;
pub mod types;

pub use channel_table::ChannelTable;
pub use error::{ListenerPhase, ListenerState, PgNotifyError, Result, SqlState, Superstate};
pub use fsm::PgListener;
pub use hook::{NotifyHook, NotifyPayload, NotifyRouter};
pub use metrics::PgNotifyMetricsSnapshot;
pub use notify_ref::NotificationRef;
pub use options::ListenerOptions;
pub use types::*;

// Re-exports keep optional adapters and downstream users on the same ABI versions.
#[cfg(feature = "diesel")]
pub use diesel as diesel_api;
#[cfg(feature = "uring")]
pub use liburing as uring_api;
#[cfg(feature = "sonic")]
pub use sonic_rs as sonic_api;

#[cfg(feature = "diesel")]
pub mod diesel_ext;
#[cfg(feature = "diesel")]
pub use diesel_ext::{DieselNotifyError, DieselNotifyExt, DieselNotifyResult, pg_notify};

#[cfg(feature = "uring")]
pub mod proactor;
#[cfg(feature = "uring")]
pub use proactor::ProactorPgListener;

/// Implementation support for derives, independent of downstream dependency names.
#[doc(hidden)]
pub mod __private {
    pub use uarch::flow::{cold_path, unlikely};
    pub use uarch::prelude::hot_inline;
}
