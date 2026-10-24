//! Owner-thread counters; snapshot export is the caller's responsibility.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PgNotifyMetricsSnapshot {
    pub notifications: u64,
    pub connects: u64,
    pub reconnects: u64,
    pub disconnects: u64,
    pub errors: u64,
    pub pings: u64,
}
