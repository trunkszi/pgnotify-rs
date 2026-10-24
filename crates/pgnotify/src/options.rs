//! Listener budgets, deterministic deadlines and libpq keepalive settings.
use crate::error::{PgNotifyError, Result};
use std::time::Duration;
/// Defaults: 256 channels, 64 notifications per drive, 10s connect and 5s command deadlines,
/// 100ms..30s retry backoff, and a 30s idle probe. Capacity must be 1..=65536,
/// budgets and durations nonzero, and retry maximum at least its initial value.
/// Keepalive values must fit a positive libpq integer; defaults are 30s idle,
/// 10s interval, three probes, and a 30,000ms TCP user timeout.
#[derive(Debug, Clone)]
pub struct ListenerOptions {
    pub channel_capacity: usize,
    pub notification_budget: usize,
    pub connect_timeout: Duration,
    pub command_timeout: Duration,
    pub reconnect_initial: Duration,
    pub reconnect_max: Duration,
    pub idle_ping_interval: Option<Duration>,
    pub keepalives_idle: u32,
    pub keepalives_interval: u32,
    pub keepalives_count: u32,
    pub tcp_user_timeout_ms: u32,
}
impl Default for ListenerOptions {
    fn default() -> Self {
        Self {
            channel_capacity: 256,
            notification_budget: 64,
            connect_timeout: Duration::from_secs(10),
            command_timeout: Duration::from_secs(5),
            reconnect_initial: Duration::from_millis(100),
            reconnect_max: Duration::from_secs(30),
            idle_ping_interval: Some(Duration::from_secs(30)),
            keepalives_idle: 30,
            keepalives_interval: 10,
            keepalives_count: 3,
            tcp_user_timeout_ms: 30_000,
        }
    }
}
impl ListenerOptions {
    pub(crate) fn validate(&self) -> Result<()> {
        if !(1..=65536).contains(&self.channel_capacity) {
            return Err(PgNotifyError::InvalidConfig {
                reason: "channel_capacity",
            });
        }
        if self.notification_budget == 0 {
            return Err(PgNotifyError::InvalidConfig {
                reason: "notification_budget",
            });
        }
        if [
            Some(self.connect_timeout),
            Some(self.command_timeout),
            Some(self.reconnect_initial),
            Some(self.reconnect_max),
            self.idle_ping_interval,
        ]
        .into_iter()
        .flatten()
        .any(|duration| duration.as_secs() > i64::MAX as u64)
        {
            return Err(PgNotifyError::InvalidConfig {
                reason: "timeouts exceed signed timespec range",
            });
        }
        if self.connect_timeout.is_zero()
            || self.command_timeout.is_zero()
            || self.reconnect_initial.is_zero()
            || self.reconnect_max < self.reconnect_initial
            || self.idle_ping_interval.is_some_and(|d| d.is_zero())
        {
            return Err(PgNotifyError::InvalidConfig { reason: "timeouts" });
        }
        if [
            self.keepalives_idle,
            self.keepalives_interval,
            self.keepalives_count,
            self.tcp_user_timeout_ms,
        ]
        .into_iter()
        .any(|n| n == 0 || n > i32::MAX as u32)
        {
            return Err(PgNotifyError::InvalidConfig {
                reason: "keepalives",
            });
        }
        Ok(())
    }
}
