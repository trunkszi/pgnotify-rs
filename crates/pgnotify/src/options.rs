//! Listener budgets, deterministic deadlines and libpq keepalive settings.
use crate::error::{PgNotifyError, Result};
use std::time::Duration;
use uarch::prelude::{cold_sink, register_pass};
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
    #[cold_sink]
    pub(crate) fn validate(&self) -> Result<()> {
        if !(1..=65536).contains(&self.channel_capacity) {
            return Err(PgNotifyError::invalid_config("channel_capacity"));
        }
        if self.notification_budget == 0 {
            return Err(PgNotifyError::invalid_config("notification_budget"));
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
            return Err(PgNotifyError::invalid_config(
                "timeouts exceed signed timespec range",
            ));
        }
        if self.connect_timeout.is_zero()
            || self.command_timeout.is_zero()
            || self.reconnect_initial.is_zero()
            || self.reconnect_max < self.reconnect_initial
            || self.idle_ping_interval.is_some_and(|d| d.is_zero())
        {
            return Err(PgNotifyError::invalid_config("timeouts"));
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
            return Err(PgNotifyError::invalid_config("keepalives"));
        }
        Ok(())
    }
}

/// Exact nanosecond bounds for reconnect delays representable in two registers.
#[register_pass]
#[derive(Clone, Copy)]
pub(crate) struct ReconnectPolicy {
    initial_ns: u64,
    maximum_ns: u64,
}
uarch::layout::assert_layout!(ReconnectPolicy, size = 16, align = 8, fields = {
    initial_ns: 0, maximum_ns: 8,
});
impl ReconnectPolicy {
    #[cold_sink]
    pub(crate) fn from_durations(initial: Duration, maximum: Duration) -> Option<Self> {
        Some(Self {
            initial_ns: u64::try_from(initial.as_nanos()).ok()?,
            maximum_ns: u64::try_from(maximum.as_nanos()).ok()?,
        })
    }
    #[cold_sink]
    pub(crate) fn delay(self, attempt: u32) -> Duration {
        let mut remaining = attempt.min(96);
        let mut delay = self.initial_ns;
        while remaining != 0 && delay < self.maximum_ns {
            let shift = remaining.min(63);
            delay = delay.saturating_mul(1_u64 << shift).min(self.maximum_ns);
            remaining -= shift;
        }
        Duration::from_nanos(delay)
    }
}

#[cold_sink]
pub(crate) fn reconnect_delay(initial: Duration, maximum: Duration, attempt: u32) -> Duration {
    if let Some(policy) = ReconnectPolicy::from_durations(initial, maximum) {
        return policy.delay(attempt);
    }
    // Public Duration bounds can exceed u64 nanoseconds, so retain their complete precision.
    let mut remaining = attempt.min(96);
    let mut delay = initial;
    while remaining != 0 && delay < maximum {
        let shift = remaining.min(31);
        delay = delay.saturating_mul(1_u32 << shift).min(maximum);
        remaining -= shift;
    }
    delay
}

#[cfg(test)]
mod retry_tests {
    use super::*;
    #[test]
    fn retry_preserves_duration_range() {
        for (initial, maximum) in [
            (Duration::from_millis(100), Duration::from_secs(30)),
            (
                Duration::from_nanos(1),
                Duration::new(i64::MAX as u64, 999_999_999),
            ),
            (Duration::from_secs(1 << 40), Duration::from_secs(1 << 50)),
            (
                Duration::from_nanos(u64::MAX / 2 + 1),
                Duration::from_nanos(u64::MAX),
            ),
        ] {
            let mut expected = initial;
            for attempt in 0..120 {
                assert_eq!(reconnect_delay(initial, maximum, attempt), expected);
                expected = expected.saturating_mul(2).min(maximum);
            }
        }
        let initial = Duration::new(1, 23);
        let maximum = Duration::new(5, 79);
        let policy = ReconnectPolicy::from_durations(initial, maximum);
        assert!(policy.is_some());
        if let Some(policy) = policy {
            assert_eq!(policy.delay(1), Duration::new(2, 46));
            assert_eq!(policy.delay(3), maximum);
        }
        assert!(ReconnectPolicy::from_durations(initial, Duration::MAX).is_none());
        assert_eq!(size_of::<ReconnectPolicy>(), 16);
    }
}
