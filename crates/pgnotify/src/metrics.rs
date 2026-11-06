//! Owner-thread counters; snapshot export is the caller's responsibility.
#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PgNotifyMetricsSnapshot {
    pub notifications: u64,
    pub connects: u64,
    pub reconnects: u64,
    pub disconnects: u64,
    pub errors: u64,
    pub pings: u64,
}

uarch::layout::assert_layout!(PgNotifyMetricsSnapshot, size = 48, align = 8, fields = {
    notifications: 0, connects: 8, reconnects: 16, disconnects: 24, errors: 32, pings: 40,
});
// SAFETY: Six contiguous u64 counters occupy all 48 bytes, as the layout proof verifies.
unsafe impl uarch::layout::NoPadding for PgNotifyMetricsSnapshot {}
impl PgNotifyMetricsSnapshot {
    #[uarch::prelude::hot_inline]
    pub(crate) fn record_notification(&mut self) {
        self.notifications += 1;
    }
    #[uarch::prelude::hot_inline]
    pub(crate) fn record_connect(&mut self) {
        self.connects += 1;
    }
    #[uarch::prelude::hot_inline]
    pub(crate) fn record_reconnect(&mut self) {
        self.reconnects += 1;
    }
    #[uarch::prelude::hot_inline]
    pub(crate) fn record_disconnect(&mut self) {
        self.disconnects += 1;
    }
    #[uarch::prelude::hot_inline]
    pub(crate) fn record_error(&mut self) {
        self.errors += 1;
    }
    #[uarch::prelude::hot_inline]
    pub(crate) fn record_ping(&mut self) {
        self.pings += 1;
    }
}
