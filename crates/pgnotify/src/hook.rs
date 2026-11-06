//! Statically dispatched lifecycle and typed codec seams.
use crate::{
    error::{PgNotifyError, Result},
    notify_ref::NotificationRef,
    types::{ChannelId, PayloadBuf},
};
pub trait NotifyHook {
    #[uarch::prelude::hot_inline]
    fn on_connected(&mut self, _backend_pid: i32) {}
    #[uarch::prelude::hot_inline]
    fn on_reconnected(&mut self, _epoch: u64, _backend_pid: i32) {}
    #[uarch::prelude::hot_inline]
    fn on_subscribed(&mut self, _channel_id: ChannelId, _channel: &str) {}
    #[uarch::prelude::hot_inline]
    fn on_unsubscribed(&mut self, _channel_id: ChannelId, _channel: &str) {}
    fn on_notify(&mut self, notify: NotificationRef<'_>);
    #[uarch::prelude::hot_inline]
    fn on_disconnect(&mut self, _epoch: u64, _error: PgNotifyError) {}
    #[uarch::prelude::hot_inline]
    fn on_error(&mut self, _error: PgNotifyError) {}
}
pub trait NotifyPayload: Sized {
    /// # Errors
    /// Returns payload encoding or capacity errors.
    fn encode_payload(&self, out: &mut PayloadBuf) -> Result<()>;
    /// # Errors
    /// Returns payload decoding errors.
    fn decode_payload(bytes: &[u8]) -> Result<Self>;
}
pub trait NotifyRouter: Sized {
    const CHANNELS: &'static [&'static str];
    const LISTEN_BATCH_SQL: &'static str;
    fn channel_name(&self) -> &'static str;
    /// # Errors
    /// Returns payload encoding or capacity errors.
    fn encode_event_payload(&self, out: &mut PayloadBuf) -> Result<()>;
    /// # Errors
    /// Returns payload decoding errors for registered channels.
    fn decode_notification(notify: NotificationRef<'_>) -> Result<Option<Self>>;
}
