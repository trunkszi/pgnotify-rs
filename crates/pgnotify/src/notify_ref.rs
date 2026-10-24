//! Callback-scoped, borrowed PostgreSQL notification.
use crate::{
    error::{PgNotifyError, Result},
    types::{ChannelId, FixedString, NotifyFrame},
};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotificationRef<'a> {
    pub backend_pid: i32,
    pub channel_id: Option<ChannelId>,
    pub channel: &'a str,
    pub payload: &'a str,
}
impl<'a> NotificationRef<'a> {
    pub const fn payload_bytes(&self) -> &'a [u8] {
        self.payload.as_bytes()
    }
    /// # Errors
    /// Rejects unregistered channels, invalid names, and payloads exceeding the frame capacity.
    pub fn to_frame<const N: usize>(&self) -> Result<NotifyFrame<N>> {
        let channel_id = self.channel_id.ok_or(PgNotifyError::CodecError {
            reason: "unregistered channel",
        })?;
        let channel = FixedString::try_from_str(self.channel)?;
        if self.payload.len() > N || self.payload.len() > usize::from(u16::MAX) {
            return Err(PgNotifyError::PayloadTooLarge {
                len: self.payload.len(),
                max: N.min(usize::from(u16::MAX)),
            });
        }
        let mut frame = NotifyFrame {
            backend_pid: self.backend_pid,
            channel_id,
            payload_len: self.payload.len() as u16,
            channel,
            payload: [0; N],
        };
        frame.payload[..self.payload.len()].copy_from_slice(self.payload.as_bytes());
        Ok(frame)
    }
}
