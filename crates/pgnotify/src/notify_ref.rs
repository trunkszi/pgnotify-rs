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
    #[uarch::prelude::hot_inline]
    pub const fn payload_bytes(&self) -> &'a [u8] {
        self.payload.as_bytes()
    }
    /// # Errors
    /// Rejects unregistered channels, invalid names, and payloads exceeding the frame capacity.
    #[inline]
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

#[inline]
pub(crate) fn prefix_word(bytes: &[u8]) -> u64 {
    if let Some(word) = bytes.first_chunk::<8>() {
        return u64::from_le_bytes(*word);
    }
    let n = bytes.len();
    match n {
        4..=7 => {
            let first = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            let last = u32::from_le_bytes([bytes[n - 4], bytes[n - 3], bytes[n - 2], bytes[n - 1]]);
            u64::from(first) | (u64::from(last) << ((n - 4) * 8))
        }
        2..=3 => {
            u64::from(u16::from_le_bytes([bytes[0], bytes[1]]))
                | (u64::from(bytes[n - 1]) << ((n - 1) * 8))
        }
        1 => u64::from(bytes[0]),
        _ => 0,
    }
}
