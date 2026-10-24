//! Fixed-capacity notification and event contracts.
use crate::error::{PgNotifyError, Result};
use std::{io, io::Write, time::Duration};

pub const PG_CHANNEL_MAX_LEN: usize = 63;
pub const PG_NOTIFY_MAX_PAYLOAD_LEN: usize = 7999;
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChannelId(pub u16);

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FixedString<const N: usize> {
    len: u8,
    bytes: [u8; N],
}
impl<const N: usize> FixedString<N> {
    pub const EMPTY: Self = Self {
        len: 0,
        bytes: [0; N],
    };
    /// # Errors
    /// Rejects empty, oversized, or NUL-containing channel names.
    pub fn try_from_str(s: &str) -> Result<Self> {
        validate_channel_name(s)?;
        if s.len() > N {
            return Err(PgNotifyError::InvalidChannelName { len: s.len() });
        }
        let mut result = Self::EMPTY;
        result.bytes[..s.len()].copy_from_slice(s.as_bytes());
        result.len = s.len() as u8;
        Ok(result)
    }
    pub fn as_str(&self) -> &str {
        // SAFETY: only UTF-8 input enters the private bytes field; len ends at its boundary.
        unsafe { core::str::from_utf8_unchecked(self.as_bytes()) }
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }
    pub const fn len(&self) -> usize {
        self.len as usize
    }
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
}
pub const fn validate_channel_name_bytes(bytes: &[u8]) -> bool {
    if bytes.is_empty() || bytes.len() > PG_CHANNEL_MAX_LEN {
        return false;
    }
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == 0 {
            return false;
        }
        i += 1;
    }
    core::str::from_utf8(bytes).is_ok()
}
/// # Errors
/// Rejects names outside PostgreSQL's byte limit or containing NUL.
pub fn validate_channel_name(name: &str) -> Result<()> {
    if validate_channel_name_bytes(name.as_bytes()) {
        Ok(())
    } else {
        Err(PgNotifyError::InvalidChannelName { len: name.len() })
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListenCommand {
    Subscribe {
        channel_id: ChannelId,
        channel: FixedString<64>,
    },
    Unsubscribe {
        channel_id: ChannelId,
    },
    UnsubscribeByName {
        channel: FixedString<64>,
    },
    Ping,
    Reconnect,
    Shutdown,
}
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotifyFrame<const N: usize = 1024> {
    pub backend_pid: i32,
    pub channel_id: ChannelId,
    pub payload_len: u16,
    pub channel: FixedString<64>,
    pub payload: [u8; N],
}

/// Stack-backed UTF-8 payload destination with explicit overflow reporting.
pub struct PayloadBuf {
    len: usize,
    buf: [u8; PG_NOTIFY_MAX_PAYLOAD_LEN],
    overflowed: bool,
    #[cfg(feature = "sonic")]
    scratch: [core::mem::MaybeUninit<u8>; 6 * PG_NOTIFY_MAX_PAYLOAD_LEN + 35],
}
impl Default for PayloadBuf {
    fn default() -> Self {
        Self::new()
    }
}
impl PayloadBuf {
    pub const CAPACITY: usize = PG_NOTIFY_MAX_PAYLOAD_LEN;
    pub const fn new() -> Self {
        Self {
            len: 0,
            buf: [0; Self::CAPACITY],
            overflowed: false,
            #[cfg(feature = "sonic")]
            scratch: [core::mem::MaybeUninit::uninit(); 6 * PG_NOTIFY_MAX_PAYLOAD_LEN + 35],
        }
    }
    pub fn clear(&mut self) {
        self.len = 0;
        self.overflowed = false;
    }
    pub const fn len(&self) -> usize {
        self.len
    }
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf[..self.len]
    }
    pub fn as_slice(&self) -> &[u8] {
        self.as_bytes()
    }
    pub const fn overflowed(&self) -> bool {
        self.overflowed
    }
    /// # Errors
    /// Returns a codec error if raw writes produced invalid UTF-8.
    pub fn as_str(&self) -> Result<&str> {
        core::str::from_utf8(self.as_bytes()).map_err(|_| PgNotifyError::CodecError {
            reason: "payload is not UTF-8",
        })
    }
    /// # Errors
    /// Rejects writes exceeding the payload capacity without changing its length.
    pub fn extend_from_slice(&mut self, bytes: &[u8]) -> Result<()> {
        self.write_all(bytes).map_err(|_| self.write_error())
    }
    fn write_error(&self) -> PgNotifyError {
        if self.overflowed {
            PgNotifyError::PayloadTooLarge {
                len: Self::CAPACITY + 1,
                max: Self::CAPACITY,
            }
        } else {
            PgNotifyError::CodecError {
                reason: "payload serialization",
            }
        }
    }
    #[cfg(feature = "sonic")]
    /// # Errors
    /// Returns a bounded payload overflow or serialization error.
    pub fn encode_json<T: sonic_rs::Serialize>(&mut self, value: &T) -> Result<()> {
        self.clear();
        sonic_rs::to_writer(&mut *self, value).map_err(|_| self.write_error())
    }
    fn reserve(&mut self, additional: usize) -> io::Result<()> {
        if additional > Self::CAPACITY - self.len {
            self.overflowed = true;
            return Err(io::ErrorKind::WriteZero.into());
        }
        Ok(())
    }
}
impl io::Write for PayloadBuf {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.reserve(bytes.len())?;
        self.buf[self.len..self.len + bytes.len()].copy_from_slice(bytes);
        self.len += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
#[cfg(feature = "sonic")]
impl sonic_rs::writer::WriteExt for PayloadBuf {
    fn reserve_with(&mut self, additional: usize) -> io::Result<&mut [core::mem::MaybeUninit<u8>]> {
        if additional > self.scratch.len() {
            self.overflowed = true;
            return Err(io::ErrorKind::WriteZero.into());
        }
        Ok(&mut self.scratch[..additional])
    }
    // SAFETY: caller initializes exactly additional bytes returned by reserve_with before this call.
    unsafe fn flush_len(&mut self, additional: usize) -> io::Result<()> {
        self.reserve(additional)?;
        if additional > self.scratch.len() {
            self.overflowed = true;
            return Err(io::ErrorKind::WriteZero.into());
        }
        // SAFETY: WriteExt's caller initialized additional reserved bytes; both buffers are disjoint.
        unsafe {
            core::ptr::copy_nonoverlapping(
                self.scratch.as_ptr().cast::<u8>(),
                self.buf.as_mut_ptr().add(self.len),
                additional,
            );
        }
        self.len += additional;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SocketInterest {
    pub readable: bool,
    pub writable: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListenerEvent {
    Socket {
        fd: i32,
        readable: bool,
        writable: bool,
        error: bool,
    },
    Timer {
        generation: u64,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimerDeadline {
    pub generation: u64,
    pub duration: Duration,
}
const _: () = {
    assert!(core::mem::size_of::<ChannelId>() == 2);
    assert!(core::mem::size_of::<FixedString<64>>() == 65);
    assert!(core::mem::size_of::<ListenCommand>() == 72);
    assert!(core::mem::align_of::<ListenCommand>() == 4);
    assert!(core::mem::size_of::<NotifyFrame<1024>>() == 1100);
    assert!(core::mem::align_of::<NotifyFrame<1024>>() == 4);
    assert!(core::mem::offset_of!(ChannelId, 0) == 0);
    assert!(core::mem::offset_of!(FixedString<64>, len) == 0);
    assert!(core::mem::offset_of!(FixedString<64>, bytes) == 1);
    assert!(core::mem::offset_of!(NotifyFrame<1024>, backend_pid) == 0);
    assert!(core::mem::offset_of!(NotifyFrame<1024>, channel_id) == 4);
    assert!(core::mem::offset_of!(NotifyFrame<1024>, payload_len) == 6);
    assert!(core::mem::offset_of!(NotifyFrame<1024>, channel) == 8);
    assert!(core::mem::offset_of!(NotifyFrame<1024>, payload) == 73);
};
