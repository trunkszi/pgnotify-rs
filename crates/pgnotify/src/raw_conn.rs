//! Thread-affine, non-blocking libpq ownership and callback-scoped notification storage.
use crate::{
    error::{PgNotifyError, Result, SqlState},
    notify_ref::NotificationRef,
    options::ListenerOptions,
};
use std::{
    ffi::{CStr, CString},
    marker::PhantomData,
    ptr::NonNull,
    rc::Rc,
};

use uarch::prelude::{cold_path, cold_sink, unlikely};

/// A single owning libpq connection. Its socket is borrowed, never independently closed.
pub struct RawPgConn {
    ptr: NonNull<pq_sys::PGconn>,
    _affinity: PhantomData<Rc<()>>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueryProgress {
    Busy,
    Result,
    Done,
}

impl RawPgConn {
    /// # Errors
    /// Rejects invalid connection strings, hostname DNS, or libpq allocation failures.
    /// Use a numeric host/hostaddr or a Unix socket to keep establishment non-blocking.
    pub fn connect(database_url: &str, options: &ListenerOptions) -> Result<Self> {
        options.validate()?;
        let url = CString::new(database_url)
            .map_err(|_| PgNotifyError::invalid_config("database_url contains NUL"))?;
        validate_endpoint(&url)?;
        let values = [
            url,
            CString::new("1").map_err(|_| invalid_url())?,
            decimal(options.keepalives_idle),
            decimal(options.keepalives_interval),
            decimal(options.keepalives_count),
            decimal(options.tcp_user_timeout_ms),
        ];
        let keys = [
            c"dbname".as_ptr(),
            c"keepalives".as_ptr(),
            c"keepalives_idle".as_ptr(),
            c"keepalives_interval".as_ptr(),
            c"keepalives_count".as_ptr(),
            c"tcp_user_timeout".as_ptr(),
            core::ptr::null(),
        ];
        let pointers = [
            values[0].as_ptr(),
            values[1].as_ptr(),
            values[2].as_ptr(),
            values[3].as_ptr(),
            values[4].as_ptr(),
            values[5].as_ptr(),
            core::ptr::null(),
        ];
        // SAFETY: terminated key/value arrays and their C strings remain live through this copying call.
        let ptr = NonNull::new(unsafe {
            pq_sys::PQconnectStartParams(keys.as_ptr(), pointers.as_ptr(), 1)
        })
        .ok_or_else(|| PgNotifyError::connection_failed("allocate connection"))?;
        let conn = Self {
            ptr,
            _affinity: PhantomData,
        };
        // SAFETY: conn exclusively owns the initialized PGconn; setting nonblocking does not wait.
        if unsafe { pq_sys::PQsetnonblocking(conn.ptr.as_ptr(), 1) } != 0 {
            return Err(PgNotifyError::connection_failed("set nonblocking"));
        }
        // SAFETY: the callback has static code lifetime, ignores both pointers, and never unwinds.
        unsafe {
            pq_sys::PQsetNoticeReceiver(
                conn.ptr.as_ptr(),
                Some(ignore_notice),
                core::ptr::null_mut(),
            );
        }
        Ok(conn)
    }
    #[inline]
    pub fn socket(&self) -> i32 {
        // SAFETY: self holds a live PGconn throughout the call.
        unsafe { pq_sys::PQsocket(self.ptr.as_ptr()) }
    }
    #[inline]
    pub fn backend_pid(&self) -> i32 {
        // SAFETY: self holds a live PGconn throughout the call.
        unsafe { pq_sys::PQbackendPID(self.ptr.as_ptr()) }
    }
    #[inline]
    pub(crate) fn poll(&mut self, reset: bool) -> pq_sys::PostgresPollingStatusType {
        // SAFETY: exclusive access serializes polling; PGconn was created by PQconnectStartParams.
        let status = unsafe {
            if reset {
                pq_sys::PQresetPoll(self.ptr.as_ptr())
            } else {
                pq_sys::PQconnectPoll(self.ptr.as_ptr())
            }
        };
        // PQresetStart clears libpq's nonblocking flag, independently of the OS socket flags.
        if status == pq_sys::PostgresPollingStatusType::PGRES_POLLING_OK {
            // SAFETY: handshake finished and there are no queued application writes to flush.
            if unsafe { pq_sys::PQsetnonblocking(self.ptr.as_ptr(), 1) } != 0 {
                cold_path();
                return pq_sys::PostgresPollingStatusType::PGRES_POLLING_FAILED;
            }
        }
        status
    }

    pub(crate) fn reset_start(&mut self) -> Result<()> {
        // SAFETY: exclusive access; libpq retains its original connection parameters for reset.
        if unsafe { pq_sys::PQresetStart(self.ptr.as_ptr()) } == 0 {
            return Err(PgNotifyError::connection_failed("reset"));
        }
        Ok(())
    }
    pub(crate) fn send_query(&mut self, sql: &CStr) -> Result<()> {
        // SAFETY: live exclusively borrowed connection; sql is terminated and retained during call.
        if unsafe { pq_sys::PQsendQuery(self.ptr.as_ptr(), sql.as_ptr()) } == 0 {
            return Err(PgNotifyError::connection_failed("send query"));
        }
        Ok(())
    }
    #[inline]
    pub(crate) fn flush(&mut self) -> Result<bool> {
        // SAFETY: connection remains nonblocking and exclusively borrowed.
        let result = unsafe { pq_sys::PQflush(self.ptr.as_ptr()) };
        if unlikely(!matches!(result, 0 | 1)) {
            cold_path();
            return Err(PgNotifyError::connection_failed("flush"));
        }
        Ok(result == 0)
    }
    #[inline]
    pub(crate) fn consume(&mut self) -> Result<()> {
        // SAFETY: connection is nonblocking and exclusively borrowed; libpq owns its input buffer.
        if unlikely(unsafe { pq_sys::PQconsumeInput(self.ptr.as_ptr()) } == 0) {
            cold_path();
            return Err(PgNotifyError::connection_failed("consume input"));
        }
        Ok(())
    }
    #[inline]
    pub(crate) fn result(&mut self) -> Result<QueryProgress> {
        // SAFETY: exclusive live connection; PQisBusy prevents PQgetResult from waiting on input.
        if unsafe { pq_sys::PQisBusy(self.ptr.as_ptr()) } != 0 {
            return Ok(QueryProgress::Busy);
        }
        // SAFETY: no busy result remains; nonblocking PGconn owns a completed result or none.
        let Some(result) = NonNull::new(unsafe { pq_sys::PQgetResult(self.ptr.as_ptr()) }) else {
            return Ok(QueryProgress::Done);
        };
        // SAFETY: result was just returned by libpq and has not been cleared.
        let status = unsafe { pq_sys::PQresultStatus(result.as_ptr()) };
        let accepted = matches!(
            status,
            pq_sys::ExecStatusType::PGRES_COMMAND_OK
                | pq_sys::ExecStatusType::PGRES_TUPLES_OK
                | pq_sys::ExecStatusType::PGRES_EMPTY_QUERY
        );
        let error = if unlikely(!accepted) {
            // SAFETY: The result is live and exclusively owned until PQclear below.
            Some(unsafe { result_error(result) })
        } else {
            None
        };
        // SAFETY: The result has one owner and its error-field borrows have ended.
        unsafe { pq_sys::PQclear(result.as_ptr()) };
        match error {
            Some(error) => Err(error),
            None => Ok(QueryProgress::Result),
        }
    }

    #[inline]
    pub(crate) fn notification(&mut self) -> Option<NotifyGuard> {
        // SAFETY: exclusive live PGconn; a non-null PGnotify transfers ownership to the caller.
        NonNull::new(unsafe { pq_sys::PQnotifies(self.ptr.as_ptr()) }).map(|ptr| NotifyGuard {
            ptr,
            _affinity: PhantomData,
        })
    }
}
impl Drop for RawPgConn {
    fn drop(&mut self) {
        // SAFETY: this is the unique owner; no PGconn/socket borrow escapes this object.
        unsafe { pq_sys::PQfinish(self.ptr.as_ptr()) };
    }
}

/// Owns one libpq allocation; views cannot outlive this guard.
pub struct NotifyGuard {
    ptr: NonNull<pq_sys::PGnotify>,
    _affinity: PhantomData<Rc<()>>,
}
impl NotifyGuard {
    /// # Errors
    /// Rejects notifications whose channel or payload is not UTF-8.
    #[inline]
    pub fn notification(&self) -> Result<NotificationRef<'_>> {
        // SAFETY: the guard uniquely owns this live PGnotify until its Drop.
        let notify = unsafe { self.ptr.as_ref() };
        let channel = self.text(notify.relname)?;
        let payload = self.text(notify.extra)?;
        Ok(NotificationRef {
            backend_pid: notify.be_pid,
            channel_id: None,
            channel,
            payload,
        })
    }
    #[inline]
    fn text(&self, ptr: *const core::ffi::c_char) -> Result<&str> {
        if ptr.is_null() {
            return Ok("");
        }
        // SAFETY: only called on libpq notification fields; they are NUL-terminated within the guard.
        unsafe { CStr::from_ptr(ptr) }
            .to_str()
            .map_err(|_| PgNotifyError::codec_error("notification is not UTF-8"))
    }
}
impl Drop for NotifyGuard {
    fn drop(&mut self) {
        // SAFETY: PQnotifies transfers one allocation; views are lifetime-bound to this guard.
        unsafe { pq_sys::PQfreemem(self.ptr.as_ptr().cast()) };
    }
}

#[cold_sink]
fn invalid_url() -> PgNotifyError {
    PgNotifyError::invalid_config("database_url")
}
fn decimal(value: u32) -> CString {
    let mut bytes = value.to_string().into_bytes();
    bytes.push(0);
    // SAFETY: decimal digits contain no NUL and exactly one terminator was appended.
    unsafe { CString::from_vec_with_nul_unchecked(bytes) }
}
fn validate_endpoint(url: &CStr) -> Result<()> {
    let mut error = core::ptr::null_mut();
    // SAFETY: url is terminated; error is a valid output pointer owned by this call.
    let ptr = unsafe { pq_sys::PQconninfoParse(url.as_ptr(), &mut error) };
    if !error.is_null() {
        // SAFETY: libpq allocated error, and no aliases escape this function.
        unsafe { pq_sys::PQfreemem(error.cast()) };
    }
    if ptr.is_null() {
        return Err(invalid_url());
    }
    let mut result = Ok(());
    let mut host = None;
    let mut hostaddr = None;
    for index in 0..128 {
        // SAFETY: libpq returns a terminated option array, walked only through its final entry.
        let option = unsafe { &*ptr.add(index) };
        if option.keyword.is_null() {
            break;
        }
        if option.val.is_null() {
            continue;
        }
        // SAFETY: option fields are valid terminated strings until PQconninfoFree.
        let key = unsafe { CStr::from_ptr(option.keyword) }.to_bytes();
        // SAFETY: checked non-null option string belongs to the live option array.
        let value = unsafe { CStr::from_ptr(option.val) }
            .to_str()
            .map_err(|_| invalid_url());
        match (key, value) {
            (b"host", Ok(value)) => host = Some(value),
            (b"hostaddr", Ok(value)) => hostaddr = Some(value),
            (b"service", Ok(value)) if !value.is_empty() => {
                result = Err(PgNotifyError::invalid_config(
                    "service lookup is not nonblocking",
                ))
            }
            (_, Err(error)) => result = Err(error),
            _ => (),
        }
    }
    result = result.and_then(|()| validate_host_lists(host, hostaddr));
    // SAFETY: all borrowed option fields are no longer used, and ptr is uniquely owned.
    unsafe { pq_sys::PQconninfoFree(ptr) };
    result
}

fn validate_host_lists(host: Option<&str>, hostaddr: Option<&str>) -> Result<()> {
    let invalid_host = || PgNotifyError::invalid_config("numeric host or hostaddr required");
    if let Some(addresses) = hostaddr {
        if !addresses
            .split(',')
            .all(|address| address.parse::<std::net::IpAddr>().is_ok())
        {
            return Err(invalid_host());
        }
        if let Some(hosts) = host {
            // libpq pairs these lists by index; a numeric address prevents DNS only at that index.
            if hosts.split(',').count() != addresses.split(',').count() {
                return Err(PgNotifyError::invalid_config(
                    "host and hostaddr list lengths differ",
                ));
            }
            if hosts.split(',').any(str::is_empty) {
                return Err(invalid_host());
            }
        }
        return Ok(());
    }
    let hosts = host.ok_or_else(|| {
        PgNotifyError::invalid_config("explicit numeric host or Unix socket required")
    })?;
    if hosts.split(',').all(|host| {
        !host.is_empty()
            && (host.starts_with('/')
                || host.starts_with('@')
                || host.parse::<std::net::IpAddr>().is_ok())
    }) {
        Ok(())
    } else {
        Err(invalid_host())
    }
}

unsafe extern "C" fn ignore_notice(_: *mut core::ffi::c_void, _: *const pq_sys::PGresult) {}

#[cfg(test)]
#[path = "../tests/native/notification.rs"]
mod notification_tests;

/// # Safety
/// `result` must be a live PGresult retained until this function returns.
#[cold_sink]
unsafe fn result_error(result: NonNull<pq_sys::PGresult>) -> PgNotifyError {
    let mut state = *b"XXXXX";
    // SAFETY: The caller retains the live result and its terminated SQLSTATE field.
    let field =
        unsafe { pq_sys::PQresultErrorField(result.as_ptr(), i32::from(pq_sys::PG_DIAG_SQLSTATE)) };
    if !field.is_null() {
        // SAFETY: A non-null error field belongs to the still-live PGresult.
        let bytes = unsafe { CStr::from_ptr(field) }.to_bytes();
        if bytes.len() == 5 {
            state.copy_from_slice(bytes);
        }
    }
    PgNotifyError::protocol_error(SqlState(state))
}
