use super::*;
fn guard(channel: Option<&[u8]>, payload: Option<&[u8]>) -> Result<NotifyGuard> {
    let header = core::mem::size_of::<pq_sys::PGnotify>();
    let channel_len = channel.map_or(0, |bytes| bytes.len() + 1);
    let payload_len = payload.map_or(0, |bytes| bytes.len() + 1);
    // SAFETY: calloc provides aligned initialized storage for the POD header and both C strings.
    let raw =
        unsafe { libc::calloc(1, header + channel_len + payload_len) }.cast::<pq_sys::PGnotify>();
    let ptr = NonNull::new(raw).ok_or(PgNotifyError::ConnectionFailed {
        phase: "test allocation",
    })?;
    // SAFETY: raw owns an aligned initialized PGnotify, followed by the allocated extents above.
    unsafe {
        (*raw).be_pid = 42;
        let mut at = raw.cast::<u8>().add(header);
        if let Some(channel) = channel {
            core::ptr::copy_nonoverlapping(channel.as_ptr(), at, channel.len());
            (*raw).relname = at.cast();
            at = at.add(channel_len);
        }
        if let Some(payload) = payload {
            core::ptr::copy_nonoverlapping(payload.as_ptr(), at, payload.len());
            (*raw).extra = at.cast();
        }
    }
    // The libpq PQfreemem contract accepts storage from the same C malloc/free heap.
    Ok(NotifyGuard {
        ptr,
        _affinity: PhantomData,
    })
}
#[test]
fn null_notification_fields_are_empty() -> Result<()> {
    let owned = guard(None, None)?;
    let view = owned.notification()?;
    assert_eq!((view.channel, view.payload), ("", ""));
    Ok(())
}
#[test]
fn invalid_notification_utf8_is_rejected() -> Result<()> {
    assert!(
        guard(Some(&[0xff]), Some(b"valid"))?
            .notification()
            .is_err()
    );
    assert!(
        guard(Some(b"valid"), Some(&[0xff]))?
            .notification()
            .is_err()
    );
    Ok(())
}
#[test]
fn notification_c_strings_stop_at_nul() -> Result<()> {
    let owned = guard(Some(b"channel\0tail"), Some(b"payload\0tail"))?;
    let view = owned.notification()?;
    assert_eq!((view.channel, view.payload), ("channel", "payload"));
    Ok(())
}
#[test]
fn notification_guard_unwinds_after_borrow() {
    for _ in 0..100 {
        let result = std::panic::catch_unwind(|| -> Result<()> {
            let owned = guard(Some(b"channel"), Some(b"payload"))?;
            let view = owned.notification()?;
            assert_eq!(view.backend_pid, 42);
            panic!("callback failure");
        });
        assert!(result.is_err());
    }
}
