use pgnotify::{ChannelId, ChannelTable, PgNotifyError};

#[test]
fn channel_registration_is_idempotent_and_ids_are_stable() -> pgnotify::Result<()> {
    let mut table = ChannelTable::with_capacity(2)?;
    let first = table.subscribe("orders")?;
    assert_eq!(first, table.subscribe("orders")?);
    let second = table.subscribe("Quotes")?;
    assert_ne!(first, second);
    assert_eq!(table.lookup("orders"), Some(first));
    assert_eq!(table.channel(second).map(|s| s.as_str()), Some("Quotes"));
    assert!(matches!(
        table.subscribe("third"),
        Err(PgNotifyError::ChannelTableFull { .. })
    ));
    table.remove(first)?;
    assert_eq!(table.lookup("orders"), None);
    assert_eq!(table.channel(second).map(|s| s.as_str()), Some("Quotes"));
    assert!(table.subscribe("third").is_ok());
    assert!(matches!(
        table.remove(ChannelId(500)),
        Err(PgNotifyError::UnknownChannelId(_))
    ));
    Ok(())
}

#[test]
fn payload_limits_are_committed_lengths_not_serializer_scratch()
-> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write;
    let mut out = pgnotify::PayloadBuf::new();
    out.write_all(&[b'x'; 7999])?;
    assert_eq!(out.len(), 7999);
    assert!(out.write_all(b"x").is_err());
    assert_eq!(out.len(), 7999);
    assert!(out.overflowed());
    out.clear();
    #[cfg(feature = "sonic")]
    {
        out.encode_json(&"x".repeat(7997))?;
        assert_eq!(out.len(), 7999);
        assert!(matches!(
            out.encode_json(&"x".repeat(7998)),
            Err(PgNotifyError::PayloadTooLarge { max: 7999, .. })
        ));
    }
    Ok(())
}

#[test]
fn channel_validation_and_recycling_preserve_utf8_and_shared_prefixes() -> pgnotify::Result<()> {
    assert!(matches!(
        ChannelTable::with_capacity(0),
        Err(PgNotifyError::InvalidConfig {
            reason: "channel_capacity"
        })
    ));
    assert!(ChannelTable::with_capacity(65537).is_err());
    let mut table = ChannelTable::with_capacity(3)?;
    assert!(table.subscribe("").is_err());
    assert!(table.subscribe("nul\0channel").is_err());
    assert!(table.subscribe(&"x".repeat(64)).is_err());
    let prefix = table.subscribe("a")?;
    let longer = table.subscribe("ab")?;
    let utf8 = table.subscribe("quotes_\u{1f680}")?;
    table.remove(longer)?;
    assert_eq!(table.lookup("a"), Some(prefix));
    assert_eq!(table.lookup("quotes_\u{1f680}"), Some(utf8));
    for i in 0..1000 {
        let name = format!("channel_{i}");
        let id = table.subscribe(&name)?;
        assert_eq!(table.lookup(&name), Some(id));
        table.remove(id)?;
        assert_eq!(table.lookup(&name), None);
    }
    Ok(())
}

#[test]
fn frame_conversion_requires_registered_channel_and_preserves_bounds() -> pgnotify::Result<()> {
    let notify = pgnotify::NotificationRef {
        backend_pid: 42,
        channel_id: None,
        channel: "orders",
        payload: "ready",
    };
    assert!(matches!(
        notify.to_frame::<32>(),
        Err(PgNotifyError::CodecError {
            reason: "unregistered channel"
        })
    ));
    let notify = pgnotify::NotificationRef {
        channel_id: Some(ChannelId(3)),
        ..notify
    };
    let frame = notify.to_frame::<5>()?;
    assert_eq!(frame.backend_pid, 42);
    assert_eq!(frame.payload, *b"ready");
    assert!(matches!(
        notify.to_frame::<4>(),
        Err(PgNotifyError::PayloadTooLarge { len: 5, max: 4 })
    ));
    assert!(!pgnotify::validate_channel_name_bytes(&[255]));
    Ok(())
}

struct CountingAllocator;
thread_local! {
    static COUNT_ALLOCATIONS: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };
    static ALLOCATIONS: core::cell::Cell<usize> = const { core::cell::Cell::new(0) };
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
fn record_allocation() {
    if COUNT_ALLOCATIONS.try_with(core::cell::Cell::get) == Ok(true) {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
    }
}
// SAFETY: all allocations and frees delegate unchanged pointers/layouts to the system allocator.
unsafe impl std::alloc::GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: caller supplies the allocator's required nonzero valid layout.
        unsafe { std::alloc::System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        // SAFETY: caller returns an allocation produced with the same system allocator and layout.
        unsafe { std::alloc::System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: std::alloc::Layout, size: usize) -> *mut u8 {
        record_allocation();
        // SAFETY: caller supplies a live system allocation and valid replacement size.
        unsafe { std::alloc::System.realloc(ptr, layout, size) }
    }
}

#[test]
fn channel_lookup_and_successful_payload_encoding_allocate_no_heap() -> pgnotify::Result<()> {
    let mut table = ChannelTable::new();
    let id = table.subscribe("orders")?;
    let mut out = pgnotify::PayloadBuf::new();
    ALLOCATIONS.set(0);
    COUNT_ALLOCATIONS.set(true);
    for _ in 0..100 {
        let found = std::hint::black_box(&table).lookup(std::hint::black_box("orders"));
        std::hint::black_box(found);
    }
    #[cfg(feature = "sonic")]
    let result = out.encode_json(&"filled");
    #[cfg(not(feature = "sonic"))]
    let result = out.extend_from_slice(b"filled");
    COUNT_ALLOCATIONS.set(false);
    result?;
    assert_eq!(ALLOCATIONS.get(), 0);
    assert_eq!(table.lookup("orders"), Some(id));
    Ok(())
}
