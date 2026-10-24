//! Caller-thread routing and channel lookup latency with allocation checks.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::error::Error;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use criterion::Criterion;
use hdrhistogram::Histogram;
use pgnotify::{ChannelTable, NotificationRef, NotifyPayload as _, NotifyRouter as _, PayloadBuf};
use pgnotify_macros::{NotifyPayload, NotifyRouter};
use serde::{Deserialize, Serialize};

thread_local! {
    static TRACK_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
}
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

struct CountingAllocator;

fn count_allocation() {
    if matches!(TRACK_ALLOCATIONS.try_with(Cell::get), Ok(true)) {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    }
}

// SAFETY: all allocation operations preserve the System allocator's contracts.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_allocation();
        // SAFETY: layout is supplied by GlobalAlloc's caller unchanged.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_allocation();
        // SAFETY: layout is supplied by GlobalAlloc's caller unchanged.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: ptr and layout describe the allocation from this System wrapper.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count_allocation();
        // SAFETY: the caller supplies a live allocation and valid replacement size.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct AllocationScope;

impl AllocationScope {
    fn enter() -> Self {
        TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
        Self
    }
}

impl Drop for AllocationScope {
    fn drop(&mut self) {
        TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));
    }
}

fn without_allocations<T>(operation: impl FnOnce() -> T) -> T {
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    let guard = AllocationScope::enter();
    let result = operation();
    drop(guard);
    assert_eq!(
        ALLOCATIONS.load(Ordering::Relaxed),
        before,
        "hot path allocated"
    );
    result
}

#[derive(Debug, PartialEq, Serialize, Deserialize, NotifyPayload)]
struct Quote {
    ticks: u64,
    quantity: u32,
}

#[derive(Debug, PartialEq, NotifyRouter)]
enum Feed {
    #[channel = "market.quotes"]
    Quote(Quote),
    #[channel = "market.trades"]
    Trade(Quote),
    #[channel = "heartbeat"]
    Heartbeat,
}

fn notification() -> NotificationRef<'static> {
    NotificationRef {
        backend_pid: 77,
        channel_id: None,
        channel: "market.quotes",
        payload: r#"{"ticks":42000,"quantity":5}"#,
    }
}

fn report_latency(name: &str, mut operation: impl FnMut()) -> Result<(), Box<dyn Error>> {
    for _ in 0..1024 {
        operation();
    }
    let mut samples = Histogram::<u64>::new(3)?;
    for _ in 0..20_000 {
        let allocations = ALLOCATIONS.load(Ordering::Relaxed);
        let guard = AllocationScope::enter();
        let started = Instant::now();
        operation();
        let elapsed = u64::try_from(started.elapsed().as_nanos())?;
        drop(guard);
        assert_eq!(
            ALLOCATIONS.load(Ordering::Relaxed),
            allocations,
            "hot path allocated"
        );
        samples.record(elapsed.max(1))?;
    }
    println!(
        "{name}: p50={}ns p90={}ns p99={}ns p99.9={}ns allocations=0",
        samples.value_at_quantile(0.50),
        samples.value_at_quantile(0.90),
        samples.value_at_quantile(0.99),
        samples.value_at_quantile(0.999),
    );
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut channels = ChannelTable::with_capacity(1024)?;
    for index in 0..1024 {
        channels.subscribe(&format!("channel.{index:04}"))?;
    }
    let expected = channels.lookup("channel.0512");
    assert!(expected.is_some());
    let notify = notification();
    let payload = Quote {
        ticks: 42000,
        quantity: 5,
    };
    let mut out = PayloadBuf::new();

    report_latency("notify_dispatch", || {
        let decoded = Feed::decode_notification(black_box(notify));
        assert!(matches!(
            decoded,
            Ok(Some(Feed::Quote(Quote {
                ticks: 42000,
                quantity: 5
            })))
        ));
        assert!(black_box(decoded).is_ok());
    })?;
    report_latency("channel_lookup", || {
        assert_eq!(
            black_box(channels.lookup(black_box("channel.0512"))),
            expected
        );
    })?;
    report_latency("payload_encode", || {
        assert!(black_box(payload.encode_payload(black_box(&mut out))).is_ok());
    })?;

    let mut criterion = Criterion::default().configure_from_args();
    criterion.bench_function("notify_dispatch", |bencher| {
        bencher.iter(|| {
            let decoded = without_allocations(|| Feed::decode_notification(black_box(notify)));
            assert!(decoded.is_ok());
            assert!(black_box(decoded).is_ok());
        });
    });
    criterion.bench_function("channel_lookup", |bencher| {
        bencher.iter(|| {
            black_box(without_allocations(|| {
                channels.lookup(black_box("channel.0512"))
            }));
        });
    });
    criterion.bench_function("payload_encode", |bencher| {
        bencher.iter(|| {
            let encoded = without_allocations(|| payload.encode_payload(black_box(&mut out)));
            assert!(encoded.is_ok());
        });
    });
    criterion.final_summary();
    Ok(())
}
