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

fn report_latency<T>(name: &str, mut operation: impl FnMut() -> T) -> Result<(), Box<dyn Error>> {
    let mut samples = Histogram::<u64>::new(3)?;
    for _ in 0..20_000 {
        let started = Instant::now();
        black_box(operation());
        samples.record(u64::try_from(started.elapsed().as_nanos())?.max(1))?;
    }
    println!(
        "{name}: p50={}ns p90={}ns p99={}ns p99.9={}ns",
        samples.value_at_quantile(0.50),
        samples.value_at_quantile(0.90),
        samples.value_at_quantile(0.99),
        samples.value_at_quantile(0.999),
    );
    Ok(())
}

fn measure<T>(
    c: &mut Criterion,
    name: &str,
    mut operation: impl FnMut() -> T,
) -> Result<(), Box<dyn Error>> {
    for _ in 0..1024 {
        black_box(operation());
    }
    let _preflight = without_allocations(&mut operation);
    report_latency(name, &mut operation)?;
    c.bench_function(name, |b| b.iter(&mut operation));
    Ok(())
}

fn transition_cycle(mut phase: pgnotify::ListenerPhase) -> pgnotify::ListenerPhase {
    use pgnotify::ListenerPhase as P;
    for _ in 0..5 {
        assert!(black_box(phase.is_active_session()));
        let current = black_box(phase);
        phase = black_box(match current {
            P::FlushingSubscriptions => current.after_flush(),
            P::AwaitingCommandAck => current.transition(P::Listening),
            P::Listening => current.transition(P::IdlePingFlushing),
            P::IdlePingFlushing => current.after_flush(),
            P::IdlePingAwaiting => current.transition(P::FlushingSubscriptions),
            _ => unreachable!("active-session benchmark cycle"),
        });
    }
    phase
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut channels = ChannelTable::with_capacity(1024)?;
    for index in 0..1024 {
        channels.subscribe(&format!("channel.{index:04}"))?;
    }
    let notify = notification();
    let heartbeat = NotificationRef {
        channel: "heartbeat",
        payload: "",
        ..notify
    };
    assert!(channels.lookup("channel.0512").is_some());
    assert_eq!(channels.lookup("channel.9999"), None);
    assert!(matches!(
        Feed::decode_notification(notify),
        Ok(Some(Feed::Quote(_)))
    ));
    assert_eq!(
        Feed::decode_notification(heartbeat),
        Ok(Some(Feed::Heartbeat))
    );
    let payload = Quote {
        ticks: 42000,
        quantity: 5,
    };
    let mut out = PayloadBuf::new();
    let mut c = Criterion::default().configure_from_args();
    measure(&mut c, "channel_table_lookup/hit", || {
        channels.lookup(black_box("channel.0512"))
    })?;
    measure(&mut c, "channel_table_lookup/miss", || {
        channels.lookup(black_box("channel.9999"))
    })?;
    measure(&mut c, "router_dispatch/json", || {
        Feed::decode_notification(black_box(notify))
    })?;
    measure(&mut c, "router_dispatch/unit", || {
        Feed::decode_notification(black_box(heartbeat))
    })?;
    measure(&mut c, "notification_ref_access", || {
        let notification = black_box(notify);
        black_box((
            notification.backend_pid,
            notification.channel_id,
            notification.channel.as_bytes(),
            notification.payload_bytes(),
        ))
    })?;
    measure(&mut c, "hsm_state_transition_cycle", || {
        transition_cycle(black_box(pgnotify::ListenerPhase::FlushingSubscriptions))
    })?;
    measure(&mut c, "payload_encode", || {
        payload.encode_payload(black_box(&mut out))
    })?;
    c.final_summary();
    Ok(())
}
