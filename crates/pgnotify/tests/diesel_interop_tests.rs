#![cfg(feature = "diesel")]
#![allow(
    clippy::expect_used,
    reason = "test setup and assertions report fixture failures"
)]

#[path = "support/postgres.rs"]
mod postgres;

use diesel::{debug_query, pg::Pg, query_builder::QueryId};
use pgnotify::diesel_ext::pg_notify;

#[test]
fn unit_scenario_notify_sql_uses_cached_bind_parameters() {
    fn cached<T: QueryId>(_: &T) {
        assert!(T::HAS_STATIC_QUERY_ID);
    }
    let query = diesel::select(pg_notify("orders_with_\"quotes\"_and_spaces", "'\\payload"));
    cached(&query);
    let rendered = debug_query::<Pg, _>(&query).to_string();
    let (sql, _) = rendered.split_once(" -- binds:").expect("bound SQL");
    assert_eq!(sql, "SELECT pg_notify($1, $2)");
}

use pgnotify::{
    channel_table::ChannelTable,
    error::PgNotifyError,
    notify_ref::NotificationRef,
    types::{ChannelId, NotifyFrame, PG_NOTIFY_MAX_PAYLOAD_LEN, PayloadBuf},
};

#[cfg(feature = "sonic")]
use pgnotify::{error::Result as NotifyResult, hook::NotifyRouter};

#[test]
fn unit_scenario_notification_round_trip_preserves_borrowed_bytes_and_pid() {
    let source = NotificationRef {
        backend_pid: 513,
        channel_id: Some(ChannelId(7)),
        channel: "quoted\"channel",
        payload: "snowman \u{2603}",
    };
    let owned = diesel::pg::PgNotification::from(source);
    let borrowed = NotificationRef::from(&owned);
    assert_eq!(borrowed.backend_pid, 513);
    assert_eq!(borrowed.channel, source.channel);
    assert_eq!(borrowed.payload, source.payload);
    assert_eq!(borrowed.channel_id, None);
    assert_eq!(borrowed.payload_bytes().as_ptr(), owned.payload.as_ptr());
    assert!(matches!(
        NotifyFrame::<32>::try_from(&owned),
        Err(PgNotifyError::CodecError {
            reason: "unregistered channel"
        })
    ));
    let mut channels = ChannelTable::new();
    let id = channels.subscribe(source.channel).expect("valid channel");
    let frame = NotifyFrame::<32>::try_from((&owned, &channels)).expect("registered frame");
    assert_eq!(frame.channel_id, id);
    assert_eq!(frame.backend_pid, 513);
    assert_eq!(frame.channel.as_str(), source.channel);
    assert_eq!(
        &frame.payload[..usize::from(frame.payload_len)],
        source.payload_bytes()
    );
    assert!(matches!(
        NotifyFrame::<1>::try_from((&owned, &channels)),
        Err(PgNotifyError::PayloadTooLarge { len: 11, max: 1 })
    ));
    let invalid = diesel::pg::PgNotification {
        process_id: 1,
        channel: "bad\0channel".into(),
        payload: String::new(),
    };
    assert!(matches!(
        NotifyFrame::<32>::try_from(&invalid),
        Err(PgNotifyError::InvalidChannelName { .. })
    ));
}

use diesel::{Connection, connection::SimpleConnection};
use pgnotify::diesel_ext::{DieselNotifyError, DieselNotifyExt, DieselNotifyResult};

fn connection() -> PgConnection {
    let url = postgres::database_url().expect("PostgreSQL fixture readiness");
    PgConnection::establish(&url).expect("local PostgreSQL")
}

use diesel::PgConnection;

#[test]
fn integration_lifecycle_notifications_follow_transaction_commit_and_rollback()
-> DieselNotifyResult<()> {
    let mut receiver = connection();
    let mut sender = connection();
    let channel = "t03_atomic_\"quoted\"_channel";
    receiver.listen_channel(channel)?;
    sender.batch_execute("CREATE TEMP TABLE t03_crud (id integer PRIMARY KEY)")?;
    sender.transaction::<_, DieselNotifyError, _>(|sender| {
        sender.batch_execute("INSERT INTO t03_crud VALUES (1)")?;
        sender.notify_channel(channel, "committed")?;
        assert_eq!(receiver.drain_notifications(8, |_| Ok(()))?, 0);
        Ok(())
    })?;
    receiver.batch_execute("SELECT 1")?;
    let mut messages = Vec::new();
    assert_eq!(
        receiver.drain_notifications(8, |n| {
            messages.push((n.channel.to_owned(), n.payload.to_owned()));
            Ok(())
        })?,
        1
    );
    assert_eq!(messages, [(channel.to_owned(), "committed".to_owned())]);
    let rolled_back: DieselNotifyResult<()> = sender.transaction(|sender| {
        sender.batch_execute("INSERT INTO t03_crud VALUES (2)")?;
        sender.notify_channel(channel, "rolled back")?;
        Err(diesel::result::Error::RollbackTransaction.into())
    });
    assert!(matches!(
        rolled_back,
        Err(DieselNotifyError::Diesel(
            diesel::result::Error::RollbackTransaction
        ))
    ));
    receiver.batch_execute("SELECT 1")?;
    assert_eq!(receiver.drain_notifications(8, |_| Ok(()))?, 0);
    sender.notify_channel(channel, "after rollback")?;
    receiver.batch_execute("SELECT 1")?;
    assert_eq!(receiver.drain_notifications(8, |_| Ok(()))?, 1);
    receiver.unlisten_channel(channel)?;
    sender.notify_channel(channel, "not subscribed")?;
    receiver.batch_execute("SELECT 1")?;
    assert_eq!(receiver.drain_notifications(8, |_| Ok(()))?, 0);
    receiver.listen_channel(channel)?;
    receiver.unlisten_all()?;
    sender.notify_channel(channel, "unlisten all")?;
    receiver.batch_execute("SELECT 1")?;
    assert_eq!(receiver.drain_notifications(8, |_| Ok(()))?, 0);
    Ok(())
}

#[test]
fn integration_scenario_rejected_input_never_executes_sql_or_aborts_transaction()
-> DieselNotifyResult<()> {
    use diesel::connection::InstrumentationEvent;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let mut sender = connection();
    let starts = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&starts);
    sender.set_instrumentation(move |event: InstrumentationEvent<'_>| {
        if matches!(event, InstrumentationEvent::StartQuery { .. }) {
            observed.fetch_add(1, Ordering::Relaxed);
        }
    });
    sender.transaction::<_, DieselNotifyError, _>(|sender| {
        starts.store(0, Ordering::Relaxed);
        assert!(matches!(
            sender.notify_channel("t03_bounds", &"x".repeat(8000)),
            Err(DieselNotifyError::Notify(PgNotifyError::PayloadTooLarge {
                len: 8000,
                max: 7999
            }))
        ));
        for name in [
            String::new(),
            "bad\0channel".into(),
            "x".repeat(64),
            "\u{e9}".repeat(32),
        ] {
            assert!(matches!(
                sender.notify_channel(&name, "ok"),
                Err(DieselNotifyError::Notify(
                    PgNotifyError::InvalidChannelName { .. }
                ))
            ));
            assert!(sender.listen_channel(&name).is_err());
            assert!(sender.unlisten_channel(&name).is_err());
        }
        assert!(matches!(
            sender.notify_channel("t03_bounds", "a\0b"),
            Err(DieselNotifyError::Notify(PgNotifyError::CodecError { .. }))
        ));
        assert_eq!(starts.load(Ordering::Relaxed), 0);
        sender.batch_execute("SELECT 1")?;
        sender.notify_channel("t03_bounds", &"x".repeat(PG_NOTIFY_MAX_PAYLOAD_LEN))?;
        sender.notify_channel(&"x".repeat(63), "")?;
        sender.notify_channel(&format!("{}x", "\u{e9}".repeat(31)), "utf8")?;
        Ok(())
    })?;
    Ok(())
}

#[test]
fn integration_scenario_prepared_notify_is_reused_and_query_errors_preserved()
-> DieselNotifyResult<()> {
    use diesel::connection::InstrumentationEvent;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let mut sender = connection();
    let cached = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&cached);
    sender.set_instrumentation(move |event: InstrumentationEvent<'_>| {
        if let InstrumentationEvent::CacheQuery { sql, .. } = event {
            assert_eq!(sql, "SELECT pg_notify($1, $2)");
            observed.fetch_add(1, Ordering::Relaxed);
        }
    });
    sender.notify_channel("t03_cache_a", "first")?;
    sender.notify_channel("t03_cache_b", "second")?;
    assert_eq!(cached.load(Ordering::Relaxed), 1);
    sender.batch_execute("BEGIN")?;
    assert!(sender.batch_execute("SELECT 1 / 0").is_err());
    assert!(matches!(
        sender.notify_channel("t03_cache_a", "denied"),
        Err(DieselNotifyError::Diesel(
            diesel::result::Error::DatabaseError(_, _)
        ))
    ));
    sender.batch_execute("ROLLBACK")?;
    sender.notify_channel("t03_cache_a", "recovered")?;
    Ok(())
}

#[test]
fn integration_scenario_drain_budget_and_callback_failure_preserve_later_notifications()
-> DieselNotifyResult<()> {
    let mut receiver = connection();
    let mut sender = connection();
    receiver.listen_channel("t03_budget")?;
    sender.transaction::<_, DieselNotifyError, _>(|sender| {
        for payload in ["one", "two", "three"] {
            sender.notify_channel("t03_budget", payload)?;
        }
        Ok(())
    })?;
    receiver.batch_execute("SELECT 1")?;
    assert_eq!(
        receiver.drain_notifications(0, |_| panic!("zero budget polled"))?,
        0
    );
    let mut messages = Vec::new();
    assert_eq!(
        receiver.drain_notifications(1, |n| {
            messages.push(n.payload.to_owned());
            Ok(())
        })?,
        1
    );
    assert_eq!(messages, ["one"]);
    let failure = PgNotifyError::CodecError {
        reason: "consumer rejected",
    };
    assert!(
        matches!(receiver.drain_notifications(1, |_| Err(failure)), Err(DieselNotifyError::Notify(error)) if error == failure)
    );
    assert_eq!(
        receiver.drain_notifications(1, |n| {
            messages.push(n.payload.to_owned());
            Ok(())
        })?,
        1
    );
    assert_eq!(messages, ["one", "three"]);
    assert_eq!(receiver.drain_notifications(1, |_| Ok(()))?, 0);
    Ok(())
}

#[cfg(feature = "sonic")]
mod typed {
    use super::*;
    use pgnotify::hook::NotifyPayload;
    use std::io::Write;

    struct Event(sonic_rs::Value);

    impl NotifyPayload for Event {
        fn encode_payload(&self, out: &mut PayloadBuf) -> NotifyResult<()> {
            out.clear();
            sonic_rs::to_writer(&mut *out, &self.0).map_err(|_| {
                if out.overflowed() {
                    PgNotifyError::PayloadTooLarge {
                        len: 8000,
                        max: 7999,
                    }
                } else {
                    PgNotifyError::CodecError {
                        reason: "json encoding",
                    }
                }
            })
        }

        fn decode_payload(bytes: &[u8]) -> NotifyResult<Self> {
            sonic_rs::from_slice(bytes)
                .map(Self)
                .map_err(|_| PgNotifyError::CodecError {
                    reason: "json decoding",
                })
        }
    }

    impl NotifyRouter for Event {
        const CHANNELS: &'static [&'static str] = &["t03_typed"];
        const LISTEN_BATCH_SQL: &'static str = "LISTEN \"t03_typed\";";
        fn channel_name(&self) -> &'static str {
            "t03_typed"
        }
        fn encode_event_payload(&self, out: &mut PayloadBuf) -> NotifyResult<()> {
            self.encode_payload(out)
        }
        fn decode_notification(notify: NotificationRef<'_>) -> NotifyResult<Option<Self>> {
            if notify.channel == "t03_typed" {
                Self::decode_payload(notify.payload_bytes()).map(Some)
            } else {
                Ok(None)
            }
        }
    }

    #[test]
    fn unit_scenario_json_exact_payload_limit_and_overflow() -> NotifyResult<()> {
        let mut out = PayloadBuf::new();
        out.write_all(b"stale").expect("small initial write");
        let event = Event(sonic_rs::json!("x".repeat(7997)));
        event.encode_payload(&mut out)?;
        assert_eq!(out.len(), 7999);
        assert_eq!(out.as_bytes()[0], b'"');
        assert_eq!(out.as_bytes()[7998], b'"');
        assert!(!out.overflowed());
        let decoded = Event::decode_payload(out.as_bytes())?;
        assert_eq!(decoded.0, event.0);
        assert!(matches!(
            Event(sonic_rs::json!("x".repeat(7998))).encode_payload(&mut out),
            Err(PgNotifyError::PayloadTooLarge {
                len: 8000,
                max: 7999
            })
        ));
        assert!(out.overflowed());
        assert!(matches!(
            Event::decode_payload(b"not json"),
            Err(PgNotifyError::CodecError { .. })
        ));
        Ok(())
    }

    #[test]
    fn integration_scenario_typed_event_round_trip_and_rejection_before_sql()
    -> DieselNotifyResult<()> {
        use diesel::connection::InstrumentationEvent;
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        let mut receiver = connection();
        let mut sender = connection();
        receiver.listen_channel("t03_typed")?;
        let event = Event(sonic_rs::json!({"id":23,"text":"quoted '\\ value"}));
        sender.notify_event(&event)?;
        receiver.batch_execute("SELECT 1")?;
        assert_eq!(
            receiver.drain_notifications(8, |notification| {
                let decoded = Event::decode_notification(notification)?.expect("known channel");
                assert_eq!(decoded.0, event.0);
                Ok(())
            })?,
            1
        );
        let queries = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&queries);
        sender.set_instrumentation(move |event: InstrumentationEvent<'_>| {
            if matches!(event, InstrumentationEvent::StartQuery { .. }) {
                observed.fetch_add(1, Ordering::Relaxed);
            }
        });
        assert!(matches!(
            sender.notify_event(&Event(sonic_rs::json!("x".repeat(7998)))),
            Err(DieselNotifyError::Notify(PgNotifyError::PayloadTooLarge {
                len: 8000,
                max: 7999
            }))
        ));
        assert_eq!(queries.load(Ordering::Relaxed), 0);
        sender.notify_event(&Event(sonic_rs::json!("x".repeat(7997))))?;
        receiver.batch_execute("SELECT 1")?;
        assert_eq!(
            receiver.drain_notifications(8, |n| {
                assert_eq!(n.payload.len(), 7999);
                Ok(())
            })?,
            1
        );
        Ok(())
    }
}

#[test]
fn unit_scenario_payload_buffer_keeps_committed_length_on_overflow() {
    use std::io::Write;
    let mut buffer = PayloadBuf::new();
    buffer
        .write_all(&[b'x'; 7999])
        .expect("exact payload bound");
    assert_eq!(buffer.len(), 7999);
    assert!(buffer.write_all(b"x").is_err());
    assert_eq!(buffer.len(), 7999);
    assert!(buffer.overflowed());
    assert_eq!(buffer.as_bytes(), &[b'x'; 7999]);
    buffer.clear();
    assert!(buffer.is_empty());
    assert!(!buffer.overflowed());
}

#[cfg(feature = "sonic")]
mod allocation {
    use super::*;
    use std::{
        alloc::{GlobalAlloc, Layout, System},
        cell::Cell,
    };
    thread_local! { static COUNT: Cell<Option<usize>> = const { Cell::new(None) }; }
    struct CountingAllocator;
    // SAFETY: allocation and deallocation forward to System without changing layouts or pointers.
    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            COUNT.with(|count| {
                if let Some(value) = count.get() {
                    count.set(Some(value + 1));
                }
            });
            // SAFETY: the caller supplies a valid allocation layout, forwarded unchanged.
            unsafe { System.alloc(layout) }
        }
        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            // SAFETY: the pointer and layout came from this allocator's System allocation.
            unsafe { System.dealloc(pointer, layout) }
        }
    }
    #[global_allocator]
    static ALLOCATOR: CountingAllocator = CountingAllocator;

    #[test]
    fn unit_scenario_json_encode_allocates_no_heap_buffer() -> NotifyResult<()> {
        let value = sonic_rs::json!({"text":"x".repeat(7000),"id":12});
        let mut buffer = PayloadBuf::new();
        COUNT.with(|count| count.set(Some(0)));
        let result = sonic_rs::to_writer(&mut buffer, &value);
        let allocations = COUNT
            .with(|count| count.replace(None))
            .expect("active allocation probe");
        result.map_err(|_| PgNotifyError::CodecError {
            reason: "json encode",
        })?;
        assert_eq!(allocations, 0);
        assert!(buffer.len() > 7000);
        Ok(())
    }
}
