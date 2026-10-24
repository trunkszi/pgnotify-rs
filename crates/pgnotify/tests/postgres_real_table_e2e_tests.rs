//! Real PostgreSQL table writes through triggers, io_uring, and typed routing.

#[path = "support/postgres.rs"]
mod postgres;

use std::{
    error::Error,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use diesel::{
    Connection, PgConnection, QueryableByName, RunQueryDsl,
    connection::SimpleConnection,
    sql_types::{BigInt, Text},
};
use liburing::{Proactor, SubsystemId};
use pgnotify::{
    ChannelId, ListenerOptions, ListenerState, NotificationRef, NotifyHook, NotifyRouter as _,
    PgListener, PgNotifyError, ProactorPgListener,
};
use pgnotify_macros::{NotifyPayload, NotifyRouter};
use serde::{Deserialize, Serialize};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
static NEXT_SCHEMA: AtomicU64 = AtomicU64::new(0);
const WAIT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq, QueryableByName, Serialize, Deserialize, NotifyPayload)]
struct Order {
    #[diesel(sql_type = BigInt)]
    seq: i64,
    #[diesel(sql_type = Text)]
    symbol: String,
    #[diesel(sql_type = BigInt)]
    price_ticks: i64,
    #[diesel(sql_type = BigInt)]
    qty: i64,
    #[diesel(sql_type = Text)]
    status: String,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, NotifyPayload)]
#[serde(transparent)]
struct Boundary(String);

#[derive(Debug, PartialEq, Eq, NotifyRouter)]
enum Events {
    #[channel = "e2e_order_events"]
    Order(Order),
    #[channel = "e2e_bulk_events"]
    Bulk(Order),
    #[channel = "e2e_rollback_events"]
    Transaction(Order),
    #[channel = "e2e_explicit_events"]
    Explicit(Order),
    #[channel = "e2e_reconnect_events"]
    Recovery(Order),
    #[channel = "e2e_boundary_events"]
    Boundary(Boundary),
}

#[derive(Debug, PartialEq, Eq)]
enum Lifecycle {
    Subscribed,
    Connected,
    Disconnected,
    Reconnected(u64),
}

#[derive(Default)]
struct Capture {
    events: Vec<Events>,
    payload_lengths: Vec<usize>,
    decode_errors: Vec<PgNotifyError>,
    faults: Vec<PgNotifyError>,
    lifecycle: Vec<Lifecycle>,
    backend_pid: i32,
}

impl NotifyHook for Capture {
    fn on_notify(&mut self, notify: NotificationRef<'_>) {
        self.payload_lengths.push(notify.payload.len());
        match Events::decode_notification(notify) {
            Ok(Some(event)) => self.events.push(event),
            Ok(None) => self.decode_errors.push(PgNotifyError::CodecError {
                reason: "unexpected notification channel",
            }),
            Err(error) => self.decode_errors.push(error),
        }
    }
    fn on_subscribed(&mut self, _: ChannelId, _: &str) {
        self.lifecycle.push(Lifecycle::Subscribed);
    }
    fn on_connected(&mut self, backend_pid: i32) {
        self.backend_pid = backend_pid;
        self.lifecycle.push(Lifecycle::Connected);
    }
    fn on_reconnected(&mut self, epoch: u64, backend_pid: i32) {
        self.backend_pid = backend_pid;
        self.lifecycle.push(Lifecycle::Reconnected(epoch));
    }
    fn on_error(&mut self, error: PgNotifyError) {
        self.faults.push(error);
    }
    fn on_disconnect(&mut self, _: u64, _: PgNotifyError) {
        self.lifecycle.push(Lifecycle::Disconnected);
    }
}

struct Fixture {
    connection: PgConnection,
    schema: String,
    ring: Proactor,
    driver: ProactorPgListener<Capture>,
    closed: bool,
}

impl Fixture {
    fn new(channels: &[&str]) -> TestResult<Self> {
        let url = postgres::database_url()?;
        let mut connection = PgConnection::establish(&url)?;
        // NOTIFY channels are database-wide, so repeated test processes share a per-channel lock.
        diesel::sql_query("SELECT pg_advisory_lock(hashtext($1))")
            .bind::<Text, _>(channels[0])
            .execute(&mut connection)?;
        let schema = format!(
            "pgnotify_e2e_{}_{}",
            std::process::id(),
            NEXT_SCHEMA.fetch_add(1, Ordering::Relaxed)
        );
        connection.batch_execute(&format!(
            "CREATE SCHEMA {schema};
             CREATE TABLE {schema}.e2e_orders (
                 seq BIGINT PRIMARY KEY, symbol TEXT NOT NULL,
                 price_ticks BIGINT NOT NULL, qty BIGINT NOT NULL, status TEXT NOT NULL);
             CREATE FUNCTION {schema}.emit_order() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 PERFORM pg_notify('{}', json_build_object(
                     'seq', NEW.seq, 'symbol', NEW.symbol, 'price_ticks', NEW.price_ticks,
                     'qty', NEW.qty, 'status', NEW.status)::text);
                 RETURN NEW;
             END $$;
             CREATE TRIGGER order_changed AFTER INSERT OR UPDATE ON {schema}.e2e_orders
             FOR EACH ROW EXECUTE FUNCTION {schema}.emit_order();",
            channels[0]
        ))?;
        let options = ListenerOptions {
            notification_budget: 16,
            idle_ping_interval: None,
            reconnect_initial: Duration::from_millis(300),
            reconnect_max: Duration::from_millis(600),
            ..Default::default()
        };
        let mut listener = PgListener::new(options, Capture::default())?;
        for channel in channels {
            listener.subscribe(channel)?;
        }
        listener.connect(&url)?;
        let driver = ProactorPgListener::new(SubsystemId::new_unchecked(1), listener)?;
        let ring = Proactor::builder().entries(64).build()?;
        let mut fixture = Self {
            connection,
            schema,
            ring,
            driver,
            closed: false,
        };
        fixture.until(|fixture| fixture.driver.listener.state() == ListenerState::Listening)?;
        assert_eq!(fixture.driver.listener.epoch(), 1);
        assert_eq!(
            fixture.capture().lifecycle.last(),
            Some(&Lifecycle::Connected)
        );
        Ok(fixture)
    }

    fn capture(&self) -> &Capture {
        self.driver.listener.hook()
    }

    fn step(&mut self) -> TestResult {
        self.driver.flush(&mut self.ring)?;
        self.ring
            .wait_completions(1, Some(Duration::from_millis(10)), &mut self.driver)?;
        Ok(())
    }

    fn until(&mut self, condition: impl Fn(&Self) -> bool) -> TestResult {
        let deadline = Instant::now() + WAIT;
        while !condition(self) {
            if Instant::now() >= deadline {
                return Err(format!(
                    "listener timed out: {:?}, events={}, faults={:?}",
                    self.driver.listener.state(),
                    self.capture().events.len(),
                    self.capture().faults
                )
                .into());
            }
            self.step()?;
        }
        Ok(())
    }

    fn quiet(&mut self) -> TestResult {
        let deadline = Instant::now() + Duration::from_millis(100);
        while Instant::now() < deadline {
            self.step()?;
        }
        Ok(())
    }

    fn insert(&mut self, start: i64, end: i64) -> TestResult {
        insert_rows(&mut self.connection, &self.schema, start, end)?;
        Ok(())
    }

    fn rows_after(&mut self, seq: i64) -> TestResult<Vec<Order>> {
        Ok(diesel::sql_query(format!(
            "SELECT seq, symbol, price_ticks, qty, status FROM {}.e2e_orders
             WHERE seq > $1 ORDER BY seq",
            self.schema
        ))
        .bind::<BigInt, _>(seq)
        .load(&mut self.connection)?)
    }

    fn assert_clean(&self) {
        assert!(
            self.capture().decode_errors.is_empty(),
            "{:?}",
            self.capture().decode_errors
        );
        assert!(
            self.capture().faults.is_empty(),
            "{:?}",
            self.capture().faults
        );
    }

    fn close(&mut self) -> TestResult {
        self.driver.shutdown(&mut self.ring)?;
        assert!(!self.driver.has_pending_io());
        self.ring.shutdown()?;
        self.connection
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.schema))?;
        self.closed = true;
        Ok(())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if !self.closed
            && let Err(error) = self.close()
        {
            eprintln!("PostgreSQL E2E cleanup failed: {error}");
        }
    }
}

#[test]
fn e2e_real_table_trigger_and_proactor_router_round_trip() -> TestResult {
    let mut fixture = Fixture::new(&["e2e_order_events"])?;
    fixture.insert(1, 1)?;
    fixture.until(|f| !f.capture().events.is_empty())?;
    let first = fixture.rows_after(0)?;
    assert_eq!(first.len(), 1);
    assert_eq!(fixture.capture().events, [Events::Order(first[0].clone())]);
    fixture.connection.batch_execute(&format!(
        "UPDATE {}.e2e_orders SET price_ticks = 42000, qty = 7, status = 'filled' WHERE seq = 1",
        fixture.schema
    ))?;
    fixture.until(|f| f.capture().events.len() >= 2)?;
    let updated = fixture.rows_after(0)?;
    assert_eq!(updated[0].price_ticks, 42000);
    assert_eq!(updated[0].qty, 7);
    assert_eq!(updated[0].status, "filled");
    assert_eq!(
        fixture.capture().events[1],
        Events::Order(updated[0].clone())
    );
    fixture.insert(2, 2)?;
    fixture.until(|f| f.capture().events.len() >= 3)?;
    let both = fixture.rows_after(0)?;
    assert_eq!(both.len(), 2);
    assert_eq!(fixture.capture().events[2], Events::Order(both[1].clone()));
    fixture.connection.batch_execute(&format!(
        "DELETE FROM {}.e2e_orders WHERE seq = 2",
        fixture.schema
    ))?;
    fixture.quiet()?;
    assert_eq!(fixture.rows_after(0)?, updated);
    assert_eq!(fixture.capture().events.len(), 3);
    assert_eq!(fixture.driver.listener.metrics().notifications, 3);
    fixture.assert_clean();
    fixture.close()
}

fn insert_rows(
    connection: &mut PgConnection,
    schema: &str,
    start: i64,
    end: i64,
) -> diesel::QueryResult<usize> {
    diesel::sql_query(format!(
        "INSERT INTO {schema}.e2e_orders
         SELECT seq, 'BTCUSD', 1000 + seq, 2 * seq, 'new'
         FROM generate_series($1::bigint, $2::bigint) AS seq ORDER BY seq"
    ))
    .bind::<BigInt, _>(start)
    .bind::<BigInt, _>(end)
    .execute(connection)
}

#[derive(QueryableByName)]
struct Totals {
    #[diesel(sql_type = BigInt)]
    count: i64,
    #[diesel(sql_type = BigInt)]
    first: i64,
    #[diesel(sql_type = BigInt)]
    last: i64,
}

#[test]
fn e2e_high_volume_table_writes_zero_loss_and_strict_order() -> TestResult {
    use pgnotify::DieselNotifyExt;
    let mut fixture = Fixture::new(&["e2e_bulk_events", "e2e_boundary_events"])?;
    let schema = fixture.schema.clone();
    fixture
        .connection
        .transaction::<_, diesel::result::Error, _>(|connection| {
            for start in (1..=500).step_by(50) {
                insert_rows(connection, &schema, start, start + 49)?;
            }
            Ok(())
        })?;
    for seq in 501..=1000 {
        fixture.insert(seq, seq)?;
    }
    let boundary = "x".repeat(7997);
    fixture
        .connection
        .notify_event(&Events::Boundary(Boundary(boundary.clone())))?;
    fixture.until(|f| f.capture().events.len() >= 1001)?;
    fixture.quiet()?;
    assert_eq!(fixture.capture().events.len(), 1001);
    let received: Vec<Order> = fixture.capture().events[..1000]
        .iter()
        .map(|event| match event {
            Events::Bulk(order) => Ok(order.clone()),
            _ => Err("unexpected event in ordered table stream".into()),
        })
        .collect::<TestResult<_>>()?;
    let sequences: Vec<_> = received.iter().map(|order| order.seq).collect();
    let unique: std::collections::BTreeSet<_> = sequences.iter().copied().collect();
    let missing = (1..=1000).filter(|seq| !unique.contains(seq)).count();
    let duplicates = sequences.len() - unique.len();
    assert_eq!(missing, 0);
    assert_eq!(duplicates, 0);
    assert_eq!(sequences, (1..=1000).collect::<Vec<_>>());
    assert_eq!(received, fixture.rows_after(0)?);
    let totals = diesel::sql_query(format!(
        "SELECT COUNT(*) AS count, MIN(seq) AS first, MAX(seq) AS last FROM {}.e2e_orders",
        fixture.schema
    ))
    .get_result::<Totals>(&mut fixture.connection)?;
    assert_eq!((totals.count, totals.first, totals.last), (1000, 1, 1000));
    assert_eq!(
        fixture.capture().events[1000],
        Events::Boundary(Boundary(boundary))
    );
    assert_eq!(fixture.capture().payload_lengths[1000], 7999);
    assert_eq!(fixture.driver.listener.metrics().notifications, 1001);
    fixture.assert_clean();
    println!(
        "table audit: rows={}, received={}, missing={missing}, duplicates={duplicates}, ordered=true, boundary_bytes={}",
        totals.count,
        sequences.len(),
        fixture.capture().payload_lengths[1000]
    );
    fixture.close()
}

#[test]
fn e2e_transaction_rollback_emits_zero_notifications_while_commit_delivers() -> TestResult {
    use pgnotify::{DieselNotifyError, DieselNotifyExt, DieselNotifyResult};
    let mut fixture = Fixture::new(&["e2e_rollback_events", "e2e_explicit_events"])?;
    let schema = fixture.schema.clone();
    let rolled_back: DieselNotifyResult<()> = fixture.connection.transaction(|connection| {
        insert_rows(connection, &schema, 1, 1)?;
        connection.notify_event(&Events::Explicit(Order {
            seq: 1,
            symbol: "BTCUSD".into(),
            price_ticks: 1001,
            qty: 2,
            status: "new".into(),
        }))?;
        Err(diesel::result::Error::RollbackTransaction.into())
    });
    assert!(matches!(
        rolled_back,
        Err(DieselNotifyError::Diesel(
            diesel::result::Error::RollbackTransaction
        ))
    ));
    assert!(fixture.rows_after(0)?.is_empty());
    fixture.quiet()?;
    assert!(fixture.capture().events.is_empty());
    assert_eq!(fixture.driver.listener.metrics().notifications, 0);
    let committed = Order {
        seq: 2,
        symbol: "BTCUSD".into(),
        price_ticks: 1002,
        qty: 4,
        status: "new".into(),
    };
    fixture
        .connection
        .transaction::<_, DieselNotifyError, _>(|connection| {
            insert_rows(connection, &schema, 2, 2)?;
            connection.notify_event(&Events::Explicit(committed.clone()))?;
            Ok(())
        })?;
    fixture.until(|f| f.capture().events.len() >= 2)?;
    fixture.quiet()?;
    assert_eq!(
        fixture.rows_after(0)?.as_slice(),
        std::slice::from_ref(&committed)
    );
    assert_eq!(
        fixture.capture().events,
        [
            Events::Transaction(committed.clone()),
            Events::Explicit(committed)
        ]
    );
    assert_eq!(fixture.driver.listener.metrics().notifications, 2);
    fixture.assert_clean();
    fixture.close()
}

#[derive(QueryableByName)]
struct Terminated {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    terminated: bool,
}

#[test]
fn e2e_backend_kill_reconnect_listen_replay_and_table_reconciliation() -> TestResult {
    let mut fixture = Fixture::new(&["e2e_reconnect_events"])?;
    fixture.insert(1, 100)?;
    fixture.until(|f| f.capture().events.len() >= 100)?;
    let initial = fixture.rows_after(0)?;
    assert_eq!(
        initial.iter().map(|row| row.seq).collect::<Vec<_>>(),
        (1..=100).collect::<Vec<_>>()
    );
    assert_eq!(
        fixture.capture().events,
        initial
            .iter()
            .cloned()
            .map(Events::Recovery)
            .collect::<Vec<_>>()
    );
    let last_seen_seq = initial.last().ok_or("missing initial table rows")?.seq;
    let old_pid = fixture.capture().backend_pid;
    assert!(old_pid > 0);
    let mut admin = PgConnection::establish(&postgres::database_url()?)?;
    let killed = diesel::sql_query("SELECT pg_terminate_backend($1) AS terminated")
        .bind::<diesel::sql_types::Integer, _>(old_pid)
        .get_result::<Terminated>(&mut admin)?;
    assert!(killed.terminated);
    fixture.until(|f| f.driver.listener.state() == ListenerState::BackoffWait)?;
    assert_eq!(fixture.driver.listener.epoch(), 1);
    assert_eq!(
        fixture.capture().lifecycle.last(),
        Some(&Lifecycle::Disconnected)
    );
    // The listener has no worker thread: withholding ring dispatch keeps replay stopped during writes.
    fixture.insert(101, 110)?;
    assert_eq!(fixture.driver.listener.state(), ListenerState::BackoffWait);
    assert_eq!(fixture.capture().events.len(), 100);
    fixture.until(|f| {
        f.driver.listener.epoch() == 2 && f.driver.listener.state() == ListenerState::Listening
    })?;
    assert_ne!(fixture.capture().backend_pid, old_pid);
    assert_eq!(
        fixture.capture().lifecycle,
        [
            Lifecycle::Subscribed,
            Lifecycle::Connected,
            Lifecycle::Disconnected,
            Lifecycle::Subscribed,
            Lifecycle::Reconnected(2),
        ]
    );
    // Reconciliation begins only after the replay acknowledgment and epoch callback above.
    let reconciled = fixture.rows_after(last_seen_seq)?;
    assert_eq!(
        reconciled.iter().map(|row| row.seq).collect::<Vec<_>>(),
        (101..=110).collect::<Vec<_>>()
    );
    assert_eq!(fixture.capture().events.len(), 100);
    fixture.insert(111, 120)?;
    fixture.until(|f| f.capture().events.len() >= 110)?;
    fixture.quiet()?;
    assert_eq!(fixture.capture().events.len(), 110);
    let resumed = fixture.capture().events[100..]
        .iter()
        .map(|event| match event {
            Events::Recovery(order) => Ok(order.clone()),
            _ => Err("unexpected event in recovered table stream".into()),
        })
        .collect::<TestResult<Vec<_>>>()?;
    assert_eq!(
        resumed.iter().map(|row| row.seq).collect::<Vec<_>>(),
        (111..=120).collect::<Vec<_>>()
    );
    let mut combined = initial;
    combined.extend(reconciled.iter().cloned());
    combined.extend(resumed);
    assert_eq!(
        combined.iter().map(|row| row.seq).collect::<Vec<_>>(),
        (1..=120).collect::<Vec<_>>()
    );
    assert_eq!(combined, fixture.rows_after(0)?);
    assert!(fixture.capture().decode_errors.is_empty());
    assert_eq!(fixture.capture().faults.len(), 1);
    assert_eq!(fixture.driver.listener.metrics().reconnects, 1);
    assert_eq!(fixture.driver.listener.metrics().disconnects, 1);
    assert_eq!(fixture.driver.listener.metrics().notifications, 110);
    println!(
        "reconnect audit: live={}, reconciled={}, table_rows={}, final_epoch={}, missing_records=0, duplicate_records=0",
        fixture.capture().events.len(),
        reconciled.len(),
        combined.len(),
        fixture.driver.listener.epoch()
    );
    fixture.close()
}
