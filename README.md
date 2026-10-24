# pgnotify

A caller-driven PostgreSQL LISTEN/NOTIFY library built on non-blocking libpq.
`PgListener` owns its connection on one thread. The application supplies socket
readiness and timer expiry events; the library needs no async runtime or worker
thread. Linux applications can attach it to an existing io_uring Proactor.

## Workspace and features

| Path | Purpose |
| --- | --- |
| `crates/pgnotify` | Listener, bounded values, channel registry, and adapters |
| `crates/pgnotify-macros` | Typed payload and router derive macros |
| `crates/pgnotify/tests` | Protocol, lifecycle, adapter, and compile-contract tests |
| `crates/pgnotify/examples` | Caller-owned Proactor example |
| `crates/pgnotify/benches` | Routing, lookup, and encoding benchmarks |
| `xtask` | Workspace quality gates |

The `pgnotify` crate enables `uring`, `diesel`, and `sonic` by default. Disable
default features for the libpq listener alone, or select `sonic` for JSON payloads
without the adapters. `uring` uses `liburing-proactor`; `diesel` provides
synchronous PostgreSQL integration; `sonic` provides bounded JSON serialization.
The derives are supplied by the separate `pgnotify-macros` crate.

Builds use the toolchain in `rust-toolchain.toml` and bundled libpq through
`pq-sys`. Native builds require a C toolchain and CMake. Proactor execution also
requires a Linux environment that permits io_uring.

## Listener lifecycle

Construct `PgListener::new(options, hook)`, register desired channels with
`subscribe`, and start the connection with `connect`. Use a numeric database
address or explicit numeric `hostaddr` to avoid blocking hostname resolution.

Integrations using their own event loop read `poll_interest()` and
`timer_deadline()`, then call `on_event(ListenerEvent::Socket { ... })` or
`on_event(ListenerEvent::Timer { generation })`. Validate socket generations with
`owns_socket` before delivering readiness from a previously registered socket.
Schedule a timer generation once and deliver it when its duration expires;
re-reading the same deadline must not restart its timeout. A zero-duration
continuation schedules another bounded notification batch.

`NotifyHook` provides notification and lifecycle callbacks. `NotificationRef`
borrows libpq storage and expires when `on_notify` returns. Copy application data
inside the callback if it must outlive that call. Subscription acknowledgment
precedes readiness callbacks. On reconnect the listener replays desired channels
before `on_reconnected(epoch, backend_pid)` runs. Notifications sent during a
disconnection are lost; reconcile application state after an epoch change.
Faults call `on_error` followed by `on_disconnect` and enter bounded retry backoff.
`begin_shutdown` closes the core listener and invalidates pending generations.

## Channel registry and payload bounds

`ChannelTable::new()` defaults to 256 entries. `with_capacity` accepts 1..=65536.
`subscribe` and `register` validate names and return the existing ID for duplicate
registrations. `lookup`, `channel`, and `remove` expose name/ID lookup and removal;
IDs remain stable until removal and may then be reused.

Names contain 1..=63 UTF-8 bytes without NUL. Name lookup traverses at most 126
radix-16 trie edges, independent of the number of channels, without allocating.
Registration is a control-path operation and can allocate.

`PayloadBuf` holds at most 7999 committed bytes. JSON encoding uses reusable
stack storage, including private serializer scratch space. An exactly 7999-byte
encoded payload succeeds; larger output returns `PayloadTooLarge`. Its scratch
reservation size is distinct from committed output size. Allocation claims for
application lookup and encoding do not imply that libpq or Diesel never allocate.

## Typed notifications

Use Serde's `Serialize` and `Deserialize` derives on payload types, plus
`NotifyPayload` from `pgnotify-macros`. Select the `sonic` feature and include
`serde` with its `derive` feature. A router enum associates variants with channel
names and supports unit, single-field tuple, and single-field struct variants.

Add these dependencies to the application's `Cargo.toml`:

```toml
[dependencies]
pgnotify = { version = "0.1.0", default-features = false, features = ["sonic"] }
pgnotify-macros = { version = "0.1.0" }
serde = { version = "1", features = ["derive"] }
```

The derives access the codec through `pgnotify::sonic_api`.

```rust
use pgnotify::{NotificationRef, NotifyHook, NotifyPayload as _, NotifyRouter as _, PayloadBuf};
use pgnotify_macros::{NotifyPayload, NotifyRouter};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, NotifyPayload)]
struct Price { ticks: u64 }

#[derive(Debug, NotifyRouter)]
enum Events {
    #[channel = "prices"]
    Price(Price),
    #[channel = "heartbeat"]
    Heartbeat,
}

fn main() -> pgnotify::Result<()> {
    let mut out = PayloadBuf::new();
    Price { ticks: 42 }.encode_payload(&mut out)?;
    let event = Events::decode_notification(NotificationRef {
        backend_pid: 0,
        channel_id: None,
        channel: "prices",
        payload: out.as_str()?,
    })?;
    assert!(matches!(event, Some(Events::Price(Price { ticks: 42 }))));
    let mut hook = Events::router_hook(|result: pgnotify::Result<Events>| {
        match result {
            Ok(event) => println!("{event:?}"),
            Err(error) => eprintln!("{error}"),
        }
    });
    hook.on_notify(NotificationRef {
        backend_pid: 0, channel_id: None, channel: "heartbeat", payload: "",
    });
    Ok(())
}
```

`NotifyRouter` rejects invalid or duplicate channels during macro expansion. It
provides `CHANNELS`, quoted `LISTEN_BATCH_SQL`, `channel_name`,
`encode_event_payload`, and `decode_notification`. Dispatch uses bounded byte
matching without hashing; unknown channels return `Ok(None)`. Unit variants
require an empty payload. Invalid known payloads return an error.

`Events::router_hook` constructs `EventsRouterHook<R, F>`. Each enum gets a
separate hook name, so multiple routers can share one module. Its callback takes
`pgnotify::Result<R>`, receiving typed events and decoding/listener errors. The
adapter leaves `on_disconnect` empty so a single listener fault is delivered once.
The printing in this example is application policy, not a hot-path requirement.

## Proactor integration

`ProactorPgListener::new(subsystem, listener)` wraps a listener without owning the
ring. Use a distinct nonzero `SubsystemId` for every driver and keep each driver
on the same ring. Call `flush(&mut ring)` after changes, and route its completions
through `CompletionDispatcher`. Other protocols can share the ring when the
caller dispatches completions to their owning subsystem.

For a listener-only ring, `shutdown` drains outstanding work within a bounded
budget. Shared-ring owners use `begin_shutdown`, continue `flush` and completion
dispatch, and retain the driver until `has_pending_io()` is false. Cancellation
acknowledgments and target completions retire independently. Preserve the driver
and ring after a shutdown error so draining can resume.

The example listens on `events` for approximately 30 seconds:

```sh
DATABASE_URL='postgresql://127.0.0.1/pgnotify?sslmode=disable' \
  cargo run -p pgnotify --example proactor_listener --locked
```

## Diesel integration

`DieselNotifyExt` extends a caller-owned synchronous `diesel::PgConnection`.
`notify_channel` emits a validated UTF-8 payload; `notify_event` encodes a router
variant into `PayloadBuf` before issuing `pg_notify` with bound arguments.
Notifications follow PostgreSQL transaction commit semantics. `listen_channel`,
`unlisten_channel`, and `unlisten_all` quote channel identifiers.

`drain_notifications(budget, callback)` delivers at most the requested number of
currently available notifications and propagates callback errors. Its borrowed
views have no channel ID because the Diesel connection owns no `ChannelTable`.
Diesel methods may block and allocate. Use a separate `PgListener` for the
non-blocking borrowed receive path, and keep blocking CRUD off that event loop.
`DieselNotifyError` preserves codec/validation errors and original Diesel errors.

## Validation

Run all commands from the workspace root:

```sh
cargo xtask ci
cargo test -p pgnotify --test compile_contracts --locked
cargo test -p pgnotify --test router_macro_tests --locked
cargo test -p pgnotify --test postgres_real_table_e2e_tests --locked
cargo bench -p pgnotify --bench notify_dispatch_bench --no-run --locked
cargo bench -p pgnotify --bench notify_dispatch_bench --locked
```

`cargo xtask ci` checks formatting, runs Clippy with warnings denied for default,
core-only, and sonic-only configurations, tests those configurations, and builds
workspace rustdoc with warnings denied. The GitHub workflow runs the same gate
commands. Benchmarks run separately and report P50/P90/P99/P99.9 latency with
allocation assertions for dispatch, channel lookup, and numeric payload encoding.

The deterministic listener tests use a controlled loopback PostgreSQL protocol
peer. `postgres_real_table_e2e_tests` separately drives real PostgreSQL tables,
PL/pgSQL triggers, typed routing, and a real io_uring ring. Its four scenarios
cover INSERT/UPDATE payloads against queried rows, 1000 strictly ordered writes
without missing or duplicate notifications plus a 7999-byte payload, transaction
rollback isolation, and backend termination followed by subscription replay and
table reconciliation. DELETE is checked to emit no INSERT/UPDATE trigger event.
The reconnect audit distinguishes live notifications from rows recovered by SQL;
NOTIFY itself cannot recover messages sent while disconnected.

Both the table suite and all five live Diesel tests run without ignore markers.
They use only `PGNOTIFY_TEST_DATABASE_URL` to select an explicit test database.
The suite ignores `DATABASE_URL`. Without the test-specific override, the shared
cold-path fixture helper reuses or starts `pgnotify-pg-e2e` with
`postgres:18.6`, bound to `127.0.0.1:55433`, and waits for `pg_isready`. A healthy
container is never restarted or removed. Docker must be available for this
fallback; an explicit `PGNOTIFY_TEST_DATABASE_URL` avoids Docker discovery.
Use that override for an isolated test database: the suite creates/drops schemas
and terminates its listener backend.

```sh
PGNOTIFY_TEST_DATABASE_URL='postgresql://postgres:postgres@127.0.0.1:55433/postgres?sslmode=disable' \
  cargo test -p pgnotify --test postgres_real_table_e2e_tests --locked
```

Use a numeric address, never `localhost`, for the listener's non-blocking connect
path. The database role must permit schema/trigger creation and termination of
its listener backend. Each scenario uses an isolated schema and drops it after
draining ring operations; per-channel advisory locks prevent competing test
processes from mixing notifications. Callback vectors and synchronous SQL belong
to this test harness, not a production hot path. GitHub Actions supplies a healthy
`postgres:17` service at `127.0.0.1:5432` through the same environment variable.
See [AGENTS.md](AGENTS.md) for contributor contracts and change validation.
