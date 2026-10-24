# pgnotify Contributor Guide

## Workspace

Run Cargo commands from this directory. The root `Cargo.toml` owns workspace
members, shared dependencies, lint policy, and build profiles. Keep dependency
entries as single-line inline tables inherited from `[workspace.dependencies]`.
The selected toolchain and components are defined in `rust-toolchain.toml`.

- `crates/pgnotify`: libpq connection ownership, listener state machine, channel
  registry, payload storage, Diesel helpers, and the io_uring adapter.
- `crates/pgnotify-macros`: `NotifyPayload` and `NotifyRouter` derives and their
  compile-time fixtures under `tests/ui`.
- `crates/pgnotify/tests`: runtime integration tests and the compile-contract
  harness. `tests/support` provides a controlled PostgreSQL wire-protocol peer.
- `crates/pgnotify/examples` and `crates/pgnotify/benches`: runnable usage and
  latency/allocation measurements.
- `xtask`: local quality gates; `.github/workflows/ci.yml` runs the CI checks.

## Contracts

`PgListener<H: NotifyHook>` owns one connection on its caller's thread. Advance
it through explicit socket readiness and timer expiry events. Honor socket and
timer generations when dispatching events; a timer event means expiry, not a
periodic clock tick. Read `fsm.rs` and `hook.rs` before changing lifecycle code.
Callbacks run synchronously. `NotificationRef` borrows storage only for the
callback duration. Reconnect subscription replay must be acknowledged before
`on_reconnected` signals readiness; disconnected notifications cannot be replayed.

`ChannelTable` validates channel names and keeps IDs stable until removal. Its
name lookup uses a bounded byte trie. Keep registration allocation on the
subscription control path and lookup free of allocation. Names contain 1..=63
UTF-8 bytes without NUL. `PayloadBuf` exposes at most 7999 committed bytes;
serializer scratch reservations must not reject a valid exact-capacity payload.

`ProactorPgListener` adapts the listener to a caller-owned ring under `uring`.
Each driver keeps one ring and a unique nonzero subsystem ID. Preserve pending
operation ownership until target and cancellation completions retire separately.
Shared-ring shutdown uses `begin_shutdown`, `flush`, and the caller's dispatcher;
continue until `has_pending_io()` is false before releasing the driver.

`DieselNotifyExt`, under `diesel`, runs synchronous SQL on the caller's
`PgConnection`. Preserve transaction commit semantics and Diesel error details.
These operations may block and allocate; the dedicated listener is the
non-blocking receive path.

The derives emit implementations of the traits in `pgnotify::hook`.
`NotifyPayload` uses sonic-rs directly with `PayloadBuf` under `sonic`.
`NotifyRouter` validates unique channel names at expansion time, handles unit or
single-field variants, and returns `Ok(None)` for unknown channels. Generated
`<Enum>RouterHook<R, F>` callbacks take `pgnotify::Result<R>`; route errors through
`on_error` once per fault. Keep the default empty `on_disconnect` implementation
because listener faults invoke both lifecycle methods.

## Changes and validation

Preserve existing staged and unstaged changes. Inspect the affected contracts
and callers before editing. Keep Rust files at most 800 lines, source comments
and documentation in ASCII English, and comments focused on non-obvious reasons.
Use explicit errors, documented unsafe invariants, and the workspace lint policy.
Production and macro code must not use `unwrap`, `expect`, or placeholder macros.
The library uses no async runtime or background worker thread. Keep measured hot
paths free of application allocation and blocking; distinguish those claims from
libpq, Diesel, or serializer internals.

Add regression tests through public contracts. Compile-fail fixtures are checked
by `crates/pgnotify/tests/compile_contracts.rs`; review both diagnostic text and
source spans when updating their `.stderr` expectations.
`postgres_real_table_e2e_tests` requires `uring`, `diesel`, and `sonic`; it checks
real tables/triggers, 1000 ordered writes, payload boundaries, rollback, and
backend-kill recovery with SQL reconciliation. All live Diesel tests also run.
The shared `tests/support/postgres.rs` helper is included by both test binaries
and xtask. Honor only explicit `PGNOTIFY_TEST_DATABASE_URL` for an isolated test
database; otherwise reuse/start the local Docker fixture and wait for readiness.
Ignore `DATABASE_URL` when selecting test infrastructure. Keep schema cleanup
and ring drain explicit, and preserve healthy containers. CI uses a PostgreSQL 17
service; the local fallback uses PostgreSQL 18.6 on `127.0.0.1:55433`. Require zero
ignored tests in `cargo xtask ci`; table reconciliation does not imply reliable
replay of disconnected NOTIFY traffic. The README describes feature boundaries.

```sh
cargo test -p pgnotify --test compile_contracts --locked
cargo test -p pgnotify --test router_macro_tests --locked
cargo test -p pgnotify --test postgres_real_table_e2e_tests --locked
cargo xtask ci
cargo bench -p pgnotify --bench notify_dispatch_bench --no-run --locked
```

`cargo xtask ci` runs formatting, strict Clippy, tests for default/core/sonic
configurations, and rustdoc with warnings denied. Run benchmarks separately when
changing routing, lookup, or encoding. Report commands, results, skipped cases,
and external prerequisites. Finish with `git diff --check` and verify the final
file scope. Commit or push only when explicitly requested.
