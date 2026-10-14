# 2026-10-14 — FDB client verified live (Phase 0 fully exited)

Goal: unblock and execute the FDB vertical-slice prototype (`docs/fdb-verification.md`).

What changed:
- Environment arrived during the day: FDB server 7.3.77 running single-node (`status minimal` → available), `foundationdb-clients/server-7.3.77` RPMs, cargo 1.98.1, plus `clang-libs` installed via `dnf` (bindgen in `foundationdb-sys` build fails without libclang).
- Built throwaway `/tmp/opencode/fdb-probe` (not in repo): `foundationdb` 0.11.0 + `fdb-7_3`, `tokio` 1.53.3, explicit runtime API version 730. API pitfalls recorded in the doc: version feature mandatory; `FdbApiBuilder::build()` only selects the version — must `boot()` the network (first attempt failed with 2008 pre-network); `Database::from_path`; `TransactionCommitError` derefs to `FdbError` (has crate `on_error` backoff — adapter should use it).
- Evidence (all executed, outputs captured):
  - `bad-cluster`: open with bogus path succeeds (handles are lazy); first use fails `No cluster file found…` (1515). `PROBE_OK`.
  - `full`: Q1 open ok; Q2 write ~6 ms, fresh-read both keys present; Q3 100 commits p50 ~3–6 ms / p99 ~6–32 ms (run variance); 100 KiB, 1 MiB, 8 MiB values all rejected `Value length exceeds limit` (2103) against the documented 100,000-byte per-value limit — 64 KiB job cap stands. `PROBE_OK`, keyspace cleaned.
  - `write-only` → kill → `read-only`: second process sees both keys. Cross-process atomicity `PROBE_OK`; demo keys cleaned, keyspace verified empty (`getrange` count 0).
- Updated `docs/fdb-verification.md` (executed, pins + Q1–Q4 + operational facts), `docs/README.md`, `docs/phase-0-exit-criteria.md` (last box checked — **Phase 0 fully exited**), `AGENTS.md` (Phase 4 unblocked).

Open questions / next step: Phase 1 — scaffold the Rust crate from frozen contracts, redb adapter first (first real-backend conformance run). Phase 4 adapter tests to cover mid-run loss/reconnect (deliberately not tested against the live server today).
