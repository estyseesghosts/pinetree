# FoundationDB client verification

**Status:** Executed 2026-10-14 — all four questions answered (see §6); mid-run failure posture deferred to Phase 4 adapter tests
**Sources:** `sns-server-proposal.md` §§2, 9; FDB checkout (`foundationdb/`, read-only); live cluster + throwaway probe (below).

Phase 0 requires proving the FoundationDB Rust client and its operational requirements with a small vertical-slice prototype *before* building feature surface.

## 1. Questions the prototype must answer

1. Can the Rust client (pinned version, §3) open the cluster, run a read-write transaction, and return, from this project's toolchain?
2. Does one transaction atomically commit a note record plus a job/outbox record, then make both visible to a second transaction?
3. What are measured commit latencies (p50/p99 over 100 sequential commits) and the practical payload ceiling versus the documented ~5 s / 10 MB limits?
4. What breaks when the cluster is unavailable at startup and mid-run (client behavior, error surface, reconnect)?

## 2. Prototype scope (throwaway, not product code)

- Lives outside the product tree (`/tmp/opencode/fdb-probe/`), single binary. It must not become the adapter.
- Does the minimum through the real client: API-version select → boot network → open cluster → `transact` closure writing two keys (one `note:*`, one `job:*`) → commit → fresh read of both → print latencies and byte sizes.
- Asserts atomicity visibly: kill the probe between commit-ack and read (separate runs); a committed write must be fully present, an uncommitted one fully absent.
- Keyspace confined to `phase0probe/<run>/`, cleared with `clear_range` after every run; keyspace verified empty afterwards via `fdbcli getrange`.

## 3. Pins (recorded from `Cargo.lock` and the environment, 2026-10-14)

- `foundationdb` crate **0.11.0**, feature **`fdb-7_3`** (feature selection is mandatory — build fails without it); `foundationdb-sys` 0.11.0; `tokio` 1.53.3.
- Client library `libfdb_c.so` **7.3.77** (`foundationdb-clients-7.3.77` RPM, `/lib64/`); server **7.3.77** single-node, `status minimal` → available.
- Runtime API version requested explicitly: **730**.
- Build prerequisite beyond cargo: `clang-libs` (bindgen runs in `foundationdb-sys` build; fails with "Unable to find libclang" without it).
- API notes that bit during the probe: 0.11's `FdbApiBuilder::build()` only *selects* the version — the network must then be booted (`NetworkBuilder::boot()`, unsafe, guard held for process life); `Database::from_path`, not `open`; `commit()` errors are `TransactionCommitError` (derefs to `FdbError`), with a crate-provided `on_error` backoff the adapter should use instead of a naive retry loop.

## 4. Operational requirements (recorded facts)

- Client library install story: `foundationdb-clients` (+ `foundationdb-server` for local dev clusters) RPMs; Rust builds additionally need `clang-libs` (dev + CI images).
- Cluster-file distribution: path passed at startup (`FDB_CLUSTER_FILE` in the probe, default `/etc/foundationdb/fdb.cluster`); never hardcoded in product code.
- API version selection policy: request explicitly the version matching the supported server range (730 here); revisit when the fleet moves to 7.4+.
- Multi-thread guidance: client is async over one network thread; transaction closures must be short — reinforces the ports contract's bounded-TX rule.
- Failure surface observed (canonical C strings via `fdb_get_error`):
  - use before network boot → `Action not possible before the network is configured` (2008);
  - handle creation is **lazy** — bogus cluster path opens fine, first use fails `No cluster file found in current directory or default location` (1515);
  - oversize value → `Value length exceeds limit` (2103), client-side, non-retryable — retry loops must not blind-retry it.

## 5. Execution status

Executed, not blocked. Environment arrived mid-morning (server 7.3.77 running, clients RPM, then cargo, then `clang-libs` via `dnf`).

## 6. Answers (evidence in `state/logs/2026-10-14-fdb-verified.md`)

1. **Yes.** 0.11/`fdb-7_3` opens the 7.3.77 cluster and round-trips transactions from cargo-built code.
2. **Yes.** One transaction commits note + job keys; a fresh second transaction reads both; a killed-then-reread cross-process run sees both keys (`PROBE_OK` both modes).
3. p50 ≈ 3–6 ms, p99 ≈ 6–32 ms over 100 sequential commits on a local single-node cluster (76-byte values; run-to-run variance noted). Per-value ceiling confirmed in practice: 102,400-byte values rejected with 2103 against the documented 100,000-byte per-value limit. Consequence for our contracts: the 64 KiB job-payload cap (ports §6) clears the per-value limit with ~36% headroom and sits three orders of magnitude under the 10 MB transaction limit — no contract change needed.
4. **Partially.** Startup-unavailable behavior fully mapped (lazy handles, 1515 at first use, 2008 pre-network). Mid-run loss and reconnect posture deliberately not tested against the live server; they belong to Phase 4 adapter tests (kill/restart a throwaway cluster there), which this note unblocks.
