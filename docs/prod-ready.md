# Pinetree: Single-Node Production-Readiness Implementation Guide

**Audience:** OpenCode / coding-agent implementers  
**Repository:** `estyseesghosts/pinetree`  
**Scope:** Single-process, single-node Pinetree using redb and local filesystem media pools  
**Primary outcome:** Fix the identified correctness, security, test-gate, recovery, deployment, and diagnostics gaps without turning this into a distributed-systems project.

---

## 0. Mission and operating rules

Work against the current `master` HEAD, not assumptions from this guide alone. Begin each batch by reading the current code and tests for the named paths. The audit that motivated this guide was static; every defect must be revalidated against the exact source before changing it.

### Scope boundary

**In scope:** one Pinetree process, one redb database, local durable/cache media pools, incoming/outgoing ActivityPub federation, Misskey REST/WebSocket, local Docker deployment, operator diagnostics, backup and restore.

**Explicitly out of scope for this milestone:** FoundationDB, cluster routing, multi-node ownership, shared rate limiting, cross-node WebSocket delivery, S3/object storage, account movement between nodes, multi-node HA. Do not implement or count those items as release blockers for this guide. ActivityPub interoperability with other software *is* in scope; federation is a core single-node feature.

### Implementation discipline

1. Work in small, reviewable batches. Each batch should have one coherent responsibility and its own commit.
2. Do not launch multiple agents that edit the same core file at the same time. `src/persistence/redb/mod.rs`, `src/main.rs`, `src/config.rs`, `src/api/misskey/endpoints/mod.rs`, `src/federation/inbox.rs`, and `src/media/*` are highly coupled hotspots.
3. Before modifying persistence contracts, read `docs/application-ports.md`, `docs/database.md`, and the relevant traits in `src/application/ports.rs`. Update contract documentation when semantics change.
4. Do not make tests pass by weakening assertions, adding broad ignores, skipping a required phase, or treating a skipped suite as a release success.
5. Do not perform a broad rewrite/refactor while fixing correctness issues. Preserve transaction boundaries and existing Misskey/federation behavior unless the change explicitly requires otherwise.
6. Never claim a fix is verified merely because code compiles. Run targeted regressions, the corrected smoke gate, and the required Docker tests.
7. Do not use separate commands/processes to copy an open redb database file. A live backup must use a documented consistent snapshot/export approach, not an unverified filesystem copy.

### Required deliverables per batch

- Implementation and focused regression tests.
- Updated documentation/config examples where behavior changed.
- A short implementation report: files changed, invariant protected, commands run, results, unresolved concerns.
- A commit with a descriptive message, unless the caller explicitly asks for a combined uncommitted working tree.

---

## 1. Release gates and severity

Use these severity definitions throughout the work:

- **P0 / release gate:** data can be lost or damaged, the normal Docker deployment does not start, an important test result can be falsely green, or a security setting silently fails to enforce its meaning.
- **P1 / required before persistent production testing:** storage recovery/pagination correctness, backup/restore, unbounded process state, configuration safety, or mandatory single-node federation qualification.
- **P2 / required before calling the operational surface production-ready:** ingress resource bounds, meaningful readiness and status, safe detailed diagnostics, and actionable observability.
- **P3 / follow-up:** large-file maintainability improvements that do not need to be bundled into correctness work.

The project should not call itself production-ready until all P0/P1 work is implemented and tested and the P2 operational acceptance criteria are met. If live federation cannot run in a developer environment, CI/release status must be **unqualified**, not green.

---

## 2. Batch A — Make the smoke gate trustworthy first

**Primary files:** `scripts/smoke-all.sh`, `scripts/smoke-phase1.sh`, `scripts/smoke-phase2.sh`, `scripts/smoke-phase3.sh`, `scripts/keystone-sweep.sh`, `docs/testing.md`, `docs/wiki/Operations.md`, CI workflow files.

### A1. Fix Cargo exit-status propagation

The current runner uses an inner command equivalent to:

```bash
bash -c 'cd "$ROOT" && cargo test --offline 2>&1 | tail -30'
```

The outer `set -euo pipefail` does not make the inner `bash -c` pipeline pipefail-aware. A successful `tail` can hide a failed Cargo process.

Implement a portable, obvious solution. Acceptable approaches include:

```bash
bash -o pipefail -c 'cd "$ROOT" && cargo test --offline 2>&1 | tail -30'
```

or explicitly capture and return the Cargo exit code. Preserve useful failure output. Do not rely on subtle `PIPESTATUS` behavior across nested shells without a test.

### A2. Make `SKIP` distinct from a full release pass

Keep local prerequisite-aware behavior, but split outcomes into explicit categories:

- `SMOKE_ALL_OK`: every required phase actually ran and passed.
- `SMOKE_ALL_SKIP` or another distinct non-zero/unqualified result: a required release phase did not run because a prerequisite was absent.
- `SMOKE_ALL_FAIL`: a phase ran and failed.

Recommended implementation: add a strict mode such as `SMOKE_ALL_REQUIRED=1` used by CI/release checks. In strict mode, missing Docker, Node versions, or Misskey/Sharkey reference checkouts fails the gate. In developer mode, optional federation may skip, but the final output must say that the run is partial and must not print the exact all-required success sentinel.

Never make the strict gate depend on a developer having prebuilt ignored checkouts inside the repo. CI should provision the reference implementations or run a dedicated required federation workflow with pinned versions and explicit artifacts.

### A3. Cover the test runner's own failure paths

Add shell-level tests or a small harness that proves:

1. A fake `cargo` command that exits non-zero while `tail` exits zero still makes the smoke step fail.
2. A required federation phase that is skipped cannot return the all-required success result.
3. A real failing phase stops or records a failing result accurately.
4. An optional local skip is distinguishable from full qualification.
5. The reported Keystone sweep count matches the actual checks. Remove stale numbers from `docs/testing.md` and script comments.

### A4. CI requirements

- Set the CI job to strict mode.
- Upload complete logs as workflow artifacts while keeping the console summary short.
- Keep fast unit/contract tests separate from the slower live interoperability suite, but make the live suite a required release check.
- Report the tested source SHA, tool versions, image tags, and which phases ran.
- A stale locally compiled binary must not be used to claim live smoke success. The scripts should resolve/build the executable from the current checkout and fail on build failure.

**Acceptance:** intentionally break one unit test and show `smoke-all.sh` fails; intentionally remove a federation prerequisite and show strict mode fails; run a complete provisioned gate and show the success sentinel only after all required phases pass.

---

## 3. Batch B — Default configuration, local HTTP opt-in, Docker, and FFmpeg

**Primary files:** `src/config.rs`, `src/main.rs`, `Dockerfile`, `docker/pinetree.container.json`, a new example FFmpeg config if needed, config/Docker tests, `docs/wiki/Administration.md`, `docs/wiki/Operations.md`.

### B1. FFmpeg policy

Lock this decision in code and docs:

- The **default Rust `Config` and default Docker config disable transcoding** (`transcode.enabled = false`). A new install must not require FFmpeg to start.
- Pinetree must still support FFmpeg transcoding when explicitly enabled and the toolchain is installed and usable.
- If transcoding is explicitly enabled but FFmpeg is missing/broken, startup must fail clearly. Do not silently accept an untranscoded upload or job.
- The regular/minimal Docker target does not include FFmpeg. Provide a separate Docker build target (for example, `runtime-ffmpeg`) that installs a known, supported FFmpeg package/toolchain. Do not force FFmpeg into the minimal image merely to avoid modeling the two configurations.
- The FFmpeg image should not secretly override the application's default policy. Enabling it must be explicit in the chosen config JSON.

A suitable layout is a shared runtime base plus two final targets:

- `runtime`: CA certificates, required transport/runtime tools, no FFmpeg.
- `runtime-ffmpeg`: same application binary and hardening, with FFmpeg installed and version/probe tested.

Both targets use an opt-in transcode config. Provide a clearly named test/example config with `transcode.enabled: true`; do not make the repo's default config enable it.

### B2. Test FFmpeg end to end in Docker

Do not stop at `ffmpeg -version`. Test these cases using the built image:

1. **Minimal target + default config:** image starts, reports transcoding disabled, health/readiness work, `ffmpeg` is not present or not required.
2. **Minimal target + `transcode.enabled: true`:** startup exits non-zero with an actionable message naming the failed toolchain probe. It must not enter serve mode.
3. **FFmpeg target + default config:** image starts and transcoding remains disabled by default.
4. **FFmpeg target + explicit enabled config:** startup validates the actual FFmpeg binary, then runs a real media-transcode smoke case through the same worker/processing path used by normal jobs. Verify output bytes, detected MIME/extension, file-size limits, and that a failed/transient transcode cannot produce a falsely successful completed job.
5. Test shutdown while a transcode job is pending/running; ensure normal job lease/retry behavior and temporary-file cleanup are correct.

Prefer a tiny deterministic fixture or generate a minimal test media file in the test harness. Keep output sizes and run times tightly bounded. Run the test against the actual Docker targets, not only a developer's host binary.

### B3. Unsafe local HTTP must be explicit JSON configuration

The current shipped container example sets `public_url` to `http://localhost:3000`; replace that with an instructional placeholder that forces the operator to set a canonical URL. Production behavior should require HTTPS for public URLs.

Add a strongly named nested config setting, for example:

```json
{
  "unsafe": {
    "allow_local_http_for_testing": false
  }
}
```

Requirements:

- Default is `false` in `Config::default()`, all shipped production examples, and the Docker default config.
- It is controlled through the JSON config, not an undocumented environment-only override.
- With the flag off, reject `http://` public URLs during config validation with a clear error.
- With the flag on, allow only **local/test hosts** needed by current smoke rigs: loopback addresses, private IP literals where required, `localhost` / `.localhost`, and a deliberately documented test-only hostname suffix if the existing local test harness needs it. Do not let the flag silently permit arbitrary public Internet HTTP origins.
- Preserve existing explicit local federation test support (for example private-IP allowlisting) as a separate, separately documented control. The HTTP-origin exception and outbound SSRF/private-IP exception solve different problems; do not conflate them.
- Log a conspicuous startup warning whenever the unsafe exception is active. Include the effective host class, never secrets.
- Add tests for default HTTPS acceptance, default HTTP rejection, explicitly enabled loopback test URL acceptance, and enabled-but-public-HTTP rejection.
- Update smoke fixtures to set this flag only in their throwaway test configs. Never toggle it globally to make federation scripts pass.

### B4. Container runtime and health check

- Keep the container non-root and retain `tini`/signal handling.
- Ensure `/data` permissions are correct and both media pools live on persistent storage.
- Probe readiness, not only static liveness, for the container health check once the readiness contract in Batch G exists.
- Document the two Docker targets, both config examples, and the expected startup behavior.

**Acceptance:** build both targets from a clean checkout; run all four FFmpeg scenarios above; run the single-node smoke suite with its local HTTP opt-in isolated to the test config; verify the standard production example rejects localhost/HTTP and does not require FFmpeg.

---

## 4. Batch C — Fix redb media pagination, backfill, and reconciliation

**Primary files:** `src/persistence/redb/mod.rs`, `src/application/ports.rs`, `src/media/reconciler.rs`, `src/media/remote_cache.rs`, `src/jobs/worker.rs`, relevant media command modules, `tests/storage_*.rs`, `docs/database.md`, `docs/remote-media-cache-plan.md`.

This is a high-priority data-integrity batch. The current adapter for `list_all_media()` clamps reads to 100 rows and builds the output by iterating the whole media table, sorting, filtering the cursor, and truncating. The reconciler/backfill request larger pages and can therefore mistake a truncated page for the end of a scan. Treat these as defects in the live redb adapter contract, not merely tuning opportunities.

### C1. Define a real paging contract

Read `MediaStore::list_all_media` and `TimelineCursor` contracts first. Then make the live redb implementation honor the contract exactly:

- Requested page size means the number of returned matching rows, bounded only by a named and documented maximum—not a hidden clamp to 100.
- Cursors are exclusive, stable, and supported across every page. Preserve deterministic newest-first or oldest-first ordering as specified by the contract.
- Do not silently signal “end of scan” because an internal adapter returned fewer rows than requested.
- Avoid `O(total_media)` scanning and sorting for every page. Confirm the primary key/table layout and use ordered iteration/range scans over the redb key when possible. If filtering requires a scan, keep it bounded and demonstrate why it cannot skip rows.
- Keep a single, explicit page-size constant shared by callers and adapter contracts where appropriate.

Tests must use the **real `RedbStore`**, not only a fake `MediaStore`.

### C2. Prove pagination over more than 100 and 500 rows

Create durable and cache media rows with stable ordered IDs, including intentionally shared blob keys and different owners. Test:

- 0, 1, 99, 100, 101, 499, 500, 501, and several thousand rows.
- Every row appears exactly once across the full cursor walk.
- No row is skipped or repeated at page boundaries.
- A final partial page is recognized correctly.
- The scan ends even for a dataset size that is an exact page multiple.
- Cursor ordering and exclusive-bound semantics remain correct if a row is inserted or deleted during separate-page scanning. If the algorithm cannot promise snapshot consistency across separate pages, document the allowed semantics and ensure destructive operations do not rely on a weak scan.

### C3. Repair `backfill_durable_accounting`

The current backfill requests up to 500 rows and treats a page shorter than 500 as the final page. With the present 100-row adapter clamp, this can rebuild `DURABLE_BYTES_BY_OWNER` and `BLOB_REFCOUNT` from only the first 100 rows.

Requirements:

- Walk the entire media table with explicit continuation cursors and accurate completion criteria.
- Recompute durable bytes per owner and blob reference counts for all relevant rows, including shared blob keys across owners and appropriate durable/cache references.
- Do not overwrite counters from an incomplete scan.
- Do not race incremental uploads/deletes/evictions. Pick and document a safety model: execute the one-time migration/backfill before accepting requests and before starting the worker, or use a maintenance/write barrier that prevents media-counter mutations for the entire logical rebuild. An online job that scans, then overwrites counts while ordinary media writes continue is not acceptable.
- Persist a completion/version marker only after a successful full rebuild. A failed run must be safe to restart and must not mark partial results as complete.
- The process should be restartable and idempotent.

If the job payload already contains a cursor, ensure the worker actually uses it. If a full scan can span jobs, enqueue each continuation durably and only mark the migration complete after the final page. Do not label a single 100-row page “complete.”

### C4. Repair media reconciliation

Audit `reconcile_media_with_cap`, `collect_live_blob_keys`, and `sweep_orphan_blobs` as separate functions. The docs currently describe filesystem orphan cleanup as part of recovery, but the live production wiring did not show a full-key sweep being invoked by the worker.

Required behavior:

- Reconcile media metadata in bounded pages with a continuation cursor; repeated runs must make forward progress, not repeatedly revisit the newest page.
- Separate **detection-only** counters from **actually repaired** counters. Do not report counters as “healed” if they were only compared or logged.
- Recover missing/corrupt `.len` sidecars when original bytes can be safely read. Keep irrecoverable durable media visible to operators; never silently delete a user's durable metadata or silently pretend its bytes exist.
- Leave evicted/missing remote cache metadata re-fetchable without charging nonexistent bytes as present cache content.
- Reconcile blob refcounts and owner byte counters completely and safely, using setters only under the safety rule above.
- Repair orphan filesystem bytes, but **never pass a partial live-key set to a destructive sweep**. Either compute a complete, consistent key set through fully paginated traversal under a safe media lifecycle barrier, or redesign the sweep around authoritative reference checks plus conservative quarantine/grace periods. Prefer safe leaking of an orphan over deletion of live user media.
- Keep the durable and cache pools completely separate; cache cleanup must never delete durable user files.
- Make the production trigger explicit. Provide a deliberate one-time migration/backfill path and a repeatable reconciliation trigger (scheduled durable job or explicit admin/CLI maintenance operation). Tests that construct and enqueue jobs directly are not evidence that production schedules them.

### C5. Crash/failure injection

Add tests for process termination/failure between:

1. staging bytes and importing them into a pool;
2. pool write and metadata insert;
3. metadata/refcount transaction and blob delete;
4. cache eviction metadata change and physical deletion;
5. page scans and persisted backfill state;
6. completed row scan and migration marker update.

After restart and reconciliation, assert the invariant: durable media is either present and correctly accounted, or explicitly reported as missing for operator repair. Remote cache bytes may be refetched. No live blob is deleted because a scan ended early. Add a large-table regression proving that all rows/counters survive these procedures.

**Acceptance:** a test with more than 1,000 media rows rebuilds exact owner-byte and blob-refcount totals; a multi-page reconciliation reaches the oldest row; a destructive orphan sweep cannot run on an incomplete key list; all existing storage tests and single-node smoke tests pass.

---

## 5. Batch D — Bound client-key state in rate limiting and upload slots

**Primary file:** `src/api/misskey/response.rs`; tests for rate limiting and upload slots.

The current `RateLimiter` stores request buckets and auth failure state in in-memory maps keyed by `(client/account key, endpoint bucket)`. Expired entries are not pruned in the inspected methods. `UploadSlots` stores one semaphore per distinct IP but never removes the IP entry after an upload permit is released.

### Required design

- Use bounded maps with expiry/eviction or periodic opportunistic pruning; preserve active rate-limit semantics.
- Expire ordinary bucket state once the request window is gone and no interval state needs to remain.
- Expire auth-failure state after its configured window/backoff has ended.
- For upload slots, avoid removing a semaphore while it still has active permits or while another acquire path can obtain an independent semaphore for the same IP. Use a reference-counted entry or a safe map/lock lifecycle; a naive `remove(ip)` on permit drop can break the per-IP concurrency cap under races.
- Retain per-IP plus global upload concurrency caps.
- State limits must be configurable or have explicit documented bounds; eviction of stale entries must never disable throttling for active abuse keys.
- Add tests for expiry, map capacity under high-cardinality churn, simultaneous same-IP acquisitions, permits releasing, and safe eviction while requests race.

Do not add distributed/shared-state rate limiting in this batch.

**Acceptance:** a high-cardinality test runs thousands of unique keys and the retained maps remain under a documented upper bound, while all existing rate-limit/backoff/concurrency tests pass.

---

## 6. Batch E — Make RFC 9421 configuration truthful

**Primary files:** `src/config.rs`, `src/federation/inbox.rs`, `src/federation/fetch_gate.rs` as needed, `src/activitypub/signatures.rs`, federation tests, `docs/wiki/Security.md`, `docs/wiki/Federation.md`.

The config exposes `federation.rfc9421_mode` with `permissive` and `enforce`, but the current inbox authentication path detects RFC 9421 headers and falls back to legacy `Signature` verification without consulting the configured value. A recognized setting must not offer false assurance.

### Requirements

1. Trace all inbound signed request paths. The inbox is not the only signature-authenticated surface; signed GET/fetch paths have their own gate.
2. Implement the verifier and mode semantics, or reject `enforce` at configuration validation until it is implemented. Do not leave `enforce` silently ignored.
3. Define precise modes. For example:
   - `permissive`: accept valid legacy signatures; if RFC 9421 fields are present, verify them rather than merely log them when the sender's request can be authenticated that way. Preserve transitional dual-sign interoperability deliberately.
   - `enforce`: require a valid RFC 9421 signature under the supported profile and reject legacy-only requests.
4. Specify which covered components are required, how `Signature-Input` and `Signature` are parsed, how digest/body integrity is checked, how `keyid` maps to an actor/key, and how timestamp expiry/skew works. Reject duplicate/malformed components and unsupported algorithms safely.
5. Preserve existing actor-owner and object-authority checks after signature verification. Valid cryptographic signatures must not bypass these authorization gates.
6. Do not weaken legacy verification during migration. Do not claim complete RFC 9421 support until exercised against known vectors and live server implementations.
7. Add tests for legacy-only, RFC-9421-only, valid dual-signature, invalid signature, tampered body, malformed/missing covered components, replay/stale timestamps, key rotation, actor mismatch, and mode selection.
8. Update config schema/default tests and docs so the configured mode matches actual behavior.

If full RFC 9421 verification cannot land in the same batch, make the safe interim change: reject `enforce` clearly and remove any documentation that implies it works. The release gate should track interoperability with currently used federation software.

**Acceptance:** no accepted config value is silently ignored; tests demonstrate enforcement and legacy interoperability behavior for each supported mode.

---

## 7. Batch F — Implement live database + media backup and restore

**Primary files:** a new `src/backup/` module or equivalent, `src/persistence/redb/mod.rs`, shared media storage lifecycle/gate, configuration, admin command/route, backup and restore tests, `docs/wiki/Operations.md`, `docs/database.md`.

This is a core P1 requirement. A backup must include coherent application state and media, not just the `.redb` file. The current state spans `pinetree.redb`, `blobs_durable/`, and `blobs_cache/`.

### F1. Do not copy the open redb file blindly

The pinned redb 4.4 `Database` API documents read/write transactions, integrity checking, and compaction, but no general-purpose online backup method. See the [redb `Database` API](https://docs.rs/redb/4.4.0/redb/struct.Database.html) and the [`Value` byte encoding contract](https://docs.rs/redb/4.4.0/redb/trait.Value.html). Do not invent a `Database::backup` call and do not assume ordinary filesystem copying of the live file is transactionally consistent.

Implement a **logical online snapshot/export** using a single consistent redb read transaction and a complete, typed registry of all persisted tables and multimaps. Export each table's entries with exact key/value bytes via its defined redb types. Restore those entries into a fresh database using the same table definitions and type metadata. It must include primary tables, secondary indexes, dedup state, durable jobs, queues/quarantine state, configuration/instance state, account key material and every other persisted table—not just user-visible domain rows.

The registry must be explicit and maintained with the schema. Add a test that proves every declared production table and multimap is covered, and tests that make a new table fail the completeness check until backup/restore support is added.

Do not export secrets into log files. The backup bundle itself is sensitive; its permissions and retention are operator-critical.

### F2. Bundle format

Use a versioned, documented format, for example a ZIP bundle using the already available `zip` dependency (or another justified format already used in the repo). Include:

- `manifest.json`: bundle format version, application version/commit, redb version, persistence schema version, creation time, counts, logical snapshot marker, and every entry's checksum/size.
- Typed database-table entries: table identity/type signature plus encoded keys and values. Include multimap entries.
- Both media pools, including content-addressed objects and `.len` sidecars. Skip transient staging files unless the format explicitly defines recovery for them.
- A checksum/hash for every archived component and an overall manifest/checksum verification method.

Stream large content to disk; do not accumulate the entire database or media archive in RAM. Use `.partial` staging outputs and only rename to the final backup name after the bundle is complete, synced and verified. A failed/incomplete backup must never appear as a completed backup.

### F3. Coordinate the database snapshot with filesystem media

Use one shared media-storage lifecycle gate for both the durable and cache stores. The online backup process should:

1. Acquire the exclusive backup gate so no pool mutation/deletion can invalidate bytes during the snapshot/copy.
2. Start one redb read transaction for a stable view of all metadata and application state.
3. Stream the full table snapshot into the staging bundle.
4. Copy the media pools (or, if a deliberate optimization is chosen, copy every blob referenced by that snapshot under the same deletion barrier). A full-pool copy may contain harmless orphan bytes; a bundle must never contain metadata for a durable blob that disappeared during backup.
5. Verify component counts/checksums, finish the manifest, sync output, and atomically publish the final bundle.
6. Release the snapshot and storage gate reliably on both success and failure.

Any mutation path that creates, imports, repairs, evicts, or deletes pool bytes must respect the gate. Review `FsBlobStore::put`, `import_file`, `delete`, transcode import, cache eviction, reconciler cleanup, local upload, remote fetch, and account/media deletion. Avoid lock-order inversions and avoid acquiring the same gate recursively.

Keep non-media requests serving during backup. Media operations may wait behind the backup gate; expose that state and duration in diagnostics. Set configurable disk-space/size guards and a maximum number of concurrent backups (normally one).

### F4. Initiation and security

The live process must own the DB handle. An ordinary second CLI process generally cannot open the same redb file because of file locking. Provide one supported in-process trigger:

- preferred: authenticated admin action that queues/starts a background backup with a durable job/status record; or
- a carefully designed in-process maintenance command/control endpoint with the same authorization and job status semantics.

A random URL parameter or unauthenticated route must never allow backup creation. Do not expose a public download endpoint without a separate explicit design and authorization policy. Keep backup outputs inside a configured backup directory; reject path traversal and arbitrary destination paths from request bodies. Use restrictive filesystem permissions and never log passwords/tokens/keys.

The standard backup request should return accepted/job ID quickly. Provide a status view showing queued/running/completed/failed, start/end times, output identifier, bytes written, and a redacted failure reason. Keep the completed bundle at a local filesystem path for operator retrieval. Clean abandoned partial files on startup.

### F5. Restore contract

Restore is a separate operation from online backup. Do **not** hot-swap a database file while the server is using it. The required initial contract is: restore into a fresh/offline target directory, validate it, then perform a controlled server restart/cutover.

- Validate bundle version/schema compatibility before writing.
- Reject absolute paths, `..` traversal, duplicate archive entries, unexpected symlinks, excessive declared sizes, bad checksums, malformed table identities, incompatible type metadata and missing required components.
- Restore into a new temporary directory, never destructively overwrite the source or current live data directory.
- Recreate tables with the pinned schema and restore all entries.
- Reopen the restored redb database; perform redb integrity checking only when no transactions remain active, using the API's documented lifecycle requirements.
- Verify table/row counts, key invariants, queue/dedup state, media object checksums and sidecars, durable-byte/refcount consistency, and that every durable metadata record has the bytes it expects.
- Only after all checks succeed, mark the restore directory ready and document the operator cutover procedure. Preserve the previous data directory until post-restore validation passes.
- Refuse incompatible restores by default. Any schema migration from a previous bundle version must be explicit and tested.

### F6. Required backup/restore tests

1. Create accounts, notes, follows/blocks, reactions, polls, media, instance settings, keys, outstanding jobs and dedup records; make a live backup while ordinary REST writes continue.
2. Concurrently attempt media upload/delete/cache eviction while backing up; verify the resulting bundle is coherent and never missing a blob referenced by its snapshot.
3. Restore the bundle to a new data directory and verify data via real Misskey API and ActivityPub endpoints.
4. Verify durable uploads survive; cache behavior is restored or safely rebuildable by documented design; counters and refcounts match.
5. Corrupt a manifest, database table payload, blob, sidecar or checksum and verify restore refuses to mark the bundle successful.
6. Kill the process during backup and confirm incomplete output is not marked final and is safely cleaned or resumed according to documented rules.
7. Test unsupported schema versions and path-traversal archive entries.
8. Run an actual Docker backup/restore drill against a throwaway data volume.

**Acceptance:** an online snapshot of a changing test instance restores into a fresh data directory with all persisted tables and referenced durable media intact; deliberately corrupt/incomplete archives fail closed; the live service remains available outside of media operations briefly waiting on the backup gate.

---

## 8. Batch G — Ingress limits and server resource safety

**Primary files:** `src/federation/inbox.rs`, `src/api/misskey/endpoints/mod.rs`, route construction and tests.

The inbox's 64 KiB rejection currently happens inside `process_inbox()` after Axum has extracted `Bytes`. The inspected route history indicates Axum's default 2 MiB body limit still bounds those requests, so do not describe this as unbounded allocation. It is still a mismatch between the intended inbox cap and the point at which it is applied.

### Requirements

- Apply the ActivityPub inbox limit at the router/body-extraction layer before the handler buffers the full body. Use a per-route limit or bounded streaming read suitable for Axum 0.8.
- Preserve the explicit 64 KiB business limit and return a predictable 413 for larger content. Ensure the handler cannot accidentally bypass the ingress cap.
- Test with `Content-Length` larger than the cap, a chunked body without a useful length, a dishonest length, invalid JSON, and concurrent oversized requests.
- Assert bounded memory behavior from code structure: never call `to_bytes(body, usize::MAX)` for an untrusted route.
- Verify ordinary Misskey JSON keeps its intended smaller cap, and multipart uploads use a bounded streaming path and upload semaphore.
- Do not disable the framework's default limits globally as a shortcut.

**Acceptance:** requests above each route's documented limit fail before a full body is buffered by the application; allowed requests retain the expected Misskey/federation response semantics.

---

## 9. Batch H — Health, status, diagnostics, and admin-controlled detail

**Primary files:** `src/observability.rs`, `src/config.rs`, `src/main.rs`, router/admin endpoints in `src/api/misskey/endpoints/mod.rs`, service/store traits, tests, `Dockerfile`, operations/admin docs.

We need useful operations visibility without turning diagnostics into a public information leak. Preserve a distinct liveness/readiness/status model.

### H1. Health endpoint contract

- `GET /healthz`: cheap liveness only. It answers whether the process/event loop is up. It should not do a full DB scan, network probe, or expensive integrity check.
- `GET /ready`: bounded, meaningful readiness. It should verify that the instance can perform a real redb read and that critical storage roots are available. Add only bounded checks that make sense for taking traffic. Return a non-2xx status when a critical dependency prevents serving correctly.
- Docker health checks should use readiness when the container is expected to receive traffic, while process supervisors may separately track liveness.
- Do not expose detailed DB/job/federation information in unauthenticated health endpoints. Use stable small JSON responses and no file paths, keys, host inventories, or secrets.

### H2. Add an explicit JSON configuration for diagnostics

Add a nested section with safe defaults, for example:

```json
{
  "diagnostics": {
    "detailed_admin_status": false,
    "structured_logging": true
  }
}
```

The exact key names may follow repository conventions, but defaults and semantics must be explicit, serde must continue rejecting unknown keys, and tests must cover config round-trips. `detailed_admin_status: false` means detailed reports are unavailable by default; enabling it does not remove the requirement for admin authentication. It must not turn detailed status into an unauthenticated public endpoint.

If this setting is intended to raise log verbosity, keep that distinct from API response detail (or name it explicitly). Avoid a vague generic `debug=true` switch that accidentally logs request bodies or credentials.

### H3. Structured logs

- Replace ad-hoc critical-path logging with structured levels and stable event names while retaining the existing opt-in federation debugging workflow during migration.
- Add request correlation IDs and durations; correlate job IDs, delivery destination keys, media operation IDs, and backup IDs.
- Redact `Authorization`, cookies, access tokens, passwords, SMTP credentials, private key material, request bodies and sensitive personal data. Do not dump entire `Config` values.
- Include enough error class/cause to troubleshoot without copying arbitrary remote response bodies into logs.
- Keep normal logs at an operationally useful level; detailed debugging must be explicitly configured.

### H4. Authenticated admin status endpoint

Add a protected admin endpoint (or extend a coherent existing admin diagnostics surface) with a stable schema. Require the existing authentication and admin permission helpers; don't invent a weaker second admin authentication path.

Minimum useful status fields:

- application version/commit, uptime and configuration-safe mode flags;
- readiness summary and last probe error class;
- redb open/read state and basic file/database statistics where available without blocking writes;
- durable pool/cache pool sizes, configured caps, used bytes, free disk space, media accounting drift and last reconciler/backfill timestamps/results;
- job counts by kind/status, oldest pending job age, retry count and quarantine count;
- federation delivery success/failure/retry/quarantine counts, queue delay and recent failure categories (destination details may be admin-only);
- HTTP request counts/error classes/latency summaries and websocket connection/subscription counts where already tracked;
- backup jobs/recent status and last successful backup timestamp, without exposing backup contents or secrets;
- whether `transcode.enabled` is on and the validated FFmpeg version/status, but never an arbitrary binary output dump.

A basic admin status endpoint may remain available when detailed diagnostics are disabled; the JSON flag governs expensive/detailed fields. Do not run a full table walk, `Database::check_integrity()`, filesystem recursive hash, network fetch, or unbounded job scan on each status request. Use maintained counters or bounded snapshots and expose when a value is stale/unknown.

### H5. Detailed diagnostics

When `diagnostics.detailed_admin_status` is enabled and an authenticated administrator requests the detailed route, include:

- last successful/failed startup probes and subsystem check results;
- bounded recent internal errors by redacted category;
- redb file bytes and documented statistics;
- media-pool/accounting invariant results from the latest completed scan;
- queue timing and retries by job kind;
- federation key-resolution/signature/retry result classes, never private keys or full untrusted bodies;
- recent backup/restore validation results;
- version/config fingerprint excluding secrets.

Admin detail must have a response-size and time limit. If an expensive diagnostic is needed, enqueue it and return a job ID/status rather than blocking a request for minutes. Do not execute `check_integrity()` against the live DB on every request: the pinned redb API documents that it requires mutable access and no outstanding transactions and that it is intentionally slow.

### H6. Make the status outputs testable

- Unit-test JSON schemas and redaction.
- Assert anonymous users cannot get authenticated admin diagnostics (the public health routes are the deliberate exception).
- Assert a normal user/moderator without admin permission is denied as specified by current policy.
- Assert `detailed_admin_status=false` does not return expensive/private fields; `true` still requires admin authentication.
- Simulate redb unavailable, a missing/unwritable storage root, job backlog/quarantine, FFmpeg unavailable when disabled/enabled, and incomplete backup. Verify statuses and diagnostic error codes remain useful and non-sensitive.
- Add an integration test proving readiness fails for an intentionally unavailable critical dependency while liveness remains up.

**Acceptance:** operators can distinguish a live process from a ready instance, can diagnose a stuck job/federation/media/backup issue via an admin-only surface, and no tested diagnostics response includes credentials or key material.

---

## 10. Batch I — Documentation consistency and single-node acceptance

**Primary files:** `docs/wiki/Architecture.md`, `docs/wiki/Security.md`, `docs/wiki/Operations.md`, `docs/wiki/Administration.md`, `docs/testing.md`, `docs/TODOS.md`, Docker docs/examples.

Update documentation only after implementation and tests establish the actual behavior. Several docs have historical addenda whose old claims contradict newer code; do not append another note without resolving the current truth.

### Required documentation updates

- Config defaults: transcoding disabled; optional `runtime-ffmpeg` target; explicit enabled config.
- HTTP safety: HTTPS by default and exact unsafe local HTTP JSON opt-in/host constraints.
- Smoke gate semantics: developer partial run versus strict release qualification.
- Media scan/page contract and how one-time backfill/reconciliation is triggered, resumed, observed and safely run with live traffic.
- Backup format, directory permissions, live snapshot consistency, retention, integrity checks, restore procedure, compatibility rules and verified drill result.
- Health/readiness/admin status endpoints and diagnostic detail setting.
- Supported single-node boundaries and deferred distributed scope.
- Interoperability matrix: exact reference software versions, what was tested, which tests are automated, and anything still not verified against live peers.

Do not state “complete”, “healed”, “production-ready”, or “green” in docs unless tests actually prove the claim. Keep a concise Known Limitations list for remaining intentional Misskey divergences.

---

## 11. Batch J — Release verification and security sweep

Perform only after the fix batches have landed.

### Local gates

Run and report exact results for:

```bash
cargo fmt --check
cargo test --offline
cargo clippy --all-targets -- -D warnings
bash scripts/smoke-all.sh
```

Use a clean build directory or the repository's documented build-resolution procedure. If the live federation phase cannot run, report the run as partial/unqualified. Run the full, provisioned federation gate separately and require it for release.

### Docker gates

- Build/test the minimal image (no FFmpeg, default disabled).
- Build/test the FFmpeg image (actual transcoding enabled by explicit JSON config).
- Exercise local HTTP opt-in with a throwaway local URL, and verify it rejects public HTTP hosts.
- Run health/readiness/status tests from the actual image.
- Create an online backup under load, restore to a fresh mounted volume, then exercise REST, media fetch and federation after restore.

### Security and resilience gates

- Run the repository's dependency advisory/license tooling if configured; otherwise add a documented `cargo audit`/equivalent CI step as a follow-up required before a public production deployment.
- Re-run SSRF/IP/redirect/downgrade tests after any HTTP changes.
- Fuzz or property-test malformed inbox JSON, signatures, headers, remote Actor/object payloads and websocket JSON; verify no panic and bounded processing.
- Apply load tests for unique IP/account-key churn, many media rows and concurrent cache eviction/backup.
- Exercise SIGTERM/SIGKILL paths for jobs, upload/transcode temp files and backup `.partial` files.
- Verify backups and status reports never expose secrets to unprivileged users or in ordinary logs.

---

## 12. Suggested work ordering and agent assignment

Use one integration owner to review contracts and own final merges. Independent work is allowed only when file ownership is clearly separated.

| Order | Work package | Suggested owner | Depends on | Merge gate |
|---|---|---|---|---|
| A | Smoke gate, strict mode, test-runner regressions | Agent 1 | None | False-green tests prove failure |
| B | Config policy + Docker targets + FFmpeg tests | Agent 2 | A for dependable acceptance | Both Docker variants and all FFmpeg cases pass |
| C | redb media paging, reconciler and backfill safety | Agent 3 | A | >1,000-row real-redb regression; no incomplete destructive sweep |
| D | Bounded rate-limit/upload maps | Agent 4 | A | Churn/race tests show bounded state |
| E | RFC 9421 mode/verifier | Agent 5 | A; coordinate federation files | All signature mode tests pass; no ignored enforce setting |
| F | Online database/media snapshot + restore | Dedicated owner | C's storage contract/gate; A | Live-under-write backup and verified restore drill |
| G | Body limits, readiness, structured logs, admin diagnostics | Dedicated owner | B config shape; C/F metric contracts | Auth/redaction/readiness integration tests |
| I/J | Docs reconciliation + full acceptance | Integration owner | A–G | Strict tests and release checklist satisfied |

Parallelism caution: Agent C and Agent F both touch `src/persistence/redb/mod.rs` and storage ports/gates. They should agree on the paging and media-storage lifecycle contracts first; do not edit those files in parallel. Agent B and Agent G both edit `src/config.rs`; settle the nested config schema once and have one owner integrate it.

### Commit-level order

1. `test: make smoke-all failures and skips reliable`
2. `config/docker: default transcode off and gate local HTTP`
3. `storage: implement true media paging and safe full backfill`
4. `storage: complete bounded reconciliation and production triggers`
5. `security: bound rate-limit and upload-slot state`
6. `federation: enforce configured signature mode`
7. `backup: add consistent live snapshot bundle and offline restore`
8. `ops: add readiness, admin diagnostics and structured logging`
9. `docs: align single-node ops and release gates`

Adjust order only if a real dependency requires it; explain the change in the agent report.

---

## 13. Definition of done

The single-node milestone is complete only when all statements below are true:

- [ ] A failing Cargo test cannot be masked by the smoke runner.
- [ ] A skipped required federation suite can never produce a fully qualified success result.
- [ ] The default config disables FFmpeg and the normal image starts without it.
- [ ] The optional FFmpeg Docker image starts with an enabled transcode config and completes a real transcode job; both success and failure behavior are tested.
- [ ] HTTP `public_url` is rejected by default and permitted only through explicit unsafe JSON opt-in restricted to local/test addresses.
- [ ] Media pagination visits every row exactly once over multiple pages using the real redb adapter.
- [ ] Durable-byte/refcount backfill computes totals across the complete data set and cannot race live mutations into incorrect totals.
- [ ] Reconciliation cannot mistake a truncated scan for the whole library or delete a live blob based on an incomplete key set.
- [ ] Filesystem-orphan reconciliation and backfill have deliberate production triggers and observable progress/results.
- [ ] Rate-limiter and upload-slot maps remain bounded under high-cardinality IP/account churn without breaking concurrency or throttle guarantees.
- [ ] `rfc9421_mode` either works as documented or rejects unsupported enforcement modes; no silently ignored security setting remains.
- [ ] An online backup captures a consistent full database snapshot plus media pool state while the rest of the service continues operating.
- [ ] Restore validates checksums/schema, refuses corrupt or incompatible bundles, and recreates a working instance in a fresh data directory.
- [ ] Tests cover body limits before whole-body buffering on the federation inbox route.
- [ ] Liveness, readiness, basic status, and admin-only detailed diagnostics have tests for failure states and secret redaction.
- [ ] Docker health checks and operator docs reflect the real liveness/readiness contract.
- [ ] Required REST, WebSocket and live ActivityPub federation gates pass against pinned reference implementations.
- [ ] All docs match what the code and test logs demonstrate.

## 14. Final implementation report template

For each batch, report:

```text
Batch:
Commit:
Source SHA tested:
Files changed:
Invariants protected:
Tests run:
Results:
Docker image/target tested:
Federation phase status (pass/fail/skip/unqualified):
Backup/restore drill (if applicable):
Known remaining risks:
Docs updated:
```

Do not declare the milestone complete while any required item above is unchecked. Keep the project single-node until these guarantees are established; do not divert effort to cluster/FDB work during this hardening phase.
