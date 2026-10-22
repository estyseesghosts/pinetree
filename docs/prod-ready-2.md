# Pinetree — revised audit and production-readiness implementation plan

**Repository:** `estyseesghosts/pinetree`  
**Audit baseline:** `ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e` (`docs: align single-node ops and release gates`, 2026-10-22)  
**Scope:** single-process, single-node Pinetree; redb; local durable/cache media pools; Misskey API; ActivityPub federation; Docker operations.  
**Out of scope:** FoundationDB, multi-node ownership/routing, distributed streaming, shared object storage, cross-node rate limiting. Do not turn this work into a cluster implementation.

## 1. Executive verdict

Pinetree has made substantial progress: signature timestamp overflow checks, bounded rate-limit/upload-slot state, inbox body limits, media pagination, backup staging/verification, offline restore, readiness, diagnostics, and smoke-runner behavior have all improved. Do not reopen those findings without new evidence.

**The server is not yet release-qualified.** The highest risks are media lifecycle races, backup snapshot/reference consistency, unbounded resolver threads, and a mismatch between the new TOTP implementation and password sign-in. Configuration and operations also need tightening. Static source review has not proven these defects by execution: implement regression tests that fail against the current code before changing behavior.

### Severity definitions

- **P0 — production blocker:** plausible data loss, authentication-policy bypass, or a destructive operation that can delete live data. No public deployment before closure.
- **P1 — production blocker:** severe denial-of-service/resource-exhaustion risk, unreliable backup/recovery, or a security setting that does not enforce its documented policy.
- **P2 — production operations requirement:** monitoring, retention, panic containment, maintenance safety, and operational failure recovery.
- **P3 — separately scoped compatibility work:** APIs explicitly documented as unsupported, intentional divergences, or multi-node requirements. These must be accurately documented, but are not automatically single-node release blockers.

## 2. Revised findings

### P0 — Media CRUD and orphan cleanup can race

**Status:** confirmed by code-path inspection; regression reproducer still required.  
**Evidence:** [`commands/media.rs`](https://github.com/estyseesghosts/pinetree/blob/ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e/src/application/commands/media.rs), [`media/storage.rs`](https://github.com/estyseesghosts/pinetree/blob/ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e/src/media/storage.rs), [`media/reconcile/heal.rs`](https://github.com/estyseesghosts/pinetree/blob/ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e/src/media/reconcile/heal.rs), [`media/reconcile/scan.rs`](https://github.com/estyseesghosts/pinetree/blob/ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e/src/media/reconcile/scan.rs).

`FsBlobStore::put()` and `delete()` take the shared side of the media `RwLock` only while their individual filesystem operation runs. Both can therefore run concurrently. More importantly, the upload/delete logical operations span additional database updates outside that filesystem-only gate. A delete can determine that a content-addressed key has zero references while a same-content upload is creating a new reference; the delete can subsequently remove the newly reused bytes. Orphan sweeping also captures a live-key set separately from the later filesystem walk and permits concurrent shared-gate mutations.

**Required invariant:** at every committed, externally visible state, every non-evicted media row that requires durable bytes must have those bytes; an object is physically deleted only after the system has serialized competing operations and proved no live row/index requires it. Reconciliation must never delete a key that became live after the live-key set was collected.

### P0 — TOTP support and sign-in enforcement disagree

**Status:** high-confidence security inconsistency in the current source; build a focused end-to-end reproducer first.  
**Evidence:** [`auth_http/signin_flow.rs`](https://github.com/estyseesghosts/pinetree/blob/ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e/src/api/misskey/endpoints/auth_http/signin_flow.rs), [`dispatch_auth/twofa.rs`](https://github.com/estyseesghosts/pinetree/blob/ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e/src/api/misskey/endpoints/dispatch_auth/twofa.rs), [`application/commands/auth_2fa.rs`](https://github.com/estyseesghosts/pinetree/blob/ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e/src/application/commands/auth_2fa.rs), [`packers/user.rs`](https://github.com/estyseesghosts/pinetree/blob/ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e/src/api/misskey/packers/user.rs).

The sign-in flow says the TOTP/passwordless branch is unreachable because `twoFactorEnabled` is hard-coded false and password-only authentication is the only reachable branch. The same repository implements TOTP enrollment, enables a verified secret, and exposes an unauthenticated `i/2fa/authenticate` path that mints a token on successful TOTP verification. Treat these paths as one authentication design, not independent endpoints.

**Required invariant:** if an account has enabled TOTP, password-only sign-in must not issue a usable session. A second-factor endpoint must not issue a session unless it is bound to a short-lived, one-time challenge created after first-factor success, according to the intended Misskey-compatible flow. The packed `twoFactorEnabled`/passwordless fields must reflect persisted state, not hard-coded values. If full TOTP sign-in is not ready, disable enrollment rather than advertise an ineffective security control.

### P1 — Backup inventory must match its database snapshot

**Status:** consistency defect in design; archive corruption/loss outcome not locally reproduced.  
**Evidence:** [`backup/bundle.rs`](https://github.com/estyseesghosts/pinetree/blob/ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e/src/backup/bundle.rs), [`backup/mod.rs`](https://github.com/estyseesghosts/pinetree/blob/ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e/src/backup/mod.rs).

Bundle creation exports all registered tables through one consistent redb read transaction, then separately pages `list_all_media()` to collect required blob keys, then copies the media pools. The exclusive backup gate prevents pool mutations only when every logical media mutation correctly participates in it; it does not currently establish that the second database scan describes the exported snapshot. Since full pools are copied, do **not** overstate this as proven routine byte loss: the concrete defect is that table rows, reference inventory, and missing-blob gaps can describe different database states, potentially allowing a completed archive that later fails restore validation.

**Required invariant:** exported table rows, their required-media inventory, and archive gap checks derive from one database snapshot and one media lifecycle barrier. A backup with a missing required blob must fail and remain unpublished; it must not be marked completed with a known gap.

### P1 — DNS resolver timeout does not bound resolver work

**Status:** confirmed by code inspection; load reproducer still required.  
**Evidence:** [`federation/delivery/url.rs`](https://github.com/estyseesghosts/pinetree/blob/ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e/src/federation/delivery/url.rs).

Each lookup creates a new OS thread and waits on a channel with a timeout. Dropping the `JoinHandle` detaches the thread; a system resolver call can outlive the caller timeout. Under hostile federation traffic, repeated slow lookups can accumulate live threads.

**Required invariant:** outstanding DNS work and resolver threads stay below a configured hard limit even when every lookup stalls. Saturation returns a bounded transient error; it must not create another thread for every inbound request. A timeout alone does not satisfy this invariant.

### P1 — `backup.max_bundle_bytes` is not wired to enforcement

**Status:** confirmed in current source.  
**Evidence:** [`config.rs`](https://github.com/estyseesghosts/pinetree/blob/ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e/src/config.rs), [`backup/bundle.rs`](https://github.com/estyseesghosts/pinetree/blob/ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e/src/backup/bundle.rs), [`backup/restore.rs`](https://github.com/estyseesghosts/pinetree/blob/ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e/src/backup/restore.rs), [`main.rs`](https://github.com/estyseesghosts/pinetree/blob/ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e/src/main.rs).

The config validates the setting and claims it tightens the cap, but `BackupContext` has no cap field and create/restore use the compile-time 64 GiB `MAX_BUNDLE_BYTES`. A configured lower limit is currently misleading.

**Required invariant:** one explicit policy governs configured bundle creation and restore limits, bounded by an immutable hard safety ceiling. All estimated-size, streaming, final archive, and restore checks enforce it. No config value may silently be ignored.

### P1/P2 — Remote-cache accounting needs an end-to-end invariant test

**Status:** logic is more explicit, but the important interaction remains insufficiently proven by the audit.  
**Evidence:** [`media/reconcile/scan.rs`](https://github.com/estyseesghosts/pinetree/blob/ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e/src/media/reconcile/scan.rs), [`media/remote_cache/evict.rs`](https://github.com/estyseesghosts/pinetree/blob/ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e/src/media/remote_cache/evict.rs), [`media/remote_cache.rs`](https://github.com/estyseesghosts/pinetree/blob/ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e/src/media/remote_cache.rs).

Multiple metadata rows/URL variants can share a content-addressed object. The index, media metadata, refcounts, class/global byte counters, evicted state, and on-demand re-fetch must stay consistent together. The existing missing-cache-byte adjustment uses the remote-cache index as an idempotency anchor; that approach needs a lifecycle test, not just individual helper tests.

**Required test:** two or more remote URLs/variants sharing a blob → evict a reference → re-fetch → reconcile twice → backup → restore. Check every relevant row/index, object, reference count, quota, counter and restore result after each transition. A missing durable blob must be reported as damage, not normalized as an ordinary cache eviction.

### P0 — Orphan sweeping must use a stable view and barrier

**Status:** unresolved consequence of the media lifecycle finding, not merely a low-priority cleanup concern.  
**Evidence:** `collect_live_blob_keys()` pages through separate metadata reads; `sweep_orphan_blobs_checked()` takes the shared media gate for its filesystem deletion. A complete scan flag prevents destructive use of a truncated result but does not make the live-key set atomic with subsequent deletion. An upload can become live after key collection while the sweep still sees its blob as orphaned.

**Required invariant:** a destructive sweep owns the exclusive media lifecycle barrier for both collecting the full live-key set and deleting unmatched files. All logical media writers must hold the shared barrier for their complete DB+filesystem transition. Incomplete/failed walks delete nothing.

### P2 — Backup retention, staging failures, and status memory are unbounded

**Status:** operational gap.  
**Evidence:** [`backup/mod.rs`](https://github.com/estyseesghosts/pinetree/blob/ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e/src/backup/mod.rs), [`backup/bundle.rs`](https://github.com/estyseesghosts/pinetree/blob/ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e/src/backup/bundle.rs), [Operations guide](https://github.com/estyseesghosts/pinetree/blob/ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e/docs/wiki/Operations.md).

A per-bundle cap does not constrain total backup-directory usage. Successful backups and status files can accumulate indefinitely; the in-process status registry also retains loaded statuses in an unbounded map. Preflight size estimates cannot guarantee space at write time. All `ENOSPC` paths, interrupted `.partial` cleanup, and directory durability need fault-injection tests.

### P2 — Production diagnostics are improved but incomplete

**Status:** acknowledged gap in the current Operations guide.  
**Evidence:** [Operations guide](https://github.com/estyseesghosts/pinetree/blob/ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e/docs/wiki/Operations.md).

The admin surface intentionally reports `null` for several unavailable aggregates: oldest pending delivery/retry age, queue/job breakdowns, quarantine/federation counts, storage drift/backfill status, disk/free-space values and redb bytes. Some missing fields are acceptable early in development; they are not adequate as the only way to run a production federation server. Add the counters that permit detection of stalled work and broken media/backup state, or clearly retain a documented unqualified status until they exist.

### P2 — Malformed network inputs can reach panic paths

**Status:** currently documented, but individual paths must be revalidated and made reproducible before assigning a final defect count.  
**Evidence:** [Operations guide](https://github.com/estyseesghosts/pinetree/blob/ad4f7b8daaf89ca78783bc0dbdb04b3e2abb034e/docs/wiki/Operations.md) says malformed WebSocket/actor input can panic.

Audit the concrete callers and ensure untrusted input maps to a typed error, HTTP error or WebSocket close frame. A worker panic must not cause the same durable job to be reclaimed and retried forever. Add fuzz/property tests for parsers plus process/worker-survival tests.

### P1 — Role-policy behavior must not imply enforcement that does not exist

The repository documentation says custom role policy documents are persisted but authorization remains based on built-in admin/moderator checks. This is not necessarily a defect if custom roles are explicitly storage-only/not an authorization boundary. It becomes a security defect if the API/UI presents those policy documents as enforced access control. Before release, either implement one central permission evaluator and negative tests for every protected action, or make the non-enforcement unmistakable and disable misleading assignment surfaces.

### Release gate — Not qualified by repository metadata alone

The audit’s smoke-runner fixes look substantive, and prior targeted test counts are useful evidence. The latest combined commit-status response has no status records; that is **absence of qualification evidence**, not proof that tests failed or that no GitHub Actions ran. No local build, live federation run, stress test or restore drill was executed during this review.

Release must qualify the exact SHA with strict, non-skipping checks. If Docker, required reference instances or toolchains are unavailable, report the release as **UNQUALIFIED**, never as a full pass.

## 3. Findings from older audits that should not be blindly reopened

The following earlier issues appear to have been addressed in current code or explicitly scoped. Close them only with a targeted regression test and source link in the state log:

- Signature timestamp overflow and configured RFC 9421 enforcement: latest `3c9e8f6`; keep interoperability as an open qualification gate.
- Media pagination and empty-page termination: latest `5423ab3`; continue concurrency testing. Pagination correctness does not provide snapshot isolation.
- Smoke-runner pipefail, stale-binary detection, step logs and strict skip behavior: latest `b574785`; prove the gate with intentionally failing fake commands and missing prerequisites.
- Default transcoding disabled; explicit FFmpeg runtime target and startup probing: latest `80dc409`; still needs a provisioned Docker test of both runtime targets.
- Bounded rate-limit and upload-slot maps: latest `65059e0`; verify all sensitive/authentication handlers actually consume the limiter, rather than treating bounded maps as proof of complete coverage.
- Password hashing upgrade, request-body limits, trusted-proxy-aware client IP, chunked response parsing, remote actor key ownership checks, and locked-follow request state have changes in current code/current docs. Do not repeat their pre-fix claims without new counter-evidence.
- Missing Misskey features (e.g. intentionally omitted chat/channels, passkeys/WebAuthn stubs, thumbnails/range variants) are compatibility/product-scope decisions unless the server advertises them as supported or they compromise security. Keep them in a separate parity backlog; do not mix them with data-integrity release blockers.

Historical audit docs can retain their original context, but their status must be stamped `PLAN HISTORY` or `SUPERSEDED` so an agent does not implement an already-landed bugfix a second time.

## 4. Agent operating rules

1. Work from the exact current `master` head; record `git rev-parse HEAD` at the start and end of each batch. Do not assume the audit SHA remains current when implementation begins.
2. For every defect: read the named code and related port, store contract, docs and tests; add a failing regression test; then fix it; rerun the targeted test before broader suites.
3. Keep changes small and independent. One agent/workstream may own a core file at a time. Do not concurrently edit `src/main.rs`, `src/config.rs`, `src/application/ports.rs`, `src/persistence/redb/*`, or overlapping `src/media/*` files.
4. Preserve API and federation compatibility unless a documented security invariant requires a change. For a behavior change, update endpoint manifest, configuration reference, Operations/Security wiki and tests in the same batch.
5. Never weaken assertions, mark required tests optional, silently skip missing prerequisites in strict mode, or infer test success from an old binary/log. Save complete logs and exact commit SHA.
6. Never copy a live `pinetree.redb` file as a backup. Use the in-process snapshot/export path, then restore offline into a new directory.
7. For every batch, write a short `state/logs/YYYY-MM-DD-<batch>.md` report: finding, invariant, source paths, tests/commands and their output, commit, remaining risks.

## 5. Implementation order

Do not reorder phases so that Docker polish or API parity work delays data integrity and authentication fixes.

### Phase 0 — Establish an executable baseline and validate security paths

**Owners:** lead agent plus one read-only reviewer. Do not start parallel source edits yet.

- Record commit, working tree state, Rust version, target OS, Docker version and available external dependencies.
- Run targeted current tests and full offline suites; record failures separately from environment/toolchain prerequisites.
- Reproduce the media races and TOTP discrepancy before editing. If a stated concern cannot be reproduced and source semantics contradict the audit, amend/close it explicitly.
- Inspect every sign-in path (`/signin`, `/api/signin`, `/api/signin-flow`, MiAuth, `auth/session/*`, and `i/2fa/*`) as one flow. Confirm whether enabled TOTP blocks password-only issuance, whether TOTP completion reflects in the packed user and sign-in hint, and whether `i/2fa/authenticate` requires a one-time first-factor challenge. Verify whether `auth/session/generate` accepting any `appSecret` remains intentional and what `userkey` actually validates. Do not trust stale comments in isolation.
- Confirm all auth paths are rate-limited with the intended trusted-proxy/IP rules. Verify all security config keys map to runtime behavior.

**Acceptance:** a baseline report lists current pass/fail/skip results with no hidden skips; each P0 finding has a failing test, an explicit source trace, or a documented reason it was closed.

### Phase 1 — Fix the media lifecycle as one concurrency design (P0)

**Primary files:** `src/application/commands/media.rs`, `src/media/storage.rs`, `src/media/reconcile/{scan,heal}.rs`, `src/media/remote_cache/{fetch,evict,store}.rs`, relevant `MediaStore`/`BlobStore` ports and redb adapters.

1. Define one documented lock order, for example: **global media lifecycle gate → per-blob-key lock → short redb transaction → filesystem operation**. No path may acquire the global gate while holding a DB transaction or reacquire a non-reentrant guard recursively.
2. Hold the shared lifecycle gate across the *whole* logical media transition, not merely `FsBlobStore::put/delete`: blob write + metadata/refcount/index/quota commit; record removal + refcount decrement + zero-ref deletion; cache fetch/refetch + index updates; cache eviction + accounting; sidecar repair.
3. Add per-blob-key serialization so concurrent upload/delete/refetch/eviction operations for the same SHA-256 key cannot interleave. A practical design is a deferred-GC job: decrement refcounts in the same redb transaction as metadata removal, then under the per-key lock recheck authoritative references immediately before deleting bytes. Upload must take the same per-key lock before writing bytes and creating its reference. Bound/retire the key-lock map so arbitrary keys cannot grow process memory forever.
4. Separate guarded public APIs from internal `_under_guard` methods; avoid nested `acquire_media_op()` calls/deadlocks when the application service already holds the lifecycle guard.
5. For a destructive orphan sweep, acquire the exclusive global lifecycle gate before collecting the complete live-key set and retain it through the filesystem sweep. Do not take the live-key set before the barrier. If any page/read/filesystem step fails or the scan hits its cap, delete nothing.
6. Define pool/refcount/accounting semantics for shared content across local owners and remote URL variants. Durable blobs must never be deleted by cache eviction. Missing durable bytes must stay visible and be reported for repair, not marked as routine cache eviction.
7. Keep redb transactions short; never hold a DB write transaction over slow network I/O. Remote network fetch happens into a capped staging object, then the coordinated commit installs bytes/index/metadata.

**Required regression tests (deterministic barriers/channels, not sleeps):**

- Same bytes: pause delete after zero-ref transition; upload identical bytes; resume delete. Final live row must have readable bytes.
- Shared durable blob across two owners: delete either owner in both possible orders; the remaining owner can still read the blob. Delete last owner; object becomes collectible.
- Upload paused after blob put but before metadata commit while orphan sweep starts; sweep must wait or preserve the object, and final row must have bytes.
- Cache blob referenced by multiple URL/variant rows: evict one, refetch, run reconciliation twice; no duplicate indexes, negative counters or missing live object.
- Crash/failure injection between each blob/metadata/refcount/accounting step; restart + reconcile converges safely.
- Reconciliation with a deliberately incomplete scan proves it deletes nothing.
- Concurrent upload/delete/eviction/reconcile stress; after quiescence, every live non-evicted row has bytes and sidecar, refcounts/accounting match authoritative rows, no counter is negative.

**Acceptance:** all focused tests pass repeatedly; no code path can physically delete a key on a stale live-key snapshot; all media writes/removals are proven to participate in the same coordination scheme.

### Phase 2 — Make TOTP and all authentication/session flows secure (P0)

**Primary files:** `src/api/misskey/endpoints/auth_http/signin_flow.rs`, `auth_http/signup.rs`, `dispatch_auth/{password,twofa}.rs`, `src/application/commands/auth_2fa.rs`, `src/api/misskey/packers/user.rs`, auth ports/stores/tests.

- Add failing tests showing whether `i/2fa/done` enables a persisted, verified secret and whether `MeDetailed.twoFactorEnabled`/passwordless properties reflect it.
- Replace password-only sign-in behavior for enabled-TOTP accounts with the compatible second-step/challenge flow. Never accept the password-only success path if the stored account policy requires TOTP.
- Bind the second step to a short-lived, one-time, unguessable challenge issued only after password verification. Bind it to the account and flow; expire it; consume it atomically on success; do not accept arbitrary `userId + TOTP` as a fresh login unless that is explicitly intended and security-reviewed. A second-factor endpoint must not let replay mint multiple sessions.
- Keep account status checks, rate limits and audit history consistent across the first and second factors. Enforce per-IP and per-account failed-attempt caps without permanent account lockout.
- Ensure passwordless-login mode, if enabled, has explicit documented semantics and cannot unintentionally downgrade an account that enabled ordinary TOTP.
- Reconcile app-secret/app registry behavior: either ensure unregistered secrets are rejected at the point that makes them security-relevant, or document and test the intentional upstream-compatible legacy flow. Do not leave a claim of `NO_SUCH_APP` enforcement when that path cannot emit it.
- Add tests for missing/wrong/expired/replayed TOTP challenge, valid two-step login, password-only rejection for TOTP-enabled account, disabled-TOTP behavior, suspended/deleted accounts, app-secret invalid/mismatch, trusted-proxy spoof attempts, and throttle behavior across aliases.

**Acceptance:** enabled second factor cannot be bypassed with password-only login; a TOTP token cannot be replayed to mint additional sessions; API status/response and packed user state agree with persisted security settings.

### Phase 3 — Make the online backup a consistent, bounded transaction (P1)

**Primary files:** `src/backup/{mod,bundle,restore}.rs`, `src/config.rs`, `src/main.rs`, redb snapshot export interface, restore CLI and backup integration tests.

1. Keep the exclusive lifecycle barrier for the entire database export + media copy + verification + publication. This is only correct after Phase 1 makes every logical media operation participate in the shared side of that barrier.
2. Generate the `MEDIA` blob-reference inventory **inside the same redb read transaction** that exports the table snapshot. Extend `export_backup_snapshot` or add a typed snapshot object so the media table rows and blob keys are read once. Do not call `list_all_media()` after export and call it the same snapshot.
3. Compare every non-evicted media reference in that snapshot with copied objects and sidecars in the appropriate pool. Any missing required durable blob must fail backup creation and prevent publication; preserve an error report for diagnosis. Define explicitly whether an evicted cache row needs bytes (normally no) and test it.
4. Add `max_bundle_bytes` to a shared backup policy/context and pass the setting from `Config` in `main.rs`. Validate `0 < configured cap <= hard absolute cap`. Enforce at source preflight, every streamed byte/write boundary, final archive size, and restore input size. Expose the restore policy to the CLI too, while retaining a compiled hard ceiling for untrusted archives. A mismatched or omitted config must not silently bypass the hard ceiling or pretend to honor the configured limit.
5. Preserve versioned format/schema checks, table registry equality, checksums, redb integrity checks, fresh-target requirement, sidecar validation and `RESTORE_READY.json`. Inspect ZIP path/name constraints, duplicate entries and declared-vs-actual expanded sizes; reject malformed bundles before writing outside the fresh directory.
6. Make publication crash-safe: finish/sync file, verify it, rename atomically, and sync the containing directory where supported. No incomplete `.partial-*` may be listed as a completed backup.
7. Audit every error path for disk-full, permission loss, interrupted copy, checksum mismatch and restore failure. Clean staging files on ordinary failures; startup cleanup must safely handle process-kill leftovers. A failed backup must never be reported as completed.

**Required tests:** concurrent media insert/delete/eviction while backup is scheduled; snapshot rows match inventory; missing durable object aborts publishing; evicted remote cache object is handled as designed; cap just below/above boundary for writer and restore; malicious ZIP paths/duplicates/oversized entries; injected `ENOSPC`; process-kill during staging/after fsync/before rename/after rename; restore into fresh directory and reopen DB + verify all bytes; reject pre-existing target; verify `RESTORE_READY.json` is written last.

**Acceptance:** a live backup created from a changing instance restores into a new directory, the restored DB passes integrity checks, all referenced durable media can be served, and the bundle is marked complete only after all checks pass.

### Phase 4 — Bound DNS and inbound resource consumption (P1)

**Primary files:** `src/federation/delivery/url.rs`, federation HTTP client, runtime configuration, delivery worker tests.

- Replace spawn-per-lookup with a bounded resolver-worker pool or a hard semaphore before thread creation. If using semaphore + blocking resolver thread, transfer the permit into the resolver closure so a caller timeout does not release capacity while the OS resolver is still blocked.
- Set a hard maximum for in-flight resolver work and optionally a bounded queue. When full, fail fast with a classified transient/resource-busy error; never block request threads indefinitely and never spawn an unbounded replacement.
- Use an injectable resolver for tests. Verify timeout, saturation, cancellation semantics, shutdown behavior, IPv4/IPv6 results, NXDOMAIN classification, and DNS re-resolution across redirects without weakening SSRF/IP validation.
- Review overall connection/response deadlines, redirect limits, request/response sizes, in-flight remote-media downloads and upload semaphore behavior under sustained slow peers. Ensure every limit applies before expensive allocation or work is scheduled.

**Acceptance:** stress tests with all resolver slots stalled show in-flight work and OS-thread count never exceed the chosen bound; new requests reject promptly; service remains responsive; errors are classified and counted.

### Phase 5 — Cache/reconcile integration proof and backup retention (P1/P2)

- Add the full remote-cache lifecycle test specified in Phase 1 and run it through backup/restore.
- Add configured backup rotation/retention: maximum count and/or aggregate bytes, with a safe minimum keep count and a documented age policy. Never delete the only known-good backup before a newer archive has passed verification and durable publication. Permit retention to be disabled explicitly.
- Bound the in-memory backup status registry with a documented policy; durable status files/history and archive retention must agree. Do not let each status lookup grow an unbounded HashMap for the lifetime of the process.
- Check free disk space as an advisory preflight (not a guarantee); handle `ENOSPC` correctly at all writes. Optionally place backups on a distinct mounted volume. State clearly that backups on the same disk do not protect from disk loss.
- Run crash/restart tests for queued/running backup status and abandoned staging cleanup.

**Acceptance:** repeated backup jobs do not exhaust configured storage or unbounded process memory; status remains queryable for retained jobs; no failure path publishes a partial archive or deletes all recoverable backups.

### Phase 6 — Close production safety gaps and improve diagnostics (P1/P2)

**Panic containment:** locate and fix remaining panic/expect/unchecked-index paths reachable from untrusted HTTP, ActivityPub actor JSON, WebSocket frames and durable-job payloads. Parse failures must produce typed errors/close frames. Add fuzz/property tests and tests proving a malformed message cannot crash the process or wedge the worker in an endless reclaim/retry loop.

**Readiness:** preserve `/healthz` as liveness and `/ready` as bounded readiness. Test redb read/integrity failure, missing/unreadable storage roots, and shutdown/dependency failure. Do not run full disk walks or unbounded database scans in a readiness request.

**Actionable admin diagnostics:** prioritize:
- oldest pending delivery and retry age; queued/running/failed job counts;
- remote-cache bytes by class, eviction count/rate, refcount/accounting drift, missing durable blobs and last reconciliation/backfill result;
- backup last success/failure, duration, output bytes and last restore drill result;
- DNS in-flight/capacity/timeouts, outbound HTTP timeout/error classes;
- redb/storage errors and readiness failures.

Add stable bounded metrics/counters and structured logs, preserving current redaction rules. Unknown metrics may remain `null` only when the UI/docs clearly state they are unavailable and the release checklist treats a missing critical signal as unqualified.

**Roles:** make `admin/roles/*` behavior explicit. If custom policies are not enforced, say so in endpoint docs/UI and do not present them as security boundaries. If implementing enforcement, use one central permission evaluator and negative tests for every protected admin action; do not sprinkle partial role checks across handlers.

**Acceptance:** invalid input yields controlled failures, readiness turns unhealthy when a critical dependency is unavailable, operators can diagnose stalled federation and media/backup integrity without reading raw internal tables, and custom-role semantics cannot mislead an operator about authorization.

### Phase 7 — Strict release qualification on the exact commit (final release gate)

Run and save logs for the exact candidate SHA in a provisioned environment:

1. `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` under the supported production/default feature set, and the full offline unit/integration/conformance/crash suites. Test additional supported feature combinations separately; do not enable out-of-scope FoundationDB/native features merely to satisfy `--all-features`.
2. `SMOKE_ALL_REQUIRED=1 bash scripts/smoke-all.sh`; verify `SMOKE_ALL_OK` only occurs when every required phase actually ran and passed. Intentionally fail a fake Cargo command and remove a required prerequisite to prove the gate cannot report a false green.
3. Build and launch **both** Docker runtime targets (minimal and FFmpeg-enabled); exercise health/readiness, signup/signin, media upload/serve/delete, remote cache fetch/eviction, shutdown/restart and persistent-volume behavior. Transcoding is disabled by default; explicitly enable it in the FFmpeg-target test config and verify startup probe + actual transcoding. Also verify explicitly enabled transcoding fails clearly in an image without FFmpeg.
4. Run strict Misskey `2026.10.1-alpha.0` and Sharkey `2025.4.7` interoperability suites pinned by the project. Add at least one public/external ActivityPub instance or independent federation test fixture when permitted by environment. Test inbound signed activities, outbound signed deliveries, follow/Accept/Undo, public note/reply/delete, reactions/renotes, blocked hosts, retry/dead-letter behavior and remote-media fetch.
5. Repeat race/stress tests with high concurrency and injected faults. Include process termination at media lifecycle and backup publication boundaries, slow/hung DNS, and disk-full behavior.
6. Perform a real live backup and offline restore into a fresh directory. Start a process from the restored directory; check redb integrity; verify all non-evicted media, indexes, counters, auth/TOTP state, jobs and federation recovery; serve restored media; retain the old data directory until all checks pass.
7. Capture exact SHA, OS/architecture, Rust/Docker/FFmpeg/reference-server versions, commands, per-phase exit status and artifacts. No required phase may be a skip.

**Release decision:** any P0/P1 regression, failed required phase, unavailable required evidence, unrestored media, false-green sentinel, or unresolved auth policy means **NO-GO / UNQUALIFIED**. A developer-machine partial smoke pass is not a production release.

## 6. Final acceptance checklist

Do not call Pinetree production-ready until every item below is evidenced on the candidate SHA:

- [ ] Media upload/delete/refetch/eviction/reconciliation cannot race into missing bytes; shared-blob refcounts and byte counters are exact after quiescence and restart.
- [ ] Orphan sweep uses an exclusive barrier around both complete reference discovery and deletion; incomplete scans delete nothing.
- [ ] Enabled TOTP cannot be bypassed by password-only sign-in; second-factor challenges are short-lived, one-time, and account-bound; packed security settings reflect storage.
- [ ] DNS work/thread count is bounded even when all lookups stall.
- [ ] Backup table snapshot and media inventory are from the same DB snapshot; missing required blobs prevent publication; restore is actually exercised.
- [ ] Configured backup cap is enforced by both creation and restore, with a hard absolute ceiling; disk-full and crash paths do not publish partial archives.
- [ ] Backup retention and status memory have explicit bounds.
- [ ] Untrusted malformed input cannot panic the server/worker or cause endless job retries.
- [ ] Readiness and admin diagnostics expose actionable critical health signals; custom-role enforcement semantics are accurate.
- [ ] Strict smoke, both Docker targets, live pinned federation and live backup/restore drill pass for the exact SHA.
- [ ] All incomplete functionality remains clearly listed as a deliberate divergence/non-goal, separate from unresolved production defects.

## 7. Report format for each agent batch

Every implementation batch returns:

1. **Finding and invariant** addressed.
2. **Files changed** and why.
3. **Failing-first test** added, with the before/after command output.
4. **Tests run** and exact counts; explicitly list skipped suites and their cause.
5. **Commit SHA** and state-log path.
6. **Unresolved issues** or documented scope decisions.

Do not report “fixed” without a reproducer or invariant test. Do not report “production-ready” until Phase 7 is complete.
