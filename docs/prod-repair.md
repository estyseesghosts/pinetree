Rules for the implementer (non-negotiable)



\- Delegate to subagents. Use the work packages below as the unit of delegation.

&#x20; - Give each subagent a disjoint file set, per AGENTS.md "Subagent delegation".

&#x20; - The parent owns shared files: docs/\*, ports (application/ports/\*), schema and migrations, router, and shared tests.

&#x20; - Subagents propose patch snippets for those files and do not edit them.

&#x20; - Subagents use uniquely named test files, never commit, stash, reset or checkout, and check git status --short first.

\- Follow rust\_style\_guide.html (AGENTS.md calls it .md; the file is .html). Match the surrounding code. Re-read it before each package.

\- Windows native toolchain only: PowerShell and native cargo. Never WSL.

\- Do not trust a slow or green run blindly. Follow AGENTS.md: stream test output, check the binary mtime, and look for deadlocks.

\- New config keys must be in the JSON config, optional, validated, round-trip tested, and documented. No env-var-only settings.

\- Finish by appending a dated state/logs/YYYY-MM-DD-<topic>.md. Claim nothing as verified without executed evidence.



Order and dependencies



WP1 comes first, and WP2 to WP4 depend on it. WP5, WP6, WP7 and WP8 can run in parallel with each other and with WP1 to WP4. WP9 runs last.



WP1: per-blob lifecycle lock (parent designs the port, then a subagent implements)



\- Files: src/media/storage.rs, src/application/ports/blob\_store.rs, src/application/ports/media\_store.rs.

\- Design:

&#x20; - Add a striped lock keyed by blob hash, for example 256 stripes of Mutex.

&#x20; - Add a helper such as with\_blob\_lock(pool, key, closure).

&#x20; - Use separate lock sets for the durable and cache pools.

&#x20; - Lock order is: backup gate, then blob lock, then DB transaction.

&#x20; - Update the lock-order comment in storage.rs:453.

\- Refcounts: split the refcount into durable and cache. Add a BLOB\_REFCOUNT\_CACHE table, register it in the backup registry, bump the schema version, and write a migration that backfills from MEDIA\_META and the remote-cache index. Parent owns the migration.

\- Behavior:

&#x20; - Upload: hold the lock across put, the quota-checked insert, and cleanup on insert failure. If the insert fails and the refcount is 0, delete the blob we just wrote, still under the lock.

&#x20; - Delete: hold the lock across the row delete, the refcount check and blobs.delete.

\- Tests (new files):

&#x20; - A deterministic race test with a barrier, deleting the last reference while uploading identical bytes. Assert that every committed row has readable bytes.

&#x20; - A test that an insert failure after put leaves no orphan.



WP2: fix the bulk-delete bug (small, ship early)



\- File: src/application/commands/moderation.rs:421-433.

\- Change: route the loop through delete\_media-style logic. Use delete\_media\_record, delete the blob only when remaining == 0, and use the WP1 lock. Do not swallow errors. Better, factor one shared private delete\_media\_row\_and\_blob helper and have both delete\_media and this path call it.

\- Test: two users upload identical bytes. Run delete\_all\_files\_of\_a\_user on user A and assert that user B can still read the file.



WP3: eviction



\- Files: src/media/remote\_cache/evict.rs and src/media/reconcile/scan.rs.

\- Changes:

&#x20; - Under the blob lock, evict by blob key. Look up all cache index rows and media rows for the key, delete the bytes once, mark every row evicted = true, and apply accounting once for the key.

&#x20; - Persist evicted = true on the media row during eviction. This needs a new port method such as mark\_media\_evicted(id).

&#x20; - Make fetch\_and\_index\_remote\_media clear evicted when it re-fetches.

\- Tests:

&#x20; - Two source URLs with identical bytes. Evict one and assert the other still serves, accounting converges, and no duplicate rows appear.

&#x20; - After eviction, evicted is true on all sharing rows without needing a reconciler run.



WP4: stop upserts from clobbering cache fields; fix the transcode commit



\- Files: src/persistence/redb/media\_store.rs, src/jobs/transcode.rs, and the port (parent).

\- Changes:

&#x20; - Preserve variant, media\_class and evicted from the old row on update. Take them from the input only on first insert.

&#x20; - Add an atomic commit\_transcoded\_media(id, expected\_old\_blob\_key, new\_row) that:

&#x20;   - fails with NotFound if the row is gone, which stops resurrection;

&#x20;   - fails if the blob key no longer matches, which means a concurrent change;

&#x20;   - adjusts the durable quota by the size delta, and rejects if over quota.

&#x20; - After a successful commit, delete the old blob if its refcount is 0. Do this under the blob lock, for both the original and a preview that ends up unreferenced.

&#x20; - If the commit fails, delete the derived blob when it is unreferenced.

\- Tests:

&#x20; - Delete a file mid-transcode and assert the row stays deleted and no blob leaks.

&#x20; - Transcode a file and assert that the old original blob is removed.

&#x20; - Update a comment on an evicted cache row and assert evicted and variant are unchanged.



WP5: backup coherence



\- Files: src/backup/bundle.rs, src/backup/restore.rs, src/backup/mod.rs, and src/persistence/redb/backup.rs (parent approves the signature changes).

\- Changes:

&#x20; - Collect the referenced-blob set inside export\_backup\_snapshot, in the same read transaction. Do this while emitting the media\_meta rows, then discard the separate paged walk. If that can't be done, fall back to the decoded rows from the same emit callback.

&#x20; - Remove the short-page termination. Anything that remains must stop only on an empty page, and the contradicting doc comment must be fixed.

&#x20; - Canonical evicted policy:

&#x20;   - An evicted cache row never counts as a gap.

&#x20;   - A missing durable blob is a hard error and is recorded as a gap.

&#x20;   - A missing cache blob with evicted = false is a warning. Heal it by marking it evicted in the backup's reference set, but do not fail restore.

&#x20;   - Restore keeps rejecting bundles with durable gaps only.

&#x20; - Enforce backup.max\_bundle\_bytes. Pass the configured value through BackupContext and use it at bundle.rs:403 and :522. Keep MAX\_BUNDLE\_BYTES as the hard ceiling for restore.

&#x20; - Add retention config, for example backup.retain\_count and backup.min\_free\_bytes. Validate it, delete the oldest verified bundles after a successful backup, and refuse to start a backup if free space is below the threshold. Make sure the final filename is only published after verification.

&#x20; - Add a test that fails when a new table is defined but missing from the backup registry. Confirm whether the multimap tables are exported.

\- Tests:

&#x20; - Backup after eviction, then restore.

&#x20; - Backup while media rows are being deleted concurrently.

&#x20; - A configured limit lower than the data size makes backup fail.

&#x20; - Retention keeps the newest N bundles.



WP6: signature timestamp arithmetic



\- File: src/activitypub/signatures/rfc9421.rs:200-215, and the legacy verifier in signatures/legacy.rs if it does similar math (check).

\- Change: use now\_secs.abs\_diff(created) or checked\_sub, and expires.checked\_add(skew). Reject values outside a sane range, for example 0 to 2^40 seconds, before any arithmetic.

\- Tests: i64::MIN, i64::MAX, now + i64::MIN, now - 1, now + 1, and the same for expires. Run them in both debug and --release so the wrap case is exercised.



WP7: bounded unauthenticated inbound work (parent owns the router; subagent implements)



\- Files: src/federation/inbox/mod.rs, src/federation/fetch\_gate.rs, src/federation/delivery/url.rs, src/federation/delivery/resolve.rs, src/runtime.rs.

\- Changes:

&#x20; - Concurrency limiter: add a shared semaphore for key and actor fetches, with a configurable cap, defaulting to something small like 16.

&#x20;   - When the cap is hit, return 503 or 429 and do not queue.

&#x20;   - It must wrap resolve\_and\_verify, covering both the DNS lookup and the fetch.

&#x20; - Per-IP and per-host limits: apply per-IP and per-keyId-host rate limits on the inbox using the existing RateLimiter, before any fetch.

&#x20; - DNS bounding:

&#x20;   - Replace thread-per-lookup with a small fixed resolver pool and a bounded queue, or run lookups through a counted permit and fail fast when none is free.

&#x20;   - A timed-out lookup must still hold its permit until the thread actually exits.

&#x20;   - Enforce the cap on outstanding resolver threads.

&#x20; - Forced refresh: add a per-key cooldown, for example one forced refresh per keyId per N seconds. Do not run the refresh at all when the first resolution came from a fresh fetch. Avoid double-fetching across the legacy and RFC 9421 attempts.

&#x20; - No upsert before verification: change resolve\_remote\_public\_key so an unverified fetch does not write accounts or emoji. Fetch and parse into memory, and verify the signature. Only after success upsert the actor. Keep a short-lived in-memory key cache for the verified result.

&#x20; - Blocking pool: give inbox work its own bounded pool, or a semaphore around spawn\_blocking, so it cannot starve other blocking users.

&#x20; - New settings go in config with validation and tests.

\- Tests: many distinct uncached keyIds with a slow or failing resolver. Assert that thread count and database row count stay bounded, that excess requests are rejected, and that unauthenticated requests create no account rows.



WP8: operations



\- Files: Dockerfile, src/api/misskey/endpoints/middleware.rs, src/observability.rs, admin status.

\- Changes:

&#x20; - Point both HEALTHCHECK lines at /ready.

&#x20; - Extend readiness with a free-space floor on the data and media directories and a bounded check of job-queue age. Keep the checks cheap and bounded.

&#x20; - Fill in the null fields in the admin status surface: queue backlog, oldest pending delivery, federation failures, quarantine, storage conditions. Expose them as metrics or structured events.

&#x20; - Add an index or secondary table for per-owner hash lookup, then replace the full scans in find\_by\_owner\_and\_hash, list\_by\_owner and find\_by\_source\_url. That is a migration, so the parent owns it.

&#x20; - Move the remote-cache staging directory under the pool, for example <cache\_dir>/tmp. Make import\_file do a copy to a temp name and an atomic rename.



WP9: orphan sweep and release gating (last)



\- Do not wire the sweep until WP1 to WP4 are in.

\- Wire a sweep into the ReconcileStorage worker arm that:

&#x20; - takes each blob lock before deleting;

&#x20; - re-checks the refcount under the lock;

&#x20; - skips files younger than a grace period, so in-flight uploads are never swept;

&#x20; - runs only after a complete live-key collection.

\- Add the failure-injection tests from the "Data integrity" table in adversarial.md section 7, then the Docker FFmpeg matrix and the backup and restore drill, so they run in CI as required steps.

\- Run bash scripts/smoke-all.sh in strict mode through the Windows-native path, and compare the binary mtime against the last source edit before trusting it.

\- Verify all of this by executing it. Do not claim exit criteria without output.



\---

