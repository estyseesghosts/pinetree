Worked for 13m 54s

### Pinetree — fresh production-readiness audit

Audited HEAD: [`7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca`](https://github.com/estyseesghosts/pinetree/commit/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca?utm_source=chatgpt.com) Latest commit: October 22, 2026, 21:51 UTC. Change since the previous audit: three new commits, including fixes for cache eviction, concurrent TOTP redemption, backup retention, and diagnostics.

Verdict: still not production-ready. Several of the previous findings are fixed, but this pass uncovered a cache-accounting defect that can gradually disable remote-media caching, an eviction algorithm that can stop finding reclaimable data, and a resource-exhaustion risk in the public file-serving path. Those are in addition to a few concurrency and compatibility issues.

This was a read-only source audit. I did not runtime-reproduce the new findings; race findings below are based on the current code paths and their locking/transaction boundaries.

### P1 — Blockers before production

### 1\. Successful remote-media fetches leak cache reservations

Sources: [`fetch.rs`](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/src/media/remote_cache/fetch.rs?utm_source=chatgpt.com#L142-L150), [`fetch.rs` success path](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/src/media/remote_cache/fetch.rs?utm_source=chatgpt.com#L375-L412), [`media_store.rs`](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/src/persistence/redb/media_store.rs?utm_source=chatgpt.com#L545-L590)

The fetch path reserves `max_file_size` before downloading. On successful completion, it adds the actual downloaded size to stored-cache accounting, then releases only:

`max_file_size - actual_size`

That leaves `actual_size` in `in_flight_bytes`, even though those same bytes have already been committed to `total_bytes`.

The reservation check explicitly counts both:

`total_bytes + in_flight_bytes + requested_bytes`

So each successful non-empty fetch double-counts its own actual bytes against the cap. Eviction reduces stored bytes, but it does not clear these leaked reservations. The runtime resets in-flight accounting at startup, so a restart can temporarily hide the problem rather than fix it.

There is a second leak in the same function: after reservation succeeds, several fallible operations can return through `?` before entering the closure whose error branch releases the reservation. A temporary database error at one of those points can leak the full reservation.

Impact: normal cache use progressively consumes the apparent capacity even when the physical cache is small or has been evicted. Eventually, new remote-media requests can fail with `CACHE_CAPACITY_EXCEEDED` despite space being available.

Fix: make successful completion an atomic reservation-to-storage accounting transition: subtract the entire reservation and add the actual stored size exactly once. Put every post-reservation exit—including early `?` paths—under reliable cleanup. Do not silently ignore failures to release reservations.

Missing regression test: perform multiple real cache fetches, verify `in_flight_bytes` returns to zero after each successful fetch, evict the resulting files, and confirm new reservations still succeed. Inject storage failures at each point between reservation and the existing cleanup closure.

### 2\. Cache eviction can become stuck after a small number of old entries

Sources: [`evict.rs`](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/src/media/remote_cache/evict.rs?utm_source=chatgpt.com#L52-L100), [`media_store.rs`](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/src/persistence/redb/media_store.rs?utm_source=chatgpt.com#L294-L332), [`try_evict_snapshot`](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/src/media/remote_cache/evict.rs?utm_source=chatgpt.com#L100-L170)

The eviction algorithm repeatedly considers only the first eight LRU index records. There are two related problems.

First, intentionally evicted records remain candidates. The new implementation preserves cache-index rows after eviction, setting `bytes_absent = true` and `size_bytes = 0`. But candidate selection does not filter those rows out, and eviction does not advance their access timestamps. The zero-byte entries therefore remain among the oldest candidates.

`ensure_cache_budget` tracks keys it has already tried, but it continues querying the same eight oldest records. Once those records have been processed—or if the oldest eight are already absent—it can stop making progress without considering newer, still-evictable entries. The current structure also limits the number of distinct keys it can process in one call to eight.

Second, shared blobs can be impossible to evict. `try_evict_snapshot` re-reads every index row sharing the blob key, but then requires every one of those rows to appear in the original candidate list. That list contains at most eight records. If a blob has more than eight URL/variant aliases—or even a smaller number where one alias falls outside the oldest-eight window—the eviction is skipped.

Together, these can make a cache reservation fail even when reclaimable bytes exist on disk.

Fix: enumerate eligible, charged physical blob keys rather than selecting a tiny window of raw index rows. Filter out evicted/zero-size rows and paginate or iterate through eligible keys until enough space has been reclaimed. Once a candidate key is locked, fetch the complete current alias set for that key; do not require all aliases to have appeared in the candidate window.

Missing regression tests: more than eight historical tombstones followed by new live entries; a reservation requiring more than eight evictions; and a physical blob shared by more than eight URL/variant rows, including aliases with different access times.

### 3\. Public file-serving requests perform blocking I/O on the async runtime and buffer entire files

Sources: [`files_http.rs`](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/src/api/misskey/endpoints/files_http.rs?utm_source=chatgpt.com#L145-L193), [route registration](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/src/api/misskey/endpoints/mod.rs?utm_source=chatgpt.com#L240-L244), [`SafeHttpClient`](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/src/federation/delivery/client/send.rs?utm_source=chatgpt.com#L20-L100), [`FsBlobStore::get`](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/src/media/storage.rs?utm_source=chatgpt.com#L270-L330)

`GET /files/{id}` routes directly to an `async fn`, but the handler performs synchronous blob reads and, on a cache miss, calls the synchronous remote fetch pipeline. The HTTP client uses blocking socket/process operations. The handler does not move this work to a blocking pool.

This means a slow remote-media fetch can occupy a Tokio worker for the duration of the request. A burst of cold or evicted media requests can tie up runtime workers while ordinary API requests wait.

The same path also reads complete files into `Vec<u8>` values before returning the response. There is no equivalent of the upload concurrency slots limiting simultaneous public file downloads. Concurrent requests for large local or remote files can therefore consume substantial RAM, even when the cache's on-disk size is capped.

There is also a smaller correctness issue in the same handler: after a successful refetch, it uses the refreshed blob key for the bytes but constructs `Content-Type` and media security headers from the pre-fetch metadata. If the MIME type changed during refetch, the first response can carry stale headers.

Fix: stream file responses instead of buffering every file in memory. Move unavoidable blocking filesystem/network operations off Tokio worker threads and put bounded concurrency around cold-cache fetches. When a refetch succeeds, use the refreshed metadata to construct the response headers.

Required test: stress the file endpoint with slow cold origins and concurrent large-file requests while checking health/API latency, memory usage, and error rates.

### P2 — Correctness, compatibility, and operational risks

### 4\. Reconciliation can delete a newly refreshed remote-cache index entry

Source: [`scan.rs`](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/src/media/reconcile/scan.rs?utm_source=chatgpt.com#L260-L305)

The dangling-index reconciliation pass obtains candidates, then locks the candidate's old blob key. If its media row is absent, it checks references to that blob and ultimately removes the index row by `(source_url, variant)`.

It does not re-read that exact URL/variant index row and verify it still matches the candidate after acquiring the lock.

A possible sequence is:

* Reconciliation lists a stale index row pointing at `K_old`, whose metadata row is missing.

* A concurrent refetch of the same URL obtains new content under `K_new` and replaces the URL/variant index row.

* Reconciliation, still operating on the stale candidate and holding the lock for `K_old`, deletes the URL/variant row—which now points to `K_new`.

The fetch and reconcile operations can hold shared lifecycle gates while holding different per-key locks if the content changed. The old-key lock alone does not serialize them.

Impact: fresh index state can be removed, leaving new metadata and bytes unindexed and making subsequent requests refetch unnecessarily.

Fix: use a conditional, atomic removal that succeeds only when the current row still matches the expected `media_id` and `blob_key`. If the candidate changed, reconciliation must skip it and leave the new entry intact.

### 5\. Actual file serving does not update the cache's LRU timestamp or hit count

Sources: [`files_http.rs`](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/src/api/misskey/endpoints/files_http.rs?utm_source=chatgpt.com#L145-L193), [`fetch.rs`](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/src/media/remote_cache/fetch.rs?utm_source=chatgpt.com#L113-L139), [`media_store.rs`](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/src/persistence/redb/media_store.rs?utm_source=chatgpt.com#L268-L291)

When the file bytes are present, `serve_file` reads and returns them directly. It does not call `remote_cache_touch` or increment the cache-hit counter. The helper that does update those fields is only called by the serving path when bytes are absent, so ordinary successful `GET /files/{id}` requests do not make an object more recent in the LRU index.

The eviction guard also compares `last_access_ms` and `fetched_at_ms`, but not `access_count`; its validation is not serialized against every path that updates the index.

Impact: frequently served objects can be treated as old and evicted, while hit/access diagnostics under-report normal traffic. A touch within the same millisecond as the candidate snapshot can also fail to register as newer.

Fix: make the normal cache-hit path update the access record, and coordinate that update with eviction using the same locking protocol or a durable generation/CAS check. Compare a monotonic generation or other change indicator, not timestamps alone.

### 6\. Per-class cache accounting is inconsistent for AudioVideo and shared blobs

Sources: [`media_store.rs`](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/src/persistence/redb/media_store.rs?utm_source=chatgpt.com#L497-L542), [`evict.rs`](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/src/media/remote_cache/evict.rs?utm_source=chatgpt.com#L135-L170)

`cache_accounting_apply` maps `MediaClass::AudioVideo` into `stats.full_bytes`, while the eviction policy treats AudioVideo as governed by the global cap rather than the Full-class budget. Consequently, stored audio/video bytes contaminate the Full-class usage figure and can influence Full-class eviction decisions.

There is a second discrepancy for content-addressed blobs shared by multiple URL/variant rows with different classes. The eviction path chooses a `charge_class` based on the largest index-row size, with ties dependent on iteration order. That is not necessarily the class originally charged when the physical blob was fetched. Eviction can therefore subtract bytes from the wrong class counter.

Impact: total-byte accounting may look right while class-level statistics drift, changing which classes the policy considers over budget.

Fix: persist one authoritative accounting class per physical blob key or calculate per-class totals using a consistent physical-key attribution rule. AudioVideo must not silently count as Full if the policy treats it as a separate, global-only class.

### 7\. `usePasswordLessLogin` is exposed but not implemented in the sign-in path

Sources: [`auth_2fa.rs`](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/src/application/commands/auth_2fa.rs?utm_source=chatgpt.com#L190-L195), [`signin_flow.rs`](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/src/api/misskey/endpoints/auth_http/signin_flow.rs?utm_source=chatgpt.com#L155-L250), [`user.rs`](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/src/api/misskey/packers/user.rs?utm_source=chatgpt.com#L560-L585)

`set_2fa_passwordless` persists the `passwordless` flag, and the account packer advertises it through `usePasswordLessLogin`. But the sign-in flow checks whether TOTP is enabled and then still requires a password before presenting the TOTP challenge; it does not use the second value returned by `totp_flags`.

Thus the API reports a configuration option that currently has no effect on the sign-in flow.

Fix: implement the intended Misskey passwordless behavior and test it end to end, or stop advertising the feature as enabled until it is supported.

### 8\. The newest commit commits a machine-specific Cargo target directory

Source: [`.cargo/config.toml`](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/.cargo/config.toml?utm_source=chatgpt.com#L1-L10)

The tracked config hardcodes:

`C:/Users/julie/.cargo/shared-target`

That may work on the author's Windows system, but on Unix it is a relative path, so Cargo can place substantial build artifacts in a `C:/...` directory under the checkout. That path is not covered by the normal `/target/` ignore rule. On another Windows machine, the folder may not be writable or meaningful.

Adding `/.cargo/` to `.gitignore` does not solve this because the config file itself is already tracked.

Fix: remove the machine-local target directory from tracked config. Set `CARGO_TARGET_DIR` in the local environment or development scripts; share the target directory without prescribing one user's absolute path to every checkout.

### 9\. Metadata and backup-status scans still grow with history

Sources: [`media_store.rs`](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/src/persistence/redb/media_store.rs?utm_source=chatgpt.com#L294-L358), [`backup/mod.rs`](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/src/backup/mod.rs?utm_source=chatgpt.com#L340-L388)

The remote-cache index retains evicted metadata rows for refetch, but there is no visible retention/purge policy for old zero-size tombstones. Candidate selection and `cache_entries_for_blob_key` scan the remote-cache table rather than using an index keyed by blob key. With many unique remote URLs, these scans become increasingly expensive; every additional tombstone also worsens the eviction-selection issue described above.

Separately, `newest_backup_statuses` first loads and sorts the full status history through `list_backup_statuses`, then truncates to the requested cap. The admin surface may show only 20 results, but the underlying disk scan and memory use are still proportional to the total number of statuses. With backup retention unset, history can grow indefinitely.

Fix: add appropriate indexes/paging for remote-cache lookups and define a safe stale-metadata retention policy. Make backup-status retrieval bounded or maintain a durable recent-status index; do not rely on output truncation to bound the work.

### Smaller remaining issue: streaming updates can be silently dropped on storage errors

The checked-in follow-up log itself identifies this as outstanding. In [`streaming/server/connection/dispatch.rs`](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/src/streaming/server/connection/dispatch.rs?utm_source=chatgpt.com), account lookup and `pack_me_detailed` errors are converted to `None` via `.ok()`. A transient TOTP-state read error can therefore suppress a `MeUpdated` payload without surfacing the cause or retrying. This is fail-closed rather than an authentication bypass, but connected clients can retain stale account/security state until another update or reload.

### Rechecking the previous findings

The following earlier findings appear fixed in current source, rather than merely documented as fixed:

* TOTP replay/race: challenge consumption, TOTP-step replay checking, and challenge removal now share one redb transaction. The new race tests are useful coverage.

* Backup consistency and retention: backup media inventory is derived from the same redb snapshot; the configured cap is enforced; malformed media metadata/sidecar inconsistencies fail closed; retention preserves status when ZIP removal fails; diagnostics now select the newest statuses.

* Media lifecycle operations: the upload/delete/update paths use lifecycle and per-key coordination, and the main orphan sweep holds an exclusive barrier across live-key collection and sweeping. This does not resolve the distinct stale-index reconciliation race above.

* DNS resource exhaustion: the resolver now bounds concurrent DNS work and thread/resource use.

* Older federation/security findings I rechecked: key IDs are now tied to actor ownership and inbox processing compares the activity actor with the authenticated signer; locked accounts use pending follow requests; inbound attachment fetching is deferred into a deduplicated job with attachment/byte bounds; HTTPS fetching uses `curl --raw` with its own chunked decoder; new password hashes use Argon2id; proxy headers are honored only from configured trusted proxies; actor collections have cursor-based pagination; and private-key signing is delegated to `ring` rather than using the affected `rsa` private-key signing operation.

Those fixes are real progress. They do not eliminate the new cache-accounting and eviction problems, which affect the core resource limit and must be addressed before relying on that limit in production.

### Test and release qualification

The latest checked-in verification record is [`state/logs/2026-10-22-audit-fix-followup.md`](https://github.com/estyseesghosts/pinetree/blob/7aaaf7749351a1a7b0d520ca3f7592ab1cf515ca/state/logs/2026-10-22-audit-fix-followup.md?utm_source=chatgpt.com).

That log reports clean formatting, the new focused suites passing (20/20), and the targeted cache-eviction suite passing (3/3). However, the full `cargo test --locked --offline --no-fail-fast` invocation still exited 101 because two client/frontend suites could not run without Bun; the log reports 1,159 passed tests among the executed suites. No strict `SMOKE_ALL_REQUIRED=1` run, Docker runs for both transcode configurations, pinned live Misskey/Sharkey federation test, or real backup-after-eviction restore drill was completed. The GitHub status query for HEAD returned no status records, so I cannot treat that as a green CI result.

Release remains unqualified. Fix the reservation accounting, eviction starvation, and public file-serving resource risks first. Then add regression tests for the specific cases above and complete the strict smoke, Docker, live-federation, and fresh-directory backup/restore gates.
