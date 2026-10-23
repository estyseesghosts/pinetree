The previous repair is verified fixed. What follows is only the new and residual issues from `docs/audit-results/00`. Do not reopen slices 1–9 except where a residual names those files.

## Operating rules

- Use subagents for the implementation work. Partition by disjoint files. Subagents share one worktree: they must not edit the same file, must not edit shared files (`src/config.rs`, router/ports, `docs/*`, schema), and must not `git commit`, `stash`, `reset`, or `checkout`. They leave changes uncommitted and use uniquely named test files. The parent merges shared-file snippets and commits.
- Finish each item in full. No "document and defer" unless the audit itself says no code change (ActivityPub index restart, IPv6 fail-closed). A finding is done only when the defect is gone, a regression test fails on the old behavior, and a native `cargo test --locked --offline` log is in `state/logs/`.
- Follow `pinetree/rust_style_guide.html` and `AGENTS.md`. Dependency direction stays `domain ← application ← adapters`. Domain must not import HTTP, Misskey shapes, or DB crates.
- Windows only: native PowerShell and native `cargo`. Never WSL, never `tail` on a diagnostic run, never trust a green run until the test-binary mtime is newer than the sources.
- Commit from the parent after each completed wave, only files that wave owns. Append a log; do not rewrite old logs.
- Do not regress the verified-good list: SSRF core, signatures, inbox 64 KiB plus `actor == signer`, job caps and quarantine, `deny_unknown_fields`, backup traversal and streaming verify, `thread_stack_size(8<<20)`, observability redaction.

Fix order matches the audit: accounting, then remotely triggerable injection and SSRF, then liveness, then atomicity and residuals.

---

## Wave 1 — cache accounting correctness

One agent. These files overlap, so do not parallelize this wave.

**H1.** `src/persistence/redb/reconcile/scan.rs` around 316–348 debits a shared key whenever `remove_if` succeeds, even if `blob_refcount > 0`. One dangling alias removal drops the whole key from `total_bytes` while siblings stay live. Debit only when the last charged alias is removed, or route the removal through `commit_cache_eviction`. Byte deletion stays gated on `blob_refcount == 0`. Extend `tests/reconcile_dangling_cas.rs` (or a new sibling test file): shared key, delete one alias's media row, reconcile, assert `total_bytes` unchanged and the remaining alias still charged; then evict the last alias and assert a single debit of `current.size`.

**H2.** `src/persistence/redb/store_backfill.rs` around 444–516 sums `size_bytes` per `MEDIA_META` row, ignores evicted and absent rows, counts a shared blob N times, and never writes `CACHE_BLOB_CLASS`. Rebuild must be per physical key: skip evicted, absent, and `size == 0`; charge once; write `CACHE_BLOB_CLASS` with the same first-charge rule as `remote_cache_insert`. Add a test that a two-alias shared blob plus one evicted row backfills to one charge and a stored class, not `2 * size`.

**M3.** `src/media/remote_cache/fetch.rs` around 262–263 and 418–437 checks `refcount(old) == 0` and deletes old bytes without the old-key stripe lock. Acquire the old-key lock in a fixed stripe order before the check-and-delete. Test the interleaving: abandon `K_old` while a concurrent refetch reinstalls it; bytes and index must both survive.

**M4.** Same file, around 295 and 311–315: pooled-but-never-indexed bytes set `charge = false`, insert a row of `size = actual`, and add 0 to `total`. If bytes are present and indexed by this commit, charge `actual` once. Do not paper over it with `saturating_sub`. Test: crash-orphan style row is counted exactly once and a later eviction debits that same amount.

**L1.** `evict.rs` 163–171 and `fetch.rs` 245–248 compute class slack as `max - total`, ignoring `in_flight`. Use `max - total - in_flight` (fail closed at 0). Global check already includes `in_flight`; class isolation must too. Test two concurrent class reservers cannot jointly exceed class cap plus slack.

**L3.** `media_store.rs` 988 and 1010 `saturating_add` / `saturating_sub` hide double-release and undercount. Keep the heal-on-open reset of `in_flight`, but a release that would pass through 0, or a debit larger than the counter, must surface as an error (or a counted drift event), not clamp silently. `heal.rs` may still zero `in_flight` on process start; that path is explicit, not a mask. Test double-release returns `Err` and leaves the counter unchanged after the first correct release.

Commit message sketch: `fix cache debit, backfill, and abandon races`.

---

## Wave 2 — eviction scans and tombstone retention

Start only after Wave 1 commits. Same files (`media_store.rs`, `evict.rs`).

**M1.** Charged, tombstone, and dangling pages rescan from zero, and `commit_cache_eviction` full-scans `MEDIA_META` inside a write transaction, up to `MAX_EVICTION_ROUNDS` (64). Add real indexes or maintain candidate cursors so a round does not walk the whole table. Durable-refcount probes must not scan every media row under the write lock. Target: one ensure under pressure is O(pages touched), not O(64·N). Add a test with a large tombstone and charged prefix that still evicts a known tail key without depending on a full scan. Do not weaken the existing alias-completeness or generation CAS.

**M2.** `retain_expired_tombstones` always calls `tombstone_page(0, 64)` and `continue`s on unexpired or live-media rows without advancing the offset. Skip-count those head rows and page forward in the same pass (and across passes) so an expired tail is visited. Bound the pass. Test: 64 unremovable head tombstones plus one expired tail row; one retention pass removes the tail. Bytes are not the bug; the leaked index row is.

**L2.** `commit_cache_eviction` holds a write transaction for read-only `AlreadyAbsent` / `Skipped` outcomes. Do the existence and generation reads on a read transaction; open the write transaction only when a mutation will happen.

---

## Wave 3 — remotely triggerable injection and SSRF

Two agents, disjoint files.

**Agent SMTP.** `src/domain/webhook.rs` `validate_email_shape` (150–167) allows `\r\n<>,` and whitespace. `src/services/email.rs` 258–266 and 368–369 writes `RCPT TO:<…>` and `To:` raw. Reject CR, LF, `<`, `>`, `,`, whitespace, and other controls in the shape check. Canonical-trim and re-validate in `EmailMessage::validate`. `encode_subject` already strips CRLF; addresses must not rely on that. Negative test: `"a@b\r\nBcc: x@y"` and `"a@b\r\nDATA\r\n"` never reach the socket. Do not weaken length or single-`@` rules.

**Agent webhook SSRF plus header assert.** Webhook delivery in `src/jobs/worker/dispatch.rs` (351–364) is not proven to use `SafeHttpClient`. Force webhook sends through `SafeHttpClient` with `allow_private_ips = false` regardless of `validate_hook_url` currently accepting `127.0.0.1`. Add a test that delivery to `http://169.254.169.254/` is `SsrfBlocked` and that loopback is blocked on this path. Do not change federation SSRF defaults in this wave except as needed to call the client. In `src/federation/delivery/client/send.rs` 255–266, reject header names and values containing CR or LF at `send()` entry (`debug_assert` is not enough; return a permanent error). Test a value with `\r\nBcc:` never hits the wire. `src/domain/webhook.rs` is owned by the SMTP agent; if the URL validator must change, the parent applies that snippet.

Parent commit after both agents finish.

---

## Wave 4 — liveness under load

Three agents. Parent owns `src/config.rs` and any router table.

**Agent backup.** `src/main.rs` 488–517 runs `create_bundle` (redb scan, zip, sha256) on a Tokio worker. Move `drain_pending_backups` / `run_backup_blocking` onto `spawn_blocking` or a dedicated OS thread. Do not hold `runner_lock` across `.await`. `src/backup/bundle.rs` and `restore.rs`: lower `MAX_TABLE_ROW_BYTES` from 64 MiB to a small-metadata cap (1–4 MiB) and stream oversized values to temp files instead of `vec![0u8; len]`. Log corrupt `*.status.json` skips in both list paths (`backup/mod.rs` 366 and 408); do not skip silently. The O(n) directory scan may stay if retained memory stays O(cap) and corrupt files are observable; if you add an index, it must not change newest-first tie-break (`requested_ms`, then `job_id`). Do not call `list_backup_statuses` from the bounded path. Structural test in `tests/backup_status_bound.rs` must keep passing.

**Agent fan-out.** `FANOUT_INLINE_MAX` is 100 (`federation/outbox/dispatch/fanout.rs`, `payload.rs`). One worker loop (`jobs/worker/run.rs` 59–104) then does up to 100 sequential blocking POSTs. Lower the inline threshold to 10–20, add a per-job wall-clock deadline, and run inline deliveries with a small concurrency cap. Above the threshold, always split. Do not let one job starve `Notify`, `RemoteMediaFetch`, or `PollExpiry`. Test: a 100-inbox job splits or finishes under the deadline without occupying the only worker for 100 round trips.

**Agent scans and counters.** These are the hot full-table walks:

- `activitypub/webfinger/nodeinfo.rs:52` — unauthenticated NodeInfo must not `list_all_notes` 10_000 rows. Use a maintained local-note counter. Cache the NodeInfo payload briefly.
- `federation/outbox/recipients.rs` 49–68, 77–97, 187–193 — no unbounded `Vec` of every follower or shared inbox. Page and emit continuation jobs per cursor. Cap `SuspendBroadcast` expansion with a resume cursor. Renote and reply fan-out must use an index, not `list_all_notes(10_000)` plus a full reaction load.
- `note_store_read.rs` 139–159 and 167–192 — `list_all_notes` and `count_local_notes` must not decode the whole table. Clamp `TimelineCursor` at the port.
- `job_queue.rs` 278–293 — `pending_count` is a counter, not a full decode.

`fetch_nodeinfo_software` (`federation/host_caps.rs` 107–132): singleflight per host plus a probe budget. Failures may stay uncached. Document that the software string is unsigned and advisory. Do not pin or trust it for auth.

---

## Wave 5 — inbox amplification and config compliance

**Agent negative cache.** `federation/inbox/mod.rs` 237–289 and `federation/delivery/resolve.rs` 247–270 refetch actors and keys on every miss with no negative cache. Add a bounded TTL negative cache (same shape as `StampNegativeCache` in `federation/mod.rs`) and singleflight per key so one in-flight refresh is shared. Keep DNS pool bound 32. Test: N identical failed key IDs produce one outbound fetch, then cache hits, and a TTL expiry refetches once.

**Parent, `src/config.rs` only.** `PINETREE_ALLOW_PRIVATE_IPS` (`delivery/client/config.rs` 27–29), `--federation-extra-ca`, `PINETREE_DEBUG_FEDERATION`, and `PINETREE_REPLY_REPAIR_LIMIT` (`main.rs` 65–69, 282–296, 426–449) violate the JSON-config rule. Add serde fields with sane defaults (`allow_private_ips: false`), `validate()`, round-trip and typo tests, and docs. CLI may override the file; env-only knobs go away or become deprecated overrides that still land in the merged config before `validate()`. Webhook delivery ignores `allow_private_ips` and stays false (Wave 3). Extra CA: log a fingerprint, fail permanent (not transient retry) if the file is missing, and do not replace the default trust store accidentally without that being an explicit config choice. `#[cfg(debug_assertions)]` is acceptable for the debug federation toggle if it is not a production setting; say so in the config docs.

**LOW-5 stubs.** `src/persistence/foundationdb/mod.rs` and `src/cluster/*.rs` are empty stubs while `config.rs` rejects Cluster mode. Mark them as Phase 4 placeholders in module docs, or delete them if nothing imports them. Do not imply they work.

**LOW-6.** Optional open-time integrity check for the live redb (restore already does this). A config or CLI flag that runs the existing check pattern. Default off so boot stays fast.

---

## Wave 6 — crash atomicity and authority

Disjoint files; three agents.

**Fan-out split.** `jobs/worker/dispatch.rs` 88–93 enqueues follow-up batches in separate transactions, then completes the parent. A crash duplicates POSTs because `job_queue` does not dedupe on idempotency key. Persist a `fanout_cursor` on the parent, or make `enqueue_jobs` idempotent on `idempotency_key`. Restart after batch 1 of N must not insert a second row for the same key. Test that.

**Dedup mark.** `activity_dedup.rs` 95–108 aborts the whole transaction on `DUPLICATE_NOTE_URI` or `TOMBSTONED_NOTE_URI`, dropping the new activity's dedup mark. On that conflict, commit the dedup mark so the next delivery is `already_seen`, not another full insert. Test a replay flood: second delivery does not call note insert.

**Actor upsert.** `delivery/resolve.rs` 142–212 does create/update, then a second update for `also_known_as` and `moved_to_uri`. Fold handshake columns into the first `CreateAccountInput` / single `update_account_in_txn`. Crash between the old two transactions must be impossible. Test one commit contains both profile and move columns.

**Authority matrix.** `inbox/mod.rs` `check_object_authority` returns true for Follow, Accept, Reject, Undo, Announce, Like, Delete, Block, QuoteRequest, Add, and Remove. Safety depends on each dispatch arm. Add a conformance test matrix: forged signer versus each type, including `Delete(Person)` and clip `Add`/`Remove`. Either move the host check into the gate or assert every arm re-validates. A missing arm is a failed test, not a comment. This test file is new; do not edit unrelated inbox tests except to call shared helpers.

---

## Wave 7 — file serving residuals

One agent, `files_http.rs` plus the fetch slot helper. Do not reopen Wave 1 accounting logic; call it.

- **Hit to open TOCTOU.** `files_http.rs` 217–221 versus 276–296: eviction between `byte_len` and `open_reader` returns 500. Re-enter refetch or return 404, never `internal()`.
- **Location injection.** `serve_file_named` 374–380 interpolates raw `id` into `Location`. Validate and encode; reject CR, LF, and non-id characters.
- **Cold GET vs proxy.** Origin validation failure on cold GET (`:260–266`) is 500; proxy is 404. Map `PortError::Validation` (dead origin, SSRF, oversize, disabled, budget) to 404. Keep unexpected store faults as 500.
- **Touch failure.** A failed `remote_cache_touch` (`:227–230`) must not fail the hit. Serve the bytes and log the touch error.
- **Hit-path pool.** Cache hits still enqueue unbounded `spawn_blocking`. Bound hit opens separately from the 8 cold slots so a hit flood cannot fill the default blocking pool. Cold and proxy stay at 8, non-blocking 429 plus `Retry-After`.
- **Slot pinning.** Document and, if cheap, stop a parked cold fetch from holding a full `max_file_size` reservation for the whole 20 s origin timeout. At least add a metric or log when all 8 slots are held. Do not remove the fail-closed 429.
- **Proxy memory and URL.** Cap proxy URLs at 1024 characters, same as `fetch.rs`. Streaming the origin body to disk is the follow-up the audit recorded; do it if it stays on `spawn_blocking` and the size cap remains. Chunked proxy must not hold encoded and decoded bodies up to 16 MiB at once if a streaming decode is straightforward. Do not add Range.
- **Proxy oracle.** Wrong type 400 versus blocked 404 lets callers port-scan image hosts. Collapse refusal to one status (404) so SSRF-blocked and wrong-type are not distinguishable. Do not weaken the MIME allowlist.
- **Cache-Control.** `/files` hits should send a short `Cache-Control` so anonymous repeat loads are not a write transaction per image. Touch can stay on cache miss of that header, or touch on a sampled interval. Do not drop generation bump semantics covered by `file_hit_touches_cache_record`.
- **Background fetches.** Inbox and job `fetch_and_index_remote_media` bypass the 8-slot semaphore. Route them through the same semaphore (or a dedicated bound). A mention flood must 429 or defer, not stack blocking fetches.

---

## Wave 8 — API, streaming, auth, layering

Partition carefully. `src/api/misskey/errors/error.rs` is parent-owned if multiple agents need it.

**Storage 500 leak.** `error.rs:188` `StorageUnavailable(msg)` puts the DB string in `info`. Redact to a generic code. Test a sentinel substring never appears in the HTTP body.

**Silent streaming drops.** The MeUpdated path was fixed; the same `.ok()` drops remain at `streaming/server/connection/dispatch.rs` (145, 149, 175, 178, 206, 210, 230, 234, 280, 284, 326, 329, 375, 381, 459, 468, 725, 757, 765) and `support.rs` 104, 141–142. On store or pack errors, log `error_class` only (same helper pattern as `log_me_updated_skip`) and skip the frame. Do not retry. Do not build a partial packed object. Gone and invisible stay silent. `hub.rs:169`, `subscribe.rs`, and `write.rs` `let _ = send` are at-most-once by design; add a counter for lagged or full drops so they are not invisible. No behavior change to drop policy.

**Unwraps.** `dispatch_lists.rs` 271 and 388, `dispatch_i/account.rs:124`, `dispatch_auth/password.rs:222`: replace `unwrap` / `expect` after a guard with `let Some(...)`.

**Anonymous list fallback.** `dispatch_lists.rs` 270–274 and 387–391: confirm Misskey parity for anonymous plus `forPublic=false`. If the public branch is wrong, match the reference checkout. Add a test either way so the choice is pinned.

**Layer inversion.** `application/commands/accounts.rs:437`, `auth_2fa.rs:102`, `emoji_transfer.rs:156`, `pins.rs:531`, `polls.rs:593,611,622`, `drafts/scheduled.rs:193` import Misskey packers or `api::misskey::auth`. Move password verify and pack inputs down to application or domain services. Adapters keep HTTP shapes. This is a move, not a wrapper that still imports `crate::api`.

**Duplicate note paths.** `notes/create.rs` and `inbound/notes.rs` each implement reply, renote, and mention side effects. One application operation; both adapters call it. Behavior of existing tests must not change.

**Auth residuals (no bypass found; still fix the contract):**

- Sign-in oracle: `signin_flow.rs` 195–209 returns 404/403 before the password check; `signup.rs` `do_signin` checks the password first. Pick one disclosure posture and apply it to both. If upstream parity requires the flow oracle, document that in the endpoint contract and add a test that states the chosen statuses. Do not leave them opposite without a written decision.
- Deleted-account 2FA: `auth_2fa.rs` 256–274 returns `USER_SUSPENDED` before `NO_SUCH_2FA` when a pre-deletion challenge is replayed. Deleted accounts should be indistinguishable from no-2FA within the 5-minute TTL, or the test should document the intentional 403. Prefer closing the oracle.
- `set_2fa_passwordless(false)` without a verified secret returns `NO_SUCH_2FA`. Make it a no-op success when the bit is already clear and no secret exists.
- `let _ = registry_remove` on expired-challenge prune (`auth_2fa.rs` 356–359): log the remove failure; still return expired. Do not swallow it.
- `register` then `update-key` overwrites a verified row with `verified: false` and locks login. Do not clobber a verified secret until the new key is confirmed, or keep the old secret usable until done.

Passwordless packer staying `false` is correct. Do not re-enable it. The discarded tuple at `user.rs:578` can stay; do not "clean it up" by reading the bit again.

---

## Explicitly out of code scope

- **IPv6 fail-closed** (`url.rs` 89–96): unbracketed `::1` and odd literals fail closed. No bypass. Do not normalize them into a new accept path.
- **Activity id index after restart** (`federation/mod.rs` 19–31): documented Phase 4 durable table. Leave the doc comment. No new table in this repair.
- **Passwordless dead field:** not a bypass. Do not touch signup and sign-in password requirements.

---

## Per-wave done check

Each wave's log under `state/logs/YYYY-MM-DD-<topic>.md` must include changed files, the exact `cargo test --locked --offline` command and counts, test-exe mtime newer than source mtime, a note of what failed on the old code or why the new test would, and `rustfmt --check` on touched Rust files. Native cargo only.

Parent commits after every wave. Subagents never commit. Suggested order of commits: accounting, eviction scans, SMTP and webhook SSRF, liveness, negative cache plus config, atomicity, files, API and auth.
