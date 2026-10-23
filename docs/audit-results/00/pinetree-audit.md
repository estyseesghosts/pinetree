Full pinetree audit complete — 5 agents, read-only, no edits.

## Repair-plan verification (`pinetree-adversarial-2-repair.md`)

| Slice | Verdict | Evidence |
|---|---|---|
| 1 — Reservation (1,A,B,C) | **FIXED** | `media/remote_cache/fetch.rs:168-174` reserve; `182-464` closure, `465-474` single release path, no `let _=`; `evict.rs:156-172`, `media_store.rs:981-984` include `in_flight`; `fetch.rs:236,242-254` 2nd class check with `actual`; `295,311-315,439-455 + 467/471` shared-hit `total+=0, in_flight 0`. Tests: `tests/cache_reservation_accounting.rs` 7 tests STRONG, log `2026-10-22-cache-reservation.md:44-62` 2 pass/5 fail on old code. |
| 2 — Eviction/tombstones (2,9-half) | **FIXED** | `evict.rs:104-106` `remote_cache_charged_page(.,32)`, `media_store.rs:514-540` `size>0 && !absent`; alias completeness `evict.rs:179-218,273-319` + `generation` CAS `311-318` (same-ms guard); tombstones excluded by construction; retention `evict.rs:227-246` `tombstone_page(0,64)`, TTL per-row, skip if media live, `remove_if`. Tests `cache_evict_starvation.rs` STRONG, log fail-on-old 1/4. |
| 3 — Files/proxy (3,5,D) | **FIXED** | `files_http.rs:143-148,156-161` `serve_file` off-worker; `:441-447` proxy `send` in `spawn_blocking`; `blob_store.rs:12-15`, `storage.rs:397-399,147-179` `open_reader` + 64KiB stream `:301-333`; slot `MAX_CONCURRENT_FILE_FETCHES=8` `:172-188` → 429+`Retry-After` (`error.rs:146-162`); refreshed MIME `:250-258,301-342`; hit `remote_cache_touch` `:222-232` (`media_store.rs:439-467` bumps generation); allowlist `:349-353,357-371` + proxy `:459-472` unchanged; SSRF via `federation_http.send` (`url.rs:468-512`, `send.rs:141-187`); no Range. Tests `files_http_cache_hit.rs` 4 black-box STRONG. |
| 4 — Dangling CAS (4,E) | **FIXED + new debit bug** | `scan.rs:275-291` `dangling_page` (not LRU), `media_store.rs:581-614`; locked re-read `scan.rs:300-307`, live-media skip `311`, `remove_if` `325-330`; debit `338-348` uses `current.size`, once/key via `accounted_blobs`. Tests `reconcile_dangling_cas.rs` 6 tests STRONG incl. `RacyMedia`. **But H1 below.** |
| 5 — Per-class (6) | **FIXED** | `media_store.rs:937-946` apply `AudioVideo=>{}`, `858-876` commit same, `evict.rs:388-399` `class_bytes_in(AV)=0`; per-key `CACHE_BLOB_CLASS` `:44,414-428,328-330`, commit reads it `858-866`, last-alias drop `346-353/637-644`. History left inflated by design (`ports/media_store.rs:89-93`). Tests `cache_class_accounting.rs` 5 tests, log fail-on-old 0/5. |
| 6 — Passwordless (7) | **FIXED** | `packers/user.rs:583` always `false` (discard `:578`, docs `:569-577`); setter `commands/auth_2fa.rs:199-205` `true→INVALID_PARAM`; `dispatch_auth/twofa.rs:224-228`; `signin_flow.rs:210-229` + `signup.rs:264-283` password still required; legacy-true test `:567-637`, `passwordless_does_not_downgrade` `:499-562`. |
| 7 — Cargo target (8) | **FIXED** | `.cargo/config.toml:1-3` comment only; `docs/testing.md:72-75` `CARGO_TARGET_DIR` guidance. |
| 8 — Backup statuses (9-half) | **FIXED w/ residual** | `backup/mod.rs:387-444` binary-insert keep-`cap`, never `list_backup_statuses` (structural test `backup_status_bound.rs:152-172`); tie-break `requested_ms, job_id` `:438-442`. Residual: still O(n) dir-entry collect + parse every file (`:391-392,408`); retained mem O(cap) only. Corrupt files silently skipped (`:366,408`) — no log. |
| 9 — MeUpdated (10) | **FIXED** | `streaming/.../dispatch.rs:628-655` → `me_updated_body :33-44` logs via `log_me_updated_skip :51-63` + `None`→skip; no retry, no partial `MeDetailed`; tests `:789-829` assert packer never runs on error + `tests/streaming.rs:1106`. 26 neighboring `.ok()` left alone per plan. |

Logs: `state/logs/2026-10-22-cache-reservation|cache-evict-starvation|files-serving|reconcile-dangling-cas|backup-status-bound|streaming-me-updated|passwordless-advertise|cargo-target.md`, `2026-10-23-cache-class-accounting.md` all present with real `cargo test --locked --offline` output + exe-mtime > src-mtime.

## New / residual issues (ranked)

**High (from Task A/D):**
- **H1 — Dangling debit on live shared key:** `reconcile/scan.rs:316-348` debits whenever `remove_if` succeeds even if `blob_refcount>0`. Removes one alias, debits whole key → `total_bytes` permanently low. Fix: debit only on last alias or route via `commit_cache_eviction`.
- **H2 — Backfill corrupts accounting:** `store_backfill.rs:444-516` sums per-`MEDIA_META` row, no `evicted` check, N× shared blobs, never writes `CACHE_BLOB_CLASS` → inflated `total/class`, later evictions guess class.
- **SMTP CRLF injection:** `domain/webhook.rs:150-167` shape check allows `\r\n<>,`; `services/email.rs:258-266,368-369` raw `RCPT TO:<>/To:`. Fix: reject `[\r\n<>,\s]` + controls.
- **Backup drain blocks runtime:** `main.rs:488-517` `create_bundle` (redb scan+zip+sha) on tokio worker → stalls HTTP. Fix: `spawn_blocking`/dedicated thread.
- **Inbox fetch amplification:** `federation/inbox/mod.rs:237-289` + `delivery/resolve.rs:247-270` no actor/key negative cache (only stamps). Fix: bounded TTL negative cache + singleflight.
- **Fan-out 100 sequential blocking POSTs:** `federation/outbox/dispatch/fanout.rs:41-65`, single `process_one` loop `jobs/worker/run.rs:59-104`. Fix: lower inline threshold, deadline + concurrency.
- **Unbounded scans on hot paths:** `activitypub/webfinger/nodeinfo.rs:52` 10k notes/NodeInfo GET; `federation/outbox/recipients.rs:187-193` 10k/job; `note_store_read.rs:139-159`, `job_queue.rs:278-293`. Fix: indexes/counters, clamp `TimelineCursor`.
- **Webhook SSRF unclear:** `domain/webhook.rs:242` allows `127.0.0.1`; delivery `jobs/worker/dispatch.rs:351-364` not proven via `SafeHttpClient`. Fix: force `allow_private_ips=false`, test `169.254.169.254 → SsrfBlocked`.

**Medium:**
- Eviction O(64·N) full scans under write txn: `media_store.rs:525-531,556-562,594-613`, `765-772,777-784,824-830`, `MAX_EVICTION_ROUNDS=64`.
- Retention head-of-line block: `evict.rs:227-230` always page 0, `continue` w/o offset advance — tail never visited.
- Old-key abandon race: `fetch.rs:262-263,418-437` checks `refcount(old)==0` without old-key lock → live-byte delete.
- Crash-orphan uncharged: `fetch.rs:295,311-315` pooled-never-indexed → `charge=false`, row `size=actual` but `total+=0`.
- Storage 500 info leak: `api/misskey/errors/error.rs:188` echoes DB strings. Redact.
- Silent streaming drops (same class as #10, deferred): `streaming/.../dispatch.rs:145,230,280,326,375,459,722,757` etc. + `hub.rs:169`, `subscribe.rs:293,342,350,411`, `write.rs:103,216` `let _=send`.
- App→API layer inversion: `application/commands/accounts.rs:437`, `auth_2fa.rs:102`, `emoji_transfer.rs:156`, `pins.rs:531`, `polls.rs:593,611,622` import packers/auth.
- Duplicate note paths: `notes/create.rs:33` vs `inbound/notes.rs:17`.
- Non-atomic fan-out split → dup POSTs: `jobs/worker/dispatch.rs:88-93`, no idempotency dedup `ports/job_queue.rs:288`.
- Dedup-mark abort on `DUPLICATE_NOTE_URI`: `activity_dedup.rs:95-108` — replay = full insert each time.
- Remote actor upsert 2 txns: `delivery/resolve.rs:142-212`.
- Unbounded recipient collect: `recipients.rs:49-68,77-97`, `SuspendBroadcast` + `with_capacity(len)`.
- `check_object_authority` whitelist `inbox/mod.rs:385-434` — safety depends on per-arm re-validation, no matrix test.
- Env/CLI-only settings violate AGENTS.md: `PINETREE_ALLOW_PRIVATE_IPS` (`delivery/client/config.rs:27-29`), `--federation-extra-ca`, debug envs `main.rs:65-69,282-296,426-449`.
- Backup 64MiB row cap OOM on restore (offline): `backup/bundle.rs:49`, `restore.rs:677-683`.

**Low:** class slack ignores `in_flight` (`evict.rs:163-171`, `fetch.rs:245-248`); `commit_cache_eviction` read-only work under write txn; `saturating_add/sub` masks drift (`media_store.rs:988,1010`); hit→open TOCTOU 500 (`files_http.rs:217-221,276-296`); `serve_file_named` raw `id` in `Location` (`:374-380`); cold-GET 500 vs proxy 404 (`:260-266`); touch-failure 500 (`:227-230`); unbounded hit-path `spawn_blocking`; 8×`max_file_size` slot pinning; proxy 16MiB chunked transient + whole-buffer origin fetch; proxy URL length unchecked; proxy 400/404 oracle; per-hit write txn + no `Cache-Control` on `/files`; background fetches bypass 8-slot; `unwrap` on guarded Options (`dispatch_lists.rs:271,388` etc.); broadcast at-most-once invisible; `users_lists` anon fallback; signin oracle asymmetry (`signin_flow.rs:195-209` vs `signup.rs:222-258`); deleted-account 2FA oracle; `set_passwordless(false)` w/o secret → `NO_SUCH_2FA`; `let _=registry_remove` (`auth_2fa.rs:356-359`); register→update-key login lock; header `{k}: {v}` no CRLF assert; IPv6 fail-closed; FDB/cluster stubs; no open-time integrity sweep.

**Verified good:** SSRF core (literals, all-addrs, re-validate redirects, downgrade reject, curl `--resolve`, timeouts), signatures, inbox 64KiB + `actor==signer`, jobs caps/quarantine, `deny_unknown_fields` + `validate()` + round-trips, backup/restore traversal/manifest/streaming-verify, `thread_stack_size(8<<20)` + `dispatch_fits_bounded_stack`, observability redaction.

Fix order: H1+H2 accounting → SMTP CRLF + webhook SSRF → backup `spawn_blocking` + fan-out bound + scan caps → inbox negative cache → `allow_private_ips` JSON config → atomicity/dup items.
