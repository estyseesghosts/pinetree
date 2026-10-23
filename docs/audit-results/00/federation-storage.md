Task D — Broad audit: federation, storage, jobs, domain, config, backup, cluster, services, observability, runtime
Read-only. All paths relative to pinetree/src/. No edits made.
Domain dependency direction — PASS (with one nit)
src/domain/*.rs use lines are only crate::domain::*, std, serde, serde_json::Value, regex::Regex. No http/axum/reqwest/redb/foundationdb/tokio import found (grep ^use src/domain → 46 hits, none offending). domain/moderation.rs:6 + domain/webhook.rs use serde_json::Value — generic JSON, not an AP/HTTP shape; acceptable but worth noting as the only non-domain external type.
application → domain, adapters → application holds as far as sampled (federation/*, activitypub/* call AppServices, never RedbStore directly except backup/* + runtime.rs, which is the intended composition root).
Ranked findings
HIGH-1 — SMTP RCPT TO / To: CRLF injection via validate_email_shape
- src/domain/webhook.rs:150-167: checks only length 3..=254, exactly one @, non-empty parts, . in domain. Never rejects \r, \n, <, >, ,, whitespace.
- src/services/email.rs:258-266: format!("RCPT TO:<{rcpt}>") written raw to the socket; render():368-369: format!("To: {}\r\n", msg.to.join(", ")).
- Exploit: attacker signs up with to = "a\r\nDATA\r\n..." (passes shape check: split('@') still yields 2 parts if payload keeps one @). Stored address later drives EmailSend job → injects SMTP commands / extra headers (Bcc:) via a local relay → spam relay / header forgery. encode_subject() strips CRLF (email.rs:386-398) but addresses are not stripped anywhere.
- Fix: reject [\r\n<>,\s] + control chars in validate_email_shape; canonical-trim + re-validate at EmailMessage::validate; add negative test with "\r\nBcc:".
HIGH-2 — Backup drain blocks the async runtime thread
- src/main.rs:488-517: tokio::spawn(async { interval.tick(); worker.drain_pending_backups(&ctx,1) }) calls run_next_backup → bundle::create_bundle synchronously (single redb read txn streaming every table + FsBlobStore file reads + ZipWriter + sha256 + re-verify) on a tokio worker thread. Same for maintenance CLI path only, but the live drain is the problem.
- Scenario: one 10 GiB backup stalls all HTTP/streaming tasks sharing that worker for minutes; readiness/liveness probes time out; operator retries backup → Busy / duplicate .partial files.
- Fix: tokio::task::spawn_blocking around drain_pending_backups/run_backup_blocking, or a dedicated OS thread + channel; keep the runner_lock but never hold it across .await.
HIGH-3 — Inbound inbox can force unbounded outbound fetch amplification (no actor-fetch negative cache)
- src/federation/inbox/mod.rs:237-289 + src/federation/delivery/resolve.rs:247-270: every miss/rotation does fetch_and_upsert_remote_actor (signed GET, DNS + TCP/curl) on a spawn_blocking thread. Only stamps have StampNegativeCache (federation/mod.rs:153-222); actor/key failures are never cached.
- Scenario: attacker POSTs many activities each with a fresh keyId (https://<attacker>/actors/<n>#key, or NXDOMAIN hosts). Each costs DNS (bounded 32, fail-fast beyond — good) + blocking thread + curl process. Legit traffic past saturation gets ResourceBusy transient failures. Cache-miss storm ≈ application-layer DoS.
- Fix: negative cache for failed actor/key fetches (bounded, TTL like stamp cache), per-IP inbox rate limit, cap resolve_remote_public_key refresh to one flight per key (singleflight).
HIGH-4 — Fan-out job does up to 100 sequential blocking POSTs on the single worker loop
- src/federation/outbox/dispatch/fanout.rs:41-65, payload.rs:27: FANOUT_INLINE_MAX=100; jobs/worker/run.rs:59-104 is a single sequential process_one loop.
- Each deliver_single is a full SafeHttpClient round trip (10 s op timeout, 20 s total). Worst case one job holds the only worker ~100×20 s, starving Notify/RemoteMediaFetch/PollExpiry/.... DestinationTracker cooldown only helps after first failure.
- Fix: lower inline threshold and/or bound per-job wall time; deliver inline with concurrency limit + overall deadline; prefer always-split above N=10–20, or a dedicated delivery worker pool.
HIGH-5 — Unbounded full-table scans on hot paths (DoS as DB grows)
- src/activitypub/webfinger/nodeinfo.rs:52: list_all_notes(&TimelineCursor::with_limit(10_000)) per unauthenticated NodeInfo GET.
- src/federation/outbox/recipients.rs:187-193: collect_note_interacted_ids does list_all_notes(with_limit(10_000)) per edit fan-out job to find renoters, plus a 100/page reply walk and a full reactions_for_note load.
- src/persistence/redb/note_store_read.rs:139-159,167-192: list_all_notes iterates .rev() decoding every row until limit live rows (tombstone-heavy tables scan everything); count_local_notes decodes all accounts + all notes; job_queue.rs:278-293 pending_count decodes every job row.
- Scenario: 1 M-note instance → NodeInfo probe or one notes/edit costs 10 k JSON decodes; pending_count polling in drains amplifies. Anonymous remote can trigger repeatedly.
- Fix: maintain counters/indexes (renote-by-note index instead of table scan), cap limit at the port layer (TimelineCursor::with_limit clamping), cache NodeInfo counts, make pending_count a counter table.
HIGH-6 — User-controlled webhook URLs allow private-IP targets; delivery SSRF unclear
- src/domain/webhook.rs:242 test asserts validate_hook_url("http://127.0.0.1:9/x").is_ok() — private IPs explicitly allowed.
- Webhook delivery reuses classify_inbox_status + DestinationTracker (jobs/worker/dispatch.rs:351-364) but I did not find a SafeHttpClient/SSRF re-validation call on that path in the sampled files (vs. federation delivery which always goes through resolve_and_validate_url).
- Scenario: if webhook POST uses a raw client, any user with webhook permission gets SSRF to 169.254.169.254/loopback.
- Fix: route webhook sends through SafeHttpClient with allow_private_ips=false regardless of validate_hook_url leniency, or add explicit allowlist config; add test asserting http://169.254.169.254/ delivery is SsrfBlocked.
MEDIUM-1 — PINETREE_ALLOW_PRIVATE_IPS (+ --federation-extra-ca, debug envs) violate the AGENTS.md config rule
- src/federation/delivery/client/config.rs:27-29: std::env::var("PINETREE_ALLOW_PRIVATE_IPS") inside SafeHttpConfig::default(). src/main.rs:65-69,282-296,426-449: --federation-extra-ca, PINETREE_DEBUG_FEDERATION, PINETREE_REPLY_REPAIR_LIMIT are env/CLI-only.
- AGENTS.md: "Every new setting must be addressable through the JSON config file … No settings settable only via env vars or CLI flags." The SSRF exemption is security-relevant and invisible to config_summary/round-trip tests.
- Fix: add federation.allow_private_ips: bool=false (+ validate() + tests + docs), keep env as deprecated override or remove; same for repair limit/debug toggles or document them as explicitly non-config exceptions.
MEDIUM-2 — Fan-out split is not crash-atomic → duplicate deliveries after crash
- src/jobs/worker/dispatch.rs:88-93: split_followup_batches enqueues full 500-job batches each in their own txn (enqueue_jobs), then complete_with_followups atomically completes + enqueues only the last batch.
- src/application/ports/job_queue.rs:288: "No de-duplication on idempotency key"; job id is random (store.rs:464).
- Scenario: crash after batch 1 of N commits but before parent completion → restart re-executes activity job → re-derives same {job_key}:{inbox} keys but inserts new rows → duplicate POSTs (receivers dedupe only if activity id was minted once — true for most payloads — but inbox load doubles; for non-idempotent side effects this is a retry storm).
- Fix: persist a fanout_cursor on the parent job, or make enqueue_jobs idempotent on idempotency_key (unique index), or enqueue all follow-ups lazily via continuation jobs.
MEDIUM-3 — record_note_activity aborts the dedup mark on DUPLICATE_NOTE_URI
- src/persistence/redb/activity_dedup.rs:95-108: dedup insert + create_note_in_txn in one txn; on Conflict("DUPLICATE_NOTE_URI"|"TOMBSTONED_NOTE_URI") the whole txn is aborted, discarding the dedup mark for the new activity id.
- Not corruption (converges to None on retry), but every replay of the same activity id re-attempts a full note insert instead of hitting the dedup fast path — wasted write txn per replay; under replay flood this is a write-amplification DoS.
- Fix: on duplicate-URI, commit the dedup mark in a second txn (or forget+re-check_and_record split), so the second delivery is a cheap already_seen.
MEDIUM-4 — Remote actor upsert is two transactions, not one
- src/federation/delivery/resolve.rs:142-212: create/update_account then a second update_account to seed also_known_as/moved_to_uri.
- Crash between them leaves a valid-but-incomplete row (missing move-handshake columns). Converges on next fetch, but violates "state change + … atomically" and doubles write contention on hot remote-actor rows.
- Fix: fold the handshake columns into the initial CreateAccountInput/single update_account_in_txn.
MEDIUM-5 — collect_recipient_inboxes / collect_known_shared_inboxes are unbounded in memory
- recipients.rs:49-68,77-97: collect_follower_ids loops until short page with no cap (test proves 10 250 walk, slice10 tests); SuspendBroadcast walks all remote shared inboxes (SCAN_PAGE=1000 until table end) then fan_out pre-allocates Vec::with_capacity(inboxes.len()).
- 500 k followers → 500 k Strings + 500 k NewJob payloads materialized before the first send/split. OOM + MAX_JOBS_PER_TXN=500 batching happens too late.
- Fix: page-and-split at the source (emit continuation jobs per cursor page), cap SuspendBroadcast expansion per job with a resume cursor.
MEDIUM-6 — Inbox check_object_authority whitelists most types; safety depends on un-audited dispatch re-validation
- src/federation/inbox/mod.rs:385-434: Follow/Accept/Reject/Undo/Announce/Like/Delete/Block/QuoteRequest/Add/Remove → true (no host check). Comments assert dispatch re-validates (note deletes keep author/host check; stamp deletes check stamp authority; quote requests validate instrument ownership; clip Add/Remove verify collection ownership by fetch).
- Code reviewed here does not prove each dispatch arm enforces it; a single missing check (e.g., forged Delete(Person) or Add for a local clip) becomes account/collection takeover. The Create host check (object.host == signer.host) is good; Update(Note|Article|…) string-URI pass-through delegates entirely to dispatch.
- Fix: add a dispatch-level authority test matrix (forged-signer × each type) or move the checks into the gate; at minimum add debug_assert + conformance tests per arm.
MEDIUM-7 — Backup MAX_TABLE_ROW_BYTES = 64 MiB allows restore-time OOM (offline, low blast radius)
- src/backup/bundle.rs:49, restore.rs:677-683: vec![0u8; klen/vlen] per row up to 64 MiB; MAX_MANIFEST_BYTES=64 MiB, per-entry cap checks use entry.size() (uncompressed, good) but zip-bomb via many mid-size entries is bounded only by the 64 GiB bundle cap.
- Restore is offline/manual (not reachable remotely), so LOW exploitability, but a crafted bundle can OOM the restore host.
- Fix: lower row cap to ~1–4 MiB (rows are "small metadata/JSON" per the doc comment), stream large values to temp files.
MEDIUM-8 — fetch_nodeinfo_software is unsigned plain-HTTPS without pinning caveats + probe cost per host
- src/federation/host_caps.rs:107-132: https://{host}/nodeinfo/2.1 via SafeHttpClient (SSRF-safe, good), 7-day TTL, failures never cached (good availability choice).
- Every unknown host pays 2 sequential GETs on the delivery path (off-txn, good) but there is no per-host in-flight coalescing or probe budget — a burst of reactions to N new hosts = 2N outbound fetches.
- Fix: singleflight per host + probe timeout budget; document that NodeInfo is unauthenticated (software string is advisory only — already treated as such, good).
LOW-1 — Process-local federation caches are documented best-effort but lose Undo(Accept) by-ID after restart
- src/federation/mod.rs:19-31: ActivityIdIndex bounded (ACTIVITY_INDEX_CAP), restart drops it; bare-IRI Undo{object:<accept-id>} "converges without effect until the next embedded Accept". Documented, single-node only.
- Fix (Phase 4): durable per-activity table as the comment already proposes; no action now beyond keeping the doc comment.
LOW-2 — curl --cacert replacement semantics + set_extra_ca first-wins global
- src/federation/delivery/client/send.rs:79-97,319-330: --federation-extra-ca swaps in --cacert PATH (curl replaces default roots). OnceLock first-wins, no path validation, no log of fingerprint.
- Bench-only flag present in the prod binary; a mis-set path breaks all HTTPS federation (fail-closed, good) but error surfaces as generic Tls transient → retry storm.
- Fix: gate behind #[cfg(debug_assertions)] or a config knob with fingerprint log; map "extra-CA missing/unreadable" to permanent InvalidUrl-class, not transient.
LOW-3 — Header construction writes {k}: {v}\r\n without CRLF validation
- src/federation/delivery/client/send.rs:255-266: write!("{k}: {v}\r\n"). All current callers pass internally-built headers (Accept/User-Agent/Date/Signature/…), and Host comes from validated ParsedUrl, so not currently exploitable. Becomes HIGH the moment any remote-derived value (e.g., custom emoji alt text, Retry-After echo) flows into extra_headers.
- Fix: debug_assert!/validate no [\r\n] in header names/values at send() entry.
LOW-4 — parse_http_url IPv6 edge + is_ip_literal_host heuristic
- url.rs:89-96: unbracketed ::1 splits at last : → host :/port parse fails → InvalidUrl (fail-closed, fine). is_ip_literal_host:165-172 takes split(':').next() — "evil.com:80" → "evil.com" → not IP → DNS path (correct).
- No bypass found; note only that non-standard literals fail closed rather than being normalized.
LOW-5 — Cluster stubs + FDB adapter absent (expected for Phase 4, but surface is misleading)
- src/persistence/foundationdb/mod.rs (2 B), src/cluster/*.rs (2 B stubs), src/config.rs:570-572 correctly rejects Cluster mode. No FDB 5 s/10 MB exposure yet.
- Fix: delete stubs or add //! Phase 4 placeholder — Cluster rejected in validate() docs so future workstreams don't import them assuming behavior.
LOW-6 — redb durability relies on redb::commit only; no explicit integrity sweep described
- store.rs:429-441: begin_write/commit_write (+ crash-injection hook, good). Crash-recovery tests exist per Phase 1 exit (2 crash-recovery tests). No check_integrity on open for the live DB (only on restore path restore.rs:502). Acceptable for redb (MVCC + atomic commit), but a torn OS-level write would surface lazily.
- Fix: optional --check-integrity open-time flag for operators; already done on restore — good pattern.
What was verified good (do not regress)
- SSRF core: scheme allowlist, @-authority reject, full private/reserved coverage incl. CGNAT/link-local/multicast/CG-NAT/IPv4-mapped IPv6 (url.rs:125-161), literal-first check before DNS, every resolved addr checked, downgrade reject, redirect re-resolve+re-validate through the same bounded pool, DNS pool hard bound 32 + fail-fast ResourceBusy, TLS always verified (no -k), allow_private_ips correctly scoped to address checks only.
- Timeouts/size: 10 s op / 20 s total / 5 s DNS, 64 KiB AP JSON, 3 redirects, TooLarge permanent, curl --max-time/--max-filesize/--max-redirs 0/--resolve pinning (kills DNS-rebinding TOCTOU).
- Signatures: legacy requires (request-target)+host+date (+digest with body), digest verified against body; RFC 9421 requires covered digest with body, overflow-safe created/expires±skew checks, dual-signer disagreement fails closed, fetch-gate collapses all failures to anonymous→404 (no oracle). MAX_CLOCK_SKEW_SECS=300.
- Inbox: 64 KiB DefaultBodyLimit at router + backstop, actor == signer + object-authority gate, spawn_blocking off async threads, dedup+state single-txn per record_* arm.
- Jobs: 64 KiB payload cap both at new_json and validate, 500/txn cap enforced pre-txn, 8 attempts + quarantine, lease-reclaim burns an attempt, panic containment at two levels, Defer doesn't burn budget, backup attempts bounded 5.
- Config: deny_unknown_fields everywhere, validate() covers every knob sampled, round-trip + typo-safety + guard tests per section (config/tests.rs ~1065 lines). config_summary proven secret-free by test.
- Backup/restore: traversal/symlink/duplicate/unexpected-entry rejection, manifest-first version/set/signature/cap validation, streaming checksum verify before any write, fresh-dir-only restore, partial cleanup on failure, retention never deletes newest completed / never touches failed, restore-after-eviction byte-presence check + accounting/count re-verify + RESTORE_READY marker.
- Runtime: thread_stack_size(8<<20) + dispatch_fits_bounded_stack (4 MiB) directly pins the AGENTS.md stack fix; graceful SIGTERM drain with worker stop flag + lease-expiry recovery (no work loss claim is accurate).
- Observability: fail-closed redaction, 500-char truncation, bounded rings (64/128), error_class never echoes remote bodies (tested with sentinel).
Suggested fix order
1. Email CRLF (HIGH-1) + webhook SSRF (HIGH-6) — remotely triggerable, small diffs.
2. Backup spawn_blocking (HIGH-2) + fan-out inline bound (HIGH-4) + scan caps (HIGH-5) — liveness under load.
3. Inbox fetch negative cache (HIGH-3).
4. Config JSON-ification of allow_private_ips (MEDIUM-1) — AGENTS.md compliance.
5. Atomicity/duplication follow-ups (MEDIUM-2/3/4/5).
