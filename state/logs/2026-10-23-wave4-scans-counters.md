# 2026-10-23 — Wave 4 scans and counters (subagent)

Scope: adversarial-3 Wave 4 "Agent scans and counters" only. No commits
by this agent (subagent rule).

**Merge note (2026-10-22 ~22:36 UTC-4): the parent's Wave-5/6 commit
44c19b8 swept two of this wave's files.** `CreateAccountInput` gained
Wave-6 fields in the same session, and the parent's commit of
`src/application/ports/account_store.rs` (+23) and
`src/persistence/redb/account_store.rs` (+25) includes this wave's hunks
verbatim alongside Wave-6's: the `count_local_accounts` port method, the
`COUNTER_ACCOUNTS_LOCAL` import, the create-path +1, and the redb
`count_local_accounts` impl. Everything else listed below is still
uncommitted. No work was lost; the committed mix compiles and all suites
below ran against the mixed tree.

## What changed

Bounded-work pass over the hot full-table walks; no behavior change to
delivery semantics except where the audit demanded bounds.

**Counters (maintained, O(1) reads).** New `counters` redb table
(`notes:local`, `accounts:local`, `jobs:pending`) bumped in the same
transaction as the state change, seeded once at open by
`run_counters_backfill` (META `migration:counters:v1`):

- `note_store_write.rs`: create +1 when the author is local (`host.is_none`,
  same predicate as the old scan); first-time delete −1 when
  `user_host.is_none()`.
- `account_store.rs` `create_account_in_txn`: +1 for local accounts.
- `moderation_store.rs` `set_suspended` / `delete_account`: suspend −1,
  unsuspend +1, delete −1 — only on counted→uncounted transitions, so
  repeats and already-suspended deletes never double-move.
- `job_queue.rs` + `store.rs` `enqueue_jobs_in_txn`: `pending_count` is a
  counter read; enqueue +N; `complete`/`complete_with_followups` settle the
  parent (−1 iff it was claimable) and credit followups;
  `fail`/`fail_permanent`/claim-reclaim quarantine −1; `defer` moves
  nothing; `remove_scheduled_draft_jobs` −removed. Saturating (fail-safe,
  repairable by re-backfill) rather than panicking.
- New backfills in `store_backfill.rs`: `run_counters_backfill` (single
  txn, idempotent via marker) and `run_renote_index_backfill` (crash-
  resumable batches mirroring the reply-index migration).
- `persistence/redb/backup.rs`: `notes_by_renote` + `counters` tables and
  the two new META markers join the bundle/marker lists, so a
  backup/restore round-trip keeps counters and the renote index exact
  (restore is manifest-generic; no `src/backup/*` change needed).

**Renote index.** New `NOTES_BY_RENOTE (renote_target, child)` table:
maintained in create/delete/renote-changing-edit, paged newest-first read
`notes_by_renote` (port default filters one bounded page for test
doubles), backfilled at open for old DBs. `collect_note_interacted_ids`
now pages `notes_by_reply` + `notes_by_renote` (100-row pages, capped at
`INTERACTED_AUDIENCE_CAP = 2000`) and resolves reactors through the new
bounded `reactor_ids_for_note(note, limit)` (port default truncates
`reactions_for_note`). No more `list_all_notes(10_000)` + full reaction
load on the edit fan-out path.

**Port clamp.** `TimelineCursor::MAX_LIST_LIMIT = 1_000`;
`with_limit`/`clamp_limit` clamp at the port and the redb
`list_all_notes` enforces the same bound for struct-literal cursors.
Deeper reads page with `until_id`. This also bounds the remaining 10k
literals (timelines, hashtags, pins, reply repair) to one page per
request — audited trade-off, fixtures are far smaller.

**Sweep paging.** `collect_follower_id_page` /
`collect_shared_inbox_page` (cursor, `None` = table end; a full page's
cursor may yield one empty next page, never a missed row) plus
`SUSPEND_SWEEP_PAGE_ROWS = 500`. `collect_known_shared_inboxes` keeps its
signature and now walks the page primitive (deduped whole-set, as the
dispatcher consumes it). Full cursor-carried continuation needs one
payload field (excluded file) — exact snippet for the parent below.

**NodeInfo.** `render_nodeinfo` reads `count_local_accounts` (new
`AccountStore` method; Mem doubles compute it; redb serves the counter)
and `count_local_notes` — no scans. Handlers serve `cached_nodeinfo`
(60 s brief memo keyed by `(public_url, version)`, ≤8 entries).

**Host caps.** `fetch_nodeinfo_software` runs inside a process-wide
`NodeinfoProbeGate`: per-host singleflight (waiters share one fetch),
max 8 concurrent probes, 60 starts/minute budget; shed probes return
`None` (uncached failure → emoji-aware default). Docs now state the
`software` string is self-asserted, unsigned, advisory-only — never auth.

New test: `tests/wave4_scans_bound.rs` (8 tests). Existing tests
untouched; two Mem doubles gained cheap `count_*` impls.

## Verification

Native `cargo --locked --offline`, `CARGO_TARGET_DIR` shared target:

- `cargo test --locked --offline --test wave4_scans_bound`: **8/8 pass**
  (exe `.../deps/wave4_scans_bound-a4a55eb340647db4.exe` mtime 22:27,
  newer than every touched source, newest 22:24).
- `cargo test --locked --offline --lib`: **662/662 pass** (incl. the
  nodeinfo counts test, host_caps, and slice10 follower-walk tests).
- Integration suites green: federation e2e incl. NodeInfo endpoints
  (7), backup_consistency_p1 (119), backup_status_bound (4),
  backup_snapshot_f1 (15), crash_recovery (2), ingress_limits_g1 (15),
  batch_q_phase1_fed (6), streaming (11), gaps_streaming (16), and the
  gaps/media/cache/reconcile/admin/dns/totp/emoji/rate-limit batches
  listed below — all pass: gaps_api_auth/drive/gallery/hashtags/domain
  (6/7/3/3/12), gaps_fed_emoji/update (8/9), gaps_transcode/email (9/16),
  notes_like/quote/minrepro (1/6/5), media_lifecycle (7),
  storage_cache_evict/durable_quota/reconcile (5/3/5),
  storage_two_pools/cache_budget/paging_c-fix (5/3/7),
  cache_reservation/evict_starvation/transition (6/5/8),
  cache_class_accounting/reconcile_dangling (4/6), files_http_cache_hit
  (5), backup_snapshot_f-fix/retention (4/9), admin_diagnostics_h1/h-fix
  (7/3), diagnostics_roles (9), dns_bounded (10), wave1 h1/h2/m3/m4/l1/l3,
  wave3 smtp/ssrf (5/3), wave4_backup_liveness (3), gaps_api_announcements
  (5), prod_ready_merge (2), rate_limit_bounded_d4/config_d-fix (8/11),
  config_docker_b2 (5), federation_sig_mode_e-fix/e5 (5/21),
  gaps_final_wiring (7).
- Old-code failure notes: `with_limit(10_000).limit` was 10_000 (now
  1_000); 1_050 stored notes decoded 1_050 rows for a 10k limit (now one
  1_000 page); `nodeinfo.rs`/`recipients.rs` production code no longer
  contains `list_all_notes`/`search_accounts` scans (pinned by
  source-shape asserts — old sources fail them); the page/index/counter/
  gate APIs did not exist (old tree fails to compile the test).
- `rustfmt --check` on all 22 touched files: remaining diffs are only
  the pre-existing repo-wide import-ordering class (installed rustfmt
  1.10.0 also flags untouched files and today's sibling-agent files
  the same way; repo convention keeps UPPER_SNAKE-first). One genuine
  over-width line of mine was fixed. No other diffs.

## Known worktree collisions (not mine, do not merge over blindly)

- `src/persistence/redb/job_queue.rs` is co-edited by Wave-6 (idempotent
  enqueue: `filter_new_jobs_in_txn` + their `complete_with_followups`
  counter adjustment, which correctly references the new counter). Their
  semantic change intentionally breaks
  `storage_paging_c1::maintenance_triggers_enqueue_drainable_jobs`
  (repeat boot no longer stacks: pending stays 2, test expects 3) —
  Wave-6 owns that test update.
- `CreateAccountInput` gained Wave-6 fields (`also_known_as`,
  `moved_to_uri`, `moved_at_ms`): my test literals were updated; these
  test targets still need their literals updated (Wave-6/parent):
  `wave4_fanout_split`, `batch_q_phase1_app`, `conformance/*`,
  `gaps_api_admin`, `storage_eviction_heal`.
- `gaps_webhooks` (2 failures) asserts pre-Wave-3 loopback delivery;
  Wave-3 now SSRF-blocks it — Wave-3/parent updates that file.
- `client_profile`/`client_smoke` fail environmentally (`cargo` not on
  Git-Bash PATH for the smoke scripts) — pre-existing, unrelated.
- `federation_interop`/`federation_elixir` not run (external server
  checkouts, heavy); two-node live federation incl. NodeInfo endpoints
  passes via `--test federation`.

## Parent merge snippet: sweep resume cursor (excluded files)

`payload.rs` — add to `SuspendBroadcast` after `inbox_override`:

```rust
    /// Wave 4 scans: account-id cursor into the shared-inbox walk
    /// (`None` = first page). A sweep run fans out one page and, while
    /// pages remain, returns a same-payload continuation carrying the next
    /// cursor, so no run materializes the whole inbox set.
    #[serde(default)]
    after: Option<String>,
```

`execute.rs` — thread `after` through the `SuspendBroadcast` arm into
`deliver_suspend_broadcast` (destructure the new field, pass it along).
`deliver_actor.rs` — import `SUSPEND_SWEEP_PAGE_ROWS` +
`collect_shared_inbox_page` from `super::super::recipients`, and:

```rust
pub(crate) fn deliver_suspend_broadcast(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    job_key: &str,
    base: &str,
    account_id: String,
    activity_id: String,
    undo: bool,
    inbox_override: Option<String>,
    after: Option<String>,
) -> Result<DeliveryOutcome, PortError> {
    // ... render `activity` as today ...
    match inbox_override {
        Some(inbox) => deliver_single(services, client, tracker, &account_id, &inbox, &activity),
        None => {
            let (page, next) = collect_shared_inbox_page(
                services,
                after.as_deref(),
                SUSPEND_SWEEP_PAGE_ROWS,
            )?;
            let mut outcome = fan_out(services, client, tracker, job_key, &account_id, &page, &activity, &|inbox: &str| {
                FederateDeliverPayload::SuspendBroadcast {
                    account_id: account_id.clone(),
                    activity_id: activity_id.clone(),
                    undo,
                    inbox_override: Some(inbox.to_string()),
                    after: None,
                }
            })?;
            if let Some(cursor) = next {
                let cont = FederateDeliverPayload::SuspendBroadcast {
                    account_id: account_id.clone(),
                    activity_id: activity_id.clone(),
                    undo,
                    inbox_override: None,
                    after: Some(cursor.clone()),
                }
                .into_job(format!("{job_key}:sweep:{cursor}"), services.clock.now_ms())?;
                // `fan_out` only ever returns `Delivered` or `Split`.
                let mut followups = match outcome {
                    DeliveryOutcome::Split { followups } => followups,
                    DeliveryOutcome::Delivered => Vec::new(),
                    other => return Ok(other),
                };
                followups.push(cont);
                outcome = DeliveryOutcome::Split { followups };
            }
            Ok(outcome)
        }
    }
}
```

Also add `after: None` to the `SuspendBroadcast` literals in
`application/commands/moderation.rs`, `application/commands/accounts.rs`,
and `tests/wave4_fanout_split.rs` (100 inboxes < one 500-row page, so
that test's single-`Split(100)` expectation is unchanged). Cross-page
duplicate inboxes share the sweep `activity_id`, so receivers dedupe
(see `collect_shared_inbox_page` docs); Wave-6 idempotent enqueue
collapses the per-inbox dupes sender-side once it lands.
