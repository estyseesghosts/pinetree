# 2026-10-20 — Batch Q Phase 0 + Phase 1 (nine audit findings verified + fixed)

## Phase 0 — verify at HEAD (four read-only subagents, no code)

All nine audit findings confirmed still open at HEAD (post-Batch P / federated-clips-pages / JSON-config tree). Items 6 and 7 only partially fixed:

| # | Finding | Verdict at HEAD |
|---|---|---|
| 1 | `following/requests/list` requires `read:account` vs upstream `read:following` (`list.ts:18`) | STILL BROKEN (`mod.rs:5800`) |
| 2 | `delete_note` rejects every non-author; manifest promises moderator delete | STILL BROKEN, code wrong / manifest right (upstream `delete.ts:68-73` allows moderators) |
| 3a | `users/show` shows suspended users | STILL BROKEN (no gate in `queries.rs:480-499`) |
| 3b | `notes/show` emits no `CONTENT_RESTRICTED_*` | PARTIAL (mute/block/suspend hiding present as `NO_SUCH_NOTE`, stricter than upstream; both `CONTENT_RESTRICTED_*` codes absent; `requireSigninToViewContents` gated only on `notes/versions`) |
| 4a | `i/notifications` no rate bucket (upstream 30/30s) | STILL BROKEN (also `grouped`) |
| 4b | empty filter marks-all-read before returning `[]` | STILL BROKEN, both arms (upstream early-returns at `notifications.ts:72-79` before `:93-95`) |
| 5 | `following/create` `withReplies` defaults true vs upstream false | STILL BROKEN (`mod.rs:5720`; effective upstream default is the `Following.ts:49-52` column default `false`) |
| 6 | `notes/create` fileIds/mediaIds merge | PARTIAL (`fileIds`-wins only in draft/schedule shim; main path unions, `mod.rs:3978-3983`; upstream `create.ts:226` is `??`) |
| 7 | `notes/timeline` include-flags | PARTIAL (`with*` honored everywhere; `includeMyRenotes/includeRenotedMyNotes/includeLocalRenotes` honored only on user-list-timeline `queries.rs:891-980`, silently ignored on home/hybrid) |
| 8a/b/c | featured Add/Remove ingest, `Undo(Accept)`, `Delete(Person)` | ALL STILL BROKEN (Add/Remove clips-only; no `Accept` arm in `handle_undo`; no Person branch in `handle_delete`; fetch-gate/clips work covered none) |
| 9 | `meta.mediaProxy` advertises `/proxy`, no route | STILL BROKEN (advertised `packers.rs:2264-2267`, router `mod.rs:100-161` has no proxy route) |

Phase 0 second deliverable (remainder punch list, grouped WS1–WS6) is in subagent report `ses_ea7c8b4f8ffelI4Rgl4yw5KC03` in this session's transcript: ~150 local endpoint arms vs 434 upstream files. Landed-and-closed (not re-implemented): Batch O WP9/WP10/WP12/WP13/WP16, Batch P §35, local `clips/*` (11) + `pages/*` (7) + `i/import-clips`, `notes/schedule/*` shim, `i/notifications-grouped`. Recorded-out families stay out (2FA branches, webhooks, drive folders/stream, announcements, ads, hashtags-aside `search-by-tag`, channels/chat/reversi, roles, charts, gallery/flash, sw/*, thread-muting, relays, LD-signatures, OAuth, Mastodon). Unrecorded-but-missing queue for Phase 2: `my/apps`, `drive/files/find|attached-notes|check-existence|move-bulk`, `page-push`, `users/update-memo`, `following/list|update|update-all|requests/sent`, `notes/show-partial-bulk|clips|polls/recommendation`, `users/clips|pages`, `i/pages|page-likes|gallery/*`, `email-address/available`, `v2/admin/emoji/list`, `federation/*` (except instances, recorded-out), retention, plus WS6 unbucketed reads (`notes/search|search-by-tag`, `users/notes|followers|following`, `notes/show` family, `drive/files*`, `users/search`).

## Phase 1 — fixes (parent + 2 implementers, disjoint file sets)

- Parent (shared files): items 1/4/5/6/9 in `endpoints/mod.rs` + `response.rs`; P1/P2/P3 snippets, P4 error codes, P5 `ugcVisibilityForVisitor` setting, `users/show` rewrite, `admin/update-meta|meta`, `notes/conversation` gate, `update_instance_settings` signature (+3 call sites in `tests/federation.rs`), `users_show_no_such_user` 404 constructor.
- `ses_ea7c8d246ffe9pl7dWe7GMPpnv` (owned: `commands/notes.rs`, `queries.rs`, `tests/batch_q_phase1_app.rs`): item 2 (moderator bypass inside `delete_note`, signature unchanged), 3a (`users_show_by_id_as`/`_by_username_as`), 3b (`_BY_USER` gate), 7 (`renote_excluded_by_include_flags` + `home/hybrid_timeline_with_includes`, old fns delegate). 10/10 new tests.
- `ses_ea7c8d236ffeRgaJ4Rgl4yw5KC03` (owned: `federation/inbox.rs`, `tests/batch_q_phase1_fed.rs`): 8a (`handle_featured_membership`), 8b (`Accept` arm in `handle_undo`), 8c (Person branch in `handle_delete`). 5/5 new tests, zero parent snippets needed.
- Parent also added: `tests` in `mod.rs` (8 dispatch tests), live-HTTP 8a leg in `batch_q_phase1_fed.rs` (6th test), sweep `interval` param + `/proxy` checks.

Owner decisions (here, not deferred): item 2 — code changes, manifest was right; item 5 — both `following/create` and `i/import-following` go to `false` (import processor treats omitted as falsy; old §13 DIVERGENCE removed); item 9 — implement a real `/proxy` (`?url=` wins, cosmetic `/proxy/{*rest}` accepted; SSRF-guarded client, 8 MiB cap, inline-safe media only, UA required, `misskey/` UA refused, failures → plain 404); ugc default `all` (DIVERGENCE from upstream `local`: our public timelines serve remote notes to visitors, so gating only detail/show would be incoherent; timelines/streaming/pack-level ugc filtering deferred with upstream pointers recorded); bare-IRI `Undo(Accept)` converges accept-without-effect (no activity-ID index; deferred as schema/ports change); upstream-dead `ugc === 'none'` branch in `users/show.ts` not implemented. Media-pipeline transparencies (`/proxy` no transcoding/variants, no `dummy.png`, SVG refused) recorded as DIVERGENCEs. Config-rule note: `ugcVisibilityForVisitor` is store-backed admin meta (precedent: registration/blockedHosts/federateClips/federatePages, none of which have Config fields) — set via `admin/update-meta`, read via `admin/meta`, documented in manifest §37.

Caught during verification: the new ugc gate initially fired before the fail-closed pending-reply withhold, changing `unknown_parent_stays_pending_then_links_on_arrival`'s error kind — fixed by ordering the pending check first in `notes_show` (indistinguishability). `parity_import_following_acct_and_legacy_rows` updated to the new `false` default. `tests/streaming.rs::disconnect_and_unsubscribe_go_silent` failed once under full-suite load (400 ms silence window), passes alone and on suite re-run (16/16) — pre-existing timing flake, untouched code path.

## Gates (real output)

1. `cargo test --offline`: green — 497 lib + 10 `batch_q_phase1_app` + 6 `batch_q_phase1_fed` + 115 conformance + 2 crash-recovery + 5 federation + 5 federation_interop + 16 streaming + 7 misc (client_profile/smoke/elixir/minrepro/quote/client_smoke) = 663 total, 0 failed.
2. `bash scripts/smoke-all.sh` → `SMOKE_ALL_OK` (cargo test 21s, phase1 1s, phase3 2s, keystone 3s; phase2 four-server SKIP — needs `SHARKEY_NODE_BIN`, skipped before this batch too).
3. `bash scripts/keystone-sweep.sh` → 51 passed, 0 failed (includes 3 new `/proxy` checks; sweep `wait_for` gained an `interval` param, notification polls moved to 1.0 s so the new 30/30 s bucket can't flake them).
4. Live two-server proof: `inbound_featured_add_fetches_note_over_live_http` — signed `Add` against pinetree with the note fetched over real loopback HTTP (origin-checked + ingested + pinned); Undo(Accept)/Delete(Person) proven via signed-inbox handler tests + full two-node `federation.rs` suite green (5/5).
5. This log.

## Contracts

- `docs/endpoint-manifest.md` §37 (complete: all nine items + DIVERGENCEs); §4 `notes/delete` row now true; `following/create` + `i/import-following` rows `withReplies = false`; fileIds row documents `??` precedence; old import-following DIVERGENCE removed. Profile/parity-plan need no changes (no surface added/removed; `ugcVisibilityForVisitor` is admin-meta, not profile surface). README/wiki need no changes (no `mediaProxy` mentions were stale; `/proxy` now exists as advertised).

## Open questions / Phase 2 queue

Phase 2 workstreams WS1–WS6 per the handoff, in dependency order, using the Phase 0 punch list above. Explicitly still out: `notes/translate`, achievements, passkeys, Mastodon API, OAuth, local clips/channels/pages, thread-muting, relays, LD-signatures, email sending (dormant), custom roles. Uncommitted at session end: modified `AGENTS.md` (pre-existing), `docs/endpoint-manifest.md`, `docs/handoff-batch-q.md` (untracked input), `src/*`, `tests/*`, `scripts/keystone-sweep.sh` — commit left to the owner.
