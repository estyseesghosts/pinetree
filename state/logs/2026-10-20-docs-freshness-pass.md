# 2026-10-20 — docs freshness pass + human-readable wiki

Scope: docs only. Brief: `docs/handoff-docs-freshness-pass.md`. Inputs: `docs/inventory-pinetree-vs-misskey-2026-10-20.md`, `docs/TODOS.md` (verify-then-close first), `docs/README.md`, `state/logs/2026-10-20-todos-*.md`.
No file under `src/`, `tests/`, `scripts/`, or `Cargo.*` was changed. No commits, stashes, resets, or checkouts. `scripts/fediverse-bench/__pycache__/` was already untracked before this pass and was left alone.

## What changed

Five workstreams ran in parallel on disjoint file sets. Shared files (TODOS, README, wiki/Home) were merged by the parent.

1. **API contracts** (`endpoint-manifest.md`, `phase-3-profile.md`, `misskey-parity-plan.md`, `sharkey-parity.md`, `catodon.md`)
   - Frozen manifest and profile changed by dated addenda only (+86/-0 and +45/-0 lines). The manifest addendum is §39; the profile addendum is §8.
   - Corrected: streaming `updated` is emitted (`src/streaming/server.rs:700-728`); `i/revoke-token` `tokenId` is implemented (`mod.rs:6776-6799`); `notes/featured`, `i/notifications-grouped`, `notes/reactions` ship (`mod.rs:8998,9029,9067,6278`); `notes/search` offset/range ships, `channelId` still rejected (`mod.rs:8146-8178`); `server-info` and `reset-password` ship (`mod.rs:7759,7311`); `antenna`/`userList` streaming channels ship (`server.rs:341-345`); moderator bypass is live (`queries.rs:1704-1713`); `Update(Note)` edit branch is live (`inbox.rs:1796-1797,1953`); `withReplies` on timeline is confirmed absent and documented (`mod.rs:6132-6147`).
   - Still open: `users/lists/create-from-public` (404), `queue/*`, `channels/*`, `chat*`, `hashtags/*`, `drive/folders*`, `i/2fa*`, `notes/clips`, `users/update-memo`, `notes/thread-muting/*`, `following/update|list|requests/sent`, `announcements*`. `notes/reactions` cursor still diverges (`queries.rs:1944`).

2. **Domain / federation contracts** (`domain-types`, `application-ports`, `visibility-policy`, `identity-rules`, `activitypub-collections`, `quote-posts`, `replies`, `federated-collections`, `word-mutes`)
   - Frozen files changed by addenda only. Added in this pass by the parent: an `application-ports.md` addendum (+15 lines, see Finding A below).
   - Corrected: hide-followers fields exist (`domain/account.rs:142,145`) and gate AP collections (`activitypub/actor.rs:922,981`); `blocked_hosts()` is store-backed (`queries.rs:135-137`); `replies_count` decrement is landed (`redb/mod.rs:1797-1801,1865,4212-4217`); inbound unresolved `inReplyTo` is landed (`inbox.rs:2113-2126,2170-2177`); quote renote default is landed (`domain/note.rs:92`); word mutes store the pending reply body (`inbox.rs:2227`), withheld by `visibility.rs:71-73`.
   - Replies slices A and B are landed on redb; C is partial; FDB is open.

3. **Plans and decisions** (`extension-plan`, `community-planning`, `pinetree-decisions`, `remote-media-cache-plan`, `implementer-guide-batch-m`, `keystone-clip-page-surface`, `persistence-conformance`, `phase-1-plan`, `phase-2-plan`, `phase2-5`, `phase2-6`)
   - Each file got a `Status (2026-10-20 freshness pass)` header and per-section tags. Original text is untouched (0 deletions).
   - Remote-media cache plan: all 8 build steps are OPEN. No eviction, quota or reconciler. Eager fetch on arrival (`inbox.rs:2157`). Full-body buffering (`remote_cache.rs:245-256`). `delete_media` non-atomic (`commands/media.rs:211-216`).
   - Batch M: most WPs landed; WP8 (admin emoji delete/list) and WP16 (`create-from-public`, remote-member actor) are open.
   - Hide-followers gap and `blocked_hosts()` gap, both listed as open in older TODO lines, are closed.

4. **Archive and index** (28 bench/handoff/audit/exit/how-to docs + `testing.md`)
   - Each doc got a status block. Historical lines were kept. `testing.md` had stale script names corrected and gained a "Known gaps" section.
   - Landed: outbound poll `type:Note` (`objects.rs:1070`); inbound `EmojiReact` (`inbox.rs:346`); `NOTES_BY_URI` uniqueness (`redb/mod.rs:1139,1148`); S6 Delete-beats-Create tombstones (`inbox.rs:1740-1748`); `ap/show` reply and media (`inbox.rs:2081-2153`); `notes/reactions` list.
   - Harness-only: birchtree reblog and poll fixes, S7 cap, settle-until-deadline, `fetch_bytes` lab CA, interop oracle.

5. **Wiki** (`docs/wiki/`)
   - Rewritten: `About`, `Current-State`, `Feature-Matrix`, `Roadmap`. New: `Intentional-Differences`, `Glossary`, `How-To-Read-The-Docs`. Parent rewrote `Home.md` and corrected two `Current-State` "Known doubts" (see Finding B).
   - Every page has a "Read more" section linking to contract docs. No code blocks.

6. **Parent-owned shared files**
   - `docs/TODOS.md`: the verify-then-close section is resolved and empty (5 items closed or promoted). About 25 items were checked off with evidence. A new section lists 12 items the workers found (see Findings below).
   - `docs/README.md`: rewritten with the same rows, updated statuses, and an archive index. Rows added for word-mutes, sharkey-parity, catodon, phase2-5/2-6, the extension and community plans, keystone, TODOS and the inventory.
   - `docs/wiki/Home.md`: rewritten with the seven-page index.

## Findings the workers surfaced that contradict frozen or other docs

- **A. `application-ports.md:181` says "no `unimplemented!()` defaults". Code has 20** (`src/application/ports.rs:1037-1612`: `FollowStore` x3, `ActivityDedup` x17). Redb overrides all 20, so tests cannot see it; an FDB adapter that omits one would panic. Recorded as a dated addendum and a P2 TODO. Not fixed: changing the defaults is a code change.
- **B. Federation default for clips and pages.** The commit `7f0c965` message says "pages federate by default". Code says both instance switches default OFF (`src/domain/instance.rs:128-130`). Wiki follows the code.
- **C. Sharkey pin.** `phase-2-plan.md` header says `2025.4.4`; `README` and `phase-2-exit-criteria.md` say `2025.4.7`. Flagged in the header; the 2025.4.4 run is the recorded evidence. Still unreconciled.
- **D. Quote auto-approval** covers Public and Home (`inbound/quotes.rs:113-114`), not only Public as `quote-posts.md` implies. Open decision.
- **E. Gate.** `scripts/smoke-all.sh:53` pipes cargo through `tail -30`, masking the exit status. SKIP still prints `SMOKE_ALL_OK` (`:77-92`). Keystone runs 38 in the script and 41 in `testing.md`. Not fixed here (code-touching).
- **F. Config rule.** `PINETREE_REPLY_REPAIR_LIMIT` is env-only (`main.rs:307`); job lease and retry numbers are hard-coded (`main.rs:334-335`). Both violate AGENTS.md's configuration rule.

## Verification (executed 2026-10-20)

```
== 1. code/test/script/Cargo touched (expect 0)
0
== 2. frozen files: deletions (expect 0 each)
domain-types: +19 -0
identity-rules: +14 -0
visibility-policy: +18 -0
application-ports: +36 -0
endpoint-manifest: +86 -0
== 3. verify-then-close open boxes (expect 0)
0
== 4. wiki relative links (resolved per file; expect no BROKEN)
links checked=200 broken=0
== 5. conformance #[test] fns (expect 118)
118
== 6. ports unimplemented! count (expect 20, addendum documents them)
20
== 8. git status: non-doc entries
?? scripts/fediverse-bench/__pycache__/   (pre-existing, not from this pass)
```

Additional checks:
- All 20 `unimplemented!()` method names have exactly one `fn` definition in `src/persistence/redb/mod.rs`, so redb overrides each one (grep, 20/20 found).
- `tests/conformance.rs` has 118 `#[test]` fns (`grep -c "^#\[test\]"`). The "119" figure from a looser grep was wrong; 118 is correct.
- Frozen `Status:` lines were not changed. The five frozen files show only additions.

Not run: `cargo test` and `scripts/smoke-all.sh`. This pass changed no code and no tested behaviour. The conformance count was checked by grep, not by running the suite. Per-feature behaviour of Batch N/O/P was not re-tested; only endpoint presence was spot-checked.

## Exit criteria (handoff `docs/handoff-docs-freshness-pass.md`)

- [x] `docs/TODOS.md` verify-then-close section is empty (verified: 0 open boxes).
- [x] Every doc touched has an accurate `Status:` line, with no claim contradicted by code *that the workers checked*. Several archive docs were checked by spot sample, not line by line (see open questions).
- [~] Wiki reads clean for a non-technical reader. Jargon is defined in `Glossary.md`, and every link resolves (200/200). Nobody who is not an engineer has read it yet.
- [x] This log.
- [x] `git status` shows only docs and wiki edits plus the untracked handoff, inventory, and state logs from earlier passes.

## Open questions for the owner

1. Decide application-ports A: make the 20 methods required (remove the defaults) or return `StorageUnavailable`. Either is a superseding decision.
2. Quote auto-approval scope (D): keep Public + Home, or narrow to Public as the doc says?
3. The gate script (E) and the Keystone count (E): fix in a separate code change?
4. Sharkey pin (C): which version is the reference for Phase 2?
5. Verify Batch N/O/P per feature (not just endpoint presence) before closing those TODO lines.
6. Re-check `sharkey-parity.md` and `catodon.md` Sharkey and Misskey claims against the reference checkouts; the workers did not.
