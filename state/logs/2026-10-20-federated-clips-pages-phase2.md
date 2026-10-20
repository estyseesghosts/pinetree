# 2026-10-20 — Federated clips + pages phase 2 (sync, follow-push, gated pages, Article fidelity)

Implements `docs/handoff-federated-clips-pages-phase2.md` in full. Solo
build (no subagents for edits — parent-owned shared files per AGENTS.md;
three read-only investigation subagents fed the design).

## Phase 0 — reconcile with Batch Q (read-only, no code)

Batch Q Phase 1 (uncommitted at session start) landed: featured
Add/Remove ingest (`handle_featured_membership`), the `Accept` arm in
`handle_undo`, and the Person branch in `handle_delete`. None of it
overlaps this plan: the new dispatch arms are disjoint (Update/Delete of
`OrderedCollection` by embedded-type/URI shape, clip-URI branches inside
`handle_follow`/`handle_undo`-Follow before the account path), and the
clip `Add`/`Remove` eviction path is untouched. Verified at HEAD before
building: no `visibility` on `Page`, no clip tombstones, no `updated` on
collections, no follow store — the handoff's starting-state list held
except Batch Q's three federation items, which this plan does not re-touch.

## Phase 1 — pages: visibility, then gated ingest

- `Page.visibility: Visibility` (note-enum reuse) + `visible_user_ids`,
  both `#[serde(default)]` (old rows decode as public); local creates
  default Public. Enforcement (`check_page_visible`, owner passes,
  moderators bypass) on `pages/show` (id + name forms), AP
  `GET /pages/{id}` (tier gate + visibility; remote viewers of
  Followers/Specified fail closed), `pages/like`, `pages/featured`
  (public-only double-guard), pack path. New params `pageVisibility` +
  `visibleUserIds` (the existing `visibility` param keeps meaning tier).
- Ingest mapping `to`/`cc` → visibility (Public-in-to/cc ⇒ Public with
  documented collapse; followers collection ⇒ Followers; else Specified,
  best-effort ids, fail-closed). Well-formed Articles always materialize
  (only malformed objects keep the note path); existing Article-derived
  notes stay notes. Update path carries visibility forward.
- Local-viewer remote-followers rule: pass iff following the owner (local
  edge); remote viewers of cached Followers pages denied (fail closed).

## Phase 2a — clip sync

- `Clip.updated_at_ms` (served as `updated`; bumped in-txn on
  add/remove via `remove_clip_note(now_ms)` port change); resolver
  compares the stamp before full pulls. `CLIP_TOMBSTONES` index; deleted
  URIs serve 410 `Tombstone`/`formerType: OrderedCollection` (switch off ⇒
  404). `Update(Collection)` (embedded full doc, refresh-or-evict;
  `ingest_remote_clip_doc` test hook) + `Delete(Collection)` (delete and
  tier-restriction flips; evict + prune; forged owner-mismatches ignored;
  restrict prunes remote follows, delete cascades all). Fan-out tightened
  to Listed + flag + switch (documented behavior change); recipients =
  note authors + owner followers + clip followers sharing one activity id.

## Phase 2b — clip follow primitive

- `ClipFollow` rows on `ClipStore` (`follow_clip`/`unfollow_clip`/
  `is_following_clip`/`followers_of_clip`/`followed_clips_of`;
  `clip_follows` table). Endpoints `clips/follow|unfollow` (idempotent,
  `write:clip-favorite`) + `clips/followers` (owner-only, else 403
  `ACCESS_DENIED`, pinetree-fixed UUIDs) + `isFollowing` in the pack.
  Federation `Follow`/`Undo(Follow)` on collection URIs (authority =
  signer == follower + readable-clip gate; Misskey id-reuse: both ids
  forgotten on Undo so re-follow converges — caught by the new test).
  No new stream channel (notifications only).

## Phase 2c — notifications

- `ClipUpdated` (`clipUpdated`) + `PageLiked` (`pageLiked`); nullable
  `clip_id`/`page_id` on rows + payloads; packed as `clipId`/`clip` +
  `pageId`/`page`; `includeTypes`/`excludeTypes` + recieve-config accept
  both; `ALL_NOTIFICATION_KINDS` 11 → 14. Emit: membership → author;
  metadata → followers; delete/restrict → followers + member authors;
  page like → owner. Worker side-channels: body-only.

## Phase 3 — Article fidelity

- `remote_published_ms`/`remote_updated_ms` (served in place of row
  times); outbound `Delete(Article)` on local page delete; `source`
  schema documented + honored (`content` + `variables` + `presentation`
  + `slug`, applied on ingest with local-default fallback + slug
  disambiguation); audience audit (emitted `to`/`cc` matches visibility
  per the §4 table; Specified resolves local ids, keeps remote verbatim).

## Phase 4 — contracts, docs, gates

- `docs/federated-collections.md` §§2–4 + routes + non-goals (+ status);
  manifest §38 (+ two §36 superseding fixes); profile §5; parity-plan
  §14; wiki Current-State/Roadmap + README can/can't; new
  `docs/keystone-clip-page-surface.md`; conformance doc Groups Z + Y4.
- Conformance Group Z (Z1 follows converge/cascade, Z2 tombstone) + Y4
  (visibility + origin timestamps); `docs/README.md` count 115 → 118.

## Gates (real output)

1. `cargo test --offline`: 497 lib + 10 `batch_q_phase1_app` + 6
   `batch_q_phase1_fed` + 1 client_profile + 1 client_smoke + 118
   conformance (incl. new Z1/Z2/Y4) + 2 crash-recovery + 7 federation
   (incl. 2 new lifecycle tests) + 2 elixir + 1 interop + 1 minrepro + 5
   quote + 16 streaming = **667 total, 0 failed**.
2. `bash scripts/smoke-all.sh` → `SMOKE_ALL_OK` (phase2 four-server
   SKIP — no Node 22 in this environment, same as Batches O/P/Q).
3. `bash scripts/keystone-sweep.sh` → **60 passed / 0 failed** (was 51;
   +9: follow, isFollowing, followers owner/403, unfollow, gated create,
   gated show/like, new-kinds filter).
4. Fetch-gate matrix: 19 gate unit tests green (lib suite); tiers ×
   visibility × signer matrix covered live by the two new tests
   (anon 404 / stranger 403 with zero content bytes / follower reads).
5. Two-local-pinetree clip test
   (`federation_clip_follow_sync_update_delete`): Follow/Undo converge
   (incl. id-reuse re-follow), forged Update ignored, owner Update
   applies embedded doc, metadata/member/delete/restrict notify the
   right rows, TTL + `updated` revalidation, tombstone 410 at the
   deleted URI, tier-restrict evicts + prunes while capability fetch
   still serves.
6. Two-local-pinetree page test
   (`federation_page_gated_visibility_and_lifecycle`): gated Article →
   remote page row (not a note) for followers audience; stranger 403/404
   + unauthorized 400s with zero content bytes on show/AP/like/featured;
   Update/Delete/Like converge; owner notified; origin timestamps stored
   and tracked across update; public control page featured for all.

## Caught during verification

- Test-env disk pressure: `/tmp` (9.8G tmpfs) filled by ~21k stale
  `pinetree-api-test-*` dirs from earlier sessions → conformance failed
  with `No space left on device`. Removed the stale dirs (left
  `/tmp/opencode/*` alone); 118/118 after. Recurrence note: test tempdirs
  accumulate per run; clean between suites when `/tmp` is tight.
- Clip Undo id-reuse: re-follow after Undo converged without re-inserting
  (Undo never forgot the Follow id). Fixed like the account path (forget
  both ids); the new test pins it.
- Owner-signed loopback AP fetches self-deadlock (the gate resolves keys
  over HTTP even for local key ids; production nodes never self-fetch).
  The page test avoids local-signer AP fetches; owner reads go through
  the API. Documented in the test.
- `pages/show|like` hide gated pages as HTTP 400 `NO_SUCH_PAGE`
  (Misskey shape), not 404 — test asserts code + zero bytes.

## Open questions (owner defaults taken, recorded in manifest §38)

Followers-list owner-only; quieter notifications; `write:clip-favorite`
for follows; tombstones kept forever. Divergence from the handoff: none —
all four open questions took the proposed defaults.

Uncommitted at session end: everything in `git status` (this plan's
files + Batch Q's still-uncommitted files) — commit left to the owner.
