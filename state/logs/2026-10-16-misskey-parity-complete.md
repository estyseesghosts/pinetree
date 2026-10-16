# 2026-10-16 Misskey parity plan complete (backend, §§0A–13)

Session: finished `docs/misskey-parity-plan.md` (rev.3, all sections checked
LANDED + VERIFIED). Builds on commit `2f966fb` + `764ec7a`; the keystone
agent's work was confirmed already pulled (user), no rebase performed.

## Commits

- `764ec7a` — §§0A/0B/1/2/4/5/7/9/10/11/12 + §6-wire + partial §3/§8
  (previous session; the §3/§8 part was agent-cancelled mid-slice and
  unverified):
  - §0A blocking/muting: `BlockStore`/`MuteStore`, blockings/mutings/
    renote_mutings tables, block/mute endpoints + `users/relation`,
    enforcement + `FeedFilter` on every timeline/note read.
  - §0B full profile store: all 39 `i/update` params (+behaviors:
    verifiedLinks reset, alsoKnownAs resolution, word-mute validation,
    avatarDecorations role gating, column-default packer fixes), plus the
    `rejectQuotes`/`defaultReaction` columns.
  - §§1-2/9 privacy gates (`users/followers|following` masks,
    `users/reactions`, 250-follow sweep test).
  - §4 polls + §6 wire compat (bare-U+2764 FALLBACK, legacies map, FE0F
    strip, `Update(Question)` arm, expiry job).
  - §5 drafts/scheduling (`JobKind::PostScheduledDraft`,
    scheduledNotePosted/Failed).
  - §7 pins/featured (`pinnings`, `i/pin|unpin`, featured ranking: 3d
    global / 7d per-user windows, 3600s cache).
  - §10 `i/notifications-grouped`.
  - §11 notification receive-config + read-time filters; §12 timeline
    filters/mutedInstances/self-reply fix.
  - §3/§8 partial: `notes/edit` arm, `note_edits`/`note_edits_by_note`
    tables, `apply_note_edit`/`note_edits`, `QUOTE_DISABLED_FOR_USER`
    enforcement (create + edit), `handle_update_note`.
- `07d834e` — §3/§8 verification (this session): storage-contract
  conformance Group E (E1 lifecycle, E2 no-op writes nothing, E3 poll
  replace resets votes, E4 history chaining, E5 inbound dedup, E6 quote
  strip/throw), 6 endpoint tests (`parity_notes_edit_*` gates/no-op+
  history/reply-immutable+visibility-forced/poll resets/edited
  notifications, `parity_reject_quotes_end_to_end` with verbatim
  UUIDs `d697edc8-…`/`1c0ea108-…`), inbox `Update(Note)` round-trip tests
  (render→`handle_update_note` applies remote edit, replay converges via
  dedup, authority gates, local-author protection,
  `render_note_update_activity` shape pin). One test-only visibility
  tweak (`pub(crate) mod tests` in commands). No implementation bugs
  found — the cancelled agent's code was sound.
- `888f522` — §6 remainder + §13 (this session, two parallel implementer
  agents, merged with three small collision fixes):
  - §6: `notes/reactions/create` `reaction` param optional (DIVERGENCE —
    upstream requires it) resolving to `defaultReaction ?? ❤` via
    `react_with_default`; federation split: per-recipient-host emoji
    capability (`HostCapsStore` port + `HOST_SOFTWARE` redb table, lazy
    NodeInfo 2.1→2.0 probe off-transaction through the SSRF-safe client,
    7d TTL, failures never cached, unknown/failed → emoji-aware for
    upstream always-sends parity) driving `Like`+`_misskey_reaction` vs
    plain `Like` (mastodon/hometown/glitch/pixelfed/lemmy/pleroma/
    akkoma/gotosocial); `UndoLike` re-resolves; inbound bare `Like` →
    FALLBACK test-pinned; `host_of_url` keeps `host[:port]` (pinetree
    host semantics, webfinger parity).
  - §13: 13 endpoints (`i/export-following|blocking|mute|notes|
    user-lists|antennas|clips|favorites`,
    `i/import-following|blocking|muting|user-lists|antennas`), credential
    + per-endpoint 1/hr, `204` + durable `JobKind::Export|Import` jobs
    (ids only, never file content); exports write upstream-shaped files
    (CSV per `Export*ProcessorService` — `export-mute` is CSV, the plan's
    "JSON" wording was a slip, corrected in the plan — and the notes JSON
    DTO) to the caller's drive and emit `exportCompleted
    {exportedEntity, fileId}` (`NotificationKind::ExportCompleted`,
    closes the §11 type gap); imports carry the verbatim per-endpoint
    UUIDs (`NO_SUCH_FILE`/`EMPTY_FILE`/`TOO_BIG_FILE` per
    `import-blocking.ts:53-86`), validate per row, skip unresolvable
    rows, and apply through the interactive ops so §0A gates hold;
    no-subsystem entities (lists/antennas/clips/favorites) export empty
    valid files and import zero rows (documented DIVERGENCEs; manifest
    §20 + the plan §13 status line enumerate every divergence).

## Verified (executed evidence)

- Environmental linker failure (undefined `anon.*.llvm.*`/hidden
  `drop_glue` symbols on the federation/streaming test binaries) was
  corrupted incremental-cache artifacts, not a code error:
  `rm -rf /root/pinetree-target/debug/incremental` (5.2G) fixed it; the
  full gate ran with `CARGO_INCREMENTAL=0`.
- `cargo test`: **500 passed, 0 failed** (lib 390 = 355 pre-parity + 9
  §3/§8 + 12 §13 + 6 §6 + 8 host_caps + misc; conformance 83 = 77 + 6
  Group E; streaming 13; federation 2; crash_recovery 2; quote_fixtures
  5; client_smoke/minrepro/elixir/interop/doc-tests remainder).
- `cargo build`: no new warnings (4 pre-existing lib warnings unchanged).
- `bash scripts/smoke-all.sh`: **SMOKE_ALL_OK** — cargo test step,
  smoke-phase1 (real misskey-js), smoke-phase3 (real misskey-js Stream),
  keystone-sweep (41/41) all PASS. `smoke-phase2` (four-server
  federation) reported its documented SKIP: `SHARKEY_NODE_BIN` (Node 22)
  not provisioned in this environment. The §6 outbound Like change keeps
  the emoji-aware render byte-identical to before (`render_like_activity`
  delegates with `true`), so the Phase-2 interop surface is unchanged for
  unknown hosts; in-process federation paths are covered by the
  federation/crash integration tests.
- Upstream evidence read live this session: Sharkey
  `NoteCreateService.checkQuotePermissions` (`:1174-1200`, quoter-side
  gate — plan rev.2 had the semantics inverted; corrected in the plan
  with a dated note), Sharkey `admin/reject-quotes.ts`, Misskey
  `notes/reactions/create.ts` (`reaction` required upstream → the
  heart-click default is a pinetree DIVERGENCE), Misskey
  `i/export-*.ts`/`i/import-*.ts` + every `Export*`/`Import*ProcessorService`
  (payloads copied verbatim), `import-muting.ts` (CSV, not JSON).

## Open questions / carried forward

- `smoke-phase2` re-run against the parity tree needs the lab
  (`SHARKEY_NODE_BIN` + docker + Node 24); worth doing before any
  federation-facing release announcement.
- §11 `list(userListId)` receive-config: accepted on write, drops at
  creation time (upstream missing-list parity); revisit when a user-list
  subsystem lands (would also make §13's user-lists import/export
  non-empty).
- `admin/reject-quotes` has no moderation-log row and no moderator-role
  gate (TODO comments in the endpoint arm); `alsoKnownAs` remote
  resolution and `drive/files/create`-dependent bio-media checks follow
  the pinetree store-only pattern.
- Featured trailing windows (3d global / 7d per-user) are
  compatible-simplified, not upstream-identical, as the plan allows.
- The `host_software` cache grows unbounded per probed host (one small
  row each); a sweep/GC is not yet implemented.
