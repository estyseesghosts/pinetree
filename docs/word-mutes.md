# Server-side word mutes (Batch N WS8)

**Status:** Implemented (Batch N WS8; spec re-verified 2026-10-20; owner-approved DIVERGENCE from upstream)

Upstream Misskey/Sharkey never enforce word mutes server-side
(`hardMutedWords` is only packed, `UserEntityService.ts:616`); enforcement
lives in the frontend (`check-word-mute.ts` + `use-note.ts`). Pinetree
enforces them server-side so every client (including Keystone, which
implements the same matcher in `frontend/src/misskey/softMute.ts`) gets
identical behavior without re-implementing the matcher.

## Status (2026-10-20 freshness pass)

Verified against `src/domain/word_mute.rs`, `src/application/queries.rs`, `src/api/misskey/packers.rs`, `src/application/commands/{accounts,antennas,notifications}.rs`, `src/streaming/server.rs`, and the upstream checkout `misskey/packages/backend/src`.

**LANDED (verified)**
- Matcher: compile (`src/domain/word_mute.rs:42-64`; empty keywords and empty groups dropped, invalid shapes dropped). Match (`:72-94`; self-exempt; CW-then-text haystack, trimmed). Flags (`:103-131`; `v` and other unknown flags never match; `y` anchors). Unit tests from `:133`.
- Hard drop: the `feed_filter_hides` arm (`src/application/queries.rs:232-233`) and the `note_hard_muted` self, then reply, then renote chain (`:258-300`). The filter is compiled per read (`:125-126`).
- Surfaces that call the filter. `apply_feed_filter` callers: `queries.rs:714,759,801,1056,1422,1489,1615`. Direct `feed_filter` callers: `:516` (notes/show), `:902` (channel eligibility), `:1027` (capture), `:1228` (replies), `:1339` (conversation), `:1870` (user reactions). Also `src/application/commands/antennas.rs:248`, streaming `src/streaming/server.rs:589,832`, and `capture_gate` (`queries.rs:999-1030`).
- Soft flag: `isSoftMuted` (`src/api/misskey/packers.rs:1738-1750`; the no-own-CW rule is at `:1744`; the matcher call at `:1478-1495`). The hard list is packed as `hardMutedWords` (`packers.rs:1322-1323`).
- Write validation: the word-mute validation loop in `src/application/commands/accounts.rs:231`. Endpoint tests at `src/api/misskey/endpoints/mod.rs:10936-10951` exercise invalid and over-length shapes.
- Tests: `word_mutes_server_enforced_hard_drops_soft_keeps` (`queries.rs:2471`) and `batch_n_word_mutes_soft_flag_packed_hard_dropped` (`endpoints/mod.rs:19715`).
- Upstream: Misskey packs `hardMutedWords` at `misskey/packages/backend/src/core/entities/UserEntityService.ts:616`. Its `i/update` validates them (`server/api/endpoints/i/update.ts:344-348`) and does not enforce them on reads. Server-side enforcement is the stated divergence.

**CORRECTED**
- The old text said pending (unresolved-parent) replies "carry no body and are skipped". That is wrong. The stored pending reply keeps its text (`src/federation/inbox.rs:2227`, `text: parsed.text.clone()`). It is skipped because `is_visible` withholds it (`src/domain/visibility.rs:71-73`) before the word-mute filter runs. The paragraph below is corrected to match. Whether `note_hard_muted` can match a pending parent's body inside a reply chain was NOT verified.

**NOT VERIFIED**
- "Notifications are not gated" is consistent with the code: `src/application/commands/notifications.rs` has no word-mute reference. The notification write path was not re-read.
- The per-read note cache matches the `note_cache` parameter (`queries.rs:176-177`). Its behavior was not tested.

## Semantics (Misskey-exact)

- Haystack is `(cw ?? '') + '\n' + (text ?? '')`, trimmed (CW first, so
  `^` anchors at the CW).
- Array filters are case-sensitive substring AND-groups (empty keywords
  dropped; a group left empty never matches).
- Bare-string filters are `/pattern/flags` regexes. Supported flags:
  `i` (case-insensitive), `m` (multi-line), `s` (dot-all), `g`/`d`/`u`
  (no matching effect), `y` (sticky → start-anchored). Anything else
  (notably `v`) never matches — fail-closed documented choice. Invalid or
  non-matching shapes never match.
- A user's own notes never match. Empty text never matches.
- A note matches when itself, its reply, or its renote matches
  (`checkNoteWordMute`: self → reply → renote, first match wins).
- Pending (unresolved-parent) replies keep their stored body but are withheld by `is_visible` (`src/domain/visibility.rs:71-73`) before the word-mute filter runs. See the 2026-10-20 status block.

## Enforcement

- **Hard mute** (`hardMutedWords`): the note is dropped in `FeedFilter`
  (`feed_filter_hides`, after the mute/block/instance/suspended arms) on
  every timeline/note read: home/local/hybrid/global timelines,
  `users/notes`, `notes/show|replies|children|conversation|renotes`,
  `notes/search|search-by-tag`, `users/reactions`, `antennas/notes`, the
  `antenna` stream event, streaming timeline eligibility
  (`note_eligible_for_channel`), and note capture (`capture_gate`).
  Self-notes exempt per the matcher.
- **Soft mute** (`mutedWords`): do NOT drop. Pack a viewer-specific
  `isSoftMuted: true` on the note when the set transitively matches AND
  the note has no CW of its own (the Keystone badge rule: a note with its
  own CW needs no badge). Any client can badge/hide-with-reveal from this
  flag; Keystone migrates its badge source from local computation to this
  flag (cross-repo follow-up — reported, not implemented here).
- **Notifications are NOT gated** on word mutes (neither client does;
  creation-time semantics unchanged).

## Perf / FDB safety

- Per-user filter sets compile once per read (`feed_filter`) or per pack
  (`pack_note`); the stored set is char-capped at write time (200), so
  compile cost is bounded. No per-note regex compile at write time, no
  transaction growth (read-time only): FDB-safe.
- Reply/renote bodies resolve through a per-read note cache (one storage
  read per distinct target per page/scan).

## Conformance

- Domain matcher: `src/domain/word_mute.rs` unit tests (haystack order +
  trim, AND-group case-sensitivity + empty-drop, regex flags, non-shape
  and unknown-flag never-match, self-exemption).
- Read path: `word_mutes_server_enforced_hard_drops_soft_keeps`
  (`src/application/queries.rs`): hard regex/AND drop on timelines,
  partial-AND survives, reply-transitive drop, self-exempt, soft/CW notes
  survive, `notes/show` fail-closed, streaming gates agree.
- Packing: `batch_n_word_mutes_soft_flag_packed_hard_dropped`
  (endpoint tests): `isSoftMuted` true/false, hard reads as
  `NO_SUCH_NOTE`, unmuted viewers see `false`.
