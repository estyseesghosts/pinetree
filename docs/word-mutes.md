# Server-side word mutes (Batch N WS8)

**Status:** Draft (2026-10-20; owner-approved DIVERGENCE from upstream)

Upstream Misskey/Sharkey never enforce word mutes server-side
(`hardMutedWords` is only packed, `UserEntityService.ts:616`); enforcement
lives in the frontend (`check-word-mute.ts` + `use-note.ts`). Pinetree
enforces them server-side so every client (including Keystone, which
implements the same matcher in `frontend/src/misskey/softMute.ts`) gets
identical behavior without re-implementing the matcher.

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
- Pending (unresolved-parent) replies carry no body and are skipped.

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
