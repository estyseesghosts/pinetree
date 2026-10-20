# Handoff: Pinetree — Akkoma/Pleroma poll + emoji compat + notes/reactions

## Context (read first)

- Bench run5: `/root/pinetree/docs/federation-bench-2026-10-19-phase7-run5.md` (see "If put online today") + log `/root/pinetree/docs/federation-bench-2026-10-19-run5-log.md`
- Bug reports: `/root/bug_reports/README.md`, `/root/bug_reports/akkoma-delete-reblog-no-undo.md` (not yours — context only)
- Current pinetree behavior: `docs/misskey-parity-plan.md` §6 (reactions out-of-scope note)
- Code: `src/activitypub/objects.rs:1110-1111` (Question options, no `type`), `src/activitypub/inbox.rs:983`, `src/activitypub/objects.rs:1666`, `src/activitypub/inbound/notes.rs:244` (inbound Like/EmojiReact), `src/activitypub/host_caps.rs` (per-host Like shaping), `src/domain/` reaction model (`REACTIONS` + per-emoji counters, `coerce_reaction` → ❤)
- Counterparts (read-only): `/root/fedifedifedi/akkoma` v3.20.1 (`lib/pleroma/object_validators/question_options_validator.ex:30-35`, `lib/pleroma/activity/builder.ex:71-82`, `lib/pleroma/web/federator/transmogrifier.ex:509-526,667-692`), `/root/fedifedifedi/pleroma` ref checkout, `/root/bench/run5/runs/20261019T181810Z/features.out + stress.out` (grep akkoma)

## Objective

1. Polls you make votable from Akkoma/Pleroma.
2. Emoji reactions interoperable with Akkoma (both directions) if feasible without breaking Misskey/Mastodon.
3. Implement `notes/reactions` endpoint (currently count-only over API).

## Instructions (use subagents)

You MUST use subagents (`general` or `explore`) for parallel investigation. Suggested split:

- Subagent A (polls): compare pinetree Question emit vs Mastodon/Misskey/Catodon shapes; confirm Akkoma requires `options[].type == "Note"`; check Pleroma same validator; propose minimal emit change + risk to Misskey/Mastodon.
- Subagent B (reactions): map Akkoma `EmojiReact` vs `Like+content` vs bare `Like`; pinetree inbound dispatch drop point; Misskey `_misskey_reaction`/`content` fallback; Mastodon bare-Like path; propose accept + emit plan that preserves existing passes.
- Subagent C (notes/reactions): spec the Misskey endpoint from `/root/fedifedifedi/misskey` + Sharkey ref, domain port, storage query, pagination, auth/visibility rules; list contract updates needed in `docs/`.

Do not skip the subagent step — run A+B+C in parallel, then synthesize.

## Investigation questions to answer

- Polls: does adding `"type":"Note"` to each Question option fix Akkoma with zero regression on Misskey/Catodon/Mastodon/bt? Any other required fields (`replies`, `closed`, `votersCount`)? Verify against real Akkoma validator, not just bench.
- Reactions: can we accept `EmojiReact` (map to same per-(user,note) row + counter) and emit `EmojiReact` to Pleroma-family hosts (via `host_caps`) while keeping bare `Like` to Mastodon and `Like+content+_misskey_reaction` to emoji-aware hosts? What does Pleroma expect for custom vs unicode emoji? Document heartbeat risk.
- notes/reactions: exact Misskey request/response, `type` filter, limit/offset, visibility/mute/block filtering, performance (no unbounded scan, respect FDB 5s/10MB limits).

## Implementation (only if feasible per above)

- Keep `domain <- application <- adapters` direction; no business logic duplicated per adapter; one storage txn commits state + outbox/job atomically.
- Update `docs/` contract in place (Draft/Frozen header), then code, then tests.
- Gate: `cargo test` green, `bash scripts/smoke-all.sh` green, plus live interop check vs real Akkoma/Pleroma if available (at minimum bench harness `features` poll+vote and reaction rows for pt-involved pairs).
- State maintenance per `AGENTS.md`: read proposal §9 + `docs/README.md` + latest `state/logs/` before starting; append one dated log to `state/logs/YYYY-MM-DD-<topic>.md` when finishing; never rewrite past logs.

## Deliverables

- Decision (feasible / not) for polls + reactions with code evidence (file:line on both sides).
- If feasible: code + tests + `notes/reactions` endpoint + contract updates.
- Bench-style verification output pasted or referenced in the state log.

## Non-goals

- Do not fix Akkoma/Misskey/Catodon themselves. Do not change `DELETE`-of-reblog, `renoteCount`, or Catodon fragment handling — those live upstream (see bug_reports).
