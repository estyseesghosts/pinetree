# Visibility policy

**Status:** Frozen (Phase 0 freeze review 2026-10-14)
**Sources:** `sns-server-proposal.md` §§4–5; Misskey checkout (read-only): `NoteCreateService.ts:465-571`, `NoteEntityService.ts:277-328`, `stream/channel.ts:68-103`.

This is the single definition of who may see a note. The Misskey API adapters, streaming channels, timeline queries, and federation delivery all enforce exactly this — no per-adapter variants (proposal §4). Misskey keeps three synchronized implementations of the read predicate (`NoteEntityService`, stream `Channel`, `QueryService.generateVisibilityQuery`); we keep one predicate with three call sites, tested for agreement in the conformance suite.

## 1. Visibility levels

Ordered least → most restrictive: `public` < `home` < `followers` < `specified`.

| Level | Meaning |
|---|---|
| `public` | Visible to everyone including unauthenticated readers; eligible for federated delivery and public timelines |
| `home` | Visible to everyone, but excluded from federated/public timelines (home-timeline-only remotely) |
| `followers` | Author, author's followers, mentioned users, and the author of the replied-to note |
| `specified` | Author + explicitly listed users only (`visible_user_ids`) |

`local_only` is orthogonal (see §4), not a fifth level. Channels do not exist in this server; channel-forced rules from the reference are not adopted.

## 2. Creation-time narrowing

Defaults at creation: `visibility = public`, `local_only = false` (M `NoteCreateService.ts:466-467`).

The server may only narrow, never widen. Renote target rules (M `:497-528`):

| Target visibility | Rule |
|---|---|
| `public` | always allowed |
| `home` | allowed; forces result to `home` or narrower |
| `followers` | allowed only for own notes; forces result to `followers` or narrower; others' → reject |
| `specified` | reject |

Reply target rules (M `:542-565`): same shape, except replying to a `specified` note forces (not rejects) `specified` on the reply. Renote/reply to a `local_only` note propagates `local_only = true` (M `:525-527,568-570`).

Instance-policy downgrades (M `:472-495`): a `public` note is forced to `home` when the text/CW matches configured sensitive words or the author lacks public-post permission. The trigger set is instance configuration; the narrowing direction is fixed.

Blocking: pure renote (non-quote) of a local user who blocks the author is rejected (M `:531-540`).

## 3. Read-time predicate `is_visible(note, viewer) -> bool`

Single definition, three call sites (API packing, streaming fan-out, timeline queries). Grounded in M `NoteEntityService.ts:280-327` + `stream/channel.ts:73-100`:

- `specified`: viewer null → false. Author → true. Otherwise viewer ∈ `visible_user_ids`.
- `followers`: viewer null → false. Author → true. Viewer is the replied-to note's author → true. Viewer ∈ mentions → true. Otherwise viewer follows the author. Remote-remote fallback: when neither side's follow graph is knowable (both remote), assume visible (M `:316-323`) — required so federated followers-timeline traffic doesn't vanish.
- `public`, `home`: always true at this layer. (`public` vs `home` differs only in timeline eligibility and federation reach, §5.)

Deleted/tombstoned notes are invisible to all non-author viewers regardless of level (tombstone rule; enforcement in `NoteStore` reads).

## 4. `local_only` semantics

- Never delivered over federation, never rendered to remote viewers, never appears on federated/public timelines.
- Visible on local timelines subject to the §3 predicate.
- Propagates through renote/reply (§2). Settable only at creation; immutable afterwards.

## 5. Timeline and delivery eligibility

| Level | Local timelines | Federated/public timelines | AP delivery to remotes |
|---|---|---|---|
| `public` | yes | yes | yes |
| `home` | yes | no (home-timeline-only) | followers + mentioned + replied-to scope |
| `followers` | subject to §3 | no | followers + mentioned + replied-to scope |
| `specified` | author + listed only | no | listed users only |

## 6. Unblocks `specified` in `create_note`

The ports contract (`docs/application-ports.md` §5) deferred `visible_users` to this document. Decision: `specified` is fully supported. `create_note` with `visibility = specified` requires a non-empty `visible_user_ids` (all resolvable accounts, local or known remote); otherwise `Validation`. Replies to `specified` notes inherit per §2. The application-ports open decision #3 is hereby resolved; `application-ports.md` should drop that row at freeze.
