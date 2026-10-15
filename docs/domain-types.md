# Core domain types

**Status:** Frozen (Phase 0 freeze review 2026-10-14)
**Sources:** `sns-server-proposal.md` §4; `docs/identity-rules.md`; `docs/visibility-policy.md`; Misskey checkout (read-only): `models/User.ts`, `models/Note.ts`, `models/Following.ts`, `models/NoteReaction.ts`, `models/DriveFile.ts`, `models/util/id.ts:6-9`, `types.ts:59-61` (all key claims re-verified locally this session).

Pure data + validation. No I/O, no HTTP, no per-adapter variants. Field bounds are client-compatibility surface: clients read these shapes, so narrowing a bound later is a breaking change — the values below are fixed at freeze.

Conventions: every entity ID is an opaque `aidx` string (`varchar(32)` in storage, M `models/util/id.ts`). No `createdAt` columns — time-ordering comes from ID prefixes. `null` vs absent follows the reference: absent means unknown/remote-irrelevant, never a second kind of empty.

## 1. Account

Must-carry subset of `MiUser` (M `models/User.ts`). Deferred to later contracts: roles/policies, `alsoKnownAs`/`movedTo` (out of scope per identity rules §7), chat, antennas, pages/clips/drive-housekeeping.

| Field | Type | Null | Rule |
|---|---|---|---|
| `id` | id | no | immutable PK |
| `username` / `username_lower` | string ≤128 | no | `^\w{1,20}$` local; unique `(username_lower, host)` (M `:11`) |
| `host` | string ≤128 | yes | null = local; remote = hostname |
| `display_name` | string ≤128 | yes | — |
| `description` | string ≤1500 | yes | profile bio (manifest §3) |
| `location` | string ≤50 | yes | free-form location (manifest §3) |
| `birthday` | string | yes | `YYYY-MM-DD` (manifest §3) |
| `uri` / `inbox` / `shared_inbox` | string ≤512 | yes | null when local; required when remote |
| `followers_count` / `following_count` / `notes_count` | integer | no | default 0; maintained in the mutating transaction (bounded single-row increments), never eventually-consistent for local accounts |
| `avatar_id` / `banner_id` | id (media) | yes | `SET NULL` semantics on media delete |
| `is_locked` / `is_bot` / `is_suspended` / `is_explorable` / `is_deleted` | bool | no | defaults false/false/false/true/false |
| `is_cat` | bool | no | default false; client-visible flag, carried for compat |

Remote-only denorms (`last_fetched_at`, remote token never stored) stay adapter-side, not domain.

## 2. Note

Must-carry subset of `MiNote` (M `models/Note.ts`). No channels (`channel_id` does not exist here); no polls (`has_poll` does not exist — proposal §7 defers polls; `notes/create` rejects `poll` per ports contract).

| Field | Type | Null | Rule |
|---|---|---|---|
| `id` | id | no | immutable PK |
| `author_id` | id | no | local account at creation |
| `text` | text | yes | API max 3000 chars (manifest §4; reference `MAX_NOTE_TEXT_LENGTH`, `const.ts:6`); storage carries full text, no silent truncation |
| `cw` | string ≤512 | yes | content warning (M `:75-78`) |
| `visibility` | enum | no | `public\|home\|followers\|specified` (M `types.ts:59`) |
| `visible_user_ids` | id[] | no | default empty; non-empty required when `specified` |
| `local_only` | bool | no | default false; immutable after creation |
| `reply_id` / `renote_id` | id | yes | indexed; narrowing rules in visibility policy §2 |
| `reply_user_id` (denorm) | id | yes | author of replied-to note; read predicate needs it without a join (M `:225-230`) |
| `file_ids` | id[] | no | default empty; each committed + owned by author |
| `mentions` | id[] | no | default empty; local mentioned users (read predicate + notify jobs) |
| `mentioned_remote_users` | JSON string | no | default `[]`; remote mention routing (delivery jobs) |
| `tags` | string[] | no | default empty; tag values ≤128 |
| `emojis` | string[] | no | default empty; custom-emoji names ≤128 (resolution deferred, names carried) |
| `reactions` | map string→int | no | default empty; denormalized counts |
| `renote_count` / `replies_count` | integer | no | default 0; same-TX maintenance as account counters |
| `reaction_acceptance` | enum \| null | yes | `likeOnly\|likeOnlyForRemote\|nonSensitiveOnly\|nonSensitiveOnlyForLocalLikeOnlyForRemote\|null` (M `types.ts:61`) |
| `uri` / `url` | string ≤512 | yes | null when local (M `:138-149`); canonical forms in identity rules §4 |
| `user_host` (denorm) | string ≤128 | yes | author's host at creation; federated filtering without join (M `:218-223`) |

## 3. Follow

One row per directed pair (M `models/Following.ts`):

| Field | Type | Null | Rule |
|---|---|---|---|
| `id` | id | no | immutable PK |
| `follower_id` / `followee_id` | id | no | unique pair `(follower_id, followee_id)` (M `:11`) |
| `with_replies` | bool | no | default false; include followee replies in follower's home timeline |
| `notify` | `normal` \| null | yes | notify follower of followee's new notes |

Host/inbox denorms from the reference stay adapter-side (delivery reads resolve them via account reads; reintroduce as measured, Phase 5).

## 4. Reaction

One reaction per user per note (M `models/NoteReaction.ts`):

| Field | Type | Null | Rule |
|---|---|---|---|
| `id` | id | no | immutable PK |
| `user_id` / `note_id` | id | no | unique pair `(user_id, note_id)` (M `:12`) |
| `reaction` | string ≤260 | no | unicode emoji or custom-emoji reference; opaque at this layer |

Rules: reacting requires the note visible to the reactor (M `ReactionService.ts:116` pattern); re-reacting replaces (upsert on the unique pair), never duplicates; `reactions` counts on the note update in the same transaction. `_misskey_reaction` AP metadata is carried on the federation path (proposal §7); legacy `_misskey_content` is never produced.

## 5. Media metadata

Must-carry subset of `MiDriveFile` (M `models/DriveFile.ts`). Bytes are never here (`BlobStore`, ports contract §9). Out of scope: folders, access keys, webpublic machinery, request forensics (`request_headers`, `request_ip`).

| Field | Type | Null | Rule |
|---|---|---|---|
| `id` | id | no | immutable PK |
| `owner_id` | id | yes | null = system/remote-cached |
| `name` | string ≤256 | no | original filename |
| `mime` | string ≤128 | no | detected content type (sniffed, not client-asserted) |
| `size_bytes` | integer | no | cap is instance configuration (reference enforces per-user capacity in `DriveService`, surfacing `MAX_FILE_SIZE_EXCEEDED`); recorded in the manifest at implementation |
| `hash` | string ≤128 | no | content hash (md5-compatible width); dedup + cache key |
| `blurhash` | string ≤128 | yes | placeholder for clients |
| `properties` | JSON | no | default `{}`; `{width?, height?, orientation?}` |
| `is_sensitive` | bool | no | default false |
| `is_link` | bool | no | default false; URL-only attachment, no stored bytes |
| `url` / `thumbnail_url` | string ≤1024/512 | — | served locations; remote-cache entries carry source `uri` instead |

## 6. Open decisions (must resolve before Frozen)

1. ~~Exact `text` max length and media `size_bytes` cap~~ — resolved: text API-max 3000 (manifest §4); media cap is instance config surfacing `MAX_FILE_SIZE_EXCEEDED`.
2. Custom-emoji model (names are carried; resolution/rollout deferred with polls-era compat work).
3. ~~Display-name/description/profile fields~~ — resolved: manifest §3 subset (`description` ≤1500, `location` ≤50, `birthday`, avatar/banner, flags) joins the Account type above; `fields[]`, decorations, mute/notification config deferred to Phase 3 with `UNSUPPORTED_FIELD`.

## Superseding note — Phase 2.6 slice 4 (2026-10-15)

Added `FollowRequest { id, follower_id, followee_id, with_replies, notify }` (decision D-a: a separate record mirroring Misskey's `FollowRequest`, not a state field on `Follow`). A request is **not** an edge: it never feeds `follows_author`, `followers_of` fan-out, counters or AP collections. `NotificationKind` gains `ReceiveFollowRequest` (wire type `receiveFollowRequest`). See `state/logs/2026-10-15-phase2-6-slice4-follow-requests.md`.

## Superseding note — Phase 2.6 slice 8 (2026-10-15)

Blob content address is SHA-256 (decision D6). `MediaMeta` gains `blob_key` (SHA-256 hex of the stored bytes, 1..=128 ASCII alnum like `hash`): it is the sole `BlobStore` address (`put`/`get`/`delete` all key on it) and never surfaces to clients. `hash` stays the MD5 with its value unchanged for the same bytes: it still feeds the packer `"md5"` and the per-owner `find_by_owner_and_hash` upload dedup (same bytes → same md5 → existing row; `force` still bypasses). `InsertMediaInput` mirrors both fields; both are computed on upload (local + remote paths, via the shared `media::ingest::{md5_hex, sha256_hex}`). `FsBlobStore` keeps the skip-on-existing-key `put` (safe under SHA-256) and gains an atomic length sidecar (`{object}.len`, tmp+rename alongside the bytes): `get` treats a missing/mismatched sidecar as corrupt (`Ok(None)` — callers already map `None` to 500/404) so a torn write can never serve short bytes. The `BlobStore` trait is unchanged (sidecar is internal). **Data wipe (decision D-e): there are no production instances, so no migration is written — old MD5-keyed blob files (which have no sidecar and read as `None`) and pre-slice-8 `MediaMeta` rows (no `blob_key`) are orphaned/incompatible; deploy by wiping the blob directory and the media table.** See `state/logs/2026-10-15-phase2-6-slice8-content-addressing.md`.

## Superseding note — Phase 2.6 slice 7 (2026-10-15)

`MediaMeta` gains nullable `source_url` (remote origin URL for cache rows; `None` for local uploads). With the owning note's `file_ids` it is the idempotent-insert key that keeps inbox replays and job retries from orphaning rows. MIME rule tightened: the stored `mime` comes from magic-byte sniffing only (allowlist `image/png|jpeg|gif|webp|avif`, `video/mp4|webm|mpeg`, `audio/mpeg|ogg|wav`, `text/plain` for non-markup UTF-8 only, else `application/octet-stream`); declared `mediaType` and response `Content-Type` never influence it. Never `text/html`, `image/svg+xml`, `application/xhtml+xml`, `text/xml`. See `state/logs/2026-10-15-phase2-6-slice7-media-safety.md`.
