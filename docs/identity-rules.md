# Identity rules

**Status:** Frozen (Phase 0 freeze review 2026-10-14)
**Sources:** `sns-server-proposal.md` §§3, 2, 7, 8; Misskey checkout (`misskey/`), Sharkey checkout (`sharkey/`), both read-only.

Reference paths below are relative to the checkout roots. Misskey and Sharkey agree on all identity shapes; where line numbers differ both are cited (M = misskey, S = sharkey).

## 1. Account identity

Every local account has exactly one of each, assigned at creation and never reused:

- **Account ID**: globally unique within the logical instance, immutable for the life of the account. Survives renames and node moves. Preserved across export/import migration (proposal §2).
- **Handle**: globally unique local name, `@{name}@{domain}`. Mutable via rename; **old handles are not reusable** (tombstoned, §2).
- **Actor URL**: canonical ActivityPub actor id, `https://{domain}/users/{account_id}` — ID-based, never name-based, so renames and moves preserve it (proposal §3).

Normative: public actor, note, inbox, and media URLs must not expose physical node hostnames. Internal hosts are never valid in public IDs or URLs.

## 2. ID scheme — fixed as `aidx`

Misskey/Sharkey support six configurable ID methods behind one dispatcher (`IdService`, M `packages/backend/src/core/IdService.ts:19-59`, S same path `:26-72`); every shipped config defaults to **`aidx`** (M `.config/example.yml:296`, S `.config/example.yml:286`). The in-config comment warns: once started, never change the setting (M `.config/example.yml:286-296`).

| Method | Regexp | Length/charset | Time encoding |
|---|---|---|---|
| `aid` (legacy) | `/^[0-9a-z]{10}$/` (M `misc/id/aid.ts:12`, S `:13`) | 10, base36 lower | 8 chars ms since 2000-01-01 + 2-char counter |
| **`aidx` (adopted)** | `/^[0-9a-z]{16}$/` (M `misc/id/aidx.ts:14`, S `:15`) | 16, base36 lower | 8 time + 4 random node id + 4 counter |
| `meid` | `/^[0-9a-f]{24}$/` (M `misc/id/meid.ts:11`) | 24 hex lower | 12 hex time (+`0x8000…` sort offset) + 12 random |
| `meidg` | `/^g[0-9a-f]{23}$/` (M `misc/id/meidg.ts:13`) | 24 (`g` + 23 hex) | `g` + 11 hex unix-ms + 12 random |
| `ulid` | Crockford Base32, 26 chars (M `misc/id/ulid.ts:11`) | 26 upper | 10 time + 16 random (npm `ulid` lib) |
| `objectid` | `/^[0-9a-f]{24}$/` (M `misc/id/object-id.ts:11`) | 24 hex lower | 8 hex **seconds** + 16 random (coarsest) |

Fixed decisions for this server:

1. **Canonical ID format is `aidx`**: 16 chars, `[0-9a-z]{16}`, ms-resolution time prefix. Rationale: current default of both references; lexicographically time-sortable (client `sinceId`/`untilId` pagination depends on ordering); compact.
2. IDs are **opaque strings** to clients; the generic API validation shape is `/^[a-zA-Z0-9]+$/` (M `server/api/endpoint-base.ts:20`). Servers may parse embedded timestamps (both references expose `IdService.parse()`), but cross-server parsing is not a contract.
3. Stored as `varchar(32)` (S `models/util/id.ts:6-9`); our schema reserves the same width.
4. The method is fixed at instance creation and **never changed afterwards** (per the reference warning).

## 3. Handles (usernames)

- **Local charset/length**: `^\w{1,20}$`, i.e. `[A-Za-z0-9_]{1,20}` (M `models/User.ts:319`, S `models/User.ts:439`; enforced at signup M `core/SignupService.ts:63-65`, S `:66-68`).
- **Case-insensitive, case-preserving**: `username` + `usernameLower` columns (`varchar(128)` each, M `models/User.ts:39-50`); uniqueness is `UNIQUE(usernameLower, host)` (M `models/User.ts:11`, S `models/User.ts:14`). All lookups lowercase first (M `WellKnownServerService.ts:141`, `ClientServerService.ts:455,626`).
- **Uniqueness scope**: `(usernameLower, host)` — local names unique among `host IS NULL` (M `SignupService.ts:82,124-127`, S `:85,122-127`); remote names keyed per host by the same index.
- **No reuse**: deleted names persist in a `UsedUsername` tombstone table and block re-registration (M `SignupService.ts:87,151-154`, `models/UsedUsername.ts:9-18`; S `SignupService.ts:90-92`).
- **Reserved names**: instance `preservedUsernames` + prohibited-words lists also block signup (M `SignupService.ts:91-100`).
- **Remote (federated) names are looser**: 1–128 chars matching `/^\w([\w-.]*\w)?$/` (allows interior `.`/`-`; M `core/activitypub/models/ApPersonService.ts:183-184`, S `:218`). Stored in the same `username`/`usernameLower` columns (M `:381-382`, S `:437-439`).
- Usernames map to account IDs; they are *not* actor identities. Rename changes only the handle→ID mapping; actor URL, signing identity, content, relationships, and federation state are unchanged.

## 4. Public URL shapes

Base: instance public origin (`config.url`); no node hostnames anywhere.

**Actor** — canonical URI built as `{url}/users/{id}` (M `core/entities/UserEntityService.ts:401-403`, S `:418-420`; verified). Person document (M `core/activitypub/ApRendererService.ts:498-556`, S `:580-620`):

| Field | Shape |
|---|---|
| `id` | `{url}/users/{id}` |
| `inbox` / `outbox` | `{actor}/inbox`, `{actor}/outbox` |
| `followers` / `following` | `{actor}/followers`, `{actor}/following` |
| `featured` | `{actor}/collections/featured` |
| shared inbox | `{url}/inbox` (also under `endpoints.sharedInbox`) |
| human `url` | `{url}/@{username}` (not an identity) |
| `preferredUsername` | bare username |
| `publicKey` / signing `keyId` | `{actor}/publickey`, `{actor}#main-key` (key render M `:299`, S `:321`; keyId M `:697`, S `:809`) |

System accounts (username containing `.`) render `type: Application`, bots `Service`, else `Person` (M `:500`, S `:582`) — passthrough detail, not identity.

**Serving** (M `server/ActivityPubServerService.ts`, S same file): `POST /inbox` + `POST /users/:user/inbox`; `GET /users/:user` (AP content-negotiation), `/outbox`, `/followers`, `/following`, `/collections/featured`, `/publickey`. `:user` resolves **by ID** (M `:767-783`, S `:998-1003`); `GET /@:acct` resolves by `usernameLower`+host (M `:785-804`, S `:1008-1022`). HTML `GET /users/:user` looks up by ID then **302 redirects to `/@{username}`** (M `server/web/ClientServerService.ts:562-577`, S `:575-590`).

**Notes** — local rows store `uri=NULL, url=NULL` (M `models/Note.ts:138-149`); canonical URI is built as `{url}/notes/{id}` (M `ApRendererService.ts:472`, S `core/entities/NoteEntityService.ts:836-838`; verified). Activity wrapper `{note-uri}/activity`, replies collection `{note-uri}/replies` (M `:113,145`, S `:124,156,876-912`). Remote references fall back to `{url}/notes/{id}` when no `uri` is stored (M `:317,381,396,400`).

**WebFinger** (M `server/WellKnownServerService.ts:119-198`, S `:131-175`): accepts `{url}/users/{id}`, `{url}/@{name}`, `acct:` or bare names; `self` link is the canonical actor URI, `profile-page` is `/@{username}`, plus a `subscribe` template.

**Media**: bytes live outside the DB in all cases (proposal §5); exact media URL paths deferred to the media contract (not an identity rule).

## 5. Ownership and directory (multi-node)

- In partitioned (multi-node) mode each account has exactly one authoritative write owner node at any time (proposal §3).
- The cluster-wide account directory maps account ID and normalized handle → current ownership. Handle uniqueness is enforced here.
- Ownership changes require an explicit migration/recovery procedure; nodes must never independently guess ownership.
- Single-node mode: the sole node owns all accounts; the directory interface still applies so behavior is identical.

(Full routing protocol and internal auth are a separate later contract; the principle — authenticate, resolve owner, execute at owner, respond via public domain — is fixed by proposal §3 and restated here.)

## 6. Migration and preservation

Changing deployment modes (redb ↔ FoundationDB) uses explicit export/import with downtime (proposal §2, not a live upgrade). Preserved byte-identically: account IDs, account keys/signing identity, handle→ID mappings (including `UsedUsername` tombstones), relationships, notes, and activity-deduplication records.

## 7. Compatibility notes (reference behavior adopted)

- Both references default to `aidx`; `aid` is legacy. We fix `aidx` only and do not implement the method switch.
- Sharkey extras treated as **out of scope** for identity: redacted-person authorized-fetch variant, `_misskey_*` privacy fields, `alsoKnownAs`/`movedTo` passthrough (S `ApRendererService.ts:624-626,645-651,669-698`).
- Misskey quote metadata / polls / `_misskey_content` are deferred per proposal §7 and unaffected by this contract.

## Superseding note — Phase 2.6 slice 11 (2026-10-15)

The collection endpoints in §4 (`/outbox`, `/followers`, `/following`) are now paged with opaque cursors and `next` links, and their `totalItems` rules changed (outbox `totalItems` omitted; followers/following from counters). The full contract is `docs/activitypub-collections.md`. The URL shapes in §4 are unchanged.

## Addendum 2026-10-20 (freshness pass)

The contract text above is unchanged. The Status line above stays `Frozen`. Verified against `src/cluster/*`, `src/persistence/redb/mod.rs`, and `src/api/misskey/endpoints/mod.rs`.

### Status (2026-10-20 freshness pass)

**OPEN (Phase 4, no code):**
- §5 directory and routing: `src/cluster/routing.rs` is 0 lines. `src/cluster/account_directory.rs` and `src/cluster/internal_protocol.rs` are also empty (the `src/cluster` grep matched only `mod.rs`). In redb, `get_owner` (`src/persistence/redb/mod.rs:3970-3981`) returns the local node id for any existing account and `NotFound` otherwise. `set_owner` (`:3983`) has no cluster validation. The internal routing protocol and internal auth still need their own contract (deferred at §5, line 82).
- Exact media URL paths: still deferred to a media contract (§4, line 73). The code has grown beyond that: `/files/{id}`, `/files/{id}/{name}`, `/identicon/{id}`, `/proxy`, and `/proxy/{*rest}` (`src/api/misskey/endpoints/mod.rs:149-164`). The contract has not caught up.

**Routes in code, not in §4 (additive, no shape changes):**
- `/users/{id}/stamps/{stamp}` (quote authorization stamp, `endpoints/mod.rs:137`).
- `/clips/{id}`, `/users/{id}/clips`, `/pages/{id}` (`endpoints/mod.rs:141-143`; contract in `federated-collections.md`).
