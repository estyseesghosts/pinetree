# 2026-10-15 — Phase 2.6 Slice 2: Key ownership (C1)

Scope: `docs/phase2-6.md` Slice 2 (lines 43–47). Highest-severity item:
public-key cache poisoning allowed full impersonation (`publicKey.id`
never tied to the actor, blind overwrite by key id, signer derived from
the keyId string).

## What changed

- `src/activitypub/actor.rs`
  - New `key_id_belongs_to_actor()`: `publicKey.id` must equal the actor id,
    or be `{actor}#fragment` (non-empty) or `{actor}/sub-path`
    (non-empty, not `/?…`/`/#…`), and in all cases be on the same origin
    (scheme + host via `parse_url_host`) as the actor id. `parse_remote_actor`
    rejects anything else. The existing `owner == actor id` check is kept.
    Honest shapes (`{actor}#main-key`, `{actor}/publickey`, bare `{actor}`)
    still parse, with or without `owner`.
- `src/application/commands/keys.rs` (`cache_remote_public_key`)
  - Overwrite guard: re-caching the same `(key_id, actor_uri)` pair still
    overwrites (key rotation keeps working), but caching an already-bound
    `key_id` under a *different* `actor_uri` fails with
    `PortError::Conflict` — never a silent overwrite. Decision: `Conflict`
    (not `Validation`) because the document itself is well-formed; it
    collides with established cache state. This is the single choke point:
    `keys.rs` is the only caller of `AuthStore::save_remote_public_key`,
    so no backend change was needed (redb save path untouched; FDB stub
    has no key store to guard).
- `src/federation/delivery.rs` (upsert + resolve only; Slice 1
  curl/chunked code untouched)
  - `upsert_remote_actor_from_doc`: dropped the speculative
    `{actor}/publickey` alias row. Only the advertised `publicKey.id` is
    cached (one row for the honest flow). Alias policy: store it only when
    it IS the real key id, which falls out naturally — no second write.
  - `resolve_remote_public_key`: after fetch-and-upsert, verifies
    `parsed.public_key_id == requested key_id`, else `PortError::Validation`.
    Returns `(owner_actor_uri, pem)` as before; doc comment updated to say
    the URI is the key owner.
- `src/federation/inbox.rs` (`authenticate` + caller)
  - `authenticate` now returns the key owner's actor URI (cached
    `actor_uri`, or the fresh fetch's `parsed.uri` after the refetch —
    parse checks re-applied by the fetch-and-upsert path plus the new
    resolve key-id equality check). `force_refresh` behavior unchanged.
  - `process_inbox` compares `activity.actor` to that owner; the
    `actor_uri_from_key_id` string-strip is gone from the authority path.
- `src/activitypub/signatures.rs`
  - `actor_uri_from_key_id` kept for compat (still the fetch locator in
    `resolve_remote_public_key`); doc comment added warning it must never
    be used as authority.
- Tests: all in one place — new `key_ownership_tests` module in
  `src/federation/inbox.rs` (in-crate, so it can drive the private
  `process_inbox` end to end with generated RSA-2048 keypairs). 11 tests:
  honest `#main-key`/`/publickey`/bare parse; cross-origin key without
  owner; cross-origin key with `owner == attacker`; owner-mismatch and
  victim-owner variants; poison upsert rejected + victim row kept;
  direct overwrite refused with `Conflict`; honest upserts cache exactly
  one row (both `#main-key` and `/publickey` real ids, no alias row);
  resolve returns owner and rejects a fetch whose advertised key id
  differs (loopback TCP fixture); forged victim `Follow` (victim keyId,
  attacker signature) → `401`; honest signed `Follow` → `202`.

## Verification (caller-executed 2026-10-15)

- `cargo test --lib` green: 122 passed, 0 failed (11 new
  `key_ownership_tests` included).
- Full `cargo test` green: lib 122 + client_smoke 1 + conformance 36
  + crash_recovery 2 + federation 2 + federation_interop 1 + minrepro 1
  (165 total, 0 failed).
- Follow-up fix by caller: `cache_remote_public_key` now trims the PEM
  before storing (canonical form — `parse_remote_actor` already trims, so
  direct-cache and parse paths compared differently by a trailing
  newline). Three upsert/resolve tests updated to compare against
  `victim_pub.trim()`; all green after.

## Decisions

- `Conflict` vs `Validation` for the overwrite guard: `Conflict`, since
  the incoming document is well-formed but collides with bound cache
  state; documented on `cache_remote_public_key`.
- Alias policy: no speculative alias rows at all; a non-advertised key-id
  shape resolves via refetch rather than cache.
- `owner` semantics unchanged (must equal actor id exactly after trim
  when present); the key-id binding is the new primary check and also
  covers the `owner`-absent and `owner == self` attacker cases.

## Open questions

- None for this slice. Note: caches poisoned before this fix stay
  poisoned until the rows are refetched — no migration was in scope.
