# 2026-10-15 — Phase 2.6 Slice 3: Inbox URL validation (H5)

Scope: `docs/phase2-6.md` Slice 3 (lines 49–52). `sharedInbox` was only
trimmed, never validated; the personal-inbox check compared host only, so
an http inbox for an https actor passed and delivery could be steered to a
third-party host.

## What changed

- `src/activitypub/actor.rs` (`parse_remote_actor` only; publicKey /
  key-id / owner logic from Slice 2 untouched)
  - `endpoints.sharedInbox` is now parsed with the same `parse_url_host`
    parser: it must parse, be same-host as the actor id, be ≤512 chars
    (matching the `Account::validate` `shared_inbox` bound), and must not
    be http when the actor id is https (downgrade refused). Any failure
    drops it to `None` — the actor still parses and delivery falls back to
    the personal inbox (`preferred_inbox` in `federation/outbox.rs`
    already prefers `shared_inbox` when non-empty, else `inbox`), so
    Misskey/Sharkey actors without a usable sharedInbox still federate.
  - Personal `inbox`: same scheme rule (https actor + http inbox rejects
    the actor with a `Validation`-mapped `Err`, since inbox is required)
    plus fail-fast rejection when >512 chars (the existing host-equality
    check is kept). Plain-http actors (local test rigs) still pass.
- `src/federation/delivery.rs` (`upsert_remote_actor_from_doc` only;
  Slice 1 curl/chunked and Slice 2 key-id code untouched)
  - After the account is built (both the update and create branches),
    `account.validate()?` runs before the public-key cache write, so
    overlong/illegal inbox/URI/username values from the document are
    refused even if parsing ever let them through. (`PortError` is an
    alias for `DomainError`, so `?` preserves the `Validation` variant.)
    The create branch already passes `parsed.shared_inbox` through, so the
    `None` fallback lands in `account.shared_inbox` as stored.
- Tests: all in one place — new `tests` module in
  `src/activitypub/actor.rs` (7 tests), following the existing
  `test_services` (redb tempdir) pattern from `federation/inbox.rs`:
  - `shared_inbox_on_other_host_falls_back_to_personal_inbox`
  - `https_actor_http_shared_inbox_downgrade_dropped`
  - `unparseable_shared_inbox_dropped`
  - `https_actor_http_personal_inbox_rejected`
  - `overlong_inbox_urls_rejected_or_dropped` (>512 same-host fixtures)
  - `http_actor_with_http_inboxes_accepted` (http guard)
  - `upsert_applies_inbox_validation_and_shared_inbox_fallback`
    (upsert level: third-party/downgraded/overlong sharedInbox → Ok with
    `shared_inbox == None` and personal inbox kept; https+http inbox →
    Err; domain-invalid `preferredUsername` → Err with no account row
    left behind)

## Verification — EXECUTED by caller 2026-10-15

- Full `cargo test` green: lib 129 passed (122 post-Slice-2 + 7 new
  `activitypub::actor::tests`), client_smoke 1, conformance 36,
  crash_recovery 2, federation 2, federation_interop 1, minrepro 1
  (172 total, 0 failed).
- `scripts/smoke-phase2.sh` not run (needs external Misskey/Sharkey
  servers; Slice 13 gate owns that evidence).
- Fail-first analysis (by inspection, for the caller to confirm):
  - The 5 parse tests asserting `None`/`Err` fail pre-fix (old code kept
    the raw string / accepted the downgrade); they pass post-fix.
  - The upsert test fails pre-fix on the first assertion (third-party
    sharedInbox was kept and stored); post-fix all phases pass. Note the
    overlong-sharedInbox phase inverts: pre-fix the upsert *errored* (the
    redb store's own `validate()` rejects >512), post-fix it succeeds
    with `None` — still a pre-fix failure, as required.
  - Guards passing both pre- and post-fix: `http_actor_..._accepted`,
    and the domain-invalid-username phase (the redb store already
    validated on create; the new explicit `validate()` is defense in
    depth and also covers the update branch, where parsed inbox/URI
    values are otherwise not re-checked).
- Regression risk reviewed (no execution): existing `parse_remote_actor`
  fixtures stay green — Slice 2 `inbox.rs` key tests use `{id}/inbox`
  same-scheme inboxes with no `endpoints`; the `objects.rs` render/parse
  roundtrip uses same-host same-scheme URLs from `render_actor_document`;
  live federation docs from pinetree/Misskey/Sharkey use same-host
  same-scheme inboxes. No `shared_inbox` unwraps exist outside tests.

## Decisions

- Drop-to-`None` (not reject) for sharedInbox, per the slice: the
  fallback is load-bearing for interop.
- No `UpdateAccountPatch` extension: the update branch keeps stored
  inbox/sharedInbox values (patch has no such fields); extending ports +
  stores + conformance is out of scope. Only the create branch persists
  fresh inbox URLs, and both branches now run `validate()`.
- `account.validate()?` rather than manual `DomainError → Validation`
  mapping: the types are aliases, so `?` is the mapping; noted in a code
  comment.

## Open questions

- None for this slice. Slice 13 owns the `phase2-6.md` checklist and
  gate evidence.
