# 2026-10-23 Wave 5 — JSON-config compliance + integrity flag (parent)

Parent work (`src/config.rs` is parent-owned). The negative-cache agent
work landed in `44c19b8`.

## Changed
- `src/config.rs` — `FederationSection` gains `allow_private_ips`
  (default `false`), `federation_extra_ca: Option<PathBuf>` (default
  `None`), `debug_federation: bool` (default `false`),
  `reply_repair_limit: Option<usize>` (default `None`); top-level
  `storage_integrity_check_at_open: bool` (default `false`, LOW-6).
  `validate()` enforces: empty CA path rejected, debug toggle rejected in
  release builds (`!cfg!(debug_assertions)`), reply bound 1..=5000.
  Webhook delivery ignores `allow_private_ips` (always strict) — stated
  in the field docs.
- `src/federation/delivery/client/config.rs` — `SafeHttpConfig::default()`
  no longer reads `PINETREE_ALLOW_PRIVATE_IPS` (always `false`); the
  serve path wires `cfg.federation.allow_private_ips` in `runtime.rs`.
- `src/main.rs` — deprecated env shims (`PINETREE_ALLOW_PRIVATE_IPS`,
  `PINETREE_DEBUG_FEDERATION`, `PINETREE_REPLY_REPAIR_LIMIT`) land in the
  merged config before `validate()` with a deprecation warning, losing to
  explicit CLI flags; new CLI `--allow-private-ips[=bool]`,
  `--reply-repair-limit=N`, `--storage-integrity-check-at-open`
  (plus existing `--federation-extra-ca`, `--debug-federation` now writing
  the same fields); reply repair reads the merged field; extra CA gates
  startup (missing/unreadable file → exit 1, permanent; SHA-256
  fingerprint logged; platform trust store never replaced).
- `src/runtime.rs` — wires `allow_private_ips` into the federation
  client; LOW-6 integrity check before `RedbStore::open` (same
  `check_integrity` the restore path uses; failure refuses boot).
- `src/config/tests.rs` — `federation_wave5_keys_round_trip_typo_and_guards`
  (defaults, round-trip, both-level typo rejection, guard failures,
  debug-tracks-build-profile).
- `src/persistence/foundationdb/mod.rs`, `src/cluster/*` (LOW-5) —
  Phase 4 placeholder module docs (nothing imports them; `Cluster` mode
  still rejected). Kept as docs, not deletion, so `lib.rs` is untouched.
- `docs/wiki/Administration.md` — new keys in the JSON schema, rules, and
  the former env-only exceptions line.

## Verified (parent, native cargo, shared target)
- `--lib -- config::` → 30 passed (incl. the new test).
- `cargo check --locked --offline --bins` → exit 0, no errors.
- `cargo check --locked --offline --all-targets` → exit 0, no errors.
- `rustfmt --edition 2024 --check` on all touched Rust files → clean.
