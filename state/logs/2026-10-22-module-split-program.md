# 2026-10-22 — module-split program (docs/decomposition.md), complete

Goal: execute the full decomposition program — every `src/**` leaf ≤800
non-blank lines, no integration test file >1,500, parents re-export only.
Behavior, public paths, JSON shapes, table names, test names unchanged.

## How it ran

Six parallel split agents with disjoint file sets (Wave 1: redb+ports,
endpoints+packers/errors/response, federation, activitypub, media/jobs/streaming,
queries/commands/domain/config/main). No agent edited `src/lib.rs`, `docs/*`,
`state/*`, `Cargo.toml`, or another family's `mod.rs`. No `cargo` inside
agents (parent ran all gates after parallels landed — avoids lock contention).
Native Windows toolchain only; WSL never used. `cargo fmt` on touched paths;
final `cargo fmt --all` at the end (exit 0).

Follow-up waves (all disjoint, same rules): 4 import-fix agents → 1
dispatch-arm restore agent → 2 lib-test-import agents → 5 over-cap/test-split
agents (conformance, federation, redb children, endpoints children,
streaming/packers-tests) → 2 final agents (federation 800+ files,
gaps_final_wiring) → parent-applied mechanical fixes (derive/use hoist,
pub(crate) widening x75+13, missing imports).

## Defects the splits introduced (all found and fixed)

- Hoisted `use` lines split `#[derive]` from structs (16 files) — scripted fix.
- Cross-file private items (~830 errors): widened to `pub(crate)` at
  definition sites only. Cascading `[u8]`/`str` inference errors vanished.
- Dropped all 13 combined `"a" | "b" =>` dispatch arms (28 endpoints 404'd);
  restored byte-identical from HEAD + 10 failing tests back to green.
- Splitter mangled 2 signatures (dropped arg/param) — restored from HEAD.
- `tests/conformance` crash harness passed stale flat test names to `--exact`
  (9 tests) — prefixed with module paths. `tests/ingress_limits_g1`
  `include_str!` paths updated to new homes (response/upload_slots.rs,
  endpoints/files_http.rs, inbox/mod.rs + children, actor/webfinger children).
- `cargo fix` (run once for unused imports) deleted load-bearing `pub(crate)`
  re-exports and cfg(test)-only imports in 11 files — repaired by hand.
  Lesson: do not run `cargo fix` on this tree; repair from diagnostics.
  Warnings 1364 → ~50.
- Transient infra: C: hit 0 bytes free mid-program (query-cache write
  failures masquerading as compile errors); recovered to 159 GB free.

## Gates (native `cargo`, all `--offline`)

- `cargo test --lib`: 626 passed, 0 failed.
- `cargo test --test conformance --test crash_recovery`: 119 + 2 pass.
- API gates (`notes_like_contract`, `gaps_api_admin/auth/drive/gallery`): 33 pass.
- Per-target loop over all 40 `tests/*.rs`: every target exit 0.
- Out of scope by decomposition rule (bash-only scripts): `client_smoke`
  (`scripts/smoke-phase1.sh`) and `client_profile` (`scripts/smoke-phase3.sh`).
- `cargo check --lib --offline --tests`: 0 errors. `cargo fmt --all`: clean.

## Open items (documented exceptions, not silent)

- `src/api/misskey/endpoints/mod.rs` (1,338): thin match only, no handlers.
  Splitting further needs a new routing layer (new logic) — left as the single
  over-cap production file.
- `src/persistence/redb/note_store_read.rs` (1,005): single atomic
  `impl NoteStore` trait block; trait-level split is ports-owned.
- `src/activitypub/quotes.rs` (was 817 incl. tests): extracted `quotes/tests.rs`
  post-wave; now 477.

## Final table (non-blank lines; every production leaf ≤800 except the two above; every test file ≤1500)

Former `persistence/redb/mod.rs` (12,591) → 31 files, largest `note_store_read` 1005*.
Former `application/ports.rs` (3,243) → 29 files, all ≤800.
Former `endpoints/mod.rs` (25,485) → mod 1338* + 15 dispatch/parse/auth/middleware
files split once more into per-family children (all ≤800) + 11 test files
(largest `tests_users` 1425, `tests_batch_pq` 1395 — under 1500).
`packers.rs` (4,326) → 25 files + `tests_*` (largest `tests_support` 1227).
`errors.rs` (2,170) → 13 files. `response.rs` (905) → 3 files.
`inbox.rs` (4,699) → 10 + `follow/` (6) + `create/` (2) children, largest 577.
`outbox.rs` (2,004) → 7 + `dispatch/` (9) children, largest 487.
`delivery.rs` (1,756) → 6 + `client/` (4) children, largest 390.
`objects.rs` (2,539) → 9 children, largest 554. `signatures.rs` (2,435) → 7,
largest 752. `webfinger.rs` (1,911) → 7, largest 625. `actor.rs` (1,745) → 4
(+`collections` 757 max). `federated_collections.rs` (1,236) → 4, largest 463.
`reconciler.rs` (1,283) → 5, largest 444. `transcode.rs` (1,269) → 4+3,
largest 413. `remote_cache.rs` (1,033) → 4, largest 532. `worker.rs`
(1,231) → 18, largest 425. `server.rs` (streaming, 1,649) → 3 + `connection/`
(4), largest 714. `queries.rs` (2,521) → 7, largest 617. `exports.rs`
(1,390) → 12, largest 344. `config.rs` (1,334) → 704+795. `main.rs` (885) →
531+171+264. `account.rs` (1,086) → 699+tests. `note.rs` (837) → 579+tests.
`tests/conformance.rs` (7,110→7,948 incl. agent-added headers) → shim + 24
files (largest `note_replies` 1,292). `tests/federation.rs` (2,589) → shim + 5
(largest `end_to_end` 1,029). `federation_sig_mode_e5.rs` (1,677) → shim + 6
(largest `rejection` 858). `gaps_final_wiring.rs` (1,616) → shim + 19
(largest `admin` 302).

## Doc updates (same slice, per decomposition deliverables)

- docs/application-ports.md, docs/activitypub-collections.md: dated 2026-10-22 addenda with new file paths (module paths unchanged). Frozen text untouched. Cargo.toml/Cargo.lock untouched (no dependency changes).
