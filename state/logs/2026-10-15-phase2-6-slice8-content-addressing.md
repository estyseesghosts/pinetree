# Phase 2.6 Slice 8 — Content addressing (D6)

Date: 2026-10-15. Author: Beeline (Muse Spark 1.3 Contributor).
Status: implemented and verified — full `cargo test` green (see §Verification).

## What changed

Blob key is SHA-256 hex; MD5 stays as a separate, value-stable field:

- `src/media/ingest.rs` — new shared `sha256_hex` beside `md5_hex` (`sha2`/`hex` already dependencies; mirrors the existing hasher pattern). No new crates.
- `src/domain/media.rs` — `MediaMeta` gains `blob_key: String` (`#[serde(default)]` so pre-slice-8 rows decode instead of crashing reads; same 1..=128 ASCII-alnum validation as `hash`). `hash` keeps its meaning (MD5) and its validation untouched. `valid_media()` carries the empty-input vectors for both (md5 `d41d8…`, sha256 `e3b0c…`).
- `src/application/ports.rs` — `InsertMediaInput` gains required `blob_key`. No trait signature changed (`MediaStore` and `BlobStore` keep exact signatures; see the ports superseding note).
- `src/application/commands/media.rs` — `upload_media` computes both hashes per upload: md5 stays the `find_by_owner_and_hash` per-owner dedup key (same bytes → same md5 → existing row; `force` still bypasses) and the stored `hash`; `blobs.put` is keyed by the SHA-256 `blob_key`. The local `md5_hex` duplicate is deleted (imports from `media::ingest`).
- `src/media/remote_cache.rs` — `PreparedRemoteMedia` carries both hashes; `download_and_sniff` computes both; both blob-write sites (`fetch_and_attach_remote_media`, standalone `fetch_and_cache_remote_media`) address by `blob_key`; both `InsertMediaInput`s carry `hash` + `blob_key` + `source_url` as before. The standalone function is kept (slice-7 log left the fold-or-keep choice open; it now also serves as the remote-path regression vehicle).
- `src/media/storage.rs` — `FsBlobStore::put` keeps the skip-on-existing-key (safe under SHA-256) but now requires the length sidecar to match, otherwise it rewrites both files (heals torn writes / missing sidecars). New atomic length sidecar `{object}.len` (decimal length, tmp+rename, internal `sidecar_path` + `write_atomic` helpers). `get` reads the sidecar first: missing/unparsable sidecar or length mismatch → `Ok(None)` (callers already map `None` to 500/404; documented choice — never serve short bytes). `delete` removes both files. `BlobStore` trait unchanged.
- `src/persistence/redb/mod.rs` — both `MediaMeta` constructors (`attach_remote_media`, `insert_metadata`) carry `blob_key`; `find_by_owner_and_hash` still compares the md5 (comment added).
- `src/api/misskey/endpoints/mod.rs` — `serve_file` reads `blobs.get(&meta.blob_key)`.
- `src/api/misskey/packers.rs` — packer untouched (`"md5": media.hash`); `test_media` gains a `blob_key`.
- Slices 1–7 untouched: body caps, upload slots, sniff-only MIME, serve headers, `RemoteMediaFetch` job, follow requests, `NO_SUCH_NOTE` replies — no changes to those paths (verified by grep: no edits outside media/blob call sites).

## Tests (written first, in edit order)

- `media::storage` — `blob_store_get_verifies_length_sidecar` (put→get; truncate object→`None`; re-put heals; corrupt sidecar→`None`; missing sidecar→`None`). Pre-existing `blob_store_put_get_delete_and_partial_write_invisible` and `blob_store_rejects_traversal_hashes` updated only by adjacency (traversal test header restored after an edit slip — reviewer: confirm its body is intact).
- `domain::media` — `blob_key_rejects_empty_and_binary` (same bounds as md5).
- `application::commands::tests` — `slice8_distinct_uploads_never_share_blob_storage` (a), `slice8_owner_dedup_still_works_and_force_bypasses` (b), `slice8_empty_upload_pins_known_md5_and_sha256_vectors` (c; absolute pin via the empty-input vectors both fixtures already carry — the `5eb63…` constant from the task brief was deliberately NOT trusted or used).
- `api::misskey::packers` — `slice8_pack_drive_file_md5_stable_for_same_bytes` (c; `md5` equals the pre-slice value, no `blob_key`/`sha256` leak into the packed shape).
- `media::remote_cache` — `remote_media_ssrf_and_fetch_cache_roundtrip` extended in place: asserts `meta.hash == md5_hex`, `meta.blob_key == sha256_hex`, blob under `blob_key`.
- `tests/conformance.rs` — `case_m3_media_blob_key_roundtrip` / `m3_media_blob_key_roundtrip` (e; real digests via `pinetree::media::ingest`, distinct pairs coexist, md5 lookup resolves each row, unknown md5 → `None`). `valid_media_input` gains a `blob_key`. FDB inherits.
- Updated call sites: `tests/federation.rs` (2× `blobs.get(&meta.blob_key)`), `tests/crash_recovery.rs` (manifest carries `media_blob_key`; parent asserts both hashes and reads the blob by `blob_key`).

## Numbers picked

Sidecar format: decimal ASCII length in `{object}.len`. `get` corruption → `Ok(None)` (not an error) so torn writes degrade to 500/404 exactly like missing blobs, and the next upload/worker retry heals via `put`.

## Open questions

- D-e wipe is documented, not coded: deploy by wiping the blob dir + media table (see note below). Pre-slice-8 blobs read as `None` (no sidecar) rather than erroring — deliberate.
- The standalone `fetch_and_cache_remote_media` still has no production caller; kept for its roundtrip test. A later slice may fold it.
- Reviewer fix (no shell; found by reading): the first `put` draft skipped whenever the object file merely existed and the sidecar matched, so a torn object with an intact sidecar would never heal and the new sidecar test's re-put assertion would fail. The skip now also stats the object length (`fs::metadata`, no content read) — `put` rewrites whenever object length, sidecar content, or sidecar presence disagrees.

## D-e wipe note (also in the `docs/domain-types.md` slice-8 superseding note)

There are no production instances, so no migration is written. Old MD5-keyed blob files (no sidecar → read as `None`) and pre-slice-8 `MediaMeta` rows (no `blob_key`) are orphaned/incompatible: deploy by wiping the blob directory and the media table.

## §Verification — EXECUTED 2026-10-15 (reviewer; authoring subagent has no shell)

Baseline at slice start: `cargo test` 200 green (HEAD 358a15e).

- `cargo test --lib` — 154 passed, 0 failed (148 + 6 new: sidecar,
  blob_key bounds, 3× `slice8_*` commands, packer md5 stability; remote_cache
  roundtrip extended in place).
- `cargo test --test conformance` — 46 passed, 0 failed (45 + M3).
- `cargo test` (full, `EXIT=0`): lib 154, main 0, client_smoke 1
  (`SMOKE_OK` via `scripts/smoke-phase1.sh` — live misskey-js upload +
  `GET /files/:id` byte-compare green under SHA-256 blob keys),
  conformance 46, crash_recovery 2, federation 2, federation_interop 1,
  minrepro 1. **Grand total: 207 passed, 0 failed** (baseline 200; +7).
- Warnings: set identical to the slice-7 baseline (same files/lines;
  all pre-existing Phase 2 / phase2-5 era). No new warnings.
- Failing-first evidence (reconstructed honestly): with `sha256_hex`
  neutered to a constant key, `slice8_distinct_uploads_never_share_blob_storage`,
  `slice8_empty_upload_pins_known_md5_and_sha256_vectors`, and conformance
  `m3_media_blob_key_roundtrip` FAIL; md5-stability and owner-dedup tests
  pass regardless (they pin md5 behavior, orthogonal by design). Neuter
  reverted; final tree has no `TEMP-NEUTER` markers (verified by grep).
- NOT run: `scripts/smoke-phase2.sh` (needs Node 22 + Docker; same sandbox
  limitation as slices 1–7). Do not claim `SMOKE_OK`.
- Reviewer-confirmed by reading: storage sidecar/`delete` paths,
  `domain/media.rs` + traversal-test repaired regions intact, all blob call
  sites on `blob_key`, `find_by_owner_and_hash` still md5, packer untouched.

1. `cargo test --lib` — expect all green (new: `blob_store_get_verifies_length_sidecar`, `blob_key_rejects_empty_and_binary`, 3× `slice8_*` commands tests, `slice8_pack_drive_file_md5_stable_for_same_bytes`, plus the extended remote_cache roundtrip).
2. `cargo test --test conformance` — expect all green incl. new `m3_media_blob_key_roundtrip`.
3. `cargo test` (full) — expect 200 + new tests green, zero failures; confirm no new warnings (`cargo build 2>&1 | grep -i warning` vs baseline).
4. NOT run by author (no shell): everything above, plus `scripts/smoke-phase2.sh` (needs Node 22 + Docker — same sandbox limitation as slices 1–7; do not claim `SMOKE_OK`).

## Compile-risk spots for the reviewer (no shell here, read extra carefully)

- `src/media/storage.rs` — new `write_atomic(&self, hash, final_path: &Path, bytes)` helper: `Path` import already exists (line 3); `sidecar_path` uses `set_file_name` on an owned `PathBuf`. `delete` chains two `match`es with `?` on the first — confirm the `Ok(()) => Ok(())` arms typecheck under `?`.
- `src/media/ingest.rs::sha256_hex` — `hex::encode(hasher.finalize())` mirrors the md5 pattern; `sha2::Output<Sha256>` must satisfy `AsRef<[u8]>` for `hex 0.4` (same as `md5::Md5` output did).
- Every `InsertMediaInput`/`MediaMeta` constructor updated: `ports.rs` (def), `commands/media.rs`, `remote_cache.rs` ×2, `redb/mod.rs` ×2, `domain/media.rs::valid_media`, `packers.rs::test_media`, `conformance.rs::valid_media_input`. Webfinger `Mem` doubles take `_input` and construct nothing — no change needed. Grep `InsertMediaInput {` / `MediaMeta {` to confirm none missed.
- `domain/media.rs` tests module was hand-repaired twice (a dropped `url_comment_bounds` header during test insertion): confirm `blob_key_rejects_empty_and_binary` and `url_comment_bounds` are both well-formed (read lines ~313–350).
- `storage.rs` tests module likewise: confirm `blob_store_rejects_traversal_hashes` kept its `#[test]` header and body (edit slip repaired; read the tail of the module).
- Conformance `m1`/`m2` reuse `valid_media_input` (now with a fixed `blob_key` shared across rows) — metadata rows have no uniqueness on `blob_key`, so sharing is legal; `m3` uses distinct computed keys.
- `slice8_empty_upload_pins_known_md5_and_sha256_vectors` uploads `b""`: `sniff_mime(b"")` → `text/plain`, size 0 ≤ max — confirm no empty-bytes rejection exists on the path.
- Crash-recovery manifest gains `media_blob_key`: old `restart_state.json` files (none persist — temp dirs) are irrelevant.
