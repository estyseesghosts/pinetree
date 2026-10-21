> **Status (2026-10-21 overhaul S3 — DONE, gate green):** everything in §§2–6 is implemented (see S3 log `state/logs/2026-10-21-storage-overhaul-s3.md`): ref-safe delete + quota, `(source_url, variant)` index + accounting, metadata-preserving LRU+TTL eviction, on-demand default with in-flight reservation, separate pools + rename migration, bounded idempotent reconciler, `JobKind::CacheEvict|ReconcileStorage|BackfillDurableAccounting` + worker arms, `durable_dir`/`cache_dir` overrides + reserved-sum guard, stream-to-disk staging, `drive/cache/stats` observability. `bash scripts/smoke-all.sh` → `SMOKE_ALL_OK` (phase2 SKIP, needs Node 24). Stack-overflow fix included: 8 MiB runtime threads + `dispatch_fits_bounded_stack` guardrail. Remaining: socket-to-disk transport buffering (follow-up); FDB parity is Phase 4.
>
> **Status (2026-10-21 overhaul WIP):** S1 shared contracts landed (config keys, ports, redb tables, dual pools, drive/cache/stats). Pending subagent slices: MediaMeta variant/class/evicted, fetch_stream, quota-aware commands/media, reconciler, remote-cache rework, tests/storage_*.rs. Open boxes below unchanged until cargo test --offline + smoke-all.sh (cap-never-exceeded, local-survives-eviction, cross-owner delete, crash-heal) are green.
>
> **Status (2026-10-20 freshness pass):** Build-step status below. Landed: per-note 64 MiB budget, 16-attachment cap, SHA-256 blob keys, durable RemoteMediaFetch job. Open: reference-safe delete, eviction, global/class budgets, on-demand default, stream-to-disk, reconciler, observability, warming, FDB parity. No GC/reconciler exists (grep `evict|quota|warm|reconcil` over `src/media` and `src/config.rs` is empty).
> Tags: `[LANDED — evidence]` verified in code at HEAD; `[OPEN]` not in code; `[SUPERSEDED by …]`; `[DECISION — still binding]`; `[PLAN HISTORY]`. Original text is unchanged; tag blocks are additions. Short paths are under `src/`; `mod.rs` = `src/api/misskey/endpoints/mod.rs`.

Status: Draft

# Pinetree — remote-media cache plan (from `storage-cache.md` review)

Source: `/root/storage-cache.md` (upstream comparison: Akkoma / Mastodon /
Misskey + shared-policy recommendation). This doc is the Pinetree-specific
forward plan. Birchtree's counterpart is
`birchtree/docs/remote-media-cache-plan.md` — same policy concepts,
different starting point (Birchtree: lifecycle stubs + eager fetch, no
content addressing) and schema (Pinetree: Misskey-compatible media
semantics; Birchtree: Mastodon-compatible attachments).

## 1. Where Pinetree stands

> **[LANDED — remote_cache.rs:28,35,134,293]** Building blocks: 16-cap `remote_cache.rs:28`; 64 MiB per-job-run budget `:35,:133-136`; SHA-256 blob key `:293`; SSRF/URL validation `:38`.
> The 'Missing' paragraph (plan `:35-39`) still holds: `[OPEN]` for global budget, class accounting, and eviction.


Existing building blocks (`src/media/remote_cache.rs`,
`src/media/storage.rs`, `src/media/ingest.rs`):

- URL validation, SSRF checks, per-file limits, MIME sniffing, durable
  background jobs (`JobKind::RemoteMediaFetch`, enqueued atomically with the
  note; worker `process_remote_media_fetch`). Already-implemented guards:
  1024-char URL cap (`remote_cache.rs:49`), HTTPS→HTTP downgrade refusal +
  redirect limits (`delivery.rs:560-570`), 32 KiB header cap / interim-block
  handling (`delivery.rs:863-901`).
- 64 MiB cumulative per-note **per job run**, first attachment always
  attempted (`remote_cache.rs:30-35,133-136`: the budget is checked between
  attachments, so a single `max_file_size` attachment plus retries can
  exceed 64 MiB — it is not a hard total) + attachment-count cap of 16
  (`MAX_REMOTE_ATTACHMENTS_PER_NOTE = MAX_FILE_IDS`,
  `remote_cache.rs:28`, `domain/note.rs:15`; >16 truncated keeping document
  order, `remote_cache.rs:598-626`).
- SHA-256 content-addressed `BlobStore` + stored-length checks.
- Metadata already carries `source_url`, `blob_key`, `size_bytes`,
  remote-file marker (`is_link`, `owner_id = None`).

Missing: a global remote-cache budget, class-specific accounting
(no reference-count index exists — `ports.rs:1778-1786` offers only
`find_by_owner_and_hash` / `find_by_source_url`), and an eviction
lifecycle. The per-note cap cannot stop thousands of individually
acceptable notes from filling the disk.

## 2. Fix-first: shared-blob deletion bug (before any eviction work)

> **[OPEN — commands/media.rs:211-216]** `delete_media` still calls `delete_metadata` (`:213`) then `blobs.delete` (`:214`) with no reference check, in two separate writes. Plan line refs `:147-151` are stale; the function is now at `:211`.
> Same-owner dedup holds: `upload_media` early-returns on `find_by_owner_and_hash` (`commands/media.rs:45`).


`delete_media` (`src/application/commands/media.rs:147-151`) deletes the
metadata row then the SHA-256 blob with no reference check, and the two
deletes are not in one transaction (crash between them already
orphans/misses regardless of refcounting). Two scoping facts bound the
blast radius — and shape the fix:

- Same-owner duplicates cannot occur: `upload_media` (`media.rs:43-46`)
  early-returns on `find_by_owner_and_hash`. Sharing requires **cross-owner**
  identical bytes or byte-identical local/remote overlap.
- Remote rows are currently unreachable via this path: `get_media_for`
  (`media.rs:91-100`) returns `NO_SUCH_FILE` for `owner_id = None` rows. So
  the live hole today is local-owner-A's delete orphaning local-owner-B's
  row (and any byte-identical remote row) — but the same no-check rule must
  govern eviction from day one, where remote rows *are* deletable.

Fix requirements:

- Reference accounting (or remaining-reference check) before blob deletion;
  delete physical bytes only when no metadata row references the key.
  Metadata + accounting updates belong in the transaction; blob deletion
  happens outside it with reconciler sweep (per §5.4's own no-atomicity
  rule).
- Regression tests: cross-owner same-bytes delete preserves the survivor;
  local delete preserves a byte-identical remote row; crash between
  metadata-delete and blob-delete heals via the reconciler.
- Decide whether remote rows ever become directly deletable, and document
  it.
- Audit the eviction path (§5.3) against the same rule from day one.

## 3. Corrections adopted from the review

> **[OPEN]** Item 1 (eager vs lazy): `[OPEN]`. Still eager on arrival: `inbox.rs:2157-2164` builds the job per note. The job runs off-request (`worker.rs:147`), but there is no fetch-on-first-request.
> Item 5 (stream enforcement): `[OPEN]`. `fetch_and_attach_remote_media` still holds the full body (`remote_cache.rs:245-256`) and calls `blobs.put` on the Vec (`:174`, `:323`). Plain-HTTP and curl cap claims (`delivery.rs:469-470,928-995`) were not re-verified here.


1. **Current fetch is async-eager, not lazy.** Queueing downloads when notes
   arrive moves work off ingest but still fetches everything. Default must
   become fetch-on-first-media-request; keep the job system for optional
   warming.
2. **Per-class limits AND a hard global cap.** Protect avatars/emoji from
   post-image pressure and vice versa; revalidate avatar/emoji URLs (content
   at a URL can change).
3. **SHA-256 ≠ identity.** Dedups byte-identical files only, not
   resized/re-encoded variants. Keep source URLs after eviction so bytes can
   be re-fetched.
4. **Durable vs expendable separation.** Local uploads must not disappear
   under remote-cache pressure: separate pools, or reference accounting that
   explicitly protects durable references.
5. **Stream enforcement — precise state.** Plain-HTTP already aborts
   mid-body (`delivery.rs:991-995`) and dishonest `Content-Length` is
   rejected pre-body (`delivery.rs:928-932`); the HTTPS path relies on
   `curl --max-filesize` (`delivery.rs:469-470`); chunked bodies get
   `max+4096` wire slack before the decode check (`delivery.rs:986-1004`).
   Remaining work is structural, not verification: `fetch_and_attach` still
   holds the full `resp.body` in memory before `blobs.put`
   (`remote_cache.rs:174,232-254`) — move to stream-to-disk under the byte
   cap.
6. **Terminology:** Akkoma `prune_objects` is DB-object pruning, not media
   eviction — do not model our reconciler on it.
7. **Budget arithmetic:** starting budget below sums exactly (512 MiB
   global); the review's illustrative 5 GB / 3.5 GB mismatch is not
   repeated here.

## 4. Recommended changes (ordered)

> **[OPEN]** 1 reference-safe deletion: `[OPEN]`. 2 (source-URL, variant) index: `[OPEN]`, `find_by_source_url` exists (`remote_cache.rs:128`) but there is no variant key and no refcount. 3 eviction keeps metadata: `[OPEN]`. 4 on-demand default: `[OPEN]`. 5 separate local vs remote stores: `[OPEN]`. 6 global + class budgets: `[OPEN]`.


1. **Reference-safe deletion** (§2) — prerequisite, land first.
2. **Remote-cache index keyed by (source URL, media variant)** (new schema —
   no such index exists today), separate from the SHA-256 physical blob
   key. Index = what to fetch/revalidate; hash = what is stored. Reference
   accounting for §2 rides on the same new schema.
3. **Eviction keeps metadata + source URLs**, drops bytes only. Evicted
   media re-fetches while the source lives.
4. **Default to on-demand fetch.** Replace download-on-note-arrival with
   fetch-on-first-media-request; retain background jobs for opt-in warming.
   Warming excludes relays by default.
5. **Separate local uploads from remote cached blobs** (preferred), or
   reference accounting that provably protects durable references.
6. **One global budget + per-class caps + automatic eviction.** Keep the
   existing per-file and per-note safety limits as inner guardrails (with
   their actual semantics: per-note = per job run, first attachment always
   attempted, §1).

## 5. Shared policy (same concepts as Birchtree)

> **[OPEN]** No `disabled | on_demand | warm` mode, no class caps, and no global cap exist in `src/`. Grep for `on_demand|warm|evict` in `src/media` and `src/config.rs` is empty.


Modes: `disabled` / `on_demand` / `warm`. **Default: `on_demand`, warming
off.**

| Media class | Max | Example retention |
|---|---|---|
| Avatars, headers, custom emoji | 128 MiB | 30 days |
| Image previews / thumbnails | 256 MiB | 7 days |
| Full image originals | 128 MiB | 3 days |
| Audio/video originals | 0 MiB persistent | stream / fetch on demand |
| **Global cap** | **512 MiB** | — |

Starting values only — tune from measured workloads. Unused class capacity
may be borrowed within the global cap. Admins may raise caps to tens /
hundreds of GB (sponge profile), still bounded. Byte-weighted LRU + expiry
(not LFU); protected avatar/emoji allocation inside (never above) the
global cap.

### Implementation requirements (Pinetree adapters)

1. **Independent accounting:** total + per-class bytes, including in-flight
   fetches (in-flight reservation, not just post-hoc totals). Must respect
   FDB constraints (proposal §§2–5: ~5s txn lifetime, ~10 MB txn size — no
   blobs, no unbounded fan-out in transactions).
2. **Streaming safety:** enforce byte limits during download (see §3.5 for
   the exact current state); keep MIME sniffing, redirect/SSRF/timeout
   checks (§1 guard inventory).
3. **Safe eviction:** remove mapping → update accounting → delete blob only
   when unreferenced. The reference check is new-schema work (§4.2 + §2),
   not an available primitive. Never delete durable local media.
4. **Recovery:** periodic idempotent reconciler (orphaned/missing blobs,
   stale metadata, unfinished deletions, `.len` sidecar mismatches per
   `storage.rs:117-147`). Must explicitly handle pre-slice-8 MD5-keyed rows
   (`domain/media.rs:21-27`: decode but unsupported, `get` returns `None`) —
   purge or migrate them, or "missing blob" reports will flap. No assumed
   filesystem+DB atomicity.
5. **Relay isolation:** relay media out of warming by default; explicit
   limits + global cap if enabled.
6. **Observability:** bytes used, utilization, per-class breakdown,
   hits/misses, bytes fetched, evictions, failed origin fetches — available
   without shell access.

Misskey-compat note: upstream `cacheRemoteFiles` defaults false with
per-remote-user Drive-capacity expiry (avatars/banners excluded) plus an
admin clear operation — that is per-user quota, not a global LRU budget.
Our global+class budget is the deliberate divergence; document it in the
parity plan if it affects `docs/misskey-parity-plan.md`.

## 6. Suggested build order

> **[OPEN]** Step status: 1 shared-blob fix `[OPEN]`; 2 index + refcount `[OPEN]`; 3 eviction + accounting `[OPEN]`; 4 on-demand default `[OPEN]` (`inbox.rs:2157`); 5 reconciler + observability `[OPEN]`; 6 stream-to-disk `[OPEN]`; 7 scoped warming `[OPEN]`; 8 FDB parity `[OPEN]` (`src/persistence/foundationdb/mod.rs` is 0 lines).


1. Shared-blob deletion fix + regression tests (§2).
2. (source-URL, variant) index + reference accounting alongside SHA-256
   blob keys (new schema).
3. Metadata-preserving eviction + global/class accounting (keep per-file /
   per-note limits with §1 semantics).
4. On-demand default (ingest records metadata; fetch on first request).
5. Reconciler (including pre-slice-8 MD5 rows + sidecar mismatches) +
   observability.
6. Stream-to-disk fetch (§3.5) — may move earlier if memory pressure is
   measured first.
7. Scoped warming (opt-in, rate-limited, relay-excluded) last.
8. FDB adapter parity for the accounting/eviction paths (Phase 4 context),
   **including the Phase-4 shared object store**: per proposal §2,
   multi-node needs shared S3-compatible object storage — `FsBlobStore`
   (`storage.rs:9-12`) is single-node local filesystem. Accounting/eviction
   must run against shared storage, with the same contract tests on both
   backends.

## 7. Acceptance tests (must prove)

> **[OPEN]** Existing tests (`remote_cache.rs:423-606`) cover SSRF round-trip, oversize rejection, HTML/SVG-as-octet-stream, and truncation to 16. Not found: in-flight reservation, cap-never-exceeded, local-survives-eviction, eviction re-fetch, cross-owner delete regression.


- Cache never exceeds configured cap under concurrent downloads — with an
  explicit in-flight reservation test (simultaneous downloads cannot
  overshoot), not just post-hoc totals.
- Local uploads survive aggressive eviction.
- Shared blobs not deleted prematurely (§2 regressions: cross-owner,
  local-vs-byte-identical-remote, crash-heal).
- Oversized bodies aborted mid-stream: absent/dishonest `Content-Length`
  over plain HTTP *and* HTTPS (curl path), plus chunked-encoding cases per
  §3.5.
- Per-note budget semantics pinned: two large attachments → second skipped;
  single oversize-first attachment → attempted then rejected.
- Evicted media re-fetches while source remains available.

## 8. Sequencing vs Birchtree

> **[OPEN]** Cross-repo sequencing; `birchtree/` not audited in this pass.


Per the review: implement lifecycle + eviction here first (fetching +
SHA-256 addressing already exist), including the §2 fix, then carry the
same policy + store separation to Birchtree before its remote-media
implementation lands (Birchtree starts from lifecycle stubs + eager fetch,
no content addressing — the transfer is policy concepts and store
separation, not a symmetric merge).
