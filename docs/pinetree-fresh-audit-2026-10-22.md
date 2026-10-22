# Pinetree fresh production-readiness audit

**Audited revision:** [`c4b6e6bb2bfcd55f7ca8503ade40011b5913a817`](https://github.com/estyseesghosts/pinetree/commit/c4b6e6bb2bfcd55f7ca8503ade40011b5913a817) — 2026-10-22 20:15 UTC  
**Scope:** Re-check of the previous production-readiness findings, changed code, and adjacent media, authentication, backup, DNS, and diagnostics paths.  
**Method:** Read-only source review of current GitHub files and the commit's verification log. No local build, live server, Docker run, or external federation run was performed as part of this audit.

## Verdict

The push materially improves the code. The original media CRUD/sweep race, password-only TOTP bypass, backup snapshot skew/configured size cap, and unbounded DNS-thread problems have substantial fixes. However, **Pinetree is still not production-qualified**: the commit's own log says the strict release gate, Docker targets, live federation, and live backup/restore drill were not run.

The current source also exposes three correctness/security defects that should be fixed before production use:

1. Cache eviction does not persist eviction state or revalidate stale candidates. This can corrupt cache accounting, delete newly refreshed bytes, and make backup fail after normal eviction.
2. TOTP challenge consumption and replay-step tracking are separate database operations, so two concurrent requests can both pass the check and mint sessions.
3. Backup retention can delete a status record even when deletion of its ZIP failed, leaving an orphaned backup that is no longer tracked.

## Previous findings: status after the push

| Prior finding | Current assessment |
|---|---|
| Media CRUD and orphan-sweep races | **Mostly fixed.** Whole-operation lifecycle guards and per-key serialization now cover upload/delete/update, and the production sweep holds an exclusive barrier across live-key collection and filesystem deletion. Cache eviction still has separate races/state defects below. |
| Password-only sign-in bypassing enabled TOTP | **Fixed for sequential paths.** Password sign-in refuses TOTP-enabled accounts or issues a short-lived challenge; MiAuth refuses TOTP accounts; the second-factor endpoint requires a challenge; TOTP failure buckets are registered. **Concurrent challenge use remains unsafe** (Finding 2). |
| Backup snapshot/media inventory skew | **Fixed in design.** Table rows and media references are derived from the same redb snapshot, and creation refuses missing referenced blobs. Backup still silently skips a media row if it cannot deserialize it (Finding 5). |
| Configured backup-size limit ignored | **Fixed.** The policy is wired through `BackupContext`, applied during streaming and final-size checks, and passed to offline restore. |
| Unbounded DNS resolver threads | **Fixed in design.** A bounded resolver retains its slot until underlying DNS work returns even when the caller times out. The commit reports 9/9 bounded-DNS tests. |
| Unbounded backup status registry | **In-memory registry is bounded** (1024 terminal-first), and count/byte retention is configurable. Disk history still defaults to “keep all,” so status enumeration remains linear in retained history. |
| Misleading/unimplemented admin diagnostics | **Improved.** Added backup failure/duration/size fields; absent port aggregates remain explicit nulls and are documented. One recent-status truncation bug remains (Finding 4). |
| Panic on malformed WS/actor input; infinite worker lease reclaim | The current merge log says the panic concern was not reproduced as a code defect and worker reclaim is bounded/quarantined. Treat fuzzing and hostile-input stress as remaining qualification work, not as a confirmed current defect. |

## Findings requiring fixes

### P1 — Cache eviction state and accounting are inconsistent

**Evidence:** [`evict.rs`](https://github.com/estyseesghosts/pinetree/blob/c4b6e6bb2bfcd55f7ca8503ade40011b5913a817/src/media/remote_cache/evict.rs#L52-L84), [`media_store.rs`](https://github.com/estyseesghosts/pinetree/blob/c4b6e6bb2bfcd55f7ca8503ade40011b5913a817/src/persistence/redb/media_store.rs#L294-L321), [`scan.rs`](https://github.com/estyseesghosts/pinetree/blob/c4b6e6bb2bfcd55f7ca8503ade40011b5913a817/src/media/reconcile/scan.rs#L190-L251), [`bundle.rs`](https://github.com/estyseesghosts/pinetree/blob/c4b6e6bb2bfcd55f7ca8503ade40011b5913a817/src/backup/bundle.rs#L552-L564).

`ensure_cache_budget` deletes cache bytes and decrements counters, but does not update the corresponding remote-cache index record's `size_bytes` or mark referencing `MediaMeta` rows `evicted=true`. The metadata insertion path always writes `evicted: false`. By contrast, the reconciler's missing-byte repair zeroes the index size but also does not set the media row's eviction flag.

Consequences:

- The same already-evicted candidate can be selected on later calls and charged off repeatedly. Saturating subtraction can make `CacheStats.total_bytes` lower than the actual cache contents, allowing reservations beyond the intended physical-byte budget.
- A later reconciliation can subtract the same bytes again because the metadata row still says `evicted=false`.
- A normal backup after cache eviction can fail: backup derives references from `!meta.evicted` rows and requires object plus `.len` sidecar, but the eviction code leaves the flag false after deleting those bytes.
- `collect_live_blob_keys` currently inserts every row's key without excluding evicted/empty keys, so an eventual correct eviction marker would still keep those absent bytes classified as live during orphan sweeping.
- Several source URLs/variants can map to the same SHA-256 key. Deleting the shared file and charging only one index record leaves other records stale and can cause repeated accounting errors.

**Required fix:** define one authoritative eviction transition under the lifecycle/key lock. Revalidate that the candidate is still evictable; delete bytes; update every affected metadata/index record to the chosen evicted representation; and apply accounting once per physical blob key, not repeatedly per stale index row. Make refetch restore all related state and avoid double counting. Update live-key collection to skip intentionally evicted rows and empty keys. Do not merely add `evicted=true` without defining shared-key semantics.

**Regression tests:** repeated eviction of one key leaves accounting unchanged after the first eviction; shared key referenced by multiple URLs/variants; concurrent refetch between candidate listing and key-lock acquisition; eviction followed by reconcile; eviction followed by successful backup/restore; orphan sweep removes only genuinely unreferenced bytes.

### P1 — Eviction acts on a stale candidate after acquiring locks

**Evidence:** [`evict.rs`](https://github.com/estyseesghosts/pinetree/blob/c4b6e6bb2bfcd55f7ca8503ade40011b5913a817/src/media/remote_cache/evict.rs#L52-L84) obtains candidates before the per-key lock, then deletes the selected key without re-reading the current index/metadata under that lock.

The key lock serializes operations, but does not make the earlier candidate decision current. A fetch can refresh/reinstall the same blob after the candidate list is read but before eviction acquires the key lock. Eviction then deletes the newly refreshed bytes using the stale record and stale size. This risk persists even after adding correct eviction flags/accounting.

**Required fix:** after acquiring the lifecycle/key locks, re-read the source/variant index, byte presence, last-use/TTL eligibility, current references and current accounting. Skip or restart if the candidate is no longer eligible. Then commit the physical delete and bookkeeping as one serialized logical transition.

**Regression test:** deterministic barrier: list candidate → perform successful refresh/touch → allow eviction to acquire lock → assert refreshed live bytes remain readable and are not debited.

### P1 — TOTP challenge is not atomically single-use

**Evidence:** [`auth_2fa.rs`](https://github.com/estyseesghosts/pinetree/blob/c4b6e6bb2bfcd55f7ca8503ade40011b5913a817/src/application/commands/auth_2fa.rs#L233-L271), [`twofa.rs`](https://github.com/estyseesghosts/pinetree/blob/c4b6e6bb2bfcd55f7ca8503ade40011b5913a817/src/api/misskey/endpoints/dispatch_auth/twofa.rs#L318-L360).

`authenticate_2fa` reads/checks the challenge, reads the last TOTP step, writes the last step, and removes the challenge in separate store operations. Two concurrent requests with the same valid challenge and code can both read the still-valid challenge and old step before either update commits. Both may return success, after which the handler independently stores a full session token. Existing replay tests test sequential reuse, not concurrent use.

**Required fix:** make “verify live challenge + compare/update last-used TOTP step + consume challenge” atomic in one persistence transaction. If the current storage port cannot express that transaction, add a narrowly scoped per-account serialization primitive with durable compare-and-consume semantics; an in-process mutex alone does not protect future multi-process deployments, although that is the current declared scope.

**Regression test:** launch two second-factor requests simultaneously with the same account, challenge, and valid TOTP step. Exactly one may succeed and exactly one session may be minted. Also test two simultaneous challenges with the same TOTP step.

### P2 — Backup retention can orphan an untracked ZIP

**Evidence:** [`backup/mod.rs`](https://github.com/estyseesghosts/pinetree/blob/c4b6e6bb2bfcd55f7ca8503ade40011b5913a817/src/backup/mod.rs#L438-L450).

The code attempts to remove the ZIP, but attempts to remove the `.status.json` whether or not ZIP removal succeeded. If ZIP deletion fails for a reason other than “already absent,” the status file can still be removed. The archive then remains on disk but is invisible to backup listing and future retention calculations, permitting untracked backup files to accumulate.

**Required fix:** delete status/registry metadata only after the ZIP was deleted successfully or was confirmed already absent. Preserve status on permission, sharing, and other I/O failures. Only count successful ZIP removals in retention totals.

**Regression test:** simulate a ZIP deletion failure; assert both ZIP and status remain discoverable and a later retention run can retry.

### P2 — Admin backup diagnostics report stale “latest” values after enough backups

**Evidence:** [`admin_status.rs`](https://github.com/estyseesghosts/pinetree/blob/c4b6e6bb2bfcd55f7ca8503ade40011b5913a817/src/api/misskey/endpoints/admin_status.rs#L202-L218) and [`backup/mod.rs`](https://github.com/estyseesghosts/pinetree/blob/c4b6e6bb2bfcd55f7ca8503ade40011b5913a817/src/backup/mod.rs#L319-L343).

`list_backup_statuses` sorts ascending by request time (oldest first). `admin_status` then calls `statuses.truncate(STATUS_BACKUP_LIST_CAP * 4)`, which keeps the *oldest* statuses and discards newer ones before calculating the last success, last failure, duration, byte count, and recent list. Once there are more than 80 status records, the “recent” diagnostics can become stale.

**Required fix:** retain the newest bounded tail before deriving diagnostics, or stream the directory and maintain a bounded newest-first selection. Keep ordering consistent when timestamps tie.

**Regression test:** seed more than `STATUS_BACKUP_LIST_CAP * 4` statuses with the newest success/failure at the end; assert every “last*” field and the displayed recent list use the newest records.

### P2 — Backup can silently omit malformed media rows from its integrity inventory

**Evidence:** [`bundle.rs`](https://github.com/estyseesghosts/pinetree/blob/c4b6e6bb2bfcd55f7ca8503ade40011b5913a817/src/backup/bundle.rs#L552-L564).

While exporting the table snapshot, the callback uses `if let Ok(meta) = serde_json::from_slice::<MediaMeta>(&value)`. If a `media_meta` row cannot be decoded, the raw row is still written into the bundle but omitted from the required-blob inventory. The backup can therefore be published as completed without proving that its media references are present; restore may reject it later. The same consistency principle should apply to semantic checks for `.len` sidecars, not just entry existence and checksums.

**Required fix:** treat an undecodable `media_meta` row as a backup failure. Validate each referenced object and sidecar relationship sufficiently that “completed” means the archive passes the same semantic checks expected by restore.

**Regression test:** inject a malformed media row and a malformed/mismatched `.len` sidecar; backup must fail without publishing a final ZIP. Keep a valid sidecar round-trip test.

## Minor diagnostic/documentation issue

`pack_me_detailed` converts a `totp_flags` storage error into `(false, false)` at [`user.rs`](https://github.com/estyseesghosts/pinetree/blob/c4b6e6bb2bfcd55f7ca8503ade40011b5913a817/src/api/misskey/packers/user.rs#L569-L575). Authentication paths themselves fail closed on a TOTP state-read error, so this is not a demonstrated bypass, but account metadata can falsely display 2FA as disabled during a store fault. Prefer returning the storage error from security-sensitive metadata packing, or expose an explicit unknown/error state rather than false.

`FEDERATION.md` still describes its source baseline as `ad4f7b8`, while it was included in the later `c4b6e6b` push. Update its provenance/status to the actual audited revision and mention the configured DNS pool bound where relevant.

## Release qualification

The latest commit's own log reports unit/integration suites green on the author's native Windows toolchain, but `SMOKE_ALL_REQUIRED=1` was **not** run. Docker was unavailable; the two Docker targets, pinned Misskey/Sharkey live federation, public peer interoperability, and live backup/restore drill remain unqualified. The GitHub combined status API returned no status records for this SHA in this review.

Before calling the server production-ready:

1. Fix the three P1 findings and the backup retention/diagnostic issues above; add failing-first tests for every defect.
2. Run `cargo test --locked --offline`, formatting, clippy, the complete strict `scripts/smoke-all.sh` gate from a fresh build, and the Keystone sweep with prerequisite versions recorded.
3. Build and start both Docker targets from the exact commit; prove readiness, FFmpeg-disabled startup, and FFmpeg-enabled probe/transcode behavior.
4. Run pinned live Misskey + Sharkey federation against this exact SHA.
5. Create a backup after real cache eviction, restore it into a fresh data directory, boot the restored server, and verify accounts, notes, local media, remote-media refetch, and federation continue to work. Include disk-full / backup-write failure and failed retention deletion tests.

**Disposition:** improved, but still **NO-GO for production qualification** until cache eviction/refetch/accounting, atomic TOTP consumption, and backup retention integrity are fixed and the provisioned release gate passes.
