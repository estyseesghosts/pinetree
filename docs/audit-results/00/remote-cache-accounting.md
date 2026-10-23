Read-only audit complete. All line numbers below are current-tree evidence.
1) Per-finding verdicts (slices 1,2,4,5)
Finding 1 — reservation leaks — FIXED
fetch.rs:168-174 reserves max_bytes; 182-464 runs everything post-reserve inside one closure that never releases; 465-474 is the single cleanup path:
match outcome { Ok(m)=>{release(max)?;Ok(m)} Err(e)=>{release(max)?;Err(e)} }
Every ? inside (acquire_media_op/acquire_blob_key 263, lookup/byte_len/touch/staging 264-232, cache_stats 242, commit 294-456) propagates to that release. No let _ = on release remains. reserve false-path 169-174 returns without release (correct — nothing held).
Finding A — ensure_cache_budget ignores in_flight — FIXED
evict.rs:156-172 is_over_budget: total+in_flight+need>max. media_store.rs:981-984 reserve_cache_bytes: want=total+in_flight+bytes; want>cap?false. Both include in_flight.
Finding B — provisional-class check only — FIXED
fetch.rs:167 provisional force_class|Preview for ensure; 236 real class=force_class|classify_mime(staged.mime); 242-254 second budget check with actual (not max_bytes), class_bytes_in(class)+actual>cap+slack, fail-closed CACHE_CAPACITY_EXCEEDED to outer release. No bytes/totals moved on that exit.
Finding C — shared-blob charge==false phantom — FIXED
fetch.rs:295 was_absent=byte_len.is_none(); 311-315 previously_debited=all absent/zero; 315 charge=was_absent||previously_debited; 439-455 if charge {apply(actual,actual)} else {apply(0,0)}; outer 467/471 always release(max). Net shared-hit: in_flight +max-max=0, total+=0. Charge-owner: total+=actual exactly once. Matches plan §1.2.
Finding 2 — oldest-8 / tombstone / alias-window — FIXED
- Charged pagination, not oldest-8: evict.rs:104-106 remote_cache_charged_page(offset,32); media_store.rs:514-540 filters size>0 && !absent store-side.
- Tombstones excluded from window by construction (528).
- Alias completeness under lock: evict.rs:179-218 snapshot_key_candidate via cache_entries_for_blob_key (full alias set, never window); 273-319 locked re-read + len + per-alias media_id/blob_key/generation/last_access/fetched_at CAS. >8 aliases covered; evict_snapshot_skips_alias_added_after_listing test pins it.
- Same-ms guard: AliasBaseline{generation,...} 37-45; store bumps on insert media_store.rs:333/357, touch 460, eviction 814; compare evict.rs:311-318 includes generation. Test 527-574 proves same-last_access_ms skip.
Finding 9 (tombstone half) — FIXED with residual
evict.rs:227-246 retain_expired_tombstones: tombstone_page(0,64), per-row own-class TTL 232, get(media_id).is_some()=>skip 236, CAS remove_if(url,variant,media_id,blob_key) 239. No last_access fake-advance. Residual §2.4 below (head-of-line block, single page).
Finding 4 + E — dangling CAS — FIXED (selection + CAS), with new debit bug (§2.1)
- Not LRU: scan.rs:275-291 remote_cache_dangling_page(offset,page); media_store.rs:581-614 media-absent, (source_url,variant) key order, skipped<offset counts only dangling.
- CAS: scan.rs:300-307 locked lookup must still equal candidate.media_id/blob_key; 311 live-media recreated =>skip; 325-330 remove_if with same pair.
- Debit after delete, current size: 338-348 if charged && accounted.insert(key) {apply(-current.size,...)}. Uses current.size_bytes, not stale candidate. One debit/key/pass via accounted_blobs.
Finding 6 / slice 5 — per-class — FIXED (rule + per-key class)
- One rule: media_store.rs:937-946 cache_accounting_apply AudioVideo=>{}; 858-876 commit_cache_eviction AudioVideo=>{} only total moves; evict.rs:388-399 class_bytes_in(AudioVideo)=0; 379-386 class_cap(AudioVideo)=0. ports/media_store.rs:89-93 documents historical full_bytes left alone, never re-guessed.
- Per-key authoritative: CACHE_BLOB_CLASS defined media_store.rs:44; remote_cache_insert 414-428 first-charge wins / restore-overwrites / live-sharer-keeps + self_live_rewrite 328-330/414 guard; commit 858-866 reads it, caller class only pre-migration fallback; last-alias removal drops it 346-353/637-644/686-693. backup.rs:27,202, store.rs:317 carry it.
2) New / residual bugs
H1 — dangling path debits live shared key. scan.rs:316-348: charged=!absent&&size>0; byte-delete gated on blob_refcount==0 317, but debit 338-348 runs whenever charged && remove_if succeeded, even if refcount>0 (other live cache/durable sharers). One dangling alias removal debits the whole physical key while siblings stay charged → total_bytes undercounts, evictions phantom. Should debit only when last charged alias removed (or route through commit_cache_eviction).
H2 — open-time backfill corrupts cache accounting. store_backfill.rs:444-516: sums meta.size_bytes per MEDIA_META row (480 total, 467-477 class), no evicted check, per-row not per-key (shared blob N aliases counted N×), and never writes CACHE_BLOB_CLASS. After any fresh rebuild: total/class inflated, evicted bytes counted as live, all backfilled keys fall back to max-alias guess in commit_cache_eviction.
M1 — eviction still full-scans per round (finding 9 perf residual). media_store.rs:525-531 charged, 556-562 tombstone, 594-613 dangling re-scan from zero per page; commit_cache_eviction:765-772 durable probe full MEDIA_META scan + 777-784 + 824-830 full scans inside a write txn. Called up to MAX_EVICTION_ROUNDS=64 (evict.rs:99). O(64·N) per ensure_cache_budget + writer lock held during scans.
M2 — retention head-of-line block. evict.rs:227-230 always tombstone_page(0,64); 233-236 continue on unexpired/live-media rows without advancing offset. Unremovable head rows pin the window; expired tail beyond 64 never visited while over-budget persists. Index-row leak (not bytes).
M3 — old-key abandon GC races. fetch.rs:262-263 holds lifecycle + new-key lock only; 418-437 checks blob_refcount(old)==0 && cache_entries(old).is_empty() then cache_blobs.delete(old). Old-key stripe not held, so concurrent refetch of old bytes can re-install between check and delete → live-byte delete. Must acquire old-key lock (ordered to avoid inversion).
M4 — crash-orphan bytes never charged. fetch.rs:295,311-315: pooled-but-never-indexed bytes → was_absent=false, entries.empty()=>previously_debited=false, charge=false. Row inserted with size=actual but total+=0. Bytes present, uncounted; later eviction saturating_sub hides drift. Comment 418-421 acknowledges but leaves total low.
L1 — class slack ignores in_flight. evict.rs:163-171 and fetch.rs:245-248 slack=max-total (not max-total-in_flight). Concurrent reservers over-borrow class allowance; global check covers it, class isolation does not.
L2 — commit_cache_eviction write-txn for reads. media_store.rs:774,801-805 return Ok(AlreadyAbsent/Skipped) from inside let outcome={...} holding a write txn — drops uncommitted (safe) but did a full scan under a write lock. Contention only.
L3 — saturating reserve/release masks bugs. media_store.rs:988,1010 saturating_add/sub. Double-release clamps to 0 silently; heal.rs:13-21 reset_in_flight relies on it. No error signal on drift.
3) DB-corruption / drift scenarios
1. Shared blob, one URL's media row deleted → reconciler dangling pass removes that alias and debits full key (H1) → total permanently low; subsequent eviction of remaining alias saturating_sub sticks at 0 with bytes still live.
2. Restart with unmarked DB → backfill (H2) double-counts every shared key, counts evicted rows, leaves CACHE_BLOB_CLASS empty → full/preview inflated, all later evictions guess class until next charge.
3. M3 interleaving: A abandons K_old→K_new (refcount 0 observed), B refetches K_old (re-put + index), A deletes K_old bytes → B's index points at missing bytes; heals only via next fetch/reconcile miss path.
4. M2 + pressure: tombstone table grows without bound (one 64-row head-blocked page per pressure event); charged_page full scans degrade until MAX_EVICTION_ROUNDS fail-closed reservations under load.
