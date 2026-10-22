use crate::application::commands::AppServices;
use crate::application::ports::{
    CacheEvictionOutcome, CacheStats, MediaClass, PortError, RemoteCacheRecord,
};
use crate::media::storage;
use std::collections::HashSet;

/// Bound on eviction rounds per [`ensure_cache_budget`] call: the loop
/// re-reads accounting after every evicted candidate, so a backend whose
/// counters never move (or candidates that never shrink the total) cannot
/// spin forever — the fetch then fails closed at the reservation.
const MAX_EVICTION_ROUNDS: usize = 64;

/// Ensure `need_bytes` for `class` fit under the global cap and the class
/// cap with borrowing (plan §5): a class may borrow unused global room, so
/// eviction only triggers while `total + need > max`, or while the class
/// exceeds its cap *plus* the currently unused global slack.
///
/// Candidates come LRU-first from the store (at most 8 per round) and are
/// ordered stale-first, where staleness uses EACH candidate's OWN class TTL
/// (Avatar 30d, Preview 7d, Full 3d, AudioVideo always stale) against its
/// `fetched_at_ms` — never the incoming reservation's class. TTL only
/// orders: when nothing stale is eligible, fresh rows still evict LRU-first
/// so the budget can fit `need_bytes`.
///
/// Every candidate is re-validated under the whole-transition hold (see
/// [`try_evict_snapshot`]): the listed snapshot is compared against the
/// current index rows plus the live byte length, so a concurrent touch,
/// refetch, or eviction wins over a stale listing. Each eviction deletes
/// cache-pool bytes ONCE per physical key and then transitions the shared
/// rows through `commit_cache_eviction` (exactly one accounting move per
/// key, idempotent across runs). Terminates after
/// [`MAX_EVICTION_ROUNDS`] re-reads; whatever still does not fit fails
/// closed at the reservation.
///
/// Pool/refcount semantics (Phase 1 P0): the delete targets the cache pool
/// (`services.cache_blobs`, a separate directory tree) and therefore can
/// never remove durable bytes — durable rows live in `services.blobs` and
/// are unreachable here. What eviction MUST hold is the shared lifecycle
/// gate plus the per-blob-key lock across the byte delete + accounting
/// commit, serializing against concurrent fetches, deletes, and the
/// exclusive orphan sweep.
pub fn ensure_cache_budget(
    services: &AppServices,
    need_bytes: u64,
    class: MediaClass,
) -> Result<(), PortError> {
    let max = services.remote_cache_max_bytes;
    let cap = class_cap_bytes(services, class);
    let now_ms = services.clock.now_ms();
    let mut evicted_keys: HashSet<String> = HashSet::new();
    for _ in 0..MAX_EVICTION_ROUNDS {
        let stats = services.media.cache_stats()?;
        let slack = max.saturating_sub(stats.total_bytes);
        let allowance = cap.saturating_add(slack);
        let over_global = stats.total_bytes.saturating_add(need_bytes) > max;
        let over_class = class_bytes_in(&stats, class).saturating_add(need_bytes) > allowance;
        if !over_global && !over_class {
            return Ok(());
        }
        let candidates = services.media.remote_cache_evict_candidates(8)?;
        if candidates.is_empty() {
            break;
        }
        let ordered = order_candidates(services, now_ms, candidates);
        let mut progressed = false;
        for snapshot in &ordered {
            if !evicted_keys.insert(snapshot.blob_key.clone()) {
                continue;
            }
            if try_evict_snapshot(services, snapshot)? {
                progressed = true;
                break; // re-read accounting before the next eviction.
            }
        }
        if !progressed {
            break;
        }
    }
    Ok(())
}

/// Re-validate one listed candidate under the whole-transition hold and
/// evict it when still eligible. Returns `true` only when accounting moved
/// (one debit for the physical key); every other outcome leaves bytes,
/// rows, and counters exactly as found.
///
/// Skip (return `false`) when: no index row references the key anymore;
/// every row is already charged off (`bytes_absent` or size 0); any row was
/// touched or refetched after the snapshot (`last_access_ms` or
/// `fetched_at_ms` newer); the live byte length differs from the charged
/// size (replaced/refetched under us, or absent); or a durable row still
/// references the key (side-effect-free probe: committing size 0 mutates
/// nothing on any backend but still reports `SkippedDurableRef`).
///
/// When still eligible: delete the cache-pool bytes once, then commit with
/// the size actually on the index (max when rows disagree, never the sum).
/// A failed delete propagates before the commit, so the metadata transition
/// never runs without the byte delete.
fn try_evict_snapshot(
    services: &AppServices,
    snapshot: &RemoteCacheRecord,
) -> Result<bool, PortError> {
    // Bytes ONLY + accounting, under the whole-transition hold:
    // serialized against a concurrent fetch commit or delete for this key.
    // Metadata + index bookkeeping stay for re-fetch.
    let _lifecycle = storage::acquire_media_op("cache_evict")?;
    let _key_lock = storage::acquire_blob_key(&snapshot.blob_key)?;
    // Re-read every index row for this key (the candidate plus sharers).
    let entries = services
        .media
        .cache_entries_for_blob_key(&snapshot.blob_key)?;
    if entries.is_empty() {
        return Ok(false);
    }
    // Already charged off on an earlier run: idempotent skip, no second
    // debit.
    if entries
        .iter()
        .all(|entry| entry.bytes_absent || entry.size_bytes == 0)
    {
        return Ok(false);
    }
    // Touched or refetched after the candidate was listed: a concurrent
    // fetch won the key — leave the fresh bytes alone.
    for entry in &entries {
        if entry.last_access_ms > snapshot.last_access_ms
            || entry.fetched_at_ms > snapshot.fetched_at_ms
        {
            return Ok(false);
        }
    }
    // One charge per physical key: disagreeing rows debit the max
    // previously added, never the sum.
    let mut charge_bytes = 0u64;
    let mut charge_class = snapshot.class;
    for entry in &entries {
        if entry.size_bytes > charge_bytes {
            charge_bytes = entry.size_bytes;
            charge_class = entry.class;
        }
    }
    if charge_bytes == 0 {
        return Ok(false);
    }
    // Byte presence plus length re-check: absent bytes (already evicted or
    // lost) and replaced bytes both skip without moving counters.
    match services.cache_blobs.byte_len(&snapshot.blob_key)? {
        Some(len) if len == charge_bytes => {}
        _ => return Ok(false),
    }
    let index_keys: Vec<(String, String)> = entries
        .iter()
        .map(|entry| (entry.source_url.clone(), entry.variant.clone()))
        .collect();
    // Durable probe (side-effect-free): size 0 commits nothing, but a
    // durable reference still reports `SkippedDurableRef` — leave the blob
    // (and its charge) alone.
    match services
        .media
        .commit_cache_eviction(&snapshot.blob_key, 0, charge_class, &index_keys)?
    {
        CacheEvictionOutcome::SkippedDurableRef => return Ok(false),
        // Unreachable (size 0 never debits); fail closed regardless.
        CacheEvictionOutcome::Evicted { .. } => return Ok(false),
        CacheEvictionOutcome::AlreadyAbsent => {}
    }
    services.cache_blobs.delete(&snapshot.blob_key)?;
    match services.media.commit_cache_eviction(
        &snapshot.blob_key,
        charge_bytes,
        charge_class,
        &index_keys,
    )? {
        CacheEvictionOutcome::Evicted { .. } => Ok(true),
        CacheEvictionOutcome::AlreadyAbsent | CacheEvictionOutcome::SkippedDurableRef => Ok(false),
    }
}

/// Stale when `fetched_at_ms` reached the class TTL. AudioVideo TTL 0 is
/// always stale, so its order collapses to LRU.
fn cache_entry_stale(ttl_days: u64, now_ms: i64, fetched_at_ms: i64) -> bool {
    let ttl_ms = ttl_days as i64 * 86_400_000;
    now_ms.saturating_sub(fetched_at_ms) >= ttl_ms
}

/// Stale-first (each candidate under its OWN class TTL), LRU within each
/// group, then `(source_url, variant)` for determinism.
fn order_candidates(
    services: &AppServices,
    now_ms: i64,
    mut candidates: Vec<RemoteCacheRecord>,
) -> Vec<RemoteCacheRecord> {
    candidates.sort_by(|a, b| {
        let a_stale = cache_entry_stale(ttl_days_for(services, a.class), now_ms, a.fetched_at_ms);
        let b_stale = cache_entry_stale(ttl_days_for(services, b.class), now_ms, b.fetched_at_ms);
        (!a_stale)
            .cmp(&(!b_stale))
            .then_with(|| a.last_access_ms.cmp(&b.last_access_ms))
            .then_with(|| a.source_url.cmp(&b.source_url))
            .then_with(|| a.variant.cmp(&b.variant))
    });
    candidates
}

/// Per-class cap in bytes (AudioVideo caches on borrowed global room only —
/// plan §5 gives it a 0 MiB persistent allocation: stream/fetch on demand).
fn class_cap_bytes(services: &AppServices, class: MediaClass) -> u64 {
    match class {
        MediaClass::Avatar => services.remote_cache_avatar_bytes,
        MediaClass::Preview => services.remote_cache_preview_bytes,
        MediaClass::Full => services.remote_cache_full_bytes,
        MediaClass::AudioVideo => 0,
    }
}

/// Class bytes as exposed by [`CacheStats`] (AudioVideo has no dedicated
/// counter — it is governed by the global cap alone).
fn class_bytes_in(stats: &CacheStats, class: MediaClass) -> u64 {
    match class {
        MediaClass::Avatar => stats.avatar_bytes,
        MediaClass::Preview => stats.preview_bytes,
        MediaClass::Full => stats.full_bytes,
        MediaClass::AudioVideo => 0,
    }
}

/// Per-class TTL in days (AudioVideo TTL 0: always eviction-eligible first).
fn ttl_days_for(services: &AppServices, class: MediaClass) -> u64 {
    match class {
        MediaClass::Avatar => services.remote_cache_avatar_ttl_days,
        MediaClass::Preview => services.remote_cache_preview_ttl_days,
        MediaClass::Full => services.remote_cache_full_ttl_days,
        MediaClass::AudioVideo => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::super::support::test_services;
    use super::*;
    use crate::application::ports::{InsertMediaInput, MediaClass, RemoteCacheRecord};
    use crate::domain::media::MediaProperties;

    #[test]
    fn cache_class_caps_and_ttls_match_plan() {
        // Plan §5 starting values (also the Config defaults).
        assert_eq!(MediaClass::classify_mime("image/png"), MediaClass::Preview);
        assert_eq!(
            MediaClass::classify_mime("video/mp4"),
            MediaClass::AudioVideo
        );
        assert_eq!(
            MediaClass::classify_mime("audio/mpeg"),
            MediaClass::AudioVideo
        );
        assert_eq!(MediaClass::classify_mime("text/plain"), MediaClass::Full);
        assert_eq!(
            MediaClass::classify_mime("application/octet-stream"),
            MediaClass::Full
        );
        let services = test_services("class-caps", 4096);
        assert_eq!(class_cap_bytes(&services, MediaClass::Avatar), 128 << 20);
        assert_eq!(class_cap_bytes(&services, MediaClass::Preview), 256 << 20);
        assert_eq!(class_cap_bytes(&services, MediaClass::Full), 128 << 20);
        assert_eq!(class_cap_bytes(&services, MediaClass::AudioVideo), 0);
        assert_eq!(ttl_days_for(&services, MediaClass::Avatar), 30);
        assert_eq!(ttl_days_for(&services, MediaClass::Preview), 7);
        assert_eq!(ttl_days_for(&services, MediaClass::Full), 3);
        assert_eq!(ttl_days_for(&services, MediaClass::AudioVideo), 0);
    }

    #[test]
    fn stale_uses_candidate_class_ttl_not_incoming_class() {
        // Table on the sort key: AudioVideo (TTL 0) is stale even when
        // fetched now; Avatar (TTL 30d) is fresh at 8 days old.
        assert!(cache_entry_stale(0, 100, 100));
        assert!(cache_entry_stale(0, 100, 0));
        assert!(!cache_entry_stale(30, 100, 100 - 8 * 86_400_000));
        assert!(cache_entry_stale(30, 100, 100 - 30 * 86_400_000));
        assert!(!cache_entry_stale(7, 100, 100 - 6 * 86_400_000));
        assert!(cache_entry_stale(7, 100, 100 - 7 * 86_400_000));
        assert!(!cache_entry_stale(3, 100, 100 - 2 * 86_400_000));
        assert!(cache_entry_stale(3, 100, 100 - 3 * 86_400_000));
    }

    /// Insert one charged cache row exactly the way the production fetch
    /// path does (bytes + metadata + index + accounting).
    fn insert_cache_row(
        services: &AppServices,
        source_url: &str,
        blob_key: &str,
        bytes: &[u8],
        class: MediaClass,
        fetched_at_ms: i64,
        last_access_ms: i64,
    ) -> String {
        services.cache_blobs.put(blob_key, bytes).unwrap();
        let meta = services
            .media
            .insert_metadata(InsertMediaInput {
                id: None,
                owner_id: None,
                name: "remote.bin".to_string(),
                mime: "image/png".to_string(),
                size_bytes: bytes.len() as u64,
                hash: "0cc175b9c0f1b6a831c399e269772661".to_string(),
                blob_key: blob_key.to_string(),
                blurhash: None,
                properties: MediaProperties::default(),
                is_sensitive: false,
                is_link: true,
                url: format!("https://pinetree.local/files/{source_url}"),
                thumbnail_url: None,
                comment: None,
                source_url: Some(source_url.to_string()),
                max_file_size: 4096,
            })
            .unwrap();
        services
            .media
            .remote_cache_insert(&RemoteCacheRecord {
                source_url: source_url.to_string(),
                variant: "original".to_string(),
                media_id: meta.id.clone(),
                blob_key: blob_key.to_string(),
                size_bytes: bytes.len() as u64,
                class,
                fetched_at_ms,
                last_access_ms,
                access_count: 1,
                bytes_absent: false,
            })
            .unwrap();
        services
            .media
            .cache_accounting_apply(
                bytes.len() as i64,
                class,
                bytes.len() as i64,
                0,
                1,
                bytes.len() as u64,
                0,
                0,
            )
            .unwrap();
        meta.id
    }

    #[test]
    fn evict_snapshot_skips_row_touched_after_listing() {
        // Barrier: a touch landing after the candidate listing (stale
        // snapshot vs fresh re-read) must skip without moving bytes or
        // counters. A fresh snapshot still evicts, exactly once.
        let services = test_services("evict-barrier", 4096);
        let key = "b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1";
        let bytes = vec![7u8; 24];
        insert_cache_row(
            &services,
            "https://r.example/pic.png",
            key,
            &bytes,
            MediaClass::Preview,
            100,
            100,
        );
        let snapshot = services
            .media
            .remote_cache_lookup("https://r.example/pic.png", "original")
            .unwrap()
            .unwrap();
        // Concurrent touch after the listing: the re-read sees it and wins.
        services
            .media
            .remote_cache_touch("https://r.example/pic.png", "original", 200)
            .unwrap();
        assert!(!try_evict_snapshot(&services, &snapshot).unwrap());
        assert_eq!(services.cache_blobs.get(key).unwrap().unwrap(), bytes);
        assert_eq!(services.media.cache_stats().unwrap().total_bytes, 24);
        // Fresh snapshot evicts.
        let fresh = services
            .media
            .remote_cache_lookup("https://r.example/pic.png", "original")
            .unwrap()
            .unwrap();
        assert!(try_evict_snapshot(&services, &fresh).unwrap());
        assert!(services.cache_blobs.byte_len(key).unwrap().is_none());
        // Idempotent: a second call moves nothing.
        let stats = services.media.cache_stats().unwrap();
        assert!(!try_evict_snapshot(&services, &fresh).unwrap());
        assert_eq!(services.media.cache_stats().unwrap(), stats);
    }
}
