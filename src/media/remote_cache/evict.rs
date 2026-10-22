use crate::application::commands::AppServices;
use crate::application::ports::{CacheStats, MediaClass, PortError};
use crate::media::storage::{acquire_blob_key, acquire_media_op};
use std::collections::HashSet;

/// Bound on eviction rounds per [`ensure_cache_budget`] call: the loop
/// re-reads accounting after every evicted candidate, so a backend whose
/// counters never move (or candidates that never shrink the total) cannot
/// spin forever — the fetch then fails closed at the reservation.
const MAX_EVICTION_ROUNDS: usize = 64;

/// Ensure `need_bytes` for `class` fit under the global cap and the class
/// cap with borrowing (plan §5): a class may borrow unused global room, so
/// eviction only triggers while `total + need > max`, or while the class
/// exceeds its cap *plus* the currently unused global slack. Candidates
/// come LRU-first from the store; TTL-stale rows (by `fetched_at_ms`
/// against the class TTL) evict first. Each eviction deletes cache-pool
/// bytes ONLY — metadata rows and the `(source_url, variant)` index row
/// stay, so the next request re-fetches while the source lives — then
/// accounting drops the bytes and counts the eviction. Terminates after
/// [`MAX_EVICTION_ROUNDS`] re-reads; whatever still does not fit fails
/// closed at the reservation.
///
/// Pool/refcount semantics (Phase 1 P0): the delete targets the cache pool
/// (`services.cache_blobs`, a separate directory tree) and therefore can
/// never remove durable bytes — durable rows live in `services.blobs` and
/// are unreachable here. No zero-refcount precondition applies: eviction
/// keeps the referencing rows by design (they become byte-less and
/// re-fetchable), so a positive refcount is the normal case, not a reason
/// to skip. What eviction MUST hold is the shared lifecycle gate plus the
/// per-blob-key lock across the byte delete + accounting commit, serializing
/// against concurrent fetches, deletes, and the exclusive orphan sweep.
pub fn ensure_cache_budget(
    services: &AppServices,
    need_bytes: u64,
    class: MediaClass,
) -> Result<(), PortError> {
    let max = services.remote_cache_max_bytes;
    let cap = class_cap_bytes(services, class);
    let ttl_ms = ttl_days_for(services, class) as i64 * 86_400_000;
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
        // TTL-stale first (stable: LRU order kept within each group).
        let mut ordered = candidates;
        ordered.sort_by_key(|rec| {
            let age = now_ms.saturating_sub(rec.fetched_at_ms);
            i64::from(age <= ttl_ms)
        });
        let mut progressed = false;
        for rec in &ordered {
            if !evicted_keys.insert(rec.blob_key.clone()) {
                continue;
            }
            // Bytes ONLY + accounting, under the whole-transition hold:
            // serialized against a concurrent fetch commit or delete for
            // this key. Metadata + index bookkeeping stay for re-fetch.
            let _lifecycle = acquire_media_op("cache_evict")?;
            let _key_lock = acquire_blob_key(&rec.blob_key)?;
            services.cache_blobs.delete(&rec.blob_key)?;
            services.media.cache_accounting_apply(
                -(rec.size_bytes as i64),
                rec.class,
                -(rec.size_bytes as i64),
                0,
                0,
                0,
                1,
                0,
            )?;
            progressed = true;
            break; // re-read accounting before the next eviction.
        }
        if !progressed {
            break;
        }
    }
    Ok(())
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
    use crate::application::ports::MediaClass;

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
}
