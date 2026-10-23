use crate::application::commands::AppServices;
use crate::application::ports::{CacheEvictionOutcome, CacheStats, MediaClass, PortError};
use crate::media::storage;
use std::collections::HashSet;

/// Bound on eviction rounds per [`ensure_cache_budget`] call: the loop
/// re-reads accounting after every evicted candidate, so a backend whose
/// counters never move (or candidates that never shrink the total) cannot
/// spin forever — the fetch then fails closed at the reservation.
const MAX_EVICTION_ROUNDS: usize = 64;

/// Charged rows read per eviction round. Tombstones are filtered
/// store-side, so the window always holds live candidates; when a whole
/// window proves un-evictable (concurrent touches, durable references,
/// replaced bytes) the loop advances past it instead of retrying the same
/// rows.
const CHARGED_PAGE_SIZE: usize = 32;

/// Tombstone rows examined per [`ensure_cache_budget`] call by the
/// retention pass. One bounded page: steady-state cleanup amortized across
/// pressure events, never a full-history walk.
const TOMBSTONE_RETENTION_SCAN: usize = 64;

/// Listing-time fingerprint of one physical blob key: the COMPLETE alias
/// set resolved through the blob-key index at listing time — never just
/// the aliases that happened to fall inside the candidate window.
struct KeyCandidate {
    blob_key: String,
    stale: bool,
    min_last_access_ms: i64,
    baseline: Vec<AliasBaseline>,
}

/// One alias row as listed: identity plus every change indicator the
/// locked re-read compares. Generation first — timestamps alone cannot
/// see a same-millisecond touch.
struct AliasBaseline {
    source_url: String,
    variant: String,
    media_id: String,
    blob_key: String,
    generation: u64,
    last_access_ms: i64,
    fetched_at_ms: i64,
}

/// Ensure `need_bytes` for `class` fit under the global cap and the class
/// cap with borrowing (plan §5): a class may borrow unused global room, so
/// eviction only triggers while `total + need > max`, or while the class
/// exceeds its cap *plus* the currently unused global slack.
///
/// Candidates are CHARGED physical blob keys (`size_bytes > 0 &&
/// !bytes_absent`), paged oldest-first through
/// [`MediaStore::remote_cache_charged_page`](crate::application::ports::MediaStore::remote_cache_charged_page).
/// Tombstones stay for refetch but never occupy the candidate window, and
/// ordering uses EACH key's own stalest alias (each alias under its OWN
/// class TTL — Avatar 30d, Preview 7d, Full 3d, AudioVideo always stale —
/// never the incoming reservation's class). TTL only orders: when nothing
/// stale is eligible, fresh keys still evict oldest-first so the budget can
/// fit `need_bytes`.
///
/// Every candidate is re-validated under the whole-transition hold (see
/// [`try_evict_snapshot`]): the complete listed alias set is compared
/// against the locked re-read plus the live byte length, so a concurrent
/// touch, refetch, alias add, or eviction wins over a stale listing. Each
/// eviction deletes cache-pool bytes ONCE per physical key and then
/// transitions the shared rows through `commit_cache_eviction` (exactly
/// one accounting move per key, idempotent across runs). A stuck window
/// (every key skipped) advances the page offset instead of spinning;
/// termination is guaranteed by [`MAX_EVICTION_ROUNDS`], and whatever
/// still does not fit fails closed at the reservation.
///
/// A bounded tombstone-retention pass runs first whenever the budget is
/// already exceeded (see [`retain_expired_tombstones`]): zero-byte rows
/// older than their class TTL whose media row is already gone are
/// removed — never a tombstone a live media row still references, and
/// never any bytes (these rows are already absent).
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
    if is_over_budget(max, cap, &services.media.cache_stats()?, need_bytes, class) {
        retain_expired_tombstones(services, now_ms)?;
    }
    let mut visited_keys: HashSet<String> = HashSet::new();
    let mut offset: usize = 0;
    for _ in 0..MAX_EVICTION_ROUNDS {
        let stats = services.media.cache_stats()?;
        if !is_over_budget(max, cap, &stats, need_bytes, class) {
            return Ok(());
        }
        let page = services
            .media
            .remote_cache_charged_page(offset, CHARGED_PAGE_SIZE)?;
        if page.is_empty() {
            break;
        }
        // Distinct keys in page (LRU) order; keys already tried this call
        // are filtered BEFORE the alias-set load so a stuck window costs
        // no extra index reads. Keys are marked tried only when the
        // eviction below actually runs for them — never for merely
        // appearing in the page — so an early success in one round does
        // not poison the untried remainder.
        let mut keyed: Vec<KeyCandidate> = Vec::new();
        for snapshot in &page {
            if visited_keys.contains(&snapshot.blob_key)
                || keyed.iter().any(|c| c.blob_key == snapshot.blob_key)
            {
                continue;
            }
            if let Some(candidate) = snapshot_key_candidate(services, now_ms, &snapshot.blob_key)? {
                keyed.push(candidate);
            }
        }
        // Stale-first (each key under its stalest alias's own class TTL),
        // oldest use within each group, then key for determinism.
        keyed.sort_by(|a, b| {
            (!a.stale)
                .cmp(&(!b.stale))
                .then_with(|| a.min_last_access_ms.cmp(&b.min_last_access_ms))
                .then_with(|| a.blob_key.cmp(&b.blob_key))
        });
        let mut progressed = false;
        for candidate in &keyed {
            visited_keys.insert(candidate.blob_key.clone());
            if try_evict_snapshot(services, candidate)? {
                progressed = true;
                break; // re-read accounting before the next eviction.
            }
        }
        if !progressed {
            // Whole window tried and nothing moved (touched, durable-held,
            // or replaced keys): slide past it. A short next page ends the
            // scan; the round cap bounds it regardless.
            offset = offset.saturating_add(page.len());
        }
    }
    Ok(())
}

/// Global/class pressure predicate (slice 1, finding A): global pressure
/// counts in-flight reservations, not just committed bytes, so a burst of
/// concurrent fetches evicts (or fails closed) before over-admitting.
fn is_over_budget(
    max: u64,
    cap: u64,
    stats: &CacheStats,
    need_bytes: u64,
    class: MediaClass,
) -> bool {
    // Class slack borrows only unused global room *after* in-flight
    // reservations (L1): `max - total - in_flight`, fail-closed at 0, so
    // two concurrent class reservers cannot jointly exceed the cap plus
    // slack. The global check already counted `in_flight`; class
    // isolation must too.
    let slack = max
        .saturating_sub(stats.total_bytes)
        .saturating_sub(stats.in_flight_bytes);
    let allowance = cap.saturating_add(slack);
    let over_global = stats
        .total_bytes
        .saturating_add(stats.in_flight_bytes)
        .saturating_add(need_bytes)
        > max;
    let over_class = class_bytes_in(stats, class).saturating_add(need_bytes) > allowance;
    over_global || over_class
}

/// Resolve the complete current alias set for one physical key (through
/// the blob-key index, never the listing window) and fingerprint it for
/// the locked re-read. Returns `None` when the key settled between the
/// page read and now (evicted or restored away): nothing charged is left
/// to fingerprint, so the caller simply moves on.
fn snapshot_key_candidate(
    services: &AppServices,
    now_ms: i64,
    blob_key: &str,
) -> Result<Option<KeyCandidate>, PortError> {
    let entries = services.media.cache_entries_for_blob_key(blob_key)?;
    if entries.is_empty() || entries.iter().all(|e| e.bytes_absent || e.size_bytes == 0) {
        return Ok(None);
    }
    let mut baseline = Vec::with_capacity(entries.len());
    let mut stale = false;
    let mut min_last_access_ms = i64::MAX;
    for entry in &entries {
        baseline.push(AliasBaseline {
            source_url: entry.source_url.clone(),
            variant: entry.variant.clone(),
            media_id: entry.media_id.clone(),
            blob_key: entry.blob_key.clone(),
            generation: entry.generation,
            last_access_ms: entry.last_access_ms,
            fetched_at_ms: entry.fetched_at_ms,
        });
        min_last_access_ms = min_last_access_ms.min(entry.last_access_ms);
        if !stale
            && cache_entry_stale(
                ttl_days_for(services, entry.class),
                now_ms,
                entry.fetched_at_ms,
            )
        {
            stale = true;
        }
    }
    Ok(Some(KeyCandidate {
        blob_key: blob_key.to_string(),
        stale,
        min_last_access_ms,
        baseline,
    }))
}

/// Drop tombstone index rows that are safe to forget: zero-byte/absent,
/// older than their OWN class TTL, and already missing their media row.
/// Rows a live media row still references are explicitly skipped — they
/// are the refetch anchors. No bytes are deleted here; every row in this
/// window is already absent by construction. The conditional remove is the
/// whole race protocol: a concurrent refetch installs a new `media_id`,
/// so the CAS fails and the fresh row survives.
fn retain_expired_tombstones(services: &AppServices, now_ms: i64) -> Result<(), PortError> {
    let page = services
        .media
        .remote_cache_tombstone_page(0, TOMBSTONE_RETENTION_SCAN)?;
    for rec in &page {
        let ttl_ms = ttl_days_for(services, rec.class) as i64 * 86_400_000;
        if now_ms.saturating_sub(rec.last_access_ms) < ttl_ms {
            continue;
        }
        if services.media.get(&rec.media_id)?.is_some() {
            continue;
        }
        let _removed = services.media.remote_cache_remove_if(
            &rec.source_url,
            &rec.variant,
            &rec.media_id,
            &rec.blob_key,
        )?;
    }
    Ok(())
}

/// Re-validate one listed key candidate under the whole-transition hold
/// and evict it when still eligible. Returns `true` only when accounting
/// moved (one debit for the physical key); every other outcome leaves
/// bytes, rows, and counters exactly as found.
///
/// Skip (return `false`) when: no index row references the key anymore;
/// every row is already charged off (`bytes_absent` or size 0); the locked
/// alias set is no longer exactly the listed set (an alias was added or
/// removed under us); any alias changed since the listing — generation
/// first, so a same-millisecond touch still counts as newer — or its
/// identity moved; the live byte length differs from the charged size
/// (replaced/refetched under us, or absent); or a durable row still
/// references the key (side-effect-free probe: committing size 0 mutates
/// nothing on any backend but still reports `SkippedDurableRef`).
///
/// Aliases are NEVER required to have appeared in the candidate window:
/// the baseline is the complete alias set resolved through the blob-key
/// index at listing time. A key shared by any number of URL/variant rows
/// evicts once and debits once.
///
/// When still eligible: delete the cache-pool bytes once, then commit with
/// the size actually on the index (max when rows disagree, never the sum).
/// A failed delete propagates before the commit, so the metadata transition
/// never runs without the byte delete.
fn try_evict_snapshot(services: &AppServices, candidate: &KeyCandidate) -> Result<bool, PortError> {
    // Bytes ONLY + accounting, under the whole-transition hold:
    // serialized against a concurrent fetch commit or delete for this key.
    // Metadata + index bookkeeping stay for re-fetch.
    let _lifecycle = storage::acquire_media_op("cache_evict")?;
    let _key_lock = storage::acquire_blob_key(&candidate.blob_key)?;
    // Re-read the COMPLETE index alias set for this key (never just the
    // rows the listing window happened to contain).
    let entries = services
        .media
        .cache_entries_for_blob_key(&candidate.blob_key)?;
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
    // Alias-set compare-and-swap against the listing baseline. A matching
    // (source_url, variant) row must agree on identity AND change
    // indicators; a row with no baseline match (added under us) or a
    // missing baseline row (removed under us) both skip. Each alias is
    // compared against ITS OWN baseline — never a sibling's.
    if entries.len() != candidate.baseline.len() {
        return Ok(false);
    }
    for entry in &entries {
        let Some(base) = candidate
            .baseline
            .iter()
            .find(|base| base.source_url == entry.source_url && base.variant == entry.variant)
        else {
            return Ok(false);
        };
        if entry.media_id != base.media_id
            || entry.blob_key != base.blob_key
            || entry.generation != base.generation
            || entry.last_access_ms != base.last_access_ms
            || entry.fetched_at_ms != base.fetched_at_ms
        {
            return Ok(false);
        }
    }
    // One charge per physical key: disagreeing rows debit the max
    // previously added, never the sum. The passed class is only the
    // pre-migration fallback: `commit_cache_eviction` (slice 5) debits
    // the authoritative per-key accounting class recorded at first
    // charge whenever one exists.
    let mut charge_bytes = 0u64;
    let mut charge_class = entries[0].class;
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
    match services.cache_blobs.byte_len(&candidate.blob_key)? {
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
        .commit_cache_eviction(&candidate.blob_key, 0, charge_class, &index_keys)?
    {
        CacheEvictionOutcome::SkippedDurableRef => return Ok(false),
        // Unreachable (size 0 never debits); fail closed regardless.
        CacheEvictionOutcome::Evicted { .. } => return Ok(false),
        CacheEvictionOutcome::AlreadyAbsent => {}
    }
    services.cache_blobs.delete(&candidate.blob_key)?;
    match services.media.commit_cache_eviction(
        &candidate.blob_key,
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

/// Per-class cap in bytes (AudioVideo caches on borrowed global room only —
/// plan §5 gives it a 0 MiB persistent allocation: stream/fetch on demand).
pub(crate) fn class_cap_bytes(services: &AppServices, class: MediaClass) -> u64 {
    match class {
        MediaClass::Avatar => services.remote_cache_avatar_bytes,
        MediaClass::Preview => services.remote_cache_preview_bytes,
        MediaClass::Full => services.remote_cache_full_bytes,
        MediaClass::AudioVideo => 0,
    }
}

/// Class bytes as exposed by [`CacheStats`] (adversarial-2 slice 5 one
/// rule: AudioVideo has no dedicated counter — it is governed by the
/// global cap alone, in apply and evict alike — so it reads 0 here while
/// its bytes live in `total_bytes`).
pub(crate) fn class_bytes_in(stats: &CacheStats, class: MediaClass) -> u64 {
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
                // Store-assigned: the insert bumps this to 1.
                generation: 0,
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
        // baseline vs fresh re-read) must skip without moving bytes or
        // counters. The touch below lands at the SAME millisecond as the
        // listing: timestamps alone would miss it, so the generation
        // carries the verdict. A fresh baseline still evicts, exactly once.
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
        let listed = snapshot_key_candidate(&services, 100, key)
            .unwrap()
            .unwrap();
        // Concurrent same-millisecond touch after the listing: the
        // generation moved (1 → 2) while every timestamp stands still.
        services
            .media
            .remote_cache_touch("https://r.example/pic.png", "original", 100)
            .unwrap();
        let touched = services
            .media
            .remote_cache_lookup("https://r.example/pic.png", "original")
            .unwrap()
            .unwrap();
        assert_eq!(touched.last_access_ms, 100);
        assert!(touched.generation > listed.baseline[0].generation);
        assert!(!try_evict_snapshot(&services, &listed).unwrap());
        assert_eq!(services.cache_blobs.get(key).unwrap().unwrap(), bytes);
        assert_eq!(services.media.cache_stats().unwrap().total_bytes, 24);
        // Fresh baseline evicts.
        let fresh = snapshot_key_candidate(&services, 100, key)
            .unwrap()
            .unwrap();
        assert!(try_evict_snapshot(&services, &fresh).unwrap());
        assert!(services.cache_blobs.byte_len(key).unwrap().is_none());
        // Idempotent: a second call moves nothing.
        let stats = services.media.cache_stats().unwrap();
        assert!(!try_evict_snapshot(&services, &fresh).unwrap());
        assert_eq!(services.media.cache_stats().unwrap(), stats);
    }

    #[test]
    fn evict_snapshot_skips_alias_added_after_listing() {
        // Window independence, both directions: the baseline is the
        // complete alias set, so a sharer added after the listing (a
        // concurrent refetch of the same bytes under a new URL) fails the
        // alias-set CAS and protects the key.
        let services = test_services("evict-new-alias", 4096);
        let key = "c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2";
        let bytes = vec![9u8; 24];
        insert_cache_row(
            &services,
            "https://r.example/first.png",
            key,
            &bytes,
            MediaClass::Preview,
            100,
            100,
        );
        let listed = snapshot_key_candidate(&services, 100, key)
            .unwrap()
            .unwrap();
        assert_eq!(listed.baseline.len(), 1);
        // Concurrent fetch installs a second alias for the same bytes
        // (metadata + index rows, like the production sharer path; the
        // helper charges per row while production charges per key — the
        // single debit below is what this test pins).
        insert_cache_row(
            &services,
            "https://r.example/second.png",
            key,
            &bytes,
            MediaClass::Preview,
            100,
            100,
        );
        assert!(!try_evict_snapshot(&services, &listed).unwrap());
        assert_eq!(services.cache_blobs.get(key).unwrap().unwrap(), bytes);
        assert_eq!(services.media.cache_stats().unwrap().total_bytes, 48);
        // Fresh baseline covers both aliases and evicts once.
        let fresh = snapshot_key_candidate(&services, 100, key)
            .unwrap()
            .unwrap();
        assert_eq!(fresh.baseline.len(), 2);
        assert!(try_evict_snapshot(&services, &fresh).unwrap());
        assert!(services.cache_blobs.byte_len(key).unwrap().is_none());
        assert_eq!(services.media.cache_stats().unwrap().total_bytes, 24);
    }
}
