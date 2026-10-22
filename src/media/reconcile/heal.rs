use super::scan::collect_live_blob_keys_under_guard;
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::media::storage::acquire_media_exclusive;
use std::collections::HashSet;
use std::path::Path;

/// Reset a leaked in-flight reservation (crash-heal). Call at startup:
/// with no live fetches running, any `in_flight_bytes` is a leak from a
/// crash between `reserve_cache_bytes` and its matching release. The
/// release is saturating on correct backends, so an already-zero counter
/// is a no-op.
pub fn reset_in_flight(services: &AppServices) -> Result<(), PortError> {
    let stats = services.media.cache_stats()?;
    if stats.in_flight_bytes > 0 {
        services
            .media
            .release_cache_reservation(stats.in_flight_bytes)?;
    }
    Ok(())
}

/// Outcome of one [`sweep_orphan_pools`] run. `complete` is true only when
/// the live-key collection walked the whole table to an empty page before
/// any byte was deleted; a non-complete collection deletes nothing and
/// surfaces `Validation` instead of counts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// Durable-pool (`blobs_durable/objects`) object keys removed.
    pub durable_removed: u64,
    /// Cache-pool (`blobs_cache/objects`) object keys removed.
    pub cache_removed: u64,
    /// The collection covered the whole table (always true on `Ok`; an
    /// interrupted walk returns `Err` before deleting anything).
    pub complete: bool,
}

/// Sweep both pools under ONE exclusive lifecycle barrier: collect the full
/// live-key set and delete unmatched files while holding
/// [`acquire_media_exclusive`](crate::media::storage::acquire_media_exclusive)
/// across both phases. Concurrent whole-transition writers (shared side)
/// either commit before collection — so the sweep sees their rows — or
/// block the sweep until they commit. Either way a key that becomes live
/// can never be deleted as an orphan.
///
/// Each key is swept only from the pool its rows reference (durable keys
/// from the durable pool, cache keys from the cache pool); the pools are
/// separate directory trees, so a cache-pool delete can never touch durable
/// bytes. Any page/read/filesystem failure during collection returns before
/// any byte is deleted; per-file delete races (`NotFound`) are ignored.
/// Metadata is never touched here.
pub fn sweep_orphan_pools(
    services: &AppServices,
    durable_objects_dir: &Path,
    cache_objects_dir: &Path,
) -> Result<SweepReport, PortError> {
    let _barrier = acquire_media_exclusive("sweep_orphan_pools")?;
    let (durable_live, cache_live) = collect_live_blob_keys_under_guard(services)?;
    let durable_removed = sweep_orphan_blobs_under_guard(durable_objects_dir, &durable_live, true)?;
    let cache_removed = sweep_orphan_blobs_under_guard(cache_objects_dir, &cache_live, true)?;
    Ok(SweepReport {
        durable_removed,
        cache_removed,
        complete: true,
    })
}

/// Sweep one `objects/` pool directory (`<pool>/objects/xx/<key>` shards
/// with `<key>.len` sidecars): delete every key file — plus its sidecar —
/// not in `live_keys`. Returns the number of object keys removed.
///
/// Convenience wrapper: assumes a COMPLETE live-key set (as if
/// `complete == true`) and takes the exclusive barrier itself. Production
/// wiring must prefer [`sweep_orphan_pools`], which holds ONE exclusive
/// barrier across collection and both pools; a partial set must refuse via
/// [`sweep_orphan_blobs_checked`], never sweep.
///
/// Safety: only alphanumeric keys of length 4–128 are eligible (the
/// `FsBlobStore` key shape), `.len` sidecars are never matched directly
/// (they die with their object), non-directories and unreadable entries
/// are skipped, a missing pool directory sweeps nothing, and `NotFound`
/// races on delete are ignored. Metadata is never touched here.
pub fn sweep_orphan_blobs(
    objects_dir: &Path,
    live_keys: &HashSet<String>,
) -> Result<u64, PortError> {
    sweep_orphan_blobs_checked(objects_dir, live_keys, true)
}

/// [`sweep_orphan_blobs`] with an explicit completeness gate: `complete`
/// must be true — the result of a fully walked live-key set — or the sweep
/// refuses with `Validation` and deletes nothing. This is the destructive
/// operation's guard against truncated scans: a short page, a capped walk,
/// or any interrupted collection must never feed a delete sweep.
///
/// Barrier: takes the EXCLUSIVE lifecycle side across the whole walk, so a
/// concurrent whole-transition writer (shared side) either committed before
/// the caller's collection — and the key is live — or blocks this sweep
/// until it commits. Callers that already hold the shared side get a
/// fail-closed error (upgrading would self-deadlock); use
/// [`sweep_orphan_pools`] for the collect-and-sweep wiring.
pub fn sweep_orphan_blobs_checked(
    objects_dir: &Path,
    live_keys: &HashSet<String>,
    complete: bool,
) -> Result<u64, PortError> {
    if !complete {
        return Err(PortError::Validation(
            "refusing orphan-blob sweep on an incomplete live-key set; \
             finish the paged collection to an empty page first"
                .to_string(),
        ));
    }
    let _barrier = acquire_media_exclusive("sweep")?;
    sweep_orphan_blobs_under_guard(objects_dir, live_keys, true)
}

/// [`sweep_orphan_blobs_checked`] without barrier acquisition. The caller
/// must hold the exclusive lifecycle barrier
/// ([`acquire_media_exclusive`](crate::media::storage::acquire_media_exclusive))
/// and must have collected `live_keys` under that same hold (see
/// [`collect_live_blob_keys_under_guard`](super::scan::collect_live_blob_keys_under_guard)).
/// `complete` is re-checked here so a direct call with a partial set still
/// refuses.
pub fn sweep_orphan_blobs_under_guard(
    objects_dir: &Path,
    live_keys: &HashSet<String>,
    complete: bool,
) -> Result<u64, PortError> {
    if !complete {
        return Err(PortError::Validation(
            "refusing orphan-blob sweep on an incomplete live-key set; \
             finish the paged collection to an empty page first"
                .to_string(),
        ));
    }
    let shards = match std::fs::read_dir(objects_dir) {
        Ok(shards) => shards,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => {
            return Err(PortError::StorageUnavailable(format!(
                "list orphan-sweep pool dir: {e}"
            )));
        }
    };
    let mut removed: u64 = 0;
    for shard in shards {
        let shard = match shard {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        if !shard.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let entries = match std::fs::read_dir(shard.path()) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => continue,
            };
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".len") {
                continue;
            }
            if live_keys.contains(&name) {
                continue;
            }
            if !(4..=128).contains(&name.len()) || !name.bytes().all(|b| b.is_ascii_alphanumeric())
            {
                continue;
            }
            match std::fs::remove_file(entry.path()) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => {
                    return Err(PortError::StorageUnavailable(format!(
                        "delete orphan blob {name}: {e}"
                    )));
                }
            }
            let mut sidecar = entry.path().into_os_string();
            sidecar.push(".len");
            let _ = std::fs::remove_file(&sidecar);
            removed += 1;
        }
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::super::support::{fake_services, scratch_dir};
    use super::*;
    use crate::application::ports::{BlobStore, MediaStore};
    use crate::media::storage::FsBlobStore;
    use std::collections::HashSet;

    #[test]
    fn sweep_orphan_blobs_removes_only_unlisted_keys() {
        let mut live_keys = HashSet::new();
        let live = "abababababababababababababababababababababababababababababababab";
        let dead = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
        live_keys.insert(live.to_string());
        // Sweep a scratch pool (same `objects/xx/<key>` layout as the
        // production pools).
        let scratch = scratch_dir("sweep");
        let pool = FsBlobStore::new(&scratch).unwrap();
        pool.put(live, b"keep").unwrap();
        pool.put(dead, b"drop").unwrap();
        let objects = scratch.join("objects");
        let removed = sweep_orphan_blobs(&objects, &live_keys).unwrap();
        assert_eq!(removed, 1);
        assert!(pool.get(dead).unwrap().is_none());
        assert_eq!(pool.get(live).unwrap().unwrap(), b"keep");
        // Missing dir sweeps nothing.
        assert_eq!(
            sweep_orphan_blobs(&scratch.join("nope"), &live_keys).unwrap(),
            0
        );
        // Non-key junk and sidecar-only names are never eligible.
        let _ = std::fs::create_dir_all(objects.join("zz"));
        std::fs::write(objects.join("zz").join("!!!not-a-key!!!"), b"x").unwrap();
        std::fs::write(objects.join("zz").join("orphan.len"), b"3").unwrap();
        assert_eq!(sweep_orphan_blobs(&objects, &live_keys).unwrap(), 0);
    }

    #[test]
    fn reset_in_flight_releases_leaked_reservation() {
        let (services, fake, _dir) = fake_services();
        assert!(fake.reserve_cache_bytes(100).unwrap());
        assert_eq!(fake.cache_stats().unwrap().in_flight_bytes, 100);
        reset_in_flight(&services).unwrap();
        assert_eq!(fake.cache_stats().unwrap().in_flight_bytes, 0);
    }
}
