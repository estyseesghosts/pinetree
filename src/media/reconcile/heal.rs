use crate::application::commands::AppServices;
use crate::application::ports::PortError;
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

/// Sweep one `objects/` pool directory (`<pool>/objects/xx/<key>` shards
/// with `<key>.len` sidecars): delete every key file — plus its sidecar —
/// not in `live_keys`. Returns the number of object keys removed.
///
/// Safety: only alphanumeric keys of length 4–128 are eligible (the
/// `FsBlobStore` key shape), `.len` sidecars are never matched directly
/// (they die with their object), non-directories and unreadable entries
/// are skipped, a missing pool directory sweeps nothing, and `NotFound`
/// races on delete are ignored. Metadata is never touched here.
///
/// Test-only convenience wrapper: assumes a COMPLETE live-key set (as if
/// `complete == true`). Production wiring must call
/// [`sweep_orphan_blobs_checked`] with the completion signal from a full
/// [`crate::media::reconcile::collect_live_blob_keys`] walk; a partial set
/// must refuse, never sweep.
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
    // Batch F gate (Batch H one-snippet): pool deletions wait behind an
    // active backup so a concurrent snapshot/copy can never lose bytes the
    // bundle's metadata references. Held for the whole sweep.
    let _media_gate = crate::media::storage::acquire_media_op("sweep")?;
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
