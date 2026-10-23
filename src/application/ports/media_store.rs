use super::note_store::TimelineCursor;
use crate::domain::id::PortError;
use crate::domain::media::MediaClass;
use crate::domain::media::{MediaMeta, MediaProperties};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InsertMediaInput {
    pub id: Option<String>,
    pub owner_id: Option<String>,
    pub name: String,
    pub mime: String,
    pub size_bytes: u64,
    pub hash: String,
    /// SHA-256 hex of the stored bytes: the `BlobStore` content address
    /// (Phase 2.6 slice 8). `hash` stays the MD5 (packer `md5`, per-owner
    /// dedup key). Both are computed by the caller on upload (local +
    /// remote paths).
    pub blob_key: String,
    pub blurhash: Option<String>,
    pub properties: MediaProperties,
    pub is_sensitive: bool,
    pub is_link: bool,
    pub url: String,
    pub thumbnail_url: Option<String>,
    pub comment: Option<String>,
    /// Remote origin URL for cache rows (`None` for local uploads). With
    /// the owning note's `file_ids` this is the idempotent-insert key.
    pub source_url: Option<String>,
    pub max_file_size: u64,
}

/// One remote attachment carried inside a `RemoteMediaFetch` job payload.
/// Mirrors the ActivityPub `ParsedAttachment` shape in contract-stable
/// form so the application layer never imports ActivityPub types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteMediaAttachment {
    pub url: String,
    pub media_type: Option<String>,
    pub name: Option<String>,
    pub is_sensitive: bool,
    pub blurhash: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

/// One `(source_url, variant)` remote-cache index row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteCacheRecord {
    pub source_url: String,
    pub variant: String,
    pub media_id: String,
    pub blob_key: String,
    pub size_bytes: u64,
    pub class: MediaClass,
    pub fetched_at_ms: i64,
    pub last_access_ms: i64,
    pub access_count: u64,
    /// Monotonic per-row change counter (adversarial-2 slice 2, finding 5
    /// guard half). The store bumps it on every mutation of the row
    /// (insert, touch, eviction transition); eviction compares it — never
    /// timestamps alone — so a same-millisecond touch still counts as
    /// newer. Old rows decode as `0`.
    #[serde(default)]
    pub generation: u64,
    /// True once the bytes for this key have been charged off. Old rows
    /// decode as `false` (present). Set by `commit_cache_eviction`; a
    /// refetch clears it when bytes are restored.
    #[serde(default)]
    pub bytes_absent: bool,
}

/// Outcome of `commit_cache_eviction`: one physical key transitions once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheEvictionOutcome {
    /// Bytes were charged off in this call.
    Evicted { debited_bytes: u64 },
    /// Key was already debited (`size_bytes == 0` or every index row
    /// already `bytes_absent`); no accounting moved.
    AlreadyAbsent,
    /// A durable row still references the key; the caller must leave the
    /// blob alone and charge nothing.
    SkippedDurableRef,
}

/// Shell-free observability snapshot (`docs/remote-media-cache-plan.md`
/// §5.6): totals plus per-class breakdown and hit/miss counters.
///
/// Class rule (adversarial-2 slice 5): `AudioVideo` has NO class counter —
/// it is governed by `total_bytes` alone, in every backend, in apply and
/// evict alike. `full_bytes` counts `Full` only; stored snapshots that
/// predate the rule may still hold AudioVideo charges inside `full_bytes`
/// (left alone, never re-attributed by guessing).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct CacheStats {
    pub total_bytes: u64,
    pub avatar_bytes: u64,
    pub preview_bytes: u64,
    pub full_bytes: u64,
    pub in_flight_bytes: u64,
    pub hits: u64,
    pub misses: u64,
    pub bytes_fetched: u64,
    pub evictions: u64,
    pub failed_origins: u64,
}

pub trait MediaStore: Send + Sync {
    fn insert_metadata(&self, input: InsertMediaInput) -> Result<MediaMeta, PortError>;
    fn get(&self, id: &str) -> Result<Option<MediaMeta>, PortError>;
    fn find_by_owner_and_hash(
        &self,
        owner_id: &str,
        hash: &str,
    ) -> Result<Option<MediaMeta>, PortError>;
    /// All media rows cached from `source_url` (Phase 2.6 slice 7). The
    /// worker intersects this with the note's `file_ids` to skip
    /// already-attached URLs without any network fetch.
    fn find_by_source_url(&self, source_url: &str) -> Result<Vec<MediaMeta>, PortError>;
    fn delete_metadata(&self, id: &str) -> Result<(), PortError>;
    /// Newest-first metadata rows owned by `owner_id` (Phase 3
    /// `drive/files`). Ordering is by row id (aidx ids are time-ordered).
    fn list_by_owner(&self, owner_id: &str) -> Result<Vec<MediaMeta>, PortError>;
    /// Newest-first metadata rows across all owners (Batch N WS6
    /// `admin/drive/files`). Paging contract: newest-first, exclusive
    /// `until_id`, `limit` counts matching rows bounded by
    /// `MAX_MEDIA_PAGE_ROWS`. Only an empty page terminates a scan; a short
    /// page is never end-of-scan (concurrent deletes / exact multiples).
    fn list_all_media(&self, cursor: &TimelineCursor) -> Result<Vec<MediaMeta>, PortError>;
    /// Durable bytes currently charged to `owner_id` (sum over owned rows).
    /// Test-double default: no rows.
    fn durable_bytes(&self, _owner_id: &str) -> Result<u64, PortError> {
        Ok(0)
    }
    /// Total stored drive-file rows, local and remote (`admin/drive`
    /// `filesCount` parity). Defaults to `StorageUnavailable` like
    /// [`NoteStore::count_notes`]; durable backends must override it.
    fn count_drive_files(&self) -> Result<u64, PortError> {
        Err(PortError::StorageUnavailable(
            "MediaStore::count_drive_files not implemented".to_string(),
        ))
    }
    /// Live metadata references to `blob_key` (0 ⇒ bytes may be deleted).
    /// Test-double default: unreferenced.
    fn blob_refcount(&self, _blob_key: &str) -> Result<u64, PortError> {
        Ok(0)
    }
    /// Overwrite the stored `BLOB_REFCOUNT` row (backfill/reconciler).
    /// Default no-op for test doubles; durable backends must override.
    fn set_blob_refcount(&self, _blob_key: &str, _count: u64) -> Result<(), PortError> {
        Ok(())
    }
    /// Overwrite the stored `DURABLE_BYTES_BY_OWNER` row
    /// (backfill/reconciler). Default no-op; durable backends override.
    fn set_durable_bytes(&self, _owner_id: &str, _bytes: u64) -> Result<(), PortError> {
        Ok(())
    }
    /// Atomic durable insert: quota check + metadata write + refcount inc +
    /// owner-bytes adjust in one transaction. Breach ⇒
    /// `Validation("DRIVE_CAPACITY_EXCEEDED")`.
    fn insert_metadata_with_durable_quota(
        &self,
        input: InsertMediaInput,
        quota_bytes: u64,
    ) -> Result<MediaMeta, PortError> {
        let _ = quota_bytes;
        self.insert_metadata(input)
    }
    /// Atomic media delete: remove the row, decrement refcount and durable
    /// owner bytes in one transaction. Returns the removed row plus the
    /// remaining reference count for its blob key (`None` when no row).
    fn delete_media_record(&self, id: &str) -> Result<Option<(MediaMeta, u64)>, PortError> {
        let meta = self.get(id)?;
        if let Some(meta) = meta {
            self.delete_metadata(&meta.id)?;
            let remaining = self.blob_refcount(&meta.blob_key)?;
            Ok(Some((meta, remaining)))
        } else {
            Ok(None)
        }
    }
    /// Remote-cache index lookup by `(source_url, variant)`.
    /// Test-double defaults: empty index, zero stats, reservations always
    /// succeed. Production backends must override (accounting correctness
    /// depends on it).
    fn remote_cache_lookup(
        &self,
        _source_url: &str,
        _variant: &str,
    ) -> Result<Option<RemoteCacheRecord>, PortError> {
        Ok(None)
    }
    fn remote_cache_insert(&self, _rec: &RemoteCacheRecord) -> Result<(), PortError> {
        Ok(())
    }
    fn remote_cache_touch(
        &self,
        _source_url: &str,
        _variant: &str,
        _now_ms: i64,
    ) -> Result<(), PortError> {
        Ok(())
    }
    /// LRU-ordered eviction candidates (oldest `last_access_ms` first),
    /// at most `limit` rows.
    fn remote_cache_evict_candidates(
        &self,
        _limit: usize,
    ) -> Result<Vec<RemoteCacheRecord>, PortError> {
        Ok(Vec::new())
    }
    /// Charged-row pagination for eviction (adversarial-2 slice 2): at most
    /// `limit` CHARGED rows (`size_bytes > 0 && !bytes_absent`) starting at
    /// `offset`, oldest `last_access_ms` first. Tombstones never occupy
    /// this window. The eviction loop pages `offset` forward until the
    /// budget fits or a page comes back short/empty (eligible set
    /// exhausted). Test-double default: empty (no candidates).
    fn remote_cache_charged_page(
        &self,
        _offset: usize,
        _limit: usize,
    ) -> Result<Vec<RemoteCacheRecord>, PortError> {
        Ok(Vec::new())
    }
    /// Tombstone pagination for the retention pass (adversarial-2 slice 2,
    /// finding 9): at most `limit` zero-byte/absent rows starting at
    /// `offset`, oldest `last_access_ms` first. The retention consumer
    /// (never eviction) filters by class TTL and live-media reference.
    /// Test-double default: empty (nothing to retain).
    fn remote_cache_tombstone_page(
        &self,
        _offset: usize,
        _limit: usize,
    ) -> Result<Vec<RemoteCacheRecord>, PortError> {
        Ok(Vec::new())
    }
    /// Dangling-index pagination for the reconciler (adversarial-2 slice 4,
    /// finding E): at most `limit` index rows whose media row is GONE,
    /// starting at `offset`, in `(source_url, variant)` key order — never
    /// LRU order, so a newer dangling row is visited even while old live
    /// rows or tombstones would fill an oldest-N window. Every returned
    /// row is still only a candidate: the caller re-reads the exact row
    /// under the lifecycle gate plus the per-key lock and removes it with
    /// [`MediaStore::remote_cache_remove_if`]. Test-double default: empty
    /// (nothing dangling).
    fn remote_cache_dangling_page(
        &self,
        _offset: usize,
        _limit: usize,
    ) -> Result<Vec<RemoteCacheRecord>, PortError> {
        Ok(Vec::new())
    }
    /// Conditional index-row removal (adversarial-2 slice 2 retention; the
    /// slice 4 dangling path reuses it): removes `(source_url, variant)`
    /// only when the current row still points at (`media_id`, `blob_key`).
    /// Returns `true` when a row was removed. A concurrent refetch installs
    /// a new `media_id`, so the CAS fails and the fresh row survives.
    fn remote_cache_remove_if(
        &self,
        _source_url: &str,
        _variant: &str,
        _media_id: &str,
        _blob_key: &str,
    ) -> Result<bool, PortError> {
        Ok(false)
    }
    fn remote_cache_remove(&self, _source_url: &str, _variant: &str) -> Result<(), PortError> {
        Ok(())
    }
    fn cache_stats(&self) -> Result<CacheStats, PortError> {
        Ok(CacheStats::default())
    }
    /// One physical-key eviction transition in a single write transaction.
    /// The caller holds `acquire_media_op` + `acquire_blob_key` and has
    /// already deleted the cache-pool bytes (or verified absence).
    /// The backend debits the authoritative per-key accounting class
    /// recorded at first charge (adversarial-2 slice 5), not necessarily
    /// `class`: callers pass their best-known class (e.g. the max alias)
    /// as the pre-migration fallback. `AudioVideo` never moves a class
    /// counter — only `total_bytes` — on every backend.
    fn commit_cache_eviction(
        &self,
        blob_key: &str,
        size_bytes: u64,
        class: MediaClass,
        index_keys: &[(String, String)],
    ) -> Result<CacheEvictionOutcome, PortError>;
    /// Every index row still pointing at `blob_key` (bounded to matching
    /// rows only, never a full-table clone).
    fn cache_entries_for_blob_key(
        &self,
        blob_key: &str,
    ) -> Result<Vec<RemoteCacheRecord>, PortError>;
    /// Apply accounting deltas (all saturating). `total_delta`/`class_delta`
    /// are signed (eviction/release negative, insert positive).
    /// `AudioVideo` moves `total_bytes` only: `class_delta` for that class
    /// is ignored on every backend (adversarial-2 slice 5).
    fn cache_accounting_apply(
        &self,
        _total_delta: i64,
        _class: MediaClass,
        _class_delta: i64,
        _hits: u64,
        _misses: u64,
        _bytes_fetched: u64,
        _evictions: u64,
        _failed_origins: u64,
    ) -> Result<(), PortError> {
        Ok(())
    }
    /// In-flight byte reservation under the global cap. Returns `false`
    /// (no reservation made) when `bytes` would exceed the cap.
    fn reserve_cache_bytes(&self, _bytes: u64) -> Result<bool, PortError> {
        Ok(true)
    }
    fn release_cache_reservation(&self, _bytes: u64) -> Result<(), PortError> {
        Ok(())
    }
    /// Persist the global cache cap consulted by `reserve_cache_bytes`.
    /// Backends without persistent limits may no-op.
    fn set_cache_cap(&self, _cap_bytes: u64) -> Result<(), PortError> {
        Ok(())
    }
}
