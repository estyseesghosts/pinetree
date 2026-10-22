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
}

/// Shell-free observability snapshot (`docs/remote-media-cache-plan.md`
/// §5.6): totals plus per-class breakdown and hit/miss counters.
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
    /// `admin/drive/files`). Backends return rows in any order; the
    /// application op sorts newest-first and pages.
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
    fn remote_cache_remove(&self, _source_url: &str, _variant: &str) -> Result<(), PortError> {
        Ok(())
    }
    fn cache_stats(&self) -> Result<CacheStats, PortError> {
        Ok(CacheStats::default())
    }
    /// Apply accounting deltas (all saturating). `total_delta`/`class_delta`
    /// are signed (eviction/release negative, insert positive).
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
