use crate::application::commands::AppServices;
use crate::application::ports::{
    ActivityDedup, BlobStore, CacheStats, InsertMediaInput, MediaClass, MediaStore,
    NoopStreamingSink, PortError, RemoteCacheRecord, TimelineCursor,
};
use crate::domain::id::{AidxGenerator, Clock, IdGenerator, ManualClock};
use crate::domain::media::{MediaMeta, MediaProperties};
use crate::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use crate::media::storage::FsBlobStore;
use crate::persistence::redb::RedbStore;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
pub(crate) struct FakeMedia {
    rows: Mutex<HashMap<String, MediaMeta>>,
    index: Mutex<HashMap<(String, String), RemoteCacheRecord>>,
    stats: Mutex<CacheStats>,
}

impl MediaStore for FakeMedia {
    fn insert_metadata(&self, input: InsertMediaInput) -> Result<MediaMeta, PortError> {
        let meta = MediaMeta {
            id: input.id.unwrap_or_else(|| "mid00000000000001".to_string()),
            owner_id: input.owner_id,
            name: input.name,
            mime: input.mime,
            size_bytes: input.size_bytes,
            hash: input.hash,
            blob_key: input.blob_key,
            blurhash: input.blurhash,
            properties: input.properties,
            is_sensitive: input.is_sensitive,
            is_link: input.is_link,
            url: input.url,
            thumbnail_url: input.thumbnail_url,
            comment: input.comment,
            source_url: input.source_url,
            // `InsertMediaInput` carries no variant/class/evicted
            // fields: the fake stores the documented defaults
            // (agent A follow-up decides the real backend mapping).
            variant: "original".to_string(),
            media_class: MediaClass::Preview.as_str().to_string(),
            evicted: false,
        };
        self.rows
            .lock()
            .unwrap()
            .insert(meta.id.clone(), meta.clone());
        Ok(meta)
    }
    fn get(&self, id: &str) -> Result<Option<MediaMeta>, PortError> {
        Ok(self.rows.lock().unwrap().get(id).cloned())
    }
    fn find_by_owner_and_hash(
        &self,
        owner_id: &str,
        hash: &str,
    ) -> Result<Option<MediaMeta>, PortError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .values()
            .find(|m| m.owner_id.as_deref() == Some(owner_id) && m.hash == hash)
            .cloned())
    }
    fn find_by_source_url(&self, source_url: &str) -> Result<Vec<MediaMeta>, PortError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|m| m.source_url.as_deref() == Some(source_url))
            .cloned()
            .collect())
    }
    fn delete_metadata(&self, id: &str) -> Result<(), PortError> {
        self.rows.lock().unwrap().remove(id);
        Ok(())
    }
    fn list_by_owner(&self, owner_id: &str) -> Result<Vec<MediaMeta>, PortError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|m| m.owner_id.as_deref() == Some(owner_id))
            .cloned()
            .collect())
    }
    fn list_all_media(&self, cursor: &TimelineCursor) -> Result<Vec<MediaMeta>, PortError> {
        let mut out: Vec<MediaMeta> = self.rows.lock().unwrap().values().cloned().collect();
        out.sort_by(|a, b| b.id.cmp(&a.id));
        // Exclusive cursor bounds, mirroring the production adapter
        // contract (newest-first, `until_id` walks to an empty page).
        out.retain(|m| {
            if let Some(since) = cursor.since_id.as_deref() {
                if m.id.as_str() <= since {
                    return false;
                }
            }
            if let Some(until) = cursor.until_id.as_deref() {
                if m.id.as_str() >= until {
                    return false;
                }
            }
            true
        });
        let limit = if cursor.limit == 0 { 10 } else { cursor.limit };
        out.truncate(limit);
        Ok(out)
    }
    fn durable_bytes(&self, owner_id: &str) -> Result<u64, PortError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|m| m.owner_id.as_deref() == Some(owner_id))
            .map(|m| m.size_bytes)
            .sum())
    }
    fn blob_refcount(&self, blob_key: &str) -> Result<u64, PortError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|m| m.blob_key == blob_key)
            .count() as u64)
    }
    fn remote_cache_lookup(
        &self,
        source_url: &str,
        variant: &str,
    ) -> Result<Option<RemoteCacheRecord>, PortError> {
        Ok(self
            .index
            .lock()
            .unwrap()
            .get(&(source_url.to_string(), variant.to_string()))
            .cloned())
    }
    fn remote_cache_insert(&self, rec: &RemoteCacheRecord) -> Result<(), PortError> {
        self.index
            .lock()
            .unwrap()
            .insert((rec.source_url.clone(), rec.variant.clone()), rec.clone());
        Ok(())
    }
    fn remote_cache_touch(
        &self,
        source_url: &str,
        variant: &str,
        now_ms: i64,
    ) -> Result<(), PortError> {
        if let Some(rec) = self
            .index
            .lock()
            .unwrap()
            .get_mut(&(source_url.to_string(), variant.to_string()))
        {
            rec.last_access_ms = now_ms;
            rec.access_count += 1;
        }
        Ok(())
    }
    fn remote_cache_evict_candidates(
        &self,
        limit: usize,
    ) -> Result<Vec<RemoteCacheRecord>, PortError> {
        let mut out: Vec<RemoteCacheRecord> =
            self.index.lock().unwrap().values().cloned().collect();
        out.sort_by_key(|r| r.last_access_ms);
        out.truncate(limit);
        Ok(out)
    }
    fn remote_cache_remove(&self, source_url: &str, variant: &str) -> Result<(), PortError> {
        self.index
            .lock()
            .unwrap()
            .remove(&(source_url.to_string(), variant.to_string()));
        Ok(())
    }
    fn cache_stats(&self) -> Result<CacheStats, PortError> {
        Ok(self.stats.lock().unwrap().clone())
    }
    fn cache_accounting_apply(
        &self,
        total_delta: i64,
        class: MediaClass,
        class_delta: i64,
        hits: u64,
        misses: u64,
        bytes_fetched: u64,
        evictions: u64,
        failed_origins: u64,
    ) -> Result<(), PortError> {
        let mut s = self.stats.lock().unwrap();
        let apply = |v: u64, d: i64| -> u64 {
            if d >= 0 {
                v.saturating_add(d as u64)
            } else {
                v.saturating_sub((-d) as u64)
            }
        };
        s.total_bytes = apply(s.total_bytes, total_delta);
        match class {
            MediaClass::Avatar => s.avatar_bytes = apply(s.avatar_bytes, class_delta),
            MediaClass::Preview => s.preview_bytes = apply(s.preview_bytes, class_delta),
            MediaClass::Full => s.full_bytes = apply(s.full_bytes, class_delta),
            // AudioVideo has no dedicated counter: fold any residual
            // class delta into the total (callers pass equal deltas).
            MediaClass::AudioVideo => {
                s.total_bytes = apply(s.total_bytes, class_delta.saturating_sub(total_delta));
            }
        }
        s.hits += hits;
        s.misses += misses;
        s.bytes_fetched += bytes_fetched;
        s.evictions += evictions;
        s.failed_origins += failed_origins;
        Ok(())
    }
    fn reserve_cache_bytes(&self, bytes: u64) -> Result<bool, PortError> {
        let mut s = self.stats.lock().unwrap();
        // Fake cap: 1 MiB, to mirror a real global-cap check.
        if s.total_bytes
            .saturating_add(s.in_flight_bytes)
            .saturating_add(bytes)
            > (1 << 20)
        {
            return Ok(false);
        }
        s.in_flight_bytes = s.in_flight_bytes.saturating_add(bytes);
        Ok(true)
    }
    fn release_cache_reservation(&self, bytes: u64) -> Result<(), PortError> {
        let mut s = self.stats.lock().unwrap();
        s.in_flight_bytes = s.in_flight_bytes.saturating_sub(bytes);
        Ok(())
    }
}

/// Scratch temp dir (no `tempfile` dev-dependency; mirrors the other
/// test modules' `std::env::temp_dir` pattern).
pub(crate) fn scratch_dir(tag: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pinetree-reconcile-{}-{tag}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// `AppServices` with a fake media store and real filesystem blob pools.
/// Every other store is the production redb backend, which the
/// reconciler never touches — this keeps `AppServices` fully populated
/// without hand-rolled stubs for 20+ stores.
pub(crate) fn fake_services() -> (AppServices, Arc<FakeMedia>, std::path::PathBuf) {
    let dir = scratch_dir("svc");
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_760_400_000_000));
    let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
    let fake = Arc::new(FakeMedia::default());
    // Reuse the production redb backend for the stores the reconciler
    // never touches, so `AppServices` stays fully populated.
    let store = crate::persistence::redb::RedbStore::open(
        dir.join("test.redb"),
        clock.clone(),
        ids.clone(),
    )
    .unwrap();
    let services = AppServices {
        clock,
        ids,
        accounts: store.clone(),
        notes: store.clone(),
        follows: store.clone(),
        blocks: store.clone(),
        mutes: store.clone(),
        reactions: store.clone(),
        favorites: store.clone(),
        emojis: store.clone(),
        antennas: store.clone(),
        antenna_notes: store.clone(),
        polls: store.clone(),
        jobs: store.clone(),
        media: fake.clone(),
        blobs: Arc::new(FsBlobStore::new(dir.join("blobs_durable")).unwrap()),
        cache_blobs: Arc::new(FsBlobStore::new(dir.join("blobs_cache")).unwrap()),
        notifications: store.clone(),
        auth: store.clone(),
        streaming: Arc::new(NoopStreamingSink),
        dedup: store.clone() as Arc<dyn ActivityDedup>,
        host_caps: store.clone(),
        moderation: store.clone(),
        instance_settings: store.clone(),
        invites: store.clone(),
        abuse_reports: store.clone(),
        lists: store.clone(),
        signin: store.clone(),
        password_resets: store.clone(),
        registry: store.clone(),
        clips: store.clone(),
        pages: store.clone(),
        folders: store.clone(),
        system_webhooks: store.clone(),
        user_webhooks: store.clone(),
        abuse_recipients: store.clone(),
        memos: store.clone(),
        thread_mutes: store.clone(),
        federation_http: SafeHttpClient::new(SafeHttpConfig {
            allow_private_ips: true,
            ..Default::default()
        }),
        public_url: "https://pinetree.local".to_string(),
        max_file_size: 4096,
        preserved_usernames: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        note_draft_limit: crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
        scheduled_note_limit: crate::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
        pin_limit: crate::domain::pinning::DEFAULT_PIN_LIMIT,
        default_like: None,
        drive_capacity_per_user_bytes: 1 << 30,
        remote_cache_max_bytes: 512 << 20,
        remote_cache_avatar_bytes: 128 << 20,
        remote_cache_preview_bytes: 256 << 20,
        remote_cache_full_bytes: 128 << 20,
        remote_cache_mode: crate::config::RemoteCacheMode::OnDemand,
        remote_cache_avatar_ttl_days: 30,
        remote_cache_preview_ttl_days: 7,
        remote_cache_full_ttl_days: 3,
        remote_cache_warming_enabled: false,
    };
    (services, fake, dir)
}

pub(crate) fn insert_row(
    services: &AppServices,
    id: &str,
    owner: Option<&str>,
    blob_key: &str,
    size: u64,
    source_url: Option<&str>,
) -> MediaMeta {
    services
        .media
        .insert_metadata(InsertMediaInput {
            id: Some(id.to_string()),
            owner_id: owner.map(ToOwned::to_owned),
            name: "f.bin".to_string(),
            mime: "application/octet-stream".to_string(),
            size_bytes: size,
            hash: "0cc175b9c0f1b6a831c399e269772661".to_string(),
            blob_key: blob_key.to_string(),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: owner.is_none(),
            url: format!("https://pinetree.local/files/{id}"),
            thumbnail_url: None,
            comment: None,
            source_url: source_url.map(ToOwned::to_owned),
            max_file_size: 4096,
        })
        .unwrap()
}

// --- Minimal in-memory MediaStore fake (trait-complete) ---
