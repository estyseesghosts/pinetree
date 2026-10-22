use super::codec::{decode_json, encode_json, map_redb_err};
use super::note_store::matches_cursor;
use super::store::{BLOB_REFCOUNT, DURABLE_BYTES_BY_OWNER, MEDIA_META, RedbStore};
use crate::application::ports::{
    CacheEvictionOutcome, CacheStats, InsertMediaInput, MediaClass, MediaStore, PortError,
    RemoteCacheRecord, TimelineCursor,
};
use crate::domain::media::MediaMeta;
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};

/// Paging contract (Batch C, `prod-ready.md` §4 C1): the single shared bound
/// on `MediaStore::list_all_media` page sizes. A requested `limit` counts
/// returned matching rows (newest-first, exclusive cursor bounds) and is
/// honored up to this maximum — never silently clamped below it, and a short
/// page is never end-of-scan (callers walk `until_id` to an empty page).
/// Callers (`reconciler`, worker arms) and tests use this constant so the
/// contract stays in one place (`ports.rs` documents the semantics; the
/// value lives here with the adapter that enforces it).
pub const MAX_MEDIA_PAGE_ROWS: usize = 500;
/// Remote-cache index `(source_url, variant)` → JSON [`RemoteCacheRecord`].
pub(crate) const REMOTE_CACHE: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("remote_cache");
/// Cache accounting singleton (`"stats"` → JSON [`CacheStats`],
/// `"cap"` → u64 le global byte cap for in-flight reservations).
pub(crate) const CACHE_ACCOUNTING: TableDefinition<&str, &[u8]> =
    TableDefinition::new("cache_accounting");
impl MediaStore for RedbStore {
    fn insert_metadata(&self, input: InsertMediaInput) -> Result<MediaMeta, PortError> {
        // Quota-unchecked path (metadata updates, remote-cache rows,
        // backfills): `u64::MAX` never breaches, but refcounts and
        // durable totals are still maintained so every writer agrees.
        self.insert_metadata_with_durable_quota(input, u64::MAX)
    }

    fn get(&self, id: &str) -> Result<Option<MediaMeta>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(MEDIA_META).map_err(map_redb_err)?;
        match table.get(id).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn find_by_owner_and_hash(
        &self,
        owner_id: &str,
        hash: &str,
    ) -> Result<Option<MediaMeta>, PortError> {
        // Slice 8 (D6): the dedup key stays the md5 (`MediaMeta.hash`), not
        // the SHA-256 blob address — same bytes → same md5 → existing row.
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(MEDIA_META).map_err(map_redb_err)?;
        for entry in table.iter().map_err(map_redb_err)? {
            let (_, v) = entry.map_err(map_redb_err)?;
            let meta: MediaMeta = decode_json(v.value())?;
            if meta.owner_id.as_deref() == Some(owner_id) && meta.hash == hash {
                return Ok(Some(meta));
            }
        }
        Ok(None)
    }

    fn find_by_source_url(&self, source_url: &str) -> Result<Vec<MediaMeta>, PortError> {
        let target = source_url.trim();
        if target.is_empty() {
            return Ok(Vec::new());
        }
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(MEDIA_META).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (_, v) = entry.map_err(map_redb_err)?;
            let meta: MediaMeta = decode_json(v.value())?;
            if meta.source_url.as_deref().map(str::trim) == Some(target) {
                out.push(meta);
            }
        }
        Ok(out)
    }

    fn delete_metadata(&self, id: &str) -> Result<(), PortError> {
        // Accounting-consistent delete: the refcount and durable totals move
        // in the same transaction as the row removal.
        self.delete_media_record(id).map(|_| ())
    }

    fn list_by_owner(&self, owner_id: &str) -> Result<Vec<MediaMeta>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(MEDIA_META).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (_, v) = entry.map_err(map_redb_err)?;
            let meta: MediaMeta = decode_json(v.value())?;
            if meta.owner_id.as_deref() == Some(owner_id) {
                out.push(meta);
            }
        }
        out.sort_by(|a, b| b.id.cmp(&a.id));
        Ok(out)
    }

    fn list_all_media(&self, cursor: &TimelineCursor) -> Result<Vec<MediaMeta>, PortError> {
        // Paging contract (Batch C C1): `limit` counts returned MATCHING
        // rows, newest-first (aidx ids are time-ordered, so reverse key
        // order), exclusive cursor bounds via `matches_cursor`. Bounded only
        // by `MAX_MEDIA_PAGE_ROWS` — never a hidden smaller clamp. Reverse
        // range iteration with early break is O(skipped + page): no
        // full-table scan-and-sort per page. A short page is NOT
        // end-of-scan: callers advance `until_id` to the last id and stop
        // only on an empty page (exact-multiple datasets need that trailing
        // read). No snapshot across pages: rows inserted after the scan
        // started (larger ids, already passed) are not revisited, and rows
        // deleted mid-scan are skipped — destructive operations must never
        // rely on a partial walk (see the reconciler's complete-set gate).
        let limit = if cursor.limit == 0 {
            10
        } else {
            cursor.limit.min(MAX_MEDIA_PAGE_ROWS)
        };
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(MEDIA_META).map_err(map_redb_err)?;
        let mut out: Vec<MediaMeta> = Vec::new();
        for entry in table.iter().map_err(map_redb_err)?.rev() {
            let (k, v) = entry.map_err(map_redb_err)?;
            if !matches_cursor(k.value(), cursor) {
                continue;
            }
            out.push(decode_json(v.value())?);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    fn durable_bytes(&self, owner_id: &str) -> Result<u64, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn
            .open_table(DURABLE_BYTES_BY_OWNER)
            .map_err(map_redb_err)?;
        Ok(table
            .get(owner_id)
            .map_err(map_redb_err)?
            .map(|g| g.value())
            .unwrap_or(0))
    }

    fn blob_refcount(&self, blob_key: &str) -> Result<u64, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(BLOB_REFCOUNT).map_err(map_redb_err)?;
        Ok(table
            .get(blob_key)
            .map_err(map_redb_err)?
            .map(|g| g.value())
            .unwrap_or(0))
    }

    fn set_blob_refcount(&self, blob_key: &str, count: u64) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(BLOB_REFCOUNT).map_err(map_redb_err)?;
            if count == 0 {
                table.remove(blob_key).map_err(map_redb_err)?;
            } else {
                table.insert(blob_key, count).map_err(map_redb_err)?;
            }
        }
        self.commit_write(write_txn)
    }

    fn set_durable_bytes(&self, owner_id: &str, bytes: u64) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn
                .open_table(DURABLE_BYTES_BY_OWNER)
                .map_err(map_redb_err)?;
            if bytes == 0 {
                table.remove(owner_id).map_err(map_redb_err)?;
            } else {
                table.insert(owner_id, bytes).map_err(map_redb_err)?;
            }
        }
        self.commit_write(write_txn)
    }

    fn delete_media_record(&self, id: &str) -> Result<Option<(MediaMeta, u64)>, PortError> {
        let write_txn = self.begin_write()?;
        let removed = {
            let mut media = write_txn.open_table(MEDIA_META).map_err(map_redb_err)?;
            let mut refcounts = write_txn.open_table(BLOB_REFCOUNT).map_err(map_redb_err)?;
            let mut durable = write_txn
                .open_table(DURABLE_BYTES_BY_OWNER)
                .map_err(map_redb_err)?;
            let meta: Option<MediaMeta> = {
                let guard = media.get(id).map_err(map_redb_err)?;
                guard.map(|g| decode_json(g.value())).transpose()?
            };
            let Some(meta) = meta else {
                return Ok(None);
            };
            media.remove(id).map_err(map_redb_err)?;
            let remaining = refcounts
                .get(meta.blob_key.as_str())
                .map_err(map_redb_err)?
                .map(|g| g.value())
                .unwrap_or(0);
            let remaining = remaining.saturating_sub(1);
            if remaining == 0 {
                refcounts
                    .remove(meta.blob_key.as_str())
                    .map_err(map_redb_err)?;
            } else {
                refcounts
                    .insert(meta.blob_key.as_str(), remaining)
                    .map_err(map_redb_err)?;
            }
            if let Some(owner) = meta.owner_id.as_deref() {
                let total = durable
                    .get(owner)
                    .map_err(map_redb_err)?
                    .map(|g| g.value())
                    .unwrap_or(0);
                let total = total.saturating_sub(meta.size_bytes);
                // Symmetric with refcounts above: absent ≡ zero everywhere
                // (backfill recomputes from rows, verify treats a stored
                // zero for a row-less owner as no drift). Keeping a zero
                // row would flag phantom drift after the last byte is gone.
                if total == 0 {
                    durable.remove(owner).map_err(map_redb_err)?;
                } else {
                    durable.insert(owner, total).map_err(map_redb_err)?;
                }
            }
            (meta, remaining)
        };
        self.commit_write(write_txn)?;
        Ok(Some(removed))
    }

    fn remote_cache_lookup(
        &self,
        source_url: &str,
        variant: &str,
    ) -> Result<Option<RemoteCacheRecord>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(REMOTE_CACHE).map_err(map_redb_err)?;
        match table.get((source_url, variant)).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn remote_cache_insert(&self, rec: &RemoteCacheRecord) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(REMOTE_CACHE).map_err(map_redb_err)?;
            let encoded = encode_json(rec)?;
            table
                .insert(
                    (rec.source_url.as_str(), rec.variant.as_str()),
                    encoded.as_slice(),
                )
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn remote_cache_touch(
        &self,
        source_url: &str,
        variant: &str,
        now_ms: i64,
    ) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(REMOTE_CACHE).map_err(map_redb_err)?;
            let rec: Option<RemoteCacheRecord> = {
                let guard = table.get((source_url, variant)).map_err(map_redb_err)?;
                guard.map(|g| decode_json(g.value())).transpose()?
            };
            let Some(mut rec) = rec else {
                return Ok(());
            };
            rec.last_access_ms = now_ms;
            rec.access_count = rec.access_count.saturating_add(1);
            let encoded = encode_json(&rec)?;
            table
                .insert((source_url, variant), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn remote_cache_evict_candidates(
        &self,
        limit: usize,
    ) -> Result<Vec<RemoteCacheRecord>, PortError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(REMOTE_CACHE).map_err(map_redb_err)?;
        let mut best: Vec<(i64, String, String, RemoteCacheRecord)> = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (_, v) = entry.map_err(map_redb_err)?;
            let rec: RemoteCacheRecord = decode_json(v.value())?;
            best.push((
                rec.last_access_ms,
                rec.source_url.clone(),
                rec.variant.clone(),
                rec,
            ));
            if best.len() > limit {
                if let Some(pos) = best
                    .iter()
                    .enumerate()
                    .max_by(|(_, a), (_, b)| (a.0, &a.1, &a.2).cmp(&(b.0, &b.1, &b.2)))
                    .map(|(i, _)| i)
                {
                    best.swap_remove(pos);
                }
            }
        }
        let mut out: Vec<RemoteCacheRecord> = best.into_iter().map(|(_, _, _, r)| r).collect();
        out.sort_by(|a, b| {
            (a.last_access_ms, &a.source_url, &a.variant).cmp(&(
                b.last_access_ms,
                &b.source_url,
                &b.variant,
            ))
        });
        Ok(out)
    }

    fn remote_cache_remove(&self, source_url: &str, variant: &str) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(REMOTE_CACHE).map_err(map_redb_err)?;
            table.remove((source_url, variant)).map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn cache_entries_for_blob_key(
        &self,
        blob_key: &str,
    ) -> Result<Vec<RemoteCacheRecord>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(REMOTE_CACHE).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (_, v) = entry.map_err(map_redb_err)?;
            let rec: RemoteCacheRecord = decode_json(v.value())?;
            if rec.blob_key == blob_key {
                out.push(rec);
            }
        }
        Ok(out)
    }

    fn commit_cache_eviction(
        &self,
        blob_key: &str,
        size_bytes: u64,
        class: MediaClass,
        index_keys: &[(String, String)],
    ) -> Result<CacheEvictionOutcome, PortError> {
        fn apply_signed(base: u64, delta: i64) -> u64 {
            if delta >= 0 {
                base.saturating_add(delta as u64)
            } else {
                base.saturating_sub(delta.unsigned_abs())
            }
        }
        let write_txn = self.begin_write()?;
        let outcome = {
            let mut media = write_txn.open_table(MEDIA_META).map_err(map_redb_err)?;
            let mut remote = write_txn.open_table(REMOTE_CACHE).map_err(map_redb_err)?;
            let mut accounting = write_txn
                .open_table(CACHE_ACCOUNTING)
                .map_err(map_redb_err)?;
            let mut has_durable = false;
            for entry in media.iter().map_err(map_redb_err)? {
                let (_, v) = entry.map_err(map_redb_err)?;
                let meta: MediaMeta = decode_json(v.value())?;
                if meta.blob_key == blob_key && meta.owner_id.is_some() {
                    has_durable = true;
                    break;
                }
            }
            if has_durable {
                return Ok(CacheEvictionOutcome::SkippedDurableRef);
            }
            let mut targets: Vec<RemoteCacheRecord> = Vec::new();
            if index_keys.is_empty() {
                for entry in remote.iter().map_err(map_redb_err)? {
                    let (_, v) = entry.map_err(map_redb_err)?;
                    let rec: RemoteCacheRecord = decode_json(v.value())?;
                    if rec.blob_key == blob_key {
                        targets.push(rec);
                    }
                }
            } else {
                for (url, variant) in index_keys {
                    match remote
                        .get((url.as_str(), variant.as_str()))
                        .map_err(map_redb_err)?
                    {
                        Some(g) => {
                            let rec: RemoteCacheRecord = decode_json(g.value())?;
                            if rec.blob_key == blob_key {
                                targets.push(rec);
                            }
                        }
                        None => {}
                    }
                }
            }
            if size_bytes == 0 {
                return Ok(CacheEvictionOutcome::AlreadyAbsent);
            }
            if !targets.is_empty() && targets.iter().all(|r| r.bytes_absent || r.size_bytes == 0) {
                return Ok(CacheEvictionOutcome::AlreadyAbsent);
            }
            for rec in &targets {
                let mut updated = rec.clone();
                updated.size_bytes = 0;
                updated.bytes_absent = true;
                let encoded = encode_json(&updated)?;
                remote
                    .insert(
                        (updated.source_url.as_str(), updated.variant.as_str()),
                        encoded.as_slice(),
                    )
                    .map_err(map_redb_err)?;
            }
            let mut evict_ids: Vec<String> = Vec::new();
            for entry in media.iter().map_err(map_redb_err)? {
                let (k, v) = entry.map_err(map_redb_err)?;
                let meta: MediaMeta = decode_json(v.value())?;
                if meta.blob_key == blob_key && meta.owner_id.is_none() && !meta.evicted {
                    evict_ids.push(k.value().to_string());
                }
            }
            for id in &evict_ids {
                let meta_opt: Option<MediaMeta> =
                    match media.get(id.as_str()).map_err(map_redb_err)? {
                        Some(g) => Some(decode_json(g.value())?),
                        None => None,
                    };
                let Some(mut meta) = meta_opt else {
                    continue;
                };
                meta.evicted = true;
                let encoded = encode_json(&meta)?;
                media
                    .insert(id.as_str(), encoded.as_slice())
                    .map_err(map_redb_err)?;
            }
            let mut stats: CacheStats = match accounting.get("stats").map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => CacheStats::default(),
            };
            let debit = size_bytes as i64;
            stats.total_bytes = apply_signed(stats.total_bytes, -debit);
            let class_total = match class {
                MediaClass::Avatar => &mut stats.avatar_bytes,
                MediaClass::Preview => &mut stats.preview_bytes,
                MediaClass::Full => &mut stats.full_bytes,
                MediaClass::AudioVideo => &mut stats.full_bytes,
            };
            *class_total = apply_signed(*class_total, -debit);
            stats.evictions = stats.evictions.saturating_add(1);
            let encoded = encode_json(&stats)?;
            accounting
                .insert("stats", encoded.as_slice())
                .map_err(map_redb_err)?;
            CacheEvictionOutcome::Evicted {
                debited_bytes: size_bytes,
            }
        };
        self.commit_write(write_txn)?;
        Ok(outcome)
    }

    fn cache_stats(&self) -> Result<CacheStats, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn
            .open_table(CACHE_ACCOUNTING)
            .map_err(map_redb_err)?;
        match table.get("stats").map_err(map_redb_err)? {
            Some(g) => Ok(decode_json(g.value())?),
            None => Ok(CacheStats::default()),
        }
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
        fn apply_signed(base: u64, delta: i64) -> u64 {
            if delta >= 0 {
                base.saturating_add(delta as u64)
            } else {
                base.saturating_sub(delta.unsigned_abs())
            }
        }
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn
                .open_table(CACHE_ACCOUNTING)
                .map_err(map_redb_err)?;
            let mut stats: CacheStats = match table.get("stats").map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => CacheStats::default(),
            };
            stats.total_bytes = apply_signed(stats.total_bytes, total_delta);
            let class_total = match class {
                MediaClass::Avatar => &mut stats.avatar_bytes,
                MediaClass::Preview => &mut stats.preview_bytes,
                MediaClass::Full => &mut stats.full_bytes,
                MediaClass::AudioVideo => &mut stats.full_bytes,
            };
            *class_total = apply_signed(*class_total, class_delta);
            stats.hits = stats.hits.saturating_add(hits);
            stats.misses = stats.misses.saturating_add(misses);
            stats.bytes_fetched = stats.bytes_fetched.saturating_add(bytes_fetched);
            stats.evictions = stats.evictions.saturating_add(evictions);
            stats.failed_origins = stats.failed_origins.saturating_add(failed_origins);
            let encoded = encode_json(&stats)?;
            table
                .insert("stats", encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn reserve_cache_bytes(&self, bytes: u64) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        let reserved = {
            let mut table = write_txn
                .open_table(CACHE_ACCOUNTING)
                .map_err(map_redb_err)?;
            let cap: u64 = match table.get("cap").map_err(map_redb_err)? {
                Some(g) => {
                    let raw: &[u8] = g.value();
                    if raw.len() == 8 {
                        u64::from_le_bytes(raw.try_into().unwrap_or([0xFF; 8]))
                    } else {
                        u64::MAX
                    }
                }
                None => u64::MAX,
            };
            let mut stats: CacheStats = match table.get("stats").map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => CacheStats::default(),
            };
            let want = stats
                .total_bytes
                .saturating_add(stats.in_flight_bytes)
                .saturating_add(bytes);
            if want > cap {
                false
            } else {
                stats.in_flight_bytes = stats.in_flight_bytes.saturating_add(bytes);
                let encoded = encode_json(&stats)?;
                table
                    .insert("stats", encoded.as_slice())
                    .map_err(map_redb_err)?;
                true
            }
        };
        self.commit_write(write_txn)?;
        Ok(reserved)
    }

    fn release_cache_reservation(&self, bytes: u64) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn
                .open_table(CACHE_ACCOUNTING)
                .map_err(map_redb_err)?;
            let mut stats: CacheStats = match table.get("stats").map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => CacheStats::default(),
            };
            stats.in_flight_bytes = stats.in_flight_bytes.saturating_sub(bytes);
            let encoded = encode_json(&stats)?;
            table
                .insert("stats", encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn set_cache_cap(&self, cap_bytes: u64) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn
                .open_table(CACHE_ACCOUNTING)
                .map_err(map_redb_err)?;
            table
                .insert("cap", cap_bytes.to_le_bytes().as_slice())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    /// Shared insert core: quota check + metadata write + refcount and
    /// durable-total maintenance in one transaction. `u64::MAX` disables
    /// the quota check (metadata updates, remote rows, backfills).
    fn insert_metadata_with_durable_quota(
        &self,
        input: InsertMediaInput,
        quota_bytes: u64,
    ) -> Result<MediaMeta, PortError> {
        let id = input.id.unwrap_or_else(|| self.ids.generate());
        let media_class = MediaClass::classify_mime(input.mime.as_str())
            .as_str()
            .to_string();
        let meta = MediaMeta {
            id,
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
            variant: "original".to_string(),
            media_class,
            evicted: false,
        };
        meta.validate(input.max_file_size)?;

        let write_txn = self.begin_write()?;
        {
            let mut media = write_txn.open_table(MEDIA_META).map_err(map_redb_err)?;
            let mut refcounts = write_txn.open_table(BLOB_REFCOUNT).map_err(map_redb_err)?;
            let mut durable = write_txn
                .open_table(DURABLE_BYTES_BY_OWNER)
                .map_err(map_redb_err)?;
            // Upsert: back out the old row's accounting before charging the
            // new one, so metadata-only updates are quota- and count-neutral.
            let old: Option<MediaMeta> = match media.get(meta.id.as_str()).map_err(map_redb_err)? {
                Some(g) => Some(decode_json(g.value())?),
                None => None,
            };
            if let Some(old) = old.as_ref() {
                let old_count = refcounts
                    .get(old.blob_key.as_str())
                    .map_err(map_redb_err)?
                    .map(|g| g.value())
                    .unwrap_or(0);
                let left = old_count.saturating_sub(1);
                if left == 0 {
                    refcounts
                        .remove(old.blob_key.as_str())
                        .map_err(map_redb_err)?;
                } else {
                    refcounts
                        .insert(old.blob_key.as_str(), left)
                        .map_err(map_redb_err)?;
                }
                if let Some(owner) = old.owner_id.as_deref() {
                    let total = durable
                        .get(owner)
                        .map_err(map_redb_err)?
                        .map(|g| g.value())
                        .unwrap_or(0);
                    durable
                        .insert(owner, total.saturating_sub(old.size_bytes))
                        .map_err(map_redb_err)?;
                }
            }
            if let Some(owner) = meta.owner_id.as_deref() {
                let total = durable
                    .get(owner)
                    .map_err(map_redb_err)?
                    .map(|g| g.value())
                    .unwrap_or(0);
                let next = total.saturating_add(meta.size_bytes);
                // Grandfathered updates: rewriting the same row with the
                // same owner and no byte growth never breaches, so
                // over-quota users keep rename/comment/sensitivity edits.
                let same_row_same_owner = old.as_ref().is_some_and(|o| {
                    o.id == meta.id
                        && o.owner_id.as_deref() == Some(owner)
                        && meta.size_bytes <= o.size_bytes
                });
                if next > quota_bytes && !same_row_same_owner {
                    return Err(PortError::Validation("DRIVE_CAPACITY_EXCEEDED".to_string()));
                }
                durable.insert(owner, next).map_err(map_redb_err)?;
            }
            let count = refcounts
                .get(meta.blob_key.as_str())
                .map_err(map_redb_err)?
                .map(|g| g.value())
                .unwrap_or(0);
            refcounts
                .insert(meta.blob_key.as_str(), count.saturating_add(1))
                .map_err(map_redb_err)?;
            let encoded = encode_json(&meta)?;
            media
                .insert(meta.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)?;
        Ok(meta)
    }
}
