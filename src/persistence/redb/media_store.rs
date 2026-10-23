use super::codec::{decode_json, encode_json, map_redb_err};
use super::note_store::matches_cursor;
use super::store::{BLOB_REFCOUNT, DURABLE_BYTES_BY_OWNER, MEDIA_META, RedbStore};
use crate::application::ports::{
    CacheEvictionOutcome, CacheStats, InsertMediaInput, MediaClass, MediaStore, PortError,
    RemoteCacheRecord, TimelineCursor,
};
use crate::domain::media::MediaMeta;
use redb::{
    ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition, WriteTransaction,
};

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
/// Secondary alias index `(blob_key, source_url, variant)` → unit marker
/// (adversarial-2 slice 2, finding 9): [`MediaStore::cache_entries_for_blob_key`]
/// resolves every alias of a physical key with one prefix range plus
/// point lookups — never a full `REMOTE_CACHE` scan. Derived state:
/// maintained inline by every `REMOTE_CACHE` writer below, rebuilt from
/// `REMOTE_CACHE` at open when the migration marker is absent (see
/// `run_remote_cache_blob_index_backfill`), and carried in backup like
/// every other live table.
pub(crate) const REMOTE_CACHE_BY_BLOB: TableDefinition<(&str, &str, &str), u8> =
    TableDefinition::new("remote_cache_by_blob");
/// Authoritative accounting class per physical blob key (adversarial-2
/// slice 5, finding 6): `blob_key` → `MediaClass::as_str()`. Recorded at
/// first charge inside [`MediaStore::remote_cache_insert`] (same write
/// transaction as the index row) and read by `commit_cache_eviction`, so
/// eviction debits the class charged at first charge — never the class of
/// the largest alias or whichever row iteration returned first. A restore
/// charge (every prior alias absent) overwrites it with the restoring
/// row's class. Keys that predate this table stay unrecorded until such a
/// charge; eviction falls back to the caller-passed class for those.
/// Dropped when the last alias row for the key is removed (a later charge
/// re-records it). Carried in backup like every other live table.
pub(crate) const CACHE_BLOB_CLASS: TableDefinition<&str, &str> =
    TableDefinition::new("cache_blob_class");
/// LRU cursor for CHARGED remote-cache rows (Wave 2 M1):
/// `(last_access_ms, source_url, variant)` → unit marker, oldest first.
/// Only rows with `size_bytes > 0 && !bytes_absent` are present here, so a
/// charged page is a bounded prefix walk — never a full-table collect and
/// sort — no matter how many tombstones precede the live rows. Tombstones
/// live in [`REMOTE_CACHE_TOMB_LRU`]. Both cursors are maintained inline by
/// every `REMOTE_CACHE` writer in this file in the SAME write transaction
/// as the row mutation (crash-atomic: index and row always agree), and are
/// rebuilt from `REMOTE_CACHE` when a page finds their counts diverged
/// (open-time migration, restore without these tables, test-only
/// clear-plus-backfill). A stale entry (row changed status without a
/// cursor move) is impossible outside those paths; pages skip-and-flag
/// them defensively and rebuild once before re-reading.
pub(crate) const REMOTE_CACHE_CHARGED_LRU: TableDefinition<(i64, &str, &str), u8> =
    TableDefinition::new("remote_cache_charged_lru");
/// LRU cursor for TOMBSTONE remote-cache rows (Wave 2 M1): same key shape
/// as [`REMOTE_CACHE_CHARGED_LRU`], only rows with
/// `bytes_absent || size_bytes == 0`. Lets the retention pass page past
/// unremovable head rows to an expired tail instead of rescanning from
/// zero every pass.
pub(crate) const REMOTE_CACHE_TOMB_LRU: TableDefinition<(i64, &str, &str), u8> =
    TableDefinition::new("remote_cache_tomb_lru");

/// Charged-row predicate shared by the LRU cursors and the page filters:
/// `size_bytes > 0 && !bytes_absent`. Everything else is a tombstone.
fn cache_row_charged(rec: &RemoteCacheRecord) -> bool {
    rec.size_bytes > 0 && !rec.bytes_absent
}

/// Move the LRU cursor entries for one `REMOTE_CACHE` row transition in
/// the ambient write transaction. `old` is the pre-image (removed from
/// its status cursor), `new` the post-image (added to its status cursor);
/// either may be `None` for pure deletes/inserts. Both tables are opened
/// exactly once here — never alongside another open of the same table in
/// one transaction.
fn lru_replace_in_txn(
    write_txn: &WriteTransaction,
    old: Option<&RemoteCacheRecord>,
    new: Option<&RemoteCacheRecord>,
) -> Result<(), PortError> {
    let mut charged = write_txn
        .open_table(REMOTE_CACHE_CHARGED_LRU)
        .map_err(map_redb_err)?;
    let mut tomb = write_txn
        .open_table(REMOTE_CACHE_TOMB_LRU)
        .map_err(map_redb_err)?;
    if let Some(prev) = old {
        let key = (
            prev.last_access_ms,
            prev.source_url.as_str(),
            prev.variant.as_str(),
        );
        if cache_row_charged(prev) {
            charged.remove(key).map_err(map_redb_err)?;
        } else {
            tomb.remove(key).map_err(map_redb_err)?;
        }
    }
    if let Some(next) = new {
        let key = (
            next.last_access_ms,
            next.source_url.as_str(),
            next.variant.as_str(),
        );
        if cache_row_charged(next) {
            charged.insert(key, &1u8).map_err(map_redb_err)?;
        } else {
            tomb.insert(key, &1u8).map_err(map_redb_err)?;
        }
    }
    Ok(())
}
/// Cache accounting singleton (`"stats"` → JSON [`CacheStats`],
/// `"cap"` → u64 le global byte cap for in-flight reservations).
pub(crate) const CACHE_ACCOUNTING: TableDefinition<&str, &[u8]> =
    TableDefinition::new("cache_accounting");

/// True while at least one `REMOTE_CACHE` row still points at `blob_key`,
/// resolved through the alias index (torn entries whose row points
/// elsewhere do not count). Slice 5 uses it to drop the authoritative
/// accounting class once the last alias row for a key is removed.
fn blob_key_still_indexed(
    rows: &redb::Table<'_, (&str, &str), &[u8]>,
    by_blob: &redb::Table<'_, (&str, &str, &str), u8>,
    blob_key: &str,
) -> Result<bool, PortError> {
    let range = by_blob
        .range((blob_key, "", "")..=(blob_key, "\u{10ffff}", "\u{10ffff}"))
        .map_err(map_redb_err)?;
    for entry in range {
        let (k, _) = entry.map_err(map_redb_err)?;
        let (_, url, variant) = k.value();
        match rows.get((url, variant)).map_err(map_redb_err)? {
            Some(g) => {
                let rec: RemoteCacheRecord = decode_json(g.value())?;
                if rec.blob_key == blob_key {
                    return Ok(true);
                }
            }
            None => {}
        }
    }
    Ok(false)
}
/// Read-phase snapshot for `commit_cache_eviction` (Wave 2 L2). Built on a
/// read transaction by [`RedbStore::cache_eviction_read_view`]; the write
/// phase re-validates the refcount (CAS) and every mark id (point lookup)
/// under the write lock before mutating.
struct CacheEvictionReadView {
    /// A live durable (`owner_id.is_some()`) media row still references the
    /// key: eviction must report `SkippedDurableRef` without mutating.
    has_durable: bool,
    /// `BLOB_REFCOUNT` at read time; the write phase fails closed when it
    /// moved (a concurrent insert/delete changed the alias set).
    refcount: u64,
    /// Alias-anchored `REMOTE_CACHE` rows still pointing at the key (for
    /// the read-only `AlreadyAbsent` check).
    targets: Vec<RemoteCacheRecord>,
    /// `MEDIA_META` ids with this key that are non-durable and not yet
    /// marked evicted (the write phase re-validates each by point lookup).
    mark_media_ids: Vec<String>,
}
impl RedbStore {
    /// Count check for the Wave 2 LRU cursors: `(REMOTE_CACHE rows,
    /// cursor entries)`. Errors when the cursor tables do not exist yet
    /// (pre-migration database); the caller rebuilds in that case.
    fn remote_cache_lru_counts(&self) -> Result<(u64, u64), PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let cache_len = read_txn
            .open_table(REMOTE_CACHE)
            .map_err(map_redb_err)?
            .len()
            .map_err(map_redb_err)?;
        let charged_len = read_txn
            .open_table(REMOTE_CACHE_CHARGED_LRU)
            .map_err(map_redb_err)?
            .len()
            .map_err(map_redb_err)?;
        let tomb_len = read_txn
            .open_table(REMOTE_CACHE_TOMB_LRU)
            .map_err(map_redb_err)?
            .len()
            .map_err(map_redb_err)?;
        Ok((cache_len, charged_len + tomb_len))
    }

    /// Rebuild both LRU cursors from `REMOTE_CACHE` in one write
    /// transaction. Unconditional: callers gate on
    /// [`Self::remote_cache_lru_counts`] (or on a stale entry sighting).
    fn rebuild_remote_cache_lru(&self) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let rows: Vec<(i64, String, String, bool)> = {
                let cache = write_txn.open_table(REMOTE_CACHE).map_err(map_redb_err)?;
                let mut out = Vec::new();
                for entry in cache.iter().map_err(map_redb_err)? {
                    let (_, v) = entry.map_err(map_redb_err)?;
                    let rec: RemoteCacheRecord = decode_json(v.value())?;
                    out.push((
                        rec.last_access_ms,
                        rec.source_url.clone(),
                        rec.variant.clone(),
                        cache_row_charged(&rec),
                    ));
                }
                out
            };
            {
                let mut charged = write_txn
                    .open_table(REMOTE_CACHE_CHARGED_LRU)
                    .map_err(map_redb_err)?;
                let stale: Vec<(i64, String, String)> = charged
                    .iter()
                    .map_err(map_redb_err)?
                    .map(|entry| {
                        entry.map(|(k, _)| {
                            let (ms, url, variant) = k.value();
                            (ms, url.to_string(), variant.to_string())
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(map_redb_err)?;
                for (ms, url, variant) in &stale {
                    charged
                        .remove((*ms, url.as_str(), variant.as_str()))
                        .map_err(map_redb_err)?;
                }
            }
            {
                let mut tomb = write_txn
                    .open_table(REMOTE_CACHE_TOMB_LRU)
                    .map_err(map_redb_err)?;
                let stale: Vec<(i64, String, String)> = tomb
                    .iter()
                    .map_err(map_redb_err)?
                    .map(|entry| {
                        entry.map(|(k, _)| {
                            let (ms, url, variant) = k.value();
                            (ms, url.to_string(), variant.to_string())
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(map_redb_err)?;
                for (ms, url, variant) in &stale {
                    tomb.remove((*ms, url.as_str(), variant.as_str()))
                        .map_err(map_redb_err)?;
                }
            }
            {
                let mut charged = write_txn
                    .open_table(REMOTE_CACHE_CHARGED_LRU)
                    .map_err(map_redb_err)?;
                let mut tomb = write_txn
                    .open_table(REMOTE_CACHE_TOMB_LRU)
                    .map_err(map_redb_err)?;
                for (ms, url, variant, is_charged) in &rows {
                    let key = (*ms, url.as_str(), variant.as_str());
                    if *is_charged {
                        charged.insert(key, &1u8).map_err(map_redb_err)?;
                    } else {
                        tomb.insert(key, &1u8).map_err(map_redb_err)?;
                    }
                }
            }
        }
        self.commit_write(write_txn)
    }

    /// Repair-on-read for the LRU cursors: rebuild when their entry count
    /// diverges from the `REMOTE_CACHE` row count (migration, restore, or
    /// test-only clear-plus-backfill which rewrites rows without cursor
    /// maintenance). Steady-state writers keep both in the same
    /// transaction, so this is a no-op read-txn lens check per page.
    fn heal_remote_cache_lru(&self) -> Result<(), PortError> {
        let needs_rebuild = match self.remote_cache_lru_counts() {
            Ok((cache_len, cursor_len)) => cursor_len != cache_len,
            Err(_) => true,
        };
        if !needs_rebuild {
            return Ok(());
        }
        self.rebuild_remote_cache_lru()
    }

    /// One LRU-cursor page walk: skip `offset` entries in
    /// `(last_access_ms, source_url, variant)` order, then resolve up to
    /// `limit` entries through point lookups, keeping only rows whose
    /// current charged status matches `want_charged`. Returns the rows plus
    /// whether any cursor entry failed validation (torn status or missing
    /// row: impossible for same-transaction writers, possible only after a
    /// cursor-bypassing rewrite — the caller rebuilds once and re-reads).
    /// Cost is O(offset + limit) cursor steps plus O(limit) point lookups:
    /// no full-table collect, decode, or sort per page.
    fn lru_cursor_page(
        &self,
        cursor: TableDefinition<(i64, &str, &str), u8>,
        offset: usize,
        limit: usize,
        want_charged: bool,
    ) -> Result<(Vec<RemoteCacheRecord>, bool), PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let index = read_txn.open_table(cursor).map_err(map_redb_err)?;
        let remote = read_txn.open_table(REMOTE_CACHE).map_err(map_redb_err)?;
        let mut out: Vec<RemoteCacheRecord> = Vec::new();
        let mut skipped = 0usize;
        let mut stale = false;
        for entry in index.iter().map_err(map_redb_err)? {
            let (k, _) = entry.map_err(map_redb_err)?;
            let (_, url, variant) = k.value();
            if skipped < offset {
                skipped += 1;
                continue;
            }
            if out.len() >= limit {
                break;
            }
            let (url, variant) = (url.to_string(), variant.to_string());
            match remote
                .get((url.as_str(), variant.as_str()))
                .map_err(map_redb_err)?
            {
                Some(g) => {
                    let rec: RemoteCacheRecord = decode_json(g.value())?;
                    if cache_row_charged(&rec) == want_charged {
                        out.push(rec);
                    } else {
                        stale = true;
                    }
                }
                None => {
                    stale = true;
                }
            }
        }
        Ok((out, stale))
    }

    /// Cursor-backed page with repair: heal on count divergence, walk,
    /// and on a stale-entry sighting rebuild once and re-read so a
    /// cursor-bypassing rewrite can never hide rows from eviction.
    fn lru_backed_page(
        &self,
        cursor: TableDefinition<(i64, &str, &str), u8>,
        offset: usize,
        limit: usize,
        want_charged: bool,
    ) -> Result<Vec<RemoteCacheRecord>, PortError> {
        self.heal_remote_cache_lru()?;
        let (rows, stale) = self.lru_cursor_page(cursor, offset, limit, want_charged)?;
        if stale {
            self.rebuild_remote_cache_lru()?;
            let (rows, _) = self.lru_cursor_page(cursor, offset, limit, want_charged)?;
            return Ok(rows);
        }
        Ok(rows)
    }

    /// Read-phase snapshot for [`MediaStore::commit_cache_eviction`]
    /// (Wave 2 L2): existence, durability, refcount, and mark-set reads run
    /// on a READ transaction; the write transaction opens only when a
    /// mutation will actually happen. Alias resolution is index-anchored
    /// (the `REMOTE_CACHE_BY_BLOB` prefix range plus point lookups, or the
    /// caller-supplied `index_keys` point lookups) — never a `REMOTE_CACHE`
    /// scan. The durability/mark walk still scans `MEDIA_META`, but on the
    /// read transaction: non-aliased rows (durable sharers, orphan cache
    /// media rows) share the key without a cache alias, so only a full read
    /// keeps the Wave 1 alias-completeness. The write phase re-validates
    /// every id by point lookup plus a refcount CAS, so a concurrent
    /// insert/delete between the phases fails closed instead of evicting
    /// under a new durable reference.
    fn cache_eviction_read_view(
        &self,
        blob_key: &str,
        index_keys: &[(String, String)],
    ) -> Result<CacheEvictionReadView, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let refcount = read_txn
            .open_table(BLOB_REFCOUNT)
            .map_err(map_redb_err)?
            .get(blob_key)
            .map_err(map_redb_err)?
            .map(|g| g.value())
            .unwrap_or(0);
        let remote = read_txn.open_table(REMOTE_CACHE).map_err(map_redb_err)?;
        let mut targets: Vec<RemoteCacheRecord> = Vec::new();
        if index_keys.is_empty() {
            let by_blob = read_txn
                .open_table(REMOTE_CACHE_BY_BLOB)
                .map_err(map_redb_err)?;
            let range = by_blob
                .range((blob_key, "", "")..=(blob_key, "\u{10ffff}", "\u{10ffff}"))
                .map_err(map_redb_err)?;
            for entry in range {
                let (k, _) = entry.map_err(map_redb_err)?;
                let (_, url, variant) = k.value();
                let (url, variant) = (url.to_string(), variant.to_string());
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
        let media = read_txn.open_table(MEDIA_META).map_err(map_redb_err)?;
        let mut has_durable = false;
        let mut mark_media_ids: Vec<String> = Vec::new();
        for entry in media.iter().map_err(map_redb_err)? {
            let (k, v) = entry.map_err(map_redb_err)?;
            let meta: MediaMeta = decode_json(v.value())?;
            if meta.blob_key != blob_key {
                continue;
            }
            if meta.owner_id.is_some() {
                has_durable = true;
            } else if !meta.evicted {
                mark_media_ids.push(k.value().to_string());
            }
        }
        Ok(CacheEvictionReadView {
            has_durable,
            refcount,
            targets,
            mark_media_ids,
        })
    }
}
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
            let mut by_blob = write_txn
                .open_table(REMOTE_CACHE_BY_BLOB)
                .map_err(map_redb_err)?;
            // Generation is store-assigned, monotonic per row: a rewrite
            // (touch-equivalent refetch, sharer restore, re-insert) always
            // moves it forward, so eviction's locked re-read can tell a
            // same-millisecond update apart from the listed snapshot. New
            // rows start at 1; rewrites advance past both the stored and
            // the incoming value so a stale caller can never reuse one.
            let mut stored = rec.clone();
            let old: Option<RemoteCacheRecord> = {
                let guard = table
                    .get((rec.source_url.as_str(), rec.variant.as_str()))
                    .map_err(map_redb_err)?;
                guard.map(|g| decode_json(g.value())).transpose()?
            };
            // Wave 2 M1: pre-image for the LRU cursor move below (`match
            // old` consumes the value).
            let old_lru_image = old.clone();
            // Slice 5: a rewrite of a still-live same-key row accompanies
            // no charge (the fetch sharer path only charges for absent or
            // previously-debited keys), so it must not (re-)record the
            // authoritative class — even when it is the key's only row.
            let self_live_rewrite = old.as_ref().is_some_and(|prev| {
                prev.blob_key == stored.blob_key && !prev.bytes_absent && prev.size_bytes > 0
            });
            match old {
                Some(previous) => {
                    stored.generation = previous.generation.saturating_add(1).max(rec.generation);
                    if previous.blob_key != stored.blob_key && !previous.blob_key.is_empty() {
                        by_blob
                            .remove((
                                previous.blob_key.as_str(),
                                rec.source_url.as_str(),
                                rec.variant.as_str(),
                            ))
                            .map_err(map_redb_err)?;
                        // The row abandoned its previous key (content
                        // changed at this URL): forget that key's
                        // authoritative class once its last alias is gone
                        // (a later charge re-records it).
                        if !blob_key_still_indexed(&table, &by_blob, &previous.blob_key)? {
                            let mut class_table = write_txn
                                .open_table(CACHE_BLOB_CLASS)
                                .map_err(map_redb_err)?;
                            class_table
                                .remove(previous.blob_key.as_str())
                                .map_err(map_redb_err)?;
                        }
                    }
                }
                None => {
                    stored.generation = rec.generation.max(1);
                }
            }
            let encoded = encode_json(&stored)?;
            table
                .insert(
                    (rec.source_url.as_str(), rec.variant.as_str()),
                    encoded.as_slice(),
                )
                .map_err(map_redb_err)?;
            if !stored.blob_key.is_empty() {
                by_blob
                    .insert(
                        (
                            stored.blob_key.as_str(),
                            rec.source_url.as_str(),
                            rec.variant.as_str(),
                        ),
                        &1u8,
                    )
                    .map_err(map_redb_err)?;
                // Authoritative accounting class (slice 5): first charge
                // wins, a restore charge overwrites, live sharers keep.
                // Prior aliases are every other row still pointing at this
                // key (the just-written row is excluded; a same-key old row
                // at this (url, variant) was already replaced above).
                let mut prior_any = false;
                let mut prior_live = false;
                let range = by_blob
                    .range(
                        (stored.blob_key.as_str(), "", "")
                            ..=(stored.blob_key.as_str(), "\u{10ffff}", "\u{10ffff}"),
                    )
                    .map_err(map_redb_err)?;
                for entry in range {
                    let (k, _) = entry.map_err(map_redb_err)?;
                    let (_, url, variant) = k.value();
                    if url == rec.source_url && variant == rec.variant {
                        continue;
                    }
                    let prior: Option<RemoteCacheRecord> =
                        match table.get((url, variant)).map_err(map_redb_err)? {
                            Some(g) => Some(decode_json(g.value())?),
                            None => None,
                        };
                    let Some(prior) = prior else {
                        continue;
                    };
                    if prior.blob_key != stored.blob_key {
                        continue;
                    }
                    prior_any = true;
                    if !prior.bytes_absent && prior.size_bytes > 0 {
                        prior_live = true;
                        break;
                    }
                }
                if (!prior_any || !prior_live) && !self_live_rewrite {
                    // No aliases (brand-new key) or every alias absent
                    // (restore after eviction): this row's class is the
                    // (re-)charge class, so it becomes authoritative.
                    // (A brand-new row over already-pooled, never-charged
                    // crash-orphan bytes still records here; the orphan
                    // sweep heals the bytes, and the drift is bounded by
                    // one saturating charge — never a guess.)
                    let mut class_table = write_txn
                        .open_table(CACHE_BLOB_CLASS)
                        .map_err(map_redb_err)?;
                    class_table
                        .insert(stored.blob_key.as_str(), stored.class.as_str())
                        .map_err(map_redb_err)?;
                }
                // Otherwise the key is already charged with live sharers:
                // the first charge stays authoritative. A pre-migration
                // shared key (live aliases, no record) is deliberately
                // left unrecorded — attributing it to any one alias would
                // be a guess (see `commit_cache_eviction` fallback).
            }
            // Wave 2 M1: keep the LRU cursors in step with the row write
            // (same transaction, so cursor and row never disagree).
            lru_replace_in_txn(&write_txn, old_lru_image.as_ref(), Some(&stored))?;
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
            // Wave 2 M1: pre-image for the cursor move (the touch may
            // change `last_access_ms`, i.e. the cursor key itself).
            let old_lru_image = rec.clone();
            rec.last_access_ms = now_ms;
            rec.access_count = rec.access_count.saturating_add(1);
            // Same-millisecond touches must read as newer than any listed
            // snapshot: the timestamp alone cannot express them, so the
            // generation moves on every touch (finding 5 guard half).
            rec.generation = rec.generation.saturating_add(1);
            let encoded = encode_json(&rec)?;
            table
                .insert((source_url, variant), encoded.as_slice())
                .map_err(map_redb_err)?;
            // Wave 2 M1: keep the LRU cursors in step (same transaction).
            lru_replace_in_txn(&write_txn, Some(&old_lru_image), Some(&rec))?;
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

    /// Charged-row window for eviction (slice 2): tombstones are filtered
    /// store-side, so they can never starve the window no matter how many
    /// precede the live rows. One cursor walk per page (Wave 2 M1): the
    /// `(last_access_ms, source_url, variant)` cursor holds exactly the
    /// charged rows in eviction order, so a page costs O(offset + limit)
    /// cursor steps plus O(limit) point lookups — never a full-table
    /// collect, JSON decode, and sort. The eviction loop advances `offset`
    /// past stuck windows and stops on a short page.
    fn remote_cache_charged_page(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<RemoteCacheRecord>, PortError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.lru_backed_page(REMOTE_CACHE_CHARGED_LRU, offset, limit, true)
    }

    /// Tombstone window for the retention pass (slice 2, finding 9): only
    /// zero-byte/absent rows, oldest first. The caller filters by class TTL
    /// and live-media reference before the conditional remove. Backed by
    /// the tombstone LRU cursor (Wave 2 M1): one O(offset + limit) walk
    /// per page, so the retention pass can page past unremovable head
    /// rows to an expired tail instead of rescanning from zero.
    fn remote_cache_tombstone_page(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<RemoteCacheRecord>, PortError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.lru_backed_page(REMOTE_CACHE_TOMB_LRU, offset, limit, false)
    }

    /// Dangling-index window for the reconciler (slice 4, finding E): only
    /// rows whose media row is already gone, in `(source_url, variant)`
    /// key order — never LRU order, so a newer dangling row is visited
    /// even while old live rows or tombstones would fill an oldest-N
    /// window. The media-absence check is one point lookup per index row
    /// in the same read transaction. Callers treat every row as a
    /// candidate and re-validate under the lifecycle gate plus the
    /// per-key lock (see `remote_cache_remove_if`).
    ///
    /// Wave 2 M1: unlike the charged/tombstone windows this page never
    /// collected or sorted the table — it streams the key-ordered index
    /// from zero, so a call already costs O(offset + limit) point
    /// lookups, not O(N). A cursor API would avoid re-walking the
    /// skipped prefix across calls, but the port signature is
    /// offset-based (shared with the parent-owned trait); the reconciler
    /// loop bounds the total by advancing `offset` and stopping on a
    /// short page.
    fn remote_cache_dangling_page(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<RemoteCacheRecord>, PortError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let index = read_txn.open_table(REMOTE_CACHE).map_err(map_redb_err)?;
        let media = read_txn.open_table(MEDIA_META).map_err(map_redb_err)?;
        let mut out: Vec<RemoteCacheRecord> = Vec::new();
        let mut skipped = 0usize;
        for entry in index.iter().map_err(map_redb_err)? {
            let (_, v) = entry.map_err(map_redb_err)?;
            let rec: RemoteCacheRecord = decode_json(v.value())?;
            if media
                .get(rec.media_id.as_str())
                .map_err(map_redb_err)?
                .is_some()
            {
                continue;
            }
            if skipped < offset {
                skipped += 1;
                continue;
            }
            out.push(rec);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    fn remote_cache_remove(&self, source_url: &str, variant: &str) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(REMOTE_CACHE).map_err(map_redb_err)?;
            let mut by_blob = write_txn
                .open_table(REMOTE_CACHE_BY_BLOB)
                .map_err(map_redb_err)?;
            let old: Option<RemoteCacheRecord> = {
                let guard = table.get((source_url, variant)).map_err(map_redb_err)?;
                guard.map(|g| decode_json(g.value())).transpose()?
            };
            table.remove((source_url, variant)).map_err(map_redb_err)?;
            if let Some(previous) = old {
                // Wave 2 M1: the cursor entry goes with the row (same
                // transaction).
                lru_replace_in_txn(&write_txn, Some(&previous), None)?;
                if !previous.blob_key.is_empty() {
                    by_blob
                        .remove((previous.blob_key.as_str(), source_url, variant))
                        .map_err(map_redb_err)?;
                    // Slice 5: forget the authoritative class once the last
                    // alias row for the key is gone (a later charge
                    // re-records it via `remote_cache_insert`). Torn alias
                    // entries whose row points elsewhere do not retain it.
                    if !blob_key_still_indexed(&table, &by_blob, &previous.blob_key)? {
                        let mut class_table = write_txn
                            .open_table(CACHE_BLOB_CLASS)
                            .map_err(map_redb_err)?;
                        class_table
                            .remove(previous.blob_key.as_str())
                            .map_err(map_redb_err)?;
                    }
                }
            }
        }
        self.commit_write(write_txn)
    }

    fn remote_cache_remove_if(
        &self,
        source_url: &str,
        variant: &str,
        media_id: &str,
        blob_key: &str,
    ) -> Result<bool, PortError> {
        // Conditional atomic remove: the row disappears only when it still
        // points at the expected media/blob pair. A concurrent refetch
        // installs a new `media_id`, so the CAS fails and the fresh row
        // (and its bytes) survives. The alias-index entry goes in the same
        // transaction, so the secondary lookup can never dangle.
        let write_txn = self.begin_write()?;
        let removed = {
            let mut table = write_txn.open_table(REMOTE_CACHE).map_err(map_redb_err)?;
            let mut by_blob = write_txn
                .open_table(REMOTE_CACHE_BY_BLOB)
                .map_err(map_redb_err)?;
            let current: Option<RemoteCacheRecord> = {
                let guard = table.get((source_url, variant)).map_err(map_redb_err)?;
                guard.map(|g| decode_json(g.value())).transpose()?
            };
            let Some(current) = current else {
                return Ok(false);
            };
            if current.media_id != media_id || current.blob_key != blob_key {
                return Ok(false);
            }
            table.remove((source_url, variant)).map_err(map_redb_err)?;
            // Wave 2 M1: the cursor entry goes with the row (same
            // transaction), so pages never serve the removed row.
            lru_replace_in_txn(&write_txn, Some(&current), None)?;
            if !current.blob_key.is_empty() {
                by_blob
                    .remove((current.blob_key.as_str(), source_url, variant))
                    .map_err(map_redb_err)?;
                // Slice 5: same last-alias cleanup as `remote_cache_remove`
                // (a later charge re-records the authoritative class).
                if !blob_key_still_indexed(&table, &by_blob, &current.blob_key)? {
                    let mut class_table = write_txn
                        .open_table(CACHE_BLOB_CLASS)
                        .map_err(map_redb_err)?;
                    class_table
                        .remove(current.blob_key.as_str())
                        .map_err(map_redb_err)?;
                }
            }
            true
        };
        self.commit_write(write_txn)?;
        Ok(removed)
    }

    fn cache_entries_for_blob_key(
        &self,
        blob_key: &str,
    ) -> Result<Vec<RemoteCacheRecord>, PortError> {
        // Alias resolution through the `(blob_key, source_url, variant)`
        // secondary index: one prefix range plus one point lookup per
        // alias. No full-table scan regardless of history size (finding 9).
        if blob_key.is_empty() {
            return Ok(Vec::new());
        }
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_blob = read_txn
            .open_table(REMOTE_CACHE_BY_BLOB)
            .map_err(map_redb_err)?;
        let table = read_txn.open_table(REMOTE_CACHE).map_err(map_redb_err)?;
        let mut out = Vec::new();
        let range = by_blob
            .range((blob_key, "", "")..=(blob_key, "\u{10ffff}", "\u{10ffff}"))
            .map_err(map_redb_err)?;
        for entry in range {
            let (k, _) = entry.map_err(map_redb_err)?;
            let (_, url, variant) = k.value();
            let (url, variant) = (url.to_string(), variant.to_string());
            match table
                .get((url.as_str(), variant.as_str()))
                .map_err(map_redb_err)?
            {
                Some(g) => {
                    let rec: RemoteCacheRecord = decode_json(g.value())?;
                    // Index entries are removed with their rows, but a
                    // torn write (crash between the two puts) heals here:
                    // only rows still pointing at this key are returned.
                    if rec.blob_key == blob_key {
                        out.push(rec);
                    }
                }
                None => {}
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
        // Read phase on a READ transaction (Wave 2 L2): the existence and
        // durability probes that decide the read-only outcomes
        // (`AlreadyAbsent`, `SkippedDurableRef`) run here, never under the
        // write lock. The write transaction below opens only when a
        // mutation will actually happen.
        let view = self.cache_eviction_read_view(blob_key, index_keys)?;
        if view.has_durable {
            return Ok(CacheEvictionOutcome::SkippedDurableRef);
        }
        if size_bytes == 0 {
            return Ok(CacheEvictionOutcome::AlreadyAbsent);
        }
        if !view.targets.is_empty()
            && view
                .targets
                .iter()
                .all(|r| r.bytes_absent || r.size_bytes == 0)
        {
            return Ok(CacheEvictionOutcome::AlreadyAbsent);
        }
        // Write phase: point lookups and point updates only. The durable
        // probe is re-validated cheaply (alias range plus point lookups,
        // O(aliases)) instead of rescanning `MEDIA_META`; a refcount change
        // or a new durable alias since the read phase fails closed with
        // `SkippedDurableRef` and no mutation.
        let write_txn = self.begin_write()?;
        let outcome = {
            let mut remote = write_txn.open_table(REMOTE_CACHE).map_err(map_redb_err)?;
            let by_blob = write_txn
                .open_table(REMOTE_CACHE_BY_BLOB)
                .map_err(map_redb_err)?;
            let mut media = write_txn.open_table(MEDIA_META).map_err(map_redb_err)?;
            let refcounts = write_txn.open_table(BLOB_REFCOUNT).map_err(map_redb_err)?;
            let refcount_now = refcounts
                .get(blob_key)
                .map_err(map_redb_err)?
                .map(|g| g.value())
                .unwrap_or(0);
            if refcount_now != view.refcount {
                return Ok(CacheEvictionOutcome::SkippedDurableRef);
            }
            // Alias pairs for this key (index-anchored, O(aliases)).
            let alias_pairs: Vec<(String, String)> = {
                let range = by_blob
                    .range((blob_key, "", "")..=(blob_key, "\u{10ffff}", "\u{10ffff}"))
                    .map_err(map_redb_err)?;
                let mut out = Vec::new();
                for entry in range {
                    let (k, _) = entry.map_err(map_redb_err)?;
                    let (_, url, variant) = k.value();
                    out.push((url.to_string(), variant.to_string()));
                }
                out
            };
            let mut durable_now = false;
            for (url, variant) in &alias_pairs {
                let Some(g) = remote
                    .get((url.as_str(), variant.as_str()))
                    .map_err(map_redb_err)?
                else {
                    continue;
                };
                let rec: RemoteCacheRecord = decode_json(g.value())?;
                if rec.blob_key != blob_key {
                    continue;
                }
                if let Some(mg) = media.get(rec.media_id.as_str()).map_err(map_redb_err)? {
                    let meta: MediaMeta = decode_json(mg.value())?;
                    if meta.blob_key == blob_key && meta.owner_id.is_some() {
                        durable_now = true;
                        break;
                    }
                }
            }
            if durable_now {
                return Ok(CacheEvictionOutcome::SkippedDurableRef);
            }
            let mut targets: Vec<RemoteCacheRecord> = Vec::new();
            if index_keys.is_empty() {
                // Same alias-anchored load as the read phase (equivalent
                // to the old full `REMOTE_CACHE` scan for this key, but
                // O(aliases)).
                for (url, variant) in &alias_pairs {
                    if let Some(g) = remote
                        .get((url.as_str(), variant.as_str()))
                        .map_err(map_redb_err)?
                    {
                        let rec: RemoteCacheRecord = decode_json(g.value())?;
                        if rec.blob_key == blob_key {
                            targets.push(rec);
                        }
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
            let mut charged_lru = write_txn
                .open_table(REMOTE_CACHE_CHARGED_LRU)
                .map_err(map_redb_err)?;
            let mut tomb_lru = write_txn
                .open_table(REMOTE_CACHE_TOMB_LRU)
                .map_err(map_redb_err)?;
            for rec in &targets {
                let mut updated = rec.clone();
                updated.size_bytes = 0;
                updated.bytes_absent = true;
                // The transition is itself a mutation: move the generation
                // so a concurrent snapshot baseline can never match a
                // tombstoned row.
                updated.generation = rec.generation.saturating_add(1);
                let encoded = encode_json(&updated)?;
                remote
                    .insert(
                        (updated.source_url.as_str(), updated.variant.as_str()),
                        encoded.as_slice(),
                    )
                    .map_err(map_redb_err)?;
                // Wave 2 M1: the row changed cursors (charged to
                // tombstone); the `last_access_ms` key itself is
                // untouched, matching the old "no fake freshness" rule.
                charged_lru
                    .remove((
                        rec.last_access_ms,
                        rec.source_url.as_str(),
                        rec.variant.as_str(),
                    ))
                    .map_err(map_redb_err)?;
                tomb_lru
                    .insert(
                        (
                            updated.last_access_ms,
                            updated.source_url.as_str(),
                            updated.variant.as_str(),
                        ),
                        &1u8,
                    )
                    .map_err(map_redb_err)?;
            }
            // Mark the cache media rows evicted by point lookup over the
            // read-phase alias set (plus the read-scan fallback ids for
            // non-aliased rows): no full `MEDIA_META` scan under the
            // write lock. Each row is re-validated before the mark.
            for id in &view.mark_media_ids {
                let meta_opt: Option<MediaMeta> =
                    match media.get(id.as_str()).map_err(map_redb_err)? {
                        Some(g) => Some(decode_json(g.value())?),
                        None => None,
                    };
                let Some(mut meta) = meta_opt else {
                    continue;
                };
                if meta.blob_key != blob_key || meta.owner_id.is_some() || meta.evicted {
                    continue;
                }
                meta.evicted = true;
                let encoded = encode_json(&meta)?;
                media
                    .insert(id.as_str(), encoded.as_slice())
                    .map_err(map_redb_err)?;
            }
            let mut accounting = write_txn
                .open_table(CACHE_ACCOUNTING)
                .map_err(map_redb_err)?;
            let mut stats: CacheStats = match accounting.get("stats").map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => CacheStats::default(),
            };
            // Slice 5 (finding 6): debit the authoritative per-key
            // accounting class recorded at first charge, not the
            // caller-passed class (a max-alias / iteration-order guess).
            // Pre-migration keys with no record fall back to the caller's
            // class; their attribution is fixed at the next charge, never
            // guessed here. `AudioVideo` has no class counter anywhere
            // (governed by `total_bytes` alone, like the backfill, the
            // test doubles, and `class_bytes_in`): only the total moves.
            let debit_class = match write_txn
                .open_table(CACHE_BLOB_CLASS)
                .map_err(map_redb_err)?
                .get(blob_key)
                .map_err(map_redb_err)?
            {
                Some(g) => MediaClass::from_str(g.value()).unwrap_or(class),
                None => class,
            };
            let debit = size_bytes as i64;
            stats.total_bytes = apply_signed(stats.total_bytes, -debit);
            match debit_class {
                MediaClass::Avatar => stats.avatar_bytes = apply_signed(stats.avatar_bytes, -debit),
                MediaClass::Preview => {
                    stats.preview_bytes = apply_signed(stats.preview_bytes, -debit)
                }
                MediaClass::Full => stats.full_bytes = apply_signed(stats.full_bytes, -debit),
                MediaClass::AudioVideo => {}
            };
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
            // Slice 5 (finding 6): `AudioVideo` is never folded into
            // `full_bytes` (or any class counter). It is governed by
            // `total_bytes` alone — same rule as `commit_cache_eviction`,
            // the storage-accounting backfill, the test doubles, and
            // `class_bytes_in` in the eviction budget checks. Historical
            // `full_bytes` may still hold pre-slice AudioVideo charges
            // (see the `CACHE_BLOB_CLASS` table docs); those are left
            // alone, never re-attributed by guessing.
            match class {
                MediaClass::Avatar => {
                    stats.avatar_bytes = apply_signed(stats.avatar_bytes, class_delta)
                }
                MediaClass::Preview => {
                    stats.preview_bytes = apply_signed(stats.preview_bytes, class_delta)
                }
                MediaClass::Full => stats.full_bytes = apply_signed(stats.full_bytes, class_delta),
                MediaClass::AudioVideo => {}
            };
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
                .checked_add(stats.in_flight_bytes)
                .and_then(|partial| partial.checked_add(bytes));
            let Some(want) = want else {
                return Err(PortError::StorageUnavailable(
                    "cache reservation counter overflow".to_string(),
                ));
            };
            if want > cap {
                false
            } else {
                stats.in_flight_bytes =
                    stats.in_flight_bytes.checked_add(bytes).ok_or_else(|| {
                        PortError::StorageUnavailable(
                            "cache reservation counter overflow".to_string(),
                        )
                    })?;
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
        // Fail loudly on drift (L3): a release larger than the outstanding
        // counter means a double-release or an undercounted reserve. Clamp
        // silently and the leak hides; return `Err` and leave the counter
        // untouched instead. The only sanctioned zeroing is
        // `reset_in_flight` at startup, which releases exactly the observed
        // remainder in one call (never through zero).
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn
                .open_table(CACHE_ACCOUNTING)
                .map_err(map_redb_err)?;
            let mut stats: CacheStats = match table.get("stats").map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => CacheStats::default(),
            };
            let Some(remainder) = stats.in_flight_bytes.checked_sub(bytes) else {
                return Err(PortError::StorageUnavailable(format!(
                    "cache reservation underflow: releasing {bytes} with {} in flight",
                    stats.in_flight_bytes
                )));
            };
            stats.in_flight_bytes = remainder;
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
