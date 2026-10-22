use super::super::account_store::{ACCOUNTS_BY_HANDLE, ACCOUNTS_BY_URI};
use super::super::auth_store::{API_TOKENS, API_TOKENS_BY_ID};
use super::super::codec::{decode_json, encode_json, map_redb_err};
use super::super::media_store::{CACHE_ACCOUNTING, REMOTE_CACHE};
use super::super::note_store::{NOTES_BY_QUOTE_REQUEST, NOTES_BY_QUOTE_STAMP, NOTES_BY_REPLY};
use super::super::notification_store::{
    NOTIFICATIONS, NOTIFICATIONS_BY_USER, notification_order_key,
};
use super::super::reaction_store::{REACTIONS, REACTIONS_BY_NOTE};
use super::{
    ACCOUNT_URI_INDEX_DONE_KEY, ACCOUNTS, BLOB_REFCOUNT, DURABLE_BYTES_BY_OWNER, MEDIA_META, META,
    NOTES, NOTES_BY_URI, NOTIFICATION_ORDER_DONE_KEY, QUOTE_INDEX_DONE_KEY,
    REACTIONS_BY_NOTE_DONE_KEY, REPLY_INDEX_DONE_KEY, REPLY_INDEX_DONE_VALUE,
    REPLY_INDEX_PROGRESS_KEY, RedbStore, STORAGE_ACCOUNTING_BATCH, STORAGE_ACCOUNTING_DONE_KEY,
    STORAGE_ACCOUNTING_PROGRESS_KEY, TOKEN_ID_DONE_KEY, URI_INDEX_DONE_KEY,
};
use crate::application::ports::{
    ApiTokenRecord, CacheStats, IdGenerator, MediaClass, PortError, RemoteCacheRecord,
};
use crate::domain::account::Account;
use crate::domain::media::MediaMeta;
use crate::domain::note::Note;
use crate::domain::notification::Notification;
use crate::domain::reaction::Reaction;
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata};

impl RedbStore {
    // --- Slice A reply-index migration (`docs/replies.md` §4) ---------------

    /// Notes scanned per backfill batch. Small enough to keep every batch
    /// transaction far under the FDB ~10 MB shape this schema must also fit.
    const REPLY_BACKFILL_BATCH: usize = 500;

    /// Run the `NOTES_BY_REPLY` backfill to completion in bounded batches.
    /// Each batch reads at most `REPLY_BACKFILL_BATCH` note rows, inserts
    /// their `(parent, child)` index rows, and persists the resume cursor in
    /// the SAME write transaction; the done marker commits only with the
    /// final batch. Crash-resumable (progress survives; replays re-insert
    /// idempotently) and a no-op when the marker is already set.
    pub fn run_reply_index_backfill(&self) -> Result<(), PortError> {
        loop {
            if self.reply_index_backfill_batch(Self::REPLY_BACKFILL_BATCH)? {
                return Ok(());
            }
        }
    }

    /// One backfill batch. Returns `true` when the migration is complete
    /// (done marker observed or just written).
    fn reply_index_backfill_batch(&self, batch_limit: usize) -> Result<bool, PortError> {
        if self.meta_get(REPLY_INDEX_DONE_KEY)?.as_deref() == Some(REPLY_INDEX_DONE_VALUE) {
            return Ok(true);
        }
        let progress = self.meta_get(REPLY_INDEX_PROGRESS_KEY)?.unwrap_or_default();
        // Bounded read: at most `batch_limit` note rows past the cursor.
        let scanned: Vec<(String, Option<String>)> = {
            let read_txn = self.db.begin_read().map_err(map_redb_err)?;
            let notes_table = read_txn.open_table(NOTES).map_err(map_redb_err)?;
            let mut out = Vec::new();
            let range = notes_table
                .range(progress.as_str()..)
                .map_err(map_redb_err)?;
            for entry in range {
                let (k, v) = entry.map_err(map_redb_err)?;
                let note_id: String = k.value().to_string();
                if note_id.as_str() <= progress.as_str() {
                    continue;
                }
                let note: Note = decode_json(v.value())?;
                out.push((note_id, note.reply_id));
                if out.len() >= batch_limit.max(1) {
                    break;
                }
            }
            out
        };
        let exhausted = scanned.len() < batch_limit.max(1);
        let write_txn = self.begin_write()?;
        {
            let mut index_table = write_txn.open_table(NOTES_BY_REPLY).map_err(map_redb_err)?;
            let mut meta_table = write_txn.open_table(META).map_err(map_redb_err)?;
            for (child_id, parent_opt) in &scanned {
                if let Some(parent_id) = parent_opt {
                    index_table
                        .insert((parent_id.as_str(), child_id.as_str()), &1u8)
                        .map_err(map_redb_err)?;
                }
            }
            if let Some((last_id, _)) = scanned.last() {
                meta_table
                    .insert(REPLY_INDEX_PROGRESS_KEY, last_id.as_str())
                    .map_err(map_redb_err)?;
            }
            if exhausted {
                meta_table
                    .insert(REPLY_INDEX_DONE_KEY, REPLY_INDEX_DONE_VALUE)
                    .map_err(map_redb_err)?;
            }
        }
        self.commit_write(write_txn)?;
        Ok(exhausted)
    }

    /// Build `NOTES_BY_URI` for databases created before the index existed.
    /// One pass; when several live rows share a `uri` (duplicates created
    /// before uniqueness was enforced) the richest row wins (resolved reply
    /// link or pending parent, then attachments), ties going to the oldest.
    /// The losing rows stay in `NOTES` but are unreachable by uri.
    pub fn run_uri_index_backfill(&self) -> Result<(), PortError> {
        if self.meta_get(URI_INDEX_DONE_KEY)?.as_deref() == Some(REPLY_INDEX_DONE_VALUE) {
            return Ok(());
        }
        let write_txn = self.begin_write()?;
        {
            let notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
            let mut by_uri = write_txn.open_table(NOTES_BY_URI).map_err(map_redb_err)?;
            let mut best: std::collections::HashMap<String, (u8, String)> =
                std::collections::HashMap::new();
            for entry in notes_table.iter().map_err(map_redb_err)? {
                let (k, v) = entry.map_err(map_redb_err)?;
                let note: Note = decode_json(v.value())?;
                let Some(uri) = note.uri.as_deref().map(str::trim).filter(|u| !u.is_empty()) else {
                    continue;
                };
                if note.is_deleted {
                    continue;
                }
                let rank = u8::from(note.reply_id.is_some() || note.reply_parent_uri.is_some())
                    + u8::from(!note.file_ids.is_empty());
                let id = k.value().to_string();
                match best.get(uri) {
                    Some((r, _)) if *r >= rank => {}
                    _ => {
                        best.insert(uri.to_string(), (rank, id));
                    }
                }
            }
            for (uri, (_, id)) in &best {
                by_uri
                    .insert(uri.as_str(), id.as_str())
                    .map_err(map_redb_err)?;
            }
            let mut meta_table = write_txn.open_table(META).map_err(map_redb_err)?;
            meta_table
                .insert(URI_INDEX_DONE_KEY, REPLY_INDEX_DONE_VALUE)
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    /// Build `NOTIFICATIONS_BY_USER` (order-keyed) from `NOTIFICATIONS` once, for
    /// databases written before the index was keyed by the id's time-ordered
    /// suffix. Idempotent; the META marker makes later opens a no-op.
    pub fn run_notification_order_backfill(&self) -> Result<(), PortError> {
        if self.meta_get(NOTIFICATION_ORDER_DONE_KEY)?.as_deref() == Some("done") {
            return Ok(());
        }
        let write_txn = self.begin_write()?;
        {
            let notif_table = write_txn.open_table(NOTIFICATIONS).map_err(map_redb_err)?;
            let mut by_user = write_txn
                .open_table(NOTIFICATIONS_BY_USER)
                .map_err(map_redb_err)?;
            for entry in notif_table.iter().map_err(map_redb_err)? {
                let (k, v) = entry.map_err(map_redb_err)?;
                let stored: Notification = decode_json(v.value())?;
                let nid = k.value();
                by_user
                    .insert(
                        (stored.notifiee_id.as_str(), notification_order_key(nid)),
                        nid,
                    )
                    .map_err(map_redb_err)?;
            }
            let mut meta_table = write_txn.open_table(META).map_err(map_redb_err)?;
            meta_table
                .insert(NOTIFICATION_ORDER_DONE_KEY, "done")
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    /// Build `REACTIONS_BY_NOTE` from the primary `REACTIONS` table.
    /// Idempotent; the META marker makes later opens a no-op.
    pub fn run_reactions_by_note_backfill(&self) -> Result<(), PortError> {
        if self.meta_get(REACTIONS_BY_NOTE_DONE_KEY)?.as_deref() == Some("done") {
            return Ok(());
        }
        let write_txn = self.begin_write()?;
        {
            let reactions_table = write_txn.open_table(REACTIONS).map_err(map_redb_err)?;
            let mut by_note = write_txn
                .open_table(REACTIONS_BY_NOTE)
                .map_err(map_redb_err)?;
            for entry in reactions_table.iter().map_err(map_redb_err)? {
                let (_, v) = entry.map_err(map_redb_err)?;
                let reaction: Reaction = decode_json(v.value())?;
                let reaction_bytes = encode_json(&reaction)?;
                by_note
                    .insert(
                        (reaction.note_id.as_str(), reaction.id.as_str()),
                        reaction_bytes.as_slice(),
                    )
                    .map_err(map_redb_err)?;
            }
            let mut meta_table = write_txn.open_table(META).map_err(map_redb_err)?;
            meta_table
                .insert(REACTIONS_BY_NOTE_DONE_KEY, "done")
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    /// Assign ids to `API_TOKENS` rows minted before Batch O (empty `id`)
    /// and index every row in `API_TOKENS_BY_ID`. Idempotent; the META
    /// marker makes later opens a no-op.
    pub fn run_token_id_backfill(&self) -> Result<(), PortError> {
        if self.meta_get(TOKEN_ID_DONE_KEY)?.as_deref() == Some("done") {
            return Ok(());
        }
        let write_txn = self.begin_write()?;
        {
            let mut tokens = write_txn.open_table(API_TOKENS).map_err(map_redb_err)?;
            let mut by_id = write_txn
                .open_table(API_TOKENS_BY_ID)
                .map_err(map_redb_err)?;
            let mut patched: Vec<(String, ApiTokenRecord)> = Vec::new();
            for entry in tokens.iter().map_err(map_redb_err)? {
                let (k, v) = entry.map_err(map_redb_err)?;
                let mut record: ApiTokenRecord = decode_json(v.value())?;
                if record.id.is_empty() {
                    record.id = self.ids.generate();
                    patched.push((k.value().to_string(), record));
                } else if by_id
                    .get(record.id.as_str())
                    .map_err(map_redb_err)?
                    .is_none()
                {
                    by_id
                        .insert(record.id.as_str(), record.token_hash.as_str())
                        .map_err(map_redb_err)?;
                }
            }
            for (key, record) in &patched {
                let encoded = encode_json(record)?;
                tokens
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(map_redb_err)?;
                by_id
                    .insert(record.id.as_str(), record.token_hash.as_str())
                    .map_err(map_redb_err)?;
            }
            let mut meta_table = write_txn.open_table(META).map_err(map_redb_err)?;
            meta_table
                .insert(TOKEN_ID_DONE_KEY, "done")
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    /// Storage-overhaul backfill: rebuild `BLOB_REFCOUNT`,
    /// `DURABLE_BYTES_BY_OWNER`, the `REMOTE_CACHE` index, and
    /// `CACHE_ACCOUNTING` totals from `MEDIA_META`. Grandfathered over-quota
    /// durable rows are kept as-is (reads/deletes still work; only new
    /// uploads are quota-checked).
    ///
    /// Safety model (Batch C, `prod-ready.md` §4 C3): this runs inside
    /// `RedbStore::open`, before the handle serves requests and before the
    /// worker starts, so no concurrent media-counter mutation can race the
    /// rebuild. Derived state is rebuilt from zero in bounded batches; the
    /// done marker is written only with the final batch. Restartable and
    /// idempotent: a crash before the marker re-runs the rebuild on next
    /// open (a missing progress cursor means no batch committed, so derived
    /// tables are cleared first; a present cursor resumes without
    /// double-counting because each batch commits its deltas together with
    /// the advanced cursor).
    pub fn run_storage_accounting_backfill(&self) -> Result<(), PortError> {
        if self.meta_get(STORAGE_ACCOUNTING_DONE_KEY)?.as_deref() == Some("done") {
            return Ok(());
        }
        if self.meta_get(STORAGE_ACCOUNTING_PROGRESS_KEY)?.is_none() {
            // Fresh attempt: no batch committed yet (batches always commit
            // the cursor with their deltas), so clearing derived state is
            // safe and makes a crashed-attempt rerun converge.
            let write_txn = self.begin_write()?;
            {
                let mut refcounts = write_txn.open_table(BLOB_REFCOUNT).map_err(map_redb_err)?;
                let stale_refs: Vec<String> = refcounts
                    .iter()
                    .map_err(map_redb_err)?
                    .map(|entry| entry.map(|(k, _)| k.value().to_string()))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(map_redb_err)?;
                for key in &stale_refs {
                    refcounts.remove(key.as_str()).map_err(map_redb_err)?;
                }
                let mut durable = write_txn
                    .open_table(DURABLE_BYTES_BY_OWNER)
                    .map_err(map_redb_err)?;
                let stale_owners: Vec<String> = durable
                    .iter()
                    .map_err(map_redb_err)?
                    .map(|entry| entry.map(|(k, _)| k.value().to_string()))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(map_redb_err)?;
                for owner in &stale_owners {
                    durable.remove(owner.as_str()).map_err(map_redb_err)?;
                }
                let mut remote_cache = write_txn.open_table(REMOTE_CACHE).map_err(map_redb_err)?;
                let stale_index: Vec<(String, String)> = remote_cache
                    .iter()
                    .map_err(map_redb_err)?
                    .map(|entry| {
                        entry.map(|(k, _)| (k.value().0.to_string(), k.value().1.to_string()))
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(map_redb_err)?;
                for (url, variant) in &stale_index {
                    remote_cache
                        .remove((url.as_str(), variant.as_str()))
                        .map_err(map_redb_err)?;
                }
                // Drop the rebuilt `"stats"` row but keep the operator-set
                // `"cap"` row (persistent global cache cap).
                let mut accounting = write_txn
                    .open_table(CACHE_ACCOUNTING)
                    .map_err(map_redb_err)?;
                accounting.remove("stats").map_err(map_redb_err)?;
            }
            self.commit_write(write_txn)?;
        }
        loop {
            let progress = self
                .meta_get(STORAGE_ACCOUNTING_PROGRESS_KEY)?
                .unwrap_or_default();
            // Bounded read: at most `STORAGE_ACCOUNTING_BATCH` media rows
            // past the cursor, ascending id order.
            let scanned: Vec<MediaMeta> = {
                let read_txn = self.db.begin_read().map_err(map_redb_err)?;
                let media_table = read_txn.open_table(MEDIA_META).map_err(map_redb_err)?;
                let mut out = Vec::new();
                let range = media_table
                    .range(progress.as_str()..)
                    .map_err(map_redb_err)?;
                for entry in range {
                    let (k, v) = entry.map_err(map_redb_err)?;
                    let media_id: String = k.value().to_string();
                    if media_id.as_str() <= progress.as_str() {
                        continue;
                    }
                    out.push(decode_json::<MediaMeta>(v.value())?);
                    if out.len() >= STORAGE_ACCOUNTING_BATCH {
                        break;
                    }
                }
                out
            };
            let exhausted = scanned.len() < STORAGE_ACCOUNTING_BATCH;
            let write_txn = self.begin_write()?;
            {
                let mut refcounts = write_txn.open_table(BLOB_REFCOUNT).map_err(map_redb_err)?;
                let mut durable = write_txn
                    .open_table(DURABLE_BYTES_BY_OWNER)
                    .map_err(map_redb_err)?;
                let mut remote_cache = write_txn.open_table(REMOTE_CACHE).map_err(map_redb_err)?;
                let mut accounting = write_txn
                    .open_table(CACHE_ACCOUNTING)
                    .map_err(map_redb_err)?;
                let mut stats: CacheStats = match accounting.get("stats").map_err(map_redb_err)? {
                    Some(g) => decode_json(g.value())?,
                    None => CacheStats::default(),
                };
                for meta in &scanned {
                    let count = refcounts
                        .get(meta.blob_key.as_str())
                        .map_err(map_redb_err)?
                        .map(|g| g.value())
                        .unwrap_or(0);
                    refcounts
                        .insert(meta.blob_key.as_str(), count.saturating_add(1))
                        .map_err(map_redb_err)?;
                    if let Some(owner) = meta.owner_id.as_deref() {
                        let total = durable
                            .get(owner)
                            .map_err(map_redb_err)?
                            .map(|g| g.value())
                            .unwrap_or(0);
                        durable
                            .insert(owner, total.saturating_add(meta.size_bytes))
                            .map_err(map_redb_err)?;
                    } else {
                        // Remote row: index by (source_url, variant) for
                        // re-fetch and count toward cache totals.
                        let class = MediaClass::classify_mime(meta.mime.as_str());
                        match class {
                            MediaClass::Avatar => {
                                stats.avatar_bytes =
                                    stats.avatar_bytes.saturating_add(meta.size_bytes)
                            }
                            MediaClass::Preview => {
                                stats.preview_bytes =
                                    stats.preview_bytes.saturating_add(meta.size_bytes)
                            }
                            MediaClass::Full => {
                                stats.full_bytes = stats.full_bytes.saturating_add(meta.size_bytes)
                            }
                            MediaClass::AudioVideo => {}
                        }
                        stats.total_bytes = stats.total_bytes.saturating_add(meta.size_bytes);
                        if let Some(url) = meta.source_url.as_deref() {
                            let rec = RemoteCacheRecord {
                                source_url: url.to_string(),
                                variant: "original".to_string(),
                                media_id: meta.id.clone(),
                                blob_key: meta.blob_key.clone(),
                                size_bytes: meta.size_bytes,
                                class,
                                fetched_at_ms: 0,
                                last_access_ms: 0,
                                access_count: 0,
                            };
                            let encoded = encode_json(&rec)?;
                            remote_cache
                                .insert(
                                    (rec.source_url.as_str(), rec.variant.as_str()),
                                    encoded.as_slice(),
                                )
                                .map_err(map_redb_err)?;
                        }
                    }
                }
                let encoded = encode_json(&stats)?;
                accounting
                    .insert("stats", encoded.as_slice())
                    .map_err(map_redb_err)?;
                let mut meta_table = write_txn.open_table(META).map_err(map_redb_err)?;
                if let Some(last) = scanned.last() {
                    meta_table
                        .insert(STORAGE_ACCOUNTING_PROGRESS_KEY, last.id.as_str())
                        .map_err(map_redb_err)?;
                }
                if exhausted {
                    meta_table
                        .insert(STORAGE_ACCOUNTING_DONE_KEY, "done")
                        .map_err(map_redb_err)?;
                }
            }
            self.commit_write(write_txn)?;
            if exhausted {
                return Ok(());
            }
        }
    }

    /// WP5 federation backfill: build `ACCOUNTS_BY_URI` (trimmed non-empty
    /// `uri` → id) and `ACCOUNTS_BY_HANDLE` (canonical full handle → id)
    /// from `ACCOUNTS` once. Single transaction at open; idempotent via the
    /// META marker. Colliding URIs resolve last-writer-wins in id order
    /// (creation never enforced URI uniqueness); soft-deleted rows stay
    /// indexed (scan parity — `find_by_uri` never filtered on flags).
    pub fn run_account_uri_index_backfill(&self) -> Result<(), PortError> {
        if self.meta_get(ACCOUNT_URI_INDEX_DONE_KEY)?.as_deref() == Some("done") {
            return Ok(());
        }
        let write_txn = self.begin_write()?;
        {
            let accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
            let mut by_uri = write_txn
                .open_table(ACCOUNTS_BY_URI)
                .map_err(map_redb_err)?;
            let mut by_handle = write_txn
                .open_table(ACCOUNTS_BY_HANDLE)
                .map_err(map_redb_err)?;
            for entry in accounts_table.iter().map_err(map_redb_err)? {
                let (key, value) = entry.map_err(map_redb_err)?;
                let account: Account = decode_json(value.value())?;
                if let Some(uri) = account
                    .uri
                    .as_deref()
                    .map(str::trim)
                    .filter(|u| !u.is_empty())
                {
                    by_uri.insert(uri, key.value()).map_err(map_redb_err)?;
                }
                by_handle
                    .insert(Self::canonical_handle(&account).as_str(), key.value())
                    .map_err(map_redb_err)?;
            }
            let mut meta_table = write_txn.open_table(META).map_err(map_redb_err)?;
            meta_table
                .insert(ACCOUNT_URI_INDEX_DONE_KEY, "done")
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    /// WP5 federation backfill: build `NOTES_BY_QUOTE_REQUEST`
    /// (request IRI → quoting note id) and `NOTES_BY_QUOTE_STAMP` (stamp
    /// IRI → quoting note id) from live `NOTES` rows once. Single
    /// transaction at open; idempotent via the META marker. Deleted rows
    /// stay out (both lookups skip deleted notes); collisions resolve
    /// last-writer-wins in id order.
    pub fn run_quote_index_backfill(&self) -> Result<(), PortError> {
        if self.meta_get(QUOTE_INDEX_DONE_KEY)?.as_deref() == Some("done") {
            return Ok(());
        }
        let write_txn = self.begin_write()?;
        {
            let notes_table = write_txn.open_table(NOTES).map_err(map_redb_err)?;
            let mut by_request = write_txn
                .open_table(NOTES_BY_QUOTE_REQUEST)
                .map_err(map_redb_err)?;
            let mut by_stamp = write_txn
                .open_table(NOTES_BY_QUOTE_STAMP)
                .map_err(map_redb_err)?;
            for entry in notes_table.iter().map_err(map_redb_err)? {
                let (key, value) = entry.map_err(map_redb_err)?;
                let note: Note = decode_json(value.value())?;
                if note.is_deleted {
                    continue;
                }
                if let Some(quote) = note.quote.as_ref() {
                    if let Some(uri) = quote
                        .request_activity_uri
                        .as_deref()
                        .map(str::trim)
                        .filter(|u| !u.is_empty())
                    {
                        by_request.insert(uri, key.value()).map_err(map_redb_err)?;
                    }
                    if let Some(uri) = quote
                        .authorization_uri
                        .as_deref()
                        .map(str::trim)
                        .filter(|u| !u.is_empty())
                    {
                        by_stamp.insert(uri, key.value()).map_err(map_redb_err)?;
                    }
                }
            }
            let mut meta_table = write_txn.open_table(META).map_err(map_redb_err)?;
            meta_table
                .insert(QUOTE_INDEX_DONE_KEY, "done")
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn meta_get(&self, key: &str) -> Result<Option<String>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let meta_table = read_txn.open_table(META).map_err(map_redb_err)?;
        match meta_table.get(key).map_err(map_redb_err)? {
            Some(guard) => Ok(Some(guard.value().to_string())),
            None => Ok(None),
        }
    }

    /// Test hook: drop every `NOTES_BY_REPLY` row and clear the migration
    /// marker/cursor, simulating a database created before Slice A. The
    /// next `run_reply_index_backfill` (or reopen) must rebuild the index
    /// from stored notes without touching note rows, counters, or jobs.
    pub fn remove_reply_index_for_test(&self) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut index_table = write_txn.open_table(NOTES_BY_REPLY).map_err(map_redb_err)?;
            let keys: Vec<(String, String)> = {
                let mut out = Vec::new();
                for entry in index_table.iter().map_err(map_redb_err)? {
                    let (k, _) = entry.map_err(map_redb_err)?;
                    let (parent, child) = k.value();
                    out.push((parent.to_string(), child.to_string()));
                }
                out
            };
            for (parent, child) in &keys {
                index_table
                    .remove((parent.as_str(), child.as_str()))
                    .map_err(map_redb_err)?;
            }
            let mut meta_table = write_txn.open_table(META).map_err(map_redb_err)?;
            let _ = meta_table.remove(REPLY_INDEX_DONE_KEY);
            let _ = meta_table.remove(REPLY_INDEX_PROGRESS_KEY);
        }
        self.commit_write(write_txn)
    }
}
