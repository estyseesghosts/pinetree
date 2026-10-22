use super::codec::{decode_json, encode_json, map_redb_err};
use super::store::RedbStore;
use crate::application::commands::announcements::Announcement;
use crate::application::commands::announcements::AnnouncementStore;
use crate::application::ports::{NewJob, PortError};
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use std::collections::HashSet;

/// Announcements by id (gaps WP2b). Values are JSON
/// [`Announcement`](crate::application::commands::announcements::Announcement) rows.
pub(crate) const ANNOUNCEMENTS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("announcements");
/// Announcement reads `(announcement_id, user_id)` → () (idempotent marks).
pub(crate) const ANNOUNCE_READS: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("announce_reads");
/// Promo reads `user_id` → () (one flag per user).
pub(crate) const PROMO_READS: TableDefinition<&str, u8> = TableDefinition::new("promo_reads");
impl AnnouncementStore for RedbStore {
    fn insert_announcement(
        &self,
        announcement: Announcement,
        jobs: Vec<NewJob>,
    ) -> Result<Announcement, PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(ANNOUNCEMENTS).map_err(map_redb_err)?;
            let encoded = encode_json(&announcement)?;
            table
                .insert(announcement.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(announcement)
    }

    fn get_announcement(&self, announcement_id: &str) -> Result<Option<Announcement>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(ANNOUNCEMENTS).map_err(map_redb_err)?;
        match table.get(announcement_id).map_err(map_redb_err)? {
            Some(value) => Ok(Some(decode_json(value.value())?)),
            None => Ok(None),
        }
    }

    fn all_announcements(&self) -> Result<Vec<Announcement>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(ANNOUNCEMENTS).map_err(map_redb_err)?;
        let mut out: Vec<Announcement> = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (_, value) = entry.map_err(map_redb_err)?;
            out.push(decode_json(value.value())?);
        }
        out.sort_by(|a: &Announcement, b: &Announcement| {
            b.created_at_ms.cmp(&a.created_at_ms).then(b.id.cmp(&a.id))
        });
        Ok(out)
    }

    fn mark_announcement_read(
        &self,
        announcement_id: &str,
        user_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Announcement>, PortError> {
        let write_txn = self.begin_write()?;
        let row = {
            let announcements = write_txn.open_table(ANNOUNCEMENTS).map_err(map_redb_err)?;
            match announcements.get(announcement_id).map_err(map_redb_err)? {
                Some(value) => decode_json(value.value())?,
                None => return Ok(None),
            }
        };
        {
            let mut reads = write_txn.open_table(ANNOUNCE_READS).map_err(map_redb_err)?;
            reads
                .insert((announcement_id, user_id), 1u8)
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(Some(row))
    }

    fn read_announcement_ids(&self, user_id: &str) -> Result<HashSet<String>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(ANNOUNCE_READS).map_err(map_redb_err)?;
        let mut out = HashSet::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (key, _) = entry.map_err(map_redb_err)?;
            if key.value().1 == user_id {
                out.insert(key.value().0.to_string());
            }
        }
        Ok(out)
    }

    fn mark_promo_read(&self, user_id: &str, jobs: Vec<NewJob>) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        let inserted = {
            let mut table = write_txn.open_table(PROMO_READS).map_err(map_redb_err)?;
            let existed = table.get(user_id).map_err(map_redb_err)?.is_some();
            if !existed {
                table.insert(user_id, 1u8).map_err(map_redb_err)?;
            }
            !existed
        };
        if inserted {
            self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        }
        self.commit_write(write_txn)?;
        Ok(inserted)
    }

    fn promo_is_read(&self, user_id: &str) -> Result<bool, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(PROMO_READS).map_err(map_redb_err)?;
        Ok(table.get(user_id).map_err(map_redb_err)?.is_some())
    }
}

// ===== Batch F backup support (append-only region) ========================
// Logical online snapshot/export for `docs/prod-ready.md` §7 Batch F.
//
// The pinned redb 4.4 `Database` API has no online backup method and a live
// file must not be copied blindly, so backup exports every table through
// its declared types inside ONE read transaction (a stable snapshot view;
// writers proceed concurrently under MVCC) and restore re-imports through
// the same definitions into a fresh database.
//
// Lock order with Batch C's at-open backfill (which runs inside
// `RedbStore::open` with write txns only): the backup media gate is a leaf
// lock acquired BEFORE any DB transaction and never re-acquired while one
// is held; the backfill never touches the gate or the pools, and no backup
// can exist before `open` returns. No lock-order inversion, no recursive
// acquisition.
//
// The registry below is the ONE canonical table list shared by schema
// creation order, export, import, and the completeness test: `init_tables`
// opens these tables, `export_backup_snapshot` / `import_backup_rows`
// dispatch on these names, and `live_table_names` (via redb's own
// `list_tables`) lets the test fail when a new table lacks backup coverage.
// There are no multimap tables in this schema; `live_multimap_names` plus
// the test pin that invariant so a future multimap fails loudly instead of
// silently skipping backup coverage.
