use super::codec::map_redb_err;
use super::store::RedbStore;
use crate::application::ports::{NoteThreadMuteStore, PortError};
use redb::{ReadableDatabase, ReadableTable, TableDefinition};

/// Thread mutes `(user_id, note_id)` → () (gaps WP2a;
/// `notes/thread-muting/*`). The muted thread root is resolved by the op.
pub(crate) const THREAD_MUTES: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("thread_mutes");
impl NoteThreadMuteStore for RedbStore {
    fn mute_thread(&self, user_id: &str, note_id: &str) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        let inserted = {
            let mut table = write_txn.open_table(THREAD_MUTES).map_err(map_redb_err)?;
            let existed = table
                .get((user_id, note_id))
                .map_err(map_redb_err)?
                .is_some();
            if !existed {
                table
                    .insert((user_id, note_id), 1u8)
                    .map_err(map_redb_err)?;
            }
            !existed
        };
        self.commit_write(write_txn)?;
        Ok(inserted)
    }

    fn unmute_thread(&self, user_id: &str, note_id: &str) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        let removed = {
            let mut table = write_txn.open_table(THREAD_MUTES).map_err(map_redb_err)?;
            table
                .remove((user_id, note_id))
                .map_err(map_redb_err)?
                .is_some()
        };
        self.commit_write(write_txn)?;
        Ok(removed)
    }

    fn is_thread_muted(&self, user_id: &str, note_id: &str) -> Result<bool, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(THREAD_MUTES).map_err(map_redb_err)?;
        Ok(table
            .get((user_id, note_id))
            .map_err(map_redb_err)?
            .is_some())
    }
}
