use super::codec::{decode_json, encode_json, map_redb_err};
use super::note_store::matches_cursor;
use super::store::RedbStore;
use crate::application::ports::{FavoriteStore, NewJob, PortError, TimelineCursor};
use crate::domain::favorite::NoteFavorite;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};

/// Note favorites keyed `(user_id, note_id)` (`UNIQUE(user,note)`;
/// Misskey `MiNoteFavorite` parity). Values are JSON [`NoteFavorite`] rows.
pub(crate) const FAVORITES: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("favorites");
impl FavoriteStore for RedbStore {
    fn favorite_note(
        &self,
        favorite: NoteFavorite,
        jobs: Vec<NewJob>,
    ) -> Result<NoteFavorite, PortError> {
        favorite
            .validate()
            .map_err(|e| PortError::Validation(format!("{e:?}")))?;
        let write_txn = self.begin_write()?;
        {
            let mut fav_table = write_txn.open_table(FAVORITES).map_err(map_redb_err)?;
            if fav_table
                .get((favorite.user_id.as_str(), favorite.note_id.as_str()))
                .map_err(map_redb_err)?
                .is_some()
            {
                drop(fav_table);
                let _ = write_txn.abort();
                return Err(PortError::Conflict("ALREADY_FAVORITED".to_string()));
            }
            let encoded = encode_json(&favorite)?;
            fav_table
                .insert(
                    (favorite.user_id.as_str(), favorite.note_id.as_str()),
                    encoded.as_slice(),
                )
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(favorite)
    }

    fn unfavorite_note(&self, user_id: &str, note_id: &str) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        let removed = {
            let mut fav_table = write_txn.open_table(FAVORITES).map_err(map_redb_err)?;
            fav_table
                .remove((user_id, note_id))
                .map_err(map_redb_err)?
                .is_some()
        };
        self.commit_write(write_txn)?;
        Ok(removed)
    }

    fn is_favorited(&self, user_id: &str, note_id: &str) -> Result<bool, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let fav_table = read_txn.open_table(FAVORITES).map_err(map_redb_err)?;
        Ok(fav_table
            .get((user_id, note_id))
            .map_err(map_redb_err)?
            .is_some())
    }

    fn favorites_by_user(
        &self,
        user_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<NoteFavorite>, PortError> {
        let limit = if cursor.limit == 0 { 10 } else { cursor.limit };
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let fav_table = read_txn.open_table(FAVORITES).map_err(map_redb_err)?;
        let mut out: Vec<NoteFavorite> = Vec::new();
        for entry in fav_table
            .range((user_id, "")..=(user_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
        {
            let (_, v) = entry.map_err(map_redb_err)?;
            out.push(decode_json(v.value())?);
        }
        // Newest-first by favorite id (aidx ids are time-ordered).
        out.sort_by(|a: &NoteFavorite, b: &NoteFavorite| b.id.cmp(&a.id));
        out.retain(|f| matches_cursor(&f.id, cursor));
        out.truncate(limit);
        Ok(out)
    }
}
