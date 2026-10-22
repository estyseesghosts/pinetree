use super::codec::{decode_json, encode_json, map_redb_err};
use super::note_store::matches_cursor;
use super::store::RedbStore;
use crate::application::ports::{
    AntennaNoteStore, AntennaStore, NewJob, PortError, TimelineCursor,
};
use crate::domain::antenna::Antenna;
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};

/// Antennas by id (Batch N WS4; guide WP15). Values are JSON [`Antenna`] rows.
pub(crate) const ANTENNAS: TableDefinition<&str, &[u8]> = TableDefinition::new("antennas");
/// Owner index `(owner_id, antenna_id)` (listing + per-owner cap).
pub(crate) const ANTENNAS_BY_OWNER: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("antennas_by_owner");
/// Per-antenna feeds `(antenna_id, note_id)` (aidx note ids are
/// time-ordered, so reverse ranges read newest-first). Written with the trim
/// in the same transaction as the append.
pub(crate) const ANTENNA_NOTES: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("antenna_notes");
impl AntennaStore for RedbStore {
    fn create_antenna(&self, antenna: Antenna, jobs: Vec<NewJob>) -> Result<Antenna, PortError> {
        antenna.validate().map_err(|e| match e {
            crate::domain::id::DomainError::Validation(msg) => PortError::Validation(msg),
            crate::domain::id::DomainError::Conflict(msg) => PortError::Conflict(msg),
            crate::domain::id::DomainError::NotFound(msg) => PortError::NotFound(msg),
            crate::domain::id::DomainError::StorageUnavailable(msg) => {
                PortError::StorageUnavailable(msg)
            }
        })?;
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(ANTENNAS).map_err(map_redb_err)?;
            let encoded = encode_json(&antenna)?;
            table
                .insert(antenna.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
            drop(table);
            let mut by_owner = write_txn
                .open_table(ANTENNAS_BY_OWNER)
                .map_err(map_redb_err)?;
            by_owner
                .insert((antenna.owner_id.as_str(), antenna.id.as_str()), 1u8)
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(antenna)
    }

    fn update_antenna(&self, antenna: Antenna, jobs: Vec<NewJob>) -> Result<Antenna, PortError> {
        antenna.validate().map_err(|e| match e {
            crate::domain::id::DomainError::Validation(msg) => PortError::Validation(msg),
            crate::domain::id::DomainError::Conflict(msg) => PortError::Conflict(msg),
            crate::domain::id::DomainError::NotFound(msg) => PortError::NotFound(msg),
            crate::domain::id::DomainError::StorageUnavailable(msg) => {
                PortError::StorageUnavailable(msg)
            }
        })?;
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(ANTENNAS).map_err(map_redb_err)?;
            let existing: Option<Antenna> =
                match table.get(antenna.id.as_str()).map_err(map_redb_err)? {
                    Some(guard) => Some(decode_json(guard.value())?),
                    None => None,
                };
            match existing {
                Some(prev) if prev.owner_id == antenna.owner_id => {}
                _ => {
                    drop(table);
                    let _ = write_txn.abort();
                    return Err(PortError::NotFound("NO_SUCH_ANTENNA".to_string()));
                }
            }
            let encoded = encode_json(&antenna)?;
            table
                .insert(antenna.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(antenna)
    }

    fn delete_antenna(&self, owner_id: &str, id: &str) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        let removed = {
            let mut table = write_txn.open_table(ANTENNAS).map_err(map_redb_err)?;
            let existing: Option<Antenna> = match table.get(id).map_err(map_redb_err)? {
                Some(guard) => Some(decode_json(guard.value())?),
                None => None,
            };
            let Some(prev) = existing else {
                return Ok(false);
            };
            if prev.owner_id != owner_id {
                return Ok(false);
            }
            let _ = table.remove(id);
            drop(table);
            let mut by_owner = write_txn
                .open_table(ANTENNAS_BY_OWNER)
                .map_err(map_redb_err)?;
            let _ = by_owner.remove((owner_id, id));
            drop(by_owner);
            let mut notes_table = write_txn.open_table(ANTENNA_NOTES).map_err(map_redb_err)?;
            let stale: Vec<(String, String)> = notes_table
                .range((id, "")..=(id, "\u{10ffff}"))
                .map_err(map_redb_err)?
                .filter_map(|entry| {
                    entry.ok().map(|(k, _)| {
                        let (a, n) = k.value();
                        (a.to_string(), n.to_string())
                    })
                })
                .collect();
            for (a, n) in stale {
                let _ = notes_table.remove((a.as_str(), n.as_str()));
            }
            true
        };
        self.commit_write(write_txn)?;
        Ok(removed)
    }

    fn get_antenna(&self, owner_id: &str, id: &str) -> Result<Option<Antenna>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(ANTENNAS).map_err(map_redb_err)?;
        match table.get(id).map_err(map_redb_err)? {
            Some(guard) => {
                let antenna: Antenna = decode_json(guard.value())?;
                Ok(if antenna.owner_id == owner_id {
                    Some(antenna)
                } else {
                    None
                })
            }
            None => Ok(None),
        }
    }

    fn antennas_by_owner(&self, owner_id: &str) -> Result<Vec<Antenna>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_owner = read_txn
            .open_table(ANTENNAS_BY_OWNER)
            .map_err(map_redb_err)?;
        let table = read_txn.open_table(ANTENNAS).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in by_owner
            .range((owner_id, "")..=(owner_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
        {
            let (k, _) = entry.map_err(map_redb_err)?;
            let (_, antenna_id) = k.value();
            if let Some(guard) = table.get(antenna_id).map_err(map_redb_err)? {
                out.push(decode_json(guard.value())?);
            }
        }
        // Newest-first by antenna id (aidx ids are time-ordered).
        out.sort_by(|a: &Antenna, b: &Antenna| b.id.cmp(&a.id));
        Ok(out)
    }

    fn all_active_antennas(&self) -> Result<Vec<Antenna>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(ANTENNAS).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (_, v) = entry.map_err(map_redb_err)?;
            let antenna: Antenna = decode_json(v.value())?;
            if antenna.is_active {
                out.push(antenna);
            }
        }
        Ok(out)
    }

    fn touch_antenna(&self, owner_id: &str, id: &str, now_ms: i64) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(ANTENNAS).map_err(map_redb_err)?;
            let existing: Option<Antenna> = match table.get(id).map_err(map_redb_err)? {
                Some(guard) => Some(decode_json(guard.value())?),
                None => None,
            };
            if let Some(mut antenna) = existing {
                if antenna.owner_id != owner_id {
                    return Ok(());
                }
                antenna.is_active = true;
                antenna.last_used_at_ms = now_ms;
                let encoded = encode_json(&antenna)?;
                table.insert(id, encoded.as_slice()).map_err(map_redb_err)?;
            }
        }
        self.commit_write(write_txn)?;
        Ok(())
    }
}

impl AntennaNoteStore for RedbStore {
    fn append_antenna_note(&self, antenna_id: &str, note_id: &str) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(ANTENNA_NOTES).map_err(map_redb_err)?;
            table
                .insert((antenna_id, note_id), 1u8)
                .map_err(map_redb_err)?;
            // Trim to the newest CAP entries: note ids are time-ordered, so
            // the oldest rows sort first — drop the overflow head.
            let ids: Vec<String> = table
                .range((antenna_id, "")..=(antenna_id, "\u{10ffff}"))
                .map_err(map_redb_err)?
                .filter_map(|entry| {
                    entry.ok().map(|(k, _)| {
                        let (_, n) = k.value();
                        n.to_string()
                    })
                })
                .collect();
            if ids.len() > crate::domain::antenna::ANTENNA_NOTES_CAP {
                let drop_n = ids.len() - crate::domain::antenna::ANTENNA_NOTES_CAP;
                for stale in ids.iter().take(drop_n) {
                    let _ = table.remove((antenna_id, stale.as_str()));
                }
            }
        }
        self.commit_write(write_txn)?;
        Ok(())
    }

    fn remove_antenna_note(&self, antenna_id: &str, note_id: &str) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(ANTENNA_NOTES).map_err(map_redb_err)?;
            let _ = table.remove((antenna_id, note_id));
        }
        self.commit_write(write_txn)?;
        Ok(())
    }

    fn delete_antenna_notes(&self, antenna_id: &str) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(ANTENNA_NOTES).map_err(map_redb_err)?;
            let stale: Vec<String> = table
                .range((antenna_id, "")..=(antenna_id, "\u{10ffff}"))
                .map_err(map_redb_err)?
                .filter_map(|entry| {
                    entry.ok().map(|(k, _)| {
                        let (_, n) = k.value();
                        n.to_string()
                    })
                })
                .collect();
            for stale in stale {
                let _ = table.remove((antenna_id, stale.as_str()));
            }
        }
        self.commit_write(write_txn)?;
        Ok(())
    }

    fn antenna_notes(
        &self,
        antenna_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<String>, PortError> {
        let limit = if cursor.limit == 0 {
            10
        } else {
            cursor.limit.min(100)
        };
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(ANTENNA_NOTES).map_err(map_redb_err)?;
        let mut ids: Vec<String> = table
            .range((antenna_id, "")..=(antenna_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
            .filter_map(|entry| {
                entry.ok().map(|(k, _)| {
                    let (_, n) = k.value();
                    n.to_string()
                })
            })
            .collect();
        // Newest-first by note id (aidx ids are time-ordered).
        ids.sort_by(|a, b| b.cmp(a));
        ids.retain(|id| matches_cursor(id, cursor));
        ids.truncate(limit);
        Ok(ids)
    }
}
