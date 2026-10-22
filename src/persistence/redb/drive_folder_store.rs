use super::codec::{decode_json, encode_json, map_redb_err};
use super::store::RedbStore;
use crate::application::ports::{DriveFolderStore, NewJob, PortError};
use crate::domain::drive_folder::DriveFolder;
use redb::{
    ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition, WriteTransaction,
};

/// Drive folders by id (WP2 part A; Misskey `MiDriveFolder` parity).
/// Values are JSON [`DriveFolder`] rows.
pub(crate) const DRIVE_FOLDERS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("drive_folders");
/// Owner placement index `(owner_id, parent_key, folder_id)` (WP2 part A),
/// where `parent_key` is the parent folder id or `""` for the drive root
/// (aidx ids are never empty, so `""` never collides). Prefix ranges serve
/// `child_folders`; every mutation commits row + indexes + jobs in one txn.
pub(crate) const DRIVE_FOLDERS_BY_OWNER_PARENT: TableDefinition<(&str, &str, &str), u8> =
    TableDefinition::new("drive_folders_by_owner_parent");
/// Owner name index `(owner_id, parent_key, name)` → JSON sorted folder-id
/// list (WP2 part A; `drive/folders/find` parity). One row per name bucket
/// (arbitrary names, no charset games); the id list stays id-ordered.
pub(crate) const DRIVE_FOLDERS_BY_NAME: TableDefinition<(&str, &str, &str), &[u8]> =
    TableDefinition::new("drive_folders_by_name");
/// File-to-folder placement `file_id` → folder id (WP2 part A; upstream
/// nullable `folderId` column parity). Absent row = drive root; a row always
/// names a live folder (the op empties folders before delete).
pub(crate) const DRIVE_FILE_FOLDERS: TableDefinition<&str, &str> =
    TableDefinition::new("drive_file_folders");
/// Reverse placement index `(folder_id, file_id)` (WP2 part A) backing
/// `count_files_in_folder`. Only non-root placements are indexed.
pub(crate) const DRIVE_FILES_IN_FOLDER: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("drive_files_in_folder");
impl RedbStore {
    /// Storage key for a folder parent: the parent id, or `""` for the
    /// drive root (aidx ids are never empty, so `""` never collides).
    fn folder_parent_key(parent_id: Option<&str>) -> &str {
        parent_id.unwrap_or("")
    }

    /// Append `folder_id` to the `(owner, parent, name)` bucket, keeping
    /// the id list ordered. Buckets hold one name's folders, so they stay
    /// tiny; the whole bucket rewrites in the ambient transaction.
    fn drive_name_bucket_add(
        write_txn: &WriteTransaction,
        owner_id: &str,
        parent_key: &str,
        name: &str,
        folder_id: &str,
    ) -> Result<(), PortError> {
        let mut bucket_table = write_txn
            .open_table(DRIVE_FOLDERS_BY_NAME)
            .map_err(map_redb_err)?;
        let mut ids: Vec<String> = match bucket_table
            .get((owner_id, parent_key, name))
            .map_err(map_redb_err)?
        {
            Some(guard) => decode_json(guard.value())?,
            None => Vec::new(),
        };
        if !ids.iter().any(|id| id == folder_id) {
            ids.push(folder_id.to_string());
            ids.sort();
            let encoded = encode_json(&ids)?;
            bucket_table
                .insert((owner_id, parent_key, name), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        Ok(())
    }

    /// Drop `folder_id` from the `(owner, parent, name)` bucket, removing
    /// the bucket row once empty.
    fn drive_name_bucket_remove(
        write_txn: &WriteTransaction,
        owner_id: &str,
        parent_key: &str,
        name: &str,
        folder_id: &str,
    ) -> Result<(), PortError> {
        let mut bucket_table = write_txn
            .open_table(DRIVE_FOLDERS_BY_NAME)
            .map_err(map_redb_err)?;
        let mut ids: Vec<String> = match bucket_table
            .get((owner_id, parent_key, name))
            .map_err(map_redb_err)?
        {
            Some(guard) => decode_json(guard.value())?,
            None => return Ok(()),
        };
        let before = ids.len();
        ids.retain(|id| id != folder_id);
        if ids.len() == before {
            return Ok(());
        }
        if ids.is_empty() {
            bucket_table
                .remove((owner_id, parent_key, name))
                .map_err(map_redb_err)?;
        } else {
            let encoded = encode_json(&ids)?;
            bucket_table
                .insert((owner_id, parent_key, name), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        Ok(())
    }
}
impl DriveFolderStore for RedbStore {
    fn create_folder(
        &self,
        folder: DriveFolder,
        jobs: Vec<NewJob>,
    ) -> Result<DriveFolder, PortError> {
        folder.validate().map_err(|e| match e {
            crate::domain::id::DomainError::Validation(msg) => PortError::Validation(msg),
            crate::domain::id::DomainError::Conflict(msg) => PortError::Conflict(msg),
            crate::domain::id::DomainError::NotFound(msg) => PortError::NotFound(msg),
            crate::domain::id::DomainError::StorageUnavailable(msg) => {
                PortError::StorageUnavailable(msg)
            }
        })?;
        for job in &jobs {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        {
            let mut folders = write_txn.open_table(DRIVE_FOLDERS).map_err(map_redb_err)?;
            let encoded = encode_json(&folder)?;
            folders
                .insert(folder.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
            drop(folders);
            let mut by_parent = write_txn
                .open_table(DRIVE_FOLDERS_BY_OWNER_PARENT)
                .map_err(map_redb_err)?;
            by_parent
                .insert(
                    (
                        folder.owner_id.as_str(),
                        Self::folder_parent_key(folder.parent_id.as_deref()),
                        folder.id.as_str(),
                    ),
                    &1u8,
                )
                .map_err(map_redb_err)?;
            drop(by_parent);
            Self::drive_name_bucket_add(
                &write_txn,
                folder.owner_id.as_str(),
                Self::folder_parent_key(folder.parent_id.as_deref()),
                folder.name.as_str(),
                folder.id.as_str(),
            )?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(folder)
    }

    fn get_folder(&self, id: &str) -> Result<Option<DriveFolder>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let folders = read_txn.open_table(DRIVE_FOLDERS).map_err(map_redb_err)?;
        match folders.get(id).map_err(map_redb_err)? {
            Some(guard) => Ok(Some(decode_json(guard.value())?)),
            None => Ok(None),
        }
    }

    fn update_folder(
        &self,
        folder: DriveFolder,
        jobs: Vec<NewJob>,
    ) -> Result<DriveFolder, PortError> {
        folder.validate().map_err(|e| match e {
            crate::domain::id::DomainError::Validation(msg) => PortError::Validation(msg),
            crate::domain::id::DomainError::Conflict(msg) => PortError::Conflict(msg),
            crate::domain::id::DomainError::NotFound(msg) => PortError::NotFound(msg),
            crate::domain::id::DomainError::StorageUnavailable(msg) => {
                PortError::StorageUnavailable(msg)
            }
        })?;
        for job in &jobs {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        {
            let mut folders = write_txn.open_table(DRIVE_FOLDERS).map_err(map_redb_err)?;
            let previous: Option<DriveFolder> =
                match folders.get(folder.id.as_str()).map_err(map_redb_err)? {
                    Some(guard) => Some(decode_json(guard.value())?),
                    None => None,
                };
            let Some(previous) = previous else {
                drop(folders);
                let _ = write_txn.abort();
                return Err(PortError::NotFound("NO_SUCH_FOLDER".to_string()));
            };
            let encoded = encode_json(&folder)?;
            folders
                .insert(folder.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
            drop(folders);
            // Re-home both indexes when placement or the name moved.
            if previous.owner_id != folder.owner_id || previous.parent_id != folder.parent_id {
                let mut by_parent = write_txn
                    .open_table(DRIVE_FOLDERS_BY_OWNER_PARENT)
                    .map_err(map_redb_err)?;
                by_parent
                    .remove((
                        previous.owner_id.as_str(),
                        Self::folder_parent_key(previous.parent_id.as_deref()),
                        previous.id.as_str(),
                    ))
                    .map_err(map_redb_err)?;
                by_parent
                    .insert(
                        (
                            folder.owner_id.as_str(),
                            Self::folder_parent_key(folder.parent_id.as_deref()),
                            folder.id.as_str(),
                        ),
                        &1u8,
                    )
                    .map_err(map_redb_err)?;
            }
            if previous.owner_id != folder.owner_id
                || previous.parent_id != folder.parent_id
                || previous.name != folder.name
            {
                Self::drive_name_bucket_remove(
                    &write_txn,
                    previous.owner_id.as_str(),
                    Self::folder_parent_key(previous.parent_id.as_deref()),
                    previous.name.as_str(),
                    previous.id.as_str(),
                )?;
                Self::drive_name_bucket_add(
                    &write_txn,
                    folder.owner_id.as_str(),
                    Self::folder_parent_key(folder.parent_id.as_deref()),
                    folder.name.as_str(),
                    folder.id.as_str(),
                )?;
            }
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(folder)
    }

    fn delete_folder(&self, id: &str, jobs: Vec<NewJob>) -> Result<(), PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        {
            let mut folders = write_txn.open_table(DRIVE_FOLDERS).map_err(map_redb_err)?;
            let previous: Option<DriveFolder> = match folders.get(id).map_err(map_redb_err)? {
                Some(guard) => Some(decode_json(guard.value())?),
                None => None,
            };
            let Some(previous) = previous else {
                drop(folders);
                let _ = write_txn.abort();
                return Err(PortError::NotFound("NO_SUCH_FOLDER".to_string()));
            };
            folders.remove(id).map_err(map_redb_err)?;
            drop(folders);
            let mut by_parent = write_txn
                .open_table(DRIVE_FOLDERS_BY_OWNER_PARENT)
                .map_err(map_redb_err)?;
            by_parent
                .remove((
                    previous.owner_id.as_str(),
                    Self::folder_parent_key(previous.parent_id.as_deref()),
                    previous.id.as_str(),
                ))
                .map_err(map_redb_err)?;
            drop(by_parent);
            Self::drive_name_bucket_remove(
                &write_txn,
                previous.owner_id.as_str(),
                Self::folder_parent_key(previous.parent_id.as_deref()),
                previous.name.as_str(),
                previous.id.as_str(),
            )?;
            // Files mapped here keep their placement rows: the op refuses
            // non-empty deletes, so no row can name this folder. The
            // reverse rows name live folders only.
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)
    }

    fn child_folders(
        &self,
        owner_id: &str,
        parent_id: Option<&str>,
    ) -> Result<Vec<DriveFolder>, PortError> {
        let parent_key = Self::folder_parent_key(parent_id);
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_parent = read_txn
            .open_table(DRIVE_FOLDERS_BY_OWNER_PARENT)
            .map_err(map_redb_err)?;
        let folders = read_txn.open_table(DRIVE_FOLDERS).map_err(map_redb_err)?;
        // The triple key orders by folder id, so the range reads id-ordered.
        let mut out = Vec::new();
        for entry in by_parent
            .range((owner_id, parent_key, "")..=(owner_id, parent_key, "\u{10ffff}"))
            .map_err(map_redb_err)?
        {
            let (key, _) = entry.map_err(map_redb_err)?;
            let (_, _, folder_id) = key.value();
            if let Some(guard) = folders.get(folder_id).map_err(map_redb_err)? {
                out.push(decode_json(guard.value())?);
            }
        }
        Ok(out)
    }

    fn find_folders_by_name(
        &self,
        owner_id: &str,
        name: &str,
        parent_id: Option<&str>,
    ) -> Result<Vec<DriveFolder>, PortError> {
        let parent_key = Self::folder_parent_key(parent_id);
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let buckets = read_txn
            .open_table(DRIVE_FOLDERS_BY_NAME)
            .map_err(map_redb_err)?;
        let ids: Vec<String> = match buckets
            .get((owner_id, parent_key, name))
            .map_err(map_redb_err)?
        {
            Some(guard) => decode_json(guard.value())?,
            None => return Ok(Vec::new()),
        };
        let folders = read_txn.open_table(DRIVE_FOLDERS).map_err(map_redb_err)?;
        // Buckets stay id-ordered at write time, so this reads id-ordered.
        let mut out = Vec::new();
        for id in &ids {
            if let Some(guard) = folders.get(id.as_str()).map_err(map_redb_err)? {
                let folder: DriveFolder = decode_json(guard.value())?;
                if folder.owner_id == owner_id {
                    out.push(folder);
                }
            }
        }
        Ok(out)
    }

    fn count_child_folders(&self, folder_id: &str) -> Result<u64, PortError> {
        // Parents are globally unique, so no owner prefix applies: one
        // bounded pass over the placement index counts exact parent hits.
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_parent = read_txn
            .open_table(DRIVE_FOLDERS_BY_OWNER_PARENT)
            .map_err(map_redb_err)?;
        let mut total = 0u64;
        for entry in by_parent.iter().map_err(map_redb_err)? {
            let (key, _) = entry.map_err(map_redb_err)?;
            let (_, parent, _) = key.value();
            if parent == folder_id {
                total = total.saturating_add(1);
            }
        }
        Ok(total)
    }

    fn set_file_folder(
        &self,
        file_id: &str,
        folder_id: Option<&str>,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let write_txn = self.begin_write()?;
        {
            let mut forward = write_txn
                .open_table(DRIVE_FILE_FOLDERS)
                .map_err(map_redb_err)?;
            let previous: Option<String> = match forward.get(file_id).map_err(map_redb_err)? {
                Some(guard) => Some(guard.value().to_string()),
                None => None,
            };
            if previous.as_deref() != folder_id {
                match (previous, folder_id) {
                    (Some(old), Some(target)) => {
                        forward.insert(file_id, target).map_err(map_redb_err)?;
                        drop(forward);
                        let mut reverse = write_txn
                            .open_table(DRIVE_FILES_IN_FOLDER)
                            .map_err(map_redb_err)?;
                        reverse
                            .remove((old.as_str(), file_id))
                            .map_err(map_redb_err)?;
                        reverse
                            .insert((target, file_id), &1u8)
                            .map_err(map_redb_err)?;
                    }
                    (Some(old), None) => {
                        forward.remove(file_id).map_err(map_redb_err)?;
                        drop(forward);
                        let mut reverse = write_txn
                            .open_table(DRIVE_FILES_IN_FOLDER)
                            .map_err(map_redb_err)?;
                        reverse
                            .remove((old.as_str(), file_id))
                            .map_err(map_redb_err)?;
                    }
                    (None, Some(target)) => {
                        forward.insert(file_id, target).map_err(map_redb_err)?;
                        drop(forward);
                        let mut reverse = write_txn
                            .open_table(DRIVE_FILES_IN_FOLDER)
                            .map_err(map_redb_err)?;
                        reverse
                            .insert((target, file_id), &1u8)
                            .map_err(map_redb_err)?;
                    }
                    (None, None) => {}
                }
            }
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)
    }

    fn file_folder_id(&self, file_id: &str) -> Result<Option<Option<String>>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let forward = read_txn
            .open_table(DRIVE_FILE_FOLDERS)
            .map_err(map_redb_err)?;
        match forward.get(file_id).map_err(map_redb_err)? {
            Some(guard) => Ok(Some(Some(guard.value().to_string()))),
            None => Ok(None),
        }
    }

    fn count_files_in_folder(&self, folder_id: &str) -> Result<u64, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let reverse = read_txn
            .open_table(DRIVE_FILES_IN_FOLDER)
            .map_err(map_redb_err)?;
        let mut total = 0u64;
        for entry in reverse
            .range((folder_id, "")..=(folder_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
        {
            entry.map_err(map_redb_err)?;
            total = total.saturating_add(1);
        }
        Ok(total)
    }
}

// --- Webhooks (WP3) ----------------------------------------------------------
