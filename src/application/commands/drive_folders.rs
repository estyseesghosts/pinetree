//! Drive folders + folder-aware file bulk ops (WP2 part A).
//!
//! One application operation per Misskey `drive/folders/*` endpoint plus the
//! folder-aware `drive/files/*` bulk surfaces, shared by the Misskey
//! adapter. Upstream reference: `packages/backend/src/server/api/endpoints/`
//! `drive/folders/{create,delete,find,show,update}.ts` and
//! `drive/files/{find,move-bulk,check-existence,attached-notes}.ts` plus
//! `drive/stream.ts` (Misskey `develop`).
//!
//! Store wiring (parent merge): the [`DriveFolderStore`] trait below is
//! PROPOSED for `src/application/ports.rs`, with a redb implementation in
//! `src/persistence/redb/mod.rs` and an `Arc<dyn DriveFolderStore>` field on
//! [`AppServices`]. Until then every op takes the store explicitly as
//! `folders: &dyn DriveFolderStore`, so this module compiles and tests green
//! with an in-memory fake. The merge drops the explicit parameter.
//!
//! Error-code discipline: every failure carries the verbatim Misskey `code`
//! (`NO_SUCH_FOLDER`, `NO_SUCH_PARENT_FOLDER`, `RECURSIVE_NESTING`,
//! `HAS_CHILD_FILES_OR_FOLDERS`, `NO_SUCH_FILE`); the per-endpoint UUIDs
//! live in the dispatcher's error map (see the WP2 report) because the same
//! code has a different id per endpoint upstream.
//!
//! DIVERGENCES from upstream:
//! - `move-bulk` with a missing folder: upstream `DriveService.moveFiles`
//!   does `findOneByOrFail` (HTTP 500); we return `NotFound(NO_SUCH_FOLDER)`
//!   with a pinetree-minted UUID (dispatcher map) instead of 500ing.
//! - Folder/file events (`folderCreated`, `folderUpdated`, `folderDeleted`,
//!   `fileUpdated`, `fileCreated` on the `drive` stream channel) are not
//!   emitted: the `drive` streaming channel is WP4 scope. State commits are
//!   still one-transaction (row + index + jobs) per store call.

use super::AppServices;

use crate::application::ports::{NewJob, PortError, TimelineCursor};
use crate::domain::drive_folder::{
    require_folder_empty, validate_folder_name, validate_folder_parent_change,
    validate_new_folder, DriveFolder,
};
use crate::domain::id::DomainError;
use crate::domain::media::MediaMeta;
use crate::domain::note::Note;

fn domain_err(e: DomainError) -> PortError {
    match e {
        DomainError::Validation(msg) => PortError::Validation(msg),
        DomainError::Conflict(msg) => PortError::Conflict(msg),
        DomainError::NotFound(msg) => PortError::NotFound(msg),
        DomainError::StorageUnavailable(msg) => PortError::StorageUnavailable(msg),
    }
}

/// Drive-folder + file-folder-index store (PROPOSED: move to
/// `src/application/ports.rs`; redb implements, parent wires).
///
/// One storage transaction commits each mutation with `jobs` (same
/// discipline as `MediaStore`/`UserListStore`). The file-folder index maps
/// media rows to folders: files with no mapping row read as drive-root
/// (`folder_id == None`), matching upstream's nullable `folderId` column.
pub trait DriveFolderStore: Send + Sync {
    /// Insert one folder row with `jobs`, one transaction.
    fn create_folder(
        &self,
        folder: DriveFolder,
        jobs: Vec<NewJob>,
    ) -> Result<DriveFolder, PortError>;
    /// The live folder `id`, if any. No ownership check (the op enforces
    /// it so error precedence matches upstream: miss and foreign both read
    /// as `NO_SUCH_FOLDER`).
    fn get_folder(&self, id: &str) -> Result<Option<DriveFolder>, PortError>;
    /// Rewrite name/parent with `jobs`, one transaction.
    fn update_folder(
        &self,
        folder: DriveFolder,
        jobs: Vec<NewJob>,
    ) -> Result<DriveFolder, PortError>;
    /// Delete the row with `jobs`, one transaction. The op checks
    /// emptiness first (`HAS_CHILD_FILES_OR_FOLDERS`).
    fn delete_folder(&self, id: &str, jobs: Vec<NewJob>) -> Result<(), PortError>;
    /// Owner's folders under `parent_id` (`None` = drive root), id-ordered.
    fn child_folders(
        &self,
        owner_id: &str,
        parent_id: Option<&str>,
    ) -> Result<Vec<DriveFolder>, PortError>;
    /// `drive/folders/find` parity: owner's folders with `name` under
    /// `parent_id` (`None` = root).
    fn find_folders_by_name(
        &self,
        owner_id: &str,
        name: &str,
        parent_id: Option<&str>,
    ) -> Result<Vec<DriveFolder>, PortError>;
    /// Number of folders whose parent is `folder_id`.
    fn count_child_folders(&self, folder_id: &str) -> Result<u64, PortError>;
    /// Point `file_id` at `folder_id` (`None` = drive root) with `jobs`,
    /// one transaction.
    fn set_file_folder(
        &self,
        file_id: &str,
        folder_id: Option<&str>,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError>;
    /// Current placement: `None` = no mapping row (drive root by default),
    /// `Some(x)` = mapped (possibly root via `Some(None)`).
    fn file_folder_id(&self, file_id: &str) -> Result<Option<Option<String>>, PortError>;
    /// Number of files mapped directly under `folder_id`.
    fn count_files_in_folder(&self, folder_id: &str) -> Result<u64, PortError>;
}

impl AppServices {
    /// `drive/folders/create {name = "Untitled", parentId = null}`.
    /// `NO_SUCH_FOLDER` (verbatim) when a requested parent is missing or
    /// foreign.
    pub fn create_drive_folder(
        &self,
        folders: &dyn DriveFolderStore,
        owner_id: &str,
        name: Option<String>,
        parent_id: Option<String>,
    ) -> Result<DriveFolder, PortError> {
        self.accounts.get_owner(owner_id)?;
        let name = name.unwrap_or_else(|| "Untitled".to_string());
        let parent = match parent_id {
            None => None,
            Some(pid) => match folders.get_folder(&pid)? {
                Some(p) if p.owner_id == owner_id => Some(p),
                _ => return Err(PortError::NotFound("NO_SUCH_FOLDER".to_string())),
            },
        };
        validate_new_folder(owner_id, &name, parent.as_ref()).map_err(domain_err)?;
        let folder = DriveFolder {
            id: self.ids.generate(),
            owner_id: owner_id.to_string(),
            name,
            parent_id: parent.map(|p| p.id),
        };
        folders.create_folder(folder, Vec::new())
    }

    /// `drive/folders/show {folderId}`. Miss and foreign both read as
    /// `NO_SUCH_FOLDER` (upstream `findOneBy({id, userId})` parity).
    pub fn show_drive_folder(
        &self,
        folders: &dyn DriveFolderStore,
        owner_id: &str,
        folder_id: &str,
    ) -> Result<DriveFolder, PortError> {
        self.accounts.get_owner(owner_id)?;
        match folders.get_folder(folder_id)? {
            Some(f) if f.owner_id == owner_id => Ok(f),
            _ => Err(PortError::NotFound("NO_SUCH_FOLDER".to_string())),
        }
    }

    /// `drive/folders/find {name, parentId = null}`: owner's folders with
    /// `name` directly under `parent_id` (`None` = drive root).
    pub fn find_drive_folders(
        &self,
        folders: &dyn DriveFolderStore,
        owner_id: &str,
        name: &str,
        parent_id: Option<&str>,
    ) -> Result<Vec<DriveFolder>, PortError> {
        self.accounts.get_owner(owner_id)?;
        // A non-null `parentId` that is missing/foreign matches nothing
        // (upstream `findBy` on the id would also match nothing).
        if let Some(pid) = parent_id {
            match folders.get_folder(pid)? {
                Some(p) if p.owner_id == owner_id => {}
                _ => return Ok(Vec::new()),
            }
        }
        folders.find_folders_by_name(owner_id, name, parent_id)
    }

    /// Tree navigation helper (folderId null = root per Misskey): owner's
    /// folders directly under `parent_id`. Backs the Keystone drive tree
    /// (packed via `drive/folders/show detail` upstream); no dedicated
    /// upstream endpoint, so no dispatcher arm — used by file listing.
    pub fn child_drive_folders(
        &self,
        folders: &dyn DriveFolderStore,
        owner_id: &str,
        parent_id: Option<&str>,
    ) -> Result<Vec<DriveFolder>, PortError> {
        self.accounts.get_owner(owner_id)?;
        folders.child_folders(owner_id, parent_id)
    }

    /// `drive/folders/update {folderId, name?, parentId?}` (`parentId: null`
    /// moves to root; absent leaves placement). `NO_SUCH_FOLDER` on miss,
    /// `NO_SUCH_PARENT_FOLDER` on a missing/foreign parent,
    /// `RECURSIVE_NESTING` on self-parenting or cycles.
    pub fn update_drive_folder(
        &self,
        folders: &dyn DriveFolderStore,
        owner_id: &str,
        folder_id: &str,
        name: Option<String>,
        parent_id: Option<Option<String>>,
    ) -> Result<DriveFolder, PortError> {
        self.accounts.get_owner(owner_id)?;
        let mut folder = self.show_drive_folder(folders, owner_id, folder_id)?;
        if let Some(name) = name {
            validate_folder_name(&name).map_err(domain_err)?;
            folder.name = name;
        }
        if let Some(parent_param) = parent_id {
            let new_parent = match parent_param {
                None => None,
                Some(pid) => match folders.get_folder(&pid)? {
                    Some(p) if p.owner_id == owner_id => Some(p),
                    _ => {
                        return Err(PortError::NotFound(
                            "NO_SUCH_PARENT_FOLDER".to_string(),
                        ))
                    }
                },
            };
            // Walk the proposed parent's ancestor chain for cycle detection.
            let mut ancestors = Vec::new();
            let mut cursor = new_parent.as_ref().and_then(|p| p.parent_id.clone());
            while let Some(id) = cursor {
                ancestors.push(id.clone());
                match folders.get_folder(&id)? {
                    Some(f) if f.owner_id == owner_id => {
                        cursor = f.parent_id.clone();
                    }
                    _ => break,
                }
            }
            validate_folder_parent_change(&folder.id, new_parent.as_ref(), owner_id, &ancestors)
                .map_err(domain_err)?;
            folder.parent_id = new_parent.map(|p| p.id);
        }
        folder.validate().map_err(domain_err)?;
        folders.update_folder(folder, Vec::new())
    }

    /// `drive/folders/delete {folderId}`. Refuses non-empty folders with
    /// `HAS_CHILD_FILES_OR_FOLDERS` (counts child folders + mapped files).
    pub fn delete_drive_folder(
        &self,
        folders: &dyn DriveFolderStore,
        owner_id: &str,
        folder_id: &str,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(owner_id)?;
        let folder = self.show_drive_folder(folders, owner_id, folder_id)?;
        let child_folders = folders.count_child_folders(&folder.id)?;
        let child_files = folders.count_files_in_folder(&folder.id)?;
        require_folder_empty(child_folders, child_files).map_err(domain_err)?;
        folders.delete_folder(&folder.id, Vec::new())
    }

    /// `drive/files/move-bulk {fileIds[1..=100], folderId = null}`.
    /// `NO_SUCH_FOLDER` when a non-null target is missing/foreign
    /// (DIVERGENCE: upstream `findOneByOrFail` 500s here). Files that are
    /// missing or unowned are skipped (upstream bulk-`update` parity: the
    /// `WHERE id IN (...) AND userId` matches only live owned rows).
    pub fn move_drive_files(
        &self,
        folders: &dyn DriveFolderStore,
        owner_id: &str,
        file_ids: &[String],
        folder_id: Option<&str>,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(owner_id)?;
        if let Some(fid) = folder_id {
            match folders.get_folder(fid)? {
                Some(f) if f.owner_id == owner_id => {}
                _ => return Err(PortError::NotFound("NO_SUCH_FOLDER".to_string())),
            }
        }
        for file_id in file_ids {
            match self.media.get(file_id)? {
                Some(meta) if meta.owner_id.as_deref() == Some(owner_id) => {
                    folders.set_file_folder(file_id, folder_id, Vec::new())?;
                }
                _ => continue,
            }
        }
        Ok(())
    }

    /// `drive/files/find {name, folderId = null}`: owner's files with `name`
    /// directly in `folder_id` (`None` = root).
    pub fn find_drive_files(
        &self,
        folders: &dyn DriveFolderStore,
        owner_id: &str,
        name: &str,
        folder_id: Option<&str>,
    ) -> Result<Vec<MediaMeta>, PortError> {
        self.accounts.get_owner(owner_id)?;
        if let Some(fid) = folder_id {
            match folders.get_folder(fid)? {
                Some(f) if f.owner_id == owner_id => {}
                _ => return Ok(Vec::new()),
            }
        }
        let mut out = Vec::new();
        for meta in self.media.list_by_owner(owner_id)? {
            if meta.name != name {
                continue;
            }
            let placement = folders.file_folder_id(&meta.id)?.flatten();
            if placement.as_deref() == folder_id {
                out.push(meta);
            }
        }
        Ok(out)
    }

    /// `drive/files/check-existence {md5}`: owner-scoped md5 hit test.
    pub fn drive_file_exists(
        &self,
        owner_id: &str,
        md5: &str,
    ) -> Result<bool, PortError> {
        self.accounts.get_owner(owner_id)?;
        Ok(self
            .media
            .find_by_owner_and_hash(owner_id, md5)?
            .is_some())
    }

    /// `drive/files/attached-notes {fileId, ...cursor}`: notes referencing
    /// the file, newest-first, cursor-paged. `NO_SUCH_FILE` (verbatim id
    /// `c118ece3-…` in the dispatcher map) on miss; non-moderator callers
    /// see only their own files (`is_moderator` is decided by the
    /// dispatcher; the op takes it as a plain flag so no new port is
    /// needed). No visibility filtering beyond that (upstream
    /// `makePaginationQuery` parity — the file check is the gate).
    pub fn attached_notes(
        &self,
        owner_id: &str,
        file_id: &str,
        is_moderator: bool,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Note>, PortError> {
        self.accounts.get_owner(owner_id)?;
        match self.media.get(file_id)? {
            Some(meta)
                if meta.owner_id.as_deref() == Some(owner_id)
                    || (is_moderator && meta.owner_id.is_some()) => {}
            _ => return Err(PortError::NotFound("NO_SUCH_FILE".to_string())),
        }
        // Bounded scan over the id-ordered note table (single-node MVP;
        // the parent's `NoteStore::notes_with_file` port replaces this with
        // an index lookup — see the WP2 report).
        let mut out = Vec::new();
        let mut page = TimelineCursor::with_limit(cursor.limit.max(1).min(100));
        for _ in 0..50 {
            let batch = self.notes.list_all_notes(&page)?;
            if batch.is_empty() {
                break;
            }
            for note in &batch {
                if note.file_ids.iter().any(|id| id == file_id) {
                    if let Some(since_id) = cursor.since_id.as_deref() {
                        if note.id.as_str() <= since_id {
                            continue;
                        }
                    }
                    if let Some(until_id) = cursor.until_id.as_deref() {
                        if note.id.as_str() >= until_id {
                            continue;
                        }
                    }
                    if cursor.since_date.is_some_and(|s| {
                        self.ids.parse_time(&note.id).map(|t| t <= s).unwrap_or(true)
                    }) {
                        continue;
                    }
                    if cursor.until_date.is_some_and(|u| {
                        self.ids.parse_time(&note.id).map(|t| t >= u).unwrap_or(true)
                    }) {
                        continue;
                    }
                    out.push(note.clone());
                }
            }
            if batch.len() < page.limit {
                break;
            }
            let last = batch.last().map(|n| n.id.clone()).unwrap_or_default();
            page = TimelineCursor {
                since_id: None,
                until_id: Some(last),
                since_date: None,
                until_date: None,
                limit: page.limit,
            };
            if out.len() >= cursor.limit {
                break;
            }
        }
        out.sort_by(|a, b| b.id.cmp(&a.id));
        out.truncate(cursor.limit);
        Ok(out)
    }

    /// `drive/stream` (upstream file-stream parity): the caller's files with
    /// the shared `drive/files` filter semantics (Misskey `type` exact match
    /// unless `/*`-suffixed, in which case prefix match). Thin alias over
    /// [`Self::list_drive_files`]; the dispatcher's `type` normalization
    /// feeds `type_prefix` (upstream strips the trailing `/*`).
    pub fn stream_drive_files(
        &self,
        owner_id: &str,
        cursor: &TimelineCursor,
        type_prefix: Option<&str>,
        sort: Option<&str>,
    ) -> Result<Vec<MediaMeta>, PortError> {
        self.list_drive_files(owner_id, cursor, type_prefix, sort)
    }

    /// Folder-aware `drive/files` listing: as [`Self::list_drive_files`]
    /// plus `folder_id` (`None` = root). The dispatcher keeps rejecting
    /// non-null `folderId` on the flat `drive/files` arm until the parent
    /// wires this op in (see the WP2 report snippet).
    pub fn list_drive_files_in(
        &self,
        folders: &dyn DriveFolderStore,
        owner_id: &str,
        folder_id: Option<&str>,
        cursor: &TimelineCursor,
        type_prefix: Option<&str>,
        sort: Option<&str>,
    ) -> Result<Vec<MediaMeta>, PortError> {
        self.accounts.get_owner(owner_id)?;
        if let Some(fid) = folder_id {
            match folders.get_folder(fid)? {
                Some(f) if f.owner_id == owner_id => {}
                _ => return Ok(Vec::new()),
            }
        }
        let files = self.list_drive_files(owner_id, cursor, type_prefix, sort)?;
        let mut out = Vec::with_capacity(files.len());
        for meta in files {
            // `list_drive_files` already truncated to `cursor.limit`; the
            // folder filter can only shrink the page (documented: clients
            // page with `untilId` as usual).
            if folders.file_folder_id(&meta.id)?.flatten().as_deref() == folder_id {
                out.push(meta);
            }
        }
        Ok(out)
    }

    /// `drive/files/update` folder leg (parent dispatcher owns the
    /// name/sensitive/comment legs in `media.rs::update_media`): move one
    /// file to `folder_id` (`None` = root). `NO_SUCH_FILE` on miss,
    /// `ACCESS_DENIED` for non-moderator foreign files, `NO_SUCH_FOLDER`
    /// for a missing/foreign target. Ids in the dispatcher map.
    pub fn set_drive_file_folder(
        &self,
        folders: &dyn DriveFolderStore,
        owner_id: &str,
        file_id: &str,
        folder_id: Option<&str>,
        is_moderator: bool,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(owner_id)?;
        match self.media.get(file_id)? {
            Some(meta) if meta.owner_id.as_deref() == Some(owner_id) => {}
            Some(_) if is_moderator => {}
            Some(_) => return Err(PortError::Validation("ACCESS_DENIED".to_string())),
            None => return Err(PortError::NotFound("NO_SUCH_FILE".to_string())),
        }
        if let Some(fid) = folder_id {
            // Upstream binds the folder to the FILE owner's id (moderator
            // moving a foreign file targets that owner's tree).
            let file_owner = self
                .media
                .get(file_id)?
                .and_then(|m| m.owner_id)
                .unwrap_or_else(|| owner_id.to_string());
            match folders.get_folder(fid)? {
                Some(f) if f.owner_id == file_owner => {}
                _ => return Err(PortError::NotFound("NO_SUCH_FOLDER".to_string())),
            }
        }
        folders.set_file_folder(file_id, folder_id, Vec::new())
    }
}
