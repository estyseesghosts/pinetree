use super::job_queue::NewJob;
use crate::domain::drive_folder::DriveFolder;
use crate::domain::id::PortError;

/// Drive-folder + file-folder-index store.
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

// ---------------------------------------------------------------------------
// Gaps WP2a: follow options, user memos, thread mutes.
// ---------------------------------------------------------------------------
