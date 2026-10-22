//! `drive` error constructors (split from `errors.rs`).
//!
#![allow(unused_imports)]
use super::codes::*;
use super::error::ApiError;

impl ApiError {
    /// Misskey `admin/drive/show-file.ts` `NO_SUCH_FILE` (`No such file.`,
    /// verbatim UUID `caf3ca38-…`).
    pub fn drive_show_file_no_such_file() -> Self {
        Self::client(
            "No such file.",
            "NO_SUCH_FILE",
            DRIVE_SHOW_FILE_NO_SUCH_FILE_ID,
            400,
        )
    }
    /// Gaps final wiring: `drive/files/{check-existence,attached-notes}`
    /// `NO_SUCH_FILE` (one UUID per endpoint, upstream convention;
    /// pinetree-minted — `// TODO(final): verify against misskey`).
    pub fn drive_no_such_file(endpoint: &str) -> Self {
        let id = match endpoint {
            "drive/files/attached-notes" => DRIVE_ATTACHED_NOTES_NO_SUCH_FILE_ID,
            _ => DRIVE_CHECK_EXISTENCE_NO_SUCH_FILE_ID,
        };
        Self::client("No such file.", "NO_SUCH_FILE", id, 400)
    }
    /// Gaps final wiring: `drive/folders/{create,show,update,delete}` +
    /// `drive/files/{move-bulk,update-folder-leg}` `NO_SUCH_FOLDER` (one
    /// UUID per endpoint, upstream convention; pinetree-minted —
    /// `// TODO(final): verify against misskey`).
    pub fn folder_no_such_folder(endpoint: &str) -> Self {
        let id = match endpoint {
            "drive/folders/create" => FOLDER_CREATE_NO_SUCH_FOLDER_ID,
            "drive/folders/show" => FOLDER_SHOW_NO_SUCH_FOLDER_ID,
            "drive/folders/update" => FOLDER_UPDATE_NO_SUCH_FOLDER_ID,
            "drive/folders/delete" => FOLDER_DELETE_NO_SUCH_FOLDER_ID,
            "drive/files/move-bulk" => MOVE_BULK_NO_SUCH_FOLDER_ID,
            "drive/files/update" => FILES_UPDATE_NO_SUCH_FOLDER_ID,
            _ => FOLDER_SHOW_NO_SUCH_FOLDER_ID,
        };
        Self::client("No such folder.", "NO_SUCH_FOLDER", id, 400)
    }
    /// Gaps final wiring: `drive/folders/update` `NO_SUCH_PARENT_FOLDER` /
    /// `RECURSIVE_NESTING` (verbatim codes; pinetree-minted UUIDs).
    pub fn folder_update_parent_error(code: &str) -> Self {
        match code {
            "NO_SUCH_PARENT_FOLDER" => Self::client(
                "No such parent folder.",
                "NO_SUCH_PARENT_FOLDER",
                FOLDER_UPDATE_NO_SUCH_PARENT_FOLDER_ID,
                400,
            ),
            _ => Self::client(
                "Recursive nesting.",
                "RECURSIVE_NESTING",
                FOLDER_UPDATE_RECURSIVE_NESTING_ID,
                400,
            ),
        }
    }
    /// Gaps final wiring: `drive/folders/delete`
    /// `HAS_CHILD_FILES_OR_FOLDERS` (verbatim code; pinetree-minted UUID).
    pub fn folder_delete_has_child() -> Self {
        Self::client(
            "The folder has child files or folders.",
            "HAS_CHILD_FILES_OR_FOLDERS",
            FOLDER_DELETE_HAS_CHILD_ID,
            400,
        )
    }
}
