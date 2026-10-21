//! Drive folders: pure data + validation (no I/O, no HTTP, no DB).
//!
//! Mirrors Misskey's `MiDriveFolder{userId,name,parentId}` (a per-user folder
//! tree; files point at it via `folderId`, `null` = drive root). Upstream
//! reference: `packages/backend/src/server/api/endpoints/drive/folders/*`
//! (Misskey `develop`).
//!
//! Bound choice: the API contract allows `name` up to 200 chars
//! (`create.ts`/`update.ts` `maxLength: 200`), while the upstream DB column is
//! `varchar(128)` (`models/DriveFolder.ts:15-18`, which would fail long names
//! at the database — see `docs/implementer-guide-batch-m.md` §9 WP8 note).
//! Pinetree enforces the **200-char API bound** in the domain (redb has no
//! varchar limit); the parent's redb schema must not reintroduce a narrower
//! column. Recorded here so the merge keeps one limit.

use crate::domain::id::DomainError;

/// Maximum folder-name length in chars (API contract parity:
/// `drive/folders/create|update` `maxLength: 200`).
pub const DRIVE_FOLDER_NAME_MAX: usize = 200;

/// One drive folder: `owner_id` owns it, `parent_id == None` means it hangs
/// off the drive root.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DriveFolder {
    pub id: String,
    pub owner_id: String,
    pub name: String,
    pub parent_id: Option<String>,
}

impl DriveFolder {
    pub fn validate(&self) -> Result<(), DomainError> {
        validate_folder_name(&self.name)?;
        if self.id.is_empty() || self.owner_id.is_empty() {
            return Err(DomainError::Validation(
                "folder id and owner_id must be non-empty".to_string(),
            ));
        }
        if let Some(parent) = &self.parent_id {
            if parent.is_empty() {
                return Err(DomainError::Validation(
                    "folder parent_id must be non-empty when set".to_string(),
                ));
            }
            if parent == &self.id {
                return Err(DomainError::Validation("RECURSIVE_NESTING".to_string()));
            }
        }
        Ok(())
    }
}

/// API-parity name bound: 0..=200 chars (upstream sets no `minLength`, so an
/// empty name is accepted and stored verbatim).
pub fn validate_folder_name(name: &str) -> Result<(), DomainError> {
    if name.chars().count() > DRIVE_FOLDER_NAME_MAX {
        return Err(DomainError::Validation(format!(
            "folder name must be 0..={DRIVE_FOLDER_NAME_MAX} chars"
        )));
    }
    Ok(())
}

/// Creation rule (`drive/folders/create` parity): the name obeys the API
/// bound and a requested parent — when present — is owned by the same user.
/// A foreign/missing parent is `NotFound(NO_SUCH_FOLDER)` (verbatim
/// `create.ts` `noSuchFolder`, id `53326628-a00d-40a6-a3cd-8975105c0f95`).
pub fn validate_new_folder(
    owner_id: &str,
    name: &str,
    parent: Option<&DriveFolder>,
) -> Result<(), DomainError> {
    validate_folder_name(name)?;
    if owner_id.is_empty() {
        return Err(DomainError::Validation(
            "folder owner_id must be non-empty".to_string(),
        ));
    }
    if let Some(parent) = parent {
        if parent.owner_id != owner_id {
            return Err(DomainError::NotFound("NO_SUCH_FOLDER".to_string()));
        }
    }
    Ok(())
}

/// Parent-change rule (`drive/folders/update` parity): moving to the root
/// (`None`) always passes; moving under `Some(parent)` requires same-owner
/// parentage (`NotFound(NO_SUCH_PARENT_FOLDER)`, verbatim `update.ts`
/// `noSuchParentFolder`), rejects self-parenting, and rejects moves that
/// would close a cycle — i.e. the folder being moved already appears on the
/// proposed parent's ancestor chain (`Validation(RECURSIVE_NESTING)`,
/// verbatim `update.ts` `recursiveNesting`). The caller walks the chain and
/// passes it as `ancestor_ids` (nearest parent first); this function stays
/// pure.
pub fn validate_folder_parent_change(
    folder_id: &str,
    new_parent: Option<&DriveFolder>,
    owner_id: &str,
    ancestor_ids: &[String],
) -> Result<(), DomainError> {
    let Some(parent) = new_parent else {
        return Ok(());
    };
    if parent.owner_id != owner_id {
        return Err(DomainError::NotFound("NO_SUCH_PARENT_FOLDER".to_string()));
    }
    if parent.id == folder_id || ancestor_ids.iter().any(|id| id == folder_id) {
        return Err(DomainError::Validation("RECURSIVE_NESTING".to_string()));
    }
    Ok(())
}

/// Deletion guard (`drive/folders/delete` parity): a folder with child
/// folders or files refuses deletion (`Conflict` carrying the verbatim
/// `HAS_CHILD_FILES_OR_FOLDERS` code; `delete.ts` `hasChildFilesOrFolders`).
pub fn require_folder_empty(child_folders: u64, child_files: u64) -> Result<(), DomainError> {
    if child_folders > 0 || child_files > 0 {
        return Err(DomainError::Conflict(
            "HAS_CHILD_FILES_OR_FOLDERS".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn folder(id: &str, owner: &str, parent: Option<&str>) -> DriveFolder {
        DriveFolder {
            id: id.to_string(),
            owner_id: owner.to_string(),
            name: "docs".to_string(),
            parent_id: parent.map(str::to_string),
        }
    }

    #[test]
    fn valid_folder_passes() {
        assert!(folder("f1", "u1", None).validate().is_ok());
        assert!(folder("f1", "u1", Some("f0")).validate().is_ok());
    }

    #[test]
    fn name_bound_is_200_chars() {
        assert!(validate_folder_name(&"n".repeat(200)).is_ok());
        assert!(validate_folder_name(&"n".repeat(201)).is_err());
        // Upstream sets no minLength: empty names are stored verbatim.
        assert!(validate_folder_name("").is_ok());
    }

    #[test]
    fn self_parent_rejected() {
        assert!(folder("f1", "u1", Some("f1")).validate().is_err());
    }

    #[test]
    fn foreign_parent_is_no_such_folder() {
        let other = folder("p", "u2", None);
        assert_eq!(
            validate_new_folder("u1", "x", Some(&other)),
            Err(DomainError::NotFound("NO_SUCH_FOLDER".to_string()))
        );
        assert!(validate_new_folder("u1", "x", Some(&folder("p", "u1", None))).is_ok());
    }

    #[test]
    fn parent_change_rules() {
        let parent = folder("p", "u1", None);
        // Move to root always passes.
        assert!(validate_folder_parent_change("f", None, "u1", &[]).is_ok());
        // Foreign parent.
        let foreign = folder("p", "u2", None);
        assert_eq!(
            validate_folder_parent_change("f", Some(&foreign), "u1", &[]),
            Err(DomainError::NotFound("NO_SUCH_PARENT_FOLDER".to_string()))
        );
        // Self-parenting and cycles.
        assert_eq!(
            validate_folder_parent_change("f", Some(&folder("f", "u1", None)), "u1", &[]),
            Err(DomainError::Validation("RECURSIVE_NESTING".to_string()))
        );
        assert_eq!(
            validate_folder_parent_change(
                "f",
                Some(&parent),
                "u1",
                &["g".to_string(), "f".to_string()]
            ),
            Err(DomainError::Validation("RECURSIVE_NESTING".to_string()))
        );
        assert!(
            validate_folder_parent_change("f", Some(&parent), "u1", &["g".to_string()]).is_ok()
        );
    }

    #[test]
    fn delete_guard() {
        assert!(require_folder_empty(0, 0).is_ok());
        assert_eq!(
            require_folder_empty(1, 0),
            Err(DomainError::Conflict(
                "HAS_CHILD_FILES_OR_FOLDERS".to_string()
            ))
        );
        assert!(require_folder_empty(0, 3).is_err());
    }
}
