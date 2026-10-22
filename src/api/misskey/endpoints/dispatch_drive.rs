//! Drive dispatcher arms.
//!
//! Split from `endpoints/mod.rs` (decomposition). Pure move, no behavior change.
#![allow(unused_imports)]

mod admin;
mod files;
mod folders;

pub(crate) use admin::admin_drive_files;
pub(crate) use admin::admin_drive_show_file;
pub(crate) use files::drive_cache_stats;
pub(crate) use files::drive_files;
pub(crate) use files::drive_files_attached_notes;
pub(crate) use files::drive_files_check_existence;
pub(crate) use files::drive_files_delete;
pub(crate) use files::drive_files_find_by_hash;
pub(crate) use files::drive_files_show;
pub(crate) use files::drive_files_update;
pub(crate) use files::drive_files_upload_from_url;
pub(crate) use files::drive_stream;
pub(crate) use folders::drive_files_find;
pub(crate) use folders::drive_files_move_bulk;
pub(crate) use folders::drive_folders_create;
pub(crate) use folders::drive_folders_delete;
pub(crate) use folders::drive_folders_find;
pub(crate) use folders::drive_folders_show;
pub(crate) use folders::drive_folders_update;
