use super::account_store::{ACCOUNT_OWNERS, ACCOUNTS_BY_HANDLE, ACCOUNTS_BY_URI, USED_USERNAMES};
use super::announcement_store::{ANNOUNCE_READS, ANNOUNCEMENTS, PROMO_READS};
use super::antenna_store::{ANTENNA_NOTES, ANTENNAS, ANTENNAS_BY_OWNER};
use super::auth_store::{
    ACTOR_KEYPAIRS, API_TOKENS, API_TOKENS_BY_ID, LEGACY_SESSIONS, MIAUTH_SESSIONS,
    PASSWORD_RESET_IDS, PASSWORD_RESETS, REMOTE_PUBLIC_KEYS, SIGNINS, SIGNINS_BY_USER,
};
use super::block_store::{
    BLOCKINGS_FORWARD, BLOCKINGS_REVERSE, MUTINGS_FORWARD, RENOTE_MUTINGS_FORWARD,
};
use super::clip_store::{
    CLIP_FAVORITES, CLIP_FOLLOWS, CLIP_NOTES, CLIP_TOMBSTONES, CLIPS, CLIPS_BY_OWNER,
};
use super::codec::{decode_json, map_redb_err};
use super::drive_folder_store::{
    DRIVE_FILE_FOLDERS, DRIVE_FILES_IN_FOLDER, DRIVE_FOLDERS, DRIVE_FOLDERS_BY_NAME,
    DRIVE_FOLDERS_BY_OWNER_PARENT,
};
use super::emoji_store::{EMOJIS, EMOJIS_BY_NAME, REMOTE_EMOJIS};
use super::favorite_store::FAVORITES;
use super::follow_store::{FOLLOW_REQ_FORWARD, FOLLOW_REQ_REVERSE};
use super::gallery_store::{FLASH_LIKES, FLASHES, GALLERY_POST_LIKES, GALLERY_POSTS};
use super::host_caps_store::HOST_SOFTWARE;
use super::instance_settings_store::INSTANCE_SETTINGS;
use super::invite_store::{INVITE_CODES, INVITES};
use super::job_queue::{JOBS, JOBS_DUE_INDEX};
use super::media_store::{CACHE_ACCOUNTING, REMOTE_CACHE};
use super::moderation_store::{
    ABUSE_REPORT_RECIPIENTS, ABUSE_REPORTS, MODERATION_LOGS, USER_MEMOS,
};
use super::note_store::{
    NOTE_DRAFTS, NOTE_DRAFTS_BY_USER, NOTE_EDITS, NOTE_EDITS_BY_NOTE, NOTES_BY_AUTHOR,
    NOTES_BY_QUOTE_REQUEST, NOTES_BY_QUOTE_STAMP, NOTES_BY_REPLY, NOTES_BY_REPLY_PENDING, PINNINGS,
};
use super::note_thread_mute_store::THREAD_MUTES;
use super::notification_store::{NOTIFICATIONS, NOTIFICATIONS_BY_USER};
use super::page_store::{PAGE_LIKES, PAGES, PAGES_BY_OWNER, PAGES_BY_OWNER_NAME, PAGES_BY_URI};
use super::reaction_store::{REACTIONS, REACTIONS_BY_NOTE};
use super::registry_store::REGISTRY;
use super::store::{
    ACCOUNT_URI_INDEX_DONE_KEY, ACCOUNTS, ACTIVITY_DEDUP, BLOB_REFCOUNT, CREDENTIALS,
    DURABLE_BYTES_BY_OWNER, FOLLOWS_FORWARD, FOLLOWS_REVERSE, HANDLE_INDEX, MEDIA_META, META,
    NOTES, NOTES_BY_URI, NOTIFICATION_ORDER_DONE_KEY, POLL_VOTES, POLLS, QUOTE_INDEX_DONE_KEY,
    QUOTE_STAMPS, REACTIONS_BY_NOTE_DONE_KEY, REPLY_INDEX_DONE_KEY, REPLY_INDEX_PROGRESS_KEY,
    RedbStore, STORAGE_ACCOUNTING_DONE_KEY, STORAGE_ACCOUNTING_PROGRESS_KEY, TOKEN_ID_DONE_KEY,
    TOMBSTONES_BY_URI, URI_INDEX_DONE_KEY,
};
use super::user_list_store::{
    USER_LIST_FAVORITES, USER_LIST_MEMBERSHIPS, USER_LISTS, USER_LISTS_BY_OWNER,
};
use super::webhook_store::{
    SYSTEM_WEBHOOKS, SYSTEM_WEBHOOKS_BY_TRIGGER, USER_WEBHOOKS, USER_WEBHOOKS_BY_OWNER,
    USER_WEBHOOKS_BY_OWNER_TRIGGER,
};
use crate::application::ports::PortError;
use crate::domain::media::MediaMeta;
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata};
use std::path::Path;

use redb::{
    Key as RedbKey, MultimapTableHandle as RedbMultimapTableHandle, TableHandle as RedbTableHandle,
    Value as RedbValue,
};

/// Key/value type-signature vocabulary used in bundle manifests.
pub const BACKUP_SIG_STR: &str = "str";
pub const BACKUP_SIG_BYTES: &str = "bytes";
pub const BACKUP_SIG_U8: &str = "u8";
pub const BACKUP_SIG_U64: &str = "u64";
pub const BACKUP_SIG_STR2: &str = "(str,str)";
pub const BACKUP_SIG_STR3: &str = "(str,str,str)";
pub const BACKUP_SIG_STR2_I64: &str = "(str,str,i64)";
pub const BACKUP_SIG_I64_STR: &str = "(i64,str)";

macro_rules! backup_tables {
    ($( $name:literal => $def:ident : $ksig:ident / $vsig:ident ),* $(,)?) => {
        /// Canonical backup registry: every production table with its type
        /// identity. Add new tables HERE (and only here — export, import,
        /// and signatures all expand from this list).
        pub fn backup_table_names() -> Vec<&'static str> {
            vec![$($name),*]
        }

        /// Declared `(key_sig, value_sig)` for `name`, or `None` when the
        /// table is not in the pinned schema (restore rejects those).
        pub fn backup_table_signature(name: &str) -> Option<(&'static str, &'static str)> {
            match name {
                $($name => Some(($ksig, $vsig)),)*
                _ => None,
            }
        }

        fn export_one_table(
            read_txn: &redb::ReadTransaction,
            name: &str,
            emit: &mut dyn FnMut(&str, Vec<u8>, Vec<u8>) -> Result<(), PortError>,
        ) -> Result<u64, PortError> {
            match name {
                $($name => export_typed_table(read_txn, $def, name, emit),)*
                _ => Err(PortError::StorageUnavailable(format!(
                    "backup registry misses table {name}"
                ))),
            }
        }

        fn import_one_table(
            write_txn: &redb::WriteTransaction,
            name: &str,
            rows: &[(Vec<u8>, Vec<u8>)],
        ) -> Result<u64, PortError> {
            match name {
                $($name => {
                    // Checked pre-validation: redb's `from_bytes` unwraps
                    // (e.g. `&str` on invalid UTF-8) and would panic on
                    // hostile input. Fail closed instead — before any write.
                    for (i, (key_bytes, value_bytes)) in rows.iter().enumerate() {
                        check_key_bytes($ksig, key_bytes).map_err(|e| {
                            PortError::Validation(format!(
                                "backup row check failed in table {} row {}: {}",
                                $name, i, e
                            ))
                        })?;
                        check_value_bytes($vsig, value_bytes).map_err(|e| {
                            PortError::Validation(format!(
                                "backup row check failed in table {} row {}: {}",
                                $name, i, e
                            ))
                        })?;
                    }
                    import_typed_table(write_txn, $def, rows)
                })*
                _ => Err(PortError::Validation(format!(
                    "backup import of unknown table {name}"
                ))),
            }
        }
    };
}

backup_tables!(
    "accounts" => ACCOUNTS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "handle_index" => HANDLE_INDEX : BACKUP_SIG_STR / BACKUP_SIG_STR,
    "used_usernames" => USED_USERNAMES : BACKUP_SIG_STR / BACKUP_SIG_U8,
    "account_owners" => ACCOUNT_OWNERS : BACKUP_SIG_STR / BACKUP_SIG_STR,
    "notes" => NOTES : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "notes_by_author" => NOTES_BY_AUTHOR : BACKUP_SIG_STR2 / BACKUP_SIG_U8,
    "follows_forward" => FOLLOWS_FORWARD : BACKUP_SIG_STR2 / BACKUP_SIG_BYTES,
    "follows_reverse" => FOLLOWS_REVERSE : BACKUP_SIG_STR2 / BACKUP_SIG_BYTES,
    "blockings_forward" => BLOCKINGS_FORWARD : BACKUP_SIG_STR2 / BACKUP_SIG_BYTES,
    "blockings_reverse" => BLOCKINGS_REVERSE : BACKUP_SIG_STR2 / BACKUP_SIG_BYTES,
    "mutings_forward" => MUTINGS_FORWARD : BACKUP_SIG_STR2 / BACKUP_SIG_BYTES,
    "renote_mutings_forward" => RENOTE_MUTINGS_FORWARD : BACKUP_SIG_STR2 / BACKUP_SIG_BYTES,
    "follow_requests_forward" => FOLLOW_REQ_FORWARD : BACKUP_SIG_STR2 / BACKUP_SIG_BYTES,
    "follow_requests_reverse" => FOLLOW_REQ_REVERSE : BACKUP_SIG_STR2 / BACKUP_SIG_BYTES,
    "reactions" => REACTIONS : BACKUP_SIG_STR2 / BACKUP_SIG_BYTES,
    "reactions_by_note" => REACTIONS_BY_NOTE : BACKUP_SIG_STR2 / BACKUP_SIG_BYTES,
    "note_drafts" => NOTE_DRAFTS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "note_drafts_by_user" => NOTE_DRAFTS_BY_USER : BACKUP_SIG_STR2 / BACKUP_SIG_U8,
    "pinnings" => PINNINGS : BACKUP_SIG_STR2 / BACKUP_SIG_BYTES,
    "favorites" => FAVORITES : BACKUP_SIG_STR2 / BACKUP_SIG_BYTES,
    "emojis" => EMOJIS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "emojis_by_name" => EMOJIS_BY_NAME : BACKUP_SIG_STR / BACKUP_SIG_STR,
    "remote_emojis" => REMOTE_EMOJIS : BACKUP_SIG_STR2 / BACKUP_SIG_BYTES,
    "antennas" => ANTENNAS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "antennas_by_owner" => ANTENNAS_BY_OWNER : BACKUP_SIG_STR2 / BACKUP_SIG_U8,
    "antenna_notes" => ANTENNA_NOTES : BACKUP_SIG_STR2 / BACKUP_SIG_U8,
    "user_lists" => USER_LISTS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "user_lists_by_owner" => USER_LISTS_BY_OWNER : BACKUP_SIG_STR2 / BACKUP_SIG_U8,
    "user_list_memberships" => USER_LIST_MEMBERSHIPS : BACKUP_SIG_STR2 / BACKUP_SIG_BYTES,
    "user_list_favorites" => USER_LIST_FAVORITES : BACKUP_SIG_STR2 / BACKUP_SIG_U8,
    "clips" => CLIPS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "clips_by_owner" => CLIPS_BY_OWNER : BACKUP_SIG_STR2 / BACKUP_SIG_U8,
    "clip_notes" => CLIP_NOTES : BACKUP_SIG_STR2 / BACKUP_SIG_BYTES,
    "clip_favorites" => CLIP_FAVORITES : BACKUP_SIG_STR2 / BACKUP_SIG_BYTES,
    "clip_follows" => CLIP_FOLLOWS : BACKUP_SIG_STR2 / BACKUP_SIG_BYTES,
    "clip_tombstones" => CLIP_TOMBSTONES : BACKUP_SIG_STR / BACKUP_SIG_U64,
    "pages" => PAGES : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "pages_by_owner" => PAGES_BY_OWNER : BACKUP_SIG_STR2 / BACKUP_SIG_U8,
    "pages_by_owner_name" => PAGES_BY_OWNER_NAME : BACKUP_SIG_STR2 / BACKUP_SIG_STR,
    "pages_by_uri" => PAGES_BY_URI : BACKUP_SIG_STR / BACKUP_SIG_STR,
    "page_likes" => PAGE_LIKES : BACKUP_SIG_STR2 / BACKUP_SIG_BYTES,
    "user_memos" => USER_MEMOS : BACKUP_SIG_STR2 / BACKUP_SIG_BYTES,
    "thread_mutes" => THREAD_MUTES : BACKUP_SIG_STR2 / BACKUP_SIG_U8,
    "gallery_posts" => GALLERY_POSTS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "gallery_post_likes" => GALLERY_POST_LIKES : BACKUP_SIG_STR2 / BACKUP_SIG_BYTES,
    "flashes" => FLASHES : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "flash_likes" => FLASH_LIKES : BACKUP_SIG_STR2 / BACKUP_SIG_BYTES,
    "announcements" => ANNOUNCEMENTS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "announce_reads" => ANNOUNCE_READS : BACKUP_SIG_STR2 / BACKUP_SIG_U8,
    "promo_reads" => PROMO_READS : BACKUP_SIG_STR / BACKUP_SIG_U8,
    "polls" => POLLS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "poll_votes" => POLL_VOTES : BACKUP_SIG_STR2_I64 / BACKUP_SIG_BYTES,
    "jobs" => JOBS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "jobs_due_index" => JOBS_DUE_INDEX : BACKUP_SIG_I64_STR / BACKUP_SIG_U8,
    "activity_dedup" => ACTIVITY_DEDUP : BACKUP_SIG_STR / BACKUP_SIG_U8,
    "media_meta" => MEDIA_META : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "blob_refcount" => BLOB_REFCOUNT : BACKUP_SIG_STR / BACKUP_SIG_U64,
    "durable_bytes_by_owner" => DURABLE_BYTES_BY_OWNER : BACKUP_SIG_STR / BACKUP_SIG_U64,
    "remote_cache" => REMOTE_CACHE : BACKUP_SIG_STR2 / BACKUP_SIG_BYTES,
    "cache_accounting" => CACHE_ACCOUNTING : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "notifications" => NOTIFICATIONS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "notifications_by_user_ordered" => NOTIFICATIONS_BY_USER : BACKUP_SIG_STR2 / BACKUP_SIG_STR,
    "credentials" => CREDENTIALS : BACKUP_SIG_STR / BACKUP_SIG_STR,
    "api_tokens" => API_TOKENS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "api_tokens_by_id" => API_TOKENS_BY_ID : BACKUP_SIG_STR / BACKUP_SIG_STR,
    "signins" => SIGNINS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "signins_by_user" => SIGNINS_BY_USER : BACKUP_SIG_STR2 / BACKUP_SIG_U8,
    "miauth_sessions" => MIAUTH_SESSIONS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "legacy_sessions" => LEGACY_SESSIONS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "password_resets" => PASSWORD_RESETS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "password_reset_ids" => PASSWORD_RESET_IDS : BACKUP_SIG_STR / BACKUP_SIG_STR,
    "registry" => REGISTRY : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "actor_keypairs" => ACTOR_KEYPAIRS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "remote_public_keys" => REMOTE_PUBLIC_KEYS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "quote_stamps" => QUOTE_STAMPS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "notes_by_reply" => NOTES_BY_REPLY : BACKUP_SIG_STR2 / BACKUP_SIG_U8,
    "notes_by_reply_pending" => NOTES_BY_REPLY_PENDING : BACKUP_SIG_STR2 / BACKUP_SIG_U8,
    "notes_by_uri" => NOTES_BY_URI : BACKUP_SIG_STR / BACKUP_SIG_STR,
    "tombstones_by_uri" => TOMBSTONES_BY_URI : BACKUP_SIG_STR / BACKUP_SIG_U8,
    "meta" => META : BACKUP_SIG_STR / BACKUP_SIG_STR,
    "note_edits" => NOTE_EDITS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "note_edits_by_note" => NOTE_EDITS_BY_NOTE : BACKUP_SIG_STR2 / BACKUP_SIG_U8,
    "host_software" => HOST_SOFTWARE : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "moderation_logs" => MODERATION_LOGS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "abuse_reports" => ABUSE_REPORTS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "instance_settings" => INSTANCE_SETTINGS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "invites" => INVITES : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "invite_codes" => INVITE_CODES : BACKUP_SIG_STR / BACKUP_SIG_STR,
    "drive_folders" => DRIVE_FOLDERS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "drive_folders_by_owner_parent" => DRIVE_FOLDERS_BY_OWNER_PARENT : BACKUP_SIG_STR3 / BACKUP_SIG_U8,
    "drive_folders_by_name" => DRIVE_FOLDERS_BY_NAME : BACKUP_SIG_STR3 / BACKUP_SIG_BYTES,
    "drive_file_folders" => DRIVE_FILE_FOLDERS : BACKUP_SIG_STR / BACKUP_SIG_STR,
    "drive_files_in_folder" => DRIVE_FILES_IN_FOLDER : BACKUP_SIG_STR2 / BACKUP_SIG_U8,
    "system_webhooks" => SYSTEM_WEBHOOKS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "system_webhooks_by_trigger" => SYSTEM_WEBHOOKS_BY_TRIGGER : BACKUP_SIG_STR2 / BACKUP_SIG_U8,
    "user_webhooks" => USER_WEBHOOKS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "user_webhooks_by_owner" => USER_WEBHOOKS_BY_OWNER : BACKUP_SIG_STR2 / BACKUP_SIG_U8,
    "user_webhooks_by_owner_trigger" => USER_WEBHOOKS_BY_OWNER_TRIGGER : BACKUP_SIG_STR3 / BACKUP_SIG_U8,
    "abuse_report_recipients" => ABUSE_REPORT_RECIPIENTS : BACKUP_SIG_STR / BACKUP_SIG_BYTES,
    "accounts_by_uri" => ACCOUNTS_BY_URI : BACKUP_SIG_STR / BACKUP_SIG_STR,
    "accounts_by_handle" => ACCOUNTS_BY_HANDLE : BACKUP_SIG_STR / BACKUP_SIG_STR,
    "notes_by_quote_request" => NOTES_BY_QUOTE_REQUEST : BACKUP_SIG_STR / BACKUP_SIG_STR,
    "notes_by_quote_stamp" => NOTES_BY_QUOTE_STAMP : BACKUP_SIG_STR / BACKUP_SIG_STR,
);

fn export_typed_table<K: RedbKey + 'static, V: RedbValue + 'static>(
    read_txn: &redb::ReadTransaction,
    definition: redb::TableDefinition<K, V>,
    name: &str,
    emit: &mut dyn FnMut(&str, Vec<u8>, Vec<u8>) -> Result<(), PortError>,
) -> Result<u64, PortError> {
    let table = read_txn.open_table(definition).map_err(map_redb_err)?;
    let mut count = 0u64;
    for entry in table.iter().map_err(map_redb_err)? {
        let (key, value) = entry.map_err(map_redb_err)?;
        let key_value = key.value();
        let value_value = value.value();
        let key_bytes: Vec<u8> = K::as_bytes(&key_value).as_ref().to_vec();
        let value_bytes: Vec<u8> = V::as_bytes(&value_value).as_ref().to_vec();
        emit(name, key_bytes, value_bytes)?;
        count += 1;
    }
    Ok(count)
}

fn import_typed_table<K: RedbKey + 'static, V: RedbValue + 'static>(
    write_txn: &redb::WriteTransaction,
    definition: redb::TableDefinition<K, V>,
    rows: &[(Vec<u8>, Vec<u8>)],
) -> Result<u64, PortError> {
    let mut table = write_txn.open_table(definition).map_err(map_redb_err)?;
    for (key_bytes, value_bytes) in rows {
        // Pre-validated by `import_one_table`, so these decodes cannot
        // panic on hostile input.
        let key = K::from_bytes(key_bytes);
        let value = V::from_bytes(value_bytes);
        table.insert(key, value).map_err(map_redb_err)?;
    }
    Ok(rows.len() as u64)
}

// --- Checked byte-shape validation (panic-free restore) -------------------
// redb's tuple encoding is varint-length-prefixed concatenation (all but
// the last element carry a varint len; fixed-width elements like `i64`
// carry none). These validators use checked ops only.

fn read_varint(data: &[u8]) -> Option<(usize, usize)> {
    let first = *data.first()?;
    if first < 254 {
        Some((first as usize, 1))
    } else if first == 254 {
        if data.len() < 3 {
            return None;
        }
        Some((u16::from_le_bytes([data[1], data[2]]) as usize, 3))
    } else if data.len() < 5 {
        None
    } else {
        Some((
            u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize,
            5,
        ))
    }
}

fn check_utf8(bytes: &[u8]) -> Result<(), PortError> {
    std::str::from_utf8(bytes)
        .map(|_| ())
        .map_err(|_| PortError::Validation("backup bytes are not valid text".to_string()))
}

fn split_str_tuple(
    data: &[u8],
    varints: usize,
    tail_fixed: usize,
) -> Result<Vec<&[u8]>, PortError> {
    // redb emits ALL varint lengths first (one per non-fixed element except
    // the last), then ALL element bytes contiguously. Lengths must be read
    // up front: reading length/data interleaved would parse element bytes
    // as lengths.
    let mut rest = data;
    let mut lens = Vec::with_capacity(varints);
    for _ in 0..varints {
        let (len, consumed) = read_varint(rest)
            .ok_or_else(|| PortError::Validation("bad tuple header".to_string()))?;
        rest = rest
            .get(consumed..)
            .ok_or_else(|| PortError::Validation("bad tuple header".to_string()))?;
        lens.push(len);
    }
    let mut parts = Vec::with_capacity(lens.len() + 1);
    for len in lens {
        let part = rest
            .get(..len)
            .ok_or_else(|| PortError::Validation("truncated tuple part".to_string()))?;
        parts.push(part);
        rest = rest
            .get(len..)
            .ok_or_else(|| PortError::Validation("truncated tuple part".to_string()))?;
    }
    if rest.len() < tail_fixed {
        return Err(PortError::Validation("truncated tuple tail".to_string()));
    }
    let (fixed, tail) = rest.split_at(tail_fixed.min(rest.len()));
    if tail_fixed > 0 {
        parts.push(fixed);
    }
    parts.push(tail);
    Ok(parts)
}

fn check_key_bytes(key_sig: &str, key: &[u8]) -> Result<(), PortError> {
    match key_sig {
        BACKUP_SIG_STR => check_utf8(key),
        BACKUP_SIG_STR2 => {
            let parts = split_str_tuple(key, 1, 0)?;
            if parts.len() != 2 {
                return Err(PortError::Validation("bad (str,str) key".to_string()));
            }
            check_utf8(parts[0])?;
            check_utf8(parts[1])
        }
        BACKUP_SIG_STR3 => {
            let parts = split_str_tuple(key, 2, 0)?;
            if parts.len() != 3 {
                return Err(PortError::Validation("bad (str,str,str) key".to_string()));
            }
            check_utf8(parts[0])?;
            check_utf8(parts[1])?;
            check_utf8(parts[2])
        }
        BACKUP_SIG_STR2_I64 => {
            // (str, str, i64): two varint lens, then str, str, and exactly
            // 8 fixed tail bytes.
            let parts = split_str_tuple(key, 2, 0)?;
            if parts.len() != 3 || parts[2].len() != 8 {
                return Err(PortError::Validation("bad (str,str,i64) key".to_string()));
            }
            check_utf8(parts[0])?;
            check_utf8(parts[1])
        }
        BACKUP_SIG_I64_STR => {
            // (i64, str): 8 fixed bytes, then the tail str (no varint: the
            // fixed head carries no length).
            let parts = split_str_tuple(key, 0, 8)?;
            if parts.len() != 2 || parts[0].len() != 8 {
                return Err(PortError::Validation("bad (i64,str) key".to_string()));
            }
            check_utf8(parts[1])
        }
        _ => Err(PortError::Validation(
            "unknown backup key signature".to_string(),
        )),
    }
}

fn check_value_bytes(value_sig: &str, value: &[u8]) -> Result<(), PortError> {
    match value_sig {
        BACKUP_SIG_BYTES => Ok(()),
        BACKUP_SIG_STR => check_utf8(value),
        BACKUP_SIG_U8 => {
            if value.len() != 1 {
                return Err(PortError::Validation("bad u8 value".to_string()));
            }
            Ok(())
        }
        BACKUP_SIG_U64 => {
            if value.len() != 8 {
                return Err(PortError::Validation("bad u64 value".to_string()));
            }
            Ok(())
        }
        _ => Err(PortError::Validation(
            "unknown backup value signature".to_string(),
        )),
    }
}

/// One table's exported row count (registry order).
#[derive(Debug, Clone)]
pub struct BackupTableCount {
    pub name: String,
    pub rows: u64,
}

impl RedbStore {
    /// Stream every registry table through `emit` inside ONE read
    /// transaction (a single consistent snapshot). Tables arrive in
    /// registry order without interleaving. Returns per-table counts.
    pub fn export_backup_snapshot(
        &self,
        emit: &mut dyn FnMut(&str, Vec<u8>, Vec<u8>) -> Result<(), PortError>,
    ) -> Result<Vec<BackupTableCount>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let mut counts = Vec::new();
        for name in backup_table_names() {
            let rows = export_one_table(&read_txn, name, emit)?;
            counts.push(BackupTableCount {
                name: name.to_string(),
                rows,
            });
        }
        Ok(counts)
    }

    /// Import one chunk of rows into `table` (one write transaction;
    /// callers chunk to bound txn size). Unknown tables, bad type shapes,
    /// and undecodable bytes fail closed before any write.
    pub fn import_backup_rows(
        &self,
        table: &str,
        rows: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<u64, PortError> {
        if backup_table_signature(table).is_none() {
            return Err(PortError::Validation(format!(
                "backup import of unknown table {table}"
            )));
        }
        let write_txn = self.begin_write()?;
        let imported = import_one_table(&write_txn, table, rows)?;
        self.commit_write(write_txn)?;
        Ok(imported)
    }

    /// Live table names from redb itself (`ReadTransaction::list_tables`).
    /// The completeness test asserts set equality with
    /// [`backup_table_names`]: any table added to `init_tables` without
    /// registry coverage fails the build.
    pub fn live_table_names(&self) -> Result<Vec<String>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let mut names: Vec<String> = read_txn
            .list_tables()
            .map_err(map_redb_err)?
            .map(|handle| handle.name().to_string())
            .collect();
        names.sort();
        Ok(names)
    }

    /// Live multimap names. This schema uses none; the test pins the empty
    /// set so a future multimap fails loudly instead of skipping backup.
    pub fn live_multimap_names(&self) -> Result<Vec<String>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let mut names: Vec<String> = read_txn
            .list_multimap_tables()
            .map_err(map_redb_err)?
            .map(|handle| handle.name().to_string())
            .collect();
        names.sort();
        Ok(names)
    }

    /// Row counts per registry table in one read transaction.
    pub fn table_row_counts(&self) -> Result<Vec<(String, u64)>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let mut out = Vec::new();
        for name in backup_table_names() {
            let rows = count_one_table(&read_txn, name)?;
            out.push((name.to_string(), rows));
        }
        Ok(out)
    }

    /// Recompute blob refcounts and durable owner totals from `MEDIA_META`
    /// with exactly Batch C's backfill math, and diff against the stored
    /// `BLOB_REFCOUNT` / `DURABLE_BYTES_BY_OWNER` tables. Empty means
    /// consistent; entries describe mismatches (bounded for logs).
    pub fn verify_storage_accounting(&self) -> Result<Vec<String>, PortError> {
        use std::collections::HashMap;
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let media_table = read_txn.open_table(MEDIA_META).map_err(map_redb_err)?;
        let mut expected_ref: HashMap<String, u64> = HashMap::new();
        let mut expected_durable: HashMap<String, u64> = HashMap::new();
        for entry in media_table.iter().map_err(map_redb_err)? {
            let (key, value) = entry.map_err(map_redb_err)?;
            let _ = key;
            let meta: MediaMeta = decode_json(value.value())?;
            *expected_ref.entry(meta.blob_key.clone()).or_insert(0) += 1;
            if let Some(owner) = meta.owner_id.as_deref() {
                *expected_durable.entry(owner.to_string()).or_insert(0) = expected_durable
                    .get(owner)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(meta.size_bytes);
            }
        }
        let mut mismatches = Vec::new();
        let ref_table = read_txn.open_table(BLOB_REFCOUNT).map_err(map_redb_err)?;
        let mut stored_ref: HashMap<String, u64> = HashMap::new();
        for entry in ref_table.iter().map_err(map_redb_err)? {
            let (key, value) = entry.map_err(map_redb_err)?;
            stored_ref.insert(key.value().to_string(), value.value());
        }
        // Absent ≡ zero: counter rows removed at zero (see
        // `delete_media_record`) carry no information, so a stored zero for
        // a key with no rows is not drift. Anything else extra is.
        stored_ref.retain(|key, value| *value != 0 || expected_ref.contains_key(key));
        if stored_ref != expected_ref {
            mismatches.push(format!(
                "blob_refcount drifts (stored {} keys, expected {} keys)",
                stored_ref.len(),
                expected_ref.len()
            ));
            for (key, expected) in expected_ref.iter().take(5) {
                let stored = stored_ref.get(key).copied().unwrap_or(0);
                if stored != *expected {
                    mismatches.push(format!(
                        "refcount {key}: stored {stored}, expected {expected}"
                    ));
                }
            }
            for key in stored_ref.keys().take(5) {
                if !expected_ref.contains_key(key) {
                    mismatches.push(format!(
                        "refcount {key}: stored {}, expected absent",
                        stored_ref[key]
                    ));
                }
            }
        }
        let durable_table = read_txn
            .open_table(DURABLE_BYTES_BY_OWNER)
            .map_err(map_redb_err)?;
        let mut stored_durable: HashMap<String, u64> = HashMap::new();
        for entry in durable_table.iter().map_err(map_redb_err)? {
            let (key, value) = entry.map_err(map_redb_err)?;
            stored_durable.insert(key.value().to_string(), value.value());
        }
        stored_durable.retain(|key, value| *value != 0 || expected_durable.contains_key(key));
        if stored_durable != expected_durable {
            mismatches.push(format!(
                "durable_bytes drifts (stored {} owners, expected {} owners)",
                stored_durable.len(),
                expected_durable.len()
            ));
            for (owner, expected) in expected_durable.iter().take(5) {
                let stored = stored_durable.get(owner).copied().unwrap_or(0);
                if stored != *expected {
                    mismatches.push(format!(
                        "durable {owner}: stored {stored}, expected {expected}"
                    ));
                }
            }
            for owner in stored_durable.keys().take(5) {
                if !expected_durable.contains_key(owner) {
                    mismatches.push(format!(
                        "durable {owner}: stored {}, expected absent",
                        stored_durable[owner]
                    ));
                }
            }
        }
        Ok(mismatches)
    }
}
fn count_one_table(read_txn: &redb::ReadTransaction, name: &str) -> Result<u64, PortError> {
    // Count via the same typed definitions as export (single place where
    // the registry meets the schema).
    let mut seen = 0u64;
    export_one_table(read_txn, name, &mut |_, _, _| {
        seen += 1;
        Ok(())
    })?;
    Ok(seen)
}

/// Batch C META migration markers carried inside the `meta` table (and
/// therefore inside every bundle wholesale). Exposed so tests can assert
/// the markers survive a backup/restore round-trip.
pub fn backup_meta_marker_keys() -> Vec<&'static str> {
    vec![
        REACTIONS_BY_NOTE_DONE_KEY,
        TOKEN_ID_DONE_KEY,
        REPLY_INDEX_DONE_KEY,
        REPLY_INDEX_PROGRESS_KEY,
        URI_INDEX_DONE_KEY,
        NOTIFICATION_ORDER_DONE_KEY,
        STORAGE_ACCOUNTING_DONE_KEY,
        STORAGE_ACCOUNTING_PROGRESS_KEY,
        ACCOUNT_URI_INDEX_DONE_KEY,
        QUOTE_INDEX_DONE_KEY,
    ]
}

/// redb integrity check on a database file with no live handles: reopen
/// mutably (the API's documented lifecycle — no active transactions) and
/// report the check result. Callers must have dropped every `RedbStore`
/// and transaction first.
pub fn verify_backup_integrity(db_path: &Path) -> Result<bool, PortError> {
    let mut db = redb::Database::create(db_path)
        .map_err(|e| PortError::StorageUnavailable(format!("reopen for integrity: {e}")))?;
    db.check_integrity().map_err(map_redb_err)
}
