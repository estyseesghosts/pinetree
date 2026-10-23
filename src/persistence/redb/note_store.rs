use crate::application::ports::TimelineCursor;
use crate::domain::id::parse_aidx_time;
use redb::{ReadableTableMetadata, TableDefinition};

#[path = "note_store_edit.rs"]
mod note_store_edit;
#[path = "note_store_read.rs"]
mod note_store_read;
#[path = "note_store_write.rs"]
mod note_store_write;

pub(crate) const NOTES_BY_AUTHOR: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("notes_by_author");
/// Note drafts by id (`docs/misskey-parity-plan.md` §5; Misskey
/// `note_draft`). Values are JSON [`NoteDraft`] rows.
pub(crate) const NOTE_DRAFTS: TableDefinition<&str, &[u8]> = TableDefinition::new("note_drafts");
/// Draft index keyed `(user_id, draft_id)` (aidx draft ids are
/// time-ordered, so reverse ranges read newest-first).
pub(crate) const NOTE_DRAFTS_BY_USER: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("note_drafts_by_user");
/// Note pinnings keyed `(user_id, note_id)` (`UNIQUE(user,note)`;
/// `docs/misskey-parity-plan.md` §7; Misskey `user_note_pining`).
/// Values are JSON [`UserNotePining`] rows (which carry the pinning id
/// used for oldest-first ordering).
pub(crate) const PINNINGS: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("pinnings");
/// Direct reply edges, keyed by `(parent_note_id, child_note_id)`
/// (`docs/replies.md` Slice A). `Note.reply_id` stays authoritative; this
/// is rebuildable derived state, backfilled by a versioned migration for
/// databases created before the index existed.
pub(crate) const NOTES_BY_REPLY: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("notes_by_reply");
/// Renote edges, keyed by `(renote_target_id, child_note_id)` (Wave 4
/// scans: renote fan-out and `notes/renotes`-style reads page this index
/// instead of scanning `NOTES`). `Note.renote_id` stays authoritative;
/// maintained in the same transactions as the note row (create, delete,
/// renote-changing edit) and backfilled by a versioned migration for
/// databases created before the index existed.
pub(crate) const NOTES_BY_RENOTE: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("notes_by_renote");
/// Unresolved reply parents, keyed by `(parent_uri, child_note_id)`
/// (`docs/replies.md` Slice B). One row per note waiting on an `inReplyTo`
/// URI; removed atomically when the child links (or is deleted).
pub(crate) const NOTES_BY_REPLY_PENDING: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("notes_by_reply_pending");
/// Note edit history, keyed by edit row id (§8 `docs/misskey-parity-plan.md`;
/// Sharkey `MiNoteEdit` parity).
pub(crate) const NOTE_EDITS: TableDefinition<&str, &[u8]> = TableDefinition::new("note_edits");
/// Edit rows by `(note_id, edit_id)`, oldest-first by construction (edit ids
/// are time-ordered), driving `NoteStore::note_edits`.
pub(crate) const NOTE_EDITS_BY_NOTE: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("note_edits_by_note");
/// Unique index of live notes by locally minted quote-request IRI →
/// quoting note id (WP5 federation). Written in the same transaction as
/// the note row; rebuildable derived state, backfilled at open.
/// Last-writer-wins on collision (request IRIs are minted per quote).
pub(crate) const NOTES_BY_QUOTE_REQUEST: TableDefinition<&str, &str> =
    TableDefinition::new("notes_by_quote_request");
/// Unique index of live notes by verified quote-authorization stamp IRI →
/// quoting note id (WP5 federation). Maintained in the same transaction
/// as the note row (create/edit/delete/authorization transitions);
/// rebuildable derived state, backfilled at open. Last-writer-wins.
pub(crate) const NOTES_BY_QUOTE_STAMP: TableDefinition<&str, &str> =
    TableDefinition::new("notes_by_quote_stamp");

/// Case-insensitive substring match of any non-empty word in any haystack
/// (prohibited-words check shared by the create and edit transactions).
fn contains_any_word(haystacks: &[&str], words: &[String]) -> bool {
    for text in haystacks {
        let lower = text.to_lowercase();
        for word in words {
            let w = word.trim().to_lowercase();
            if !w.is_empty() && lower.contains(w.as_str()) {
                return true;
            }
        }
    }
    false
}

pub(crate) fn matches_cursor(note_id: &str, cursor: &TimelineCursor) -> bool {
    if let Some(since_id) = &cursor.since_id {
        if note_id <= since_id.as_str() {
            return false;
        }
    }
    if let Some(until_id) = &cursor.until_id {
        if note_id >= until_id.as_str() {
            return false;
        }
    }
    if cursor.since_date.is_some() || cursor.until_date.is_some() {
        if let Ok(ts) = parse_aidx_time(note_id) {
            if let Some(since_date) = cursor.since_date {
                if ts <= since_date {
                    return false;
                }
            }
            if let Some(until_date) = cursor.until_date {
                if ts >= until_date {
                    return false;
                }
            }
        }
    }
    true
}
