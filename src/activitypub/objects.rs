//! ActivityPub JSON-LD `@context`, object/activity rendering and parsing,
//! `to`/`cc` ↔ `Visibility` audience mapping, and HTML ↔ MFM conversion +
//! sanitization (Phase 2 Slices B1 & B2).
//!
//! Never emits deprecated `_misskey_content` (uses `source: { content,
//! mediaType: "text/x.misskeymarkdown" }` instead). Reactions use
//! `_misskey_reaction` alongside `content` per proposal §7.

mod activities;
mod audience_context;
mod markup;
mod note_document;
mod note_parser;
mod object_handlers;
mod poll;
mod quote_activities;
mod tombstone;

pub use activities::{
    render_accept_follow_activity, render_block_activity, render_create_or_announce_for_note,
    render_emoji_react_activity, render_featured_change_activity, render_flag_activity,
    render_follow_activity, render_like_activity, render_like_activity_for, render_move_activity,
    render_note_update_activity, render_reject_follow_activity, render_undo_activity,
    render_update_actor_activity,
};
pub use audience_context::{
    AP_CONTENT_TYPE, PUBLIC_COLLECTION_COMPACT_1, PUBLIC_COLLECTION_COMPACT_2,
    PUBLIC_COLLECTION_URI, ap_context, extract_uri_list, is_ap_accept, is_public_collection_uri,
    parse_note_audience, render_note_audience,
};
pub use markup::{html_escape, mfm_to_html, sanitize_html_to_mfm, sanitize_mfm_text};
pub use note_document::render_note_object;
pub use note_parser::{
    ParsedAttachment, ParsedRemoteNote, extract_like_reaction, parse_remote_note,
};
pub use object_handlers::{
    dedup_strings, extract_object_uri, get_note_activity_ap, get_note_ap, get_quote_stamp,
};
pub use poll::{
    ParsedPoll, parse_ap_time_ms, parse_question_poll, render_question,
    render_question_update_activity, render_vote_activity,
};
pub use quote_activities::{
    render_delete_stamp_activity, render_quote_request_activity, render_quote_response_activity,
    render_quote_stamp_document,
};
pub use tombstone::{
    render_delete_note_activity, render_delete_person_activity, render_tombstone,
    tombstone_inner_id,
};
