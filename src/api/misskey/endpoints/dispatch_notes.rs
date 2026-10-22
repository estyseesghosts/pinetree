//! Notes dispatcher arms.
//!
//! Split from `endpoints/mod.rs` (decomposition). Pure move, no behavior change.
#![allow(unused_imports)]

mod create;
mod drafts;
mod reactions;
mod threads;
mod timeline;

pub(crate) use create::notes_create;
pub(crate) use create::notes_delete;
pub(crate) use create::notes_edit;
pub(crate) use create::notes_show;
pub(crate) use drafts::i_pin;
pub(crate) use drafts::i_unpin;
pub(crate) use drafts::notes_drafts_count;
pub(crate) use drafts::notes_drafts_create;
pub(crate) use drafts::notes_drafts_delete;
pub(crate) use drafts::notes_drafts_list;
pub(crate) use drafts::notes_drafts_update;
pub(crate) use drafts::notes_schedule_create;
pub(crate) use drafts::notes_schedule_delete;
pub(crate) use drafts::notes_schedule_list;
pub(crate) use reactions::i_favorites;
pub(crate) use reactions::notes_favorites_create;
pub(crate) use reactions::notes_favorites_delete;
pub(crate) use reactions::notes_like;
pub(crate) use reactions::notes_polls_vote;
pub(crate) use reactions::notes_reactions;
pub(crate) use reactions::notes_reactions_create;
pub(crate) use reactions::notes_reactions_delete;
pub(crate) use reactions::notes_state;
pub(crate) use reactions::users_reactions;
pub(crate) use threads::notes_children;
pub(crate) use threads::notes_conversation;
pub(crate) use threads::notes_featured;
pub(crate) use threads::notes_renotes;
pub(crate) use threads::notes_replies;
pub(crate) use threads::notes_thread_muting_create;
pub(crate) use threads::notes_thread_muting_delete;
pub(crate) use threads::notes_translate;
pub(crate) use threads::notes_unrenote;
pub(crate) use threads::notes_versions;
pub(crate) use threads::users_featured_notes;
pub(crate) use timeline::notes_global_timeline;
pub(crate) use timeline::notes_hybrid_timeline;
pub(crate) use timeline::notes_local_timeline;
pub(crate) use timeline::notes_mentions;
pub(crate) use timeline::notes_recommendations;
pub(crate) use timeline::notes_search;
pub(crate) use timeline::notes_search_by_tag;
pub(crate) use timeline::notes_timeline;
pub(crate) use timeline::notes_user_list_timeline;
