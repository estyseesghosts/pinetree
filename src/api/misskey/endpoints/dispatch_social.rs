//! Following/clips/pages/antennas/invites dispatcher arms.
//!
//! Split from `endpoints/mod.rs` (decomposition). Pure move, no behavior change.
#![allow(unused_imports)]

mod antennas;
mod clips;
mod following;
mod invites;
mod pages;

pub(crate) use antennas::antennas_create;
pub(crate) use antennas::antennas_delete;
pub(crate) use antennas::antennas_list;
pub(crate) use antennas::antennas_notes;
pub(crate) use antennas::antennas_remove_note;
pub(crate) use antennas::antennas_show;
pub(crate) use antennas::antennas_update;
pub(crate) use clips::clips_add_note;
pub(crate) use clips::clips_create;
pub(crate) use clips::clips_delete;
pub(crate) use clips::clips_favorite;
pub(crate) use clips::clips_follow;
pub(crate) use clips::clips_followers;
pub(crate) use clips::clips_list;
pub(crate) use clips::clips_my_favorites;
pub(crate) use clips::clips_notes;
pub(crate) use clips::clips_remove_note;
pub(crate) use clips::clips_show;
pub(crate) use clips::clips_unfavorite;
pub(crate) use clips::clips_unfollow;
pub(crate) use clips::clips_update;
pub(crate) use following::following_create;
pub(crate) use following::following_delete;
pub(crate) use following::following_invalidate;
pub(crate) use following::following_list;
pub(crate) use following::following_requests_accept;
pub(crate) use following::following_requests_cancel;
pub(crate) use following::following_requests_list;
pub(crate) use following::following_requests_reject;
pub(crate) use following::following_requests_sent;
pub(crate) use following::following_update;
pub(crate) use following::following_update_all;
pub(crate) use following::pack_follow_request;
pub(crate) use invites::invite_create;
pub(crate) use invites::invite_delete;
pub(crate) use invites::invite_limit;
pub(crate) use invites::invite_list;
pub(crate) use pages::pages_create;
pub(crate) use pages::pages_delete;
pub(crate) use pages::pages_featured;
pub(crate) use pages::pages_like;
pub(crate) use pages::pages_show;
pub(crate) use pages::pages_unlike;
pub(crate) use pages::pages_update;
