//! `social` error constructors (split from `errors.rs`).
//!
#![allow(unused_imports)]
use super::codes::*;
use super::error::ApiError;

impl ApiError {
    /// Misskey `following/invalidate.ts` verbatim errors (per-endpoint
    /// UUIDs, upstream `29-45`). Pinetree's shared `NO_SUCH_USER` /
    /// `NOT_FOLLOWING` UUIDs differ, so invalidate mints its own.
    pub fn invalidate_no_such_user() -> Self {
        Self::client(
            "No such user.",
            "NO_SUCH_USER",
            INVALIDATE_NO_SUCH_USER_ID,
            400,
        )
    }
    /// Misskey `following/invalidate.ts` `FOLLOWER_IS_YOURSELF`.
    pub fn follower_is_yourself() -> Self {
        Self::client(
            "Follower is yourself.",
            "FOLLOWER_IS_YOURSELF",
            FOLLOWER_IS_YOURSELF_ID,
            400,
        )
    }
    /// Misskey `following/invalidate.ts` `NOT_FOLLOWING`.
    pub fn invalidate_not_following() -> Self {
        Self::client(
            "You are not following that user.",
            "NOT_FOLLOWING",
            INVALIDATE_NOT_FOLLOWING_ID,
            400,
        )
    }
    /// Misskey `clips/*` `NO_SUCH_CLIP` (`No such clip.`, verbatim
    /// per-endpoint UUID — upstream assigns one UUID per endpoint).
    pub fn clip_no_such_clip(endpoint: &str) -> Self {
        let id = match endpoint {
            "clips/create" => CLIP_CREATE_TOO_MANY_ID,
            "clips/update" => CLIP_UPDATE_NO_SUCH_CLIP_ID,
            "clips/delete" => CLIP_DELETE_NO_SUCH_CLIP_ID,
            "clips/show" => CLIP_SHOW_NO_SUCH_CLIP_ID,
            "clips/notes" => CLIP_NOTES_NO_SUCH_CLIP_ID,
            "clips/add-note" => CLIP_ADD_NOTE_NO_SUCH_CLIP_ID,
            "clips/remove-note" => CLIP_REMOVE_NOTE_NO_SUCH_CLIP_ID,
            "clips/favorite" => CLIP_FAVORITE_NO_SUCH_CLIP_ID,
            "clips/unfavorite" => CLIP_UNFAVORITE_NO_SUCH_CLIP_ID,
            "clips/follow" => CLIP_FOLLOW_NO_SUCH_CLIP_ID,
            "clips/unfollow" => CLIP_FOLLOW_NO_SUCH_CLIP_ID,
            "clips/followers" => CLIP_FOLLOW_NO_SUCH_CLIP_ID,
            _ => CLIP_SHOW_NO_SUCH_CLIP_ID,
        };
        // `clips/create` never emits `NO_SUCH_CLIP` (it emits
        // `TOO_MANY_CLIPS`); the arm above is unreachable defensively.
        Self::client("No such clip.", "NO_SUCH_CLIP", id, 400)
    }
    /// Misskey `clips/create` `TOO_MANY_CLIPS` (verbatim).
    pub fn clip_too_many_clips() -> Self {
        Self::client(
            "You cannot create clip any more.",
            "TOO_MANY_CLIPS",
            CLIP_CREATE_TOO_MANY_ID,
            400,
        )
    }
    /// Misskey `clips/{add-note,remove-note}` `NO_SUCH_NOTE` (`No such
    /// note.`, verbatim per-endpoint UUID).
    pub fn clip_no_such_note(endpoint: &str) -> Self {
        let id = match endpoint {
            "clips/remove-note" => CLIP_REMOVE_NOTE_NO_SUCH_NOTE_ID,
            _ => CLIP_ADD_NOTE_NO_SUCH_NOTE_ID,
        };
        Self::client("No such note.", "NO_SUCH_NOTE", id, 400)
    }
    /// Misskey `clips/add-note` `ALREADY_CLIPPED` (verbatim).
    pub fn clip_already_clipped() -> Self {
        Self::client(
            "The note has already been clipped.",
            "ALREADY_CLIPPED",
            CLIP_ALREADY_CLIPPED_ID,
            400,
        )
    }
    /// Misskey `clips/add-note` `TOO_MANY_CLIP_NOTES` (verbatim).
    pub fn clip_too_many_clip_notes() -> Self {
        Self::client(
            "You cannot add notes to the clip any more.",
            "TOO_MANY_CLIP_NOTES",
            CLIP_TOO_MANY_CLIP_NOTES_ID,
            400,
        )
    }
    /// Misskey `clips/favorite` `ALREADY_FAVORITED` (verbatim).
    pub fn clip_already_favorited() -> Self {
        Self::client(
            "The clip has already been favorited.",
            "ALREADY_FAVORITED",
            CLIP_ALREADY_FAVORITED_ID,
            400,
        )
    }
    /// Misskey `clips/unfavorite` `NOT_FAVORITED` (verbatim).
    pub fn clip_not_favorited() -> Self {
        Self::client(
            "You have not favorited the clip.",
            "NOT_FAVORITED",
            CLIP_NOT_FAVORITED_ID,
            400,
        )
    }
    /// Pinetree `clips/followers` non-owner read (`Access denied.`,
    /// pinetree-fixed UUID — follower sets never advertise to non-owners).
    pub fn clip_followers_forbidden() -> Self {
        Self::client(
            "Access denied.",
            "ACCESS_DENIED",
            CLIP_FOLLOWERS_FORBIDDEN_ID,
            403,
        )
    }
    /// Misskey `pages/*` `NO_SUCH_PAGE` (`No such page.`, verbatim
    /// per-endpoint UUID).
    pub fn page_no_such_page(endpoint: &str) -> Self {
        let id = match endpoint {
            "pages/create" => PAGE_CREATE_NO_SUCH_FILE_ID,
            "pages/update" => PAGE_UPDATE_NO_SUCH_PAGE_ID,
            "pages/delete" => PAGE_DELETE_NO_SUCH_PAGE_ID,
            "pages/show" => PAGE_SHOW_NO_SUCH_PAGE_ID,
            "pages/like" => PAGE_LIKE_NO_SUCH_PAGE_ID,
            "pages/unlike" => PAGE_UNLIKE_NO_SUCH_PAGE_ID,
            _ => PAGE_SHOW_NO_SUCH_PAGE_ID,
        };
        // `pages/create` never emits `NO_SUCH_PAGE`; defensive arm only.
        Self::client("No such page.", "NO_SUCH_PAGE", id, 400)
    }
    /// Misskey `pages/{update,delete}` `ACCESS_DENIED` (`Access denied.`,
    /// verbatim per-endpoint UUID).
    pub fn page_access_denied(endpoint: &str) -> Self {
        let id = match endpoint {
            "pages/delete" => PAGE_DELETE_ACCESS_DENIED_ID,
            _ => PAGE_UPDATE_ACCESS_DENIED_ID,
        };
        Self::permission("Access denied.", "ACCESS_DENIED", id, 403)
    }
    /// Misskey `pages/{create,update}` `NO_SUCH_FILE` (`No such file.`,
    /// verbatim per-endpoint UUID — the eye-catch image must be owned).
    pub fn page_no_such_file(endpoint: &str) -> Self {
        let id = match endpoint {
            "pages/update" => PAGE_UPDATE_NO_SUCH_FILE_ID,
            _ => PAGE_CREATE_NO_SUCH_FILE_ID,
        };
        Self::client("No such file.", "NO_SUCH_FILE", id, 400)
    }
    /// Misskey `pages/{create,update}` `NAME_ALREADY_EXISTS` (`Specified
    /// name already exists.`, verbatim per-endpoint UUID).
    pub fn page_name_already_exists(endpoint: &str) -> Self {
        let id = match endpoint {
            "pages/update" => PAGE_UPDATE_NAME_ALREADY_EXISTS_ID,
            _ => PAGE_CREATE_NAME_ALREADY_EXISTS_ID,
        };
        Self::client(
            "Specified name already exists.",
            "NAME_ALREADY_EXISTS",
            id,
            400,
        )
    }
    /// Misskey `pages/like` `YOUR_PAGE` (verbatim).
    pub fn page_your_page() -> Self {
        Self::client(
            "You cannot like your page.",
            "YOUR_PAGE",
            PAGE_YOUR_PAGE_ID,
            400,
        )
    }
    /// Misskey `pages/like` `ALREADY_LIKED` (verbatim).
    pub fn page_already_liked() -> Self {
        Self::client(
            "The page has already been liked.",
            "ALREADY_LIKED",
            PAGE_ALREADY_LIKED_ID,
            400,
        )
    }
    /// Misskey `pages/unlike` `NOT_LIKED` (verbatim).
    pub fn page_not_liked() -> Self {
        Self::client(
            "You have not liked that page.",
            "NOT_LIKED",
            PAGE_NOT_LIKED_ID,
            400,
        )
    }
}
