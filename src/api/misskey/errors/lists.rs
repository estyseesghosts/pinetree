//! `lists` error constructors (split from `errors.rs`).
//!
#![allow(unused_imports)]
use super::codes::*;
use super::error::ApiError;

impl ApiError {
    /// Misskey `users/lists/*` `NO_SUCH_LIST` (`No such list.`, verbatim
    /// per-endpoint UUID): `endpoint` is the Misskey endpoint name
    /// (`users/lists/update|delete|show|get-memberships|pull|
    /// update-membership`, `notes/user-list-timeline`).
    pub fn user_list_no_such_list(endpoint: &str) -> Self {
        let id = match endpoint {
            "users/lists/update" => LIST_NO_SUCH_LIST_UPDATE_ID,
            "users/lists/delete" => LIST_NO_SUCH_LIST_DELETE_ID,
            "users/lists/show" | "users/lists/get-memberships" => LIST_NO_SUCH_LIST_SHOW_ID,
            "users/lists/pull" | "users/lists/update-membership" | "users/lists/push" => {
                LIST_NO_SUCH_LIST_MEMBER_ID
            }
            "notes/user-list-timeline" => LIST_TIMELINE_NO_SUCH_LIST_ID,
            _ => LIST_NO_SUCH_LIST_SHOW_ID,
        };
        Self::client("No such list.", "NO_SUCH_LIST", id, 400)
    }
    /// Misskey `users/lists/*` `NO_SUCH_USER` (verbatim per-endpoint UUID).
    pub fn user_list_no_such_user(endpoint: &str) -> Self {
        let id = match endpoint {
            "users/lists/list" => LIST_NO_SUCH_USER_LIST_QUERY_ID,
            "users/lists/push" => LIST_NO_SUCH_USER_PUSH_ID,
            _ => LIST_NO_SUCH_USER_PULL_ID,
        };
        Self::client("No such user.", "NO_SUCH_USER", id, 400)
    }
    /// Misskey `users/lists/list` `REMOTE_USER_NOT_ALLOWED`.
    pub fn user_list_remote_not_allowed() -> Self {
        Self::client(
            "Not allowed to load the remote user's list",
            "REMOTE_USER_NOT_ALLOWED",
            LIST_REMOTE_USER_NOT_ALLOWED_ID,
            400,
        )
    }
    /// Misskey `users/lists/list` `INVALID_PARAM` (anonymous without
    /// `userId`; verbatim id).
    pub fn user_list_query_invalid_param() -> Self {
        Self::client(
            "Invalid param.",
            "INVALID_PARAM",
            LIST_QUERY_INVALID_PARAM_ID,
            400,
        )
    }
    /// Misskey `users/lists/create` `TOO_MANY_USERLISTS`.
    pub fn user_list_too_many() -> Self {
        Self::client(
            "You cannot create user list any more.",
            "TOO_MANY_USERLISTS",
            LIST_TOO_MANY_USERLISTS_ID,
            400,
        )
    }
    /// Misskey `users/lists/push` `ALREADY_ADDED`.
    pub fn user_list_already_added() -> Self {
        Self::client(
            "That user has already been added to that list.",
            "ALREADY_ADDED",
            LIST_ALREADY_ADDED_ID,
            400,
        )
    }
    /// Misskey `users/lists/push` `YOU_HAVE_BEEN_BLOCKED`.
    pub fn user_list_blocked() -> Self {
        Self::client(
            "You cannot push this user because you have been blocked by this user.",
            "YOU_HAVE_BEEN_BLOCKED",
            LIST_YOU_HAVE_BEEN_BLOCKED_ID,
            400,
        )
    }
    /// Misskey `users/lists/push` `TOO_MANY_USERS`.
    pub fn user_list_too_many_users() -> Self {
        Self::client(
            "You can not push users any more.",
            "TOO_MANY_USERS",
            LIST_TOO_MANY_USERS_ID,
            400,
        )
    }
    /// Misskey `users/lists/favorite|unfavorite` `NO_SUCH_USER_LIST`
    /// (verbatim per-endpoint UUID).
    pub fn user_list_no_such_user_list(endpoint: &str) -> Self {
        let id = match endpoint {
            "users/lists/favorite" => LIST_NO_SUCH_USER_LIST_FAVORITE_ID,
            _ => LIST_NO_SUCH_USER_LIST_UNFAVORITE_ID,
        };
        Self::client("No such user list.", "NO_SUCH_USER_LIST", id, 400)
    }
    /// Misskey `users/lists/favorite` `ALREADY_FAVORITED`.
    pub fn user_list_already_favorited() -> Self {
        Self::client(
            "The list has already been favorited.",
            "ALREADY_FAVORITED",
            LIST_ALREADY_FAVORITED_ID,
            400,
        )
    }
    /// Misskey `users/lists/unfavorite` `notFavorited` quirk: the wire code
    /// stays `ALREADY_FAVORITED` with its own UUID (verbatim).
    pub fn user_list_not_favorited() -> Self {
        Self::client(
            "You have not favorited the list.",
            "ALREADY_FAVORITED",
            LIST_NOT_FAVORITED_ID,
            400,
        )
    }
}
