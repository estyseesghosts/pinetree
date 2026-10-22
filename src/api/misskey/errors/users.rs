//! `users` error constructors (split from `errors.rs`).
//!
#![allow(unused_imports)]
use super::codes::*;
use super::error::ApiError;

impl ApiError {
    /// Misskey `users/show.ts:52-57` `noSuchUser` (Batch Q Phase 1 item 3):
    /// verbatim id + 404, covering both missing and suspended users for
    /// non-moderators.
    pub fn users_show_no_such_user() -> Self {
        Self::client(
            "No such user.",
            "NO_SUCH_USER",
            USERS_SHOW_NO_SUCH_USER_ID,
            404,
        )
    }
    /// Gaps final wiring: `users/update-memo` `NO_SUCH_USER` (target
    /// missing; per-endpoint UUID like `users/show`).
    pub fn update_memo_no_such_user() -> Self {
        Self::client(
            "No such user.",
            "NO_SUCH_USER",
            UPDATE_MEMO_NO_SUCH_USER_ID,
            400,
        )
    }
}
