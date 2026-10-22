//! `emoji` error constructors (split from `errors.rs`).
//!
#![allow(unused_imports)]
use super::codes::*;
use super::error::ApiError;

impl ApiError {
    /// Misskey `admin/emoji/add.ts` `NO_SUCH_FILE` (`No such file.`,
    /// verbatim add-endpoint UUID `fc46b5a4-…`). The update endpoint mints
    /// its own UUID (`14fb9fd9-…`); pinetree emits the add UUID for both
    /// (one-UUID-per-code, §16 convention).
    pub fn emoji_no_such_file() -> Self {
        Self::client(
            "No such file.",
            "NO_SUCH_FILE",
            EMOJI_ADD_NO_SUCH_FILE_ID,
            400,
        )
    }
}
