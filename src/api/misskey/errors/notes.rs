//! `notes` error constructors (split from `errors.rs`).
//!
#![allow(unused_imports)]
use super::codes::*;
use super::error::ApiError;

impl ApiError {
    /// Sharkey `notes/schedule/create.ts` `SCHEDULE_NOTE_MAX` (`Schedule
    /// note max.`, verbatim UUID).
    pub fn schedule_note_max() -> Self {
        Self::client(
            "Schedule note max.",
            "SCHEDULE_NOTE_MAX",
            SCHEDULE_NOTE_MAX_ID,
            400,
        )
    }
    /// Sharkey `notes/schedule/create.ts` `CANNOT_RENOTE_TO_A_PURE_RENOTE`
    /// (`You can not Renote a pure Renote.`, verbatim UUID). The
    /// `notes/create` path emits the same code under Misskey's
    /// `CANNOT_RENOTE` UUID (one-UUID-per-code for the shared code
    /// string); the shim needs Sharkey's UUID verbatim.
    pub fn sharkey_cannot_rerenote() -> Self {
        Self::client(
            "You can not Renote a pure Renote.",
            "CANNOT_RENOTE_TO_A_PURE_RENOTE",
            SHARKEY_CANNOT_RERENOTE_ID,
            400,
        )
    }
    /// Sharkey `notes/schedule/delete.ts` `NO_SUCH_NOTE` (`No such note.`,
    /// verbatim UUID `a58056ba-…` — different from pinetree's
    /// `NO_SUCH_NOTE` UUID).
    pub fn schedule_no_such_note() -> Self {
        Self::client(
            "No such note.",
            "NO_SUCH_NOTE",
            SCHEDULE_NO_SUCH_NOTE_ID,
            400,
        )
    }
    /// Sharkey `notes/schedule/delete.ts` `PERMISSION_DENIED`
    /// (`Permission denied.`, verbatim UUID, HTTP 403).
    pub fn schedule_permission_denied() -> Self {
        Self::permission(
            "Permission denied.",
            "PERMISSION_DENIED",
            SCHEDULE_PERMISSION_DENIED_ID,
            403,
        )
    }
    /// Sharkey `notes/versions.ts` `SIGNIN_REQUIRED` (private-note history
    /// without a viewer).
    pub fn versions_signin_required() -> Self {
        Self::client(
            "Signin required.",
            "SIGNIN_REQUIRED",
            VERSIONS_SIGNIN_REQUIRED_ID,
            401,
        )
    }
    /// Gaps final wiring: `notes/thread-muting/{create,delete}`
    /// `NO_SUCH_NOTE` (per-endpoint UUID, upstream convention).
    pub fn thread_mute_no_such_note(endpoint: &str) -> Self {
        let id = match endpoint {
            "notes/thread-muting/delete" => THREAD_MUTE_DELETE_NO_SUCH_NOTE_ID,
            _ => THREAD_MUTE_CREATE_NO_SUCH_NOTE_ID,
        };
        Self::client("No such note.", "NO_SUCH_NOTE", id, 400)
    }
}
