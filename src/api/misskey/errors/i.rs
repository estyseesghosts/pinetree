//! `i` error constructors (split from `errors.rs`).
//!
#![allow(unused_imports)]
use super::codes::*;
use super::error::ApiError;
use super::lookup::new_id_for;

impl ApiError {
    /// Misskey `i/import-*.ts:26-50` file-guard errors (verbatim upstream
    /// code/message/id, keyed per endpoint — upstream mints one UUID per
    /// endpoint per code). `endpoint` is the Misskey endpoint name
    /// (`i/import-blocking`, …); unknown names fall back to a deterministic
    /// pinetree UUID with the same code/message.
    pub fn import_no_such_file(endpoint: &str) -> Self {
        let id = match endpoint {
            "i/import-blocking" => IMPORT_BLOCKING_NO_SUCH_FILE_ID,
            "i/import-following" => IMPORT_FOLLOWING_NO_SUCH_FILE_ID,
            "i/import-muting" => IMPORT_MUTING_NO_SUCH_FILE_ID,
            "i/import-user-lists" => IMPORT_USER_LISTS_NO_SUCH_FILE_ID,
            "i/import-antennas" => IMPORT_ANTENNAS_NO_SUCH_FILE_ID,
            "i/import-clips" => IMPORT_CLIPS_NO_SUCH_FILE_ID,
            _ => NO_SUCH_FILE_ID,
        };
        Self::client("No such file.", "NO_SUCH_FILE", id, 400)
    }
    /// Misskey `i/import-*.ts` `EMPTY_FILE` (`That file is empty.`,
    /// verbatim per-endpoint UUID).
    pub fn import_empty_file(endpoint: &str) -> Self {
        let id: String = match endpoint {
            "i/import-blocking" => IMPORT_BLOCKING_EMPTY_FILE_ID.to_string(),
            "i/import-following" => IMPORT_FOLLOWING_EMPTY_FILE_ID.to_string(),
            "i/import-muting" => IMPORT_MUTING_EMPTY_FILE_ID.to_string(),
            "i/import-user-lists" => IMPORT_USER_LISTS_EMPTY_FILE_ID.to_string(),
            "i/import-antennas" => IMPORT_ANTENNAS_EMPTY_FILE_ID.to_string(),
            "i/import-clips" => IMPORT_CLIPS_EMPTY_FILE_ID.to_string(),
            _ => new_id_for("EMPTY_FILE"),
        };
        Self::client("That file is empty.", "EMPTY_FILE", &id, 400)
    }
    /// Misskey `i/import-*.ts` `TOO_BIG_FILE` (`That file is too big.`,
    /// verbatim per-endpoint UUID). `i/import-antennas` has no such error
    /// upstream (it never size-checks), so it is absent from the match.
    pub fn import_too_big_file(endpoint: &str) -> Self {
        let id: String = match endpoint {
            "i/import-blocking" => IMPORT_BLOCKING_TOO_BIG_FILE_ID.to_string(),
            "i/import-following" => IMPORT_FOLLOWING_TOO_BIG_FILE_ID.to_string(),
            "i/import-muting" => IMPORT_MUTING_TOO_BIG_FILE_ID.to_string(),
            "i/import-user-lists" => IMPORT_USER_LISTS_TOO_BIG_FILE_ID.to_string(),
            "i/import-clips" => IMPORT_CLIPS_TOO_BIG_FILE_ID.to_string(),
            _ => new_id_for("TOO_BIG_FILE"),
        };
        Self::client("That file is too big.", "TOO_BIG_FILE", &id, 400)
    }
    /// Misskey `i/import-antennas.ts` `TOO_MANY_ANTENNAS` (`You cannot
    /// create antenna any more.`, verbatim import-endpoint UUID
    /// `600917d4-…`). The create/update path emits the create-endpoint UUID
    /// (`faf47050-…`, one-UUID-per-code).
    pub fn import_too_many_antennas() -> Self {
        Self::client(
            "You cannot create antenna any more.",
            "TOO_MANY_ANTENNAS",
            IMPORT_ANTENNAS_TOO_MANY_ID,
            400,
        )
    }
    /// Gaps final wiring: `system-webhook/{show,update,delete}` +
    /// `webhooks/{show,update,delete}` `NO_SUCH_WEBHOOK` (one UUID per
    /// endpoint, upstream `i/webhooks/*` convention; pinetree-minted).
    /// The store layer reports lowercase `no such webhook`; this maps it
    /// with the arm's endpoint identity.
    pub fn webhook_no_such_webhook(endpoint: &str) -> Self {
        let id = match endpoint {
            "system-webhook/show" => SYSTEM_WEBHOOK_SHOW_NO_SUCH_ID,
            "system-webhook/update" => SYSTEM_WEBHOOK_UPDATE_NO_SUCH_ID,
            "system-webhook/delete" => SYSTEM_WEBHOOK_DELETE_NO_SUCH_ID,
            "webhooks/show" => WEBHOOK_SHOW_NO_SUCH_ID,
            "webhooks/update" => WEBHOOK_UPDATE_NO_SUCH_ID,
            "webhooks/delete" => WEBHOOK_DELETE_NO_SUCH_ID,
            _ => WEBHOOK_SHOW_NO_SUCH_ID,
        };
        Self::client("No such webhook.", "NO_SUCH_WEBHOOK", id, 400)
    }
    /// Gaps final wiring: `webhooks/create` over the per-user cap.
    /// Upstream has no limit error; pinetree-defined `TOO_MANY_WEBHOOKS`.
    pub fn webhook_too_many() -> Self {
        Self::client(
            "You cannot create webhooks any more.",
            "TOO_MANY_WEBHOOKS",
            WEBHOOK_CREATE_TOO_MANY_ID,
            400,
        )
    }
}
