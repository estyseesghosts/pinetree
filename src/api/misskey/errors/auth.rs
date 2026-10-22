//! `auth` error constructors (split from `errors.rs`).
//!
#![allow(unused_imports)]
use serde_json::{Map, Value};

use super::codes::*;
use super::error::ApiError;

impl ApiError {
    /// Misskey `signin-flow` unknown username (verbatim upstream id,
    /// 404; `SigninApiService.ts` answers `{ error: { id } }` with no
    /// code/message — the code/message here are pinetree-chosen).
    pub fn signin_no_such_user() -> Self {
        Self::client("No such user.", "NO_SUCH_USER", SIGNIN_NO_SUCH_USER_ID, 404)
    }
    /// Misskey `signin-flow` suspended account (verbatim upstream id,
    /// 403). Checked before the password, like upstream (unlike the
    /// legacy `signin` arm, which checks after).
    pub fn signin_suspended() -> Self {
        Self::permission(
            "Your account has been suspended.",
            "USER_SUSPENDED",
            SIGNIN_SUSPENDED_ID,
            403,
        )
    }
    /// Misskey `signin-flow` wrong password / wrong totp (verbatim
    /// upstream id, 403).
    pub fn signin_incorrect_password() -> Self {
        Self::client(
            "Incorrect password.",
            "INCORRECT_PASSWORD",
            SIGNIN_INCORRECT_PASSWORD_ID,
            403,
        )
    }
    /// Misskey `signin-flow` IP rate limit (verbatim upstream
    /// code/message/id, 429 with `Retry-After`).
    pub fn signin_too_many_failures(retry_after_secs: u64) -> Self {
        let mut err = Self::client(
            "Too many failed attempts to sign in. Try again later.",
            "TOO_MANY_AUTHENTICATION_FAILURES",
            SIGNIN_RATE_LIMITED_ID,
            429,
        )
        .with_info(Value::Object({
            let mut info = Map::new();
            info.insert(
                "retryAfter".to_string(),
                Value::Number(retry_after_secs.into()),
            );
            info
        }));
        err.retry_after_secs = Some(retry_after_secs);
        err
    }
    /// Batch P email/move/registry/translate verbatim errors (upstream
    /// `code` + UUID per endpoint file).
    /// Misskey `i/update-email.ts` `INCORRECT_PASSWORD`.
    pub fn update_email_incorrect_password() -> Self {
        Self::client(
            "Incorrect password.",
            "INCORRECT_PASSWORD",
            UPDATE_EMAIL_INCORRECT_PASSWORD_ID,
            400,
        )
    }
    /// Misskey `i/update-email.ts` `UNAVAILABLE`.
    pub fn update_email_unavailable() -> Self {
        Self::client(
            "Unavailable email address.",
            "UNAVAILABLE",
            UPDATE_EMAIL_UNAVAILABLE_ID,
            400,
        )
    }
    /// Misskey `i/update-email.ts` `EMAIL_REQUIRED`.
    pub fn update_email_required() -> Self {
        Self::client(
            "Email address is required.",
            "EMAIL_REQUIRED",
            UPDATE_EMAIL_REQUIRED_ID,
            400,
        )
    }
    /// Misskey `verify-email.ts` `NO_SUCH_CODE`.
    pub fn verify_email_no_such_code() -> Self {
        Self::client(
            "No such code.",
            "NO_SUCH_CODE",
            VERIFY_EMAIL_NO_SUCH_CODE_ID,
            404,
        )
    }
    /// Misskey `i/move.ts` `DESTINATION_ACCOUNT_FORBIDS`.
    pub fn move_destination_forbids() -> Self {
        Self::client(
            "Destination account doesn't have proper 'Known As' alias, or has already moved.",
            "DESTINATION_ACCOUNT_FORBIDS",
            MOVE_DESTINATION_FORBIDS_ID,
            400,
        )
    }
    /// Misskey `i/move.ts` `NOT_ROOT_FORBIDDEN` (upstream message/code:
    /// "The root can't migrate." / `NOT_ROOT_FORBIDDEN`).
    pub fn move_root_forbidden() -> Self {
        Self::client(
            "The root can't migrate.",
            "NOT_ROOT_FORBIDDEN",
            MOVE_ROOT_FORBIDDEN_ID,
            400,
        )
    }
    /// Misskey `i/move.ts` `NO_SUCH_USER` (shared `NO_SUCH_USER` UUID).
    pub fn move_no_such_user() -> Self {
        Self::client("No such user.", "NO_SUCH_USER", NO_SUCH_USER_ID, 404)
    }
    /// Misskey `i/move.ts` `URI_NULL`.
    pub fn move_uri_null() -> Self {
        Self::client(
            "User ActivityPup URI is null.",
            "URI_NULL",
            MOVE_URI_NULL_ID,
            400,
        )
    }
    /// Misskey `i/move.ts` `ALREADY_MOVED`.
    pub fn move_already_moved() -> Self {
        Self::client(
            "Account was already moved to another account.",
            "ALREADY_MOVED",
            MOVE_ALREADY_MOVED_ID,
            400,
        )
    }
    /// Misskey `i/registry/get|get-detail.ts` `NO_SUCH_KEY`.
    pub fn registry_no_such_key() -> Self {
        Self::client("No such key.", "NO_SUCH_KEY", REGISTRY_NO_SUCH_KEY_ID, 404)
    }
    /// Misskey `fetch-external-resources.ts`
    /// `EXT_RESOURCE_RETURNED_INVALID_SCHEMA`.
    pub fn ext_resource_invalid_schema() -> Self {
        Self::client(
            "External resource returned invalid schema.",
            "EXT_RESOURCE_RETURNED_INVALID_SCHEMA",
            EXT_RESOURCE_INVALID_SCHEMA_ID,
            400,
        )
    }
    /// Misskey `fetch-external-resources.ts`
    /// `EXT_RESOURCE_HASH_DIDNT_MATCH`.
    pub fn ext_resource_hash_mismatch() -> Self {
        Self::client(
            "Hash did not match.",
            "EXT_RESOURCE_HASH_DIDNT_MATCH",
            EXT_RESOURCE_HASH_MISMATCH_ID,
            400,
        )
    }
    /// Misskey `notes/translate.ts` `UNAVAILABLE` (no backend / no
    /// permission). The dormant-transport posture reuses this code with a
    /// first-party UUID (`TRANSLATE_NOT_CONFIGURED_ID`).
    pub fn translate_unavailable() -> Self {
        Self::client(
            "Translate of notes unavailable.",
            "UNAVAILABLE",
            TRANSLATE_UNAVAILABLE_ID,
            400,
        )
    }
    /// Batch P dormant-transport posture: no translation backend is
    /// configured, so the send path fails loudly (never silently drops).
    /// Same `UNAVAILABLE` code as upstream, first-party UUID.
    pub fn translate_not_configured() -> Self {
        Self::client(
            "Translate of notes unavailable.",
            "UNAVAILABLE",
            TRANSLATE_NOT_CONFIGURED_ID,
            400,
        )
    }
    /// Misskey `notes/translate.ts` `CANNOT_TRANSLATE_INVISIBLE_NOTE`.
    pub fn translate_invisible_note() -> Self {
        Self::client(
            "Cannot translate invisible note.",
            "CANNOT_TRANSLATE_INVISIBLE_NOTE",
            TRANSLATE_INVISIBLE_NOTE_ID,
            400,
        )
    }
    /// Batch P dormant-transport posture for the email loop: no SMTP is
    /// configured, so send paths fail loudly (never silently drop a
    /// password reset). First-party UUID.
    pub fn email_not_configured() -> Self {
        Self::client(
            "Email sending is not configured on this server.",
            "EMAIL_NOT_CONFIGURED",
            EMAIL_NOT_CONFIGURED_ID,
            400,
        )
    }
}
