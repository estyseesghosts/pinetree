//! Misskey API error envelope (Slice D1).
//!
//! Matches Misskey 2026.10 (`ApiCallService.ts:306-325`, `error.ts:6-32`)
//! and `docs/endpoint-manifest.md` §1: `{ error: { message, code, id, kind,
//! info? } }` over HTTP with the status mapped from the error.
//!
//! The serialized body carries **both** top-level `id`/`code`/`message`
//! and the nested `error` object so Misskey SDK clients reading
//! `body.error.{id,code,message,kind}` and callers reading top-level
//! `res["code"]` both work.

use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::application::ports::PortError;

// Stable Misskey error idents. UUIDs are taken from the Misskey 2026.10
// checkout where the endpoint defines them; the rest are fixed pinetree
// UUIDs that never change once shipped.
pub const UNKNOWN_API_ENDPOINT_ID: &str = "2ca3b769-b6d7-4b65-96c9-302f3620a591";
pub const AUTHENTICATION_FAILED_ID: &str = "b0a7f5f8-dc2f-4171-b91f-de88ad238e14";
pub const CREDENTIAL_REQUIRED_ID: &str = "1384574d-a912-4b81-8601-c7b1c4085df1";
pub const PERMISSION_DENIED_ID: &str = "1370e5b7-d4eb-4566-bb1d-7748ee6a1838";
pub const ACCESS_DENIED_ID: &str = "56f35758-7dd5-468b-8439-5d6fb8ec9b8e";
pub const USER_SUSPENDED_ID: &str = "a8c724b3-6e9c-4b46-b1a8-bc3ed6258370";
pub const RATE_LIMIT_EXCEEDED_ID: &str = "d5826d14-3982-4d2e-8011-b9e9f02499ef";
pub const INVALID_PARAM_ID: &str = "3d81ceae-475f-4600-b2a8-2bc116157532";
pub const UNSUPPORTED_FIELD_ID: &str = "9307f30c-4400-421c-bf2f-1e6a37d0f978";
pub const NO_SUCH_NOTE_ID: &str = "24fcbfc6-2e37-42b6-8388-c29b3861a08d";
pub const NO_SUCH_USER_ID: &str = "fcd2eef9-a9b2-4c4f-8624-038099e90aa5";
pub const NO_SUCH_REPLY_TARGET_ID: &str = "749ee0f6-d3da-459a-bf02-282e2da4292c";
pub const NO_SUCH_RENOTE_TARGET_ID: &str = "b5c90186-4ab0-49c8-9bba-a1f76c282ba4";
pub const NO_SUCH_FILE_ID: &str = "b6992544-63e7-67f0-fa7f-32444b1b5306";
pub const CONTAINS_PROHIBITED_WORDS_ID: &str = "aa6e01d3-a85c-669d-758a-76aab43af334";
pub const FOLLOWEE_IS_YOURSELF_ID: &str = "26fbe7bb-a331-4857-af17-205b426669a9";
pub const ALREADY_FOLLOWING_ID: &str = "35387507-38c7-4cb9-9197-300b93783fa0";pub const NO_FOLLOW_REQUEST_ID: &str = "bcde4f8b-0913-4614-8736-2e2d5fa29d5b";
pub const NOT_FOLLOWING_ID: &str = "7f6e1c2d-9f9b-4c1a-9e2a-9b9b9b9b9b01";
pub const BLOCKEE_IS_YOURSELF_ID: &str = "88b19138-f28d-42c0-8499-6a31bbd0fdc6";
pub const ALREADY_BLOCKING_ID: &str = "787fed64-acb9-464a-82eb-afbd745b9614";
pub const NOT_BLOCKING_ID: &str = "291b2efa-60c6-45c0-9f6a-045c8f9b02cd";
pub const MUTEE_IS_YOURSELF_ID: &str = "a4619cb2-5f23-484b-9301-94c903074e10";
pub const ALREADY_MUTING_ID: &str = "7e7359cb-160c-4956-b08f-4d1c653cd007";
pub const NOT_MUTING_ID: &str = "5467d020-daa9-4553-81e1-135c0c35a96d4";
pub const NO_SUCH_AVATAR_ID: &str = "539f3a45-f215-4f81-a9a8-31293640207f";
pub const NO_SUCH_BANNER_ID: &str = "0d8f5629-f210-41c2-9433-735831a58595";
pub const AVATAR_NOT_AN_IMAGE_ID: &str = "f419f9f8-2f4d-46b1-9fb4-49d3a2fd7191";
pub const BANNER_NOT_AN_IMAGE_ID: &str = "75aedb19-2afd-4e6d-87fc-67941256fa60";
pub const NO_SUCH_PAGE_ID: &str = "8e01b590-7eb9-431b-a239-860e086c408e";
pub const INVALID_REGEXP_ID: &str = "0d786918-10df-41cd-8f33-8dec7d9a89a5";
pub const TOO_MANY_MUTED_WORDS_ID: &str = "010665b1-a211-42d2-bc64-8f6609d79785";
pub const URI_NULL_ID: &str = "bf326f31-d430-4f97-9933-5d61e4d48a23";
pub const FORBIDDEN_TO_SET_YOURSELF_ID: &str = "25c90186-4ab0-49c8-9bba-a1fa6c202ba4";
pub const RESTRICTED_BY_ROLE_ID: &str = "8feff0ba-5ab5-585b-31f4-4df816663fad";
pub const YOUR_NAME_CONTAINS_PROHIBITED_WORDS_ID: &str = "0b3f9f6a-2f4d-4b1f-9fb4-49d3a2fd7191";
pub const YOUR_ACCOUNT_MOVED_ID: &str = "56f20ec9-fd06-4fa5-841b-edd6d7d4fa31";
pub const BLOCKING_ID: &str = "4e2206ec-aa4f-4960-b865-6c23ac38e2d9";
pub const BLOCKED_ID: &str = "c4ab57cc-4e41-45e9-bfd9-584f61e35ce0";
pub const ALREADY_REACTED_ID: &str = "71efcf98-86d6-4e2b-b2ad-9d032369366b";
pub const NOT_REACTED_ID: &str = "7f6e1c2d-9f9b-4c1a-9e2a-9b9b9b9b9b02";
pub const CANNOT_REACT_TO_RENOTE_ID: &str = "eaccdc08-ddef-43fe-908f-d108faad57f5";
pub const CANNOT_RENOTE_ID: &str = "be9529e9-fe72-4de0-ae43-0b363c4938af";
pub const BOTH_WITH_REPLIES_AND_WITH_FILES_ID: &str = "91c8cb9f-36ed-46e7-9ca2-7df96ed6e222";
pub const YOU_HAVE_BEEN_BLOCKED_ID: &str = "b390d7e1-8a5e-46ed-b625-06271cafd3d3";
pub const MAX_FILE_SIZE_EXCEEDED_ID: &str = "7f6e1c2d-9f9b-4c1a-9e2a-9b9b9b9b9b03";
pub const INTERNAL_ERROR_ID: &str = "5d37dbcb-891e-41ca-a3d6-e690c97775ac";
pub const FORBIDDEN_ID: &str = "3c6a84db-d619-26af-ca14-06232a21df8a";
pub const REACTIONS_NOT_PUBLIC_ID: &str = "673a7dd2-6924-1093-e0c0-e68456ceae5c";
pub const IS_REMOTE_USER_ID: &str = "6b95fa98-8cf9-2350-e284-f0ffdb54a805";
/// Misskey `notes/polls/vote.ts` idents (verbatim upstream UUIDs).
pub const NO_POLL_ID: &str = "5f979967-52d9-4314-a911-1c673727f92f";
pub const INVALID_CHOICE_ID: &str = "e0cc9a04-f2e8-41e4-a5f1-4127293260cc";
pub const ALREADY_VOTED_ID: &str = "0963fc77-efac-419b-9424-b391608dc6d8";
pub const ALREADY_EXPIRED_ID: &str = "1022a357-b085-4054-9083-8f8de358337e";
/// Misskey `notes/create.ts` ident (verbatim upstream UUID).
pub const CANNOT_CREATE_ALREADY_EXPIRED_POLL_ID: &str = "04da457d-b083-4055-9082-955525eda5a5";
/// Misskey `notes/drafts/*` idents (verbatim upstream UUIDs; the
/// create-endpoint UUID is canonical — the update endpoint reuses the same
/// `code` with its own UUID, see the endpoint manifest §§16–17).
pub const TOO_MANY_DRAFTS_ID: &str = "9ee33bbe-fde3-4c71-9b51-e50492c6b9c8";
pub const TOO_MANY_SCHEDULED_NOTES_ID: &str = "22ae69eb-09e3-4541-a850-773cfa45e693";
pub const SCHEDULED_AT_REQUIRED_ID: &str = "15e28a55-e74c-4d65-89b7-8880cdaaa87d";
pub const SCHEDULED_AT_MUST_BE_IN_FUTURE_ID: &str = "e4bed6c9-017e-4934-aed0-01c22cc60ec1";
pub const NO_SUCH_NOTE_DRAFT_ID: &str = "49cd6b9d-848e-41ee-b0b9-adaca711a6b1";
/// Misskey `i/pin.ts` idents (verbatim upstream UUIDs).
pub const PIN_LIMIT_EXCEEDED_ID: &str = "72dab508-c64d-498f-8740-a8eec1ba385a";
pub const ALREADY_PINNED_ID: &str = "8b18c2b7-68fe-4edb-9892-c0cbaeb6c913";
/// Sharkey `notes/edit.ts` idents (verbatim upstream UUIDs). Codes already
/// known to pinetree (`NO_SUCH_NOTE`, `NO_SUCH_REPLY_TARGET`, …) keep the
/// existing pinetree UUID (one UUID per code, §16 convention); only new
/// codes take their Sharkey UUID here.
pub const NEEDS_EDIT_ID: &str = "d697edc8-8c73-4de8-bded-35fd198b79e5";
pub const YOU_ARE_NOT_THE_AUTHOR_ID: &str = "c6e61685-411d-43d0-b90a-a448d2539001";
pub const NOT_LOCAL_USER_ID: &str = "b907f407-2aa0-4283-800b-a2c56290b822";
pub const CANNOT_QUOTE_THE_CURRENT_NOTE_ID: &str = "33510210-8452-094c-6227-4a6c05d99f02";
pub const CANNOT_QUOTE_A_QUOTE_OF_EDITED_NOTE_ID: &str = "33510210-8452-094c-6227-4a6c05d99f01";
pub const QUOTE_DISABLED_FOR_USER_ID: &str = "1c0ea108-d1e3-4e8e-aa3f-4d2487626153";
pub const CANNOT_PRIVATE_RENOTE_ID: &str = "19a50f1c-84fa-4e33-81d3-17834ccc0ad8";
pub const CANNOT_RENOTE_OUTSIDE_OF_CHANNEL_ID: &str = "33510210-8452-094c-6227-4a6c05d99f00";
pub const CANNOT_REPLY_TO_SPECIFIED_VISIBILITY_NOTE_WITH_EXTENDED_VISIBILITY_ID: &str =
    "ed940410-535c-4d5e-bfa3-af798671e93c";
pub const MAX_LENGTH_ID: &str = "3ac74a84-8fd5-4bb0-870f-01804f82ce16";
pub const MAX_CW_LENGTH_ID: &str = "7004c478-bda3-4b4f-acb2-4316398c9d52";

/// Misskey API error: stable `code`/`id` ident, human `message`,
/// machine `kind` (`client` | `permission` | `server`), optional `info`,
/// and the HTTP status to answer with.
#[derive(Debug, Clone)]
pub struct ApiError {
    pub message: String,
    pub code: String,
    pub id: String,
    pub kind: String,
    pub info: Option<Value>,
    pub http_status: u16,
    pub retry_after_secs: Option<u64>,
}

impl ApiError {
    fn new(
        message: impl Into<String>,
        code: impl Into<String>,
        id: impl Into<String>,
        kind: impl Into<String>,
        http_status: u16,
    ) -> Self {
        Self {
            message: message.into(),
            code: code.into(),
            id: id.into(),
            kind: kind.into(),
            info: None,
            http_status,
            retry_after_secs: None,
        }
    }

    fn client(message: &str, code: &str, id: &str, http_status: u16) -> Self {
        Self::new(message, code, id, "client", http_status)
    }

    fn permission(message: &str, code: &str, id: &str, http_status: u16) -> Self {
        Self::new(message, code, id, "permission", http_status)
    }

    pub fn with_info(mut self, info: Value) -> Self {
        self.info = Some(info);
        self
    }

    /// 404 `UNKNOWN_API_ENDPOINT`.
    pub fn unknown_endpoint() -> Self {
        Self::client(
            "Unknown API endpoint.",
            "UNKNOWN_API_ENDPOINT",
            UNKNOWN_API_ENDPOINT_ID,
            404,
        )
    }

    /// 400 `UNSUPPORTED_FIELD` naming `field` in `info.param`.
    pub fn unsupported_field(field: &str) -> Self {
        Self::client(
            &format!("Unsupported field: {field}."),
            "UNSUPPORTED_FIELD",
            UNSUPPORTED_FIELD_ID,
            400,
        )
        .with_info(Value::Object({
            let mut info = Map::new();
            info.insert("param".to_string(), Value::String(field.to_string()));
            info
        }))
    }

    /// 400 `INVALID_PARAM` naming `field` in `info.param` with `info.reason`.
    pub fn invalid_param(field: &str, reason: &str) -> Self {
        Self::client("Invalid param.", "INVALID_PARAM", INVALID_PARAM_ID, 400).with_info(
            Value::Object({
                let mut info = Map::new();
                info.insert("param".to_string(), Value::String(field.to_string()));
                info.insert("reason".to_string(), Value::String(reason.to_string()));
                info
            }),
        )
    }

    /// 401 `AUTHENTICATION_FAILED`.
    pub fn authentication_failed() -> Self {
        Self::client(
            "Authentication failed. Please ensure your token is correct.",
            "AUTHENTICATION_FAILED",
            AUTHENTICATION_FAILED_ID,
            401,
        )
    }

    /// 401 `CREDENTIAL_REQUIRED`.
    pub fn credential_required() -> Self {
        Self::client(
            "Credential required.",
            "CREDENTIAL_REQUIRED",
            CREDENTIAL_REQUIRED_ID,
            401,
        )
    }

    /// 403 `PERMISSION_DENIED`.
    pub fn permission_denied() -> Self {
        Self::permission(
            "Your app does not have the necessary permissions to use this endpoint.",
            "PERMISSION_DENIED",
            PERMISSION_DENIED_ID,
            403,
        )
    }

    /// 403 `ACCESS_DENIED`.
    pub fn access_denied() -> Self {
        Self::permission("Access denied.", "ACCESS_DENIED", ACCESS_DENIED_ID, 403)
    }

    /// 403 `USER_SUSPENDED`.
    pub fn user_suspended() -> Self {
        Self::permission(
            "Your account has been suspended.",
            "USER_SUSPENDED",
            USER_SUSPENDED_ID,
            403,
        )
    }

    /// 429 `RATE_LIMIT_EXCEEDED` with a `Retry-After` hint.
    pub fn rate_limit_exceeded(retry_after_secs: u64) -> Self {
        let mut err = Self::client(
            "Rate limit exceeded. Please try again later.",
            "RATE_LIMIT_EXCEEDED",
            RATE_LIMIT_EXCEEDED_ID,
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

    /// 413 `MAX_FILE_SIZE_EXCEEDED`.
    pub fn max_file_size_exceeded() -> Self {
        Self::client(
            "Max file size exceeded.",
            "MAX_FILE_SIZE_EXCEEDED",
            MAX_FILE_SIZE_EXCEEDED_ID,
            413,
        )
    }

    /// 500 `INTERNAL_ERROR`.
    pub fn internal() -> Self {
        Self::new(
            "Internal error occurred. Please contact us if the error persists.",
            "INTERNAL_ERROR",
            INTERNAL_ERROR_ID,
            "server",
            500,
        )
    }

    /// Map a domain/port error carrying a Misskey error ident
    /// (`NO_SUCH_NOTE`, `ALREADY_FOLLOWING`, …) to its exact Misskey
    /// error code, message, id, and HTTP status.
    pub fn from_port_error(err: PortError) -> Self {
        match err {
            PortError::StorageUnavailable(msg) => {
                Self::internal().with_info(Value::String(msg))
            }
            PortError::Validation(msg) => Self::from_code_string(&msg, 400),
            PortError::Conflict(msg) => Self::from_code_string(&msg, 400),
            PortError::NotFound(msg) => {
                let mapped = Self::from_code_string(&msg, 400);
                if is_known_client_code(&mapped.code) {
                    mapped
                } else {
                    // Genuinely unknown resource: 404.
                    let code = code_token(&msg).unwrap_or_else(|| "NO_SUCH_RESOURCE".to_string());
                    Self::client(&format!("No such resource: {code}."), &code, &new_id_for(&code), 404)
                }
            }
        }
    }

    /// Build an error from a free-form domain message whose leading token
    /// is the Misskey error code. Falls back to a generic client error
    /// with `fallback_status`.
    fn from_code_string(msg: &str, fallback_status: u16) -> Self {
        if let Some(code) = code_token(msg) {
            if let Some((message, id, status, kind)) = lookup_known_code(&code) {
                let mut err = Self::new(message, &code, id, kind, status);
                // Keep any trailing detail after the code token as info.
                let rest = msg[code.len()..].trim();
                if !rest.is_empty() {
                    err.info = Some(Value::String(rest.to_string()));
                }
                return err;
            }
            // Unknown code token: preserve it verbatim.
            let id = new_id_for(&code);
            return Self::new(msg, code, &id, "client", fallback_status);
        }
        Self::client(msg, "INVALID_PARAM", INVALID_PARAM_ID, fallback_status)
            .with_info(Value::Object({
                let mut info = Map::new();
                info.insert("reason".to_string(), Value::String(msg.to_string()));
                info
            }))
    }

    pub fn status(&self) -> StatusCode {
        StatusCode::from_u16(self.http_status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
    }

    /// Full response body: top-level `id`/`code`/`message` plus the nested
    /// Misskey `error` object.
    pub fn body(&self) -> Value {
        serde_json::to_value(self).unwrap_or_else(|_| {
            serde_json::json!({
                "id": self.id,
                "code": self.code,
                "message": self.message,
                "error": {
                    "message": self.message,
                    "code": self.code,
                    "id": self.id,
                    "kind": self.kind,
                },
            })
        })
    }

    fn from_value(v: &Value) -> Self {
        let obj = v
            .get("error")
            .and_then(Value::as_object)
            .or_else(|| v.as_object());
        let message = obj
            .and_then(|o| o.get("message"))
            .and_then(Value::as_str);
        let code = obj
            .and_then(|o| o.get("code"))
            .and_then(Value::as_str);
        let id = obj.and_then(|o| o.get("id")).and_then(Value::as_str);
        let kind = obj
            .and_then(|o| o.get("kind"))
            .and_then(Value::as_str);
        let info = obj.and_then(|o| o.get("info")).cloned();
        let code = code.unwrap_or("INTERNAL_ERROR").to_string();
        let (_, _, status, _) = lookup_known_code(&code)
            .map(|(m, i, s, k)| (m, i, s, k))
            .unwrap_or((
                "Internal error occurred.",
                INTERNAL_ERROR_ID,
                500u16,
                "server",
            ));
        Self {
            message: message.unwrap_or("Internal error occurred.").to_string(),
            code,
            id: id.unwrap_or(INTERNAL_ERROR_ID).to_string(),
            kind: kind.unwrap_or("server").to_string(),
            info,
            http_status: status,
            retry_after_secs: None,
        }
    }
}

/// Known Misskey codes that answer `400 Bad Request` with `kind: "client"`.
fn is_known_client_code(code: &str) -> bool {
    matches!(
        code,
        "NO_SUCH_NOTE"
            | "NO_SUCH_USER"
            | "NO_SUCH_REPLY_TARGET"
            | "NO_SUCH_RENOTE_TARGET"
            | "NO_SUCH_FILE"
            | "NO_SUCH_CHANNEL"
            | "CONTAINS_PROHIBITED_WORDS"
            | "CONTAINS_TOO_MANY_MENTIONS"
            | "FOLLOWEE_IS_YOURSELF"
            | "ALREADY_FOLLOWING"
            | "BLOCKEE_IS_YOURSELF"
            | "ALREADY_BLOCKING"
            | "NOT_BLOCKING"
            | "MUTEE_IS_YOURSELF"
            | "ALREADY_MUTING"
            | "NOT_MUTING"
            | "NO_FOLLOW_REQUEST"
            | "NOT_FOLLOWING"
            | "ALREADY_REACTED"
            | "NOT_REACTED"
            | "CANNOT_REACT_TO_RENOTE"
            | "CANNOT_RENOTE"
            | "CANNOT_RENOTE_TO_A_PURE_RENOTE"
            | "CANNOT_RENOTE_DUE_TO_VISIBILITY"
            | "BOTH_WITH_REPLIES_AND_WITH_FILES"
            | "CANNOT_REPLY_TO_AN_INVISIBLE_NOTE"
            | "CANNOT_REPLY_TO_A_PURE_RENOTE"
            | "YOU_HAVE_BEEN_BLOCKED"
            | "BLOCKING"
            | "BLOCKED"
            | "NO_SUCH_AVATAR"
            | "NO_SUCH_BANNER"
            | "AVATAR_NOT_AN_IMAGE"
            | "BANNER_NOT_AN_IMAGE"
            | "NO_SUCH_PAGE"
            | "INVALID_REGEXP"
            | "TOO_MANY_MUTED_WORDS"
            | "URI_NULL"
            | "FORBIDDEN_TO_SET_YOURSELF"
            | "RESTRICTED_BY_ROLE"
            | "YOUR_NAME_CONTAINS_PROHIBITED_WORDS"
            | "YOUR_ACCOUNT_MOVED"
            | "YOUR_ACCOUNT_MOVED"
            | "UNSUPPORTED_FIELD"
            | "INVALID_PARAM"
            | "FORBIDDEN"
            | "REACTIONS_NOT_PUBLIC"
            | "IS_REMOTE_USER"
            | "NO_POLL"
            | "INVALID_CHOICE"
            | "ALREADY_VOTED"
            | "ALREADY_EXPIRED"
            | "CANNOT_CREATE_ALREADY_EXPIRED_POLL"
            | "TOO_MANY_DRAFTS"
            | "TOO_MANY_SCHEDULED_NOTES"
            | "SCHEDULED_AT_REQUIRED"
            | "SCHEDULED_AT_MUST_BE_IN_FUTURE"
            | "NO_SUCH_NOTE_DRAFT"
            | "PIN_LIMIT_EXCEEDED"
            | "ALREADY_PINNED"
            | "FILE_REQUIRED"
            | "INVALID_FILE_NAME"
            | "UNALLOWED_FILE_TYPE"
            // `notes/edit` (Sharkey `edit.ts` verbatim) + §3 quote-disable.
            | "NEEDS_EDIT_ID"
            | "YOU_ARE_NOT_THE_AUTHOR"
            | "NOT_LOCAL_USER"
            | "CANNOT_QUOTE_THE_CURRENT_NOTE"
            | "CANNOT_QUOTE_A_QUOTE_OF_EDITED_NOTE"
            | "QUOTE_DISABLED_FOR_USER"
            | "CANNOT_PRIVATE_RENOTE"
            | "CANNOT_RENOTE_OUTSIDE_OF_CHANNEL"
            | "CANNOT_REPLY_TO_SPECIFIED_VISIBILITY_NOTE_WITH_EXTENDED_VISIBILITY"
            | "MAX_LENGTH"
            | "MAX_CW_LENGTH"
    )
}

/// (message, id, http_status, kind) for every mapped Misskey error code.
fn lookup_known_code(code: &str) -> Option<(&'static str, &'static str, u16, &'static str)> {
    Some(match code {
        "UNKNOWN_API_ENDPOINT" => ("Unknown API endpoint.", UNKNOWN_API_ENDPOINT_ID, 404, "client"),
        "AUTHENTICATION_FAILED" => (
            "Authentication failed. Please ensure your token is correct.",
            AUTHENTICATION_FAILED_ID,
            401,
            "client",
        ),
        "CREDENTIAL_REQUIRED" => ("Credential required.", CREDENTIAL_REQUIRED_ID, 401, "client"),
        "invalid_token" => (
            "Authentication failed. Please ensure your token is correct.",
            AUTHENTICATION_FAILED_ID,
            401,
            "client",
        ),
        "PERMISSION_DENIED" => (
            "Your app does not have the necessary permissions to use this endpoint.",
            PERMISSION_DENIED_ID,
            403,
            "permission",
        ),
        "ACCESS_DENIED" => ("Access denied.", ACCESS_DENIED_ID, 403, "permission"),
        "USER_SUSPENDED" | "YOUR_ACCOUNT_SUSPENDED" => (
            "Your account has been suspended.",
            USER_SUSPENDED_ID,
            403,
            "permission",
        ),
        "RATE_LIMIT_EXCEEDED" => (
            "Rate limit exceeded. Please try again later.",
            RATE_LIMIT_EXCEEDED_ID,
            429,
            "client",
        ),
        "MAX_FILE_SIZE_EXCEEDED" => (
            "Max file size exceeded.",
            MAX_FILE_SIZE_EXCEEDED_ID,
            413,
            "client",
        ),
        "NO_SUCH_NOTE" => ("No such note.", NO_SUCH_NOTE_ID, 400, "client"),
        "NO_SUCH_USER" => ("No such user.", NO_SUCH_USER_ID, 400, "client"),
        "NO_SUCH_AVATAR" => ("No such avatar file.", NO_SUCH_AVATAR_ID, 400, "client"),
        "NO_SUCH_BANNER" => ("No such banner file.", NO_SUCH_BANNER_ID, 400, "client"),
        "AVATAR_NOT_AN_IMAGE" => (
            "The file specified as an avatar is not an image.",
            AVATAR_NOT_AN_IMAGE_ID,
            400,
            "client",
        ),
        "BANNER_NOT_AN_IMAGE" => (
            "The file specified as a banner is not an image.",
            BANNER_NOT_AN_IMAGE_ID,
            400,
            "client",
        ),
        "NO_SUCH_PAGE" => ("No such page.", NO_SUCH_PAGE_ID, 400, "client"),
        "INVALID_REGEXP" => (
            "Invalid Regular Expression.",
            INVALID_REGEXP_ID,
            400,
            "client",
        ),
        "TOO_MANY_MUTED_WORDS" => (
            "Too many muted words.",
            TOO_MANY_MUTED_WORDS_ID,
            400,
            "client",
        ),
        "URI_NULL" => (
            "User ActivityPup URI is null.",
            URI_NULL_ID,
            400,
            "client",
        ),
        "FORBIDDEN_TO_SET_YOURSELF" => (
            "You can't set yourself as your own alias.",
            FORBIDDEN_TO_SET_YOURSELF_ID,
            400,
            "client",
        ),
        "RESTRICTED_BY_ROLE" => (
            "This feature is restricted by your role.",
            RESTRICTED_BY_ROLE_ID,
            400,
            "client",
        ),
        "YOUR_NAME_CONTAINS_PROHIBITED_WORDS" => (
            "Your new name contains prohibited words.",
            YOUR_NAME_CONTAINS_PROHIBITED_WORDS_ID,
            422,
            "client",
        ),
        "YOUR_ACCOUNT_MOVED" => (
            "You have moved your account.",
            YOUR_ACCOUNT_MOVED_ID,
            403,
            "permission",
        ),
        "NO_FOLLOW_REQUEST" => ("No follow request.", NO_FOLLOW_REQUEST_ID, 400, "client"),
        "NO_SUCH_REPLY_TARGET" => (
            "No such reply target.",
            NO_SUCH_REPLY_TARGET_ID,
            400,
            "client",
        ),
        "NO_SUCH_RENOTE_TARGET" => (
            "No such renote target.",
            NO_SUCH_RENOTE_TARGET_ID,
            400,
            "client",
        ),
        "NO_SUCH_FILE" => ("Some files are not found.", NO_SUCH_FILE_ID, 400, "client"),
        "CONTAINS_PROHIBITED_WORDS" => (
            "Cannot post because it contains prohibited words.",
            CONTAINS_PROHIBITED_WORDS_ID,
            400,
            "client",
        ),
        "FOLLOWEE_IS_YOURSELF" => (
            "Followee is yourself.",
            FOLLOWEE_IS_YOURSELF_ID,
            400,
            "client",
        ),
        "ALREADY_FOLLOWING" => (
            "You are already following that user.",
            ALREADY_FOLLOWING_ID,
            400,
            "client",
        ),
        "NOT_FOLLOWING" => ("You are not following that user.", NOT_FOLLOWING_ID, 400, "client"),
        "BLOCKEE_IS_YOURSELF" => (
            "Blockee is yourself.",
            BLOCKEE_IS_YOURSELF_ID,
            400,
            "client",
        ),
        "ALREADY_BLOCKING" => (
            "You are already blocking that user.",
            ALREADY_BLOCKING_ID,
            400,
            "client",
        ),
        "NOT_BLOCKING" => (
            "You are not blocking that user.",
            NOT_BLOCKING_ID,
            400,
            "client",
        ),
        "MUTEE_IS_YOURSELF" => ("Mutee is yourself.", MUTEE_IS_YOURSELF_ID, 400, "client"),
        "ALREADY_MUTING" => (
            "You are already muting that user.",
            ALREADY_MUTING_ID,
            400,
            "client",
        ),
        "NOT_MUTING" => ("You are not muting that user.", NOT_MUTING_ID, 400, "client"),
        "BLOCKING" => (
            "You are blocking that user.",
            BLOCKING_ID,
            400,
            "client",
        ),
        "BLOCKED" => (
            "You are blocked by that user.",
            BLOCKED_ID,
            400,
            "client",
        ),
        "ALREADY_REACTED" => (
            "You are already reacting to that note.",
            ALREADY_REACTED_ID,
            400,
            "client",
        ),
        "NOT_REACTED" => ("You are not reacting to that note.", NOT_REACTED_ID, 400, "client"),
        "CANNOT_REACT_TO_RENOTE" => (
            "You cannot react to Renote.",
            CANNOT_REACT_TO_RENOTE_ID,
            400,
            "client",
        ),
        "CANNOT_RENOTE" | "CANNOT_RENOTE_TO_A_PURE_RENOTE" | "CANNOT_RENOTE_DUE_TO_VISIBILITY" => {
            (
                "You can not Renote due to target visibility.",
                CANNOT_RENOTE_ID,
                400,
                "client",
            )
        }
        "BOTH_WITH_REPLIES_AND_WITH_FILES" => (
            "Specifying both withReplies and withFiles is not supported.",
            BOTH_WITH_REPLIES_AND_WITH_FILES_ID,
            400,
            "client",
        ),
        "YOU_HAVE_BEEN_BLOCKED" => (
            "You have been blocked by this user.",
            YOU_HAVE_BEEN_BLOCKED_ID,
            400,
            "client",
        ),
        "UNSUPPORTED_FIELD" => ("Unsupported field.", UNSUPPORTED_FIELD_ID, 400, "client"),
        "INVALID_PARAM" => ("Invalid param.", INVALID_PARAM_ID, 400, "client"),
        // `users/followers` + `users/following` visibility gate (Misskey
        // `FORBIDDEN`, kind `client` → HTTP 400 per `ApiCallService`
        // mapping; the two upstream endpoints carry different UUIDs for
        // the same code — pinetree emits the `users/followers` one, see
        // `FORBIDDEN_ID`).
        "FORBIDDEN" => ("Forbidden.", FORBIDDEN_ID, 400, "client"),
        // `users/reactions` gates (Misskey `reactions.ts` verbatim).
        "REACTIONS_NOT_PUBLIC" => (
            "Reactions of the user is not public.",
            REACTIONS_NOT_PUBLIC_ID,
            400,
            "client",
        ),
        "IS_REMOTE_USER" => (
            "Currently unavailable to display reactions of remote users. See https://github.com/misskey-dev/misskey/issues/12964",
            IS_REMOTE_USER_ID,
            400,
            "client",
        ),
        // `notes/polls/vote` gates (Misskey `polls/vote.ts` verbatim).
        "NO_POLL" => (
            "The note does not attach a poll.",
            NO_POLL_ID,
            400,
            "client",
        ),
        "INVALID_CHOICE" => ("Choice ID is invalid.", INVALID_CHOICE_ID, 400, "client"),
        "ALREADY_VOTED" => (
            "You have already voted.",
            ALREADY_VOTED_ID,
            400,
            "client",
        ),
        "ALREADY_EXPIRED" => (
            "The poll is already expired.",
            ALREADY_EXPIRED_ID,
            400,
            "client",
        ),
        // `notes/create` poll gate (Misskey `create.ts` verbatim).
        "CANNOT_CREATE_ALREADY_EXPIRED_POLL" => (
            "Poll is already expired.",
            CANNOT_CREATE_ALREADY_EXPIRED_POLL_ID,
            400,
            "client",
        ),
        // `notes/drafts/*` gates (Misskey `drafts/create|update.ts`
        // verbatim code/message; the create-endpoint UUID is canonical).
        "TOO_MANY_DRAFTS" => (
            "You cannot create drafts any more.",
            TOO_MANY_DRAFTS_ID,
            400,
            "client",
        ),
        "TOO_MANY_SCHEDULED_NOTES" => (
            "You cannot create scheduled notes any more.",
            TOO_MANY_SCHEDULED_NOTES_ID,
            400,
            "client",
        ),
        "SCHEDULED_AT_REQUIRED" => (
            "scheduledAt is required when isActuallyScheduled is true.",
            SCHEDULED_AT_REQUIRED_ID,
            400,
            "client",
        ),
        "SCHEDULED_AT_MUST_BE_IN_FUTURE" => (
            "scheduledAt must be in the future.",
            SCHEDULED_AT_MUST_BE_IN_FUTURE_ID,
            400,
            "client",
        ),
        "NO_SUCH_NOTE_DRAFT" => (
            "No such note draft.",
            NO_SUCH_NOTE_DRAFT_ID,
            400,
            "client",
        ),
        // `i/pin.ts` gates (verbatim code/message/id).
        "PIN_LIMIT_EXCEEDED" => (
            "You can not pin notes any more.",
            PIN_LIMIT_EXCEEDED_ID,
            400,
            "client",
        ),
        "ALREADY_PINNED" => (
            "That note has already been pinned.",
            ALREADY_PINNED_ID,
            400,
            "client",
        ),
        // `notes/edit` gates (Sharkey `edit.ts` verbatim code/message/id).
        "NEEDS_EDIT_ID" => (
            "You need to specify `editId`.",
            NEEDS_EDIT_ID,
            400,
            "client",
        ),
        "YOU_ARE_NOT_THE_AUTHOR" => (
            "You are not the author of this note.",
            YOU_ARE_NOT_THE_AUTHOR_ID,
            400,
            "client",
        ),
        "NOT_LOCAL_USER" => (
            "You are not a local user.",
            NOT_LOCAL_USER_ID,
            400,
            "client",
        ),
        "CANNOT_QUOTE_THE_CURRENT_NOTE" => (
            "Cannot quote the current note.",
            CANNOT_QUOTE_THE_CURRENT_NOTE_ID,
            400,
            "client",
        ),
        "CANNOT_QUOTE_A_QUOTE_OF_EDITED_NOTE" => (
            "Cannot quote a quote of edited note.",
            CANNOT_QUOTE_A_QUOTE_OF_EDITED_NOTE_ID,
            400,
            "client",
        ),
        // §3 quote-disable (Sharkey `notes/create.ts` + `notes/edit.ts`
        // verbatim — same code/message/id on both endpoints).
        "QUOTE_DISABLED_FOR_USER" => (
            "You do not have permission to create quote posts.",
            QUOTE_DISABLED_FOR_USER_ID,
            400,
            "client",
        ),
        "CANNOT_PRIVATE_RENOTE" => (
            "You can not perform a private renote.",
            CANNOT_PRIVATE_RENOTE_ID,
            400,
            "client",
        ),
        "CANNOT_RENOTE_OUTSIDE_OF_CHANNEL" => (
            "Cannot renote outside of channel.",
            CANNOT_RENOTE_OUTSIDE_OF_CHANNEL_ID,
            400,
            "client",
        ),
        "CANNOT_REPLY_TO_SPECIFIED_VISIBILITY_NOTE_WITH_EXTENDED_VISIBILITY" => (
            "You cannot reply to a specified visibility note with extended visibility.",
            CANNOT_REPLY_TO_SPECIFIED_VISIBILITY_NOTE_WITH_EXTENDED_VISIBILITY_ID,
            400,
            "client",
        ),
        "MAX_LENGTH" => (
            "You tried posting a note which is too long.",
            MAX_LENGTH_ID,
            400,
            "client",
        ),
        "MAX_CW_LENGTH" => (
            "You tried posting a content warning which is too long.",
            MAX_CW_LENGTH_ID,
            400,
            "client",
        ),
        "INTERNAL_ERROR" => (
            "Internal error occurred. Please contact us if the error persists.",
            INTERNAL_ERROR_ID,
            500,
            "server",
        ),
        _ => return None,
    })
}

/// Leading `LOUD_SNAKE` token of a domain message, if any.
fn code_token(msg: &str) -> Option<String> {
    let token: String = msg
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    if token.is_empty() {
        return None;
    }
    // Accept `SCREAMING_SNAKE` idents and lowercase auth shorthands.
    if token == "invalid_token"
        || (token
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'))
    {
        Some(token)
    } else {
        None
    }
}

/// Deterministic placeholder id for codes without a Misskey-issued UUID.
fn new_id_for(code: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"pinetree-api-error:");
    hasher.update(code.as_bytes());
    let digest = hasher.finalize();
    let bytes: [u8; 16] = digest[..16].try_into().unwrap_or([0u8; 16]);
    // Render as UUID-shaped text (version nibble fixed to 4).
    let mut b = bytes;
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11],
        b[12], b[13], b[14], b[15]
    )
}

impl Serialize for ApiError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut nested = Map::new();
        nested.insert("message".to_string(), Value::String(self.message.clone()));
        nested.insert("code".to_string(), Value::String(self.code.clone()));
        nested.insert("id".to_string(), Value::String(self.id.clone()));
        nested.insert("kind".to_string(), Value::String(self.kind.clone()));
        if let Some(info) = &self.info {
            nested.insert("info".to_string(), info.clone());
        }
        let mut map = serializer.serialize_map(Some(4))?;
        map.serialize_entry("id", &self.id)?;
        map.serialize_entry("code", &self.code)?;
        map.serialize_entry("message", &self.message)?;
        map.serialize_entry("error", &Value::Object(nested))?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for ApiError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let v = Value::deserialize(deserializer)?;
        Ok(Self::from_value(&v))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status();
        let mut response = (status, Json(self.body())).into_response();
        if status == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(
                "WWW-Authenticate",
                HeaderValue::from_static("Bearer realm=\"Misskey\""),
            );
        }
        if status == StatusCode::TOO_MANY_REQUESTS {
            if let Some(secs) = self.retry_after_secs {
                if let Ok(value) = HeaderValue::from_str(&secs.to_string()) {
                    response.headers_mut().insert("Retry-After", value);
                }
            }
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_has_top_level_and_nested_fields() {
        let err = ApiError::unsupported_field("channelId");
        let body = err.body();
        assert_eq!(body["code"], "UNSUPPORTED_FIELD");
        assert_eq!(body["error"]["code"], "UNSUPPORTED_FIELD");
        assert_eq!(body["error"]["kind"], "client");
        assert_eq!(body["error"]["info"]["param"], "channelId");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn status_mapping() {
        assert_eq!(ApiError::unknown_endpoint().status(), StatusCode::NOT_FOUND);
        assert_eq!(
            ApiError::authentication_failed().status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(ApiError::permission_denied().status(), StatusCode::FORBIDDEN);
        assert_eq!(ApiError::access_denied().status(), StatusCode::FORBIDDEN);
        assert_eq!(ApiError::user_suspended().status(), StatusCode::FORBIDDEN);
        assert_eq!(
            ApiError::rate_limit_exceeded(42).status(),
            StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(
            ApiError::max_file_size_exceeded().status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }

    #[test]
    fn port_error_mapping_keeps_misskey_codes() {
        let err = ApiError::from_port_error(PortError::NotFound("NO_SUCH_NOTE".to_string()));
        assert_eq!(err.code, "NO_SUCH_NOTE");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert_eq!(err.kind, "client");

        let err = ApiError::from_port_error(PortError::Validation(
            "ALREADY_FOLLOWING".to_string(),
        ));
        assert_eq!(err.code, "ALREADY_FOLLOWING");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);

        let err = ApiError::from_port_error(PortError::Validation(
            "MAX_FILE_SIZE_EXCEEDED".to_string(),
        ));
        assert_eq!(err.status(), StatusCode::PAYLOAD_TOO_LARGE);

        let err = ApiError::from_port_error(PortError::NotFound("NO_SUCH_WIDGET".to_string()));
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn rate_limit_body_carries_retry_after() {
        let err = ApiError::rate_limit_exceeded(30);
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response.headers().get("Retry-After").unwrap(),
            "30"
        );
    }
}
