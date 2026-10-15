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
pub const ALREADY_FOLLOWING_ID: &str = "35387507-38c7-4cb9-9197-300b93783fa0";
pub const NOT_FOLLOWING_ID: &str = "7f6e1c2d-9f9b-4c1a-9e2a-9b9b9b9b9b01";
pub const ALREADY_REACTED_ID: &str = "71efcf98-86d6-4e2b-b2ad-9d032369366b";
pub const NOT_REACTED_ID: &str = "7f6e1c2d-9f9b-4c1a-9e2a-9b9b9b9b9b02";
pub const CANNOT_REACT_TO_RENOTE_ID: &str = "eaccdc08-ddef-43fe-908f-d108faad57f5";
pub const CANNOT_RENOTE_ID: &str = "be9529e9-fe72-4de0-ae43-0b363c4938af";
pub const YOU_HAVE_BEEN_BLOCKED_ID: &str = "b390d7e1-8a5e-46ed-b625-06271cafd3d3";
pub const MAX_FILE_SIZE_EXCEEDED_ID: &str = "7f6e1c2d-9f9b-4c1a-9e2a-9b9b9b9b9b03";
pub const INTERNAL_ERROR_ID: &str = "5d37dbcb-891e-41ca-a3d6-e690c97775ac";

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
            | "NOT_FOLLOWING"
            | "ALREADY_REACTED"
            | "NOT_REACTED"
            | "CANNOT_REACT_TO_RENOTE"
            | "CANNOT_RENOTE"
            | "CANNOT_RENOTE_TO_A_PURE_RENOTE"
            | "CANNOT_RENOTE_DUE_TO_VISIBILITY"
            | "CANNOT_REPLY_TO_AN_INVISIBLE_NOTE"
            | "CANNOT_REPLY_TO_A_PURE_RENOTE"
            | "YOU_HAVE_BEEN_BLOCKED"
            | "BLOCKING"
            | "BLOCKED"
            | "UNSUPPORTED_FIELD"
            | "INVALID_PARAM"
            | "FILE_REQUIRED"
            | "INVALID_FILE_NAME"
            | "UNALLOWED_FILE_TYPE"
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
        "YOU_HAVE_BEEN_BLOCKED" => (
            "You have been blocked by this user.",
            YOU_HAVE_BEEN_BLOCKED_ID,
            400,
            "client",
        ),
        "UNSUPPORTED_FIELD" => ("Unsupported field.", UNSUPPORTED_FIELD_ID, 400, "client"),
        "INVALID_PARAM" => ("Invalid param.", INVALID_PARAM_ID, 400, "client"),
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
