//! `ApiError` type and HTTP mapping (split from `errors.rs`).
//!
#![allow(unused_imports)]
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

use axum::Json;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::application::ports::PortError;

// Stable Misskey error idents. UUIDs are taken from the Misskey 2026.10
// checkout where the endpoint defines them; the rest are fixed pinetree
// UUIDs that never change once shipped.
use super::codes::*;
use super::lookup::{code_token, is_known_client_code, lookup_known_code, new_id_for};

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
    pub(crate) fn client(message: &str, code: &str, id: &str, http_status: u16) -> Self {
        Self::new(message, code, id, "client", http_status)
    }
    pub(crate) fn permission(message: &str, code: &str, id: &str, http_status: u16) -> Self {
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
    ///
    /// A `StorageUnavailable` fault never reaches the HTTP body: the DB
    /// string can carry paths, keys, or redacted row contents, so the
    /// response is the generic `INTERNAL_ERROR` with no `info`.
    pub fn from_port_error(err: PortError) -> Self {
        match err {
            PortError::StorageUnavailable(_) => Self::internal(),
            PortError::Validation(msg) => Self::from_code_string(&msg, 400),
            PortError::Conflict(msg) => Self::from_code_string(&msg, 400),
            PortError::NotFound(msg) => {
                let mapped = Self::from_code_string(&msg, 400);
                if is_known_client_code(&mapped.code) {
                    mapped
                } else {
                    // Genuinely unknown resource: 404.
                    let code = code_token(&msg).unwrap_or_else(|| "NO_SUCH_RESOURCE".to_string());
                    Self::client(
                        &format!("No such resource: {code}."),
                        &code,
                        &new_id_for(&code),
                        404,
                    )
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
        Self::client(msg, "INVALID_PARAM", INVALID_PARAM_ID, fallback_status).with_info(
            Value::Object({
                let mut info = Map::new();
                info.insert("reason".to_string(), Value::String(msg.to_string()));
                info
            }),
        )
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
        let message = obj.and_then(|o| o.get("message")).and_then(Value::as_str);
        let code = obj.and_then(|o| o.get("code")).and_then(Value::as_str);
        let id = obj.and_then(|o| o.get("id")).and_then(Value::as_str);
        let kind = obj.and_then(|o| o.get("kind")).and_then(Value::as_str);
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
        assert_eq!(
            ApiError::permission_denied().status(),
            StatusCode::FORBIDDEN
        );
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

        let err = ApiError::from_port_error(PortError::Validation("ALREADY_FOLLOWING".to_string()));
        assert_eq!(err.code, "ALREADY_FOLLOWING");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);

        let err =
            ApiError::from_port_error(PortError::Validation("MAX_FILE_SIZE_EXCEEDED".to_string()));
        assert_eq!(err.status(), StatusCode::PAYLOAD_TOO_LARGE);

        let err = ApiError::from_port_error(PortError::NotFound("NO_SUCH_WIDGET".to_string()));
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn rate_limit_body_carries_retry_after() {
        let err = ApiError::rate_limit_exceeded(30);
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers().get("Retry-After").unwrap(), "30");
    }
}
