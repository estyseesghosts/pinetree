//! Misskey API request validation (Slice D1).
//!
//! Rejects unknown body fields with `UNSUPPORTED_FIELD` per
//! `docs/endpoint-manifest.md` §1. The credential parameter `"i"` is
//! always accepted.

use serde_json::{Map, Value};

use super::errors::ApiError;

pub use super::response::RateLimiter;

/// Check that every key in `obj` is listed in `allowed` (or is the
/// credential parameter `"i"`). Returns
/// `Err(ApiError::unsupported_field(key))` for the first unknown key.
pub fn check_allowed_fields(obj: &Map<String, Value>, allowed: &[&str]) -> Result<(), ApiError> {
    for key in obj.keys() {
        if key != "i" && !allowed.contains(&key.as_str()) {
            return Err(ApiError::unsupported_field(key));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn allows_listed_fields_and_i() {
        let value = json!({"i": "tok", "text": "hi", "visibility": "public"});
        let obj = value.as_object().unwrap();
        assert!(check_allowed_fields(obj, &["text", "visibility"]).is_ok());
    }

    #[test]
    fn rejects_unknown_field_naming_it() {
        let value = json!({"text": "hi", "channelId": "abc"});
        let obj = value.as_object().unwrap();
        let err = check_allowed_fields(obj, &["text"]).unwrap_err();
        assert_eq!(err.code, "UNSUPPORTED_FIELD");
        assert_eq!(err.info.unwrap()["param"], "channelId");
    }
}
