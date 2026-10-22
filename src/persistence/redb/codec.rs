use crate::application::ports::PortError;
use redb::ReadableTableMetadata;

pub(crate) fn map_redb_err(err: impl std::fmt::Display) -> PortError {
    PortError::StorageUnavailable(err.to_string())
}

pub(crate) fn encode_json<T: serde::Serialize>(val: &T) -> Result<Vec<u8>, PortError> {
    serde_json::to_vec(val).map_err(|e| PortError::StorageUnavailable(format!("json encode: {e}")))
}

pub(crate) fn decode_json<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, PortError> {
    serde_json::from_slice(bytes)
        .map_err(|e| PortError::StorageUnavailable(format!("json decode: {e}")))
}

/// Trim `raw` and truncate to `max_chars`; empty-after-trim maps to `None`
/// (Sharkey service trim parity for `notes/edit`).
pub(crate) fn trim_to_none(raw: Option<&str>, max_chars: usize) -> Option<String> {
    let trimmed = raw.unwrap_or("").trim();
    if trimmed.is_empty() {
        return None;
    }
    let truncated: String = trimmed.chars().take(max_chars).collect();
    Some(truncated)
}
