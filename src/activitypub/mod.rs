pub mod actor;
pub mod objects;
pub mod webfinger;
pub mod signatures;

/// Misskey server version we claim compatibility with.
///
/// Lives here (rather than in either HTTP adapter) so both the Misskey
/// packers and the ActivityPub NodeInfo renderer share one definition.
pub const PACKER_VERSION: &str = "2026.10.1-alpha.0";

/// Millisecond timestamps as ISO-8601 UTC strings
/// (`"YYYY-MM-DDTHH:MM:SS.sssZ"`).
pub fn format_millis(ms: i64) -> String {
    let dt: chrono::DateTime<chrono::Utc> =
        chrono::DateTime::from_timestamp_millis(ms).unwrap_or_default();
    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}
