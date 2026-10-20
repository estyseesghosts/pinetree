pub mod actor;
pub mod objects;
pub mod quotes;
pub mod webfinger;
pub mod signatures;
/// Federated clips + pages wire surface (`OrderedCollection` clips,
/// `Article` pages, fetch-gated serving, remote resolution).
pub mod federated_collections;

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

/// Parse an ISO-8601 UTC string back to epoch millis (`None` when the
/// string does not parse — remote timestamps are advisory, never trusted
/// for ordering decisions).
pub fn parse_millis(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_millis())
}
