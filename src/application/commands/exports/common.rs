use crate::application::ports::{ExportEntity, ImportEntity, PortError};

/// Largest import file the endpoints admit (Misskey
/// `import-*.ts:82` parity: `64 * 1024` unless the account moved recently;
/// pinetree has no account moves, so the small cap always applies).
pub const IMPORT_MAX_FILE_BYTES: u64 = 64 * 1024;

/// Rows read per store page while building an export (`export*.ts`
/// `take: 100` parity). One storage transaction per import row keeps every
/// write far under the FDB ~10 MB / ~5 s caps.
pub(crate) const PAGE_SIZE: usize = 100;

pub(crate) enum ImportRowOutcome {
    Skipped,
    Abort(PortError),
}

pub(crate) fn skip_as_skipped(err: PortError) -> ImportRowOutcome {
    match err {
        PortError::StorageUnavailable(_) => ImportRowOutcome::Abort(err),
        _ => ImportRowOutcome::Skipped,
    }
}

pub(crate) fn import_entity_tag(entity: ImportEntity) -> &'static str {
    match entity {
        ImportEntity::Following => "following",
        ImportEntity::Blocking => "blocking",
        ImportEntity::Muting => "muting",
        ImportEntity::UserLists => "user-lists",
        ImportEntity::Antennas => "antennas",
        ImportEntity::Clips => "clips",
    }
}

/// Misskey export filename (`following-YYYY-MM-DD-HH-mm-ss.csv` style via
/// `dateFormat(new Date(), 'yyyy-MM-dd-HH-mm-ss')` parity, UTC).

pub(crate) fn export_file_name(entity: ExportEntity, now_ms: i64) -> String {
    let dt: chrono::DateTime<chrono::Utc> =
        chrono::DateTime::from_timestamp_millis(now_ms).unwrap_or_default();
    format!(
        "{}-{}.{}",
        entity.file_stem(),
        dt.format("%Y-%m-%d-%H-%M-%S"),
        entity.file_ext()
    )
}

/// Misskey `getFullApAccount` display form, except locals render as a bare
/// `username` (DIVERGENCE: upstream appends `@selfhost` even for locals;
/// bare names keep exports valid if the domain ever changes, and the
/// importer accepts both forms).

pub(crate) fn format_acct(account: &crate::domain::account::Account) -> String {
    match account.host.as_deref() {
        None => account.username.clone(),
        Some(host) => format!("{}@{host}", account.username),
    }
}

/// Parse one `acct` cell: `username`, `user@host`, or `@user@host`
/// (leading `@` tolerated, like Misskey `Acct.parse`). Returns
/// `(username, host)`; empty usernames are `None`.

pub(crate) fn parse_acct_cell(cell: &str) -> Option<(String, Option<String>)> {
    let cell = cell.trim().strip_prefix('@').unwrap_or(cell.trim()).trim();
    if cell.is_empty() {
        return None;
    }
    match cell.split_once('@') {
        None => Some((cell.to_string(), None)),
        Some((username, host)) => {
            let username = username.trim();
            let host = host.trim();
            if username.is_empty() || host.is_empty() {
                return None;
            }
            Some((username.to_string(), Some(host.to_string())))
        }
    }
}

/// Validate one antenna import entry (upstream `exportedAntennaSchema`
/// required keys: `name/src/keywords/excludeKeywords/users/caseSensitive/
/// withReplies/withFile`). Kept for the unit shape check below; the worker
/// applies rows through [`AppServices::apply_antenna_import`].

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_file_names_match_upstream_stems() {
        let name = export_file_name(ExportEntity::Following, 1_700_000_000_000);
        assert!(name.starts_with("following-"), "{name}");
        assert!(name.ends_with(".csv"), "{name}");
        assert!(export_file_name(ExportEntity::Muting, 1_700_000_000_000).ends_with(".csv"));
        assert!(export_file_name(ExportEntity::Notes, 1_700_000_000_000).ends_with(".json"));
        // `dateFormat` shape: `stem-YYYY-MM-DD-HH-mm-ss.ext`.
        assert_eq!(name, "following-2023-11-14-22-13-20.csv");
    }

    #[test]
    fn acct_cells_parse() {
        assert_eq!(parse_acct_cell("alice"), Some(("alice".to_string(), None)));
        assert_eq!(
            parse_acct_cell("alice@example.com"),
            Some(("alice".to_string(), Some("example.com".to_string())))
        );
        assert_eq!(
            parse_acct_cell("@alice@example.com"),
            Some(("alice".to_string(), Some("example.com".to_string())))
        );
        assert_eq!(parse_acct_cell(""), None);
        assert_eq!(parse_acct_cell("@"), None);
        assert_eq!(parse_acct_cell("alice@"), None);
        // A lone leading `@` is the acct sigil (Misskey `Acct.parse`
        // parity): the rest parses as a bare username.
        assert_eq!(
            parse_acct_cell("@example.com"),
            Some(("example.com".to_string(), None))
        );
    }
}
