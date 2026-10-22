use crate::application::commands::AppServices;

use super::common::{ImportRowOutcome, parse_acct_cell, skip_as_skipped};
use crate::application::ports::{ImportEntity, PortError};

impl AppServices {
    /// Apply one following/blocking/muting CSV import. Returns
    /// `(applied, skipped)`. Every row goes through the same application
    /// op as the interactive endpoint (`follow`/`block`/`mute`), so §0A
    /// block gates and `ALREADY_*` dedup apply unchanged; a replay of the
    /// same file converges on conflicts instead of duplicating rows.
    pub(crate) fn apply_relation_import(
        &self,
        user_id: &str,
        entity: ImportEntity,
        text: &str,
        with_replies_default: Option<bool>,
    ) -> Result<(usize, usize), PortError> {
        let mut applied = 0usize;
        let mut skipped = 0usize;
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let Some((target_id, with_replies)) =
                self.resolve_import_row(entity, line, with_replies_default.unwrap_or(true))?
            else {
                skipped += 1;
                continue;
            };
            if target_id == user_id {
                skipped += 1;
                continue;
            }
            let outcome = match entity {
                ImportEntity::Following => self
                    .follow(user_id, &target_id, with_replies)
                    .map(|_| ())
                    .map_err(skip_as_skipped),
                ImportEntity::Blocking => self
                    .block(user_id, &target_id)
                    .map(|_| ())
                    .map_err(skip_as_skipped),
                ImportEntity::Muting => self
                    .mute(user_id, &target_id, None)
                    .map(|_| ())
                    .map_err(skip_as_skipped),
                ImportEntity::UserLists | ImportEntity::Antennas | ImportEntity::Clips => {
                    unreachable!("apply_relation_import only serves Following/Blocking/Muting")
                }
            };
            match outcome {
                Ok(()) => applied += 1,
                Err(ImportRowOutcome::Skipped) => skipped += 1,
                Err(ImportRowOutcome::Abort(err)) => return Err(err),
            }
        }
        Ok((applied, skipped))
    }

    /// Resolve one import CSV line to `(account_id, with_replies)`.
    /// `Ok(None)` = skip the row (unparsable or unresolvable — never a
    /// batch failure, like upstream's per-line `try/catch`).
    fn resolve_import_row(
        &self,
        entity: ImportEntity,
        line: &str,
        with_replies_default: bool,
    ) -> Result<Option<(String, bool)>, PortError> {
        let parts: Vec<&str> = line.split(',').collect();
        let first = parts.first().map(|s| s.trim()).unwrap_or("");
        // Following rows carry trailing `key=value` columns
        // (`withReplies=…`, upstream `parts.slice(2)` parity; `parts[1]` is
        // ignored by upstream too).
        let mut with_replies = with_replies_default;
        for kv in parts.iter().skip(1) {
            if let Some(value) = kv.trim().strip_prefix("withReplies=") {
                with_replies = value.trim() == "true";
            }
        }
        // Primary: `parts[0]` as an acct (`username` or `user@host`).
        // Legacy 3-column fallback (`id,username,host`, e.g. Mastodon-shaped
        // exports — a pinetree superset: upstream would try `parts[0]` as an
        // acct and skip the row when it does not resolve): when the row has
        // at least 3 columns, `parts[2]` carries no `=`, and `parts[0]` did
        // not resolve, retry as `username = parts[1], host = parts[2]`.
        let mut acct = parse_acct_cell(first);
        if entity == ImportEntity::Following
            && parts.len() >= 3
            && !parts[2].contains('=')
            && !parts[1].trim().is_empty()
            && !parts[2].trim().is_empty()
        {
            let legacy = (
                parts[1].trim().to_string(),
                Some(parts[2].trim().to_string()),
            );
            if self.resolve_acct(&acct)?.is_none() {
                acct = Some(legacy);
            }
        }
        let Some((username, host)) = acct else {
            return Ok(None);
        };
        // Local accounts export as a bare `username` (see module docs);
        // upstream-style `user@selfhost` resolves locally too.
        let target = self.resolve_acct(&Some((username, host)))?;
        Ok(target.map(|id| (id, with_replies)))
    }

    /// Resolve a parsed `(username, host)` pair to an account id via the
    /// local store only (no remote fetch — unresolvable rows skip).
    pub(crate) fn resolve_acct(
        &self,
        acct: &Option<(String, Option<String>)>,
    ) -> Result<Option<String>, PortError> {
        let Some((username, host)) = acct else {
            return Ok(None);
        };
        if username.is_empty() {
            return Ok(None);
        }
        let account = match host.as_deref() {
            None => self.accounts.find_by_username_and_host(username, None)?,
            Some(host) if host.eq_ignore_ascii_case(&self.self_host()) => {
                self.accounts.find_by_username_and_host(username, None)?
            }
            Some(host) => self
                .accounts
                .find_by_username_and_host(username, Some(&host.to_ascii_lowercase()))?,
        };
        Ok(account.map(|a| a.id))
    }

    /// Host part of `public_url` (`https://example.local` → `example.local`;
    /// lowercase): rows addressed at ourselves resolve locally.
    fn self_host(&self) -> String {
        let url = self.public_url.trim();
        let after_scheme = url.split("://").nth(1).unwrap_or(url);
        after_scheme
            .split('/')
            .next()
            .unwrap_or(after_scheme)
            .to_ascii_lowercase()
    }
}
