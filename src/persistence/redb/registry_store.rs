use super::codec::{decode_json, encode_json, map_redb_err};
use super::store::RedbStore;
use crate::application::ports::{PortError, RegistryItem, RegistryStore};
use redb::{ReadableDatabase, ReadableTable, TableDefinition};

/// Client registry items (Batch P; Misskey `MiRegistryItem`), keyed by one
/// encoded composite `user\0domain\0scope\0key` (`\0` cannot appear in user
/// ids; domains/scopes/keys are caller-validated). Prefix ranges serve the
/// per-scope reads; `registry_scopes` scans one user's prefix.
pub(crate) const REGISTRY: TableDefinition<&str, &[u8]> = TableDefinition::new("registry");
/// Encode one registry composite key. `\0` separates segments; `scope`
/// segments join on `\x1f` (both rejected from scope segments, keys, and
/// domains by endpoint validation, so the encoding is unambiguous).
fn registry_key(user_id: &str, domain: Option<&str>, scope: &[String], key: &str) -> String {
    format!(
        "{user_id}\0{}\0{}\0{key}",
        domain.unwrap_or(""),
        scope.join("\x1f")
    )
}

/// Prefix selecting every row of one (user, domain, scope).
fn registry_scope_prefix(user_id: &str, domain: Option<&str>, scope: &[String]) -> String {
    format!(
        "{user_id}\0{}\0{}\0",
        domain.unwrap_or(""),
        scope.join("\x1f")
    )
}

/// Prefix selecting every row of one user (for `scopes-with-domain`).
fn registry_user_prefix(user_id: &str) -> String {
    format!("{user_id}\0")
}

impl RegistryStore for RedbStore {
    fn registry_set(&self, item: RegistryItem) -> Result<(), PortError> {
        let key = registry_key(
            &item.user_id,
            item.domain.as_deref(),
            &item.scope,
            &item.key,
        );
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(REGISTRY).map_err(map_redb_err)?;
            let encoded = encode_json(&item)?;
            table
                .insert(key.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn registry_get(
        &self,
        user_id: &str,
        domain: Option<&str>,
        scope: &[String],
        key: &str,
    ) -> Result<Option<RegistryItem>, PortError> {
        let composite = registry_key(user_id, domain, scope, key);
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(REGISTRY).map_err(map_redb_err)?;
        let Some(g) = table.get(composite.as_str()).map_err(map_redb_err)? else {
            return Ok(None);
        };
        Ok(Some(decode_json(g.value())?))
    }

    fn registry_list(
        &self,
        user_id: &str,
        domain: Option<&str>,
        scope: &[String],
    ) -> Result<Vec<RegistryItem>, PortError> {
        let prefix = registry_scope_prefix(user_id, domain, scope);
        let end = format!("{prefix}\u{10ffff}");
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(REGISTRY).map_err(map_redb_err)?;
        let mut items = Vec::new();
        for entry in table
            .range(prefix.as_str()..end.as_str())
            .map_err(map_redb_err)?
        {
            let (_, v) = entry.map_err(map_redb_err)?;
            items.push(decode_json(v.value())?);
        }
        Ok(items)
    }

    fn registry_remove(
        &self,
        user_id: &str,
        domain: Option<&str>,
        scope: &[String],
        key: &str,
    ) -> Result<(), PortError> {
        let composite = registry_key(user_id, domain, scope, key);
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(REGISTRY).map_err(map_redb_err)?;
            table.remove(composite.as_str()).map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn registry_scopes(
        &self,
        user_id: &str,
    ) -> Result<Vec<(Option<String>, Vec<String>)>, PortError> {
        let prefix = registry_user_prefix(user_id);
        let end = format!("{prefix}\u{10ffff}");
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(REGISTRY).map_err(map_redb_err)?;
        let mut pairs: Vec<(Option<String>, Vec<String>)> = Vec::new();
        for entry in table
            .range(prefix.as_str()..end.as_str())
            .map_err(map_redb_err)?
        {
            let (_, v) = entry.map_err(map_redb_err)?;
            let item: RegistryItem = decode_json(v.value())?;
            let pair = (item.domain.clone(), item.scope.clone());
            if !pairs.contains(&pair) {
                pairs.push(pair);
            }
        }
        Ok(pairs)
    }

    fn consume_totp_login(
        &self,
        account_id: &str,
        challenge: &str,
        matched_step: u64,
        now_ms: i64,
    ) -> Result<(), PortError> {
        use serde::Deserialize;
        #[derive(Deserialize)]
        struct StoredChallenge {
            challenge: String,
            account_id: String,
            expires_at_ms: i64,
        }
        let scope = vec!["2fa".to_string()];
        let challenge_key = registry_key(account_id, None, &scope, "totp_challenge");
        let last_step_key = registry_key(account_id, None, &scope, "totp_last_step");
        let write_txn = self.begin_write()?;
        let outcome: Result<(), PortError> = {
            let mut table = write_txn.open_table(REGISTRY).map_err(map_redb_err)?;
            let item: Option<RegistryItem> =
                match table.get(challenge_key.as_str()).map_err(map_redb_err)? {
                    Some(g) => Some(decode_json(g.value())?),
                    None => None,
                };
            let Some(item) = item else {
                return Err(PortError::Validation(
                    "INVALID_TOKEN no second-factor challenge is active".to_string(),
                ));
            };
            let stored: StoredChallenge = serde_json::from_value(item.value).map_err(|e| {
                PortError::Validation(format!("INVALID_PARAM corrupt 2fa challenge: {e}"))
            })?;
            if stored.account_id != account_id || stored.challenge != challenge {
                return Err(PortError::Validation(
                    "INVALID_TOKEN invalid challenge".to_string(),
                ));
            }
            if now_ms > stored.expires_at_ms {
                table.remove(challenge_key.as_str()).map_err(map_redb_err)?;
                drop(table);
                self.commit_write(write_txn)?;
                return Err(PortError::Validation(
                    "INVALID_TOKEN challenge expired".to_string(),
                ));
            }
            let last_step: Option<u64> =
                match table.get(last_step_key.as_str()).map_err(map_redb_err)? {
                    Some(g) => {
                        let last_item: RegistryItem = decode_json(g.value())?;
                        let step = last_item
                            .value
                            .get("step")
                            .and_then(serde_json::Value::as_u64)
                            .ok_or_else(|| {
                                PortError::Validation(
                                    "INVALID_PARAM corrupt 2fa replay guard".to_string(),
                                )
                            })?;
                        Some(step)
                    }
                    None => None,
                };
            if let Some(last) = last_step
                && matched_step <= last
            {
                return Err(PortError::Validation(
                    "INVALID_TOKEN token already used".to_string(),
                ));
            }
            let last_item = RegistryItem {
                id: self.ids.generate(),
                user_id: account_id.to_string(),
                domain: None,
                scope: scope.clone(),
                key: "totp_last_step".to_string(),
                value: serde_json::json!({ "step": matched_step }),
                updated_at_ms: now_ms,
            };
            let encoded = encode_json(&last_item)?;
            table
                .insert(last_step_key.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
            table.remove(challenge_key.as_str()).map_err(map_redb_err)?;
            Ok(())
        };
        outcome?;
        self.commit_write(write_txn)
    }
}

// --- Drive folders (WP2 part A) --------------------------------------------
