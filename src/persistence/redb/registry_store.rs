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
}

// --- Drive folders (WP2 part A) --------------------------------------------
