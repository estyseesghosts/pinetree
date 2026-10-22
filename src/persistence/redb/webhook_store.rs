use super::codec::{decode_json, encode_json, map_redb_err};
use super::store::RedbStore;
use crate::application::ports::{PortError, SystemWebhookStore, UserWebhookStore};
use crate::domain::webhook::{SystemWebhook, UserWebhook};
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};

/// System webhooks by id (WP3; `system-webhook/*` parity). Values are JSON
/// [`SystemWebhook`] rows.
pub(crate) const SYSTEM_WEBHOOKS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("system_webhooks");
/// System-webhook trigger index `(trigger, webhook_id)` (WP3) backing
/// `active_system_webhooks_for`.
pub(crate) const SYSTEM_WEBHOOKS_BY_TRIGGER: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("system_webhooks_by_trigger");
/// User webhooks by id (WP3; `webhooks/*` parity). Values are JSON
/// [`UserWebhook`] rows.
pub(crate) const USER_WEBHOOKS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("user_webhooks");
/// User-webhook owner index `(owner_id, webhook_id)` (WP3) backing
/// `list_user_webhooks` + `count_user_webhooks`.
pub(crate) const USER_WEBHOOKS_BY_OWNER: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("user_webhooks_by_owner");
/// User-webhook trigger index `(owner_id, trigger, webhook_id)` (WP3)
/// backing `active_user_webhooks_for`.
pub(crate) const USER_WEBHOOKS_BY_OWNER_TRIGGER: TableDefinition<(&str, &str, &str), u8> =
    TableDefinition::new("user_webhooks_by_owner_trigger");
impl SystemWebhookStore for RedbStore {
    fn insert_system_webhook(&self, row: SystemWebhook) -> Result<SystemWebhook, PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut hooks = write_txn
                .open_table(SYSTEM_WEBHOOKS)
                .map_err(map_redb_err)?;
            if hooks.get(row.id.as_str()).map_err(map_redb_err)?.is_some() {
                drop(hooks);
                let _ = write_txn.abort();
                return Err(PortError::Conflict(format!(
                    "system webhook {} already exists",
                    row.id
                )));
            }
            let encoded = encode_json(&row)?;
            hooks
                .insert(row.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
            drop(hooks);
            let mut by_trigger = write_txn
                .open_table(SYSTEM_WEBHOOKS_BY_TRIGGER)
                .map_err(map_redb_err)?;
            by_trigger
                .insert((row.trigger.as_str(), row.id.as_str()), &1u8)
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)?;
        Ok(row)
    }

    fn get_system_webhook(&self, id: &str) -> Result<Option<SystemWebhook>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let hooks = read_txn.open_table(SYSTEM_WEBHOOKS).map_err(map_redb_err)?;
        match hooks.get(id).map_err(map_redb_err)? {
            Some(guard) => Ok(Some(decode_json(guard.value())?)),
            None => Ok(None),
        }
    }

    fn list_system_webhooks(&self) -> Result<Vec<SystemWebhook>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let hooks = read_txn.open_table(SYSTEM_WEBHOOKS).map_err(map_redb_err)?;
        let mut out: Vec<SystemWebhook> = Vec::new();
        for entry in hooks.iter().map_err(map_redb_err)? {
            let (_, value) = entry.map_err(map_redb_err)?;
            out.push(decode_json(value.value())?);
        }
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }

    fn put_system_webhook(&self, row: SystemWebhook) -> Result<SystemWebhook, PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut hooks = write_txn
                .open_table(SYSTEM_WEBHOOKS)
                .map_err(map_redb_err)?;
            let previous: Option<SystemWebhook> =
                match hooks.get(row.id.as_str()).map_err(map_redb_err)? {
                    Some(guard) => Some(decode_json(guard.value())?),
                    None => None,
                };
            let encoded = encode_json(&row)?;
            hooks
                .insert(row.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
            drop(hooks);
            if previous.as_ref().map(|hook| hook.trigger.as_str()) != Some(row.trigger.as_str()) {
                let mut by_trigger = write_txn
                    .open_table(SYSTEM_WEBHOOKS_BY_TRIGGER)
                    .map_err(map_redb_err)?;
                if let Some(previous) = previous.as_ref() {
                    by_trigger
                        .remove((previous.trigger.as_str(), previous.id.as_str()))
                        .map_err(map_redb_err)?;
                }
                by_trigger
                    .insert((row.trigger.as_str(), row.id.as_str()), &1u8)
                    .map_err(map_redb_err)?;
            }
        }
        self.commit_write(write_txn)?;
        Ok(row)
    }

    fn delete_system_webhook(&self, id: &str) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        let removed = {
            let mut hooks = write_txn
                .open_table(SYSTEM_WEBHOOKS)
                .map_err(map_redb_err)?;
            let previous: Option<SystemWebhook> = match hooks.get(id).map_err(map_redb_err)? {
                Some(guard) => Some(decode_json(guard.value())?),
                None => None,
            };
            let Some(previous) = previous else {
                return Ok(false);
            };
            hooks.remove(id).map_err(map_redb_err)?;
            drop(hooks);
            let mut by_trigger = write_txn
                .open_table(SYSTEM_WEBHOOKS_BY_TRIGGER)
                .map_err(map_redb_err)?;
            by_trigger
                .remove((previous.trigger.as_str(), previous.id.as_str()))
                .map_err(map_redb_err)?;
            true
        };
        self.commit_write(write_txn)?;
        Ok(removed)
    }

    fn active_system_webhooks_for(
        &self,
        trigger: &str,
        limit: usize,
    ) -> Result<Vec<SystemWebhook>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_trigger = read_txn
            .open_table(SYSTEM_WEBHOOKS_BY_TRIGGER)
            .map_err(map_redb_err)?;
        let hooks = read_txn.open_table(SYSTEM_WEBHOOKS).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in by_trigger
            .range((trigger, "")..=(trigger, "\u{10ffff}"))
            .map_err(map_redb_err)?
        {
            let (key, _) = entry.map_err(map_redb_err)?;
            let (_, hook_id) = key.value();
            if let Some(guard) = hooks.get(hook_id).map_err(map_redb_err)? {
                let hook: SystemWebhook = decode_json(guard.value())?;
                if hook.is_active {
                    out.push(hook);
                    if out.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(out)
    }
}

impl UserWebhookStore for RedbStore {
    fn insert_user_webhook(&self, row: UserWebhook) -> Result<UserWebhook, PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut hooks = write_txn.open_table(USER_WEBHOOKS).map_err(map_redb_err)?;
            if hooks.get(row.id.as_str()).map_err(map_redb_err)?.is_some() {
                drop(hooks);
                let _ = write_txn.abort();
                return Err(PortError::Conflict(format!(
                    "user webhook {} already exists",
                    row.id
                )));
            }
            let encoded = encode_json(&row)?;
            hooks
                .insert(row.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
            drop(hooks);
            let mut by_owner = write_txn
                .open_table(USER_WEBHOOKS_BY_OWNER)
                .map_err(map_redb_err)?;
            by_owner
                .insert((row.owner_id.as_str(), row.id.as_str()), &1u8)
                .map_err(map_redb_err)?;
            drop(by_owner);
            let mut by_trigger = write_txn
                .open_table(USER_WEBHOOKS_BY_OWNER_TRIGGER)
                .map_err(map_redb_err)?;
            by_trigger
                .insert(
                    (row.owner_id.as_str(), row.trigger.as_str(), row.id.as_str()),
                    &1u8,
                )
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)?;
        Ok(row)
    }

    fn get_user_webhook(&self, id: &str) -> Result<Option<UserWebhook>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let hooks = read_txn.open_table(USER_WEBHOOKS).map_err(map_redb_err)?;
        match hooks.get(id).map_err(map_redb_err)? {
            Some(guard) => Ok(Some(decode_json(guard.value())?)),
            None => Ok(None),
        }
    }

    fn list_user_webhooks(&self, owner_id: &str) -> Result<Vec<UserWebhook>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_owner = read_txn
            .open_table(USER_WEBHOOKS_BY_OWNER)
            .map_err(map_redb_err)?;
        let hooks = read_txn.open_table(USER_WEBHOOKS).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in by_owner
            .range((owner_id, "")..=(owner_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
        {
            let (key, _) = entry.map_err(map_redb_err)?;
            let (_, hook_id) = key.value();
            if let Some(guard) = hooks.get(hook_id).map_err(map_redb_err)? {
                out.push(decode_json(guard.value())?);
            }
        }
        out.sort_by(|a: &UserWebhook, b: &UserWebhook| a.id.cmp(&b.id));
        Ok(out)
    }

    fn count_user_webhooks(&self, owner_id: &str) -> Result<usize, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_owner = read_txn
            .open_table(USER_WEBHOOKS_BY_OWNER)
            .map_err(map_redb_err)?;
        let mut total = 0usize;
        for entry in by_owner
            .range((owner_id, "")..=(owner_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
        {
            entry.map_err(map_redb_err)?;
            total = total.saturating_add(1);
        }
        Ok(total)
    }

    fn put_user_webhook(&self, row: UserWebhook) -> Result<UserWebhook, PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut hooks = write_txn.open_table(USER_WEBHOOKS).map_err(map_redb_err)?;
            let previous: Option<UserWebhook> =
                match hooks.get(row.id.as_str()).map_err(map_redb_err)? {
                    Some(guard) => Some(decode_json(guard.value())?),
                    None => None,
                };
            let encoded = encode_json(&row)?;
            hooks
                .insert(row.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
            drop(hooks);
            let owner_moved =
                previous.as_ref().map(|hook| hook.owner_id.as_str()) != Some(row.owner_id.as_str());
            let trigger_moved =
                previous.as_ref().map(|hook| hook.trigger.as_str()) != Some(row.trigger.as_str());
            if owner_moved || previous.is_none() {
                let mut by_owner = write_txn
                    .open_table(USER_WEBHOOKS_BY_OWNER)
                    .map_err(map_redb_err)?;
                if let Some(previous) = previous.as_ref() {
                    by_owner
                        .remove((previous.owner_id.as_str(), previous.id.as_str()))
                        .map_err(map_redb_err)?;
                }
                by_owner
                    .insert((row.owner_id.as_str(), row.id.as_str()), &1u8)
                    .map_err(map_redb_err)?;
            }
            if owner_moved || trigger_moved || previous.is_none() {
                let mut by_trigger = write_txn
                    .open_table(USER_WEBHOOKS_BY_OWNER_TRIGGER)
                    .map_err(map_redb_err)?;
                if let Some(previous) = previous.as_ref() {
                    by_trigger
                        .remove((
                            previous.owner_id.as_str(),
                            previous.trigger.as_str(),
                            previous.id.as_str(),
                        ))
                        .map_err(map_redb_err)?;
                }
                by_trigger
                    .insert(
                        (row.owner_id.as_str(), row.trigger.as_str(), row.id.as_str()),
                        &1u8,
                    )
                    .map_err(map_redb_err)?;
            }
        }
        self.commit_write(write_txn)?;
        Ok(row)
    }

    fn delete_user_webhook(&self, id: &str) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        let removed = {
            let mut hooks = write_txn.open_table(USER_WEBHOOKS).map_err(map_redb_err)?;
            let previous: Option<UserWebhook> = match hooks.get(id).map_err(map_redb_err)? {
                Some(guard) => Some(decode_json(guard.value())?),
                None => None,
            };
            let Some(previous) = previous else {
                return Ok(false);
            };
            hooks.remove(id).map_err(map_redb_err)?;
            drop(hooks);
            let mut by_owner = write_txn
                .open_table(USER_WEBHOOKS_BY_OWNER)
                .map_err(map_redb_err)?;
            by_owner
                .remove((previous.owner_id.as_str(), previous.id.as_str()))
                .map_err(map_redb_err)?;
            drop(by_owner);
            let mut by_trigger = write_txn
                .open_table(USER_WEBHOOKS_BY_OWNER_TRIGGER)
                .map_err(map_redb_err)?;
            by_trigger
                .remove((
                    previous.owner_id.as_str(),
                    previous.trigger.as_str(),
                    previous.id.as_str(),
                ))
                .map_err(map_redb_err)?;
            true
        };
        self.commit_write(write_txn)?;
        Ok(removed)
    }

    fn active_user_webhooks_for(
        &self,
        owner_id: &str,
        trigger: &str,
        limit: usize,
    ) -> Result<Vec<UserWebhook>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_trigger = read_txn
            .open_table(USER_WEBHOOKS_BY_OWNER_TRIGGER)
            .map_err(map_redb_err)?;
        let hooks = read_txn.open_table(USER_WEBHOOKS).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in by_trigger
            .range((owner_id, trigger, "")..=(owner_id, trigger, "\u{10ffff}"))
            .map_err(map_redb_err)?
        {
            let (key, _) = entry.map_err(map_redb_err)?;
            let (_, _, hook_id) = key.value();
            if let Some(guard) = hooks.get(hook_id).map_err(map_redb_err)? {
                let hook: UserWebhook = decode_json(guard.value())?;
                if hook.is_active {
                    out.push(hook);
                    if out.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(out)
    }
}
