use super::codec::{decode_json, encode_json, map_redb_err};
use super::store::RedbStore;
use crate::application::ports::{InstanceSettingsPatch, InstanceSettingsStore, PortError};
use crate::domain::instance::InstanceSettings;
use crate::domain::moderation::ModerationLogEntry;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};

/// Instance settings singleton row (Batch M §3.3; `InstanceSettingsStore`),
/// keyed by `"settings"`. Absent row = [`InstanceSettings::default()`].
pub(crate) const INSTANCE_SETTINGS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("instance_settings");
impl InstanceSettingsStore for RedbStore {
    fn get(&self) -> Result<InstanceSettings, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn
            .open_table(INSTANCE_SETTINGS)
            .map_err(map_redb_err)?;
        match table.get("settings").map_err(map_redb_err)? {
            Some(g) => decode_json(g.value()),
            None => Ok(InstanceSettings::default()),
        }
    }

    fn update(
        &self,
        patch: InstanceSettingsPatch,
        log: ModerationLogEntry,
    ) -> Result<InstanceSettings, PortError> {
        let write_txn = self.begin_write()?;
        let updated = {
            let current = {
                let table = write_txn
                    .open_table(INSTANCE_SETTINGS)
                    .map_err(map_redb_err)?;
                match table.get("settings").map_err(map_redb_err)? {
                    Some(g) => decode_json(g.value())?,
                    None => InstanceSettings::default(),
                }
            };
            let mut settings = current;
            if let Some(registration) = patch.registration {
                settings.registration = registration;
            }
            if let Some(blocked_hosts) = patch.blocked_hosts {
                settings.blocked_hosts = InstanceSettings::normalize_blocked_hosts(&blocked_hosts);
            }
            if let Some(name) = patch.name {
                settings.name = name;
            }
            if let Some(description) = patch.description {
                settings.description = description;
            }
            if let Some(max_note_text_length) = patch.max_note_text_length {
                settings.max_note_text_length = max_note_text_length;
            }
            if let Some(federate_clips) = patch.federate_clips {
                settings.federate_clips = federate_clips;
            }
            if let Some(federate_pages) = patch.federate_pages {
                settings.federate_pages = federate_pages;
            }
            if let Some(ugc) = patch.ugc_visibility_for_visitor {
                settings.ugc_visibility_for_visitor = ugc;
            }
            settings.validate()?;
            let mut table = write_txn
                .open_table(INSTANCE_SETTINGS)
                .map_err(map_redb_err)?;
            let bytes = encode_json(&settings)?;
            table
                .insert("settings", bytes.as_slice())
                .map_err(map_redb_err)?;
            settings
        };
        self.append_log_in_txn(&write_txn, &log)?;
        self.commit_write(write_txn)?;
        Ok(updated)
    }
}
