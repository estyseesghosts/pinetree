use super::codec::{decode_json, encode_json, map_redb_err};
use super::note_store::matches_cursor;
use super::store::RedbStore;
use crate::application::ports::{EmojiStore, NewJob, PortError, TimelineCursor};
use crate::domain::emoji::{CustomEmoji, RemoteEmoji};
use crate::domain::moderation::ModerationLogEntry;
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};

/// Local custom emoji by id (Batch N WS1; Misskey `MiEmoji`, host null).
/// Values are JSON [`CustomEmoji`] rows.
pub(crate) const EMOJIS: TableDefinition<&str, &[u8]> = TableDefinition::new("emojis");
/// Unique shortcode index (lowercased shortcode → emoji id), case-insensitive
/// parity with the account-handle rule (conformance J2).
pub(crate) const EMOJIS_BY_NAME: TableDefinition<&str, &str> =
    TableDefinition::new("emojis_by_name");
/// Remote-emoji metadata keyed `(lower host, lower shortcode)` (WP8b opt-in;
/// metadata only, no fetch at ingest). Values are JSON [`RemoteEmoji`] rows.
pub(crate) const REMOTE_EMOJIS: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("remote_emojis");
impl EmojiStore for RedbStore {
    fn create_emoji(
        &self,
        emoji: CustomEmoji,
        log: ModerationLogEntry,
        jobs: Vec<NewJob>,
    ) -> Result<CustomEmoji, PortError> {
        emoji
            .validate()
            .map_err(|e| PortError::Validation(format!("{e:?}")))?;
        let lower = emoji.shortcode.to_ascii_lowercase();
        let write_txn = self.begin_write()?;
        {
            let mut by_name = write_txn.open_table(EMOJIS_BY_NAME).map_err(map_redb_err)?;
            if by_name.get(lower.as_str()).map_err(map_redb_err)?.is_some() {
                drop(by_name);
                let _ = write_txn.abort();
                return Err(PortError::Conflict("DUPLICATE_NAME".to_string()));
            }
            by_name
                .insert(lower.as_str(), emoji.id.as_str())
                .map_err(map_redb_err)?;
            drop(by_name);
            let mut table = write_txn.open_table(EMOJIS).map_err(map_redb_err)?;
            let encoded = encode_json(&emoji)?;
            table
                .insert(emoji.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.append_log_in_txn(&write_txn, &log)?;
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(emoji)
    }

    fn update_emoji(
        &self,
        emoji: CustomEmoji,
        log: ModerationLogEntry,
        jobs: Vec<NewJob>,
    ) -> Result<CustomEmoji, PortError> {
        emoji
            .validate()
            .map_err(|e| PortError::Validation(format!("{e:?}")))?;
        let lower = emoji.shortcode.to_ascii_lowercase();
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(EMOJIS).map_err(map_redb_err)?;
            let existing: Option<CustomEmoji> =
                match table.get(emoji.id.as_str()).map_err(map_redb_err)? {
                    Some(guard) => Some(decode_json(guard.value())?),
                    None => None,
                };
            let Some(existing) = existing else {
                drop(table);
                let _ = write_txn.abort();
                return Err(PortError::NotFound("NO_SUCH_EMOJI".to_string()));
            };
            let mut by_name = write_txn.open_table(EMOJIS_BY_NAME).map_err(map_redb_err)?;
            if existing.shortcode.to_ascii_lowercase() != lower {
                if by_name.get(lower.as_str()).map_err(map_redb_err)?.is_some() {
                    drop(by_name);
                    drop(table);
                    let _ = write_txn.abort();
                    return Err(PortError::Conflict("SAME_NAME_EMOJI_EXISTS".to_string()));
                }
                let _ = by_name.remove(existing.shortcode.to_ascii_lowercase().as_str());
                by_name
                    .insert(lower.as_str(), emoji.id.as_str())
                    .map_err(map_redb_err)?;
            }
            drop(by_name);
            let encoded = encode_json(&emoji)?;
            table
                .insert(emoji.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.append_log_in_txn(&write_txn, &log)?;
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(emoji)
    }

    fn delete_emoji(
        &self,
        id: &str,
        log: ModerationLogEntry,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        let removed = {
            let mut table = write_txn.open_table(EMOJIS).map_err(map_redb_err)?;
            let existing: Option<CustomEmoji> = match table.get(id).map_err(map_redb_err)? {
                Some(guard) => Some(decode_json(guard.value())?),
                None => None,
            };
            let Some(existing) = existing else {
                return Ok(false);
            };
            let _ = table.remove(id);
            drop(table);
            let mut by_name = write_txn.open_table(EMOJIS_BY_NAME).map_err(map_redb_err)?;
            let _ = by_name.remove(existing.shortcode.to_ascii_lowercase().as_str());
            true
        };
        self.append_log_in_txn(&write_txn, &log)?;
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(removed)
    }

    fn get_emoji(&self, id: &str) -> Result<Option<CustomEmoji>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(EMOJIS).map_err(map_redb_err)?;
        match table.get(id).map_err(map_redb_err)? {
            Some(guard) => Ok(Some(decode_json(guard.value())?)),
            None => Ok(None),
        }
    }

    fn get_emoji_by_name(&self, shortcode: &str) -> Result<Option<CustomEmoji>, PortError> {
        let lower = shortcode.to_ascii_lowercase();
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_name = read_txn.open_table(EMOJIS_BY_NAME).map_err(map_redb_err)?;
        let Some(guard) = by_name.get(lower.as_str()).map_err(map_redb_err)? else {
            return Ok(None);
        };
        let id: String = guard.value().to_string();
        drop(guard);
        drop(by_name);
        drop(read_txn);
        self.get_emoji(&id)
    }

    fn list_emojis(
        &self,
        query: Option<&str>,
        cursor: &TimelineCursor,
    ) -> Result<Vec<CustomEmoji>, PortError> {
        let limit = if cursor.limit == 0 {
            10
        } else {
            cursor.limit.min(100)
        };
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(EMOJIS).map_err(map_redb_err)?;
        let mut out: Vec<CustomEmoji> = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (_, v) = entry.map_err(map_redb_err)?;
            let emoji: CustomEmoji = decode_json(v.value())?;
            if let Some(q) = query {
                let q = q.trim();
                if !q.is_empty() {
                    // Exact `:name:` tokens match by name; anything else is a
                    // case-insensitive substring over name/aliases/category.
                    if q.starts_with(':') && q.ends_with(':') && q.len() >= 2 {
                        let want = q.trim_matches(':').to_ascii_lowercase();
                        if emoji.shortcode.to_ascii_lowercase() != want {
                            continue;
                        }
                    } else {
                        let needle = q.to_ascii_lowercase();
                        let hay_name = emoji.shortcode.to_ascii_lowercase();
                        let hay_cat = emoji.category.as_deref().unwrap_or("").to_ascii_lowercase();
                        let in_aliases = emoji
                            .aliases
                            .iter()
                            .any(|a| a.to_ascii_lowercase().contains(&needle));
                        if !hay_name.contains(&needle) && !hay_cat.contains(&needle) && !in_aliases
                        {
                            continue;
                        }
                    }
                }
            }
            out.push(emoji);
        }
        // Newest-first by emoji id (aidx ids are time-ordered).
        out.sort_by(|a, b| b.id.cmp(&a.id));
        out.retain(|e| matches_cursor(&e.id, cursor));
        out.truncate(limit);
        Ok(out)
    }

    fn all_local_emojis(&self) -> Result<Vec<CustomEmoji>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(EMOJIS).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (_, v) = entry.map_err(map_redb_err)?;
            out.push(decode_json(v.value())?);
        }
        Ok(out)
    }

    fn upsert_remote_emoji(&self, emoji: RemoteEmoji) -> Result<RemoteEmoji, PortError> {
        let write_txn = self.begin_write()?;
        let merged = {
            let mut table = write_txn.open_table(REMOTE_EMOJIS).map_err(map_redb_err)?;
            let existing: Option<RemoteEmoji> = match table
                .get((emoji.host.as_str(), emoji.shortcode.as_str()))
                .map_err(map_redb_err)?
            {
                Some(guard) => Some(decode_json(guard.value())?),
                None => None,
            };
            let merged = match existing {
                // Never touch `media_id`: the lazy-fetch job owns it.
                Some(mut prev) => {
                    prev.remote_url = emoji.remote_url.clone();
                    prev
                }
                None => emoji,
            };
            let encoded = encode_json(&merged)?;
            table
                .insert(
                    (merged.host.as_str(), merged.shortcode.as_str()),
                    encoded.as_slice(),
                )
                .map_err(map_redb_err)?;
            merged
        };
        self.commit_write(write_txn)?;
        Ok(merged)
    }

    fn get_remote_emoji(
        &self,
        host: &str,
        shortcode: &str,
    ) -> Result<Option<RemoteEmoji>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(REMOTE_EMOJIS).map_err(map_redb_err)?;
        match table
            .get((
                host.to_ascii_lowercase().as_str(),
                shortcode.to_ascii_lowercase().as_str(),
            ))
            .map_err(map_redb_err)?
        {
            Some(guard) => Ok(Some(decode_json(guard.value())?)),
            None => Ok(None),
        }
    }

    fn all_remote_emojis(&self) -> Result<Vec<RemoteEmoji>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(REMOTE_EMOJIS).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (_, v) = entry.map_err(map_redb_err)?;
            out.push(decode_json(v.value())?);
        }
        Ok(out)
    }

    fn list_remote_emojis(
        &self,
        query: Option<&str>,
        host: Option<&str>,
        cursor: &TimelineCursor,
    ) -> Result<Vec<RemoteEmoji>, PortError> {
        let limit = if cursor.limit == 0 {
            10
        } else {
            cursor.limit.min(100)
        };
        let wanted_host = host
            .map(|h| h.trim().to_ascii_lowercase())
            .filter(|h| !h.is_empty());
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(REMOTE_EMOJIS).map_err(map_redb_err)?;
        let mut out: Vec<RemoteEmoji> = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (_, v) = entry.map_err(map_redb_err)?;
            let emoji: RemoteEmoji = decode_json(v.value())?;
            if let Some(want) = wanted_host.as_deref() {
                if emoji.host.to_ascii_lowercase() != want {
                    continue;
                }
            }
            if let Some(since) = cursor.since_date {
                if emoji.first_seen_ms < since {
                    continue;
                }
            }
            if let Some(until) = cursor.until_date {
                if emoji.first_seen_ms > until {
                    continue;
                }
            }
            if let Some(q) = query {
                let q = q.trim();
                if !q.is_empty() {
                    // Same token rule as `list_emojis`; remote rows carry no
                    // aliases/category, so plain queries match the shortcode.
                    if q.starts_with(':') && q.ends_with(':') && q.len() >= 2 {
                        let want = q.trim_matches(':').to_ascii_lowercase();
                        if emoji.shortcode.to_ascii_lowercase() != want {
                            continue;
                        }
                    } else {
                        let needle = q.to_ascii_lowercase();
                        if !emoji.shortcode.to_ascii_lowercase().contains(&needle) {
                            continue;
                        }
                    }
                }
            }
            out.push(emoji);
        }
        // Newest-first by first-seen time (remote rows have no aidx ids).
        out.sort_by(|a, b| {
            b.first_seen_ms
                .cmp(&a.first_seen_ms)
                .then_with(|| a.host.cmp(&b.host))
                .then_with(|| a.shortcode.cmp(&b.shortcode))
        });
        out.truncate(limit);
        Ok(out)
    }

    fn set_remote_emoji_media(
        &self,
        host: &str,
        shortcode: &str,
        media_id: &str,
    ) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(REMOTE_EMOJIS).map_err(map_redb_err)?;
            let key = (host.to_ascii_lowercase(), shortcode.to_ascii_lowercase());
            let existing: Option<RemoteEmoji> = match table
                .get((key.0.as_str(), key.1.as_str()))
                .map_err(map_redb_err)?
            {
                Some(guard) => Some(decode_json(guard.value())?),
                None => None,
            };
            // A deleted row converges quietly (no retry can recreate it).
            if let Some(mut row) = existing {
                row.media_id = Some(media_id.to_string());
                let encoded = encode_json(&row)?;
                table
                    .insert(
                        (row.host.as_str(), row.shortcode.as_str()),
                        encoded.as_slice(),
                    )
                    .map_err(map_redb_err)?;
            }
        }
        self.commit_write(write_txn)?;
        Ok(())
    }
}
