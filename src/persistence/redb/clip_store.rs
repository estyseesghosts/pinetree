use super::codec::{decode_json, encode_json, map_redb_err};
use super::store::RedbStore;
use crate::application::ports::{ClipStore, NewJob, PortError};
use redb::{ReadableDatabase, ReadableTable, TableDefinition};

/// Clips by id (federated clips + pages handoff, Phase 1). Values are JSON
/// [`Clip`](crate::domain::clip::Clip) rows.
pub(crate) const CLIPS: TableDefinition<&str, &[u8]> = TableDefinition::new("clips");
/// Owner index `(owner_id, clip_id)` (listing + per-owner cap).
pub(crate) const CLIPS_BY_OWNER: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("clips_by_owner");
/// Clip memberships `(clip_id, note_id)` → JSON
/// [`ClipNote`](crate::domain::clip::ClipNote) row (aidx row ids are
/// time-ordered, so id-desc sorts read newest-first).
pub(crate) const CLIP_NOTES: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("clip_notes");
/// Clip favorites `(clip_id, user_id)` → JSON
/// [`ClipFavorite`](crate::domain::clip::ClipFavorite) row.
pub(crate) const CLIP_FAVORITES: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("clip_favorites");
/// Clip follows `(clip_id, user_id)` → JSON
/// [`ClipFollow`](crate::domain::clip::ClipFollow) row.
pub(crate) const CLIP_FOLLOWS: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("clip_follows");
/// Clip tombstones `clip_uri` → deleted_at_ms (u64 le bytes).
pub(crate) const CLIP_TOMBSTONES: TableDefinition<&str, u64> =
    TableDefinition::new("clip_tombstones");
impl ClipStore for RedbStore {
    fn create_clip(
        &self,
        clip: crate::domain::clip::Clip,
    ) -> Result<crate::domain::clip::Clip, PortError> {
        use crate::domain::clip::CLIP_LIMIT_PER_OWNER;
        let write_txn = self.begin_write()?;
        {
            let by_owner = write_txn.open_table(CLIPS_BY_OWNER).map_err(map_redb_err)?;
            let owned = by_owner
                .range((clip.owner_id.as_str(), "")..=(clip.owner_id.as_str(), "\u{10ffff}"))
                .map_err(map_redb_err)?
                .count();
            if owned >= CLIP_LIMIT_PER_OWNER {
                return Err(PortError::Conflict("TOO_MANY_CLIPS".to_string()));
            }
            drop(by_owner);
            let mut clips = write_txn.open_table(CLIPS).map_err(map_redb_err)?;
            let mut by_owner = write_txn.open_table(CLIPS_BY_OWNER).map_err(map_redb_err)?;
            let encoded = encode_json(&clip)?;
            clips
                .insert(clip.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
            by_owner
                .insert((clip.owner_id.as_str(), clip.id.as_str()), &1u8)
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)?;
        Ok(clip)
    }

    fn update_clip(
        &self,
        clip: crate::domain::clip::Clip,
        jobs: Vec<NewJob>,
    ) -> Result<Option<crate::domain::clip::Clip>, PortError> {
        let write_txn = self.begin_write()?;
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        let updated = {
            let mut clips = write_txn.open_table(CLIPS).map_err(map_redb_err)?;
            let existing: Option<crate::domain::clip::Clip> =
                match clips.get(clip.id.as_str()).map_err(map_redb_err)? {
                    Some(g) => Some(decode_json(g.value())?),
                    None => None,
                };
            match existing {
                Some(prev) if prev.owner_id == clip.owner_id => {
                    let encoded = encode_json(&clip)?;
                    clips
                        .insert(clip.id.as_str(), encoded.as_slice())
                        .map_err(map_redb_err)?;
                    Some(clip)
                }
                _ => None,
            }
        };
        self.commit_write(write_txn)?;
        Ok(updated)
    }

    fn delete_clip(
        &self,
        owner_id: &str,
        clip_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        let removed = {
            let mut clips = write_txn.open_table(CLIPS).map_err(map_redb_err)?;
            let existing: Option<crate::domain::clip::Clip> =
                match clips.get(clip_id).map_err(map_redb_err)? {
                    Some(g) => Some(decode_json(g.value())?),
                    None => None,
                };
            let Some(prev) = existing else {
                return Ok(false);
            };
            if prev.owner_id != owner_id {
                return Ok(false);
            }
            clips.remove(clip_id).map_err(map_redb_err)?;
            drop(clips);
            let mut by_owner = write_txn.open_table(CLIPS_BY_OWNER).map_err(map_redb_err)?;
            by_owner.remove((owner_id, clip_id)).map_err(map_redb_err)?;
            drop(by_owner);
            // Cascade memberships + favorites (same transaction).
            let mut notes = write_txn.open_table(CLIP_NOTES).map_err(map_redb_err)?;
            let stale_notes: Vec<(String, String)> = notes
                .range((clip_id, "")..=(clip_id, "\u{10ffff}"))
                .map_err(map_redb_err)?
                .filter_map(|entry| {
                    entry.ok().map(|(k, _)| {
                        let (c, n) = k.value();
                        (c.to_string(), n.to_string())
                    })
                })
                .collect();
            for (c, n) in stale_notes {
                notes
                    .remove((c.as_str(), n.as_str()))
                    .map_err(map_redb_err)?;
            }
            drop(notes);
            let mut favs = write_txn.open_table(CLIP_FAVORITES).map_err(map_redb_err)?;
            let stale_favs: Vec<(String, String)> = favs
                .range((clip_id, "")..=(clip_id, "\u{10ffff}"))
                .map_err(map_redb_err)?
                .filter_map(|entry| {
                    entry.ok().map(|(k, _)| {
                        let (c, u) = k.value();
                        (c.to_string(), u.to_string())
                    })
                })
                .collect();
            for (c, u) in stale_favs {
                favs.remove((c.as_str(), u.as_str()))
                    .map_err(map_redb_err)?;
            }
            drop(favs);
            let mut fols = write_txn.open_table(CLIP_FOLLOWS).map_err(map_redb_err)?;
            let stale_fols: Vec<(String, String)> = fols
                .range((clip_id, "")..=(clip_id, "\u{10ffff}"))
                .map_err(map_redb_err)?
                .filter_map(|entry| {
                    entry.ok().map(|(k, _)| {
                        let (c, u) = k.value();
                        (c.to_string(), u.to_string())
                    })
                })
                .collect();
            for (c, u) in stale_fols {
                fols.remove((c.as_str(), u.as_str()))
                    .map_err(map_redb_err)?;
            }
            true
        };
        self.commit_write(write_txn)?;
        Ok(removed)
    }

    fn get_clip(&self, clip_id: &str) -> Result<Option<crate::domain::clip::Clip>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(CLIPS).map_err(map_redb_err)?;
        match table.get(clip_id).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn clips_by_owner(&self, owner_id: &str) -> Result<Vec<crate::domain::clip::Clip>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_owner = read_txn.open_table(CLIPS_BY_OWNER).map_err(map_redb_err)?;
        let table = read_txn.open_table(CLIPS).map_err(map_redb_err)?;
        let mut ids: Vec<String> = by_owner
            .range((owner_id, "")..=(owner_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
            .filter_map(|entry| {
                entry.ok().map(|(k, _)| {
                    let (_, c) = k.value();
                    c.to_string()
                })
            })
            .collect();
        ids.sort_by(|a, b| b.cmp(a));
        let mut out = Vec::new();
        for id in ids {
            if let Some(g) = table.get(id.as_str()).map_err(map_redb_err)? {
                out.push(decode_json(g.value())?);
            }
        }
        Ok(out)
    }

    fn add_clip_note(
        &self,
        row: crate::domain::clip::ClipNote,
        now_ms: i64,
        jobs: Vec<NewJob>,
    ) -> Result<crate::domain::clip::ClipNote, PortError> {
        use crate::domain::clip::CLIP_NOTES_PER_CLIP;
        let write_txn = self.begin_write()?;
        {
            self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
            let mut notes = write_txn.open_table(CLIP_NOTES).map_err(map_redb_err)?;
            if notes
                .get((row.clip_id.as_str(), row.note_id.as_str()))
                .map_err(map_redb_err)?
                .is_some()
            {
                return Err(PortError::Conflict("ALREADY_CLIPPED".to_string()));
            }
            let count = notes
                .range((row.clip_id.as_str(), "")..=(row.clip_id.as_str(), "\u{10ffff}"))
                .map_err(map_redb_err)?
                .count();
            if count >= CLIP_NOTES_PER_CLIP {
                return Err(PortError::Conflict("TOO_MANY_CLIP_NOTES".to_string()));
            }
            let encoded = encode_json(&row)?;
            notes
                .insert(
                    (row.clip_id.as_str(), row.note_id.as_str()),
                    encoded.as_slice(),
                )
                .map_err(map_redb_err)?;
            drop(notes);
            // Upstream `lastClippedAt` parity: touched on add only. Read
            // the row out first so the table borrow ends before the write.
            let prev: Option<crate::domain::clip::Clip> = {
                let clips = write_txn.open_table(CLIPS).map_err(map_redb_err)?;
                match clips.get(row.clip_id.as_str()).map_err(map_redb_err)? {
                    Some(g) => Some(decode_json(g.value())?),
                    None => None,
                }
            };
            if let Some(mut clip) = prev {
                clip.last_clipped_at_ms = Some(now_ms);
                // Phase 2a: membership changes move `updated` (the
                // collection stamp remote revalidation compares).
                clip.updated_at_ms = now_ms;
                let encoded = encode_json(&clip)?;
                let mut clips = write_txn.open_table(CLIPS).map_err(map_redb_err)?;
                clips
                    .insert(clip.id.as_str(), encoded.as_slice())
                    .map_err(map_redb_err)?;
            }
        }
        self.commit_write(write_txn)?;
        Ok(row)
    }

    fn remove_clip_note(
        &self,
        clip_id: &str,
        note_id: &str,
        now_ms: i64,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        let removed = {
            let mut notes = write_txn.open_table(CLIP_NOTES).map_err(map_redb_err)?;
            notes
                .remove((clip_id, note_id))
                .map_err(map_redb_err)?
                .is_some()
        };
        // Phase 2a: removals move `updated` in the same transaction.
        if removed {
            let prev: Option<crate::domain::clip::Clip> = {
                let clips = write_txn.open_table(CLIPS).map_err(map_redb_err)?;
                match clips.get(clip_id).map_err(map_redb_err)? {
                    Some(g) => Some(decode_json(g.value())?),
                    None => None,
                }
            };
            if let Some(mut clip) = prev {
                clip.updated_at_ms = now_ms;
                let encoded = encode_json(&clip)?;
                let mut clips = write_txn.open_table(CLIPS).map_err(map_redb_err)?;
                clips
                    .insert(clip.id.as_str(), encoded.as_slice())
                    .map_err(map_redb_err)?;
            }
        }
        self.commit_write(write_txn)?;
        Ok(removed)
    }

    fn clip_note_ids(&self, clip_id: &str) -> Result<Vec<String>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(CLIP_NOTES).map_err(map_redb_err)?;
        let mut rows: Vec<(String, String)> = table
            .range((clip_id, "")..=(clip_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
            .filter_map(|entry| {
                entry.ok().map(|(k, v)| {
                    let (_, n) = k.value();
                    let row: crate::domain::clip::ClipNote =
                        decode_json(v.value()).unwrap_or(crate::domain::clip::ClipNote {
                            id: String::new(),
                            clip_id: clip_id.to_string(),
                            note_id: n.to_string(),
                            created_at_ms: 0,
                        });
                    (row.id, n.to_string())
                })
            })
            .collect();
        // aidx row ids are time-ordered: id-desc reads newest-first.
        rows.sort_by(|a, b| b.0.cmp(&a.0));
        Ok(rows.into_iter().map(|(_, n)| n).collect())
    }

    fn count_clip_notes(&self, clip_id: &str) -> Result<u64, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(CLIP_NOTES).map_err(map_redb_err)?;
        Ok(table
            .range((clip_id, "")..=(clip_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
            .count() as u64)
    }

    fn favorite_clip(&self, fav: crate::domain::clip::ClipFavorite) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        let added = {
            let mut favs = write_txn.open_table(CLIP_FAVORITES).map_err(map_redb_err)?;
            if favs
                .get((fav.clip_id.as_str(), fav.user_id.as_str()))
                .map_err(map_redb_err)?
                .is_some()
            {
                false
            } else {
                let encoded = encode_json(&fav)?;
                favs.insert(
                    (fav.clip_id.as_str(), fav.user_id.as_str()),
                    encoded.as_slice(),
                )
                .map_err(map_redb_err)?;
                true
            }
        };
        self.commit_write(write_txn)?;
        Ok(added)
    }

    fn unfavorite_clip(&self, user_id: &str, clip_id: &str) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        let removed = {
            let mut favs = write_txn.open_table(CLIP_FAVORITES).map_err(map_redb_err)?;
            favs.remove((clip_id, user_id))
                .map_err(map_redb_err)?
                .is_some()
        };
        self.commit_write(write_txn)?;
        Ok(removed)
    }

    fn is_clip_favorited(&self, user_id: &str, clip_id: &str) -> Result<bool, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(CLIP_FAVORITES).map_err(map_redb_err)?;
        Ok(table
            .get((clip_id, user_id))
            .map_err(map_redb_err)?
            .is_some())
    }

    fn clip_favorite_count(&self, clip_id: &str) -> Result<u64, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(CLIP_FAVORITES).map_err(map_redb_err)?;
        Ok(table
            .range((clip_id, "")..=(clip_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
            .count() as u64)
    }

    fn clip_favorites_by_user(
        &self,
        user_id: &str,
    ) -> Result<Vec<crate::domain::clip::ClipFavorite>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(CLIP_FAVORITES).map_err(map_redb_err)?;
        let mut rows: Vec<crate::domain::clip::ClipFavorite> = table
            .iter()
            .map_err(map_redb_err)?
            .filter_map(|entry| {
                entry.ok().and_then(|(k, v)| {
                    let (_, u) = k.value();
                    (u == user_id).then(|| decode_json(v.value()).ok())?
                })
            })
            .collect();
        rows.sort_by(|a, b| b.id.cmp(&a.id));
        Ok(rows)
    }

    fn follow_clip(&self, follow: crate::domain::clip::ClipFollow) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        let added = {
            let mut fols = write_txn.open_table(CLIP_FOLLOWS).map_err(map_redb_err)?;
            if fols
                .get((follow.clip_id.as_str(), follow.user_id.as_str()))
                .map_err(map_redb_err)?
                .is_some()
            {
                false
            } else {
                let encoded = encode_json(&follow)?;
                fols.insert(
                    (follow.clip_id.as_str(), follow.user_id.as_str()),
                    encoded.as_slice(),
                )
                .map_err(map_redb_err)?;
                true
            }
        };
        self.commit_write(write_txn)?;
        Ok(added)
    }

    fn unfollow_clip(&self, user_id: &str, clip_id: &str) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        let removed = {
            let mut fols = write_txn.open_table(CLIP_FOLLOWS).map_err(map_redb_err)?;
            fols.remove((clip_id, user_id))
                .map_err(map_redb_err)?
                .is_some()
        };
        self.commit_write(write_txn)?;
        Ok(removed)
    }

    fn is_following_clip(&self, user_id: &str, clip_id: &str) -> Result<bool, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(CLIP_FOLLOWS).map_err(map_redb_err)?;
        Ok(table
            .get((clip_id, user_id))
            .map_err(map_redb_err)?
            .is_some())
    }

    fn followers_of_clip(
        &self,
        clip_id: &str,
        limit: usize,
    ) -> Result<Vec<crate::domain::clip::ClipFollow>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(CLIP_FOLLOWS).map_err(map_redb_err)?;
        let mut rows: Vec<crate::domain::clip::ClipFollow> = table
            .range((clip_id, "")..=(clip_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
            .filter_map(|entry| entry.ok().and_then(|(_, v)| decode_json(v.value()).ok()))
            .collect();
        rows.sort_by(|a, b| b.id.cmp(&a.id));
        rows.truncate(limit);
        Ok(rows)
    }

    fn followed_clips_of(
        &self,
        user_id: &str,
    ) -> Result<Vec<crate::domain::clip::ClipFollow>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(CLIP_FOLLOWS).map_err(map_redb_err)?;
        let mut rows: Vec<crate::domain::clip::ClipFollow> = table
            .iter()
            .map_err(map_redb_err)?
            .filter_map(|entry| {
                entry.ok().and_then(|(k, v)| {
                    let (_, u) = k.value();
                    (u == user_id).then(|| decode_json(v.value()).ok())?
                })
            })
            .collect();
        rows.sort_by(|a, b| b.id.cmp(&a.id));
        Ok(rows)
    }

    fn record_clip_tombstone(&self, clip_uri: &str, deleted_at_ms: i64) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut t = write_txn
                .open_table(CLIP_TOMBSTONES)
                .map_err(map_redb_err)?;
            t.insert(clip_uri, deleted_at_ms as u64)
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)?;
        Ok(())
    }

    fn clip_tombstone(&self, clip_uri: &str) -> Result<Option<i64>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(CLIP_TOMBSTONES).map_err(map_redb_err)?;
        Ok(table
            .get(clip_uri)
            .map_err(map_redb_err)?
            .map(|g| g.value() as i64))
    }
}
