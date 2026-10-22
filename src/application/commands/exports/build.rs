use crate::application::commands::AppServices;

use super::common::{PAGE_SIZE, format_acct};
use crate::application::ports::{ExportEntity, ExportJobPayload, PortError, TimelineCursor};

impl AppServices {
    /// Build one export file's bytes (Misskey `Export*ProcessorService`
    /// payload parity; see each arm).
    pub(crate) fn build_export_bytes(
        &self,
        user_id: &str,
        payload: &ExportJobPayload,
    ) -> Result<Vec<u8>, PortError> {
        match payload.entity {
            // `following-*.csv`: one `{acct},withReplies={bool}` row per
            // followee, follow-row-id order (upstream `order: { id: 1 }`
            // parity). No header row (upstream writes none). `excludeMuting`
            // drops muted followees (upstream `Not(In(muteeIds))` parity);
            // `excludeInactive` is accepted and ignored (see module docs).
            ExportEntity::Following => {
                let mut follows = Vec::new();
                let mut cursor: Option<String> = None;
                loop {
                    let page = self
                        .follows
                        .following_of(user_id, cursor.as_deref(), PAGE_SIZE)?;
                    if page.is_empty() {
                        break;
                    }
                    cursor = page.last().map(|f| f.followee_id.clone());
                    let len = page.len();
                    follows.extend(page);
                    if len < PAGE_SIZE {
                        break;
                    }
                }
                follows.sort_by(|a, b| a.id.cmp(&b.id));
                let mut out = String::new();
                for follow in &follows {
                    let Some(target) = self.accounts.find_by_id(&follow.followee_id)? else {
                        continue;
                    };
                    if payload.exclude_muting && self.mutes.is_muting(user_id, &target.id)? {
                        continue;
                    }
                    out.push_str(&format!(
                        "{},withReplies={}\n",
                        format_acct(&target),
                        follow.with_replies
                    ));
                }
                Ok(out.into_bytes())
            }
            // `blocking-*.csv`: one `{acct}` row per blockee, block-row-id
            // order. No header row (upstream writes none).
            ExportEntity::Blocking => {
                let rows = self.all_blockings(user_id)?;
                let mut out = String::new();
                for row in &rows {
                    let Some(target) = self.accounts.find_by_id(&row.blockee_id)? else {
                        continue;
                    };
                    out.push_str(&format!("{}\n", format_acct(&target)));
                }
                Ok(out.into_bytes())
            }
            // `mute-*.csv`: one `{acct}` row per mutee, mute-row-id order
            // (upstream `mute-*.csv` — the plan text's "JSON" is a slip;
            // the processor writes CSV lines). Only non-expiring mutes
            // (upstream `expiresAt: IsNull()` parity); no header row.
            ExportEntity::Muting => {
                let rows = self.all_mutings(user_id)?;
                let mut out = String::new();
                for row in &rows {
                    if row.expires_at_ms.is_some() {
                        continue;
                    }
                    let Some(target) = self.accounts.find_by_id(&row.mutee_id)? else {
                        continue;
                    };
                    out.push_str(&format!("{}\n", format_acct(&target)));
                }
                Ok(out.into_bytes())
            }
            // `notes-*.json`: JSON array of the caller's own notes,
            // newest-first (task order; upstream pages id-ASC — DIVERGENCE
            // in order only), `ExportNotesProcessorService` DTO shape
            // (`id/text/createdAt/fileIds/files/replyId/renoteId/poll/cw/
            // visibility/visibleUserIds/localOnly/reactionAcceptance`).
            ExportEntity::Notes => {
                let mut notes = Vec::new();
                let mut until_id: Option<String> = None;
                loop {
                    let page = self.notes.notes_by_author(
                        user_id,
                        &TimelineCursor {
                            since_id: None,
                            until_id: until_id.clone(),
                            since_date: None,
                            until_date: None,
                            limit: PAGE_SIZE,
                        },
                    )?;
                    if page.is_empty() {
                        break;
                    }
                    until_id = page.last().map(|n| n.id.clone());
                    let len = page.len();
                    notes.extend(page);
                    if len < PAGE_SIZE {
                        break;
                    }
                }
                let mut items = Vec::with_capacity(notes.len());
                for note in &notes {
                    items.push(self.export_note_dto(note)?);
                }
                serde_json::to_vec(&items).map_err(|e| {
                    PortError::StorageUnavailable(format!("export notes serialize failed: {e}"))
                })
            }
            // `user-lists-*.csv`: one `listName,acct,withReplies=bool` row
            // per membership (upstream
            // `ExportUserListsProcessorService` parity; list-row order,
            // then member rows). No header row.
            ExportEntity::UserLists => {
                let mut out = Vec::new();
                let lists = self.lists.lists_by_owner(user_id)?;
                for list in &lists {
                    let members = self.lists.members_of_list(&list.id)?;
                    for m in &members {
                        let Some(member) = self.accounts.find_by_id(&m.member_id)? else {
                            continue;
                        };
                        out.push(format!(
                            "{},{},withReplies={}\n",
                            list.name,
                            format_acct(&member),
                            m.with_replies
                        ));
                    }
                }
                Ok(out.concat().into_bytes())
            }
            // Antenna subsystem now exists (Batch N WS4): the caller's
            // antennas as JSON (`ExportAntennasProcessorService` shape).
            // `src=list` antennas carry their members' accts (Batch O WP16;
            // upstream `antenna.userList?.userIds` parity) so a re-import
            // on a fresh instance rebuilds the member set.
            ExportEntity::Antennas => {
                let rows = self.antennas.antennas_by_owner(user_id)?;
                let mut items = Vec::with_capacity(rows.len());
                for row in &rows {
                    let user_list_accts = match (row.src, row.user_list_id.as_deref()) {
                        (crate::domain::antenna::AntennaSrc::List, Some(list_id)) => {
                            let accts = self
                                .lists
                                .members_of_list(list_id)?
                                .iter()
                                .filter_map(|m| {
                                    self.accounts.find_by_id(&m.member_id).ok().flatten()
                                })
                                .map(|a| format_acct(&a))
                                .collect::<Vec<_>>();
                            serde_json::Value::Array(
                                accts.into_iter().map(serde_json::Value::String).collect(),
                            )
                        }
                        _ => serde_json::Value::Null,
                    };
                    items.push(serde_json::json!({
                        "name": row.name,
                        "src": row.src.as_str(),
                        "keywords": row.keywords,
                        "excludeKeywords": row.exclude_keywords,
                        "users": row.users,
                        "userListAccts": user_list_accts,
                        "caseSensitive": row.case_sensitive,
                        "localOnly": row.local_only,
                        "excludeBots": row.exclude_bots,
                        "withReplies": row.with_replies,
                        "withFile": row.with_file,
                        "excludeNotesInSensitiveChannel": row.exclude_notes_in_sensitive_channel,
                    }));
                }
                serde_json::to_vec(&items).map_err(|e| {
                    PortError::StorageUnavailable(format!("export antennas serialize failed: {e}"))
                })
            }
            // Clips now have a subsystem (federated clips + pages handoff,
            // Phase 1): JSON array of upstream
            // `serializeClip`/`serializeClipNote` shape — `{id, name,
            // description, lastClippedAt, clipNotes: [{id, createdAt,
            // note: {...}}]}` — id-ASC order. Notes invisible to the
            // exporter or gone are skipped (visibility-query parity);
            // federation columns (`tier`, `federate`, approvers) are never
            // exported (local backup format, not a federation vector).
            ExportEntity::Clips => {
                let mut clips = self.clips.clips_by_owner(user_id)?;
                clips.sort_by(|a, b| a.id.cmp(&b.id));
                let mut items = Vec::with_capacity(clips.len());
                for clip in &clips {
                    let mut clip_notes = Vec::new();
                    for note_id in self.clips.clip_note_ids(&clip.id)? {
                        let Some(note) = self.notes.get_note(&note_id)? else {
                            continue;
                        };
                        if note.is_deleted {
                            continue;
                        }
                        if !self.note_visible_to(&note, user_id)? {
                            continue;
                        }
                        let Some(author) = self.accounts.find_by_id(&note.author_id)? else {
                            continue;
                        };
                        let note_created_at = self
                            .ids
                            .parse_time(&note.id)
                            .map(crate::activitypub::format_millis)
                            .unwrap_or_else(|_| crate::activitypub::format_millis(self.now_ms()));
                        let poll = match self.polls.get_poll(&note.id)? {
                            Some(p) => serde_json::json!({
                                "choices": p.choices,
                                "votes": p.votes,
                                "multiple": p.multiple,
                                "expiresAt": p.expires_at_ms.map(crate::activitypub::format_millis),
                            }),
                            None => serde_json::Value::Null,
                        };
                        clip_notes.push(serde_json::json!({
                            "id": format!("export-{}-{}", clip.id, note.id),
                            "createdAt": note_created_at,
                            "note": {
                                "id": note.id,
                                "text": note.text,
                                "createdAt": note_created_at,
                                "fileIds": note.file_ids,
                                "replyId": note.reply_id,
                                "renoteId": note.renote_id,
                                "poll": poll,
                                "cw": note.cw,
                                "visibility": note.visibility,
                                "visibleUserIds": note.visible_user_ids,
                                "localOnly": note.local_only,
                                "reactionAcceptance": note.reaction_acceptance,
                                "uri": note.uri,
                                "url": note.url,
                                "user": {
                                    "id": author.id,
                                    "name": author.display_name,
                                    "username": author.username,
                                    "host": author.host,
                                    "uri": author.uri,
                                },
                            },
                        }));
                    }
                    items.push(serde_json::json!({
                        "id": clip.id,
                        "name": clip.name,
                        "description": clip.description,
                        "lastClippedAt": clip.last_clipped_at_ms.map(crate::activitypub::format_millis),
                        "clipNotes": clip_notes,
                    }));
                }
                serde_json::to_vec(&items).map_err(|e| {
                    PortError::StorageUnavailable(format!("export clips serialize failed: {e}"))
                })
            } // Favorites now have a subsystem: JSON array of
            // `{id, createdAt, note: {...}}` (upstream
            // `ExportFavoritesProcessorService.serialize` parity), id-ASC
            // order. Invisible/gone notes are skipped (visibility query
            // parity); hidden-before notes are not modeled.
            ExportEntity::Favorites => {
                let rows = self.all_favorites(user_id)?;
                let mut items = Vec::with_capacity(rows.len());
                for fav in &rows {
                    let Some(note) = self.notes.get_note(&fav.note_id)? else {
                        continue;
                    };
                    if note.is_deleted {
                        continue;
                    }
                    let Some(author) = self.accounts.find_by_id(&note.author_id)? else {
                        continue;
                    };
                    let created_at = self
                        .ids
                        .parse_time(&fav.id)
                        .map(crate::activitypub::format_millis)
                        .unwrap_or_else(|_| crate::activitypub::format_millis(self.now_ms()));
                    let note_created_at = self
                        .ids
                        .parse_time(&note.id)
                        .map(crate::activitypub::format_millis)
                        .unwrap_or_else(|_| crate::activitypub::format_millis(self.now_ms()));
                    let poll = match self.polls.get_poll(&note.id)? {
                        Some(p) => serde_json::json!({
                            "choices": p.choices,
                            "votes": p.votes,
                            "multiple": p.multiple,
                            "expiresAt": p.expires_at_ms.map(crate::activitypub::format_millis),
                        }),
                        None => serde_json::Value::Null,
                    };
                    items.push(serde_json::json!({
                        "id": fav.id,
                        "createdAt": created_at,
                        "note": {
                            "id": note.id,
                            "text": note.text,
                            "createdAt": note_created_at,
                            "fileIds": note.file_ids,
                            "replyId": note.reply_id,
                            "renoteId": note.renote_id,
                            "poll": poll,
                            "cw": note.cw,
                            "visibility": note.visibility,
                            "visibleUserIds": note.visible_user_ids,
                            "localOnly": note.local_only,
                            "reactionAcceptance": note.reaction_acceptance,
                            "uri": note.uri,
                            "url": note.url,
                            "user": {
                                "id": author.id,
                                "name": author.display_name,
                                "username": author.username,
                                "host": author.host,
                                "uri": author.uri,
                            },
                        },
                    }));
                }
                serde_json::to_vec(&items).map_err(|e| {
                    PortError::StorageUnavailable(format!("export favorites serialize failed: {e}"))
                })
            }
        }
    }
}
