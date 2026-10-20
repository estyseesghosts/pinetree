//! Profile import/export (§13; Misskey `i/export-*` + `i/import-*` parity).
//!
//! Reference pins: Misskey `2026.10.1-alpha.0` (`./misskey`). Verified this
//! session against `i/export-following|blocking|mute|notes|antennas|clips|
//! favorites|user-lists.ts`, `i/import-blocking|muting|following|antennas|
//! user-lists.ts`, `QueueService.ts:322-587` (`createExport*`/`createImport*`
//! job shapes), and every `Export*`/`Import*ProcessorService.ts`.
//!
//! Shape of the slice:
//! - The endpoint arms enqueue one [`JobKind::Export`]/[`JobKind::Import`]
//!   job (ids only — never file content, per the 64KiB job-payload cap) and
//!   answer `204 No Content` immediately (upstream returns void too).
//! - The worker runs [`AppServices::run_export_job`]/[`run_import_job`].
//!   Export completion fans out through `Disposition::Fanout`, so the export
//!   job's completion and the `exportCompleted` `Notify` enqueue commit in
//!   ONE storage transaction (`complete_with_followups`).
//! - Exports land in the caller's drive via the existing `upload_media`
//!   path; imports read the file back via media metadata + `blobs.get`.
//!
//! Divergences (all documented here and in the endpoint manifest §20):
//! - `excludeInactive` is accepted and ignored: pinetree tracks no
//!   last-active timestamp, and a zero-notes heuristic would mislabel
//!   lurkers as inactive.
//! - Local accounts export as a bare `username` (upstream
//!   `getFullApAccount` always appends `@host`, even for locals); imports
//!   accept both the bare and the `user@host` form.
//! - Remote users are resolved from the local account store only: pinetree
//!   has no remote-user resolver, so unresolvable rows are skipped (upstream
//!   fetches them over federation).
//! - User lists have a subsystem and export/import real rows. Clips have
//!   one too (federated clips + pages handoff, Phase 1): `export-clips`
//!   writes the upstream `serializeClip` shape, and the pinetree-only
//!   `i/import-clips` consumes that same shape (find-or-create by name;
//!   upstream has no import-clips endpoint). Favorites (Batch N WS2) and
//!   antennas (Batch N WS4) have subsystems and export/import real rows.
//! - Drive upload dedups on content (`force == false`, unlike upstream
//!   `force: true`) so a crashed-and-retried export converges on the same
//!   file instead of orphaning a duplicate.

use super::AppServices;

use crate::application::events::NotifyJobPayload;
use crate::application::ports::{
    ExportEntity, ExportJobPayload, ImportEntity, ImportJobPayload, JobKind, NewJob, PortError,
    TimelineCursor,
};
use crate::domain::notification::NotificationKind;

/// Largest import file the endpoints admit (Misskey
/// `import-*.ts:82` parity: `64 * 1024` unless the account moved recently;
/// pinetree has no account moves, so the small cap always applies).
pub const IMPORT_MAX_FILE_BYTES: u64 = 64 * 1024;

/// Rows read per store page while building an export (`export*.ts`
/// `take: 100` parity). One storage transaction per import row keeps every
/// write far under the FDB ~10 MB / ~5 s caps.
const PAGE_SIZE: usize = 100;

impl AppServices {
    /// Enqueue one export job for `user_id` (endpoint path; the rate limit
    /// in the endpoint arm is the 1/hr gate).
    pub fn enqueue_export(
        &self,
        user_id: &str,
        entity: ExportEntity,
        exclude_muting: bool,
        exclude_inactive: bool,
    ) -> Result<String, PortError> {
        self.accounts.get_owner(user_id)?;
        let export_id = self.ids.generate();
        let payload = ExportJobPayload {
            export_id: export_id.clone(),
            user_id: user_id.to_string(),
            entity,
            exclude_muting,
            exclude_inactive,
        };
        let entity_tag = entity.exported_entity();
        self.jobs.enqueue_jobs(vec![NewJob::new_json(
            JobKind::Export,
            &payload,
            format!("export:{entity_tag}:{export_id}"),
            self.now_ms(),
        )?])?;
        Ok(export_id)
    }

    /// Enqueue one import job for `user_id` (endpoint path; guards ran in
    /// [`Self::check_import_file`] first).
    pub fn enqueue_import(
        &self,
        user_id: &str,
        entity: ImportEntity,
        file_id: &str,
        with_replies: Option<bool>,
    ) -> Result<String, PortError> {
        self.accounts.get_owner(user_id)?;
        let import_id = self.ids.generate();
        let payload = ImportJobPayload {
            import_id: import_id.clone(),
            user_id: user_id.to_string(),
            entity,
            file_id: file_id.to_string(),
            with_replies,
        };
        self.jobs.enqueue_jobs(vec![NewJob::new_json(
            JobKind::Import,
            &payload,
            format!("import:{import_id}"),
            self.now_ms(),
        )?])?;
        Ok(import_id)
    }

    /// Misskey `import-*.ts:71-82` guard shape: the file must exist AND
    /// belong to the caller (`NO_SUCH_FILE`), must be non-empty
    /// (`EMPTY_FILE`), and must fit the 64KiB cap (`TOO_BIG_FILE`). The
    /// endpoint maps these code strings to the per-endpoint verbatim UUIDs
    /// (`ApiError::import_*`). The commented-out upstream `type` check
    /// (`UNEXPECTED_FILE_TYPE`) stays unimplemented, like upstream.
    pub fn check_import_file(
        &self,
        user_id: &str,
        file_id: &str,
    ) -> Result<crate::domain::media::MediaMeta, PortError> {
        self.accounts.get_owner(user_id)?;
        let meta = match self.media.get(file_id)? {
            Some(meta) if meta.owner_id.as_deref() == Some(user_id) => meta,
            _ => return Err(PortError::NotFound("NO_SUCH_FILE".to_string())),
        };
        if meta.size_bytes == 0 {
            return Err(PortError::Validation("EMPTY_FILE".to_string()));
        }
        if meta.size_bytes > IMPORT_MAX_FILE_BYTES {
            return Err(PortError::Validation("TOO_BIG_FILE".to_string()));
        }
        Ok(meta)
    }

    /// Notifier-less delivery gate for `exportCompleted` (Misskey
    /// `NotificationService.ts:97-104` parity: with no `notifierId` only a
    /// `never` receive-config drops the notification; self-notify and mute
    /// checks do not apply). A missing notifiee fails open.
    pub fn export_completed_allowed(&self, notifiee_id: &str) -> Result<bool, PortError> {
        let Some(notifiee) = self.accounts.find_by_id(notifiee_id)? else {
            return Ok(true);
        };
        let Some(config) = notifiee
            .notification_recieve_config
            .get(NotificationKind::ExportCompleted.wire_type())
        else {
            return Ok(true);
        };
        Ok(config.kind.as_str() != "never")
    }

    /// Run one [`JobKind::Export`] job (worker path). Returns the
    /// `exportCompleted` `Notify` follow-up for the worker to commit
    /// atomically with the export job's completion (`Fanout`), or `None`
    /// when the user is gone (converges quietly, like upstream).
    pub fn run_export_job(&self, payload: &ExportJobPayload) -> Result<Option<NewJob>, PortError> {
        let Some(user) = self.accounts.find_by_id(&payload.user_id)? else {
            return Ok(None);
        };
        let bytes = self.build_export_bytes(&user.id, payload)?;
        let name = export_file_name(payload.entity, self.now_ms());
        // Content-deduped (`force == false`, unlike upstream `force: true`):
        // identical bytes return the existing row, so a crashed-and-retried
        // export converges on one file instead of orphaning a duplicate.
        let file = self.upload_media(&user.id, name, &bytes, None, false, false)?;
        let notify = NotifyJobPayload {
            notifiee_id: user.id.clone(),
            notifier_id: user.id.clone(),
            kind: NotificationKind::ExportCompleted,
            note_id: None,
            reaction: None,
            file_id: Some(file.id),
            exported_entity: Some(payload.entity.exported_entity().to_string()),
            clip_id: None,
            page_id: None,
        };
        Ok(Some(NewJob::new_json(
            JobKind::Notify,
            &notify,
            format!("export-completed:{}", payload.export_id),
            self.now_ms(),
        )?))
    }

    /// Run one [`JobKind::Import`] job (worker path). A gone user or file
    /// converges quietly (upstream returns early too). Per-row failures
    /// (unresolvable, self, `ALREADY_*`, block gates) skip the row and count
    /// it; only storage faults fail the job so it retries with backoff.
    pub fn run_import_job(&self, payload: &ImportJobPayload) -> Result<(), PortError> {
        let Some(user) = self.accounts.find_by_id(&payload.user_id)? else {
            return Ok(());
        };
        let Some(meta) = self.media.get(&payload.file_id)? else {
            return Ok(());
        };
        if meta.owner_id.as_deref() != Some(user.id.as_str()) {
            return Ok(());
        }
        let Some(bytes) = self.blobs.get(&meta.blob_key)? else {
            return Ok(());
        };
        match payload.entity {
            ImportEntity::Following | ImportEntity::Blocking | ImportEntity::Muting => {
                let text = String::from_utf8_lossy(&bytes);
                let (applied, skipped) = self.apply_relation_import(
                    &user.id,
                    payload.entity,
                    &text,
                    payload.with_replies,
                )?;
                eprintln!(
                    "import-{} for {}: applied={applied} skipped={skipped}",
                    import_entity_tag(payload.entity),
                    user.id,
                );
            }
            // User lists now have a subsystem (Batch O WP16): parse + apply
            // rows through `apply_user_list_import` (find-or-create by name,
            // local-only resolution, dup/foreign skips).
            ImportEntity::UserLists => {
                let text = String::from_utf8_lossy(&bytes);
                let (applied, skipped) = self.apply_user_list_import(&user.id, &text)?;
                eprintln!(
                    "import-user-lists for {}: applied={applied} skipped={skipped}",
                    user.id,
                );
            }
            // Antenna subsystem now exists (Batch N WS4): parse + validate
            // rows with the upstream `exportedAntennaSchema` rules, skip
            // empty-keyword rows, rewrite `src=list`+`userListAccts` to
            // `src=users` (upstream parity), and apply through
            // `create_antenna` (same op as the interactive endpoint).
            ImportEntity::Antennas => {
                let (applied, skipped) = self.apply_antenna_import(&user.id, &bytes)?;
                eprintln!(
                    "import-antennas for {}: applied={applied} skipped={skipped}",
                    user.id,
                );
            }
            // pinetree-only clip import (no upstream endpoint exists):
            // consume our own `export-clips` format — find-or-create each
            // clip by name, attach resolvable + visible notes.
            ImportEntity::Clips => {
                let text = String::from_utf8_lossy(&bytes);
                let (applied, skipped) = self.apply_clip_import(&user.id, &text)?;
                eprintln!(
                    "import-clips for {}: applied={applied} skipped={skipped}",
                    user.id,
                );
            }
        }
        Ok(())
    }

    /// Build one export file's bytes (Misskey `Export*ProcessorService`
    /// payload parity; see each arm).
    fn build_export_bytes(
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
                            .unwrap_or_else(|_| {
                                crate::activitypub::format_millis(self.now_ms())
                            });
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
            }            // Favorites now have a subsystem: JSON array of
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
                        .unwrap_or_else(|_| {
                            crate::activitypub::format_millis(self.now_ms())
                        });
                    let note_created_at = self
                        .ids
                        .parse_time(&note.id)
                        .map(crate::activitypub::format_millis)
                        .unwrap_or_else(|_| {
                            crate::activitypub::format_millis(self.now_ms())
                        });
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

    /// Every blocking row by `user_id`, block-row-id order (upstream
    /// `order: { id: 1 }` parity; the store pages newest-first, so page
    /// fully and re-sort).
    fn all_blockings(
        &self,
        user_id: &str,
    ) -> Result<Vec<crate::domain::blocking::Blocking>, PortError> {
        let mut rows = Vec::new();
        let mut until_id: Option<String> = None;
        loop {
            let page = self.blocks.list_blockings(
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
            until_id = page.last().map(|r| r.id.clone());
            let len = page.len();
            rows.extend(page);
            if len < PAGE_SIZE {
                break;
            }
        }
        rows.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(rows)
    }

    /// Every muting row by `user_id` (including expired — the caller keeps
    /// only non-expiring ones), mute-row-id order.
    fn all_mutings(
        &self,
        user_id: &str,
    ) -> Result<Vec<crate::domain::muting::Muting>, PortError> {
        let mut rows = Vec::new();
        let mut until_id: Option<String> = None;
        loop {
            let page = self.mutes.list_mutings(
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
            until_id = page.last().map(|r| r.id.clone());
            let len = page.len();
            rows.extend(page);
            if len < PAGE_SIZE {
                break;
            }
        }
        rows.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(rows)
    }

    /// Every favorite row by `user_id`, favorite-id order (upstream
    /// `order: { id: 1 }` parity).
    fn all_favorites(
        &self,
        user_id: &str,
    ) -> Result<Vec<crate::domain::favorite::NoteFavorite>, PortError> {
        let mut rows = Vec::new();
        let mut until_id: Option<String> = None;
        loop {
            let page = self.favorites.favorites_by_user(
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
            until_id = page.last().map(|r| r.id.clone());
            let len = page.len();
            rows.extend(page);
            if len < PAGE_SIZE {
                break;
            }
        }
        rows.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(rows)
    }

    /// One `ExportNotesProcessorService` note DTO.
    fn export_note_dto(
        &self,
        note: &crate::domain::note::Note,
    ) -> Result<serde_json::Value, PortError> {
        let created_at = self
            .ids
            .parse_time(&note.id)
            .map(crate::activitypub::format_millis)
            .unwrap_or_else(|_| crate::activitypub::format_millis(self.now_ms()));
        let mut files = Vec::new();
        for file_id in &note.file_ids {
            if let Some(meta) = self.media.get(file_id)? {
                files.push(crate::api::misskey::packers::pack_drive_file(&meta, self));
            }
        }
        let poll = match self.polls.get_poll(&note.id)? {
            None => serde_json::Value::Null,
            Some(poll) => serde_json::json!({
                "choices": poll.choices,
                "votes": poll.votes,
                "multiple": poll.multiple,
                "expiresAt": poll.expires_at_ms.map(crate::activitypub::format_millis),
            }),
        };
        Ok(serde_json::json!({
            "id": note.id,
            "text": note.text,
            "createdAt": created_at,
            "fileIds": note.file_ids,
            "files": files,
            "replyId": note.reply_id,
            "renoteId": note.renote_id,
            "poll": poll,
            "cw": note.cw,
            "visibility": note.visibility,
            "visibleUserIds": note.visible_user_ids,
            "localOnly": note.local_only,
            "reactionAcceptance": note.reaction_acceptance,
        }))
    }

    /// Apply one following/blocking/muting CSV import. Returns
    /// `(applied, skipped)`. Every row goes through the same application
    /// op as the interactive endpoint (`follow`/`block`/`mute`), so §0A
    /// block gates and `ALREADY_*` dedup apply unchanged; a replay of the
    /// same file converges on conflicts instead of duplicating rows.
    fn apply_relation_import(
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
            let Some((target_id, with_replies)) = self.resolve_import_row(
                entity,
                line,
                with_replies_default.unwrap_or(true),
            )?
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
                ImportEntity::Blocking => {
                    self.block(user_id, &target_id).map(|_| ()).map_err(skip_as_skipped)
                }
                ImportEntity::Muting => self
                    .mute(user_id, &target_id, None)
                    .map(|_| ())
                    .map_err(skip_as_skipped),
                ImportEntity::UserLists | ImportEntity::Antennas | ImportEntity::Clips => {
                    unreachable!(
                        "apply_relation_import only serves Following/Blocking/Muting"
                    )
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
    fn resolve_acct(
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

    /// Apply one antenna import file (Batch N WS4; upstream
    /// `ImportAntennasProcessorService` parity): skip empty-keyword rows
    /// (`keywords` empty or first group all blank — verbatim upstream
    /// rule), skip schema-invalid rows, rewrite `src=list`+`userListAccts`
    /// to `src=users` (upstream parity; `src=list` without accts is
    /// skipped — pinetree has no lists to reference), and create the rest
    /// through `create_antenna`. Returns `(applied, skipped)`; only storage
    /// faults abort the job.
    fn apply_antenna_import(&self, user_id: &str, bytes: &[u8]) -> Result<(usize, usize), PortError> {
        use crate::domain::antenna::AntennaSrc;
        let text = String::from_utf8_lossy(bytes);
        let parsed: serde_json::Value = match serde_json::from_str(&text) {
            Ok(value) => value,
            Err(_) => return Ok((0, 1)),
        };
        let entries = match parsed.as_array() {
            Some(entries) => entries,
            None => return Ok((0, 1)),
        };
        let mut applied = 0usize;
        let mut skipped = 0usize;
        for entry in entries {
            let Some(map) = entry.as_object() else {
                skipped += 1;
                continue;
            };
            // Required keys (upstream `required` parity).
            if !["name", "src", "keywords", "excludeKeywords", "users", "caseSensitive", "withReplies", "withFile"]
                .iter()
                .all(|key| map.contains_key(*key))
            {
                skipped += 1;
                continue;
            }
            let Some(name) = map.get("name").and_then(|v| v.as_str()) else {
                skipped += 1;
                continue;
            };
            let name = name.to_string();
            if name.chars().count() == 0 || name.chars().count() > 100 {
                skipped += 1;
                continue;
            }
            let Some(src_raw) = map.get("src").and_then(|v| v.as_str()) else {
                skipped += 1;
                continue;
            };
            let mut src = match AntennaSrc::parse(src_raw) {
                Some(s) => s,
                None => {
                    skipped += 1;
                    continue;
                }
            };
            let str_array = |key: &str| -> Option<Vec<Vec<String>>> {
                let arr = map.get(key)?.as_array()?;
                arr.iter()
                    .map(|group| {
                        group
                            .as_array()
                            .map(|items| items.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
                    })
                    .collect()
            };
            let flat_array = |key: &str| -> Option<Vec<String>> {
                map.get(key)?
                    .as_array()
                    .map(|items| items.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
            };
            let Some(keywords) = str_array("keywords") else {
                skipped += 1;
                continue;
            };
            let Some(exclude_keywords) = str_array("excludeKeywords") else {
                skipped += 1;
                continue;
            };
            let Some(mut users) = flat_array("users") else {
                skipped += 1;
                continue;
            };
            let (case_sensitive, with_replies, with_file) = match (
                map.get("caseSensitive").and_then(|v| v.as_bool()),
                map.get("withReplies").and_then(|v| v.as_bool()),
                map.get("withFile").and_then(|v| v.as_bool()),
            ) {
                (Some(a), Some(b), Some(c)) => (a, b, c),
                _ => {
                    skipped += 1;
                    continue;
                }
            };
            let opt_bool = |key: &str, default: bool| -> Option<bool> {
                match map.get(key) {
                    None => Some(default),
                    Some(v) => v.as_bool(),
                }
            };
            let (Some(local_only), Some(exclude_bots)) =
                (opt_bool("localOnly", false), opt_bool("excludeBots", false))
            else {
                skipped += 1;
                continue;
            };
            // Upstream empty-keyword skip (verbatim rule).
            if keywords.is_empty()
                || keywords.first().map(|g| g.iter().all(|x| x.is_empty())).unwrap_or(true)
            {
                skipped += 1;
                continue;
            }
            // `src=list` rewrite (upstream parity): accts become `users`.
            if src == AntennaSrc::List {
                match map.get("userListAccts") {
                    Some(serde_json::Value::Array(accts)) => {
                        users = accts
                            .iter()
                            .filter_map(|v| v.as_str().map(str::to_string))
                            .filter(|s| !s.is_empty())
                            .collect();
                        src = AntennaSrc::Users;
                    }
                    _ => {
                        skipped += 1;
                        continue;
                    }
                }
            }
            match self.create_antenna(
                user_id,
                name,
                src,
                None,
                keywords,
                exclude_keywords,
                users,
                case_sensitive,
                with_replies,
                with_file,
                local_only,
                exclude_bots,
            ) {
                Ok(_) => applied += 1,
                Err(e @ PortError::StorageUnavailable(_)) => return Err(e),
                Err(_) => skipped += 1,
            }
        }
        Ok((applied, skipped))
    }

    /// Read-time visibility of one note for the exporter/importer
    /// (export-clips visibility-query parity): local-user viewer with the
    /// real follow edge.
    fn note_visible_to(
        &self,
        note: &crate::domain::note::Note,
        viewer_id: &str,
    ) -> Result<bool, PortError> {
        use crate::domain::visibility::{ViewerContext, is_visible};
        let follows = note.author_id == viewer_id
            || self.follows.is_following(viewer_id, &note.author_id)?;
        Ok(is_visible(note, &ViewerContext::local_user(viewer_id, follows)))
    }

    /// pinetree-only clip import (no upstream endpoint exists): consume our
    /// own `export-clips` JSON (`[{name, description?, clipNotes?}]`).
    /// Each entry find-or-creates the caller's clip by name (first match
    /// wins; invalid entries skip); each member note attaches when the
    /// note exists, is not deleted, and is visible to the importer
    /// (per-row try/catch parity — only storage faults abort). `applied`
    /// counts clips created + notes attached; `skipped` counts invalid
    /// entries + unattachable notes.
    fn apply_clip_import(&self, user_id: &str, text: &str) -> Result<(usize, usize), PortError> {
        let parsed: serde_json::Value = match serde_json::from_str(text) {
            Ok(value) => value,
            Err(_) => return Ok((0, 1)),
        };
        let entries = match parsed.as_array() {
            Some(entries) => entries,
            None => return Ok((0, 1)),
        };
        let mut applied = 0usize;
        let mut skipped = 0usize;
        for entry in entries {
            let Some(map) = entry.as_object() else {
                skipped += 1;
                continue;
            };
            let Some(name) = map.get("name").and_then(|v| v.as_str()) else {
                skipped += 1;
                continue;
            };
            if crate::domain::clip::Clip::validate_name(name).is_err() {
                skipped += 1;
                continue;
            }
            let description = map
                .get("description")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            // Find-or-create by name (upstream user-list import parity:
            // names are not unique, first match wins).
            let clip = match self
                .clips
                .clips_by_owner(user_id)?
                .into_iter()
                .find(|c| c.name == name)
            {
                Some(clip) => clip,
                None => match self.create_clip(
                    user_id,
                    name.to_string(),
                    false,
                    description,
                    None,
                    false,
                    Vec::new(),
                ) {
                    Ok(clip) => {
                        applied += 1;
                        clip
                    }
                    Err(e @ PortError::StorageUnavailable(_)) => return Err(e),
                    Err(_) => {
                        skipped += 1;
                        continue;
                    }
                },
            };
            let members = map
                .get("clipNotes")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            for member in &members {
                let note_id = member
                    .get("note")
                    .and_then(|n| n.get("id"))
                    .and_then(|v| v.as_str())
                    .or_else(|| member.get("noteId").and_then(|v| v.as_str()));
                let Some(note_id) = note_id else {
                    skipped += 1;
                    continue;
                };
                let visible = match self.notes.get_note(note_id)? {
                    Some(note) if !note.is_deleted => self.note_visible_to(&note, user_id)?,
                    _ => false,
                };
                if !visible {
                    skipped += 1;
                    continue;
                }
                match self.add_clip_note(user_id, &clip.id, note_id) {
                    Ok(()) => applied += 1,
                    Err(e @ PortError::StorageUnavailable(_)) => return Err(e),
                    Err(_) => skipped += 1,
                }
            }
        }
        Ok((applied, skipped))
    }

    /// Apply user-list import rows (`listName,acct[,key=value]`, upstream
    /// `ImportUserListsProcessorService` parity): find-or-create the
    /// caller's list by name, resolve the acct locally (remote/unresolvable
    /// rows skip — no remote resolver), skip duplicates, apply
    /// `withReplies=`. Per-row try/catch parity: only storage faults abort
    /// (already-applied rows converge on conflicts on replay).
    fn apply_user_list_import(&self, user_id: &str, text: &str) -> Result<(usize, usize), PortError> {
        let mut applied = 0usize;
        let mut skipped = 0usize;
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let outcome = self.apply_user_list_row(user_id, line);
            match outcome {
                Ok(()) => applied += 1,
                Err(ImportRowOutcome::Skipped) => skipped += 1,
                Err(ImportRowOutcome::Abort(err)) => return Err(err),
            }
        }
        Ok((applied, skipped))
    }

    fn apply_user_list_row(&self, user_id: &str, line: &str) -> Result<(), ImportRowOutcome> {
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() < 2 || parts[0].trim().is_empty() {
            return Err(ImportRowOutcome::Skipped);
        }
        let list_name = parts[0].trim().to_string();
        let acct = parse_acct_cell(parts[1].trim()).ok_or(ImportRowOutcome::Skipped)?;
        let mut with_replies = false;
        for kv in parts.iter().skip(2) {
            let kv = kv.trim();
            if let Some(value) = kv.strip_prefix("withReplies=") {
                with_replies = value.trim() == "true";
            }
        }
        // Find-or-create the caller's list by name (upstream parity; names
        // are not unique, first match wins).
        let list = match self
            .lists
            .lists_by_owner(user_id)
            .map_err(skip_as_skipped)?
            .into_iter()
            .find(|l| l.name == list_name)
        {
            Some(list) => list,
            None => self
                .create_user_list(user_id, list_name)
                .map_err(skip_as_skipped)?,
        };
        let target_id = self
            .resolve_acct(&Some(acct))
            .map_err(skip_as_skipped)?
            .ok_or(ImportRowOutcome::Skipped)?;
        let target = self
            .accounts
            .find_by_id(&target_id)
            .map_err(skip_as_skipped)?
            .ok_or(ImportRowOutcome::Skipped)?;
        if target.id != user_id
            && self
                .blocks
                .is_blocking(&target.id, user_id)
                .map_err(skip_as_skipped)?
        {
            return Err(ImportRowOutcome::Skipped);
        }
        let member = crate::domain::user_list::UserListMembership {
            id: self.ids.generate(),
            list_id: list.id.clone(),
            member_id: target.id.clone(),
            with_replies,
            created_at_ms: self.now_ms(),
        };
        let stream_job = self
            .streaming_job(
                &crate::application::events::StreamingEvent::UserListMemberAdded {
                    list_id: list.id.clone(),
                    member_id: target.id.clone(),
                },
                format!("list-import:{}:{}:{}", list.id, target.id, self.ids.generate()),
            )
            .map_err(skip_as_skipped)?;
        // The op's full push (remote Follow + error mapping) is endpoint
        // shaped; the worker path inserts directly so a remote member's
        // Follow is still queued below.
        match self.lists.add_list_member(member, vec![stream_job]) {
            Ok(_) => {}
            Err(e @ PortError::StorageUnavailable(_)) => {
                return Err(ImportRowOutcome::Abort(e));
            }
            Err(_) => return Err(ImportRowOutcome::Skipped),
        }
        if !target.is_local() {
            if let Ok((proxy, _)) = self.ensure_system_actor() {
                if let Some(inbox_url) = crate::federation::outbox::preferred_inbox(&target) {
                    let follow_id = self.ids.generate();
                    let job = crate::federation::outbox::FederateDeliverPayload::Follow {
                        follow_id: follow_id.clone(),
                        follower_id: proxy.id,
                        followee_id: target.id.clone(),
                        inbox_url,
                    }
                    .into_job(
                        format!("federate:list-import-follow:{}:{follow_id}", list.id),
                        self.now_ms(),
                    )
                    .map_err(skip_as_skipped)?;
                    self.jobs
                        .enqueue_jobs(vec![job])
                        .map_err(skip_as_skipped)?;
                }
            }
        }
        Ok(())
    }
}

/// Per-row outcome mapping: client-level failures (unknown user, dup,
/// block gates, self) skip the row; storage faults abort the job so it
/// retries (already-applied rows converge on conflicts on replay).
enum ImportRowOutcome {
    Skipped,
    Abort(PortError),
}

fn skip_as_skipped(err: PortError) -> ImportRowOutcome {
    match err {
        PortError::StorageUnavailable(_) => ImportRowOutcome::Abort(err),
        _ => ImportRowOutcome::Skipped,
    }
}

fn import_entity_tag(entity: ImportEntity) -> &'static str {
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
fn export_file_name(entity: ExportEntity, now_ms: i64) -> String {
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
fn format_acct(account: &crate::domain::account::Account) -> String {
    match account.host.as_deref() {
        None => account.username.clone(),
        Some(host) => format!("{}@{host}", account.username),
    }
}

/// Parse one `acct` cell: `username`, `user@host`, or `@user@host`
/// (leading `@` tolerated, like Misskey `Acct.parse`). Returns
/// `(username, host)`; empty usernames are `None`.
fn parse_acct_cell(cell: &str) -> Option<(String, Option<String>)> {
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
fn validate_antenna_import(bytes: &[u8]) -> (usize, usize) {
    let text = String::from_utf8_lossy(bytes);
    let parsed: serde_json::Value = match serde_json::from_str(&text) {
        Ok(value) => value,
        Err(_) => return (0, 1),
    };
    let entries = match parsed.as_array() {
        Some(entries) => entries,
        None => return (0, 1),
    };
    let mut valid = 0usize;
    let mut invalid = 0usize;
    for entry in entries {
        let obj = entry.as_object();
        let ok = obj.is_some_and(|map| {
            [
                "name",
                "src",
                "keywords",
                "excludeKeywords",
                "users",
                "caseSensitive",
                "withReplies",
                "withFile",
            ]
            .iter()
            .all(|key| map.contains_key(*key))
        });
        if ok {
            valid += 1;
        } else {
            invalid += 1;
        }
    }
    (valid, invalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_file_names_match_upstream_stems() {
        let name = export_file_name(ExportEntity::Following, 1_700_000_000_000);
        assert!(name.starts_with("following-"), "{name}");
        assert!(name.ends_with(".csv"), "{name}");
        assert!(
            export_file_name(ExportEntity::Muting, 1_700_000_000_000).ends_with(".csv")
        );
        assert!(export_file_name(ExportEntity::Notes, 1_700_000_000_000).ends_with(".json"));
        // `dateFormat` shape: `stem-YYYY-MM-DD-HH-mm-ss.ext`.
        assert_eq!(name, "following-2023-11-14-22-13-20.csv");
    }

    #[test]
    fn acct_cells_parse() {
        assert_eq!(
            parse_acct_cell("alice"),
            Some(("alice".to_string(), None))
        );
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
