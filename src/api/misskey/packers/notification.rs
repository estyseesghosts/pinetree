//! Pack notification (split from `packers.rs`). Pure move.
//!
#![allow(unused_imports)]
//! Misskey response packers (Slice D4).
//!
//! Render domain types into Misskey 2026.10 wire shapes
//! (`misskey/packages/backend/src/models/json-schema/`:
//! `note.ts`, `user.ts`, `drive-file.ts`, `meta.ts`). Field sets are
//! pinned by snapshot tests at the bottom of this file that read the
//! actual checkout schemas from disk. The tests skip (pass vacuously) when
//! the git-ignored `./misskey` checkout is absent, e.g. in clean clones.

use serde_json::{Map, Value};

use crate::activitypub::format_millis;
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::domain::account::{Account, FollowVisibility};
use crate::domain::blocking::Blocking;
use crate::domain::draft::NoteDraft;
use crate::domain::drive_folder::DriveFolder;
use crate::domain::follow::Follow;
use crate::domain::instance::InviteCode;
use crate::domain::media::MediaMeta;
use crate::domain::moderation::{AbuseReport, ModerationLogEntry};
use crate::domain::muting::{Muting, RenoteMuting};
use crate::domain::note::{Note, Visibility};
use crate::domain::notification::{Notification, NotificationKind};
use crate::domain::reaction::Reaction;
use crate::domain::visibility::{ViewerContext, is_visible};
use crate::domain::webhook::SystemWebhook;
use crate::domain::webhook::UserWebhook;

/// Misskey server version we claim compatibility with.

/// Millisecond timestamps as ISO-8601 UTC strings
/// (`"YYYY-MM-DDTHH:MM:SS.sssZ"`).
use super::clip::pack_clip;
use super::note::pack_note;
use super::note_draft::pack_note_draft;
use super::page::pack_page;
use super::user::pack_user_lite;

/// `Notification` pack shared by `i/notifications` and the streaming
/// worker's `main` fan-out (moved from the endpoint module in Phase 3 so
/// both adapters pack identically).
pub fn pack_notification(notification: &Notification, services: &AppServices) -> Value {
    let created_at = services
        .ids
        .parse_time(&notification.id)
        .map(format_millis)
        .unwrap_or_else(|_| format_millis(services.clock.now_ms()));
    let kind = serde_json::to_value(&notification.kind)
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_else(|| "follow".to_string());
    let user = match services.accounts.find_by_id(&notification.notifier_id) {
        Ok(Some(account)) => {
            pack_user_lite(&account, &services.public_url, services.clock.now_ms())
        }
        _ => Value::Null,
    };
    let packed_note = match notification.note_id.as_deref() {
        Some(note_id) => match services.notes.get_note(note_id) {
            Ok(Some(target)) => pack_note(&target, Some(&notification.notifiee_id), services, 0)
                .unwrap_or(Value::Null),
            _ => Value::Null,
        },
        None => Value::Null,
    };
    // Misskey `notification.ts` is a per-type union: `note` exists only on
    // note-bearing variants, `reaction` only on the reaction variant,
    // `draft` only on `scheduledNotePostFailed`.
    let mut obj = serde_json::json!({
        "id": notification.id,
        "createdAt": created_at,
        "type": kind,
        "isRead": notification.is_read,
        "userId": notification.notifier_id,
        "user": user,
    });
    if let Some(map) = obj.as_object_mut() {
        if notification.note_id.is_some() {
            map.insert("note".to_string(), packed_note);
        }
        if let Some(reaction) = notification.reaction.clone() {
            map.insert("reaction".to_string(), Value::String(reaction));
        }
        // §5: `scheduledNotePostFailed` carries the draft that failed to
        // post (upstream `noteDraftId` shape packs the draft packer here).
        // A gone draft packs as `null` (same convention as gone notes).
        if notification.kind
            == crate::domain::notification::NotificationKind::ScheduledNotePostFailed
        {
            let packed_draft = match notification.draft_id.as_deref() {
                Some(draft_id) => match services.notes.get_draft(draft_id) {
                    Ok(Some(draft)) => pack_note_draft(&draft, services),
                    _ => Value::Null,
                },
                None => Value::Null,
            };
            map.insert("draft".to_string(), packed_draft);
        }
        // Phase 2c: `clipUpdated` carries the clip; `pageLiked`
        // carries the page (pinetree-only wire types `clipUpdated` /
        // `pageLiked`, following the `exportCompleted` precedent).
        if notification.kind == crate::domain::notification::NotificationKind::ClipUpdated {
            map.insert(
                "clipId".to_string(),
                notification
                    .clip_id
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            );
            if let Some(clip_id) = notification.clip_id.as_deref() {
                if let Ok(Some(clip)) = services.clips.get_clip(clip_id) {
                    if let Ok(packed) = pack_clip(&clip, Some(&notification.notifiee_id), services)
                    {
                        map.insert("clip".to_string(), packed);
                    }
                }
            }
        }
        if notification.kind == crate::domain::notification::NotificationKind::PageLiked {
            map.insert(
                "pageId".to_string(),
                notification
                    .page_id
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            );
            if let Some(page_id) = notification.page_id.as_deref() {
                if let Ok(Some(page)) = services.pages.get_page(page_id) {
                    if let Ok(packed) = pack_page(&page, Some(&notification.notifiee_id), services)
                    {
                        map.insert("page".to_string(), packed);
                    }
                }
            }
        }
        // §13: `exportCompleted` carries the exported entity + the drive
        // file it landed in (upstream `exportedEntity`/`fileId` shape,
        // `NotificationEntityService.ts:188-191` parity).
        if notification.kind == crate::domain::notification::NotificationKind::ExportCompleted {
            map.insert(
                "exportedEntity".to_string(),
                notification
                    .exported_entity
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            );
            map.insert(
                "fileId".to_string(),
                notification
                    .file_id
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            );
        }
    }
    obj
}
/// Grouped notification packing (§10; Misskey
/// `i/notifications-grouped.ts:106-153` + `packGroupedMany` parity).
/// Pure pack-layer change: no storage change. Input is the newest-first
/// flat page (same fetch as `i/notifications`). Consecutive `reaction`
/// notifications on the same note merge into one `reaction:grouped`
/// (`reactions` accumulate oldest-last, `id` rolls to the oldest member,
/// `createdAt` stays the newest member's — upstream `prev.createdAt` +
/// rolling-`id` parity); consecutive `renote` notifications on the same
/// *target* merge into one `renote:grouped` (`userIds` accumulate,
/// `createdAt` is the oldest member's — upstream `notification.createdAt`
/// parity). Our rows carry no `targetNoteId`, so the target resolves
/// through the renote note's `renote_id`; an unresolvable side never
/// merges and passes through as a plain pack.
pub fn pack_grouped_notifications(
    notifications: &[Notification],
    viewer_id: &str,
    services: &AppServices,
) -> Vec<Value> {
    enum Grouped {
        Plain(Notification),
        Reaction {
            id: String,
            created_at: String,
            note_id: String,
            pairs: Vec<(String, String)>,
        },
        Renote {
            id: String,
            created_at: String,
            note_id: String,
            user_ids: Vec<String>,
        },
    }

    fn created_at_of(notification: &Notification, services: &AppServices) -> String {
        services
            .ids
            .parse_time(&notification.id)
            .map(format_millis)
            .unwrap_or_else(|_| format_millis(services.clock.now_ms()))
    }

    fn renote_target_of(notification: &Notification, services: &AppServices) -> Option<String> {
        let note = services
            .notes
            .get_note(notification.note_id.as_deref()?)
            .ok()??;
        note.renote_id.clone()
    }

    // Mirror of the upstream index loop: `prev = notifications[i - 1]`
    // decides the merge, and the last pushed group (which always ends at
    // `prev`) absorbs the current row.
    let mut grouped: Vec<Grouped> = Vec::new();
    for (i, cur) in notifications.iter().enumerate() {
        if i == 0 {
            grouped.push(Grouped::Plain(cur.clone()));
            continue;
        }
        let prev = &notifications[i - 1];
        let reaction_merge = prev.kind == NotificationKind::Reaction
            && cur.kind == NotificationKind::Reaction
            && prev.note_id.is_some()
            && prev.note_id == cur.note_id;
        if reaction_merge {
            let seed = (
                prev.notifier_id.clone(),
                prev.reaction.clone().unwrap_or_default(),
            );
            let next_pair = (
                cur.notifier_id.clone(),
                cur.reaction.clone().unwrap_or_default(),
            );
            match grouped.pop() {
                Some(Grouped::Reaction {
                    created_at,
                    note_id,
                    mut pairs,
                    ..
                }) => {
                    pairs.push(next_pair);
                    grouped.push(Grouped::Reaction {
                        id: cur.id.clone(),
                        created_at,
                        note_id,
                        pairs,
                    });
                }
                _ => {
                    // The popped tail was `Plain(prev)`: replace it with a
                    // group seeded from `prev`, then absorb `cur`.
                    grouped.push(Grouped::Reaction {
                        id: cur.id.clone(),
                        created_at: created_at_of(prev, services),
                        note_id: prev.note_id.clone().unwrap_or_default(),
                        pairs: vec![seed, next_pair],
                    });
                }
            }
            continue;
        }
        let renote_merge = prev.kind == NotificationKind::Renote
            && cur.kind == NotificationKind::Renote
            && renote_target_of(prev, services).is_some()
            && renote_target_of(prev, services) == renote_target_of(cur, services);
        if renote_merge {
            match grouped.pop() {
                Some(Grouped::Renote {
                    note_id,
                    mut user_ids,
                    ..
                }) => {
                    user_ids.push(cur.notifier_id.clone());
                    grouped.push(Grouped::Renote {
                        id: cur.id.clone(),
                        created_at: created_at_of(cur, services),
                        note_id,
                        user_ids,
                    });
                }
                _ => {
                    grouped.push(Grouped::Renote {
                        id: cur.id.clone(),
                        created_at: created_at_of(cur, services),
                        note_id: prev.note_id.clone().unwrap_or_default(),
                        user_ids: vec![prev.notifier_id.clone(), cur.notifier_id.clone()],
                    });
                }
            }
            continue;
        }
        grouped.push(Grouped::Plain(cur.clone()));
    }

    grouped
        .into_iter()
        .map(|group| match group {
            Grouped::Plain(n) => pack_notification(&n, services),
            Grouped::Reaction {
                id,
                created_at,
                note_id,
                pairs,
            } => pack_grouped_notification(
                &id,
                &created_at,
                "reaction:grouped",
                &note_id,
                &pairs,
                &[],
                viewer_id,
                services,
            ),
            Grouped::Renote {
                id,
                created_at,
                note_id,
                user_ids,
            } => pack_grouped_notification(
                &id,
                &created_at,
                "renote:grouped",
                &note_id,
                &[],
                &user_ids,
                viewer_id,
                services,
            ),
        })
        .collect()
}
/// - `reaction:grouped`: `{id, createdAt, type, note, reactions:
///   [{user: UserLite, reaction}]}` (no top-level `user`/`userId`);
/// - `renote:grouped`: `{id, createdAt, type, note, users: [UserLite]}`.
/// Groups whose note is gone pack `note: null` (upstream returns null for
/// a deleted note and drops the group only when every *user* is gone).
pub fn pack_grouped_notification(
    id: &str,
    created_at: &str,
    kind: &str,
    note_id: &str,
    reaction_pairs: &[(String, String)],
    user_ids: &[String],
    viewer_id: &str,
    services: &AppServices,
) -> Value {
    let packed_note = match services.notes.get_note(note_id) {
        Ok(Some(note)) => pack_note(&note, Some(viewer_id), services, 0).unwrap_or(Value::Null),
        _ => Value::Null,
    };
    let pack_user = |user_id: &str| match services.accounts.find_by_id(user_id) {
        Ok(Some(account)) => Some(pack_user_lite(
            &account,
            &services.public_url,
            services.clock.now_ms(),
        )),
        _ => None,
    };
    match kind {
        "reaction:grouped" => {
            let mut reactions = Vec::new();
            for (user_id, reaction) in reaction_pairs {
                if let Some(user) = pack_user(user_id) {
                    reactions.push(serde_json::json!({
                        "user": user,
                        "reaction": reaction,
                    }));
                }
            }
            serde_json::json!({
                "id": id,
                "createdAt": created_at,
                "type": "reaction:grouped",
                "note": packed_note,
                "reactions": reactions,
            })
        }
        _ => {
            let users: Vec<Value> = user_ids.iter().filter_map(|id| pack_user(id)).collect();
            serde_json::json!({
                "id": id,
                "createdAt": created_at,
                "type": "renote:grouped",
                "note": packed_note,
                "users": users,
            })
        }
    }
}
