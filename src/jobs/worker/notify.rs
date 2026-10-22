use super::JobWorker;
use crate::api::misskey::packers::{pack_note, pack_notification, pack_user_detailed_not_me};
use crate::application::events::{MainEventKind, MainMessage, NotifyJobPayload};
use crate::application::ports::PortError;
use crate::domain::notification::{Notification, NotificationKind};

impl JobWorker {
    pub(crate) fn handle_notify(
        &self,
        payload: &[u8],
        idempotency_key: &str,
    ) -> Result<(), PortError> {
        let notify: NotifyJobPayload = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("notify payload invalid: {e}")))?;
        // §11 creation-time receive gate (Misskey `NotificationService`
        // `#createNotificationInternal` parity): self-notify, active mutes,
        // and `notificationRecieveConfig` drop the notification here — the
        // single choke point every local and inbound `Notify` job flows
        // through. Drops converge quietly (job completes, no row, no hints);
        // storage faults still retry. Scheduler-direct inserts bypass this
        // gate like upstream notifier-less notifications do.
        //
        // §13 `exportCompleted` is notifier-less upstream (no `notifierId`,
        // so neither the self-notify nor the mute check applies — only a
        // `never` receive-config drops it). Pinetree rows require a
        // notifier (the exporter), so this kind takes the dedicated
        // notifier-less gate instead. Batch P `test` rides the same gate
        // (upstream `createNotification(user.id, 'test', {})` is equally
        // notifier-less), as does §4 `pollEnded` (upstream notifies with
        // no notifier; the row carries the author as a placeholder).
        let allowed = if notify.kind == NotificationKind::ExportCompleted
            || notify.kind == NotificationKind::Test
            || notify.kind == NotificationKind::PollEnded
        {
            self.services
                .system_notification_allowed(&notify.notifiee_id, notify.kind)?
        } else if !self.services.notification_delivery_allowed(
            &notify.notifiee_id,
            notify.kind,
            &notify.notifier_id,
        )? {
            return Ok(());
        } else {
            true
        };
        if !allowed {
            return Ok(());
        }
        let id = if idempotency_key.trim().is_empty() {
            self.ids.generate()
        } else {
            idempotency_key.to_string()
        };
        let notification = Notification {
            id,
            notifiee_id: notify.notifiee_id.clone(),
            notifier_id: notify.notifier_id.clone(),
            kind: notify.kind,
            note_id: notify.note_id.clone(),
            reaction: notify.reaction.clone(),
            draft_id: None,
            file_id: notify.file_id.clone(),
            exported_entity: notify.exported_entity.clone(),
            clip_id: notify.clip_id.clone(),
            page_id: notify.page_id.clone(),
            is_read: false,
        };
        let stored = match self.notifications.insert_notification(notification) {
            Ok(stored) => stored,
            // Already inserted under this idempotency key: the live hints
            // below already went out on the first pass, so a replay only
            // completes instead of duplicating them.
            Err(PortError::Conflict(_)) => return Ok(()),
            Err(e) => return Err(e),
        };
        // Fresh insert only: fan the `main`-channel hints out through the
        // streaming sink (a no-op when streaming is disabled). Hints are
        // at-most-once per connection by design (see `streaming::hub`).
        self.push_notify_mains(&notify, &stored)
    }

    /// Expand one stored notification into `main`-channel events (Misskey
    /// `NotificationService` + `UserFollowingService` parity): every kind
    /// emits `notification`; Mention/Reply/Renote also emit the packed note
    /// under their own event name; Follow emits `follow` (to the follower,
    /// packed followee) and `followed` (to the followee, packed follower);
    /// ReceiveFollowRequest emits `receiveFollowRequest` (packed follower).
    fn push_notify_mains(
        &self,
        notify: &NotifyJobPayload,
        stored: &Notification,
    ) -> Result<(), PortError> {
        let services = &self.services;
        let notifiee = notify.notifiee_id.as_str();
        let notifier = notify.notifier_id.as_str();
        let packed_notification = pack_notification(stored, services);
        services.streaming.push_main(MainMessage {
            user_id: notifiee.to_string(),
            kind: MainEventKind::Notification,
            body: packed_notification.clone(),
        })?;
        // Every stored notification also nudges the badge counter
        // (Misskey `unreadNotification` parity): the body is the same
        // packed notification the `notification` event carries.
        services.streaming.push_main(MainMessage {
            user_id: notifiee.to_string(),
            kind: MainEventKind::UnreadNotification,
            body: packed_notification,
        })?;
        let packed_note = |note_id: &str| -> Result<Option<serde_json::Value>, PortError> {
            match services.notes.get_note(note_id)? {
                Some(note) => Ok(Some(pack_note(&note, Some(notifiee), services, 0)?)),
                None => Ok(None),
            }
        };
        let packed_user =
            |account_id: &str, viewer: &str| -> Result<Option<serde_json::Value>, PortError> {
                match services.accounts.find_by_id(account_id)? {
                    Some(account) => Ok(Some(pack_user_detailed_not_me(
                        &account,
                        Some(viewer),
                        services,
                    )?)),
                    None => Ok(None),
                }
            };
        match notify.kind {
            NotificationKind::Mention
            | NotificationKind::Reply
            | NotificationKind::Renote
            | NotificationKind::Edited => {
                let kind = match notify.kind {
                    NotificationKind::Mention => MainEventKind::Mention,
                    NotificationKind::Reply => MainEventKind::Reply,
                    NotificationKind::Edited => MainEventKind::Edited,
                    _ => MainEventKind::Renote,
                };
                if let Some(note_id) = notify.note_id.as_deref() {
                    if let Some(packed) = packed_note(note_id)? {
                        services.streaming.push_main(MainMessage {
                            user_id: notifiee.to_string(),
                            kind,
                            body: packed,
                        })?;
                    }
                }
            }
            NotificationKind::Follow => {
                if let Some(packed) = packed_user(notifiee, notifier)? {
                    services.streaming.push_main(MainMessage {
                        user_id: notifier.to_string(),
                        kind: MainEventKind::Follow,
                        body: packed,
                    })?;
                }
                if let Some(packed) = packed_user(notifier, notifiee)? {
                    services.streaming.push_main(MainMessage {
                        user_id: notifiee.to_string(),
                        kind: MainEventKind::Followed,
                        body: packed,
                    })?;
                }
            }
            NotificationKind::ReceiveFollowRequest => {
                if let Some(packed) = packed_user(notifier, notifiee)? {
                    services.streaming.push_main(MainMessage {
                        user_id: notifiee.to_string(),
                        kind: MainEventKind::ReceiveFollowRequest,
                        body: packed,
                    })?;
                }
            }
            NotificationKind::Reaction => {}
            // Scheduling notifications (§5) fan out as the plain
            // `notification` hint above (no note/user side-channel: the
            // posted note / failed draft rides in the packed body).
            // `exportCompleted` (§13) likewise: the drive file rides in the
            // packed body (`fileId`). `followRequestAccepted` likewise:
            // the packed body carries the acceptor. `pollEnded` (§4)
            // likewise: the expired note rides in the packed body.
            NotificationKind::FollowRequestAccepted
            | NotificationKind::ScheduledNotePosted
            | NotificationKind::ScheduledNotePostFailed
            | NotificationKind::ExportCompleted
            | NotificationKind::PollEnded
            | NotificationKind::ClipUpdated
            | NotificationKind::PageLiked
            | NotificationKind::Test => {}
        }
        Ok(())
    }
}
