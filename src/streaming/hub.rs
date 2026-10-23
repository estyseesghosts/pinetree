//! In-process streaming fan-out (Phase 3, single node).
//!
//! Commands never touch sockets: they commit state + durable
//! `StreamingPublish` / `Notify` jobs in one transaction, and the job worker
//! translates those jobs into [`HubMessage`]s on the [`Hub`]. WebSocket
//! connections subscribe to the hub, filter per channel, pack through
//! `AppServices`, and send. Delivery is at-most-once per connection (a
//! lagging connection drops the backlog and resubscribes, like the
//! Misskey redis fan-out it mirrors).

use std::sync::Arc;

use serde_json::Value;
use tokio::sync::broadcast;

use crate::application::events::{MainMessage, StreamingEvent};
use crate::application::ports::{PortError, StreamingSink};

/// One broadcast event. Note bodies are NOT packed here: each connection
/// packs for its own viewer (`myReaction`, visibility), exactly like
/// Misskey's per-connection `NoteEntityService.pack`.
#[derive(Debug, Clone)]
pub enum HubMessage {
    /// A note was created or federated in: timeline channels refetch it.
    /// Also drives `main` mention/reply/renote matching.
    Note { note_id: String },
    /// A reaction was added or removed.
    NoteReacted {
        note_id: String,
        user_id: String,
        reaction: String,
    },
    NoteUnreacted {
        note_id: String,
        user_id: String,
        reaction: String,
    },
    /// A poll vote was recorded on a captured note.
    PollVoted {
        note_id: String,
        user_id: String,
        choice: usize,
    },
    /// A captured note's poll expired.
    PollEnded { note_id: String },
    /// A public note entered a role timeline (fanned to `roleTimeline`
    /// channels subscribed to that role; no producer yet — see
    /// [`StreamingEvent::RoleTimelineNote`]).
    RoleTimelineNote { role_id: String, note_id: String },
    /// A drive file changed (fanned to the owner's `drive` channels;
    /// packed at socket time, deletes carry only the id).
    DriveFile {
        user_id: String,
        event: DriveFileEvent,
        file_id: String,
    },
    /// A drive folder changed (fanned to the owner's `drive` channels;
    /// bodies are `{id}` until the WP2 folder packer lands).
    DriveFolder {
        user_id: String,
        event: DriveFolderEvent,
        folder_id: String,
    },
    /// A clip changed (fanned to `clip` channels for that clip; both
    /// events carry a null body, upstream parity).
    Clip { clip_id: String, deleted: bool },
    /// An abuse report was filed (fanned to every `admin` channel —
    /// connect already requires moderator powers; packed at socket time
    /// from the report row).
    AdminReport { report_id: String },
    /// A captured note was edited: subscribers refetch it.
    NoteUpdated { note_id: String },
    /// An antenna feed gained a note (Batch N WS4).
    AntennaNote { antenna_id: String, note_id: String },
    /// A user-list member was added/removed (Batch O WP16): the `userList`
    /// channel emits `userAdded`/`userRemoved` with the packed member.
    UserListMember {
        list_id: String,
        member_id: String,
        added: bool,
    },
    /// A note was deleted.
    NoteDeleted { note_id: String },
    /// A `main`-channel event for one user.
    Main(MainMessage),
    /// A custom-emoji broadcast for every connection (Misskey
    /// `publishBroadcastStream('emojiAdded|Updated|Deleted')` parity):
    /// `event` is the wire name, `body` the `{emoji}`/`{emojis}` object.
    EmojiBroadcast { event: String, body: Value },
    /// The account was suspended: connections authenticated as it close
    /// (Batch M WP3). Anonymous sockets and other users' sockets ignore it.
    UserSuspended { account_id: String },
}

/// One drive-file stream event (Misskey `DriveEventTypes` parity).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriveFileEvent {
    Created,
    Updated,
    Deleted,
}

impl DriveFileEvent {
    pub fn wire_name(&self) -> &'static str {
        match self {
            DriveFileEvent::Created => "fileCreated",
            DriveFileEvent::Updated => "fileUpdated",
            DriveFileEvent::Deleted => "fileDeleted",
        }
    }
}

/// One drive-folder stream event (Misskey `DriveEventTypes` parity).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriveFolderEvent {
    Created,
    Updated,
    Deleted,
}

impl DriveFolderEvent {
    pub fn wire_name(&self) -> &'static str {
        match self {
            DriveFolderEvent::Created => "folderCreated",
            DriveFolderEvent::Updated => "folderUpdated",
            DriveFolderEvent::Deleted => "folderDeleted",
        }
    }
}

/// Process-wide broadcast hub. `send` is synchronous and never blocks a
/// job worker: with no receivers, or when every receiver lagged, the
/// message is dropped (live hints are at-most-once by design).
///
/// Batch P: the hub also carries streaming presence (`account_id → last
/// seen ms`) behind [`ONLINE_THRESHOLD_MS`], backing
/// `get-online-users-count`. Upstream counts `lastActiveDate` heartbeats
/// (any API activity); pinetree has no such column, so "online" is defined
/// honestly as *a live authenticated streaming connection seen within the
/// threshold* (DIVERGENCE, documented in the manifest).
#[derive(Debug, Clone)]
pub struct Hub {
    tx: broadcast::Sender<HubMessage>,
    presence: Arc<std::sync::Mutex<std::collections::HashMap<String, i64>>>,
}

/// Online threshold for [`Hub::online_count`]: Misskey
/// `USER_ONLINE_THRESHOLD` parity (10 minutes).
pub const ONLINE_THRESHOLD_MS: i64 = 10 * 60 * 1000;

impl Hub {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self {
            tx,
            presence: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<HubMessage> {
        self.tx.subscribe()
    }

    pub fn receiver_count(&self) -> usize {
        self.tx.receiver_count()
    }

    pub fn send(&self, msg: HubMessage) {
        // At-most-once by design (see the `Hub` docs): a broadcast with
        // no receivers, or one every receiver lagged, is dropped. The
        // drop is counted so it is not invisible; delivery policy and
        // the non-blocking contract are unchanged.
        if self.tx.send(msg).is_err() {
            crate::observability::record_error("streaming.hub_dropped");
        }
    }

    /// Record a live authenticated streaming connection for `account_id`
    /// at `now_ms` (called on connect and on every inbound socket message).
    pub fn mark_present(&self, account_id: &str, now_ms: i64) {
        if let Ok(mut presence) = self.presence.lock() {
            presence.insert(account_id.to_string(), now_ms);
        }
    }

    /// Forget one account's presence (called when its socket closes).
    /// Entries are also lazily pruned by [`Hub::online_count`], so a missed
    /// close only over-counts until the threshold lapses.
    pub fn mark_gone(&self, account_id: &str) {
        if let Ok(mut presence) = self.presence.lock() {
            presence.remove(account_id);
        }
    }

    /// Distinct accounts with a live streaming connection seen within
    /// `threshold_ms` of `now_ms`; stale entries are pruned.
    pub fn online_count(&self, now_ms: i64, threshold_ms: i64) -> usize {
        let Ok(mut presence) = self.presence.lock() else {
            return 0;
        };
        presence.retain(|_, seen| now_ms.saturating_sub(*seen) < threshold_ms);
        presence.len()
    }
}

/// [`StreamingSink`] backed by a [`Hub`]: the durable-job worker calls
/// `publish` with a [`StreamingEvent`] JSON payload (enqueued atomically
/// by the commands) and `push_main` with packed [`MainMessage`]s.
#[derive(Debug, Clone)]
pub struct BroadcastStreamingSink {
    hub: Hub,
}

impl BroadcastStreamingSink {
    pub fn new(hub: Hub) -> Self {
        Self { hub }
    }

    pub fn hub(&self) -> &Hub {
        &self.hub
    }
}

impl StreamingSink for BroadcastStreamingSink {
    fn publish(&self, _event_type: &str, payload: &[u8]) -> Result<(), PortError> {
        let event: StreamingEvent = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("streaming payload invalid: {e}")))?;
        match event {
            StreamingEvent::NoteCreated { note_id } => self.hub.send(HubMessage::Note { note_id }),
            StreamingEvent::NoteDeleted { note_id } => {
                self.hub.send(HubMessage::NoteDeleted { note_id });
            }
            StreamingEvent::NoteReacted {
                note_id,
                user_id,
                reaction,
            } => self.hub.send(HubMessage::NoteReacted {
                note_id,
                user_id,
                reaction,
            }),
            StreamingEvent::NoteUnreacted {
                note_id,
                user_id,
                reaction,
            } => self.hub.send(HubMessage::NoteUnreacted {
                note_id,
                user_id,
                reaction,
            }),
            StreamingEvent::PollVoted {
                note_id,
                user_id,
                choice,
            } => self.hub.send(HubMessage::PollVoted {
                note_id,
                user_id,
                choice,
            }),
            StreamingEvent::PollEnded { note_id } => {
                self.hub.send(HubMessage::PollEnded { note_id });
            }
            StreamingEvent::RoleTimelineNote { role_id, note_id } => {
                self.hub
                    .send(HubMessage::RoleTimelineNote { role_id, note_id });
            }
            StreamingEvent::DriveFileCreated { user_id, file_id } => {
                self.hub.send(HubMessage::DriveFile {
                    user_id,
                    event: DriveFileEvent::Created,
                    file_id,
                });
            }
            StreamingEvent::DriveFileUpdated { user_id, file_id } => {
                self.hub.send(HubMessage::DriveFile {
                    user_id,
                    event: DriveFileEvent::Updated,
                    file_id,
                });
            }
            StreamingEvent::DriveFileDeleted { user_id, file_id } => {
                self.hub.send(HubMessage::DriveFile {
                    user_id,
                    event: DriveFileEvent::Deleted,
                    file_id,
                });
            }
            StreamingEvent::DriveFolderCreated { user_id, folder_id } => {
                self.hub.send(HubMessage::DriveFolder {
                    user_id,
                    event: DriveFolderEvent::Created,
                    folder_id,
                });
            }
            StreamingEvent::DriveFolderUpdated { user_id, folder_id } => {
                self.hub.send(HubMessage::DriveFolder {
                    user_id,
                    event: DriveFolderEvent::Updated,
                    folder_id,
                });
            }
            StreamingEvent::DriveFolderDeleted { user_id, folder_id } => {
                self.hub.send(HubMessage::DriveFolder {
                    user_id,
                    event: DriveFolderEvent::Deleted,
                    folder_id,
                });
            }
            StreamingEvent::ClipUpdated { clip_id } => {
                self.hub.send(HubMessage::Clip {
                    clip_id,
                    deleted: false,
                });
            }
            StreamingEvent::ClipDeleted { clip_id } => {
                self.hub.send(HubMessage::Clip {
                    clip_id,
                    deleted: true,
                });
            }
            StreamingEvent::AdminReport { report_id } => {
                self.hub.send(HubMessage::AdminReport { report_id });
            }
            StreamingEvent::NoteUpdated { note_id } => {
                self.hub.send(HubMessage::NoteUpdated { note_id });
            }
            // Batch N WS4: an antenna feed gained a note (published
            // post-commit by the match worker, one per match).
            StreamingEvent::AntennaNote {
                antenna_id,
                note_id,
            } => {
                self.hub.send(HubMessage::AntennaNote {
                    antenna_id,
                    note_id,
                });
            }
            // Batch O WP16: list membership changes fan out to the
            // `userList` channel post-commit (`userAdded`/`userRemoved`).
            StreamingEvent::UserListMemberAdded { list_id, member_id } => {
                self.hub.send(HubMessage::UserListMember {
                    list_id,
                    member_id,
                    added: true,
                });
            }
            StreamingEvent::UserListMemberRemoved { list_id, member_id } => {
                self.hub.send(HubMessage::UserListMember {
                    list_id,
                    member_id,
                    added: false,
                });
            }
            // The bare `MeUpdated` marker wakes `main` channels, which pack
            // `MeDetailed` on receipt (per-connection token scoping).
            StreamingEvent::MeUpdated { account_id } => {
                self.hub.send(HubMessage::Main(MainMessage {
                    user_id: account_id,
                    kind: crate::application::events::MainEventKind::MeUpdated,
                    body: serde_json::Value::Null,
                }));
            }
            // WP3 stream close: each connection whose viewer is the
            // suspended account drops its socket (`streaming::server`);
            // everyone else's connections ignore the event.
            StreamingEvent::UserSuspended { account_id } => {
                self.hub.send(HubMessage::UserSuspended { account_id });
            }
            // Custom-emoji broadcasts never reach the hub through the
            // worker: the worker intercepts them first to pack the
            // `EmojiDetailed` bodies (it has `AppServices`, the sink does
            // not). This arm exists only so a direct `publish` cannot
            // vanish silently — it is diagnosed instead of delivered.
            StreamingEvent::EmojiBroadcast { op, emojis } => {
                eprintln!(
                    "streaming: EmojiBroadcast({:?}) reached the hub without worker expansion \
                     ({} snapshots); dropping",
                    op,
                    emojis.len()
                );
            }
            // `Unfollowed` never reaches the hub through the worker: the
            // worker intercepts it first to pack the followee (it needs
            // `AppServices`, which the sink does not have). This arm exists
            // only so a direct `publish` cannot vanish silently — it is
            // diagnosed instead of delivered.
            StreamingEvent::Unfollowed {
                follower_id,
                followee_id,
            } => {
                eprintln!(
                    "streaming: Unfollowed reached the hub without worker expansion \
                     (follower {follower_id}, followee {followee_id}); dropping"
                );
            }
        }
        Ok(())
    }

    fn push_main(&self, msg: MainMessage) -> Result<(), PortError> {
        self.hub.send(HubMessage::Main(msg));
        Ok(())
    }

    fn push_broadcast(&self, event: &str, body: serde_json::Value) -> Result<(), PortError> {
        self.hub.send(HubMessage::EmojiBroadcast {
            event: event.to_string(),
            body,
        });
        Ok(())
    }
}

/// Build the production pair: one hub, one sink on it.
pub fn hub_and_sink(capacity: usize) -> (Hub, Arc<dyn StreamingSink>) {
    let hub = Hub::new(capacity);
    let sink: Arc<dyn StreamingSink> = Arc::new(BroadcastStreamingSink::new(hub.clone()));
    (hub, sink)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::events::MainEventKind;

    #[test]
    fn publish_routes_streaming_events_to_hub() {
        let (hub, sink) = hub_and_sink(16);
        let mut rx = hub.subscribe();
        let event = StreamingEvent::NoteCreated {
            note_id: "n1".to_string(),
        };
        sink.publish("streaming", &serde_json::to_vec(&event).unwrap())
            .unwrap();
        match rx.try_recv().unwrap() {
            HubMessage::Note { note_id } => assert_eq!(note_id, "n1"),
            other => panic!("unexpected {other:?}"),
        }
        sink.push_main(MainMessage {
            user_id: "u1".to_string(),
            kind: MainEventKind::MeUpdated,
            body: serde_json::Value::Null,
        })
        .unwrap();
        match rx.try_recv().unwrap() {
            HubMessage::Main(msg) => {
                assert_eq!(msg.user_id, "u1");
                assert_eq!(msg.kind, MainEventKind::MeUpdated);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn user_suspended_event_routes_to_hub() {
        let (hub, sink) = hub_and_sink(16);
        let mut rx = hub.subscribe();
        let event = StreamingEvent::UserSuspended {
            account_id: "u9".to_string(),
        };
        sink.publish("streaming", &serde_json::to_vec(&event).unwrap())
            .unwrap();
        match rx.try_recv().unwrap() {
            HubMessage::UserSuspended { account_id } => assert_eq!(account_id, "u9"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn publish_rejects_garbage_payloads() {
        let (_, sink) = hub_and_sink(16);
        let err = sink.publish("streaming", b"not json").unwrap_err();
        assert!(matches!(err, PortError::Validation(_)));
    }

    #[test]
    fn send_without_receivers_is_not_an_error() {
        let (hub, sink) = hub_and_sink(16);
        assert_eq!(hub.receiver_count(), 0);
        let event = StreamingEvent::NoteDeleted {
            note_id: "n9".to_string(),
        };
        sink.publish("streaming", &serde_json::to_vec(&event).unwrap())
            .unwrap();
    }
}
