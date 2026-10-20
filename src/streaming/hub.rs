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
    /// A captured note was edited: subscribers refetch it.
    NoteUpdated { note_id: String },
    /// An antenna feed gained a note (Batch N WS4).
    AntennaNote { antenna_id: String, note_id: String },
    /// A note was deleted.
    NoteDeleted { note_id: String },
    /// A `main`-channel event for one user.
    Main(MainMessage),
    /// The account was suspended: connections authenticated as it close
    /// (Batch M WP3). Anonymous sockets and other users' sockets ignore it.
    UserSuspended { account_id: String },
}

/// Process-wide broadcast hub. `send` is synchronous and never blocks a
/// job worker: with no receivers, or when every receiver lagged, the
/// message is dropped (live hints are at-most-once by design).
#[derive(Debug, Clone)]
pub struct Hub {
    tx: broadcast::Sender<HubMessage>,
}

impl Hub {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<HubMessage> {
        self.tx.subscribe()
    }

    pub fn receiver_count(&self) -> usize {
        self.tx.receiver_count()
    }

    pub fn send(&self, msg: HubMessage) {
        let _ = self.tx.send(msg);
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
            StreamingEvent::NoteUpdated { note_id } => {
                self.hub.send(HubMessage::NoteUpdated { note_id });
            }
            // Batch N WS4: an antenna feed gained a note (published
            // post-commit by the match worker, one per match).
            StreamingEvent::AntennaNote { antenna_id, note_id } => {
                self.hub.send(HubMessage::AntennaNote { antenna_id, note_id });
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
