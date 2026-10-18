use serde::{Deserialize, Serialize};

use crate::domain::notification::NotificationKind;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamingEvent {
    NoteCreated { note_id: String },
    NoteDeleted { note_id: String },
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
    /// A poll vote was recorded (Misskey note-stream `pollVoted` parity:
    /// `{ choice, userId }`).
    PollVoted {
        note_id: String,
        user_id: String,
        choice: usize,
    },
    /// A poll reached its expiry (Misskey note-stream `pollEnded` parity).
    PollEnded { note_id: String },
    /// A note was edited (§8; Sharkey `publishNoteStream(note.id,
    /// 'updated', ...)` parity). The server arm packs the current note and
    /// emits the `updated` frame with its `{ cw, text }`.
    NoteUpdated { note_id: String },
    /// Local `follower_id` unfollowed `followee_id`: the follower's `main`
    /// channel gets `unfollow` with the packed followee (Misskey
    /// `UserFollowingService` parity).
    Unfollowed {
        follower_id: String,
        followee_id: String,
    },
    MeUpdated { account_id: String },
    /// The account was suspended (Batch M WP3): drop its open `/streaming`
    /// connections (upstream publishes `userChangeSuspendedState`; the
    /// pinned upstream only invalidates caches with it, so the socket drop
    /// is a pinetree hardening — see `docs/endpoint-manifest.md` §23).
    UserSuspended { account_id: String },
}

/// A `main`-channel event for one user (Phase 3 streaming). Built by the
/// job worker (which can pack bodies through `AppServices`) and fanned out
/// by the streaming hub to that user's `main` channels.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MainMessage {
    pub user_id: String,
    pub kind: MainEventKind,
    pub body: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MainEventKind {
    Notification,
    Mention,
    Reply,
    Renote,
    /// Sharkey `edited` main-stream event (the packed edited note).
    Edited,
    Follow,
    Followed,
    Unfollow,
    ReceiveFollowRequest,
    MeUpdated,
    ReadAllNotifications,
}

impl MainEventKind {
    pub fn wire_name(&self) -> &'static str {
        match self {
            MainEventKind::Notification => "notification",
            MainEventKind::Mention => "mention",
            MainEventKind::Reply => "reply",
            MainEventKind::Renote => "renote",
            MainEventKind::Edited => "edited",
            MainEventKind::Follow => "follow",
            MainEventKind::Followed => "followed",
            MainEventKind::Unfollow => "unfollow",
            MainEventKind::ReceiveFollowRequest => "receiveFollowRequest",
            MainEventKind::MeUpdated => "meUpdated",
            MainEventKind::ReadAllNotifications => "readAllNotifications",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotifyJobPayload {
    pub notifiee_id: String,
    pub notifier_id: String,
    pub kind: NotificationKind,
    pub note_id: Option<String>,
    pub reaction: Option<String>,
    /// Drive file an `ExportCompleted` notification reports (`fileId`).
    /// `None` on every other kind. `#[serde(default)]` keeps in-flight
    /// payloads written before §13 decodable.
    #[serde(default)]
    pub file_id: Option<String>,
    /// Entity an `ExportCompleted` notification reports (`exportedEntity`).
    /// `None` on every other kind. `#[serde(default)]` keeps in-flight
    /// payloads written before §13 decodable.
    #[serde(default)]
    pub exported_entity: Option<String>,
}
