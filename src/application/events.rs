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
    /// One antenna feed gained a note (Batch N WS4): the worker publishes
    /// this post-commit per match; the `antenna` stream channel packs it.
    AntennaNote { antenna_id: String, note_id: String },
    /// A user-list member was added/removed (Batch O WP16): post-commit
    /// fan-out to the `userList` stream channel (`userAdded`/`userRemoved`
    /// with the packed member).
    UserListMemberAdded { list_id: String, member_id: String },
    UserListMemberRemoved { list_id: String, member_id: String },
    /// The account was suspended (Batch M WP3): drop its open `/streaming`
    /// connections (upstream publishes `userChangeSuspendedState`; the
    /// pinned upstream only invalidates caches with it, so the socket drop
    /// is a pinetree hardening — see `docs/endpoint-manifest.md` §23).
    UserSuspended { account_id: String },
    /// Custom-emoji broadcast (Misskey `emojiAdded`/`emojiUpdated`/
    /// `emojiDeleted` parity): emitted by the admin emoji ops. Carries
    /// domain snapshots (deletes snapshot before the row is gone); the
    /// worker packs them to `EmojiDetailed` and pushes them to the hub —
    /// the application layer never touches wire shapes.
    EmojiBroadcast {
        op: EmojiBroadcastOp,
        emojis: Vec<crate::domain::emoji::CustomEmoji>,
    },
}

/// Which broadcast-stream emoji event an [`StreamingEvent::EmojiBroadcast`]
/// carries (Misskey `GlobalEventService` parity: `emojiAdded` carries
/// `{emoji}`, `emojiUpdated`/`emojiDeleted` carry `{emojis}`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EmojiBroadcastOp {
    Added,
    Updated,
    Deleted,
}

impl EmojiBroadcastOp {
    pub fn wire_name(&self) -> &'static str {
        match self {
            EmojiBroadcastOp::Added => "emojiAdded",
            EmojiBroadcastOp::Updated => "emojiUpdated",
            EmojiBroadcastOp::Deleted => "emojiDeleted",
        }
    }
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
    /// Batch P `notifications/flush` parity: upstream `flushAllNotifications`
    /// emits `notificationFlushed` (distinct from `readAllNotifications`).
    NotificationFlushed,
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
            MainEventKind::NotificationFlushed => "notificationFlushed",
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
    #[serde(default)]
    pub clip_id: Option<String>,
    #[serde(default)]
    pub page_id: Option<String>,
}
