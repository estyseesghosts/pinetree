use serde::{Deserialize, Serialize};

use crate::domain::notification::NotificationKind;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamingEvent {
    NoteCreated {
        note_id: String,
    },
    NoteDeleted {
        note_id: String,
    },
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
    PollEnded {
        note_id: String,
    },
    /// A public note entered a role timeline (Misskey
    /// `publishRoleTimelineStream(roleId, 'note', ...)` parity). The
    /// streaming server fans it to `roleTimeline` channels subscribed to
    /// that role. Pinetree has no role model yet, so no command publishes
    /// this today (DIVERGENCE, recorded in `server.rs`); the variant exists
    /// so the channel wiring is complete and testable.
    RoleTimelineNote {
        role_id: String,
        note_id: String,
    },
    /// A drive file changed (Misskey `publishDriveStream(userId, ...)`
    /// parity: `fileCreated`/`fileUpdated`/`fileDeleted`). The `user_id` is
    /// filled at enqueue time (the sink resolves nothing); files are packed
    /// at socket time from the file row (deletes carry only the id).
    DriveFileCreated {
        user_id: String,
        file_id: String,
    },
    DriveFileUpdated {
        user_id: String,
        file_id: String,
    },
    DriveFileDeleted {
        user_id: String,
        file_id: String,
    },
    /// A drive folder changed (same parity; bodies are `{id}` until the
    /// WP2 folder packer lands — recorded in `server.rs`).
    DriveFolderCreated {
        user_id: String,
        folder_id: String,
    },
    DriveFolderUpdated {
        user_id: String,
        folder_id: String,
    },
    DriveFolderDeleted {
        user_id: String,
        folder_id: String,
    },
    /// A clip changed (Misskey `publishClipStream(clipId, ...)` parity:
    /// `updated`/`deleted`, both with a null body).
    ClipUpdated {
        clip_id: String,
    },
    ClipDeleted {
        clip_id: String,
    },
    /// An abuse report was filed (Misskey `publishAdminStream(userId,
    /// 'newAbuseUserReport', ...)` parity). Packed at socket time from the
    /// report row (moderator sockets only).
    AdminReport {
        report_id: String,
    },
    /// A note was edited (§8; Sharkey `publishNoteStream(note.id,
    /// 'updated', ...)` parity). The server arm packs the current note and
    /// emits the `updated` frame with its `{ cw, text }`.
    NoteUpdated {
        note_id: String,
    },
    /// Local `follower_id` unfollowed `followee_id`: the follower's `main`
    /// channel gets `unfollow` with the packed followee (Misskey
    /// `UserFollowingService` parity).
    Unfollowed {
        follower_id: String,
        followee_id: String,
    },
    MeUpdated {
        account_id: String,
    },
    /// One antenna feed gained a note (Batch N WS4): the worker publishes
    /// this post-commit per match; the `antenna` stream channel packs it.
    AntennaNote {
        antenna_id: String,
        note_id: String,
    },
    /// A user-list member was added/removed (Batch O WP16): post-commit
    /// fan-out to the `userList` stream channel (`userAdded`/`userRemoved`
    /// with the packed member).
    UserListMemberAdded {
        list_id: String,
        member_id: String,
    },
    UserListMemberRemoved {
        list_id: String,
        member_id: String,
    },
    /// The account was suspended (Batch M WP3): drop its open `/streaming`
    /// connections (upstream publishes `userChangeSuspendedState`; the
    /// pinned upstream only invalidates caches with it, so the socket drop
    /// is a pinetree hardening — see `docs/endpoint-manifest.md` §23).
    UserSuspended {
        account_id: String,
    },
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
    /// WP4 main-event completion (Misskey `MainEventTypes` parity — every
    /// remaining upstream key is listed here so the `main` channel forwards
    /// it; producers live in the owning commands, see the WP4 report):
    /// a notification arrived but is still unread (badge-count hint).
    UnreadNotification,
    /// an antenna feed gained a note.
    UnreadAntenna,
    /// the antenna's notes were read.
    ReadAntenna,
    /// every announcement was marked read.
    ReadAllAnnouncements,
    /// an announcement was created (also broadcast `announcementCreated`).
    AnnouncementCreated,
    /// the caller's token was regenerated (re-login hint).
    MyTokenRegenerated,
    /// a new signin session was recorded.
    Signin,
    /// a registry key changed.
    RegistryUpdated,
    /// a drive file finished uploading.
    DriveFileCreated,
    /// a remote-URL upload finished (`marker` echoes the caller's marker).
    UrlUploadFinished,
    /// a page fired an event.
    PageEvent,
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
            MainEventKind::UnreadNotification => "unreadNotification",
            MainEventKind::UnreadAntenna => "unreadAntenna",
            MainEventKind::ReadAntenna => "readAntenna",
            MainEventKind::ReadAllAnnouncements => "readAllAnnouncements",
            MainEventKind::AnnouncementCreated => "announcementCreated",
            MainEventKind::MyTokenRegenerated => "myTokenRegenerated",
            MainEventKind::Signin => "signin",
            MainEventKind::RegistryUpdated => "registryUpdated",
            MainEventKind::DriveFileCreated => "driveFileCreated",
            MainEventKind::UrlUploadFinished => "urlUploadFinished",
            MainEventKind::PageEvent => "pageEvent",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wp4_main_event_wire_names_match_upstream() {
        let cases = [
            (MainEventKind::UnreadNotification, "unreadNotification"),
            (MainEventKind::UnreadAntenna, "unreadAntenna"),
            (MainEventKind::ReadAntenna, "readAntenna"),
            (MainEventKind::ReadAllAnnouncements, "readAllAnnouncements"),
            (MainEventKind::AnnouncementCreated, "announcementCreated"),
            (MainEventKind::MyTokenRegenerated, "myTokenRegenerated"),
            (MainEventKind::Signin, "signin"),
            (MainEventKind::RegistryUpdated, "registryUpdated"),
            (MainEventKind::DriveFileCreated, "driveFileCreated"),
            (MainEventKind::UrlUploadFinished, "urlUploadFinished"),
            (MainEventKind::PageEvent, "pageEvent"),
        ];
        for (kind, wire) in cases {
            assert_eq!(kind.wire_name(), wire);
            // Serde renames stay camelCase (durable job payload compat).
            let json = serde_json::to_string(&kind).unwrap();
            assert_eq!(json, format!("\"{wire}\""));
        }
    }

    #[test]
    fn wp4_streaming_events_roundtrip() {
        let events = [
            StreamingEvent::RoleTimelineNote {
                role_id: "r".to_string(),
                note_id: "n".to_string(),
            },
            StreamingEvent::DriveFileCreated {
                user_id: "u".to_string(),
                file_id: "f".to_string(),
            },
            StreamingEvent::DriveFolderDeleted {
                user_id: "u".to_string(),
                folder_id: "d".to_string(),
            },
            StreamingEvent::ClipUpdated {
                clip_id: "c".to_string(),
            },
            StreamingEvent::ClipDeleted {
                clip_id: "c".to_string(),
            },
            StreamingEvent::AdminReport {
                report_id: "a".to_string(),
            },
        ];
        for event in events {
            let bytes = serde_json::to_vec(&event).unwrap();
            let back: StreamingEvent = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(back, event);
        }
    }
}
