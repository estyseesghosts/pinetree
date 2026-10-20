//! Notification entity: pure data (no I/O).

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NotificationKind {
    Follow,
    ReceiveFollowRequest,
    Mention,
    Reply,
    Renote,
    Reaction,
    /// A scheduled draft posted successfully (Misskey
    /// `scheduledNotePosted`: carries the posted note).
    ScheduledNotePosted,
    /// A scheduled draft failed to post (Misskey
    /// `scheduledNotePostFailed`: carries the draft).
    ScheduledNotePostFailed,
    /// A note was edited (Sharkey `edited` notification: carries the edited
    /// note; sent to the reply author on `notes/edit`). Wire type `edited`
    /// (a Sharkey addition — Misskey `notificationTypes` has no such entry).
    Edited,
    /// A locked account accepted a follow request (Misskey
    /// `followRequestAccepted`: emitted to the requester on accept).
    FollowRequestAccepted,
    /// A profile export finished (Misskey `exportCompleted`: carries
    /// `exportedEntity` + `fileId`, no notifier — see
    /// `NotificationEntityService.ts:188-191`). Pinetree rows require a
    /// notifier, so the exporter's own id is stored (self-notify pack
    /// shape, same convention as §5 scheduling notifications); the
    /// `Notify` worker treats this kind as notifier-less (only a `never`
    /// receive-config drops it, like upstream).
    ExportCompleted,
    /// Test notification (`notifications/test-notification`: Misskey
    /// `NotificationService.createNotification(userId, 'test', {})`).
    /// Self-notified through the normal `Notify` path so type filters and
    /// grouped rendering exercise it like any other kind.
    Test,
    /// A followed clip's metadata changed (pinetree-only wire
    /// `clipUpdated`, following the `ExportCompleted` precedent for
    /// pinetree-only wire types; Keystone-only consumers). DIVERGENCE.
    ClipUpdated,
    /// A page received a remote `Like` (pinetree-only wire `pageLiked`).
    /// DIVERGENCE — closes the silent-likes gap.
    PageLiked,
}

impl NotificationKind {
    /// Misskey wire type name (`types.ts` `notificationTypes` parity):
    /// the key form used by `notificationRecieveConfig` and by
    /// `i/notifications` `includeTypes`/`excludeTypes`. Quotes notify as
    /// `Renote` locally, so both Misskey `renote` and `quote` select it.
    pub fn wire_type(&self) -> &'static str {
        match self {
            NotificationKind::Follow => "follow",
            NotificationKind::ReceiveFollowRequest => "receiveFollowRequest",
            NotificationKind::Mention => "mention",
            NotificationKind::Reply => "reply",
            NotificationKind::Renote => "renote",
            NotificationKind::Reaction => "reaction",
            NotificationKind::ScheduledNotePosted => "scheduledNotePosted",
            NotificationKind::ScheduledNotePostFailed => "scheduledNotePostFailed",
            NotificationKind::Edited => "edited",
            NotificationKind::FollowRequestAccepted => "followRequestAccepted",
            NotificationKind::ExportCompleted => "exportCompleted",
            NotificationKind::Test => "test",
            NotificationKind::ClipUpdated => "clipUpdated",
            NotificationKind::PageLiked => "pageLiked",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Notification {
    pub id: String,
    pub notifiee_id: String,
    pub notifier_id: String,
    pub kind: NotificationKind,
    pub note_id: Option<String>,
    pub reaction: Option<String>,
    /// Draft referenced by `ScheduledNotePostFailed` (Misskey
    /// `noteDraftId`). `None` on every other kind. `#[serde(default)]`
    /// keeps rows written before this field existed decoding as draft-less
    /// (migration by default).
    #[serde(default)]
    pub draft_id: Option<String>,
    /// Drive file produced by a profile export, referenced by
    /// `ExportCompleted` (Misskey `fileId`). `None` on every other kind.
    /// `#[serde(default)]` keeps older rows decoding as file-less.
    #[serde(default)]
    pub file_id: Option<String>,
    /// Entity an `ExportCompleted` notification reports on (Misskey
    /// `exportedEntity`: `following`/`blocking`/`muting`/`note`/
    /// `userList`/`antenna`/`clip`/`favorite`). `None` on every other kind.
    /// `#[serde(default)]` keeps older rows decoding as entity-less.
    #[serde(default)]
    pub exported_entity: Option<String>,
    /// Clip an updated/deleted-clip notification reports on
    /// (`ClipUpdated`). `None` on every other kind.
    #[serde(default)]
    pub clip_id: Option<String>,
    /// Page a page-like notification reports on (`PageLiked`).
    /// `None` on every other kind.
    #[serde(default)]
    pub page_id: Option<String>,
    pub is_read: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notification_roundtrips() {
        let n = Notification { clip_id: None, page_id: None,
            id: "nid00000000000001".to_string(),
            notifiee_id: "aid00000000000001".to_string(),
            notifier_id: "aid00000000000002".to_string(),
            kind: NotificationKind::Reaction,
            note_id: Some("oid00000000000001".to_string()),
            reaction: Some("👍".to_string()),
            draft_id: None,
            file_id: None,
            exported_entity: None,
            is_read: false,
        };
        let json = serde_json::to_string(&n).expect("serializable");
        let back: Notification = serde_json::from_str(&json).expect("deserializable");
        assert_eq!(n, back);
        assert!(json.contains("\"reaction\""));
    }
}
