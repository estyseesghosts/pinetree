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
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Notification {
    pub id: String,
    pub notifiee_id: String,
    pub notifier_id: String,
    pub kind: NotificationKind,
    pub note_id: Option<String>,
    pub reaction: Option<String>,
    pub is_read: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notification_roundtrips() {
        let n = Notification {
            id: "nid00000000000001".to_string(),
            notifiee_id: "aid00000000000001".to_string(),
            notifier_id: "aid00000000000002".to_string(),
            kind: NotificationKind::Reaction,
            note_id: Some("oid00000000000001".to_string()),
            reaction: Some("👍".to_string()),
            is_read: false,
        };
        let json = serde_json::to_string(&n).expect("serializable");
        let back: Notification = serde_json::from_str(&json).expect("deserializable");
        assert_eq!(n, back);
        assert!(json.contains("\"reaction\""));
    }
}
