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
    MeUpdated { account_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotifyJobPayload {
    pub notifiee_id: String,
    pub notifier_id: String,
    pub kind: NotificationKind,
    pub note_id: Option<String>,
    pub reaction: Option<String>,
}
