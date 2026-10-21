//! Chat rooms, invitations, and messages (Misskey `chat/*` parity, local
//! single-node slice, no streaming — `chatUser`/`chatRoom` channels stay
//! `UNSUPPORTED_CHANNEL` per the WP2/WP4 decision).
//!
//! Model (coherent local reading of the upstream surfaces; the misskey
//! checkout symlink is broken in this worktree so param-level upstream
//! details are recorded as pinetree-fixed in the report):
//! - `ChatRoom { id, owner_id, name, description, created_at_ms,
//!   updated_at_ms }`. Any signed-in user may create; owner-only update /
//!   delete.
//! - Membership is an explicit join row. `join` is open (any signed-in user,
//!   idempotent) and consumes a pending invitation when one exists.
//!   `leave` removes membership; when the owner leaves, ownership passes to
//!   the oldest remaining member; when the last member leaves the room and
//!   its messages/invitations/reads are deleted.
//! - Invitations are room-scoped `(room, invitee)` rows created by members
//!   (`chat_room_invite` app op; the dispatcher wires whichever upstream
//!   endpoint mints them). Accept joins + consumes; decline deletes.
//! - `ChatMessage { id, room_id, author_id, text, file_id, ... }`,
//!   member-only create/show, author-only update, author-or-owner delete.
//!   Read watermarks are per `(room, user)` last-read message ids; unread
//!   counts back the `hasUnreadChatMessages` badge.
//!
//! Pure validators only (no HTTP/DB); storage traits and mem backends live
//! in [`crate::application::commands::chat`] until the parent promotes them.

use crate::domain::id::PortError;

/// Room name bound (pinetree-fixed).
pub const CHAT_ROOM_NAME_MAX: usize = 100;
/// Room description bound (pinetree-fixed).
pub const CHAT_ROOM_DESCRIPTION_MAX: usize = 2048;
/// Message text bound (mirrors the note 3000-char bound).
pub const CHAT_MESSAGE_TEXT_MAX: usize = 3000;

/// One chat room (upstream `MiChatRoom` local columns).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChatRoom {
    pub id: String,
    pub owner_id: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// One room membership (muted = member-level mute flag).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChatRoomMember {
    pub room_id: String,
    pub user_id: String,
    pub muted: bool,
    pub joined_at_ms: i64,
}

/// One pending room invitation (unique on `(room_id, invitee_id)`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChatInvitation {
    pub id: String,
    pub room_id: String,
    pub inviter_id: String,
    pub invitee_id: String,
    pub created_at_ms: i64,
}

/// One chat message (single optional `file_id`, upstream shape).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChatMessage {
    pub id: String,
    pub room_id: String,
    pub author_id: String,
    pub text: Option<String>,
    pub file_id: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: Option<i64>,
    pub is_deleted: bool,
}

impl ChatMessage {
    /// Upstream `chat/messages/create.ts` rule: a message needs text or a
    /// file; text is `1..=CHAT_MESSAGE_TEXT_MAX` chars when present.
    pub fn validate_body(text: Option<&str>, file_id: Option<&str>) -> Result<(), PortError> {
        let text = text.map(str::trim).filter(|t| !t.is_empty());
        let file = file_id.map(str::trim).filter(|f| !f.is_empty());
        if text.is_none() && file.is_none() {
            return Err(PortError::Validation(
                "INVALID_PARAM text or fileId is required".to_string(),
            ));
        }
        if let Some(t) = text {
            if t.chars().count() > CHAT_MESSAGE_TEXT_MAX {
                return Err(PortError::Validation(format!(
                    "INVALID_PARAM text must be <={CHAT_MESSAGE_TEXT_MAX} chars"
                )));
            }
        }
        Ok(())
    }
}

/// Per-`(room, user)` read watermark (last-read message id).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChatRoomRead {
    pub room_id: String,
    pub user_id: String,
    pub last_read_message_id: Option<String>,
    pub updated_at_ms: i64,
}

pub fn validate_room_name(name: Option<&str>) -> Result<(), PortError> {
    if let Some(n) = name {
        let n = n.trim();
        if n.is_empty() || n.chars().count() > CHAT_ROOM_NAME_MAX {
            return Err(PortError::Validation(format!(
                "chat room name must be 1..={CHAT_ROOM_NAME_MAX} chars"
            )));
        }
    }
    Ok(())
}

pub fn validate_room_description(description: Option<&str>) -> Result<(), PortError> {
    if let Some(d) = description {
        if d.chars().count() > CHAT_ROOM_DESCRIPTION_MAX {
            return Err(PortError::Validation(format!(
                "chat room description must be <={CHAT_ROOM_DESCRIPTION_MAX} chars"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_body_rules() {
        assert!(ChatMessage::validate_body(None, None).is_err());
        assert!(ChatMessage::validate_body(Some("  "), None).is_err());
        assert!(ChatMessage::validate_body(Some("hi"), None).is_ok());
        assert!(ChatMessage::validate_body(None, Some("f1")).is_ok());
        assert!(ChatMessage::validate_body(Some(&"x".repeat(3001)), None).is_err());
    }

    #[test]
    fn room_roundtrips() {
        let r = ChatRoom {
            id: "r1".to_string(),
            owner_id: "u1".to_string(),
            name: Some("lobby".to_string()),
            description: None,
            created_at_ms: 1,
            updated_at_ms: 1,
        };
        let json = serde_json::to_string(&r).expect("serializable");
        let back: ChatRoom = serde_json::from_str(&json).expect("deserializable");
        assert_eq!(r, back);
    }
}
