//! Channels (Misskey `MiChannel` parity, local single-node slice).
//!
//! Upstream shape: `Channel { id, createdAt, lastNotedAt?, name, description,
//! bannerId?, bannerUrl?, color, isSensitive, allowRenoteToExternal,
//! userId? (owner, nullable) }` plus follower/favorite join rows. This file
//! owns the pure validators (no HTTP/DB); storage traits and mem backends
//! live in [`crate::application::commands::channels`] until the parent
//! promotes them to `ports.rs` + `redb` (see the WP2-part-B report).
//!
//! Bounds mirror the sibling clip/page validators in this repo:
//! name `1..=CHANNEL_NAME_MAX`, description `<=CHANNEL_DESCRIPTION_MAX`.
//! Upstream `channels/create.ts` param bounds are unverifiable from this
//! worktree (misskey checkout symlink is broken here); the chosen bounds
//! are recorded as pinetree-fixed in the report.

use crate::domain::id::PortError;

/// Channel name bound (pinetree-fixed; upstream unverified here).
pub const CHANNEL_NAME_MAX: usize = 100;
/// Channel description bound (pinetree-fixed; upstream unverified here).
pub const CHANNEL_DESCRIPTION_MAX: usize = 2048;
/// Channel color bound (`#rrggbb` form expected; enforced loosely).
pub const CHANNEL_COLOR_MAX: usize = 32;

/// One channel (upstream `MiChannel` local columns).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Channel {
    pub id: String,
    pub owner_id: Option<String>,
    pub name: String,
    pub description: Option<String>,
    pub banner_id: Option<String>,
    pub color: Option<String>,
    pub is_sensitive: bool,
    pub allow_renote_to_external: bool,
    pub created_at_ms: i64,
    /// Touched whenever a note would be posted (no channel posting surface
    /// exists yet, so this only moves on create/update here).
    pub last_noted_at_ms: Option<i64>,
}

impl Channel {
    /// Upstream `channels/create.ts` param rules (pinetree-fixed bounds):
    /// name 1–100 chars; description/banner/color optional.
    pub fn validate_name(name: &str) -> Result<(), PortError> {
        let n = name.chars().count();
        if n == 0 || n > CHANNEL_NAME_MAX {
            return Err(PortError::Validation(format!(
                "channel name must be 1..={CHANNEL_NAME_MAX} chars"
            )));
        }
        Ok(())
    }

    pub fn validate_description(description: Option<&str>) -> Result<(), PortError> {
        if let Some(d) = description {
            if d.chars().count() > CHANNEL_DESCRIPTION_MAX {
                return Err(PortError::Validation(format!(
                    "channel description must be <={CHANNEL_DESCRIPTION_MAX} chars"
                )));
            }
        }
        Ok(())
    }

    pub fn validate_color(color: Option<&str>) -> Result<(), PortError> {
        if let Some(c) = color {
            if c.is_empty() || c.chars().count() > CHANNEL_COLOR_MAX {
                return Err(PortError::Validation(format!(
                    "channel color must be 1..={CHANNEL_COLOR_MAX} chars"
                )));
            }
        }
        Ok(())
    }
}

/// One `(channel, user)` follow (upstream `MiChannelFollowing`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChannelFollow {
    pub id: String,
    pub channel_id: String,
    pub follower_id: String,
    pub created_at_ms: i64,
}

/// One `(channel, user)` favorite (upstream `MiChannelFavorite`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChannelFavorite {
    pub id: String,
    pub channel_id: String,
    pub user_id: String,
    pub created_at_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_bounds() {
        assert!(Channel::validate_name("").is_err());
        assert!(Channel::validate_name("general").is_ok());
        assert!(Channel::validate_name(&"x".repeat(101)).is_err());
    }

    #[test]
    fn channel_roundtrips() {
        let c = Channel {
            id: "c1".to_string(),
            owner_id: Some("u1".to_string()),
            name: "general".to_string(),
            description: None,
            banner_id: None,
            color: Some("#ff0000".to_string()),
            is_sensitive: false,
            allow_renote_to_external: true,
            created_at_ms: 1,
            last_noted_at_ms: None,
        };
        let json = serde_json::to_string(&c).expect("serializable");
        let back: Channel = serde_json::from_str(&json).expect("deserializable");
        assert_eq!(c, back);
    }
}
