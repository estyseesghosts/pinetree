//! Clips (Misskey `MiClip`/`MiClipNote`/`MiClipFavorite` parity + pinetree
//! federation tiers).
//!
//! Upstream shape: `Clip { id, userId, name (1–100 chars, `varchar(128)`
//! column), isPublic (default false), description (nullable, ≤2048) }`,
//! `lastClippedAt` (touched on add only), unique `(noteId, clipId)` pairs,
//! unique `(userId, clipId)` favorites. Caps come from the role policy with
//! defaults `clipLimit: 10` and `noteEachClipsLimit: 200`
//! (`docs/handoff-federated-clips-pages.md` Phase 1).
//!
//! Federation (DIVERGENCE — Misskey has no AP representation for clips):
//! every clip carries a [`CollectionTier`](crate::domain::collection)
//! (default derived from `isPublic`) plus a `federate` flag (default
//! false). Either the admin `federateClips` switch or the per-clip flag
//! being off ⇒ local-only behavior, zero protocol footprint. `notesCount`
//! is derived at read time; pinetree notes carry no `clippedCount` counter
//! (DIVERGENCE — counts come from the `clip_notes` index instead).

use crate::domain::collection::{tier_from_is_public, CollectionTier};
use crate::domain::id::DomainError;

/// Max clips per owner (upstream `RoleService.clipLimit` default).
pub const CLIP_LIMIT_PER_OWNER: usize = 10;
/// Max notes per clip (upstream `RoleService.noteEachClipsLimit` default).
pub const CLIP_NOTES_PER_CLIP: usize = 200;
/// Clip name bound (upstream `create.ts` `minLength: 1, maxLength: 100`).
pub const CLIP_NAME_MAX: usize = 100;
/// Clip description bound (upstream `maxLength: 2048`, nullable).
pub const CLIP_DESCRIPTION_MAX: usize = 2048;

/// One named note collection (upstream `MiClip` + federation columns).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Clip {
    pub id: String,
    pub owner_id: String,
    pub name: String,
    pub description: Option<String>,
    /// Upstream `isPublic`: drives local show/notes/favorite visibility
    /// (private clips hide as `NO_SUCH_CLIP` from non-owners).
    pub is_public: bool,
    /// Federation privacy tier (default: from `is_public`).
    pub tier: CollectionTier,
    /// Per-clip federate flag (default false; admin switch AND this flag
    /// must be on for any remote serving).
    pub federate: bool,
    /// Actor URIs allowed to fetch when `tier == RemoteApproved`.
    pub approved_actor_uris: Vec<String>,
    pub created_at_ms: i64,
    /// Touched on `add-note` only (upstream `lastClippedAt` parity; never
    /// cleared on remove).
    pub last_clipped_at_ms: Option<i64>,
    /// Last metadata/member change (served as ActivityStreams `updated`;
    /// compared by `resolve_remote_clip` before full pulls).
    /// `#[serde(default)]` keeps old rows decoding as never-updated.
    #[serde(default)]
    pub updated_at_ms: i64,
}

impl Clip {
    /// Upstream `create.ts` param rules: name 1–100 chars, description
    /// ≤2048 chars (nullable; `''` coerces to null at the endpoint layer).
    pub fn validate_name(name: &str) -> Result<(), DomainError> {
        let n = name.chars().count();
        if n == 0 || n > CLIP_NAME_MAX {
            return Err(DomainError::Validation(format!(
                "clip name must be 1..={CLIP_NAME_MAX} chars"
            )));
        }
        Ok(())
    }

    pub fn validate_description(description: Option<&str>) -> Result<(), DomainError> {
        if let Some(d) = description {
            if d.chars().count() > CLIP_DESCRIPTION_MAX {
                return Err(DomainError::Validation(format!(
                    "clip description must be <={CLIP_DESCRIPTION_MAX} chars"
                )));
            }
        }
        Ok(())
    }

    /// Fresh clip with federation defaults: tier from `is_public`,
    /// `federate` off, no approvers.
    pub fn new(
        id: String,
        owner_id: String,
        name: String,
        description: Option<String>,
        is_public: bool,
        created_at_ms: i64,
    ) -> Self {
        let tier = tier_from_is_public(is_public);
        Self {
            id,
            owner_id,
            name,
            description,
            is_public,
            tier,
            federate: false,
            approved_actor_uris: Vec::new(),
            created_at_ms,
            last_clipped_at_ms: None,
            updated_at_ms: created_at_ms,
        }
    }
}

/// One `(clip, note)` membership (upstream `MiClipNote`, unique on the
/// pair). No clip-note entity is ever packed: clip-notes reads pack Notes.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClipNote {
    pub id: String,
    pub clip_id: String,
    pub note_id: String,
    pub created_at_ms: i64,
}

/// One clip favorite (upstream `MiClipFavorite`, unique on
/// `(user, clip)`). The API packs favorited `Clip`s, never this row.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClipFavorite {
    pub id: String,
    pub user_id: String,
    pub clip_id: String,
    pub created_at_ms: i64,
}

/// One clip follow (pinetree-first: Misskey has no clip-follow; follower
/// set drives `Update`/`Delete(Collection)` fan-out + notifications).
/// Unique on `(user, clip)`. No cap in v1 (follows are cheap rows, no
/// fan-out amplification at rest).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClipFollow {
    pub id: String,
    pub user_id: String,
    pub clip_id: String,
    pub created_at_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_and_description_bounds() {
        assert!(Clip::validate_name("ok").is_ok());
        assert!(Clip::validate_name("").is_err());
        assert!(Clip::validate_name(&"n".repeat(100)).is_ok());
        assert!(Clip::validate_name(&"n".repeat(101)).is_err());
        assert!(Clip::validate_description(None).is_ok());
        assert!(Clip::validate_description(Some(&"d".repeat(2048))).is_ok());
        assert!(Clip::validate_description(Some(&"d".repeat(2049))).is_err());
    }

    #[test]
    fn federation_defaults_are_closed() {
        let clip = Clip::new(
            "c1".to_string(),
            "u1".to_string(),
            "x".to_string(),
            None,
            true,
            0,
        );
        assert_eq!(clip.tier, CollectionTier::Listed);
        assert!(!clip.federate);
        assert!(clip.approved_actor_uris.is_empty());
        let private = Clip::new(
            "c2".to_string(),
            "u1".to_string(),
            "x".to_string(),
            None,
            false,
            0,
        );
        assert_eq!(private.tier, CollectionTier::Private);
    }
}
