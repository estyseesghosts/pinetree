//! Read-time visibility predicate (`docs/visibility-policy.md` §§1, 3–5).
//!
//! Single definition of who may see a note. The Misskey API adapters,
//! streaming fan-out, timeline queries, and federation delivery all call
//! exactly this predicate — no per-adapter variants.

use crate::domain::note::{Note, Visibility};

/// Who is asking to see a note.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewerContext {
    /// `None` = anonymous / unauthenticated reader.
    pub viewer_id: Option<String>,
    /// `None` = local viewer, `Some(h)` = remote viewer from host `h`.
    pub viewer_host: Option<String>,
    /// Whether the viewer follows the note's author (local follow graph).
    pub follows_author: bool,
    /// True when rendering for federation / a remote viewer.
    pub is_remote_request: bool,
}

impl ViewerContext {
    pub fn anonymous() -> Self {
        Self {
            viewer_id: None,
            viewer_host: None,
            follows_author: false,
            is_remote_request: false,
        }
    }

    pub fn local_user(id: impl Into<String>, follows_author: bool) -> Self {
        Self {
            viewer_id: Some(id.into()),
            viewer_host: None,
            follows_author,
            is_remote_request: false,
        }
    }

    pub fn remote_user(
        id: impl Into<String>,
        host: impl Into<String>,
        follows_author: bool,
    ) -> Self {
        Self {
            viewer_id: Some(id.into()),
            viewer_host: Some(host.into()),
            follows_author,
            is_remote_request: true,
        }
    }
}

/// Read-time predicate `is_visible(note, viewer) -> bool`
/// (`docs/visibility-policy.md` §3).
pub fn is_visible(note: &Note, viewer: &ViewerContext) -> bool {
    // Tombstone rule: deleted notes are invisible on general reads.
    if note.is_deleted {
        return false;
    }
    // `local_only` (§4): never rendered to remote viewers or federation.
    if note.local_only && (viewer.is_remote_request || viewer.viewer_host.is_some()) {
        return false;
    }
    match &note.visibility {
        Visibility::Public | Visibility::Home => true,
        Visibility::Specified => match viewer.viewer_id.as_deref() {
            None => false,
            Some(viewer_id) => {
                viewer_id == note.author_id.as_str()
                    || note
                        .visible_user_ids
                        .iter()
                        .any(|id| id.as_str() == viewer_id)
            }
        },
        Visibility::Followers => match viewer.viewer_id.as_deref() {
            None => false,
            Some(viewer_id) => {
                if viewer_id == note.author_id.as_str() {
                    return true;
                }
                if note.reply_user_id.as_deref() == Some(viewer_id) {
                    return true;
                }
                if note.mentions.iter().any(|id| id.as_str() == viewer_id) {
                    return true;
                }
                if viewer.follows_author {
                    return true;
                }
                // Remote-remote fallback (§3, E2): when neither side's follow
                // graph is knowable (both note and viewer are remote), assume
                // visible so federated followers-timeline traffic is not lost.
                if note.user_host.is_some() && viewer.viewer_host.is_some() {
                    return true;
                }
                false
            }
        },
    }
}

/// Timeline and federation eligibility (`docs/visibility-policy.md` §5).
///
/// Local timelines carry local `public` notes only.
pub fn is_eligible_for_local_timeline(note: &Note) -> bool {
    !note.is_deleted && note.user_host.is_none() && note.visibility == Visibility::Public
}

/// Federated/public timelines carry non-`local_only` `public` notes.
pub fn is_eligible_for_federated_timeline(note: &Note) -> bool {
    !note.is_deleted && !note.local_only && note.visibility == Visibility::Public
}

/// ActivityPub delivery to remotes carries any non-`local_only` note;
// `home`/`followers`/`specified` reach is narrowed to followers +
/// mentioned + replied-to scope / listed users only at the delivery layer.
pub fn is_eligible_for_federation_delivery(note: &Note) -> bool {
    !note.is_deleted && !note.local_only
}
