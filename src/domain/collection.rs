//! Federated-collection privacy tiers (clips + pages, one model).
//!
//! Clips and pages are server-local in Misskey (no AP representation), so
//! federating them is a pinetree-first DIVERGENCE (pinetree↔pinetree only,
//! gracefully ignored by everyone else). Both types share this single tier
//! model (`docs/handoff-federated-clips-pages.md` §"Privacy tiers"):
//!
//! `Private` (self-only, never leaves) → `LocalApproved` (local membership
//! list, remote 403) → `Unlisted` (capability URI, served to bearer, never
//! advertised) → `Listed` (advertised, collection servable, items still
//! gated per-item) → `RemoteApproved` (Phase-0 signed fetch, listed
//! approver actors only).
//!
//! Remote-approved carries the standing trust caveat: pinetree controls who
//! can fetch, not what the remote server does next (same as federated DMs).
//! High-stakes content stays local-only. The authenticated-fetch gate that
//! enforces these tiers on inbound GETs lives in
//! [`crate::federation::fetch_gate`]; the per-object authorization
//! callbacks live with the clip/page operations. The gate itself carries no
//! clip/page/community assumptions — community membership logic
//! (`docs/community-planning.md`, private-community signed fetch) calls the
//! same interface later.

use crate::domain::id::DomainError;

/// One privacy model for federated collections (clips, pages; later:
/// community-bound objects via the same fetch gate).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CollectionTier {
    /// Never served remotely; no collection URI is advertised.
    Private,
    /// Local membership list; remote fetch answers 403.
    LocalApproved,
    /// Served to the bearer of the capability URI; never advertised.
    Unlisted,
    /// Advertised in the actor's collection reference; collection servable,
    /// items still gated per-item at read.
    Listed,
    /// Served only to signed requests from listed approver actors
    /// (authenticated-fetch gate; unknown/unsigned fetchers get 404).
    RemoteApproved,
}

impl CollectionTier {
    pub fn as_str(&self) -> &'static str {
        match self {
            CollectionTier::Private => "private",
            CollectionTier::LocalApproved => "local_approved",
            CollectionTier::Unlisted => "unlisted",
            CollectionTier::Listed => "listed",
            CollectionTier::RemoteApproved => "remote_approved",
        }
    }

    pub fn parse(s: &str) -> Option<CollectionTier> {
        match s {
            "private" | "self-only" | "self_only" => Some(CollectionTier::Private),
            "local_approved" | "local-approved" => Some(CollectionTier::LocalApproved),
            "unlisted" => Some(CollectionTier::Unlisted),
            "listed" => Some(CollectionTier::Listed),
            "remote_approved" | "remote-approved" => Some(CollectionTier::RemoteApproved),
            _ => None,
        }
    }

    /// Whether the object may appear in the actor's advertised collection
    /// reference. Only `Listed` is advertised; `Unlisted` never is, and
    /// `RemoteApproved` is not advertised either (approvers fetch by
    /// capability/out-of-band URI).
    pub fn advertised(&self) -> bool {
        matches!(self, CollectionTier::Listed)
    }

    /// Whether any remote serving is possible at all. `Private` and
    /// `LocalApproved` never leave the instance.
    pub fn remotely_servable(&self) -> bool {
        matches!(
            self,
            CollectionTier::Unlisted | CollectionTier::Listed | CollectionTier::RemoteApproved
        )
    }
}

impl Default for CollectionTier {
    fn default() -> Self {
        CollectionTier::Private
    }
}

impl std::fmt::Display for CollectionTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Upstream `isPublic` maps onto the tier model: `isPublic=true` ⇒
/// listed-global (the federate flag still defaults off, so zero protocol
/// footprint until explicitly enabled); `false` ⇒ self-only.
pub fn tier_from_is_public(is_public: bool) -> CollectionTier {
    if is_public {
        CollectionTier::Listed
    } else {
        CollectionTier::Private
    }
}

pub fn validate_tier_name(s: &str) -> Result<CollectionTier, DomainError> {
    CollectionTier::parse(s).ok_or_else(|| {
        DomainError::Validation(
            "visibility must be one of private, local_approved, unlisted, listed, remote_approved"
                .to_string(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tier_mapping_and_gates() {
        assert_eq!(tier_from_is_public(true), CollectionTier::Listed);
        assert_eq!(tier_from_is_public(false), CollectionTier::Private);
        assert!(CollectionTier::Listed.advertised());
        assert!(!CollectionTier::Unlisted.advertised());
        assert!(!CollectionTier::RemoteApproved.advertised());
        assert!(!CollectionTier::Private.remotely_servable());
        assert!(!CollectionTier::LocalApproved.remotely_servable());
        assert!(CollectionTier::Unlisted.remotely_servable());
        assert_eq!(CollectionTier::parse("self-only"), Some(CollectionTier::Private));
        assert!(CollectionTier::parse("bogus").is_none());
    }
}
