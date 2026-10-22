use crate::domain::id::PortError;
use crate::domain::instance::{InstanceSettings, RegistrationPolicy, UgcVisibilityForVisitor};
use crate::domain::moderation::ModerationLogEntry;
use serde::{Deserialize, Serialize};

/// [`InstanceSettingsStore::update`] patch (upstream `admin/update-meta`
/// subset used by Batch M). `blocked_hosts` is normalized by the store:
/// lowercased, trimmed, deduplicated, empties dropped.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceSettingsPatch {
    /// Maps to upstream `disableRegistration` (true = [`RegistrationPolicy::Closed`]).
    pub registration: Option<RegistrationPolicy>,
    pub blocked_hosts: Option<Vec<String>>,
    pub name: Option<Option<String>>,
    pub description: Option<Option<String>>,
    pub max_note_text_length: Option<usize>,
    /// Federated clips + pages handoff: admin `federateClips` switch.
    pub federate_clips: Option<bool>,
    /// Federated clips + pages handoff: admin `federatePages` switch.
    pub federate_pages: Option<bool>,
    /// Batch Q Phase 1 item 3 (P5): admin `ugcVisibilityForVisitor` switch.
    pub ugc_visibility_for_visitor: Option<UgcVisibilityForVisitor>,
}

pub trait InstanceSettingsStore: Send + Sync {
    /// Current settings; a fresh store returns [`InstanceSettings::default()`]
    /// (registration open, no blocked hosts, `maxNoteTextLength` 3000).
    fn get(&self) -> Result<InstanceSettings, PortError>;
    /// Apply `patch` and append `log` in one transaction (§3.3).
    fn update(
        &self,
        patch: InstanceSettingsPatch,
        log: ModerationLogEntry,
    ) -> Result<InstanceSettings, PortError>;
}

// ---------------------------------------------------------------------------
// Invite codes (Batch M WP2; upstream `RegistrationTicket`)
// ---------------------------------------------------------------------------
