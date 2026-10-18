//! Batch M moderation use cases: built-in roles (§3.1) and the moderation
//! log read path (§3.2). WP3–WP6 moderation commands land here as the batch
//! progresses.

use super::AppServices;

use crate::application::ports::{
    InstanceSettingsPatch, ModerationLogFilter, PortError, TimelineCursor,
};
use crate::domain::account::Account;
use crate::domain::instance::{InstanceSettings, RegistrationPolicy};
use crate::domain::moderation::{ModerationAction, ModerationLogEntry};

impl AppServices {
    /// §3.1 `set-role`: set the fixed built-in role flags. This is the
    /// only write path for `is_admin`/`is_moderator` (the CLI makes the
    /// first admin this way; `UpdateAccountPatch` cannot set roles).
    /// `actor` is the moderator taking the action, `None` for operator CLI
    /// use; the row logs as `setRole` in the same transaction (§2.3).
    pub fn set_role(
        &self,
        actor: Option<&str>,
        account_id: &str,
        is_admin: bool,
        is_moderator: bool,
    ) -> Result<Account, PortError> {
        self.accounts.get_owner(account_id)?;
        let target = self
            .accounts
            .find_by_id(account_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
        let log = ModerationLogEntry::new(
            self.ids.generate(),
            actor.map(str::to_string),
            ModerationAction::SetRole,
            Some(account_id.to_string()),
            serde_json::json!({
                "userId": account_id,
                "userUsername": target.username,
                "userHost": target.host,
                "isAdmin": is_admin,
                "isModerator": is_moderator,
            }),
            self.clock.now_ms(),
        )?;
        self.moderation
            .set_role(account_id, is_admin, is_moderator, log)
    }

    /// `admin/show-moderation-logs` read path: newest-first, cursor-paged,
    /// filtered (upstream `type`/`userId`/`search` params).
    pub fn list_moderation_logs(
        &self,
        filter: ModerationLogFilter,
        cursor: &TimelineCursor,
    ) -> Result<Vec<ModerationLogEntry>, PortError> {
        self.moderation.list_logs(&filter, cursor)
    }

    /// `admin/update-meta` subset (Batch M WP2): `disableRegistration`
    /// (maps to [`RegistrationPolicy`]) and `blockedHosts` (normalized:
    /// lowercased, trimmed, deduped, empties dropped — enforced from WP5,
    /// stored from here). The settings write and its
    /// `updateServerSettings` row commit in one transaction (§2.3); the
    /// row carries upstream's `{before, after}` payload over the changed
    /// fields (`admin/update-meta.ts:801`).
    pub fn update_instance_settings(
        &self,
        actor: &str,
        disable_registration: Option<bool>,
        blocked_hosts: Option<Vec<String>>,
        now_ms: i64,
    ) -> Result<InstanceSettings, PortError> {
        let current = self.instance_settings.get()?;
        let mut patch = InstanceSettingsPatch::default();
        if let Some(disabled) = disable_registration {
            patch.registration = Some(RegistrationPolicy::from_disable_registration(disabled));
        }
        if let Some(hosts) = blocked_hosts {
            patch.blocked_hosts = Some(InstanceSettings::normalize_blocked_hosts(&hosts));
        }
        let next_registration = patch.registration.unwrap_or(current.registration);
        let next_blocked_hosts = patch
            .blocked_hosts
            .clone()
            .unwrap_or_else(|| current.blocked_hosts.clone());
        let log = ModerationLogEntry::new(
            self.ids.generate(),
            Some(actor.to_string()),
            ModerationAction::UpdateServerSettings,
            None,
            serde_json::json!({
                "before": {
                    "disableRegistration": current.registration.disable_registration(),
                    "blockedHosts": current.blocked_hosts,
                },
                "after": {
                    "disableRegistration": next_registration.disable_registration(),
                    "blockedHosts": next_blocked_hosts,
                },
            }),
            now_ms,
        )?;
        self.instance_settings.update(patch, log)
    }
}
