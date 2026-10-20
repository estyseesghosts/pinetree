//! Moderation domain: moderation-log entries and abuse reports (Batch M
//! §3.2, §4 WP4). Pure data + validation (no I/O, no HTTP/Misskey/AP/DB
//! imports). `details` is a free-form JSON object mirroring upstream's
//! `ModerationLogs.info` payload; adapters map it to their own shapes.

use serde_json::Value;

use crate::domain::id::DomainError;

fn invalid(msg: String) -> DomainError {
    DomainError::Validation(msg)
}

/// Moderation-log action name. Where upstream Misskey has a matching
/// action type (`misskey/packages/backend/src/types.ts`
/// `moderationLogTypes`) its exact string is used, so a Misskey client's
/// moderation-log UI can render pinetree rows; pinetree-only actions use
/// the Batch M guide §3.2 names. Serialized as that string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModerationAction {
    /// `suspend` (upstream).
    Suspend,
    /// `unsuspend` (upstream).
    Unsuspend,
    /// `updateUserNote` (upstream).
    UpdateUserNote,
    /// `acceptQuotesUser` (Sharkey `types.ts`, `admin/reject-quotes.ts`
    /// parity; guide §3.2 sketches one `updateUserRejectQuotes` action, but
    /// Sharkey logs one action per direction and UIs label by exact string).
    AcceptQuotesUser,
    /// `rejectQuotesUser` (Sharkey; see [`Self::AcceptQuotesUser`]).
    RejectQuotesUser,
    /// `setRole` (pinetree; upstream models this as `assignRole`/
    /// `unassignRole` over custom roles, which pinetree does not have).
    SetRole,
    /// `updateServerSettings` (upstream; guide §3.2 calls it `updateMeta`).
    UpdateServerSettings,
    /// `createInvitation` (upstream; guide §3.2 calls it `createInvite`).
    CreateInvitation,
    /// `deleteInvite` (pinetree; no upstream type).
    DeleteInvite,
    /// `resolveAbuseReport` (upstream).
    ResolveAbuseReport,
    /// `forwardAbuseReport` (upstream).
    ForwardAbuseReport,
    /// `updateAbuseReportNote` (upstream).
    UpdateAbuseReportNote,
    /// `addCustomEmoji` (upstream).
    AddCustomEmoji,
    /// `updateCustomEmoji` (upstream).
    UpdateCustomEmoji,
    /// `deleteCustomEmoji` (upstream).
    DeleteCustomEmoji,
    /// `deleteAccount` (upstream).
    DeleteAccount,
    /// `createAccount` (pinetree; admin-created accounts, WP2 — upstream
    /// `admin/accounts/create` writes no log).
    CreateAccount,
    /// `unsetUserAvatar` (upstream).
    UnsetUserAvatar,
    /// `unsetUserBanner` (upstream).
    UnsetUserBanner,
    /// `deleteAllFilesOfAUser` — pinetree action name (upstream
    /// `admin/delete-all-files-of-a-user` writes no log row).
    DeleteAllFilesOfAUser,
    /// `deletePage` (upstream; moderator page deletes log it).
    DeletePage,
}

impl ModerationAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Suspend => "suspend",
            Self::Unsuspend => "unsuspend",
            Self::UpdateUserNote => "updateUserNote",
            Self::AcceptQuotesUser => "acceptQuotesUser",
            Self::RejectQuotesUser => "rejectQuotesUser",
            Self::SetRole => "setRole",
            Self::UpdateServerSettings => "updateServerSettings",
            Self::CreateInvitation => "createInvitation",
            Self::DeleteInvite => "deleteInvite",
            Self::ResolveAbuseReport => "resolveAbuseReport",
            Self::ForwardAbuseReport => "forwardAbuseReport",
            Self::UpdateAbuseReportNote => "updateAbuseReportNote",
            Self::AddCustomEmoji => "addCustomEmoji",
            Self::UpdateCustomEmoji => "updateCustomEmoji",
            Self::DeleteCustomEmoji => "deleteCustomEmoji",
            Self::DeleteAccount => "deleteAccount",
            Self::CreateAccount => "createAccount",
            Self::UnsetUserAvatar => "unsetUserAvatar",
            Self::UnsetUserBanner => "unsetUserBanner",
            Self::DeleteAllFilesOfAUser => "deleteAllFilesOfAUser",
            Self::DeletePage => "deletePage",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "suspend" => Self::Suspend,
            "unsuspend" => Self::Unsuspend,
            "updateUserNote" => Self::UpdateUserNote,
            "acceptQuotesUser" => Self::AcceptQuotesUser,
            "rejectQuotesUser" => Self::RejectQuotesUser,
            "setRole" => Self::SetRole,
            "updateServerSettings" => Self::UpdateServerSettings,
            "createInvitation" => Self::CreateInvitation,
            "deleteInvite" => Self::DeleteInvite,
            "resolveAbuseReport" => Self::ResolveAbuseReport,
            "forwardAbuseReport" => Self::ForwardAbuseReport,
            "updateAbuseReportNote" => Self::UpdateAbuseReportNote,
            "addCustomEmoji" => Self::AddCustomEmoji,
            "updateCustomEmoji" => Self::UpdateCustomEmoji,
            "deleteCustomEmoji" => Self::DeleteCustomEmoji,
            "deleteAccount" => Self::DeleteAccount,
            "createAccount" => Self::CreateAccount,
            "unsetUserAvatar" => Self::UnsetUserAvatar,
            "unsetUserBanner" => Self::UnsetUserBanner,
            "deleteAllFilesOfAUser" => Self::DeleteAllFilesOfAUser,
            "deletePage" => Self::DeletePage,
            _ => return None,
        })
    }
}

impl serde::Serialize for ModerationAction {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for ModerationAction {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        Self::parse(&s).ok_or_else(|| serde::de::Error::custom(format!("unknown action: {s}")))
    }
}

/// One moderation-log row. `actor_account` is the moderator who performed
/// the action (upstream `ModerationLogs.userId`); `target` is the account
/// or entity acted on (pinetree extra for filtering; upstream keeps it
/// inside `info`). `actor_account` is `None` for operator actions taken
/// through the CLI (`set-role`), which have no acting account — DIVERGENCE
/// from upstream, whose rows always carry a user.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ModerationLogEntry {
    pub id: String,
    pub actor_account: Option<String>,
    pub action: ModerationAction,
    #[serde(default)]
    pub target: Option<String>,
    /// Upstream `ModerationLogs.info` payload shape (a JSON object).
    pub details: Value,
    pub created_at_ms: i64,
}

impl ModerationLogEntry {
    pub fn new(
        id: String,
        actor_account: Option<String>,
        action: ModerationAction,
        target: Option<String>,
        details: Value,
        created_at_ms: i64,
    ) -> Result<Self, DomainError> {
        let entry = Self {
            id,
            actor_account,
            action,
            target,
            details,
            created_at_ms,
        };
        entry.validate()?;
        Ok(entry)
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.id.is_empty() || self.id.chars().count() > 32 {
            return Err(invalid("moderation log id must be 1..=32 chars".to_string()));
        }
        if let Some(actor) = &self.actor_account {
            if actor.is_empty() || actor.chars().count() > 32 {
                return Err(invalid(
                    "moderation log actor_account must be 1..=32 chars".to_string(),
                ));
            }
        }
        if let Some(target) = &self.target {
            if target.chars().count() > 128 {
                return Err(invalid(
                    "moderation log target exceeds 128 chars".to_string(),
                ));
            }
        }
        if !self.details.is_object() {
            return Err(invalid(
                "moderation log details must be a JSON object".to_string(),
            ));
        }
        Ok(())
    }
}

/// Upstream `AbuseReportResolveType` (`models/AbuseUserReport.ts`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AbuseReportResolution {
    Accept,
    Reject,
}

impl AbuseReportResolution {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "accept" => Some(Self::Accept),
            "reject" => Some(Self::Reject),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::Reject => "reject",
        }
    }
}

/// One `users/report-abuse` row (upstream `models/AbuseUserReport.ts`).
/// Local reports only in Batch M: `reporter` is always a local account
/// (upstream keeps `reporterHost`/`targetUserHost`; the target host is
/// derivable from the account).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AbuseReport {
    pub id: String,
    pub target_account: String,
    pub reporter: String,
    /// 1..=2048 chars (upstream `report-abuse.ts` paramDef).
    pub comment: String,
    pub resolved: bool,
    pub resolved_as: Option<AbuseReportResolution>,
    pub assignee: Option<String>,
    pub moderation_note: String,
    /// True once a Flag activity was forwarded to the target's instance
    /// (upstream: "リモートサーバーに転送したかどうか").
    pub forwarded: bool,
    pub created_at_ms: i64,
}

impl AbuseReport {
    pub fn new(
        id: String,
        target_account: String,
        reporter: String,
        comment: String,
        created_at_ms: i64,
    ) -> Result<Self, DomainError> {
        let report = Self {
            id,
            target_account,
            reporter,
            comment,
            resolved: false,
            resolved_as: None,
            assignee: None,
            moderation_note: String::new(),
            forwarded: false,
            created_at_ms,
        };
        report.validate()?;
        Ok(report)
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        for (label, value) in [
            ("id", &self.id),
            ("target_account", &self.target_account),
            ("reporter", &self.reporter),
        ] {
            if value.is_empty() || value.chars().count() > 32 {
                return Err(invalid(format!(
                    "abuse report {label} must be 1..=32 chars"
                )));
            }
        }
        let n = self.comment.chars().count();
        if n == 0 || n > 2048 {
            return Err(invalid(
                "abuse report comment must be 1..=2048 chars".to_string(),
            ));
        }
        if self.moderation_note.chars().count() > 4096 {
            return Err(invalid(
                "abuse report moderation_note exceeds 4096 chars".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry() -> ModerationLogEntry {
        ModerationLogEntry::new(
            "mlog0000000000001".to_string(),
            Some("aid00000000000001".to_string()),
            ModerationAction::Suspend,
            Some("aid00000000000002".to_string()),
            serde_json::json!({"userId": "aid00000000000002"}),
            1_000,
        )
        .unwrap()
    }

    #[test]
    fn action_names_match_upstream_types() {
        // Verified against misskey 2026.10.1-alpha.0 `types.ts`
        // `moderationLogTypes`.
        for (action, name) in [
            (ModerationAction::Suspend, "suspend"),
            (ModerationAction::Unsuspend, "unsuspend"),
            (ModerationAction::UpdateUserNote, "updateUserNote"),
            (ModerationAction::UpdateServerSettings, "updateServerSettings"),
            (ModerationAction::CreateInvitation, "createInvitation"),
            (ModerationAction::ResolveAbuseReport, "resolveAbuseReport"),
            (ModerationAction::ForwardAbuseReport, "forwardAbuseReport"),
            (
                ModerationAction::UpdateAbuseReportNote,
                "updateAbuseReportNote",
            ),
            (ModerationAction::AddCustomEmoji, "addCustomEmoji"),
            (ModerationAction::UpdateCustomEmoji, "updateCustomEmoji"),
            (ModerationAction::DeleteCustomEmoji, "deleteCustomEmoji"),
            (ModerationAction::UnsetUserAvatar, "unsetUserAvatar"),
            (ModerationAction::UnsetUserBanner, "unsetUserBanner"),
            (ModerationAction::DeleteAccount, "deleteAccount"),
        ] {
            assert_eq!(action.as_str(), name);
            assert_eq!(ModerationAction::parse(name), Some(action));
        }
        assert_eq!(ModerationAction::parse("nope"), None);
    }

    #[test]
    fn entry_validation_and_round_trip() {
        let e = entry();
        assert!(e.validate().is_ok());
        let json = serde_json::to_string(&e).unwrap();
        let back: ModerationLogEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back, e);

        let mut bad = entry();
        bad.details = serde_json::json!("not an object");
        assert!(bad.validate().is_err());
        let mut bad = entry();
        bad.actor_account = Some(String::new());
        assert!(bad.validate().is_err());
        let mut cli = entry();
        cli.actor_account = None;
        assert!(cli.validate().is_ok()); // operator (CLI) rows have no actor
    }

    #[test]
    fn abuse_report_defaults_and_bounds() {
        let r = AbuseReport::new(
            "abr0000000000001".to_string(),
            "aid00000000000002".to_string(),
            "aid00000000000001".to_string(),
            "spam".to_string(),
            1_000,
        )
        .unwrap();
        assert!(!r.resolved && !r.forwarded);
        assert_eq!(r.resolved_as, None);
        assert_eq!(r.assignee, None);
        assert!(r.moderation_note.is_empty());

        for bad_comment in ["", &"c".repeat(2049)] {
            assert!(AbuseReport::new(
                "abr0000000000001".to_string(),
                "aid00000000000002".to_string(),
                "aid00000000000001".to_string(),
                bad_comment.to_string(),
                1_000,
            )
            .is_err());
        }
        assert!(AbuseReportResolution::parse("accept") == Some(AbuseReportResolution::Accept));
        assert!(AbuseReportResolution::parse("nope").is_none());
    }
}
