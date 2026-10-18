//! Instance-level domain: registration policy, invite codes, blocked
//! hosts (Batch M §3.3, WP2, WP5). Pure data + validation (no I/O).

use crate::domain::id::DomainError;

fn invalid(msg: String) -> DomainError {
    DomainError::Validation(msg)
}

/// Registration policy (Batch M §3.3 / WP2 model: one switch). Misskey's
/// `meta.disableRegistration` is the inverse of `Open`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RegistrationPolicy {
    /// `disableRegistration: false`.
    #[default]
    Open,
    /// `disableRegistration: true`. Signup then requires a valid invite
    /// code (a closed instance with no codes issued is fully closed).
    Closed,
}

impl RegistrationPolicy {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "open" => Some(Self::Open),
            "closed" => Some(Self::Closed),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Closed => "closed",
        }
    }

    /// Misskey `meta.disableRegistration`.
    pub fn disable_registration(&self) -> bool {
        matches!(self, Self::Closed)
    }

    pub fn from_disable_registration(disabled: bool) -> Self {
        if disabled {
            Self::Closed
        } else {
            Self::Open
        }
    }
}

/// Instance settings (Batch M §3.3). Only what this batch uses; not a
/// generic settings bag. Defaults reproduce the pre-batch behavior:
/// registration open, no blocked hosts, `maxNoteTextLength` 3000.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct InstanceSettings {
    pub registration: RegistrationPolicy,
    /// Lowercased, deduplicated host names (upstream `blockedHosts`).
    pub blocked_hosts: Vec<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub max_note_text_length: usize,
}

impl Default for InstanceSettings {
    fn default() -> Self {
        Self {
            registration: RegistrationPolicy::Open,
            blocked_hosts: Vec::new(),
            name: None,
            description: None,
            max_note_text_length: 3000,
        }
    }
}

impl InstanceSettings {
    /// Update-path normalization for `blockedHosts` (upstream
    /// `admin/update-meta.ts:260`: `filter(Boolean).map(x => x.toLowerCase())`,
    /// plus dedup): lowercased, trimmed, empty removed, order-preserving.
    pub fn normalize_blocked_hosts(hosts: &[String]) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for host in hosts {
            let normalized = host.trim().to_ascii_lowercase();
            if normalized.is_empty() {
                continue;
            }
            if !out.iter().any(|h| h == &normalized) {
                out.push(normalized);
            }
        }
        out
    }

    /// Upstream `UtilityService.isBlockedHost`: a host is blocked when it
    /// equals a blocked entry or is a subdomain of one (`host` ends with
    /// `.` + blocked entry).
    pub fn is_blocked_host(&self, host: &str) -> bool {
        host_is_blocked(&self.blocked_hosts, host)
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        for host in &self.blocked_hosts {
            if host.chars().count() > 253 {
                return Err(invalid("blocked host exceeds 253 chars".to_string()));
            }
            if host.is_empty()
                || host != &host.trim().to_ascii_lowercase()
                || host.contains(['/', ':', ' '])
            {
                return Err(invalid(format!("invalid blocked host: {host}")));
            }
        }
        if let Some(name) = &self.name {
            if name.chars().count() > 128 {
                return Err(invalid("instance name exceeds 128 chars".to_string()));
            }
        }
        if let Some(description) = &self.description {
            if description.chars().count() > 256 {
                return Err(invalid(
                    "instance description exceeds 256 chars".to_string(),
                ));
            }
        }
        if self.max_note_text_length == 0 || self.max_note_text_length > 65_536 {
            return Err(invalid(
                "max_note_text_length must be 1..=65536".to_string(),
            ));
        }
        Ok(())
    }
}

/// Upstream `UtilityService.isBlockedHost` (suffix match: equality, or the
/// host ends with `.` + a blocked entry). Both sides must be lowercased.
pub fn host_is_blocked(blocked_hosts: &[String], host: &str) -> bool {
    let host = host.trim().to_ascii_lowercase();
    blocked_hosts
        .iter()
        .any(|blocked| host == *blocked || host.ends_with(&format!(".{blocked}")))
}

/// One invite code (upstream `models/RegistrationTicket.ts` subset used by
/// Batch M WP2). `code` is unique, 1..=64 chars (upstream column
/// `varchar(64)`). The ticket `id` (upstream `id`, a Misskey aid) is what
/// `invite/delete` addresses (`inviteId`) and what the packed `InviteCode`
/// exposes as `id`; the guide's §3.3 store sketch omits it, but the upstream
/// response shape and delete parameter both require it.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct InviteCode {
    /// Ticket id (upstream `RegistrationTicket.id`), sortable.
    pub id: String,
    pub code: String,
    /// Creator account id (upstream `createdById`, nullable).
    pub created_by: Option<String>,
    pub created_at_ms: i64,
    pub expires_at_ms: Option<i64>,
    pub used_by: Option<String>,
    pub used_at_ms: Option<i64>,
}

impl InviteCode {
    pub fn new(
        id: String,
        code: String,
        created_by: Option<String>,
        created_at_ms: i64,
        expires_at_ms: Option<i64>,
    ) -> Result<Self, DomainError> {
        let invite = Self {
            id,
            code,
            created_by,
            created_at_ms,
            expires_at_ms,
            used_by: None,
            used_at_ms: None,
        };
        invite.validate()?;
        Ok(invite)
    }

    pub fn is_used(&self) -> bool {
        self.used_by.is_some() || self.used_at_ms.is_some()
    }

    /// Upstream `SignupApiService` pre-check: expired when
    /// `expiresAt < now`.
    pub fn is_expired(&self, now_ms: i64) -> bool {
        self.expires_at_ms
            .map(|expires_at_ms| expires_at_ms < now_ms)
            .unwrap_or(false)
    }

    /// Consume this code for `account_id` at `now_ms` (single use:
    /// `used_by`/`used_at_ms` become `Some`). The caller commits the
    /// consumed invite and the new account in one transaction
    /// (conformance I2).
    pub fn consume(&mut self, account_id: &str, now_ms: i64) -> Result<(), DomainError> {
        if self.is_used() {
            return Err(DomainError::Conflict(format!(
                "invite code {} already used",
                self.code
            )));
        }
        self.used_by = Some(account_id.to_string());
        self.used_at_ms = Some(now_ms);
        Ok(())
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.id.is_empty() || self.id.chars().count() > 32 {
            return Err(invalid("invite id must be 1..=32 chars".to_string()));
        }
        if self.code.is_empty() || self.code.chars().count() > 64 {
            return Err(invalid("invite code must be 1..=64 chars".to_string()));
        }
        if !self.code.bytes().all(|b| b.is_ascii_alphanumeric()) {
            return Err(invalid(format!("invalid invite code: {}", self.code)));
        }
        if let Some(created_by) = &self.created_by {
            if created_by.chars().count() > 32 {
                return Err(invalid("invite created_by exceeds 32 chars".to_string()));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_reproduce_pre_batch_behavior() {
        let s = InstanceSettings::default();
        assert_eq!(s.registration, RegistrationPolicy::Open);
        assert!(!s.registration.disable_registration());
        assert!(s.blocked_hosts.is_empty());
        assert_eq!(s.max_note_text_length, 3000);
        assert!(s.validate().is_ok());
    }

    #[test]
    fn registration_policy_round_trip() {
        assert!(RegistrationPolicy::Closed.disable_registration());
        assert_eq!(
            RegistrationPolicy::from_disable_registration(true),
            RegistrationPolicy::Closed
        );
        assert_eq!(RegistrationPolicy::parse("open"), Some(RegistrationPolicy::Open));
        assert_eq!(RegistrationPolicy::parse("nope"), None);
    }

    #[test]
    fn blocked_host_normalization() {
        assert_eq!(
            InstanceSettings::normalize_blocked_hosts(&[
                " Example.COM ".to_string(),
                "example.com".to_string(),
                "".to_string(),
                "  ".to_string(),
                "Other.Org".to_string(),
            ]),
            vec!["example.com".to_string(), "other.org".to_string()]
        );
    }

    #[test]
    fn blocked_host_is_a_suffix_match() {
        // Upstream `UtilityService.ts` semantics.
        let blocked = vec!["example.com".to_string()];
        assert!(host_is_blocked(&blocked, "example.com"));
        assert!(host_is_blocked(&blocked, "sub.example.com"));
        assert!(host_is_blocked(&blocked, "a.b.example.com"));
        assert!(host_is_blocked(&blocked, "EXAMPLE.com"));
        assert!(!host_is_blocked(&blocked, "notexample.com"));
        assert!(!host_is_blocked(&blocked, "example.com.evil.test"));

        let mut s = InstanceSettings::default();
        s.blocked_hosts = InstanceSettings::normalize_blocked_hosts(&["Example.com".to_string()]);
        assert!(s.is_blocked_host("sub.example.com"));
        assert!(s.validate().is_ok());
    }

    #[test]
    fn settings_validation_bounds() {
        let mut s = InstanceSettings::default();
        s.blocked_hosts = vec!["UPPER.example".to_string()];
        assert!(s.validate().is_err()); // must be normalized (lowercased) first
        s.blocked_hosts = InstanceSettings::normalize_blocked_hosts(&["UPPER.example".to_string()]);
        assert!(s.validate().is_ok());

        let mut s = InstanceSettings::default();
        s.max_note_text_length = 0;
        assert!(s.validate().is_err());
        let mut s = InstanceSettings::default();
        s.name = Some("n".repeat(129));
        assert!(s.validate().is_err());
        let mut s = InstanceSettings::default();
        s.description = Some("d".repeat(257));
        assert!(s.validate().is_err());
    }

    #[test]
    fn invite_lifecycle() {
        let mut invite = InviteCode::new(
            "aid0000000000000a".to_string(),
            "ABCD2345EFGH".to_string(),
            Some("aid00000000000001".to_string()),
            1_000,
            Some(2_000),
        )
        .unwrap();
        assert!(!invite.is_used());
        assert!(!invite.is_expired(1_500));
        assert!(!invite.is_expired(2_000)); // `expiresAt < now` upstream: equal is live
        assert!(invite.is_expired(2_001));

        invite.consume("aid00000000000002", 1_500).unwrap();
        assert!(invite.is_used());
        assert!(invite
            .consume("aid00000000000003", 1_600)
            .is_err());

        for bad in ["", "has space", &"x".repeat(65), "dash-code"] {
            assert!(InviteCode::new("aid0000000000000b".to_string(), bad.to_string(), None, 0, None).is_err());
        }
    }
}
