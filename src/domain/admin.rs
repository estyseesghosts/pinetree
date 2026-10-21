//! WP2-part-C domain validators + record shapes for auth/2FA, app
//! registry, and the admin/misc families (roles, relays, ads,
//! announcements, federation instances, queue stats, abuse-report
//! notification recipients, push subscriptions, retention).
//!
//! Single-node storage note (binding constraint): `ports.rs` and the redb
//! schema are parent-owned, so no new store tables exist in this WP.
//! Per-user state (TOTP, apps, push subscriptions) lives in the existing
//! [`crate::application::ports::RegistryStore`] under the owning account.
//! Instance-global admin collections (relays, ads, announcements, roles +
//! memberships, abuse recipients, proxy account) live in the same store
//! under the reserved [`GLOBAL_SETTINGS_NAMESPACE`] user id, which can
//! never collide with an account id (aidx ids never contain `~`).
//! DEDICATED TABLES PROPOSED (see subagent report): move each global
//! collection to its own store table + ports; the record shapes below are
//! already table-ready (serde structs with stable keys).

use serde::{Deserialize, Serialize};

use crate::domain::id::DomainError;

fn invalid(msg: String) -> DomainError {
    DomainError::Validation(msg)
}

/// Reserved registry `user_id` for instance-global admin collections.
/// Documented DIVERGENCE: upstream keeps these in dedicated tables; the
/// shapes here are table-ready for the proposed migration.
pub const GLOBAL_SETTINGS_NAMESPACE: &str = "~instance";

/// Loud refusal reasons (400 `INVALID_PARAM` via the dispatcher) for ops
/// that need missing subsystems. Never silent no-ops.
pub const PASSKEY_UNSUPPORTED_REASON: &str =
    "passkey sign-in is not supported by this server: no WebAuthn RP/challenge store or attestation verification (see signin-with-passkey stub)";
pub const SECURITY_KEY_UNSUPPORTED_REASON: &str =
    "security keys are not supported by this server: no WebAuthn credential store (TOTP via i/2fa/* is the supported second factor)";
pub const RESET_DB_REFUSED_REASON: &str =
    "reset-db is refused: development-only destructive reset with no single-node equivalent";
pub const QUEUE_CONTROL_UNSUPPORTED_REASON: &str =
    "queue delayed/clear controls need job enumeration ports (single unified queue exposes stats only)";
pub const ROLE_POLICY_UNENFORCED_NOTE: &str =
    "role policies are stored but not enforced: no role-policy engine (role_ids stored never enforced, TODOS P2)";

// Registry scopes (all match `^[a-zA-Z0-9_]+$`).
pub const SCOPE_2FA: &str = "2fa";
pub const SCOPE_APPS: &str = "apps";
pub const SCOPE_SW: &str = "sw";
pub const SCOPE_ADMIN_RELAYS: &str = "admin_relays";
pub const SCOPE_ADMIN_ADS: &str = "admin_ads";
pub const SCOPE_ADMIN_ANNOUNCEMENTS: &str = "admin_announcements";
pub const SCOPE_ADMIN_ROLES: &str = "admin_roles";
pub const SCOPE_ADMIN_ROLE_MEMBERS: &str = "admin_role_members";
pub const SCOPE_ADMIN_ABUSE_RECIPIENTS: &str = "admin_abuse_recipients";
pub const SCOPE_ADMIN_META: &str = "admin_meta";
pub const SCOPE_FEDERATION_INSTANCES: &str = "federation_instances";

pub const KEY_TOTP: &str = "totp";
pub const KEY_PROXY_ACCOUNT: &str = "proxy_account";
pub const KEY_RETENTION: &str = "retention";

// ---------------------------------------------------------------------------
// Record shapes (serde-stable, table-ready).
// ---------------------------------------------------------------------------

/// TOTP enrolment state for one account.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TotpState {
    pub secret_b32: String,
    pub verified: bool,
    pub passwordless: bool,
}

/// Developer app-registry row (owner-scoped until the global app table lands).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppRecord {
    pub id: String,
    pub name: String,
    pub secret: String,
    pub callback_url: Option<String>,
    pub permission: Vec<String>,
    pub description: Option<String>,
    pub created_at_ms: i64,
}

/// Web-push subscription row (per owning account).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushSubscription {
    pub endpoint: String,
    pub auth: String,
    pub publickey: String,
}

/// Relay row (global).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayRecord {
    pub inbox: String,
    pub status: String,
}

/// Announcement row (global).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnnouncementRecord {
    pub id: String,
    pub title: String,
    pub text: String,
    pub image_url: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Ad row (global).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdRecord {
    pub id: String,
    pub url: String,
    pub memo: String,
    pub place: String,
    pub ratio: f64,
    pub priority: String,
    pub expires_at_ms: i64,
    pub starts_at_ms: i64,
    pub image_url: String,
    pub day_of_week: i64,
}

/// Abuse-report notification recipient row (global).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbuseRecipientRecord {
    pub id: String,
    pub is_active: bool,
    pub name: String,
    /// `email` or `webhook`.
    pub method: String,
    pub user_id: Option<String>,
    pub email: Option<String>,
    pub webhook_url: Option<String>,
}

/// Custom role row (global). Policies are stored verbatim and NOT enforced
/// (see [`ROLE_POLICY_UNENFORCED_NOTE`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleRecord {
    pub id: String,
    pub name: String,
    pub description: String,
    pub color: Option<String>,
    pub icon_url: Option<String>,
    pub target: String,
    pub cond_formula: serde_json::Value,
    pub is_public: bool,
    pub as_badge: bool,
    pub can_edit_members_by_moderator: bool,
    pub display_order: i64,
    pub policies: serde_json::Value,
}

/// Per-host federation override row (global). Observed traffic (hosts,
/// user counts) derives from cached remote accounts; this row only carries
/// moderator intent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FederationInstanceOverride {
    pub host: String,
    pub is_suspended: bool,
    pub is_blocked: bool,
    pub is_silenced: bool,
    pub moderation_note: Option<String>,
    pub updated_at_ms: i64,
}

// ---------------------------------------------------------------------------
// Validators.
// ---------------------------------------------------------------------------

pub fn validate_app_name(name: &str) -> Result<(), DomainError> {
    let len = name.chars().count();
    if !(1..=64).contains(&len) {
        return Err(invalid("INVALID_PARAM app name must be 1..=64 chars".to_string()));
    }
    Ok(())
}

pub fn validate_app_callback(url: Option<&str>) -> Result<(), DomainError> {
    let Some(url) = url else { return Ok(()) };
    if !is_http_url(url) {
        return Err(invalid("INVALID_PARAM invalid callbackUrl".to_string()));
    }
    Ok(())
}

pub fn validate_relay_inbox(inbox: &str) -> Result<(), DomainError> {
    if !is_http_url(inbox) {
        return Err(invalid("INVALID_PARAM invalid relay inbox URL".to_string()));
    }
    Ok(())
}

pub fn validate_announcement(title: &str, text: &str) -> Result<(), DomainError> {
    if title.trim().is_empty() || title.chars().count() > 128 {
        return Err(invalid("INVALID_PARAM title must be 1..=128 chars".to_string()));
    }
    if text.trim().is_empty() || text.chars().count() > 8192 {
        return Err(invalid("INVALID_PARAM text must be 1..=8192 chars".to_string()));
    }
    Ok(())
}

pub fn validate_ad(url: &str, memo: &str, place: &str, image_url: &str) -> Result<(), DomainError> {
    if !is_http_url(url) {
        return Err(invalid("INVALID_PARAM invalid ad url".to_string()));
    }
    if !is_http_url(image_url) {
        return Err(invalid("INVALID_PARAM invalid ad imageUrl".to_string()));
    }
    if memo.chars().count() > 512 {
        return Err(invalid("INVALID_PARAM memo must be <=512 chars".to_string()));
    }
    if place.trim().is_empty() || place.chars().count() > 64 {
        return Err(invalid("INVALID_PARAM place must be 1..=64 chars".to_string()));
    }
    Ok(())
}

pub fn validate_recipient(
    name: &str,
    method: &str,
    email: Option<&str>,
    webhook_url: Option<&str>,
) -> Result<(), DomainError> {
    if name.trim().is_empty() || name.chars().count() > 128 {
        return Err(invalid("INVALID_PARAM name must be 1..=128 chars".to_string()));
    }
    match method {
        "email" => {
            let Some(email) = email else {
                return Err(invalid("INVALID_PARAM email recipient needs an email".to_string()));
            };
            if !email.contains('@') || email.chars().count() > 256 {
                return Err(invalid("INVALID_PARAM invalid recipient email".to_string()));
            }
        }
        "webhook" => {
            let Some(url) = webhook_url else {
                return Err(invalid(
                    "INVALID_PARAM webhook recipient needs a webhookUrl".to_string(),
                ));
            };
            if !is_http_url(url) {
                return Err(invalid("INVALID_PARAM invalid recipient webhookUrl".to_string()));
            }
        }
        _ => {
            return Err(invalid(
                "INVALID_PARAM method must be one of email, webhook".to_string(),
            ));
        }
    }
    Ok(())
}

pub fn validate_role(name: &str, target: &str) -> Result<(), DomainError> {
    if name.trim().is_empty() || name.chars().count() > 256 {
        return Err(invalid("INVALID_PARAM name must be 1..=256 chars".to_string()));
    }
    match target {
        "manual" | "conditional" => {}
        _ => {
            return Err(invalid(
                "INVALID_PARAM target must be one of manual, conditional".to_string(),
            ));
        }
    }
    Ok(())
}

pub fn validate_push_subscription(
    endpoint: &str,
    auth: &str,
    publickey: &str,
) -> Result<(), DomainError> {
    if !endpoint.starts_with("https://") {
        return Err(invalid(
            "INVALID_PARAM endpoint must be an https URL".to_string(),
        ));
    }
    if auth.trim().is_empty() || auth.len() > 512 {
        return Err(invalid("INVALID_PARAM invalid auth key".to_string()));
    }
    if publickey.trim().is_empty() || publickey.len() > 512 {
        return Err(invalid("INVALID_PARAM invalid publickey".to_string()));
    }
    Ok(())
}

/// TOTP token shape: exactly 6 ASCII digits.
pub fn validate_totp_token_format(token: &str) -> Result<(), DomainError> {
    if token.len() != 6 || !token.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid("INVALID_PARAM token must be 6 digits".to_string()));
    }
    Ok(())
}

fn is_http_url(url: &str) -> bool {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"));
    match rest {
        Some(rest) => {
            let host = rest.split('/').next().unwrap_or("");
            !host.is_empty() && !host.contains(' ')
        }
        None => false,
    }
}
