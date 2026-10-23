//! Webhooks (system + user) and abuse-report notification recipients:
//! pure data + validation (no I/O).
//!
//! Shapes here are pinetree-defined against the endpoint names in
//! `docs/endpoint-manifest.md` §§24/30 (`system-webhook/*`,
//! `webhooks/*`, `abuse-report/notification-recipient/*`): verify field
//! names against the upstream checkout (`misskey/`) at merge time before
//! freezing the manifest addendum. Delivery (HTTP POST + HMAC) and SMTP
//! live in the application layer (`application::commands::webhooks`) and
//! `services::email` respectively; this module never touches the network.

use serde::{Deserialize, Serialize};

use crate::domain::id::DomainError;

/// System-webhook trigger names (admin-owned, instance-wide events).
/// Pinetree-defined set; reconcile with upstream
/// `SystemWebhookService` trigger types at merge.
pub const SYSTEM_WEBHOOK_TRIGGERS: &[&str] = &["userCreated", "abuseReport", "abuseReportResolved"];

/// User-webhook trigger names (per-user events). Pinetree-defined set;
/// reconcile with upstream `WebhookService` event types at merge.
pub const USER_WEBHOOK_TRIGGERS: &[&str] = &[
    "mention",
    "reply",
    "renote",
    "quote",
    "reaction",
    "pollEnded",
    "follow",
    "followed",
    "note",
];

/// Default cap on user webhooks per account when no role policy is wired
/// (upstream `RoleService` `webhookLimit`; the parent wires the real role
/// value at the endpoint — this default only bounds the hermetic path).
pub const DEFAULT_WEBHOOK_LIMIT: usize = 5;

/// Upper bound on abuse-report notification emails fanned out for one
/// report event (one txn, one `enqueue_jobs` call — see
/// `application::commands::webhooks::plan_abuse_report_emails`).
pub const MAX_ABUSE_REPORT_EMAILS_PER_EVENT: usize = 50;

/// Instance-wide webhook row (admin-owned; `system-webhook/*`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemWebhook {
    pub id: String,
    pub name: String,
    pub url: String,
    /// HMAC secret, shown once at create/rotate (stored as given; the
    /// redb adapter persists it verbatim — never logged).
    pub secret: String,
    pub trigger: String,
    pub is_active: bool,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Per-user webhook row (`webhooks/*`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserWebhook {
    pub id: String,
    pub owner_id: String,
    pub name: String,
    pub url: String,
    pub secret: String,
    pub trigger: String,
    pub is_active: bool,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Abuse-report notification recipient (admin-managed;
/// `abuse-report/notification-recipient/*`). Email-only in v1: a
/// `webhook`-method recipient would duplicate the system-webhook fan-out,
/// so the method column is reserved, not shipped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbuseReportRecipient {
    pub id: String,
    pub name: String,
    pub email: String,
    pub is_active: bool,
    pub created_at_ms: i64,
}

/// Validate a webhook/recipient display name (1..=100 chars).
pub fn validate_hook_name(name: &str) -> Result<(), DomainError> {
    let n = name.chars().count();
    if name.trim().is_empty() || n > 100 {
        return Err(DomainError::Validation(
            "webhook name must be 1..=100 chars".to_string(),
        ));
    }
    Ok(())
}

/// Validate a webhook target URL: `http(s)`, non-empty authority,
/// 1..=2048 bytes. SSRF is enforced again at delivery time by the
/// `SafeHttpClient`; this is only the shape gate.
pub fn validate_hook_url(url: &str) -> Result<(), DomainError> {
    if url.len() > 2048 {
        return Err(DomainError::Validation(
            "webhook url must be <= 2048 bytes".to_string(),
        ));
    }
    let lower = url.to_ascii_lowercase();
    let rest = lower
        .strip_prefix("http://")
        .or_else(|| lower.strip_prefix("https://"));
    let Some(rest) = rest else {
        return Err(DomainError::Validation(
            "webhook url must be http(s)".to_string(),
        ));
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    if authority.is_empty() {
        return Err(DomainError::Validation(
            "webhook url must include a host".to_string(),
        ));
    }
    Ok(())
}

/// Validate an explicitly supplied secret (1..=256 chars). Absent secrets
/// are minted by the application op (`mint_webhook_secret`), never here.
pub fn validate_hook_secret(secret: &str) -> Result<(), DomainError> {
    let n = secret.chars().count();
    if n == 0 || n > 256 {
        return Err(DomainError::Validation(
            "webhook secret must be 1..=256 chars".to_string(),
        ));
    }
    Ok(())
}

/// Validate `trigger` against the closed trigger set for its family.
pub fn validate_trigger(trigger: &str, allowed: &[&str]) -> Result<(), DomainError> {
    if allowed.contains(&trigger) {
        Ok(())
    } else {
        Err(DomainError::Validation(format!(
            "unknown webhook trigger: {trigger}"
        )))
    }
}

/// Basic email shape check (same rule as the `i/update-email` endpoint
/// helper: one `@`, non-empty local/domain, dot in domain, 3..=254).
///
/// SMTP-injection gate: rejects CR, LF, `<`, `>`, `,`, any whitespace, and
/// any other control character, so a validated address can never smuggle
/// extra headers (`Bcc:`) or envelope commands (`DATA`) into `RCPT TO:<…>`
/// or `To:` lines. Length and single-`@` rules are unchanged.
pub fn validate_email_shape(addr: &str) -> Result<(), DomainError> {
    if addr.len() < 3 || addr.len() > 254 {
        return Err(DomainError::Validation(
            "email must be 3..=254 chars".to_string(),
        ));
    }
    for c in addr.chars() {
        if c == '\r'
            || c == '\n'
            || c == '<'
            || c == '>'
            || c == ','
            || c.is_whitespace()
            || c.is_control()
        {
            return Err(DomainError::Validation(
                "malformed email address".to_string(),
            ));
        }
    }
    let mut parts = addr.split('@');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(local), Some(domain), None)
            if !local.is_empty() && !domain.is_empty() && domain.contains('.') =>
        {
            Ok(())
        }
        _ => Err(DomainError::Validation(
            "malformed email address".to_string(),
        )),
    }
}

/// Enforce the per-user webhook cap (upstream `webhookLimit` parity).
pub fn check_webhook_limit(count: usize, limit: usize) -> Result<(), DomainError> {
    if count >= limit {
        return Err(DomainError::Conflict(format!(
            "too many webhooks (limit {limit})"
        )));
    }
    Ok(())
}

/// Honor the role `rateLimitFactor`: a factor of N grants N times the base
/// rate budget. The parent applies this to the webhook endpoint buckets;
/// delivery backoff is intentionally NOT scaled (backoff protects remote
/// hosts, not local budgets).
pub fn apply_rate_limit_factor(base_per_window: u64, factor: u32) -> u64 {
    base_per_window.saturating_mul(factor.max(1) as u64)
}

/// HMAC-SHA256 hex of `body` under `secret` (webhook request signing).
pub fn webhook_signature_hex(secret: &str, body: &[u8]) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
    hex::encode(ring::hmac::sign(&key, body))
}

/// Verify a presented webhook signature (constant-time comparison).
pub fn verify_webhook_signature(secret: &str, body: &[u8], presented_hex: &str) -> bool {
    let expected = webhook_signature_hex(secret, body);
    let a = expected.as_bytes();
    let b = presented_hex.as_bytes();
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Canonical delivery envelope: stable field order for HMAC so receivers
/// can verify `body` bytes exactly as sent.
pub fn webhook_event_body(
    hook_id: &str,
    event_type: &str,
    created_at_ms: i64,
    payload: &serde_json::Value,
) -> Vec<u8> {
    let envelope = serde_json::json!({
        "hookId": hook_id,
        "type": event_type,
        "createdAt": created_at_ms,
        "payload": payload,
    });
    serde_json::to_vec(&envelope).unwrap_or_default()
}

/// Deterministic idempotency key for one hook × one event occurrence, so
/// replays of the producing txn dedupe instead of double-delivering.
pub fn webhook_idempotency_key(hook_id: &str, event_type: &str, dedup_id: &str) -> String {
    format!("webhook:{hook_id}:{event_type}:{dedup_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hook_shape_gates() {
        assert!(validate_hook_name("alerts").is_ok());
        assert!(validate_hook_name("").is_err());
        assert!(validate_hook_name(&"x".repeat(101)).is_err());
        assert!(validate_hook_url("https://hooks.example/in").is_ok());
        assert!(validate_hook_url("http://127.0.0.1:9/x").is_ok());
        assert!(validate_hook_url("ftp://x/y").is_err());
        assert!(validate_hook_url("https://").is_err());
        assert!(validate_trigger("abuseReport", SYSTEM_WEBHOOK_TRIGGERS).is_ok());
        assert!(validate_trigger("mention", SYSTEM_WEBHOOK_TRIGGERS).is_err());
        assert!(validate_trigger("mention", USER_WEBHOOK_TRIGGERS).is_ok());
        assert!(validate_email_shape("mod@example.com").is_ok());
        assert!(validate_email_shape("nope").is_err());
        // SMTP-injection shapes: must fail without weakening the
        // length / single-`@` rules above.
        assert!(validate_email_shape("a@b\r\nBcc: x@y").is_err());
        assert!(validate_email_shape("a@b\r\nDATA\r\n").is_err());
        assert!(validate_email_shape("a@b\rc@example.com").is_err());
        assert!(validate_email_shape("a@b\nc@example.com").is_err());
        assert!(validate_email_shape("a<b@example.com").is_err());
        assert!(validate_email_shape("a>b@example.com").is_err());
        assert!(validate_email_shape("a,b@example.com").is_err());
        assert!(validate_email_shape("a @b.example.com").is_err());
        assert!(validate_email_shape(" a@b.example.com").is_err());
        assert!(validate_email_shape("a@b.example.com ").is_err());
        assert!(validate_email_shape("a@b\x7f.example.com").is_err());
    }

    #[test]
    fn limit_gate() {
        assert!(check_webhook_limit(4, 5).is_ok());
        assert!(check_webhook_limit(5, 5).is_err());
    }

    #[test]
    fn hmac_round_trip_and_tamper() {
        let sig = webhook_signature_hex("s3cret", b"{\"a\":1}");
        assert!(verify_webhook_signature("s3cret", b"{\"a\":1}", &sig));
        assert!(!verify_webhook_signature("s3cret", b"{\"a\":2}", &sig));
        assert!(!verify_webhook_signature("other", b"{\"a\":1}", &sig));
        assert!(!verify_webhook_signature(
            "s3cret",
            b"{\"a\":1}",
            "deadbeef"
        ));
    }
}
