//! Webhooks (system + user) and abuse-report notification recipients:
//! application ops over caller-supplied stores.
//!
//! The store traits live here (not in `ports.rs`, which the parent owns
//! for merging): the redb adapter implements them per the proposed
//! `redb/mod.rs` snippet in the WP3 report. All fan-out is bounded —
//! `plan_*` caps one event to at most `MAX_JOBS_PER_TXN` jobs enqueued in a
//! single `enqueue_jobs` call (one txn on durable backends) — and every
//! delivery job carries a deterministic idempotency key so replays dedupe.
//!
//! Wiring note: `plan_*`/`enqueue_*` take the `JobKind` as a parameter.
//! Pass the proposed `JobKind::WebhookDeliver` / `JobKind::EmailSend`
//! variants once the parent lands the ports snippet; hermetic tests pass a
//! stand-in kind on a recording fake queue and assert on payload bytes.

use serde::{Deserialize, Serialize};

use crate::application::ports::{
    AbuseReportRecipientStore, Clock, IdGenerator, JobQueue, NewJob, PortError,
    SystemWebhookStore, UserWebhookStore, MAX_JOBS_PER_TXN,
};
use crate::domain::id::DomainError;
use crate::domain::webhook::{
    AbuseReportRecipient, SystemWebhook, UserWebhook, check_webhook_limit,
    validate_email_shape, validate_hook_name, validate_hook_secret, validate_hook_url,
    validate_trigger, webhook_idempotency_key, webhook_signature_hex,
    MAX_ABUSE_REPORT_EMAILS_PER_EVENT, SYSTEM_WEBHOOK_TRIGGERS, USER_WEBHOOK_TRIGGERS,
};
use crate::federation::delivery::SafeHttpClient;
use crate::federation::delivery::MAX_AP_JSON_BYTES;
use crate::federation::retry::{
    DeliveryOutcome, DestinationTracker, classify_inbox_status,
};
use crate::services::email::EmailSendPayload;

/// Low-alphanumeric secret alphabet (matches the endpoint `random_code`
/// helper: `EmailService` `secureRndstr` parity).
pub fn mint_webhook_secret() -> String {
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    (0..32)
        .map(|_| {
            let i = (rand::random::<u8>() as usize) % CHARS.len();
            CHARS[i] as char
        })
        .collect()
}

// --- Inputs ------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CreateSystemWebhookInput {
    pub name: String,
    pub url: String,
    pub secret: Option<String>,
    pub trigger: String,
}

#[derive(Debug, Clone, Default)]
pub struct UpdateSystemWebhookInput {
    pub name: Option<String>,
    pub url: Option<String>,
    /// `Some` rotates the secret; `None` leaves it.
    pub secret: Option<String>,
    pub trigger: Option<String>,
    pub is_active: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct CreateUserWebhookInput {
    pub name: String,
    pub url: String,
    pub secret: Option<String>,
    pub trigger: String,
}

#[derive(Debug, Clone, Default)]
pub struct UpdateUserWebhookInput {
    pub name: Option<String>,
    pub url: Option<String>,
    /// `Some` rotates the secret; `None` leaves it.
    pub secret: Option<String>,
    pub trigger: Option<String>,
    pub is_active: Option<bool>,
}

// --- System-webhook ops (admin-gated) -----------------------------------------

fn require_admin(is_admin: bool) -> Result<(), PortError> {
    if is_admin {
        Ok(())
    } else {
        Err(PortError::Validation("admin only".to_string()))
    }
}

pub fn create_system_webhook(
    store: &dyn SystemWebhookStore,
    ids: &dyn IdGenerator,
    clock: &dyn Clock,
    is_admin: bool,
    input: CreateSystemWebhookInput,
) -> Result<SystemWebhook, PortError> {
    require_admin(is_admin)?;
    validate_hook_name(&input.name)?;
    validate_hook_url(&input.url)?;
    validate_trigger(&input.trigger, SYSTEM_WEBHOOK_TRIGGERS)?;
    let secret = match input.secret {
        Some(s) => {
            validate_hook_secret(&s)?;
            s
        }
        None => mint_webhook_secret(),
    };
    let now = clock.now_ms();
    store.insert_system_webhook(SystemWebhook {
        id: ids.generate(),
        name: input.name,
        url: input.url,
        secret,
        trigger: input.trigger,
        is_active: true,
        created_at_ms: now,
        updated_at_ms: now,
    })
}

pub fn show_system_webhook(
    store: &dyn SystemWebhookStore,
    is_admin: bool,
    id: &str,
) -> Result<SystemWebhook, PortError> {
    require_admin(is_admin)?;
    store
        .get_system_webhook(id)?
        .ok_or_else(|| PortError::NotFound("no such webhook".to_string()))
}

pub fn update_system_webhook(
    store: &dyn SystemWebhookStore,
    clock: &dyn Clock,
    is_admin: bool,
    id: &str,
    patch: UpdateSystemWebhookInput,
) -> Result<SystemWebhook, PortError> {
    require_admin(is_admin)?;
    let mut row = store
        .get_system_webhook(id)?
        .ok_or_else(|| PortError::NotFound("no such webhook".to_string()))?;
    if let Some(name) = patch.name {
        validate_hook_name(&name)?;
        row.name = name;
    }
    if let Some(url) = patch.url {
        validate_hook_url(&url)?;
        row.url = url;
    }
    if let Some(secret) = patch.secret {
        validate_hook_secret(&secret)?;
        row.secret = secret;
    }
    if let Some(trigger) = patch.trigger {
        validate_trigger(&trigger, SYSTEM_WEBHOOK_TRIGGERS)?;
        row.trigger = trigger;
    }
    if let Some(active) = patch.is_active {
        row.is_active = active;
    }
    row.updated_at_ms = clock.now_ms();
    store.put_system_webhook(row)
}

pub fn delete_system_webhook(
    store: &dyn SystemWebhookStore,
    is_admin: bool,
    id: &str,
) -> Result<(), PortError> {
    require_admin(is_admin)?;
    if !store.delete_system_webhook(id)? {
        return Err(PortError::NotFound("no such webhook".to_string()));
    }
    Ok(())
}

// --- User-webhook ops (owner-scoped, limit-gated) ------------------------------

pub fn create_user_webhook(
    store: &dyn UserWebhookStore,
    ids: &dyn IdGenerator,
    clock: &dyn Clock,
    owner_id: &str,
    webhook_limit: usize,
    input: CreateUserWebhookInput,
) -> Result<UserWebhook, PortError> {
    validate_hook_name(&input.name)?;
    validate_hook_url(&input.url)?;
    validate_trigger(&input.trigger, USER_WEBHOOK_TRIGGERS)?;
    let count = store.count_user_webhooks(owner_id)?;
    check_webhook_limit(count, webhook_limit)?;
    let secret = match input.secret {
        Some(s) => {
            validate_hook_secret(&s)?;
            s
        }
        None => mint_webhook_secret(),
    };
    let now = clock.now_ms();
    store.insert_user_webhook(UserWebhook {
        id: ids.generate(),
        owner_id: owner_id.to_string(),
        name: input.name,
        url: input.url,
        secret,
        trigger: input.trigger,
        is_active: true,
        created_at_ms: now,
        updated_at_ms: now,
    })
}

fn owned_user_webhook(
    store: &dyn UserWebhookStore,
    owner_id: &str,
    id: &str,
) -> Result<UserWebhook, PortError> {
    let row = store
        .get_user_webhook(id)?
        .ok_or_else(|| PortError::NotFound("no such webhook".to_string()))?;
    if row.owner_id != owner_id {
        return Err(PortError::NotFound("no such webhook".to_string()));
    }
    Ok(row)
}

pub fn show_user_webhook(
    store: &dyn UserWebhookStore,
    owner_id: &str,
    id: &str,
) -> Result<UserWebhook, PortError> {
    owned_user_webhook(store, owner_id, id)
}

pub fn update_user_webhook(
    store: &dyn UserWebhookStore,
    clock: &dyn Clock,
    owner_id: &str,
    id: &str,
    patch: UpdateUserWebhookInput,
) -> Result<UserWebhook, PortError> {
    let mut row = owned_user_webhook(store, owner_id, id)?;
    if let Some(name) = patch.name {
        validate_hook_name(&name)?;
        row.name = name;
    }
    if let Some(url) = patch.url {
        validate_hook_url(&url)?;
        row.url = url;
    }
    if let Some(secret) = patch.secret {
        validate_hook_secret(&secret)?;
        row.secret = secret;
    }
    if let Some(trigger) = patch.trigger {
        validate_trigger(&trigger, USER_WEBHOOK_TRIGGERS)?;
        row.trigger = trigger;
    }
    if let Some(active) = patch.is_active {
        row.is_active = active;
    }
    row.updated_at_ms = clock.now_ms();
    store.put_user_webhook(row)
}

pub fn delete_user_webhook(
    store: &dyn UserWebhookStore,
    owner_id: &str,
    id: &str,
) -> Result<(), PortError> {
    // Ownership first (foreign ids read as not-found, never touched).
    owned_user_webhook(store, owner_id, id)?;
    store.delete_user_webhook(id)?;
    Ok(())
}

// --- Recipient ops (admin-gated) -----------------------------------------------

pub fn create_recipient(
    store: &dyn AbuseReportRecipientStore,
    ids: &dyn IdGenerator,
    clock: &dyn Clock,
    is_admin: bool,
    name: String,
    email: String,
) -> Result<AbuseReportRecipient, PortError> {
    require_admin(is_admin)?;
    validate_hook_name(&name).map_err(|e| match e {
        DomainError::Validation(m) => {
            PortError::Validation(m.replace("webhook name", "recipient name"))
        }
        other => other,
    })?;
    validate_email_shape(&email)?;
    store.insert_recipient(AbuseReportRecipient {
        id: ids.generate(),
        name,
        email,
        is_active: true,
        created_at_ms: clock.now_ms(),
    })
}

pub fn set_recipient_active(
    store: &dyn AbuseReportRecipientStore,
    clock: &dyn Clock,
    is_admin: bool,
    id: &str,
    is_active: bool,
) -> Result<AbuseReportRecipient, PortError> {
    require_admin(is_admin)?;
    let mut row = store
        .get_recipient(id)?
        .ok_or_else(|| PortError::NotFound("no such recipient".to_string()))?;
    row.is_active = is_active;
    row.created_at_ms = row.created_at_ms.min(clock.now_ms());
    store.put_recipient(row)
}

pub fn delete_recipient(
    store: &dyn AbuseReportRecipientStore,
    is_admin: bool,
    id: &str,
) -> Result<(), PortError> {
    require_admin(is_admin)?;
    if !store.delete_recipient(id)? {
        return Err(PortError::NotFound("no such recipient".to_string()));
    }
    Ok(())
}

// --- Durable delivery -----------------------------------------------------------

/// Compact payload stored inside a durable webhook-delivery job (URL +
/// secret ride along so the worker needs no store read; well under the
/// 64 KiB job cap).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookDeliverPayload {
    pub webhook_id: String,
    pub url: String,
    pub secret: String,
    pub event_type: String,
    pub body: serde_json::Value,
    pub is_system: bool,
    pub owner_id: Option<String>,
}

/// One planned job: payload + deterministic idempotency key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedWebhookJob {
    pub payload: WebhookDeliverPayload,
    pub idempotency_key: String,
    pub not_before: i64,
}

/// Plan deliveries for one system event: active subscribers of `trigger`,
/// capped to `max_jobs` (callers pass at most `MAX_JOBS_PER_TXN`).
pub fn plan_system_event(
    store: &dyn SystemWebhookStore,
    trigger: &str,
    event_type: &str,
    payload: &serde_json::Value,
    dedup_id: &str,
    now_ms: i64,
    max_jobs: usize,
) -> Result<Vec<QueuedWebhookJob>, PortError> {
    validate_trigger(trigger, SYSTEM_WEBHOOK_TRIGGERS)?;
    let cap = max_jobs.min(MAX_JOBS_PER_TXN);
    let mut out = Vec::new();
    for hook in store.active_system_webhooks_for(trigger, cap)? {
        if !hook.is_active {
            continue;
        }
        let body_value = serde_json::json!({
            "hookId": hook.id,
            "type": event_type,
            "createdAt": now_ms,
            "payload": payload,
        });
        out.push(QueuedWebhookJob {
            payload: WebhookDeliverPayload {
                webhook_id: hook.id.clone(),
                url: hook.url.clone(),
                secret: hook.secret.clone(),
                event_type: event_type.to_string(),
                body: body_value,
                is_system: true,
                owner_id: None,
            },
            idempotency_key: webhook_idempotency_key(&hook.id, event_type, dedup_id),
            not_before: now_ms,
        });
        if out.len() >= cap {
            break;
        }
    }
    Ok(out)
}

/// Plan deliveries for one user event (owner-scoped subscriptions).
pub fn plan_user_event(
    store: &dyn UserWebhookStore,
    owner_id: &str,
    trigger: &str,
    event_type: &str,
    payload: &serde_json::Value,
    dedup_id: &str,
    now_ms: i64,
    max_jobs: usize,
) -> Result<Vec<QueuedWebhookJob>, PortError> {
    validate_trigger(trigger, USER_WEBHOOK_TRIGGERS)?;
    let cap = max_jobs.min(MAX_JOBS_PER_TXN);
    let mut out = Vec::new();
    for hook in store.active_user_webhooks_for(owner_id, trigger, cap)? {
        if !hook.is_active {
            continue;
        }
        let body_value = serde_json::json!({
            "hookId": hook.id,
            "type": event_type,
            "createdAt": now_ms,
            "payload": payload,
        });
        out.push(QueuedWebhookJob {
            payload: WebhookDeliverPayload {
                webhook_id: hook.id.clone(),
                url: hook.url.clone(),
                secret: hook.secret.clone(),
                event_type: event_type.to_string(),
                body: body_value,
                is_system: false,
                owner_id: Some(owner_id.to_string()),
            },
            idempotency_key: webhook_idempotency_key(&hook.id, event_type, dedup_id),
            not_before: now_ms,
        });
        if out.len() >= cap {
            break;
        }
    }
    Ok(out)
}

/// Wrap one planned job in a durable `NewJob` of the caller's kind.
pub fn new_webhook_job(
    kind: crate::application::ports::JobKind,
    job: &QueuedWebhookJob,
) -> Result<NewJob, PortError> {
    NewJob::new_json(kind, &job.payload, job.idempotency_key.clone(), job.not_before)
}

/// Enqueue planned jobs in ONE `enqueue_jobs` call (one txn on durable
/// backends). At most `MAX_JOBS_PER_TXN` per call; larger plans must be
/// split by the caller with a continuation plan.
pub fn enqueue_planned(
    queue: &dyn JobQueue,
    kind: crate::application::ports::JobKind,
    jobs: &[QueuedWebhookJob],
) -> Result<(), PortError> {
    if jobs.len() > MAX_JOBS_PER_TXN {
        return Err(PortError::Validation(format!(
            "webhook fan-out {} exceeds per-txn cap {MAX_JOBS_PER_TXN}",
            jobs.len()
        )));
    }
    let mut batch = Vec::with_capacity(jobs.len());
    for job in jobs {
        batch.push(new_webhook_job(kind, job)?);
    }
    queue.enqueue_jobs(batch)
}

/// Execute one `WebhookDeliver` job: cooldown-aware signed POST reusing
/// the federation outbox's classification/backoff discipline
/// (`classify_inbox_status` + `DestinationTracker`). SSRF is enforced by
/// `SafeHttpClient`; a blocked URL is a permanent failure (quarantine,
/// never retry).
pub fn execute_webhook_deliver(
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    base_backoff_ms: i64,
    now_ms: i64,
    payload: &WebhookDeliverPayload,
) -> DeliveryOutcome {
    if let Some(until) = tracker.cooldown_until(&payload.url, now_ms) {
        return DeliveryOutcome::Deferred { until_ms: until };
    }
    let body = serde_json::to_vec(&payload.body).unwrap_or_default();
    let signature = webhook_signature_hex(&payload.secret, &body);
    let headers = [
        ("Content-Type", "application/json"),
        ("X-Pinetree-Hook-Id", payload.webhook_id.as_str()),
        ("X-Pinetree-Hook-Signature", signature.as_str()),
    ];
    match client.send("POST", &payload.url, &headers, Some(&body), MAX_AP_JSON_BYTES) {
        Ok(resp) => {
            let outcome = classify_inbox_status(resp.status, "webhook", None, now_ms);
            match &outcome {
                DeliveryOutcome::Delivered => tracker.record_success(&payload.url),
                DeliveryOutcome::PermanentFailure(_) => tracker.record_success(&payload.url),
                DeliveryOutcome::TransientFailure { .. } => {
                    tracker.record_failure(&payload.url, now_ms, base_backoff_ms);
                }
                DeliveryOutcome::Deferred { .. } | DeliveryOutcome::Split { .. } => {}
            }
            outcome
        }
        Err(e) => {
            if e.kind.is_transient() {
                tracker.record_failure(&payload.url, now_ms, base_backoff_ms);
                DeliveryOutcome::TransientFailure {
                    reason: e.to_string(),
                    retry_after_ms: None,
                }
            } else {
                DeliveryOutcome::PermanentFailure(e.to_string())
            }
        }
    }
}

// --- Abuse-report email fan-out --------------------------------------------------

/// One planned email job (single recipient per job: replays re-send the
/// same bytes under the same key).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedEmailJob {
    pub payload: EmailSendPayload,
    pub idempotency_key: String,
    pub not_before: i64,
}

/// Plan abuse-report notification emails: one job per active recipient,
/// capped to `MAX_ABUSE_REPORT_EMAILS_PER_EVENT`.
pub fn plan_abuse_report_emails(
    store: &dyn AbuseReportRecipientStore,
    report_id: &str,
    summary: &str,
    now_ms: i64,
) -> Result<Vec<QueuedEmailJob>, PortError> {
    let emails = store.active_recipient_emails(MAX_ABUSE_REPORT_EMAILS_PER_EVENT)?;
    let subject = format!("Abuse report {report_id} needs review");
    let body = format!("A new abuse report needs review.\n\nReport: {report_id}\n{summary}\n");
    Ok(emails
        .into_iter()
        .take(MAX_ABUSE_REPORT_EMAILS_PER_EVENT)
        .map(|email| QueuedEmailJob {
            payload: EmailSendPayload {
                to: vec![email.clone()],
                subject: subject.clone(),
                body: body.clone(),
                dedup_key: format!("abuse-report:{report_id}:{email}"),
            },
            idempotency_key: format!("email:abuse-report:{report_id}:{email}"),
            not_before: now_ms,
        })
        .collect())
}

/// Wrap one planned email job in a durable `NewJob` of the caller's kind.
pub fn new_email_job(
    kind: crate::application::ports::JobKind,
    job: &QueuedEmailJob,
) -> Result<NewJob, PortError> {
    NewJob::new_json(kind, &job.payload, job.idempotency_key.clone(), job.not_before)
}

/// Enqueue planned email jobs in ONE `enqueue_jobs` call.
pub fn enqueue_emails(
    queue: &dyn JobQueue,
    kind: crate::application::ports::JobKind,
    jobs: &[QueuedEmailJob],
) -> Result<(), PortError> {
    if jobs.len() > MAX_JOBS_PER_TXN {
        return Err(PortError::Validation(format!(
            "email fan-out {} exceeds per-txn cap {MAX_JOBS_PER_TXN}",
            jobs.len()
        )));
    }
    let mut batch = Vec::with_capacity(jobs.len());
    for job in jobs {
        batch.push(new_email_job(kind, job)?);
    }
    queue.enqueue_jobs(batch)
}
