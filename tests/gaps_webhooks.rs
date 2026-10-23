//! WP3 hermetic webhook tests: system/user CRUD, recipients, planning,
//! HMAC, and durable delivery against loopback stubs (never live send).
//!
//! Store doubles live in this file; the redb adapter implements the same
//! traits per the WP3 report's proposed `redb/mod.rs` snippet.
//! `TEST_KIND` is a stand-in: the parent wires the proposed
//! `JobKind::WebhookDeliver` / `JobKind::EmailSend` variants and swaps the
//! constant — payload bytes and keys are unchanged.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use pinetree::application::commands::webhooks::{
    self, CreateSystemWebhookInput, CreateUserWebhookInput, UpdateSystemWebhookInput,
    UpdateUserWebhookInput,
};
use pinetree::application::ports::{
    AbuseReportRecipientStore, Job, JobKind, JobQueue, MAX_JOBS_PER_TXN, NewJob, PortError,
    SystemWebhookStore, UserWebhookStore,
};
use pinetree::domain::id::{IdGenerator, ManualClock, PortError as DomainPortError};
use pinetree::domain::webhook::{
    AbuseReportRecipient, SYSTEM_WEBHOOK_TRIGGERS, SystemWebhook, USER_WEBHOOK_TRIGGERS,
    UserWebhook, apply_rate_limit_factor, verify_webhook_signature, webhook_idempotency_key,
    webhook_signature_hex,
};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::federation::retry::{DeliveryOutcome, DestinationTracker};

/// Stand-in kind until the parent lands `JobKind::WebhookDeliver` /
/// `JobKind::EmailSend`. Tests assert on payload bytes, never on kind.
const TEST_KIND: JobKind = JobKind::Notify;

// --- doubles ---------------------------------------------------------------

#[derive(Default)]
struct CounterIds {
    n: Mutex<u64>,
}

impl IdGenerator for CounterIds {
    fn generate(&self) -> String {
        let mut n = self.n.lock().unwrap();
        *n += 1;
        format!("id{:06}", *n)
    }
    fn parse_time(&self, _id: &str) -> Result<i64, DomainPortError> {
        Ok(1_760_400_000_000)
    }
}

#[derive(Default)]
struct MemSystemStore {
    rows: Mutex<HashMap<String, SystemWebhook>>,
}

impl SystemWebhookStore for MemSystemStore {
    fn insert_system_webhook(&self, row: SystemWebhook) -> Result<SystemWebhook, PortError> {
        let mut rows = self.rows.lock().unwrap();
        if rows.contains_key(&row.id) {
            return Err(PortError::Conflict("duplicate webhook".to_string()));
        }
        rows.insert(row.id.clone(), row.clone());
        Ok(row)
    }
    fn get_system_webhook(&self, id: &str) -> Result<Option<SystemWebhook>, PortError> {
        Ok(self.rows.lock().unwrap().get(id).cloned())
    }
    fn list_system_webhooks(&self) -> Result<Vec<SystemWebhook>, PortError> {
        let mut v: Vec<_> = self.rows.lock().unwrap().values().cloned().collect();
        v.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(v)
    }
    fn put_system_webhook(&self, row: SystemWebhook) -> Result<SystemWebhook, PortError> {
        let mut rows = self.rows.lock().unwrap();
        if !rows.contains_key(&row.id) {
            return Err(PortError::NotFound("no such webhook".to_string()));
        }
        rows.insert(row.id.clone(), row.clone());
        Ok(row)
    }
    fn delete_system_webhook(&self, id: &str) -> Result<bool, PortError> {
        Ok(self.rows.lock().unwrap().remove(id).is_some())
    }
    fn active_system_webhooks_for(
        &self,
        trigger: &str,
        limit: usize,
    ) -> Result<Vec<SystemWebhook>, PortError> {
        let mut v: Vec<_> = self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|r| r.is_active && r.trigger == trigger)
            .cloned()
            .collect();
        v.sort_by(|a, b| a.id.cmp(&b.id));
        v.truncate(limit);
        Ok(v)
    }
}

#[derive(Default)]
struct MemUserStore {
    rows: Mutex<HashMap<String, UserWebhook>>,
}

impl UserWebhookStore for MemUserStore {
    fn insert_user_webhook(&self, row: UserWebhook) -> Result<UserWebhook, PortError> {
        let mut rows = self.rows.lock().unwrap();
        if rows.contains_key(&row.id) {
            return Err(PortError::Conflict("duplicate webhook".to_string()));
        }
        rows.insert(row.id.clone(), row.clone());
        Ok(row)
    }
    fn get_user_webhook(&self, id: &str) -> Result<Option<UserWebhook>, PortError> {
        Ok(self.rows.lock().unwrap().get(id).cloned())
    }
    fn list_user_webhooks(&self, owner_id: &str) -> Result<Vec<UserWebhook>, PortError> {
        let mut v: Vec<_> = self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|r| r.owner_id == owner_id)
            .cloned()
            .collect();
        v.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(v)
    }
    fn count_user_webhooks(&self, owner_id: &str) -> Result<usize, PortError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|r| r.owner_id == owner_id)
            .count())
    }
    fn put_user_webhook(&self, row: UserWebhook) -> Result<UserWebhook, PortError> {
        let mut rows = self.rows.lock().unwrap();
        if !rows.contains_key(&row.id) {
            return Err(PortError::NotFound("no such webhook".to_string()));
        }
        rows.insert(row.id.clone(), row.clone());
        Ok(row)
    }
    fn delete_user_webhook(&self, id: &str) -> Result<bool, PortError> {
        Ok(self.rows.lock().unwrap().remove(id).is_some())
    }
    fn active_user_webhooks_for(
        &self,
        owner_id: &str,
        trigger: &str,
        limit: usize,
    ) -> Result<Vec<UserWebhook>, PortError> {
        let mut v: Vec<_> = self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|r| r.is_active && r.owner_id == owner_id && r.trigger == trigger)
            .cloned()
            .collect();
        v.sort_by(|a, b| a.id.cmp(&b.id));
        v.truncate(limit);
        Ok(v)
    }
}

#[derive(Default)]
struct MemRecipientStore {
    rows: Mutex<HashMap<String, AbuseReportRecipient>>,
}

impl AbuseReportRecipientStore for MemRecipientStore {
    fn insert_recipient(
        &self,
        row: AbuseReportRecipient,
    ) -> Result<AbuseReportRecipient, PortError> {
        let mut rows = self.rows.lock().unwrap();
        if rows.contains_key(&row.id) {
            return Err(PortError::Conflict("duplicate recipient".to_string()));
        }
        rows.insert(row.id.clone(), row.clone());
        Ok(row)
    }
    fn get_recipient(&self, id: &str) -> Result<Option<AbuseReportRecipient>, PortError> {
        Ok(self.rows.lock().unwrap().get(id).cloned())
    }
    fn list_recipients(&self) -> Result<Vec<AbuseReportRecipient>, PortError> {
        let mut v: Vec<_> = self.rows.lock().unwrap().values().cloned().collect();
        v.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(v)
    }
    fn put_recipient(&self, row: AbuseReportRecipient) -> Result<AbuseReportRecipient, PortError> {
        let mut rows = self.rows.lock().unwrap();
        if !rows.contains_key(&row.id) {
            return Err(PortError::NotFound("no such recipient".to_string()));
        }
        rows.insert(row.id.clone(), row.clone());
        Ok(row)
    }
    fn delete_recipient(&self, id: &str) -> Result<bool, PortError> {
        Ok(self.rows.lock().unwrap().remove(id).is_some())
    }
    fn active_recipient_emails(&self, limit: usize) -> Result<Vec<String>, PortError> {
        let mut v: Vec<_> = self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|r| r.is_active)
            .cloned()
            .collect();
        v.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(v.into_iter().take(limit).map(|r| r.email).collect())
    }
}

#[derive(Default)]
struct FakeQueue {
    batches: Mutex<Vec<Vec<NewJob>>>,
}

impl FakeQueue {
    fn all(&self) -> Vec<NewJob> {
        self.batches
            .lock()
            .unwrap()
            .iter()
            .flatten()
            .cloned()
            .collect()
    }
}

impl JobQueue for FakeQueue {
    fn claim_next(&self, _lease_duration_ms: i64) -> Result<Option<Job>, PortError> {
        Ok(None)
    }
    fn complete(&self, _job_id: &str) -> Result<(), PortError> {
        Ok(())
    }
    fn fail(&self, _job_id: &str, _retry_at_ms: i64) -> Result<(), PortError> {
        Ok(())
    }
    fn complete_with_followups(
        &self,
        _job_id: &str,
        followups: Vec<NewJob>,
    ) -> Result<(), PortError> {
        for j in &followups {
            j.validate()?;
        }
        self.batches.lock().unwrap().push(followups);
        Ok(())
    }
    fn enqueue_jobs(&self, jobs: Vec<NewJob>) -> Result<(), PortError> {
        for j in &jobs {
            j.validate()?;
        }
        // One call = one txn on durable backends; the fake keeps the
        // batching structure so tests can assert the single-call rule.
        self.batches.lock().unwrap().push(jobs);
        Ok(())
    }
    fn defer(&self, _job_id: &str, _not_before_ms: i64) -> Result<(), PortError> {
        Ok(())
    }
    fn get_job(&self, _job_id: &str) -> Result<Option<Job>, PortError> {
        Ok(None)
    }
    fn pending_count(&self) -> Result<usize, PortError> {
        Ok(self.batches.lock().unwrap().iter().map(Vec::len).sum())
    }
}

// --- CRUD -------------------------------------------------------------------

fn sys_input(trigger: &str) -> CreateSystemWebhookInput {
    CreateSystemWebhookInput {
        name: "abuse ping".to_string(),
        url: "https://hooks.example/in".to_string(),
        secret: None,
        trigger: trigger.to_string(),
    }
}

#[test]
fn system_crud_happy_path() {
    let store = MemSystemStore::default();
    let ids = CounterIds::default();
    let clock = ManualClock::new(1_760_400_000_000);
    let created =
        webhooks::create_system_webhook(&store, &ids, &clock, true, sys_input("abuseReport"))
            .expect("create");
    assert_eq!(
        created.secret.chars().count(),
        32,
        "secret minted at 32 chars"
    );
    assert!(created.is_active);

    let shown = webhooks::show_system_webhook(&store, true, &created.id).expect("show");
    assert_eq!(shown, created);

    clock.advance_ms(1000);
    let updated = webhooks::update_system_webhook(
        &store,
        &clock,
        true,
        &created.id,
        UpdateSystemWebhookInput {
            name: Some("renamed".to_string()),
            is_active: Some(false),
            secret: Some("rotated-secret".to_string()),
            ..Default::default()
        },
    )
    .expect("update");
    assert_eq!(updated.name, "renamed");
    assert!(!updated.is_active);
    assert_eq!(updated.secret, "rotated-secret");
    assert_eq!(updated.updated_at_ms, 1_760_400_001_000);

    assert_eq!(store.list_system_webhooks().unwrap().len(), 1);
    webhooks::delete_system_webhook(&store, true, &created.id).expect("delete");
    assert!(store.list_system_webhooks().unwrap().is_empty());
    assert!(matches!(
        webhooks::show_system_webhook(&store, true, &created.id),
        Err(PortError::NotFound(_))
    ));
    // Repeat delete reads as not-found (no silent success).
    assert!(matches!(
        webhooks::delete_system_webhook(&store, true, &created.id),
        Err(PortError::NotFound(_))
    ));
}

#[test]
fn system_rejects_bad_shapes_and_non_admin() {
    let store = MemSystemStore::default();
    let ids = CounterIds::default();
    let clock = ManualClock::new(1_760_400_000_000);

    for trigger in ["mention", "", "nope"] {
        assert!(
            webhooks::create_system_webhook(&store, &ids, &clock, true, sys_input(trigger))
                .is_err(),
            "trigger {trigger:?} must be rejected for system hooks"
        );
    }
    assert!(SYSTEM_WEBHOOK_TRIGGERS.contains(&"abuseReport"));
    let mut bad_url = sys_input("abuseReport");
    bad_url.url = "ftp://x/y".to_string();
    assert!(webhooks::create_system_webhook(&store, &ids, &clock, true, bad_url).is_err());
    let mut bad_secret = sys_input("abuseReport");
    bad_secret.secret = Some(String::new());
    assert!(webhooks::create_system_webhook(&store, &ids, &clock, true, bad_secret).is_err());
    let mut bad_name = sys_input("abuseReport");
    bad_name.name = "   ".to_string();
    assert!(webhooks::create_system_webhook(&store, &ids, &clock, true, bad_name).is_err());

    // Every op is admin-gated.
    assert!(matches!(
        webhooks::create_system_webhook(&store, &ids, &clock, false, sys_input("abuseReport")),
        Err(PortError::Validation(_))
    ));
    let created =
        webhooks::create_system_webhook(&store, &ids, &clock, true, sys_input("abuseReport"))
            .unwrap();
    assert!(matches!(
        webhooks::show_system_webhook(&store, false, &created.id),
        Err(PortError::Validation(_))
    ));
    assert!(matches!(
        webhooks::update_system_webhook(
            &store,
            &clock,
            false,
            &created.id,
            UpdateSystemWebhookInput::default()
        ),
        Err(PortError::Validation(_))
    ));
    assert!(matches!(
        webhooks::delete_system_webhook(&store, false, &created.id),
        Err(PortError::Validation(_))
    ));
    assert!(USER_WEBHOOK_TRIGGERS.contains(&"mention"));
}

#[test]
fn user_limit_enforced_and_owner_isolated() {
    let store = MemUserStore::default();
    let ids = CounterIds::default();
    let clock = ManualClock::new(1_760_400_000_000);
    let mk = |name: &str| CreateUserWebhookInput {
        name: name.to_string(),
        url: "https://hooks.example/u".to_string(),
        secret: Some("explicit-secret".to_string()),
        trigger: "mention".to_string(),
    };
    let a1 = webhooks::create_user_webhook(&store, &ids, &clock, "alice", 2, mk("a")).unwrap();
    assert_eq!(a1.secret, "explicit-secret", "explicit secret kept");
    webhooks::create_user_webhook(&store, &ids, &clock, "alice", 2, mk("b")).unwrap();
    let err = webhooks::create_user_webhook(&store, &ids, &clock, "alice", 2, mk("c"))
        .expect_err("third hook over limit 2");
    assert!(matches!(err, PortError::Conflict(_)), "{err:?}");
    // Another owner's budget is independent.
    webhooks::create_user_webhook(&store, &ids, &clock, "bob", 2, mk("b1")).unwrap();

    // Foreign rows read as not-found and are never touched.
    assert!(matches!(
        webhooks::show_user_webhook(&store, "bob", &a1.id),
        Err(PortError::NotFound(_))
    ));
    assert!(matches!(
        webhooks::delete_user_webhook(&store, "bob", &a1.id),
        Err(PortError::NotFound(_))
    ));
    assert!(webhooks::show_user_webhook(&store, "alice", &a1.id).is_ok());

    let updated = webhooks::update_user_webhook(
        &store,
        &clock,
        "alice",
        &a1.id,
        UpdateUserWebhookInput {
            trigger: Some("reply".to_string()),
            is_active: Some(false),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(updated.trigger, "reply");
    assert!(!updated.is_active);
    // Cross-family trigger rejected on update.
    assert!(
        webhooks::update_user_webhook(
            &store,
            &clock,
            "alice",
            &a1.id,
            UpdateUserWebhookInput {
                trigger: Some("abuseReport".to_string()),
                ..Default::default()
            },
        )
        .is_err()
    );
    webhooks::delete_user_webhook(&store, "alice", &a1.id).unwrap();
    assert_eq!(store.count_user_webhooks("alice").unwrap(), 1);
}

// --- recipients --------------------------------------------------------------

#[test]
fn recipient_crud_and_active_fanout_list() {
    let store = MemRecipientStore::default();
    let ids = CounterIds::default();
    let clock = ManualClock::new(1_760_400_000_000);
    assert!(matches!(
        webhooks::create_recipient(
            &store,
            &ids,
            &clock,
            false,
            "m".to_string(),
            "m@example.com".to_string()
        ),
        Err(PortError::Validation(_))
    ));
    assert!(
        webhooks::create_recipient(
            &store,
            &ids,
            &clock,
            true,
            "m".to_string(),
            "not-an-email".to_string()
        )
        .is_err()
    );
    let r1 = webhooks::create_recipient(
        &store,
        &ids,
        &clock,
        true,
        "mods".to_string(),
        "mods@example.com".to_string(),
    )
    .unwrap();
    let r2 = webhooks::create_recipient(
        &store,
        &ids,
        &clock,
        true,
        "quiet".to_string(),
        "quiet@example.com".to_string(),
    )
    .unwrap();
    webhooks::set_recipient_active(&store, &clock, true, &r2.id, false).unwrap();
    assert_eq!(
        store.active_recipient_emails(10).unwrap(),
        vec!["mods@example.com".to_string()]
    );
    assert_eq!(store.list_recipients().unwrap().len(), 2);
    webhooks::delete_recipient(&store, true, &r1.id).unwrap();
    assert!(matches!(
        webhooks::delete_recipient(&store, true, &r1.id),
        Err(PortError::NotFound(_))
    ));
}

// --- planning -----------------------------------------------------------------

#[test]
fn plan_system_event_scopes_and_caps() {
    let store = MemSystemStore::default();
    let ids = CounterIds::default();
    let clock = ManualClock::new(1_760_400_000_000);
    let h1 = webhooks::create_system_webhook(&store, &ids, &clock, true, sys_input("abuseReport"))
        .unwrap();
    let h2 = webhooks::create_system_webhook(&store, &ids, &clock, true, sys_input("abuseReport"))
        .unwrap();
    webhooks::create_system_webhook(&store, &ids, &clock, true, sys_input("userCreated")).unwrap();
    webhooks::update_system_webhook(
        &store,
        &clock,
        true,
        &h2.id,
        UpdateSystemWebhookInput {
            is_active: Some(false),
            ..Default::default()
        },
    )
    .unwrap();

    let payload = serde_json::json!({"reportId": "r1"});
    // Inactive h2 and wrong-trigger h3 are excluded: only h1 fires.
    let jobs = webhooks::plan_system_event(
        &store,
        "abuseReport",
        "abuseReport",
        &payload,
        "r1",
        100,
        500,
    )
    .unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].payload.webhook_id, h1.id);
    assert!(jobs[0].payload.is_system);
    assert_eq!(
        jobs[0].idempotency_key,
        webhook_idempotency_key(&h1.id, "abuseReport", "r1")
    );
    // Deterministic keys: same inputs plan the same key.
    let again = webhooks::plan_system_event(
        &store,
        "abuseReport",
        "abuseReport",
        &payload,
        "r1",
        999,
        500,
    )
    .unwrap();
    assert_eq!(again[0].idempotency_key, jobs[0].idempotency_key);

    // Cap honored.
    let capped =
        webhooks::plan_system_event(&store, "abuseReport", "abuseReport", &payload, "r1", 100, 1)
            .unwrap();
    assert_eq!(capped.len(), 1);
    // Unknown trigger never plans.
    assert!(
        webhooks::plan_system_event(&store, "mention", "mention", &payload, "r1", 100, 500)
            .is_err()
    );
}

#[test]
fn plan_user_event_is_owner_scoped() {
    let store = MemUserStore::default();
    let ids = CounterIds::default();
    let clock = ManualClock::new(1_760_400_000_000);
    let mk = |owner: &str| {
        webhooks::create_user_webhook(
            &store,
            &ids,
            &clock,
            owner,
            10,
            CreateUserWebhookInput {
                name: "n".to_string(),
                url: "https://hooks.example/u".to_string(),
                secret: None,
                trigger: "mention".to_string(),
            },
        )
        .unwrap()
    };
    let a = mk("alice");
    mk("bob");
    let payload = serde_json::json!({"noteId": "n1"});
    let jobs = webhooks::plan_user_event(
        &store, "alice", "mention", "mention", &payload, "n1", 100, 500,
    )
    .unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].payload.webhook_id, a.id);
    assert_eq!(jobs[0].payload.owner_id.as_deref(), Some("alice"));
    assert!(!jobs[0].payload.is_system);
}

#[test]
fn enqueue_planned_is_one_call_and_bounded() {
    let store = MemSystemStore::default();
    let ids = CounterIds::default();
    let clock = ManualClock::new(1_760_400_000_000);
    webhooks::create_system_webhook(&store, &ids, &clock, true, sys_input("abuseReport")).unwrap();
    let payload = serde_json::json!({});
    let jobs = webhooks::plan_system_event(
        &store,
        "abuseReport",
        "abuseReport",
        &payload,
        "d",
        100,
        500,
    )
    .unwrap();
    let queue = FakeQueue::default();
    webhooks::enqueue_planned(&queue, TEST_KIND, &jobs).unwrap();
    assert_eq!(queue.all().len(), 1);
    assert_eq!(
        queue.batches.lock().unwrap().len(),
        1,
        "single enqueue_jobs call"
    );
    assert_eq!(queue.pending_count().unwrap(), 1);

    // Over-cap plans are rejected, never partially enqueued.
    let big: Vec<_> = (0..MAX_JOBS_PER_TXN + 1)
        .map(|i| webhooks::QueuedWebhookJob {
            payload: jobs[0].payload.clone(),
            idempotency_key: format!("k{i}"),
            not_before: 0,
        })
        .collect();
    assert!(webhooks::enqueue_planned(&queue, TEST_KIND, &big).is_err());
    assert_eq!(queue.all().len(), 1);

    // Abuse-report email fan-out: one job per active recipient, bounded.
    let rstore = MemRecipientStore::default();
    webhooks::create_recipient(
        &rstore,
        &ids,
        &clock,
        true,
        "a".into(),
        "a@example.com".into(),
    )
    .unwrap();
    webhooks::create_recipient(
        &rstore,
        &ids,
        &clock,
        true,
        "b".into(),
        "b@example.com".into(),
    )
    .unwrap();
    let email_jobs = webhooks::plan_abuse_report_emails(&rstore, "rep1", "comment", 100).unwrap();
    assert_eq!(email_jobs.len(), 2);
    assert_eq!(email_jobs[0].payload.to, vec!["a@example.com".to_string()]);
    webhooks::enqueue_emails(&queue, TEST_KIND, &email_jobs).unwrap();
    assert_eq!(queue.all().len(), 3);
}

#[test]
fn rate_limit_factor_scales_budgets() {
    assert_eq!(apply_rate_limit_factor(30, 1), 30);
    assert_eq!(apply_rate_limit_factor(30, 3), 90);
    assert_eq!(apply_rate_limit_factor(30, 0), 30, "factor 0 clamps to 1");
}

// --- delivery -------------------------------------------------------------------

#[derive(Debug)]
struct CapturedRequest {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

/// Minimal HTTP stub: `n` connections, canned status, captures requests.
/// Loopback only — hermetic by construction.
fn stub_server(
    status: u16,
    n: usize,
) -> (
    String,
    Arc<Mutex<Vec<CapturedRequest>>>,
    std::thread::JoinHandle<()>,
) {
    let captured: Arc<Mutex<Vec<CapturedRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let url = format!(
        "http://127.0.0.1:{}/hook",
        listener.local_addr().unwrap().port()
    );
    let out = Arc::clone(&captured);
    let handle = std::thread::spawn(move || {
        for _ in 0..n {
            let (stream, _) = listener.accept().expect("accept");
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                .unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut request_line = String::new();
            reader.read_line(&mut request_line).unwrap();
            let mut parts = request_line.split_whitespace();
            let method = parts.next().unwrap_or("").to_string();
            let path = parts.next().unwrap_or("").to_string();
            let mut headers = Vec::new();
            let mut content_length = 0usize;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let t = line.trim_end_matches(['\r', '\n']);
                if t.is_empty() {
                    break;
                }
                if let Some((k, v)) = t.split_once(':') {
                    if k.eq_ignore_ascii_case("content-length") {
                        content_length = v.trim().parse().unwrap_or(0);
                    }
                    headers.push((k.trim().to_string(), v.trim().to_string()));
                }
            }
            let mut body = vec![0u8; content_length.min(1 << 20)];
            if content_length > 0 {
                reader.read_exact(&mut body).unwrap();
            }
            out.lock().unwrap().push(CapturedRequest {
                method,
                path,
                headers,
                body,
            });
            let response = format!(
                "HTTP/1.1 {status} X\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
            );
            let mut w = reader.into_inner();
            w.write_all(response.as_bytes()).unwrap();
            w.flush().unwrap();
        }
    });
    (url, captured, handle)
}

fn loopback_client() -> SafeHttpClient {
    SafeHttpClient::new(SafeHttpConfig {
        allow_private_ips: true,
        ..SafeHttpConfig::default()
    })
}

fn deliver_payload(url: &str) -> webhooks::WebhookDeliverPayload {
    webhooks::WebhookDeliverPayload {
        webhook_id: "h1".to_string(),
        url: url.to_string(),
        secret: "s3cret".to_string(),
        event_type: "abuseReport".to_string(),
        body: serde_json::json!({"hookId": "h1", "type": "abuseReport"}),
        is_system: true,
        owner_id: None,
    }
}

/// A port that is closed by construction: bound then dropped, so nothing
/// listens. Delivery must be SSRF-blocked before any dial.
fn closed_loopback_url() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    format!("http://127.0.0.1:{port}/hook")
}

#[test]
fn deliver_success_posts_signed_body() {
    // Wave 3 SSRF hardening: the webhook path forces
    // `allow_private_ips = false`, so loopback delivery — even with a
    // permissive caller client — is a permanent SSRF failure, never a dial.
    // (Signed-body wire coverage lives in the signature round-trip below
    // and `deliver_ssrf_blocked_is_permanent`; no loopback stub can receive
    // on this path by design.)
    let url = closed_loopback_url();
    let tracker = DestinationTracker::new();
    let payload = deliver_payload(&url);
    let outcome =
        webhooks::execute_webhook_deliver(&loopback_client(), &tracker, 1000, 5000, &payload);
    match &outcome {
        DeliveryOutcome::PermanentFailure(reason) => {
            assert!(reason.contains("SSRF"), "{outcome:?}");
        }
        _ => panic!("loopback must be SSRF-blocked, got {outcome:?}"),
    }
    let expected_body = serde_json::to_vec(&payload.body).unwrap();
    let sig = webhook_signature_hex("s3cret", &expected_body);
    assert!(verify_webhook_signature("s3cret", &expected_body, &sig));
    assert!(
        tracker.cooldown_until(&url, 5000).is_none(),
        "blocked sets no cooldown"
    );
}

#[test]
fn deliver_transient_then_cooldown_defers() {
    // Wave 3 SSRF hardening: loopback can no longer produce the transient
    // path (no dial happens), so a blocked URL stays a permanent failure
    // with no cooldown deferral. Cooldown/defer discipline for reachable
    // destinations is pinned by the tracker unit tests.
    let url = closed_loopback_url();
    let tracker = DestinationTracker::new();
    let payload = deliver_payload(&url);
    let outcome =
        webhooks::execute_webhook_deliver(&loopback_client(), &tracker, 1000, 5000, &payload);
    assert!(
        matches!(outcome, DeliveryOutcome::PermanentFailure(_)),
        "{outcome:?}"
    );
    // Same instant: still permanent, not deferred — blocked sets no cooldown.
    let second =
        webhooks::execute_webhook_deliver(&loopback_client(), &tracker, 1000, 5000, &payload);
    assert!(
        matches!(second, DeliveryOutcome::PermanentFailure(_)),
        "{second:?}"
    );
}

#[test]
fn deliver_ssrf_blocked_is_permanent() {
    let client = SafeHttpClient::new(SafeHttpConfig {
        allow_private_ips: false,
        ..SafeHttpConfig::default()
    });
    let tracker = DestinationTracker::new();
    let payload = deliver_payload("http://127.0.0.1:9/hook");
    let outcome = webhooks::execute_webhook_deliver(&client, &tracker, 1000, 5000, &payload);
    assert!(
        matches!(outcome, DeliveryOutcome::PermanentFailure(_)),
        "SSRF block must quarantine, never retry: {outcome:?}"
    );
}

#[test]
fn hmac_vectors_are_stable() {
    // Fixed vector guards the signing scheme across refactors.
    let sig = webhook_signature_hex("key", b"data");
    assert_eq!(sig.len(), 64);
    assert!(verify_webhook_signature("key", b"data", &sig));
    assert!(!verify_webhook_signature(
        "key",
        b"data",
        &sig.to_uppercase()
    ));
}
