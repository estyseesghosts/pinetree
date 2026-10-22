//! Inbound `Flag` (abuse report) handling (moved from `src/federation/inbox.rs`).

use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use serde_json::Value;
use std::sync::Arc;

use super::accepted;
use super::common::resolve_remote_account;
use super::process_inbox;
use crate::application::commands::AppServices;
use crate::application::ports::PortError;

/// Inbound `Flag` (Batch N WS5; upstream `ApInboxService.flag` parity):
/// the actor must be a known remote account; `object` may be a URI, an
/// object with an `id`, or an array of either. The first mentioned LOCAL
/// user becomes the report target; remote-only or empty targets converge
/// quietly (upstream `skip`). Blocked-host actors drop before any state
/// write or dedup record (the op re-checks). Replays converge with no
/// duplicate row (atomic check-and-record + insert).
pub(crate) fn handle_flag(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    fn object_uris(value: &Value, out: &mut Vec<String>) {
        match value {
            Value::String(s) => {
                let s = s.trim();
                if !s.is_empty() {
                    out.push(s.to_string());
                }
            }
            Value::Array(items) => {
                for item in items {
                    object_uris(item, out);
                }
            }
            Value::Object(map) => {
                if let Some(id) = map.get("id").and_then(Value::as_str) {
                    object_uris(&Value::String(id.to_string()), out);
                }
            }
            _ => {}
        }
    }
    let reporter = resolve_remote_account(services, actor_uri)?;
    let mut uris = Vec::new();
    if let Some(object) = activity.get("object") {
        object_uris(object, &mut uris);
    }
    let content = activity.get("content").and_then(Value::as_str);
    services.handle_inbound_flag(activity_id, &reporter.id, &uris, content)?;
    Ok(accepted())
}

#[cfg(test)]
mod flag_tests {
    use super::*;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicU64, Ordering};

    use axum::http::{HeaderName, HeaderValue};
    use serde_json::json;

    use crate::activitypub::signatures::{
        generate_rsa_keypair_2048, private_key_to_pkcs8_pem, public_key_to_spki_pem,
        sign_request_headers,
    };
    use crate::application::ports::{
        ActivityDedup, BlobStore, Clock, IdGenerator, NoopStreamingSink,
    };
    use crate::domain::id::{AidxGenerator, ManualClock};
    use crate::federation::delivery::{
        SafeHttpClient, SafeHttpConfig, format_http_date, upsert_remote_actor_from_doc,
    };
    use crate::media::storage::FsBlobStore;
    use crate::persistence::redb::RedbStore;

    const REPORTER_URI: &str = "https://remote.invalid/users/reporter";

    fn test_services(prefix: &str) -> AppServices {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-flag-{}-{prefix}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_760_400_000_000));
        let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
        let store = RedbStore::open(dir.join("test.redb"), clock.clone(), ids.clone()).unwrap();
        let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.join("blobs")).unwrap());
        AppServices {
            clock,
            ids,
            accounts: store.clone(),
            notes: store.clone(),
            follows: store.clone(),
            blocks: store.clone(),
            mutes: store.clone(),
            reactions: store.clone(),
            favorites: store.clone(),
            emojis: store.clone(),
            antennas: store.clone(),
            antenna_notes: store.clone(),
            polls: store.clone(),
            jobs: store.clone(),
            media: store.clone(),
            blobs: blobs.clone(),
            notifications: store.clone(),
            auth: store.clone(),
            streaming: Arc::new(NoopStreamingSink),
            dedup: store.clone() as Arc<dyn ActivityDedup>,
            host_caps: store.clone(),
            moderation: store.clone(),
            instance_settings: store.clone(),
            invites: store.clone(),
            abuse_reports: store.clone(),
            lists: store.clone(),
            signin: store.clone(),
            password_resets: store.clone(),
            registry: store.clone(),
            clips: store.clone(),
            pages: store.clone(),
            folders: store.clone(),
            system_webhooks: store.clone(),
            user_webhooks: store.clone(),
            abuse_recipients: store.clone(),
            memos: store.clone(),
            thread_mutes: store.clone(),
            federation_http: SafeHttpClient::new(SafeHttpConfig {
                allow_private_ips: true,
                ..Default::default()
            }),
            public_url: "https://pinetree.local".to_string(),
            max_file_size: 10 * 1024 * 1024,
            preserved_usernames: Vec::new(),
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
            note_draft_limit: crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
            scheduled_note_limit: crate::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
            pin_limit: crate::domain::pinning::DEFAULT_PIN_LIMIT,
            default_like: None,
            cache_blobs: blobs.clone(),
            drive_capacity_per_user_bytes: 1 << 30,
            remote_cache_max_bytes: 512 << 20,
            remote_cache_avatar_bytes: 128 << 20,
            remote_cache_preview_bytes: 256 << 20,
            remote_cache_full_bytes: 128 << 20,
            remote_cache_mode: crate::config::RemoteCacheMode::OnDemand,
            remote_cache_avatar_ttl_days: 30,
            remote_cache_preview_ttl_days: 7,
            remote_cache_full_ttl_days: 3,
            remote_cache_warming_enabled: false,
        }
    }

    fn actor_doc(id: &str, username: &str, key_id: &str, pem: &str) -> Value {
        json!({
            "id": id,
            "type": "Person",
            "preferredUsername": username,
            "inbox": format!("{id}/inbox"),
            "publicKey": {"id": key_id, "type": "Key", "owner": id, "publicKeyPem": pem},
        })
    }

    fn deliver(
        services: &AppServices,
        activity: &Value,
        key_id: &str,
        private_pem: &str,
    ) -> Response {
        let body = serde_json::to_vec(activity).unwrap();
        let date = format_http_date(services.clock.now_ms());
        let signed = sign_request_headers(
            "POST",
            "/inbox",
            "pinetree.local",
            &date,
            Some(&body),
            key_id,
            private_pem,
        )
        .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_str("date").unwrap(),
            HeaderValue::from_str(&date).unwrap(),
        );
        headers.insert(
            HeaderName::from_str("host").unwrap(),
            HeaderValue::from_static("pinetree.local"),
        );
        headers.insert(
            HeaderName::from_str("digest").unwrap(),
            HeaderValue::from_str(&signed.digest.unwrap()).unwrap(),
        );
        headers.insert(
            HeaderName::from_str("signature").unwrap(),
            HeaderValue::from_str(&signed.signature).unwrap(),
        );
        process_inbox(services, None, headers, body.into())
    }

    fn flag_activity(id: &str, object: Value, content: &str) -> Value {
        json!({
            "id": id,
            "type": "Flag",
            "actor": REPORTER_URI,
            "object": object,
            "content": content,
        })
    }

    #[test]
    fn flag_stores_report_replay_converges_and_remote_only_skips() {
        let services = Arc::new(test_services("flag"));
        let (private_key, public_key) = generate_rsa_keypair_2048().unwrap();
        let (private_pem, public_pem) = (
            private_key_to_pkcs8_pem(&private_key),
            public_key_to_spki_pem(&public_key),
        );
        let key_id = format!("{REPORTER_URI}#main-key");
        let doc = actor_doc(REPORTER_URI, "reporter", &key_id, &public_pem);
        upsert_remote_actor_from_doc(&services, REPORTER_URI, &doc).expect("honest upsert");
        let alice = services
            .create_account("alice", None)
            .expect("local target");
        let alice_uri = format!("https://pinetree.local/users/{}", alice.id);

        // First delivery: accepted, one report row (target = first local
        // user, reporter = remote actor, comment carries content + URIs).
        let resp = deliver(
            &services,
            &flag_activity(
                "https://remote.invalid/activities/flag-1",
                json!([alice_uri.clone(), "https://remote.invalid/users/stranger"]),
                "spam",
            ),
            &key_id,
            &private_pem,
        );
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let rows = services
            .abuse_reports
            .list(
                &crate::application::ports::AbuseReportFilter {
                    state: None,
                    reporter_origin: "combined".to_string(),
                    target_origin: "combined".to_string(),
                },
                &crate::application::ports::TimelineCursor::with_limit(10),
            )
            .expect("list reports");
        assert_eq!(rows.len(), 1, "one report row: {rows:?}");
        assert_eq!(rows[0].target_account, alice.id);
        assert!(rows[0].comment.contains("spam"), "{}", rows[0].comment);
        assert!(rows[0].comment.contains(&alice_uri), "{}", rows[0].comment);
        assert!(!rows[0].resolved);
        assert!(!rows[0].forwarded);

        // Replay of the same activity: accepted, zero new rows (atomic
        // check-and-record + insert).
        let resp = deliver(
            &services,
            &flag_activity(
                "https://remote.invalid/activities/flag-1",
                json!([alice_uri.clone()]),
                "spam",
            ),
            &key_id,
            &private_pem,
        );
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let rows = services
            .abuse_reports
            .list(
                &crate::application::ports::AbuseReportFilter {
                    state: None,
                    reporter_origin: "combined".to_string(),
                    target_origin: "combined".to_string(),
                },
                &crate::application::ports::TimelineCursor::with_limit(10),
            )
            .expect("list reports");
        assert_eq!(rows.len(), 1, "no duplicate row: {rows:?}");

        // Remote-only targets converge quietly (upstream `skip`).
        let resp = deliver(
            &services,
            &flag_activity(
                "https://remote.invalid/activities/flag-2",
                json!(["https://remote.invalid/users/stranger"]),
                "noise",
            ),
            &key_id,
            &private_pem,
        );
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
    }
}

// ---------------------------------------------------------------------------
// Slice 2 (C1 key ownership) regression tests
// ---------------------------------------------------------------------------
