// ---------------------------------------------------------------------------
// Inbound `Flag` (Batch N WS5; upstream `ApInboxService.flag` parity)
// ---------------------------------------------------------------------------

/// Slice B end-to-end (`docs/replies.md` §6): a reply to an unknown parent
/// is stored pending (never flattened), withheld from reads, and linked
/// exactly once when the parent later arrives — driven by the durable
/// resolver jobs, with zero network I/O in this hermetic flow.
#[cfg(test)]
mod reply_resolution_tests {
    use super::super::super::process_inbox;
    use crate::application::commands::AppServices;
    use axum::http::{HeaderMap, StatusCode};
    use serde_json::Value;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use axum::http::{HeaderName, HeaderValue};
    use serde_json::json;

    use crate::activitypub::signatures::{
        generate_rsa_keypair_2048, private_key_to_pkcs8_pem, public_key_to_spki_pem,
        sign_request_headers,
    };
    use crate::application::ports::{
        ActivityDedup, BlobStore, Clock, IdGenerator, Job, JobKind, NoopStreamingSink, PortError,
        ReplyParentResolvePayload,
    };
    use crate::domain::id::{AidxGenerator, ManualClock};
    use crate::federation::delivery::{
        SafeHttpClient, SafeHttpConfig, format_http_date, upsert_remote_actor_from_doc,
    };
    use crate::federation::retry::DestinationTracker;
    use crate::jobs::worker::JobWorker;
    use crate::media::storage::FsBlobStore;
    use crate::media::transcode::TranscodeConfig;
    use crate::persistence::redb::RedbStore;
    use crate::services::email::{SmtpConfig, SmtpEmailSender};

    const ROCKY_URI: &str = "https://remote.invalid/users/rocky";
    const PARENT_URI: &str = "https://remote.invalid/notes/parent-1";

    fn test_services(prefix: &str) -> AppServices {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-reply-resolve-{}-{prefix}-{}",
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

    fn signed_inbox_headers(
        services: &AppServices,
        path: &str,
        body: &[u8],
        key_id: &str,
        private_pem: &str,
    ) -> HeaderMap {
        let date = format_http_date(services.clock.now_ms());
        let host = "pinetree.local";
        let signed =
            sign_request_headers("POST", path, host, &date, Some(body), key_id, private_pem)
                .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_str("date").unwrap(),
            HeaderValue::from_str(&date).unwrap(),
        );
        headers.insert(
            HeaderName::from_str("host").unwrap(),
            HeaderValue::from_str(host).unwrap(),
        );
        headers.insert(
            HeaderName::from_str("digest").unwrap(),
            HeaderValue::from_str(&signed.digest.unwrap()).unwrap(),
        );
        headers.insert(
            HeaderName::from_str("signature").unwrap(),
            HeaderValue::from_str(&signed.signature).unwrap(),
        );
        headers
    }

    fn deliver(services: &AppServices, activity: &Value, key_id: &str, private_pem: &str) {
        let body = serde_json::to_vec(activity).unwrap();
        let headers = signed_inbox_headers(services, "/inbox", &body, key_id, private_pem);
        let resp = process_inbox(services, None, headers, body.into());
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
    }

    fn worker_for(services: &Arc<AppServices>) -> JobWorker {
        JobWorker {
            clock: services.clock.clone(),
            ids: services.ids.clone(),
            jobs: services.jobs.clone(),
            notifications: services.notifications.clone(),
            streaming: services.streaming.clone(),
            services: services.clone(),
            delivery_tracker: Arc::new(DestinationTracker::new()),
            lease_duration_ms: 60_000,
            base_backoff_ms: 10,
            transcode_config: TranscodeConfig::default(),
            email_sender: Arc::new(SmtpEmailSender::new(SmtpConfig::default())),
        }
    }

    /// Claim every pending job, returning them oldest-first. Claimed jobs
    /// stay leased to this caller; consumers `complete` what they inspect.
    fn drain_claims(services: &AppServices) -> Vec<Job> {
        let mut out = Vec::new();
        while let Some(job) = services.jobs.claim_next(60_000).expect("claim") {
            out.push(job);
        }
        out
    }

    #[test]
    fn unknown_parent_stays_pending_then_links_on_arrival() {
        // The reply coalescer is process-global: clear it so parallel tests
        // sharing PARENT_URI cannot suppress this test's trigger job.
        crate::federation::clear_ephemeral_caches_for_tests();
        let services = Arc::new(test_services("pending-arrival"));
        let (priv_key, pub_key) = generate_rsa_keypair_2048().unwrap();
        let (private_pem, public_pem) = (
            private_key_to_pkcs8_pem(&priv_key),
            public_key_to_spki_pem(&pub_key),
        );
        let key_id = format!("{ROCKY_URI}#main-key");
        upsert_remote_actor_from_doc(
            &services,
            ROCKY_URI,
            &actor_doc(ROCKY_URI, "rocky", &key_id, &public_pem),
        )
        .expect("honest upsert");

        // 1. Reply to an unknown parent: accepted, stored pending — never a
        // root — with its resolver job committed atomically.
        let reply_uri = "https://remote.invalid/notes/reply-1";
        deliver(
            &services,
            &json!({
                "id": "https://remote.invalid/activities/create-reply-1",
                "type": "Create",
                "actor": ROCKY_URI,
                "object": {
                    "id": reply_uri,
                    "type": "Note",
                    "attributedTo": ROCKY_URI,
                    "content": "a reply to a not-yet-seen note",
                    "inReplyTo": PARENT_URI,
                    "to": ["https://www.w3.org/ns/activitystreams#Public"],
                },
            }),
            &key_id,
            &private_pem,
        );
        let reply = services
            .notes
            .find_by_uri(reply_uri)
            .expect("find reply")
            .expect("reply stored, not dropped");
        assert_eq!(reply.reply_id, None, "no fabricated parent link");
        assert_eq!(
            reply.reply_parent_uri.as_deref(),
            Some(PARENT_URI),
            "unresolved inReplyTo preserved"
        );
        assert!(
            matches!(
                services.notes_show(&reply.id, None),
                Err(PortError::NotFound(_))
            ),
            "pending reply withheld from note detail"
        );
        let jobs = drain_claims(&services);
        assert_eq!(
            jobs.len(),
            3,
            "streaming + resolver + antenna-match committed atomically"
        );
        let resolve_job = jobs
            .iter()
            .find(|j| j.kind == JobKind::ReplyParentResolve)
            .expect("resolver job enqueued for the pending URI");
        let payload: ReplyParentResolvePayload =
            serde_json::from_slice(&resolve_job.payload).expect("resolver payload decodes");
        assert_eq!(payload.parent_uri, PARENT_URI);
        // Simulate the resolver having run while the parent was missing
        // (retry behavior itself is covered in conformance R10): consume
        // both jobs so the arrival below starts clean.
        for job in &jobs {
            services.jobs.complete(&job.id).expect("consume");
        }

        // 2. The parent arrives: accepted, and its arrival enqueues a fresh
        // resolver for the waiting child.
        deliver(
            &services,
            &json!({
                "id": "https://remote.invalid/activities/create-parent-1",
                "type": "Create",
                "actor": ROCKY_URI,
                "object": {
                    "id": PARENT_URI,
                    "type": "Note",
                    "attributedTo": ROCKY_URI,
                    "content": "the parent, arriving late",
                    "to": ["https://www.w3.org/ns/activitystreams#Public"],
                },
            }),
            &key_id,
            &private_pem,
        );
        let parent = services
            .notes
            .find_by_uri(PARENT_URI)
            .expect("find parent")
            .expect("parent stored");
        let arrival_jobs = drain_claims(&services);
        assert!(
            arrival_jobs
                .iter()
                .any(|j| j.kind == JobKind::ReplyParentResolve),
            "parent arrival schedules resolution for waiting children"
        );
        for job in &arrival_jobs {
            services.jobs.complete(&job.id).expect("consume");
        }
        // Re-enqueue one resolver (as the arrival did) and run it through
        // the real worker: the cached parent links with zero network I/O.
        let resolve =
            crate::application::commands::inbound::replies::build_reply_parent_resolve_job(
                &services, PARENT_URI,
            )
            .expect("resolver job builds");
        services.jobs.enqueue_jobs(vec![resolve]).expect("enqueue");
        let worker = worker_for(&services);
        assert!(worker.process_one().expect("process"), "resolver claimed");
        assert!(
            !worker.process_one().expect("drain"),
            "no work remains after linking"
        );

        // 3. Linked exactly once: reply_id set, pending cleared, counter
        // bumped, index swapped, note visible again.
        let linked = services
            .notes
            .find_by_uri(reply_uri)
            .expect("find linked")
            .expect("reply still stored");
        assert_eq!(linked.reply_id.as_deref(), Some(parent.id.as_str()));
        assert_eq!(linked.reply_parent_uri, None);
        let parent_after = services
            .notes
            .find_by_uri(PARENT_URI)
            .expect("find parent again")
            .expect("parent");
        assert_eq!(parent_after.replies_count, 1, "counted exactly once");
        assert!(
            services
                .notes
                .pending_reply_children(PARENT_URI, 10)
                .expect("pending after link")
                .is_empty(),
            "pending row gone after link"
        );
        assert!(
            services.notes_show(&reply.id, None).is_ok(),
            "linked reply readable again"
        );
    }
}

// ---------------------------------------------------------------------------
// `Update(Note)` round-trip (§8; Sharkey `notes/edit` federation parity)
// ---------------------------------------------------------------------------
