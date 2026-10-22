#[cfg(test)]
mod key_ownership_tests {
    use super::super::super::process_inbox;
    use crate::activitypub::actor::parse_remote_actor;
    use crate::application::commands::AppServices;
    use crate::application::ports::PortError;
    use axum::http::{HeaderMap, StatusCode};
    use serde_json::Value;
    use std::io::{Read, Write};
    use std::net::TcpListener;
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
        ActivityDedup, AuthStore, BlobStore, Clock, IdGenerator, NoopStreamingSink,
    };
    use crate::domain::id::{AidxGenerator, ManualClock};
    use crate::federation::delivery::{
        SafeHttpClient, SafeHttpConfig, format_http_date, resolve_remote_public_key,
        upsert_remote_actor_from_doc,
    };
    use crate::media::storage::FsBlobStore;
    use crate::persistence::redb::RedbStore;

    const VICTIM_URI: &str = "https://victim.invalid/users/victim";
    const ATTACKER_URI: &str = "https://attacker.invalid/users/attacker";

    fn test_services(prefix: &str) -> AppServices {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-key-own-{}-{prefix}-{}",
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

    fn actor_doc(id: &str, username: &str, key_id: &str, owner: Option<&str>, pem: &str) -> Value {
        let mut key = json!({"id": key_id, "type": "Key", "publicKeyPem": pem});
        if let Some(o) = owner {
            key["owner"] = json!(o);
        }
        json!({
            "id": id,
            "type": "Person",
            "preferredUsername": username,
            "inbox": format!("{id}/inbox"),
            "publicKey": key,
        })
    }

    fn keypair_pems() -> (String, String) {
        let (priv_key, pub_key) = generate_rsa_keypair_2048().unwrap();
        (
            private_key_to_pkcs8_pem(&priv_key),
            public_key_to_spki_pem(&pub_key),
        )
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

    #[test]
    fn parse_accepts_honest_key_shapes() {
        let (_, victim_pub) = keypair_pems();
        for key_id in [
            format!("{VICTIM_URI}#main-key"),
            format!("{VICTIM_URI}/publickey"),
            VICTIM_URI.to_string(),
        ] {
            let doc = actor_doc(VICTIM_URI, "victim", &key_id, Some(VICTIM_URI), &victim_pub);
            let parsed = parse_remote_actor(&doc).expect("honest key shape must parse");
            assert_eq!(parsed.public_key_id, key_id);
            // Bare actor id without `owner` (some implementations omit it).
            let doc = actor_doc(VICTIM_URI, "victim", &key_id, None, &victim_pub);
            parse_remote_actor(&doc).expect("honest key shape without owner must parse");
        }
    }

    #[test]
    fn parse_rejects_cross_origin_key_without_owner() {
        let (_, victim_pub) = keypair_pems();
        let victim_key = format!("{VICTIM_URI}#main-key");
        let doc = actor_doc(ATTACKER_URI, "attacker", &victim_key, None, &victim_pub);
        assert!(
            parse_remote_actor(&doc).is_err(),
            "attacker doc advertising a victim key id must be rejected"
        );
    }

    #[test]
    fn parse_rejects_cross_origin_key_with_matching_owner() {
        let (_, victim_pub) = keypair_pems();
        let victim_key = format!("{VICTIM_URI}#main-key");
        // `owner` equals the attacker actor, so only the key-id binding
        // catches this.
        let doc = actor_doc(
            ATTACKER_URI,
            "attacker",
            &victim_key,
            Some(ATTACKER_URI),
            &victim_pub,
        );
        assert!(
            parse_remote_actor(&doc).is_err(),
            "victim key id under an attacker actor must be rejected even with owner == actor"
        );
    }

    #[test]
    fn parse_rejects_owner_pointing_at_victim() {
        let (_, attacker_pub) = keypair_pems();
        let attacker_key = format!("{ATTACKER_URI}#main-key");
        // Key id is honest, but `owner` names someone else.
        let doc = actor_doc(
            ATTACKER_URI,
            "attacker",
            &attacker_key,
            Some(VICTIM_URI),
            &attacker_pub,
        );
        assert!(
            parse_remote_actor(&doc).is_err(),
            "owner mismatch must be rejected"
        );
        // Victim key id + victim owner under the attacker actor: both the
        // owner check and the key-id check fire.
        let (_, victim_pub) = keypair_pems();
        let victim_key = format!("{VICTIM_URI}#main-key");
        let doc = actor_doc(
            ATTACKER_URI,
            "attacker",
            &victim_key,
            Some(VICTIM_URI),
            &victim_pub,
        );
        assert!(
            parse_remote_actor(&doc).is_err(),
            "cross-origin key with victim owner must be rejected"
        );
    }

    #[test]
    fn upsert_poison_doc_rejected_and_real_key_kept() {
        let services = test_services("poison-upsert");
        let (_, victim_pub) = keypair_pems();
        let (_, attacker_pub) = keypair_pems();
        let victim_key = format!("{VICTIM_URI}#main-key");

        services
            .cache_remote_public_key(&victim_key, VICTIM_URI, None, &victim_pub)
            .unwrap();

        let poison = actor_doc(ATTACKER_URI, "attacker", &victim_key, None, &attacker_pub);
        assert!(
            upsert_remote_actor_from_doc(&services, ATTACKER_URI, &poison).is_err(),
            "attacker doc advertising the victim key id must not upsert"
        );

        let kept = services
            .auth
            .get_remote_public_key(&victim_key)
            .unwrap()
            .expect("victim key row must survive the poison attempt");
        assert_eq!(kept.actor_uri, VICTIM_URI);
        assert_eq!(kept.public_key_pem, victim_pub.trim());
    }

    #[test]
    fn cache_overwrite_refused_for_different_owner() {
        let services = test_services("overwrite-guard");
        let (_, victim_pub) = keypair_pems();
        let (_, attacker_pub) = keypair_pems();
        let victim_key = format!("{VICTIM_URI}#main-key");

        services
            .cache_remote_public_key(&victim_key, VICTIM_URI, None, &victim_pub)
            .unwrap();
        let err = services
            .cache_remote_public_key(&victim_key, ATTACKER_URI, None, &attacker_pub)
            .expect_err("overwriting a key bound to another actor must fail");
        assert!(
            matches!(err, PortError::Conflict(_)),
            "overwrite guard must report Conflict, got {err:?}"
        );

        let kept = services
            .auth
            .get_remote_public_key(&victim_key)
            .unwrap()
            .unwrap();
        assert_eq!(kept.actor_uri, VICTIM_URI);
        assert_eq!(kept.public_key_pem, victim_pub.trim());
    }

    #[test]
    fn upsert_honest_key_caches_single_row() {
        let services = test_services("single-row");
        let (_, victim_pub) = keypair_pems();
        let victim_key = format!("{VICTIM_URI}#main-key");
        let doc = actor_doc(
            VICTIM_URI,
            "victim",
            &victim_key,
            Some(VICTIM_URI),
            &victim_pub,
        );
        let (_, parsed) =
            upsert_remote_actor_from_doc(&services, VICTIM_URI, &doc).expect("honest upsert");
        assert_eq!(parsed.public_key_id, victim_key);
        assert!(
            services
                .auth
                .get_remote_public_key(&victim_key)
                .unwrap()
                .is_some(),
            "real key id must be cached"
        );
        assert!(
            services
                .auth
                .get_remote_public_key(&format!("{VICTIM_URI}/publickey"))
                .unwrap()
                .is_none(),
            "no speculative /publickey alias row may be written"
        );
    }

    #[test]
    fn upsert_subpath_key_caches_single_row() {
        let services = test_services("single-row-subpath");
        let (_, victim_pub) = keypair_pems();
        let victim_key = format!("{VICTIM_URI}/publickey");
        let doc = actor_doc(
            VICTIM_URI,
            "victim",
            &victim_key,
            Some(VICTIM_URI),
            &victim_pub,
        );
        let (_, parsed) =
            upsert_remote_actor_from_doc(&services, VICTIM_URI, &doc).expect("honest upsert");
        assert_eq!(parsed.public_key_id, victim_key);
        let stored = services
            .auth
            .get_remote_public_key(&victim_key)
            .unwrap()
            .expect("sub-path key id must be cached");
        assert_eq!(stored.actor_uri, VICTIM_URI);
        assert_eq!(stored.public_key_pem, victim_pub.trim());
    }

    #[test]
    fn resolve_returns_owner_and_rejects_fetch_key_mismatch() {
        let services = test_services("resolve-owner");
        let (_, victim_pub) = keypair_pems();
        let victim_key = format!("{VICTIM_URI}#main-key");
        let doc = actor_doc(
            VICTIM_URI,
            "victim",
            &victim_key,
            Some(VICTIM_URI),
            &victim_pub,
        );
        upsert_remote_actor_from_doc(&services, VICTIM_URI, &doc).expect("honest upsert");
        let (owner, pem) =
            resolve_remote_public_key(&services, &services.federation_http, &victim_key, false)
                .expect("cached resolve");
        assert_eq!(owner, VICTIM_URI, "resolve must return the key owner");
        assert_eq!(pem, victim_pub.trim());

        // Fetch path: serve an actor whose advertised key id differs from
        // the requested one; resolve must refuse it.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let served_actor = format!("http://{addr}/users/x");
        let served_doc = actor_doc(
            &served_actor,
            "x",
            &format!("{served_actor}#other-key"),
            Some(&served_actor),
            &victim_pub,
        );
        let served_body = serde_json::to_string(&served_doc).unwrap();
        std::thread::spawn(move || {
            for _ in 0..2 {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/activity+json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    served_body.len(),
                    served_body
                );
                let _ = stream.write_all(resp.as_bytes());
            }
        });
        let requested = format!("{served_actor}#main-key");
        assert!(
            resolve_remote_public_key(&services, &services.federation_http, &requested, true)
                .is_err(),
            "fetched key id must equal the requested key id"
        );
    }

    #[test]
    fn inbox_rejects_forged_victim_activity() {
        let services = test_services("forged-inbox");
        let (attacker_priv, attacker_pub) = keypair_pems();
        let (_, victim_pub) = keypair_pems();
        let victim_key = format!("{VICTIM_URI}#main-key");

        // Honest victim row first.
        let victim_doc = actor_doc(
            VICTIM_URI,
            "victim",
            &victim_key,
            Some(VICTIM_URI),
            &victim_pub,
        );
        upsert_remote_actor_from_doc(&services, VICTIM_URI, &victim_doc).expect("honest upsert");

        // Poison attempt: attacker doc advertising the victim key id.
        let poison = actor_doc(ATTACKER_URI, "attacker", &victim_key, None, &attacker_pub);
        assert!(
            upsert_remote_actor_from_doc(&services, ATTACKER_URI, &poison).is_err(),
            "poison upsert must fail so the cache keeps the victim key"
        );

        // Forged activity: claims victim authorship, carries the victim
        // keyId, but is signed with the attacker key.
        let local = services.create_account("local", None).unwrap();
        let base = services.public_url.trim_end_matches('/').to_string();
        let activity = json!({
            "id": "https://victim.invalid/activities/forged-1",
            "type": "Follow",
            "actor": VICTIM_URI,
            "object": format!("{base}/users/{}", local.id),
        });
        let body = serde_json::to_vec(&activity).unwrap();
        let headers = signed_inbox_headers(&services, "/inbox", &body, &victim_key, &attacker_priv);
        let resp = process_inbox(&services, None, headers, body.into());
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "forged victim activity must be rejected"
        );

        // The victim cache row still belongs to the victim.
        let kept = services
            .auth
            .get_remote_public_key(&victim_key)
            .unwrap()
            .unwrap();
        assert_eq!(kept.actor_uri, VICTIM_URI);
        assert_eq!(kept.public_key_pem, victim_pub.trim());
    }

    #[test]
    fn inbox_accepts_honest_signed_follow() {
        let services = test_services("honest-inbox");
        let (victim_priv, victim_pub) = keypair_pems();
        let victim_key = format!("{VICTIM_URI}#main-key");
        let victim_doc = actor_doc(
            VICTIM_URI,
            "victim",
            &victim_key,
            Some(VICTIM_URI),
            &victim_pub,
        );
        upsert_remote_actor_from_doc(&services, VICTIM_URI, &victim_doc).expect("honest upsert");

        let local = services.create_account("local", None).unwrap();
        let base = services.public_url.trim_end_matches('/').to_string();
        let activity = json!({
            "id": "https://victim.invalid/activities/honest-follow-1",
            "type": "Follow",
            "actor": VICTIM_URI,
            "object": format!("{base}/users/{}", local.id),
        });
        let body = serde_json::to_vec(&activity).unwrap();
        let headers = signed_inbox_headers(&services, "/inbox", &body, &victim_key, &victim_priv);
        let resp = process_inbox(&services, None, headers, body.into());
        assert_eq!(
            resp.status(),
            StatusCode::ACCEPTED,
            "honest signed activity must still be accepted"
        );
    }

    #[test]
    fn inbox_block_and_undo_block_roundtrip() {
        let services = test_services("block-inbox");
        let (remote_priv, remote_pub) = keypair_pems();
        let remote_key = format!("{VICTIM_URI}#main-key");
        let remote_doc = actor_doc(
            VICTIM_URI,
            "victim",
            &remote_key,
            Some(VICTIM_URI),
            &remote_pub,
        );
        upsert_remote_actor_from_doc(&services, VICTIM_URI, &remote_doc).expect("honest upsert");

        let local = services.create_account("local", None).unwrap();
        let base = services.public_url.trim_end_matches('/').to_string();
        let local_uri = format!("{base}/users/{}", local.id);
        let remote_id = services
            .accounts
            .find_by_uri(VICTIM_URI)
            .unwrap()
            .unwrap()
            .id;

        // Inbound `Block`: the remote actor blocks the local user.
        let block_id = "https://victim.invalid/activities/block-1";
        let activity = json!({
            "id": block_id,
            "type": "Block",
            "actor": VICTIM_URI,
            "object": local_uri,
        });
        let body = serde_json::to_vec(&activity).unwrap();
        let headers = signed_inbox_headers(&services, "/inbox", &body, &remote_key, &remote_priv);
        let resp = process_inbox(&services, None, headers, body.into());
        assert_eq!(
            resp.status(),
            StatusCode::ACCEPTED,
            "Block must be accepted"
        );
        assert!(
            services.blocks.is_blocking(&remote_id, &local.id).unwrap(),
            "inbound Block records the block"
        );

        // Replay converges without error.
        let body = serde_json::to_vec(&activity).unwrap();
        let headers = signed_inbox_headers(&services, "/inbox", &body, &remote_key, &remote_priv);
        let resp = process_inbox(&services, None, headers, body.into());
        assert_eq!(
            resp.status(),
            StatusCode::ACCEPTED,
            "Block replay must be accepted"
        );

        // Inbound `Undo(Block)` removes it.
        let undo = json!({
            "id": "https://victim.invalid/activities/undo-block-1",
            "type": "Undo",
            "actor": VICTIM_URI,
            "object": {
                "id": block_id,
                "type": "Block",
                "actor": VICTIM_URI,
                "object": local_uri,
            },
        });
        let body = serde_json::to_vec(&undo).unwrap();
        let headers = signed_inbox_headers(&services, "/inbox", &body, &remote_key, &remote_priv);
        let resp = process_inbox(&services, None, headers, body.into());
        assert_eq!(
            resp.status(),
            StatusCode::ACCEPTED,
            "Undo(Block) must be accepted"
        );
        assert!(
            !services.blocks.is_blocking(&remote_id, &local.id).unwrap(),
            "inbound Undo(Block) removes the block"
        );
    }
}
