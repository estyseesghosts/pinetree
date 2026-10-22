use axum::response::Response;
use serde_json::Value;

use super::super::common::{find_note_by_uri, ingest_remote_note, resolve_remote_account};
use super::super::{accepted, unauthorized};
use crate::activitypub::objects::{ParsedRemoteNote, parse_remote_note};
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::federation::delivery::fetch_remote_note;

pub(crate) fn handle_create(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    // Slice 7 (H2+C3): read-only replay gate BEFORE any remote fetch, so a
    // replayed Create triggers zero network fetches. Benign race: the final
    // `record_note_activity` stays the atomic authority, so a concurrent
    // first delivery that passes this check simply does the (idempotent)
    // work twice and still converges.
    if services.dedup.is_recorded(activity_id)? {
        crate::fed_debug!("in  drop type=Create id={activity_id:?} reason=replay-already-recorded");
        return Ok(accepted());
    }
    let client = &services.federation_http;
    let parsed: ParsedRemoteNote = match activity.get("object") {
        Some(Value::String(uri)) => {
            let uri = uri.trim();
            if uri.is_empty() {
                crate::fed_debug!(
                    "in  drop type=Create id={activity_id:?} reason=empty-object-uri"
                );
                return Ok(accepted());
            }
            match fetch_remote_note(services, client, uri) {
                Ok(parsed) => parsed,
                Err(err) => {
                    crate::fed_debug!(
                        "in  drop type=Create id={activity_id:?} reason=object-fetch-failed uri={uri:?} err={err:?}"
                    );
                    return Ok(accepted());
                }
            }
        }
        Some(obj @ Value::Object(_)) => match parse_remote_note(obj) {
            Ok(parsed) => parsed,
            Err(err) => {
                crate::fed_debug!(
                    "in  drop type=Create id={activity_id:?} reason=object-unparseable err={err:?}"
                );
                return Ok(accepted());
            }
        },
        _ => {
            crate::fed_debug!("in  drop type=Create id={activity_id:?} reason=object-missing");
            return Ok(accepted());
        }
    };
    // The note author must be the signer.
    if parsed.attributed_to != actor_uri {
        crate::fed_debug!(
            "in  reject reason=author-not-signer type=Create id={activity_id:?} author={:?} actor={actor_uri:?}",
            parsed.attributed_to
        );
        return Ok(unauthorized());
    }
    let author = resolve_remote_account(services, actor_uri)?;
    if services.inbound_host_blocked(&author.id) {
        return Ok(accepted());
    }
    // Federated pages (Phase 3): public `Article`s materialize as pages,
    // not notes. Non-public Articles (or invalid ones) return `None` and
    // keep the note pipeline below, which enforces visibility.
    if activity
        .get("object")
        .and_then(|o| o.get("type"))
        .and_then(Value::as_str)
        == Some("Article")
    {
        if let Some(obj) = activity.get("object") {
            if services.ingest_remote_article(&author.id, obj)?.is_some() {
                return Ok(accepted());
            }
        }
    }
    // §4 remote votes (Misskey `ApNoteService` vote branch parity): a
    // content-less `Create(Note)` carrying `inReplyTo` (the poll note) plus
    // `name` (the choice text) records a vote instead of a note. Misskey
    // returns after the vote attempt unconditionally — vote objects never
    // become notes — so any `inReplyTo` resolving to a poll note
    // short-circuits ingest (unknown choices, expired polls, and
    // duplicates converge inside `handle_inbound_vote`).
    if parsed.text.is_none() && parsed.cw.is_none() {
        if let (Some(choice_name), Some(reply_uri)) =
            (parsed.name.clone(), parsed.in_reply_to.clone())
        {
            if !choice_name.trim().is_empty() {
                match find_note_by_uri(services, reply_uri.trim())? {
                    Some(target) => {
                        if services.polls.get_poll(&target.id)?.is_some() {
                            let applied = services.handle_inbound_vote(
                                activity_id,
                                &author.id,
                                &target.id,
                                choice_name.trim(),
                            )?;
                            if applied {
                                eprintln!(
                                    "[inbound-vote] applied activity={activity_id} note={}",
                                    target.id
                                );
                            }
                            return Ok(accepted());
                        }
                        eprintln!(
                            "[inbound-vote] not-a-poll, ingesting as note activity={activity_id} parent={}",
                            target.id
                        );
                    }
                    None => eprintln!(
                        "[inbound-vote] parent-not-cached, ingesting as note activity={activity_id} parent={}",
                        reply_uri.trim()
                    ),
                }
            }
        }
    }
    // An `Answer` is a vote, never a note: when it did not resolve to a
    // cached poll above, drop it (logged) instead of storing an empty note.
    if is_answer_object(activity) {
        eprintln!("[inbound-vote] answer-without-cached-poll activity={activity_id}");
        return Ok(accepted());
    }
    match ingest_remote_note(services, activity_id, &parsed, &author, true)? {
        Some(note) => crate::fed_debug!(
            "in  applied type=Create id={activity_id:?} uri={:?} note={}",
            parsed.uri,
            note.id
        ),
        None => crate::fed_debug!(
            "in  drop type=Create id={activity_id:?} reason=already-cached-uri uri={:?}",
            parsed.uri
        ),
    }
    Ok(accepted())
}

pub(crate) fn is_answer_object(activity: &Value) -> bool {
    activity
        .get("object")
        .and_then(|o| o.get("type"))
        .and_then(Value::as_str)
        == Some("Answer")
}

#[cfg(test)]
mod media_safety_tests {
    use super::super::super::process_inbox;
    use crate::application::commands::AppServices;
    use axum::http::{HeaderMap, StatusCode};
    use serde_json::Value;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    use axum::http::{HeaderName, HeaderValue};
    use serde_json::json;

    use crate::activitypub::signatures::{
        generate_rsa_keypair_2048, private_key_to_pkcs8_pem, public_key_to_spki_pem,
        sign_request_headers,
    };
    use crate::application::ports::{ActivityDedup, BlobStore, Clock, NoopStreamingSink};
    use crate::domain::id::{AidxGenerator, IdGenerator, ManualClock};
    use crate::federation::delivery::{
        SafeHttpClient, SafeHttpConfig, format_http_date, upsert_remote_actor_from_doc,
    };
    use crate::federation::retry::DestinationTracker;
    use crate::jobs::worker::JobWorker;
    use crate::media::storage::FsBlobStore;
    use crate::media::transcode::TranscodeConfig;
    use crate::persistence::redb::RedbStore;
    use crate::services::email::{SmtpConfig, SmtpEmailSender};

    const REMOTE_URI: &str = "https://remote.invalid/users/rocky";

    fn test_services(prefix: &str) -> AppServices {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-inbox-media-{}-{prefix}-{}",
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

    /// Stub media origin: counts every GET, always serves `png_bytes`.
    fn stub_media_server(hits: Arc<AtomicUsize>, png_bytes: Vec<u8>) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for _ in 0..8 {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                let mut buf = [0u8; 2048];
                let _ = stream.read(&mut buf);
                hits.fetch_add(1, Ordering::SeqCst);
                let hdr = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    png_bytes.len()
                );
                let _ = stream.write_all(hdr.as_bytes());
                let _ = stream.write_all(&png_bytes);
                let _ = stream.flush();
            }
        });
        addr
    }

    #[test]
    fn replayed_create_triggers_zero_fetches() {
        let services = Arc::new(test_services("replay"));
        let (private_key, public_key) = generate_rsa_keypair_2048().unwrap();
        let (private_pem, public_pem) = (
            private_key_to_pkcs8_pem(&private_key),
            public_key_to_spki_pem(&public_key),
        );
        let key_id = format!("{REMOTE_URI}#main-key");
        let doc = actor_doc(REMOTE_URI, "rocky", &key_id, &public_pem);
        upsert_remote_actor_from_doc(&services, REMOTE_URI, &doc).expect("honest upsert");

        let hits = Arc::new(AtomicUsize::new(0));
        let png = vec![
            0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 1, 2, 3, 4, 5, 6, 7, 8,
        ];
        let addr = stub_media_server(hits.clone(), png.clone());
        let note_uri = "https://remote.invalid/notes/1";
        let activity = json!({
            "id": "https://remote.invalid/activities/create-1",
            "type": "Create",
            "actor": REMOTE_URI,
            "object": {
                "id": note_uri,
                "type": "Note",
                "attributedTo": REMOTE_URI,
                "content": "hello with pic",
                "to": ["https://www.w3.org/ns/activitystreams#Public"],
                "attachment": [{
                    "type": "Document",
                    "mediaType": "image/png",
                    "url": format!("http://{addr}/pic.png"),
                    "name": "pic",
                }],
            },
        });
        let body = serde_json::to_vec(&activity).unwrap();

        // First delivery: accepted, note stored WITHOUT files, and the
        // inbox performed zero media fetches (fetch moved to a durable job).
        let headers = signed_inbox_headers(&services, "/inbox", &body, &key_id, &private_pem);
        let resp = process_inbox(&services, None, headers, body.clone().into());
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(
            hits.load(Ordering::SeqCst),
            0,
            "inbox must not fetch remote media inline"
        );
        let note = services
            .notes
            .find_by_uri(note_uri)
            .unwrap()
            .expect("note stored on first delivery");
        assert!(note.file_ids.is_empty(), "no inline attach");
        assert_eq!(
            services.jobs.pending_count().unwrap(),
            3,
            "streaming + RemoteMediaFetch + AntennaMatch jobs enqueued atomically"
        );

        // The worker performs exactly one fetch and attaches the sniffed PNG.
        let worker = JobWorker {
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
        };
        assert_eq!(worker.drain(16).unwrap(), 3);
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        let note = services.notes.find_by_uri(note_uri).unwrap().unwrap();
        assert_eq!(note.file_ids.len(), 1);
        let meta = services
            .media
            .get(&note.file_ids[0])
            .unwrap()
            .expect("attached meta exists");
        assert_eq!(meta.mime, "image/png");
        assert_eq!(
            meta.source_url.as_deref(),
            Some(format!("http://{addr}/pic.png").as_str())
        );

        // Replay of the same Create: accepted, zero fetches, zero new jobs,
        // zero new rows.
        let headers = signed_inbox_headers(&services, "/inbox", &body, &key_id, &private_pem);
        let resp = process_inbox(&services, None, headers, body.into());
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "replay must trigger zero fetches"
        );
        assert_eq!(
            services.jobs.pending_count().unwrap(),
            0,
            "replay enqueues no jobs"
        );
        let note = services.notes.find_by_uri(note_uri).unwrap().unwrap();
        assert_eq!(note.file_ids.len(), 1, "replay attaches nothing new");
    }

    #[test]
    fn replayed_uri_object_create_triggers_zero_note_fetches() {
        // Slice 7 (H2+C3): when the Create carries a URI object, the inbox
        // must fetch the note on first delivery but the read-only
        // `is_recorded` replay gate must skip that fetch on replay. (The
        // embedded-object replay test above cannot cover the gate: without
        // any inline fetch it is trivially fetch-free.)
        let services = Arc::new(test_services("replay-uri"));
        let (private_key, public_key) = generate_rsa_keypair_2048().unwrap();
        let (private_pem, public_pem) = (
            private_key_to_pkcs8_pem(&private_key),
            public_key_to_spki_pem(&public_key),
        );
        let hits = Arc::new(AtomicUsize::new(0));
        // Path-based stub: serves the note JSON at /notes/9, 404 elsewhere.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let actor_uri = format!("http://{addr}/users/rocky");
        let note_uri = format!("http://{addr}/notes/9");
        let served = serde_json::to_vec(&json!({
            "id": note_uri,
            "type": "Note",
            "attributedTo": actor_uri,
            "content": "fetched note",
            "to": ["https://www.w3.org/ns/activitystreams#Public"],
        }))
        .unwrap();
        let hits_clone = hits.clone();
        std::thread::spawn(move || {
            for _ in 0..8 {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                let mut buf = [0u8; 2048];
                let _ = stream.read(&mut buf);
                hits_clone.fetch_add(1, Ordering::SeqCst);
                let request_line = String::from_utf8_lossy(&buf);
                if request_line.starts_with("GET /notes/9") {
                    let hdr = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/activity+json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        served.len()
                    );
                    let _ = stream.write_all(hdr.as_bytes());
                    let _ = stream.write_all(&served);
                } else {
                    let _ = stream.write_all(
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    );
                }
                let _ = stream.flush();
            }
        });
        let key_id = format!("{actor_uri}#main-key");
        let doc = actor_doc(&actor_uri, "rocky", &key_id, &public_pem);
        upsert_remote_actor_from_doc(&services, &actor_uri, &doc).expect("honest upsert");

        let activity = json!({
            "id": format!("http://{addr}/activities/create-9"),
            "type": "Create",
            "actor": actor_uri,
            "object": note_uri,
        });
        let body = serde_json::to_vec(&activity).unwrap();

        // First delivery fetches the note exactly once and stores it.
        let headers = signed_inbox_headers(&services, "/inbox", &body, &key_id, &private_pem);
        let resp = process_inbox(&services, None, headers, body.clone().into());
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "first delivery fetches note"
        );
        services
            .notes
            .find_by_uri(&note_uri)
            .unwrap()
            .expect("note stored on first delivery");
        let pending_after_first = services.jobs.pending_count().unwrap();

        // Replay: accepted, but the gate skips the note fetch entirely.
        let headers = signed_inbox_headers(&services, "/inbox", &body, &key_id, &private_pem);
        let resp = process_inbox(&services, None, headers, body.into());
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "replay must trigger zero note fetches"
        );
        assert_eq!(
            services.jobs.pending_count().unwrap(),
            pending_after_first,
            "replay enqueues no jobs"
        );
    }

    #[test]
    fn create_with_too_many_attachments_truncates_to_16() {
        // >16 attachments are truncated (not rejected): the fetch job
        // carries exactly the first 16 in document order.
        let services = test_services("truncate-inbox");
        let (private_key, public_key) = generate_rsa_keypair_2048().unwrap();
        let (private_pem, public_pem) = (
            private_key_to_pkcs8_pem(&private_key),
            public_key_to_spki_pem(&public_key),
        );
        let key_id = format!("{REMOTE_URI}#main-key");
        let doc = actor_doc(REMOTE_URI, "rocky", &key_id, &public_pem);
        upsert_remote_actor_from_doc(&services, REMOTE_URI, &doc).expect("honest upsert");

        let attachments: Vec<Value> = (0..20)
            .map(|i| {
                json!({
                    "type": "Document",
                    "mediaType": "image/png",
                    "url": format!("http://remote.invalid/{i}.png"),
                    "name": "pic",
                })
            })
            .collect();
        let activity = json!({
            "id": "https://remote.invalid/activities/create-many",
            "type": "Create",
            "actor": REMOTE_URI,
            "object": {
                "id": "https://remote.invalid/notes/many",
                "type": "Note",
                "attributedTo": REMOTE_URI,
                "content": "many pics",
                "to": ["https://www.w3.org/ns/activitystreams#Public"],
                "attachment": attachments,
            },
        });
        let body = serde_json::to_vec(&activity).unwrap();
        let headers = signed_inbox_headers(&services, "/inbox", &body, &key_id, &private_pem);
        let resp = process_inbox(&services, None, headers, body.into());
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        // Find the fetch job and inspect its payload without running it
        // (the URLs are unroutable; nothing may be fetched in this test).
        let mut saw_fetch = false;
        while let Some(job) = services.jobs.claim_next(60_000).unwrap() {
            if job.kind == crate::application::ports::JobKind::RemoteMediaFetch {
                let payload: crate::application::ports::RemoteMediaFetchPayload =
                    serde_json::from_slice(&job.payload).unwrap();
                assert_eq!(payload.attachments.len(), 16);
                assert_eq!(payload.attachments[0].url, "http://remote.invalid/0.png");
                assert_eq!(payload.attachments[15].url, "http://remote.invalid/15.png");
                saw_fetch = true;
            }
            services.jobs.complete(&job.id).unwrap();
        }
        assert!(saw_fetch, "a truncated fetch job was enqueued");
    }
}
