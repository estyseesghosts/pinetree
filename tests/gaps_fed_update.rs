//! WP5 gaps: Update(Note) fetch-and-apply, Tombstone deletes,
//! Undo(Accept) by-ID, RFC 9421 permissive-first, generic Add/Remove,
//! stamp exact-host, reply coalescing, stamp negative cache, relay refusal.
//!
//! Hermetic (no live internet): loopback stub origin servers + direct
//! `inbox_handler` calls with real HTTP signatures. Live round-trips
//! against real Akkoma/Pleroma/Mastodon/Sharkey still need the
//! public-internet soak — see the WP5 report.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use serde_json::json;

use pinetree::activitypub::quotes::verify_stamp_for_quote;
use pinetree::activitypub::signatures::{
    generate_rsa_keypair_2048, private_key_to_pkcs8_pem, public_key_to_spki_pem,
    sign_request_headers,
};
use pinetree::application::commands::AppServices;
use pinetree::application::ports::{BlobStore, Clock, CreateAccountInput, NoopStreamingSink};
use pinetree::domain::fed_policy::{late_link_notify, relay_decision, reply_resolve_key, RelayDecision};
use pinetree::domain::id::{AidxGenerator, IdGenerator, ManualClock};
use pinetree::federation::delivery::{
    format_http_date, upsert_remote_actor_from_doc, SafeHttpClient, SafeHttpConfig,
};
use pinetree::federation::inbox::{has_rfc9421_headers, inbox_handler};
use pinetree::federation::{ActivityIdIndex, ReplyCoalescer, StampNegativeCache};
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

fn test_services(tag: &str) -> AppServices {
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pinetree-gaps-fed-update-{}-{}-{}",
        tag,
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
        dedup: store.clone(),
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
        federation_http: SafeHttpClient::new(SafeHttpConfig {
            allow_private_ips: true,
            ..Default::default()
        }),
        public_url: "https://pinetree.local".to_string(),
        max_file_size: 10 * 1024 * 1024,
        preserved_usernames: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        note_draft_limit: pinetree::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
        scheduled_note_limit: pinetree::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
        pin_limit: pinetree::domain::pinning::DEFAULT_PIN_LIMIT,
        default_like: None,
        cache_blobs: blobs,
        drive_capacity_per_user_bytes: 1 << 30,
        remote_cache_max_bytes: 512 << 20,
        remote_cache_avatar_bytes: 128 << 20,
        remote_cache_preview_bytes: 256 << 20,
        remote_cache_full_bytes: 128 << 20,
        remote_cache_mode: pinetree::config::RemoteCacheMode::OnDemand,
        remote_cache_avatar_ttl_days: 30,
        remote_cache_preview_ttl_days: 7,
        remote_cache_full_ttl_days: 3,
        remote_cache_warming_enabled: false,
    }
}

struct Remote {
    uri: String,
    note_uri: String,
    key_id: String,
    private_pem: String,
}

fn upsert_remote(services: &AppServices, addr: &std::net::SocketAddr, username: &str) -> Remote {
    let (private_key, public_key) = generate_rsa_keypair_2048().unwrap();
    let (private_pem, public_pem) = (
        private_key_to_pkcs8_pem(&private_key),
        public_key_to_spki_pem(&public_key),
    );
    let uri = format!("http://{addr}/users/{username}");
    let key_id = format!("{uri}#main-key");
    let doc = json!({
        "id": uri,
        "type": "Person",
        "preferredUsername": username,
        "inbox": format!("{uri}/inbox"),
        "publicKey": {
            "id": key_id,
            "type": "Key",
            "owner": uri,
            "publicKeyPem": public_pem,
        },
    });
    upsert_remote_actor_from_doc(services, &uri, &doc).expect("honest upsert");
    Remote {
        uri: uri.clone(),
        note_uri: format!("http://{addr}/notes/{username}-1"),
        key_id,
        private_pem,
    }
}

fn signed_headers(
    services: &AppServices,
    path: &str,
    body: &[u8],
    key_id: &str,
    private_pem: &str,
    extra: &[(&str, &str)],
) -> HeaderMap {
    let date = format_http_date(services.clock.now_ms());
    let signed =
        sign_request_headers("POST", path, "pinetree.local", &date, Some(body), key_id, private_pem)
            .unwrap();
    let mut headers = HeaderMap::new();
    for (k, v) in [
        ("date".to_string(), date),
        ("host".to_string(), "pinetree.local".to_string()),
        ("digest".to_string(), signed.digest.unwrap()),
        ("signature".to_string(), signed.signature),
    ]
    .into_iter()
    .chain(extra.iter().map(|(k, v)| (k.to_string(), v.to_string())))
    {
        headers.insert(
            HeaderName::from_bytes(k.as_bytes()).unwrap(),
            HeaderValue::from_str(&v).unwrap(),
        );
    }
    headers
}

async fn post_inbox(
    services: &Arc<AppServices>,
    activity: &serde_json::Value,
    remote: &Remote,
    extra_headers: &[(&str, &str)],
) -> StatusCode {
    let body = serde_json::to_vec(activity).unwrap();
    let headers = signed_headers(services, "/inbox", &body, &remote.key_id, &remote.private_pem, extra_headers);
    inbox_handler(State(services.clone()), headers, Bytes::from(body))
        .await
        .status()
}

/// Loopback stub origin: every GET returns the current `doc`.
fn stub_doc_server(doc: Arc<std::sync::Mutex<String>>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for _ in 0..32 {
            let Ok((mut stream, _)) = listener.accept() else {
                break;
            };
            let mut buf = [0u8; 8192];
            let _ = stream.read(&mut buf);
            let body = doc.lock().unwrap().clone();
            let hdr = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/activity+json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(hdr.as_bytes());
            let _ = stream.write_all(body.as_bytes());
            let _ = stream.flush();
        }
    });
    addr
}

fn note_doc(note_uri: &str, author_uri: &str, content: &str) -> String {
    json!({
        "id": note_uri,
        "type": "Note",
        "attributedTo": author_uri,
        "content": content,
        "to": ["https://www.w3.org/ns/activitystreams#Public"],
    })
    .to_string()
}

#[tokio::test]
async fn update_note_string_uri_fetch_and_apply() {
    let services = Arc::new(test_services("update-fetch"));
    services.ensure_system_actor().expect("system actor");
    let doc = Arc::new(std::sync::Mutex::new(String::new()));
    let addr = stub_doc_server(doc.clone());
    let alice = upsert_remote(&services, &addr, "alice-wp5");

    // Seed the cached copy via an embedded Create.
    *doc.lock().unwrap() = note_doc(&alice.note_uri, &alice.uri, "v1-seed");
    let create = json!({
        "id": format!("http://{addr}/activities/alice-wp5-create-1"),
        "type": "Create",
        "actor": alice.uri,
        "object": serde_json::from_str::<serde_json::Value>(&note_doc(&alice.note_uri, &alice.uri, "v1")).unwrap(),
    });
    assert_eq!(
        post_inbox(&services, &create, &alice, &[]).await,
        StatusCode::ACCEPTED
    );
    let cached = services
        .notes
        .find_by_uri(&alice.note_uri)
        .unwrap()
        .expect("note cached");
    assert_eq!(cached.text.as_deref(), Some("v1"));

    // Origin now serves the edit; string-URI Update fetches and applies it.
    *doc.lock().unwrap() = note_doc(&alice.note_uri, &alice.uri, "v2-edited");
    let update = json!({
        "id": format!("http://{addr}/activities/alice-wp5-update-1"),
        "type": "Update",
        "actor": alice.uri,
        "object": alice.note_uri,
    });
    assert_eq!(
        post_inbox(&services, &update, &alice, &[]).await,
        StatusCode::ACCEPTED
    );
    let edited = services
        .notes
        .find_by_uri(&alice.note_uri)
        .unwrap()
        .expect("note still cached");
    assert_eq!(
        edited.text.as_deref(),
        Some("v2-edited"),
        "string-URI Update must fetch-and-apply"
    );
}

#[tokio::test]
async fn update_note_forged_signer_ignored() {
    let services = Arc::new(test_services("update-forged"));
    services.ensure_system_actor().expect("system actor");
    let doc = Arc::new(std::sync::Mutex::new(String::new()));
    let addr = stub_doc_server(doc.clone());
    let alice = upsert_remote(&services, &addr, "alice-forge");
    let bob = upsert_remote(&services, &addr, "bob-forge");

    *doc.lock().unwrap() = note_doc(&alice.note_uri, &alice.uri, "v1");
    let create = json!({
        "id": format!("http://{addr}/activities/alice-forge-create-1"),
        "type": "Create",
        "actor": alice.uri,
        "object": serde_json::from_str::<serde_json::Value>(&note_doc(&alice.note_uri, &alice.uri, "v1")).unwrap(),
    });
    assert_eq!(
        post_inbox(&services, &create, &alice, &[]).await,
        StatusCode::ACCEPTED
    );

    // The origin now serves a document attributing the note to Bob, but the
    // activity is signed by Alice: signer != author → no change.
    *doc.lock().unwrap() = note_doc(&alice.note_uri, &bob.uri, "v2-forged");
    let update = json!({
        "id": format!("http://{addr}/activities/alice-forge-update-1"),
        "type": "Update",
        "actor": alice.uri,
        "object": alice.note_uri,
    });
    assert_eq!(
        post_inbox(&services, &update, &alice, &[]).await,
        StatusCode::ACCEPTED
    );
    let cached = services
        .notes
        .find_by_uri(&alice.note_uri)
        .unwrap()
        .expect("note still cached");
    assert_eq!(cached.text.as_deref(), Some("v1"), "forged edit must not apply");
}

#[tokio::test]
async fn delete_tombstone_wrapped_deletes_note() {
    let services = Arc::new(test_services("tombstone"));
    let doc = Arc::new(std::sync::Mutex::new(String::new()));
    let addr = stub_doc_server(doc.clone());
    let alice = upsert_remote(&services, &addr, "alice-tomb");

    *doc.lock().unwrap() = note_doc(&alice.note_uri, &alice.uri, "bye");
    let create = json!({
        "id": format!("http://{addr}/activities/alice-tomb-create-1"),
        "type": "Create",
        "actor": alice.uri,
        "object": serde_json::from_str::<serde_json::Value>(&note_doc(&alice.note_uri, &alice.uri, "bye")).unwrap(),
    });
    assert_eq!(
        post_inbox(&services, &create, &alice, &[]).await,
        StatusCode::ACCEPTED
    );
    assert!(services.notes.find_by_uri(&alice.note_uri).unwrap().is_some());

    // Misskey dispatch shape: Delete{object: Tombstone{id}}.
    let delete = json!({
        "id": format!("http://{addr}/activities/alice-tomb-delete-1"),
        "type": "Delete",
        "actor": alice.uri,
        "object": { "id": alice.note_uri, "type": "Tombstone", "formerType": "Note" },
    });
    assert_eq!(
        post_inbox(&services, &delete, &alice, &[]).await,
        StatusCode::ACCEPTED
    );
    assert!(
        services.notes.find_by_uri(&alice.note_uri).unwrap().is_none(),
        "Delete(Tombstone) must delete the remote note"
    );
}

#[tokio::test]
async fn undo_accept_by_id_revokes_follow() {
    let services = Arc::new(test_services("undo-accept"));
    let doc = Arc::new(std::sync::Mutex::new(String::new()));
    let addr = stub_doc_server(doc.clone());
    let bob = upsert_remote(&services, &addr, "bob-undowp5");

    // Local follower + remote followee, follow edge first.
    let alice = services
        .accounts
        .create_account(CreateAccountInput::local("alice_undowp5"), None)
        .unwrap();
    let bob_account = services.accounts.find_by_uri(&bob.uri).unwrap().unwrap();
    services.follow(&alice.id, &bob_account.id, false).unwrap();
    assert!(services.follows.is_following(&alice.id, &bob_account.id).unwrap());

    let base = "https://pinetree.local";
    let alice_uri = format!("{base}/users/{}", alice.id);
    let accept_id = format!("http://{addr}/activities/bob-undowp5-accept-1");
    let accept = json!({
        "id": accept_id,
        "type": "Accept",
        "actor": bob.uri,
        "object": {
            "type": "Follow",
            "actor": alice_uri,
            "object": bob.uri,
        },
    });
    assert_eq!(
        post_inbox(&services, &accept, &bob, &[]).await,
        StatusCode::ACCEPTED
    );
    assert_eq!(
        ActivityIdIndex::lookup_accept(&accept_id),
        Some((alice_uri.clone(), bob.uri.clone())),
        "embedded Accept must seed the by-ID index"
    );

    // Bare-IRI Undo resolves via the index (no embedded Follow to read).
    let undo = json!({
        "id": format!("http://{addr}/activities/bob-undowp5-undo-1"),
        "type": "Undo",
        "actor": bob.uri,
        "object": accept_id,
    });
    assert_eq!(
        post_inbox(&services, &undo, &bob, &[]).await,
        StatusCode::ACCEPTED
    );
    assert!(
        !services.follows.is_following(&alice.id, &bob_account.id).unwrap(),
        "Undo(Accept) by-ID must revoke the follow"
    );
    assert_eq!(
        ActivityIdIndex::lookup_accept(&accept_id),
        None,
        "consumed index entry is forgotten"
    );
}

#[tokio::test]
async fn undo_accept_unknown_id_is_noop() {
    let services = Arc::new(test_services("undo-unknown"));
    let doc = Arc::new(std::sync::Mutex::new(String::new()));
    let addr = stub_doc_server(doc.clone());
    let bob = upsert_remote(&services, &addr, "bob-undounknown");

    let undo = json!({
        "id": format!("http://{addr}/activities/bob-undounknown-undo-1"),
        "type": "Undo",
        "actor": bob.uri,
        "object": format!("http://{addr}/activities/never-seen-accept"),
    });
    assert_eq!(
        post_inbox(&services, &undo, &bob, &[]).await,
        StatusCode::ACCEPTED
    );
}

#[tokio::test]
async fn rfc9421_fields_are_permissive_first() {
    assert!(has_rfc9421_headers(&{
        let mut h = HeaderMap::new();
        h.insert("signature-input", HeaderValue::from_static("sig1=(:method)"));
        h
    }));
    assert!(!has_rfc9421_headers(&HeaderMap::new()));

    // A request carrying (bogus) RFC 9421 fields alongside a VALID legacy
    // signature still authenticates: presence is logged, auth proceeds.
    let services = Arc::new(test_services("rfc9421"));
    let doc = Arc::new(std::sync::Mutex::new(String::new()));
    let addr = stub_doc_server(doc.clone());
    let alice = upsert_remote(&services, &addr, "alice-rfc9421");

    *doc.lock().unwrap() = note_doc(&alice.note_uri, &alice.uri, "hello");
    let create = json!({
        "id": format!("http://{addr}/activities/alice-rfc9421-create-1"),
        "type": "Create",
        "actor": alice.uri,
        "object": serde_json::from_str::<serde_json::Value>(&note_doc(&alice.note_uri, &alice.uri, "hello")).unwrap(),
    });
    let status = post_inbox(
        &services,
        &create,
        &alice,
        &[("signature-input", "sig1=(:method :path);keyid=\"x\"")],
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "RFC 9421 presence must not break legacy auth");
    assert!(services.notes.find_by_uri(&alice.note_uri).unwrap().is_some());
}

#[tokio::test]
async fn generic_add_remove_are_accepted_and_ignored() {
    let services = Arc::new(test_services("add-remove"));
    let doc = Arc::new(std::sync::Mutex::new(String::new()));
    let addr = stub_doc_server(doc.clone());
    let bob = upsert_remote(&services, &addr, "bob-addrm");

    for (i, act) in ["Add", "Remove"].iter().enumerate() {
        let activity = json!({
            "id": format!("http://{addr}/activities/bob-addrm-{i}"),
            "type": act,
            "actor": bob.uri,
            "target": "https://remote.example/lists/1",
            "object": "https://remote.example/notes/9",
        });
        assert_eq!(
            post_inbox(&services, &activity, &bob, &[]).await,
            StatusCode::ACCEPTED,
            "{act} for a non-clip target must be accepted and ignored"
        );
    }
}

#[test]
fn stamp_authority_exact_host_cases() {
    let quote = "https://quoting.example/notes/q";
    let target = "https://target.example/notes/1";
    let author = "https://target.example/users/bob";
    let good = json!({
        "id": "https://target.example/stamps/1",
        "type": "QuoteAuthorization",
        "attributedTo": author,
        "interactingObject": quote,
        "interactionTarget": target,
    });
    assert!(verify_stamp_for_quote(&good, "https://target.example/stamps/1", quote, target, author).is_ok());
    // Port split: different exact authority.
    let stamped_port = "https://target.example:8443/stamps/1";
    let mut bad = good.clone();
    bad["id"] = json!(stamped_port);
    assert!(verify_stamp_for_quote(&bad, stamped_port, quote, target, author).is_err());
}

#[test]
fn reply_coalescer_stamp_cache_and_relay_policy() {
    let now = 1_760_400_000_000;
    let parent = "https://remote.example/notes/coalesce-wp5";
    assert!(ReplyCoalescer::should_enqueue(parent, now));
    assert!(!ReplyCoalescer::should_enqueue(parent, now + 1));
    assert_eq!(reply_resolve_key(parent), format!("reply-resolve:{parent}"));

    let stamp = "https://target.example/stamps/neg-wp5";
    assert!(StampNegativeCache::should_fetch(stamp, now));
    StampNegativeCache::record(stamp, now, false);
    assert!(!StampNegativeCache::should_fetch(stamp, now + 1));
    StampNegativeCache::record(stamp, now, true);
    assert!(StampNegativeCache::should_fetch(stamp, now + 2));

    assert!(late_link_notify(true, false, false));
    assert!(!late_link_notify(false, false, false));
    match relay_decision() {
        RelayDecision::Refuse { reason } => assert!(reason.contains("no-relay-actor")),
    }
}
