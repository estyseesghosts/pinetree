//! Batch Q Phase 1 federation fix (item 8 trio): inbound `Add`/`Remove`
//! pin sync, `Undo(Accept)` unfollow, and `Delete(Person)` remote-account
//! tombstoning.
//!
//! Harness: drives the real `POST /inbox` handler (`inbox_handler`) with
//! properly signed requests (same pattern as the in-crate
//! `media_safety_tests`), so every assertion exercises dispatch,
//! authority gates, and stores end to end. Each test gets a fresh
//! `RedbStore` temp dir and `SystemClock` (real time, so HTTP `Date` skew
//! checks pass). Remote actors (`remote.invalid`) are pre-cached via
//! `upsert_remote_actor_from_doc`, so no test depends on the network.

use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use serde_json::{Value, json};

use pinetree::activitypub::signatures::{
    generate_rsa_keypair_2048, private_key_to_pkcs8_pem, public_key_to_spki_pem,
    sign_request_headers,
};
use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    AccountStore, ActivityDedup, BlobStore, Clock, CreateNoteInput, FollowStore,
    NoopStreamingSink, NoteStore,
};
use pinetree::domain::id::SystemClock;
use pinetree::federation::delivery::{
    SafeHttpClient, SafeHttpConfig, format_http_date, upsert_remote_actor_from_doc,
};
use pinetree::federation::inbox::inbox_handler;
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

static N: AtomicU64 = AtomicU64::new(0);

const BOB_URI: &str = "https://remote.invalid/users/bob";
const CHARLIE_URI: &str = "https://remote.invalid/users/charlie";
const BOB_NOTE_URI: &str = "https://remote.invalid/notes/b1";
const BOB_NOTE2_URI: &str = "https://remote.invalid/notes/b2";

fn test_services() -> Arc<AppServices> {
    let dir = std::env::temp_dir().join(format!(
        "pinetree-batchq-fed-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let store =
        RedbStore::open_with_default_ids(dir.join("test.redb"), clock.clone()).unwrap();
    let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.join("blobs")).unwrap());
    Arc::new(AppServices {
        clock: clock.clone(),
        ids: store.ids().clone(),
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
        note_draft_limit: pinetree::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
        scheduled_note_limit: pinetree::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
        pin_limit: pinetree::domain::pinning::DEFAULT_PIN_LIMIT,
        default_like: None,
        cache_blobs: blobs.clone(),
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
    })
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

fn keypair() -> (String, String) {
    let (private_key, public_key) = generate_rsa_keypair_2048().unwrap();
    (
        private_key_to_pkcs8_pem(&private_key),
        public_key_to_spki_pem(&public_key),
    )
}

fn signed_inbox_headers(
    services: &AppServices,
    body: &[u8],
    key_id: &str,
    private_pem: &str,
) -> HeaderMap {
    let date = format_http_date(services.clock.now_ms());
    let host = "pinetree.local";
    let signed =
        sign_request_headers("POST", "/inbox", host, &date, Some(body), key_id, private_pem)
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

async fn post_inbox(
    services: &Arc<AppServices>,
    activity: &Value,
    key_id: &str,
    private_pem: &str,
) -> StatusCode {
    let body = serde_json::to_vec(activity).unwrap();
    let headers = signed_inbox_headers(services, &body, key_id, private_pem);
    let resp = inbox_handler(State(services.clone()), headers, Bytes::from(body)).await;
    resp.status()
}

/// Pre-cache remote actor `uri` with a fresh keypair; returns
/// `(key_id, private_pem)`.
fn cache_remote(
    services: &Arc<AppServices>,
    uri: &str,
    username: &str,
) -> (String, String) {
    let (private_pem, public_pem) = keypair();
    let key_id = format!("{uri}#main-key");
    let doc = actor_doc(uri, username, &key_id, &public_pem);
    upsert_remote_actor_from_doc(services, uri, &doc).expect("honest upsert");
    (key_id, private_pem)
}

/// Cache one inbound note by `uri`, authored by `author_id`.
fn cache_note(services: &Arc<AppServices>, setup_id: &str, author_id: &str, uri: &str) {
    let input = CreateNoteInput {
        uri: Some(uri.to_string()),
        ..CreateNoteInput::simple_public(author_id, "hello")
    };
    services
        .handle_inbound_create_note(setup_id, input, Vec::new())
        .expect("note ingest");
}

// ---------------------------------------------------------------------------
// 8a: featured Add/Remove ingest
// ---------------------------------------------------------------------------

fn featured_activity(
    id: &str,
    added: bool,
    actor: &str,
    target: &str,
    object: &str,
) -> Value {
    json!({
        "id": id,
        "type": if added { "Add" } else { "Remove" },
        "actor": actor,
        "target": target,
        "object": object,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_featured_add_remove_pin_sync() {
    let services = test_services();
    let (bob_key, bob_priv) = cache_remote(&services, BOB_URI, "bob");
    let bob = services.accounts.find_by_uri(BOB_URI).unwrap().unwrap();
    cache_note(&services, "setup-8a-1", &bob.id, BOB_NOTE_URI);
    let featured = format!("{BOB_URI}/collections/featured");

    // Add pins.
    let add = featured_activity(
        "https://remote.invalid/activities/add-1",
        true,
        BOB_URI,
        &featured,
        BOB_NOTE_URI,
    );
    assert_eq!(
        post_inbox(&services, &add, &bob_key, &bob_priv).await,
        StatusCode::ACCEPTED
    );
    let pins = services.notes.pinnings_for_user(&bob.id).unwrap();
    assert_eq!(pins.len(), 1, "inbound Add must record one pin");
    let note = services.notes.find_by_uri(BOB_NOTE_URI).unwrap().unwrap();
    assert_eq!(pins[0].note_id, note.id);

    // Duplicate Add (same activity id): idempotent, still one row.
    assert_eq!(
        post_inbox(&services, &add, &bob_key, &bob_priv).await,
        StatusCode::ACCEPTED
    );
    assert_eq!(services.notes.pinnings_for_user(&bob.id).unwrap().len(), 1);

    // Remove unpins.
    let remove = featured_activity(
        "https://remote.invalid/activities/remove-1",
        false,
        BOB_URI,
        &featured,
        BOB_NOTE_URI,
    );
    assert_eq!(
        post_inbox(&services, &remove, &bob_key, &bob_priv).await,
        StatusCode::ACCEPTED
    );
    assert!(services.notes.pinnings_for_user(&bob.id).unwrap().is_empty());

    // Duplicate Remove: safe no-op.
    assert_eq!(
        post_inbox(&services, &remove, &bob_key, &bob_priv).await,
        StatusCode::ACCEPTED
    );
    assert!(services.notes.pinnings_for_user(&bob.id).unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_featured_forged_and_clips_add_untouched() {
    let services = test_services();
    let (bob_key, bob_priv) = cache_remote(&services, BOB_URI, "bob");
    let bob = services.accounts.find_by_uri(BOB_URI).unwrap().unwrap();
    cache_note(&services, "setup-8a-2", &bob.id, BOB_NOTE_URI);
    // A local note bob does not own: pinning it must not apply.
    let alice = services.create_account("alice", None).unwrap();
    let alice_note = services
        .create_note(CreateNoteInput::simple_public(&alice.id, "alice note"))
        .unwrap();
    let alice_note_uri = format!("https://pinetree.local/notes/{}", alice_note.id);

    // Forged target: someone else's featured collection.
    let forged_target = featured_activity(
        "https://remote.invalid/activities/add-forged-1",
        true,
        BOB_URI,
        &format!("{CHARLIE_URI}/collections/featured"),
        BOB_NOTE_URI,
    );
    assert_eq!(
        post_inbox(&services, &forged_target, &bob_key, &bob_priv).await,
        StatusCode::ACCEPTED
    );
    assert!(services.notes.pinnings_for_user(&bob.id).unwrap().is_empty());

    // Foreign note: bob cannot pin alice's note.
    let foreign_note = featured_activity(
        "https://remote.invalid/activities/add-forged-2",
        true,
        BOB_URI,
        &format!("{BOB_URI}/collections/featured"),
        &alice_note_uri,
    );
    assert_eq!(
        post_inbox(&services, &foreign_note, &bob_key, &bob_priv).await,
        StatusCode::ACCEPTED
    );
    assert!(services.notes.pinnings_for_user(&bob.id).unwrap().is_empty());

    // Clips-targeted Add still takes the clip path: accepted, no pin rows.
    let clips_add = featured_activity(
        "https://remote.invalid/activities/add-clip-1",
        true,
        BOB_URI,
        "https://remote.invalid/clips/xyz",
        BOB_NOTE_URI,
    );
    assert_eq!(
        post_inbox(&services, &clips_add, &bob_key, &bob_priv).await,
        StatusCode::ACCEPTED
    );
    assert!(
        services.notes.pinnings_for_user(&bob.id).unwrap().is_empty(),
        "clips Add must not touch pin rows"
    );
}

// ---------------------------------------------------------------------------
// 8b: Undo(Accept) unfollow
// ---------------------------------------------------------------------------

fn undo_accept_activity(id: &str, undo_actor: &str, accept_actor: &str, follower: &str, followee: &str) -> Value {
    json!({
        "id": id,
        "type": "Undo",
        "actor": undo_actor,
        "object": {
            "type": "Accept",
            "actor": accept_actor,
            "object": {
                "type": "Follow",
                "actor": follower,
                "object": followee,
            },
        },
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_undo_accept_tears_down_follow() {
    let services = test_services();
    let (bob_key, bob_priv) = cache_remote(&services, BOB_URI, "bob");
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.accounts.find_by_uri(BOB_URI).unwrap().unwrap();
    let alice_url = alice.actor_url("https://pinetree.local");

    services.follow(&alice.id, &bob.id, false).unwrap();
    assert!(services.follows.is_following(&alice.id, &bob.id).unwrap());

    // The followee revokes its Accept: edge comes down.
    let undo = undo_accept_activity(
        "https://remote.invalid/activities/undo-accept-1",
        BOB_URI,
        BOB_URI,
        &alice_url,
        BOB_URI,
    );
    assert_eq!(
        post_inbox(&services, &undo, &bob_key, &bob_priv).await,
        StatusCode::ACCEPTED
    );
    assert!(
        !services.follows.is_following(&alice.id, &bob.id).unwrap(),
        "Undo(Accept(Follow)) must tear down the edge"
    );

    // Duplicate Undo: safe.
    assert_eq!(
        post_inbox(&services, &undo, &bob_key, &bob_priv).await,
        StatusCode::ACCEPTED
    );
    assert!(!services.follows.is_following(&alice.id, &bob.id).unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_undo_accept_by_third_party_keeps_edge() {
    let services = test_services();
    let (bob_key, bob_priv) = cache_remote(&services, BOB_URI, "bob");
    let (charlie_key, charlie_priv) = cache_remote(&services, CHARLIE_URI, "charlie");
    let _ = (bob_key, bob_priv);
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.accounts.find_by_uri(BOB_URI).unwrap().unwrap();
    let alice_url = alice.actor_url("https://pinetree.local");

    services.follow(&alice.id, &bob.id, false).unwrap();
    assert!(services.follows.is_following(&alice.id, &bob.id).unwrap());

    // Charlie undoes bob's Accept: forged, edge must survive.
    let forged = undo_accept_activity(
        "https://remote.invalid/activities/undo-accept-forged-1",
        CHARLIE_URI,
        BOB_URI,
        &alice_url,
        BOB_URI,
    );
    let status = post_inbox(&services, &forged, &charlie_key, &charlie_priv).await;
    assert!(
        status == StatusCode::ACCEPTED || status == StatusCode::UNAUTHORIZED,
        "forged Undo must not error, got {status}"
    );
    assert!(
        services.follows.is_following(&alice.id, &bob.id).unwrap(),
        "third-party Undo must not tear down the edge"
    );
}

// ---------------------------------------------------------------------------
// 8c: Delete(Person) inbound
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_delete_person_marks_remote_deleted() {
    let services = test_services();
    let (bob_key, bob_priv) = cache_remote(&services, BOB_URI, "bob");
    let bob = services.accounts.find_by_uri(BOB_URI).unwrap().unwrap();

    // Note-Delete regression first (Path 1 unchanged): bob deletes his note.
    cache_note(&services, "setup-8c-1", &bob.id, BOB_NOTE2_URI);
    let note_delete = json!({
        "id": "https://remote.invalid/activities/delete-note-1",
        "type": "Delete",
        "actor": BOB_URI,
        "object": BOB_NOTE2_URI,
    });
    assert_eq!(
        post_inbox(&services, &note_delete, &bob_key, &bob_priv).await,
        StatusCode::ACCEPTED
    );
    assert!(
        services.notes.find_by_uri(BOB_NOTE2_URI).unwrap().is_none(),
        "note Delete must keep working"
    );

    // A Delete naming a local account URI must NOT mark anything.
    let alice = services.create_account("alice", None).unwrap();
    let alice_url = alice.actor_url("https://pinetree.local");
    let local_delete = json!({
        "id": "https://remote.invalid/activities/delete-local-1",
        "type": "Delete",
        "actor": BOB_URI,
        "object": alice_url,
    });
    assert_eq!(
        post_inbox(&services, &local_delete, &bob_key, &bob_priv).await,
        StatusCode::ACCEPTED
    );
    let alice_after = services.accounts.find_by_id(&alice.id).unwrap().unwrap();
    assert!(!alice_after.is_deleted && !alice_after.is_suspended);

    // Follow teardown check: alice follows bob, then bob's server deletes him.
    services.follow(&alice.id, &bob.id, false).unwrap();
    assert!(services.follows.is_following(&alice.id, &bob.id).unwrap());

    let delete_bob = json!({
        "id": "https://remote.invalid/activities/delete-bob-1",
        "type": "Delete",
        "actor": BOB_URI,
        "object": BOB_URI,
    });
    assert_eq!(
        post_inbox(&services, &delete_bob, &bob_key, &bob_priv).await,
        StatusCode::ACCEPTED
    );
    let bob_after = services.accounts.find_by_uri(BOB_URI).unwrap().unwrap();
    assert!(bob_after.is_deleted, "remote account must be marked deleted");
    assert!(bob_after.is_suspended);
    assert!(
        !services.follows.is_following(&alice.id, &bob.id).unwrap(),
        "deleted account's edges must be torn down"
    );

    // Duplicate Delete: converges.
    assert_eq!(
        post_inbox(&services, &delete_bob, &bob_key, &bob_priv).await,
        StatusCode::ACCEPTED
    );
}

// ---------------------------------------------------------------------------
// Gate 4: live two-server leg — the pinned note is fetched over real HTTP
// from the "remote" node (loopback stub; `allow_private_ips: true` in this
// harness, as in `tests/federation.rs`), never pre-cached.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_featured_add_fetches_note_over_live_http() {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    let services = test_services();
    // Bind first so the Note doc can carry its final URI (the fetch path
    // origin-checks note-host == request-host == author-host).
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let dave_uri = format!("{base}/users/dave");
    let note_uri = format!("{base}/notes/d1");
    let note_doc = json!({
        "id": note_uri,
        "type": "Note",
        "attributedTo": dave_uri,
        "content": "<p>live note</p>",
        "to": ["https://www.w3.org/ns/activitystreams#Public"],
    });
    let body = serde_json::to_vec(&note_doc).unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming().take(4) {
            let mut stream = stream.unwrap();
            let mut buf = vec![0u8; 8192];
            let _ = stream.read(&mut buf);
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/activity+json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(header.as_bytes());
            let _ = stream.write_all(&body);
        }
    });
    // Remote actor pre-cached for signature verification (key lookup, not
    // the note fetch — the note itself must arrive over the wire).
    let (dave_key, dave_priv) = cache_remote(&services, &dave_uri, "dave");
    let dave = services.accounts.find_by_uri(&dave_uri).unwrap().unwrap();
    assert!(
        services.notes.find_by_uri(&note_uri).unwrap().is_none(),
        "note must not be pre-cached: the Add has to fetch it live"
    );
    let featured = format!("{dave_uri}/collections/featured");
    let add = featured_activity(
        &format!("{base}/activities/add-live-1"),
        true,
        &dave_uri,
        &featured,
        &note_uri,
    );
    assert_eq!(
        post_inbox(&services, &add, &dave_key, &dave_priv).await,
        StatusCode::ACCEPTED
    );
    // The note arrived over live HTTP (origin-checked + ingested)…
    let note = services
        .notes
        .find_by_uri(&note_uri)
        .unwrap()
        .expect("fetched note cached");
    assert_eq!(note.author_id, dave.id);
    // …and the pin was recorded against it.
    let pins = services.notes.pinnings_for_user(&dave.id).unwrap();
    assert_eq!(pins.len(), 1);
    assert_eq!(pins[0].note_id, note.id);
}
