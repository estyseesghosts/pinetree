//! Wave 6 authority matrix: forged signer versus every inbox dispatch arm.
//!
//! `check_object_authority` in `src/federation/inbox/mod.rs` deliberately
//! passes `Follow`, `Accept`, `Reject`, `Undo`, `Announce`, `Like`, `Delete`,
//! `Block`, `QuoteRequest`, `Add`, and `Remove` through (plus `Create` when
//! the object shares the signer's host and every `Update` that names a note,
//! question, or collection). Safety therefore rests on each dispatch arm
//! re-validating authority against the signer. This file pins that contract:
//! every row delivers an activity whose `actor` equals the signing key owner
//! (so the `actor == signer` gate and the signature check both pass) but
//! whose object claims someone else's authority, then asserts the victim's
//! state is untouched.
//!
//! The matrix goes through the real `POST /inbox` handler with properly
//! signed requests (same harness as `tests/batch_q_phase1_fed.rs`), so each
//! row exercises dispatch end to end. All remote actors are pre-cached and
//! every object is embedded or local, so no row touches the network.
//!
//! Would fail on old code: without the per-arm checks a forged `Delete`
//! would remove a local note (row `delete_note_forged_401_and_survives`
//! asserts 401 + survival), a forged `Move` would set `moved_to_uri` and
//! migrate followers (`move_forged_401`), a forged `QuoteRequest` would
//! create a pending quote (`quote_request_forged_instrument_401`),
//! `Delete(Person)` from a third party would tombstone the victim
//! (`delete_person_forged_by_third_party_ignored`), and forged `Accept` /
//! `Reject` / `Update(Person)` would apply instead of returning 401.
//! A dispatch arm with no row here is a missing test, not a comment.

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
    AbuseReportFilter, ActivityDedup, BlobStore, Clock, CreateNoteInput, NoopStreamingSink,
    TimelineCursor,
};
use pinetree::domain::id::SystemClock;
use pinetree::domain::poll::PollInput;
use pinetree::federation::delivery::{
    SafeHttpClient, SafeHttpConfig, format_http_date, upsert_remote_actor_from_doc,
};
use pinetree::federation::inbox::inbox_handler;
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

static N: AtomicU64 = AtomicU64::new(0);

const ATTACKER_URI: &str = "https://attacker.invalid/users/attacker";
const VICTIM_URI: &str = "https://victim.invalid/users/victim";

fn test_services() -> Arc<AppServices> {
    let dir = std::env::temp_dir().join(format!(
        "pinetree-wave6-auth-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let store = RedbStore::open_with_default_ids(dir.join("test.redb"), clock.clone()).unwrap();
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
    let signed = sign_request_headers(
        "POST",
        "/inbox",
        host,
        &date,
        Some(body),
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

/// Pre-cache a remote actor; returns `(key_id, private_pem)` for signing.
fn cache_remote(services: &Arc<AppServices>, uri: &str, username: &str) -> (String, String) {
    let (private_pem, public_pem) = keypair();
    let key_id = format!("{uri}#main-key");
    let doc = actor_doc(uri, username, &key_id, &public_pem);
    upsert_remote_actor_from_doc(services, uri, &doc).expect("honest upsert");
    (key_id, private_pem)
}

fn local_alice(services: &Arc<AppServices>) -> pinetree::domain::account::Account {
    services.create_account("alice", None).unwrap()
}

fn alice_url(alice: &pinetree::domain::account::Account) -> String {
    format!("https://pinetree.local/users/{}", alice.id)
}

fn alice_note_uri(note: &pinetree::domain::note::Note, services: &Arc<AppServices>) -> String {
    let base = services.public_url.trim_end_matches('/');
    note.uri
        .clone()
        .unwrap_or_else(|| format!("{base}/notes/{}", note.id))
}

/// Cache one remote-authored note by URI through the inbound path.
fn cache_remote_note(
    services: &Arc<AppServices>,
    setup_id: &str,
    author_id: &str,
    uri: &str,
    text: &str,
) {
    let input = CreateNoteInput {
        uri: Some(uri.to_string()),
        ..CreateNoteInput::simple_public(author_id, text)
    };
    services
        .handle_inbound_create_note(setup_id, input, Vec::new())
        .expect("note ingest");
}

fn activity(id: &str, activity_type: &str, actor: &str, object: Value) -> Value {
    json!({
        "id": id,
        "type": activity_type,
        "actor": actor,
        "object": object,
    })
}

// ---------------------------------------------------------------------------
// Gate: actor must equal the signing key owner.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gate_rejects_actor_signer_mismatch() {
    let services = test_services();
    let (attacker_key, attacker_priv) = cache_remote(&services, ATTACKER_URI, "attacker");
    let _ = cache_remote(&services, VICTIM_URI, "victim");
    let alice = local_alice(&services);
    // Signed by the attacker key but claiming victim authorship: the
    // signature verifies, but `actor != signer` must reject before dispatch.
    let forged = activity(
        "https://attacker.invalid/activities/gate-1",
        "Follow",
        VICTIM_URI,
        json!(alice_url(&alice)),
    );
    assert_eq!(
        post_inbox(&services, &forged, &attacker_key, &attacker_priv).await,
        StatusCode::UNAUTHORIZED
    );
}

// ---------------------------------------------------------------------------
// Follow: any remote may follow a local user; a remote object is ignored.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follow_forged_remote_object_creates_no_edge() {
    let services = test_services();
    let (attacker_key, attacker_priv) = cache_remote(&services, ATTACKER_URI, "attacker");
    let _ = cache_remote(&services, VICTIM_URI, "victim");
    let alice = local_alice(&services);
    let attacker_id = services
        .accounts
        .find_by_uri(ATTACKER_URI)
        .unwrap()
        .unwrap()
        .id;
    let victim_id = services
        .accounts
        .find_by_uri(VICTIM_URI)
        .unwrap()
        .unwrap()
        .id;

    // Forged in the object-authority sense: the object names a remote user
    // the signer has no relationship with. Accepted, but no edge appears.
    let forged = activity(
        "https://attacker.invalid/activities/follow-forged-1",
        "Follow",
        ATTACKER_URI,
        json!(VICTIM_URI),
    );
    assert_eq!(
        post_inbox(&services, &forged, &attacker_key, &attacker_priv).await,
        StatusCode::ACCEPTED
    );
    assert!(
        !services
            .follows
            .is_following(&attacker_id, &victim_id)
            .unwrap(),
        "Follow of a remote object must create no edge"
    );

    // Control: a Follow of the local user still works, proving the arm is live.
    let honest = activity(
        "https://attacker.invalid/activities/follow-honest-1",
        "Follow",
        ATTACKER_URI,
        json!(alice_url(&alice)),
    );
    assert_eq!(
        post_inbox(&services, &honest, &attacker_key, &attacker_priv).await,
        StatusCode::ACCEPTED
    );
    assert!(
        services
            .follows
            .is_following(&attacker_id, &alice.id)
            .unwrap()
    );
}

// ---------------------------------------------------------------------------
// Accept / Reject: only the followee (inner object) may answer its follow.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accept_forged_followee_mismatch_401() {
    let services = test_services();
    let (attacker_key, attacker_priv) = cache_remote(&services, ATTACKER_URI, "attacker");
    let _ = cache_remote(&services, VICTIM_URI, "victim");
    let alice = local_alice(&services);
    let alice_uri = alice_url(&alice);
    let victim_id = services
        .accounts
        .find_by_uri(VICTIM_URI)
        .unwrap()
        .unwrap()
        .id;
    services.follow(&alice.id, &victim_id, false).unwrap();

    // The signer is not the followee named in the embedded Follow.
    let forged = json!({
        "id": "https://attacker.invalid/activities/accept-forged-1",
        "type": "Accept",
        "actor": ATTACKER_URI,
        "object": {
            "type": "Follow",
            "actor": alice_uri,
            "object": VICTIM_URI,
        },
    });
    assert_eq!(
        post_inbox(&services, &forged, &attacker_key, &attacker_priv).await,
        StatusCode::UNAUTHORIZED,
        "Accept by anyone but the followee must be rejected"
    );
    assert!(
        services
            .follows
            .is_following(&alice.id, &victim_id)
            .unwrap(),
        "forged Accept must not disturb the edge"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reject_forged_followee_mismatch_401() {
    let services = test_services();
    let (attacker_key, attacker_priv) = cache_remote(&services, ATTACKER_URI, "attacker");
    let _ = cache_remote(&services, VICTIM_URI, "victim");
    let alice = local_alice(&services);
    let alice_uri = alice_url(&alice);
    let victim_id = services
        .accounts
        .find_by_uri(VICTIM_URI)
        .unwrap()
        .unwrap()
        .id;
    services.follow(&alice.id, &victim_id, false).unwrap();

    let forged = json!({
        "id": "https://attacker.invalid/activities/reject-forged-1",
        "type": "Reject",
        "actor": ATTACKER_URI,
        "object": {
            "type": "Follow",
            "actor": alice_uri,
            "object": VICTIM_URI,
        },
    });
    assert_eq!(
        post_inbox(&services, &forged, &attacker_key, &attacker_priv).await,
        StatusCode::UNAUTHORIZED,
        "Reject by anyone but the followee must be rejected"
    );
    assert!(
        services
            .follows
            .is_following(&alice.id, &victim_id)
            .unwrap(),
        "forged Reject must not disturb the edge"
    );
}

// ---------------------------------------------------------------------------
// Undo: each sub-arm keys off the signer, never the embedded actor.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn undo_follow_forged_cannot_drop_victim_edge() {
    let services = test_services();
    let (attacker_key, attacker_priv) = cache_remote(&services, ATTACKER_URI, "attacker");
    let _ = cache_remote(&services, VICTIM_URI, "victim");
    let alice = local_alice(&services);
    let alice_uri = alice_url(&alice);
    let victim_id = services
        .accounts
        .find_by_uri(VICTIM_URI)
        .unwrap()
        .unwrap()
        .id;
    services.follow(&alice.id, &victim_id, false).unwrap();

    // The embedded Follow names the victim, but the handler unfollows the
    // *signer's* edge only (a no-op here): the victim's edge must survive.
    let forged = json!({
        "id": "https://attacker.invalid/activities/undo-forged-1",
        "type": "Undo",
        "actor": ATTACKER_URI,
        "object": {
            "id": "https://victim.invalid/follows/1",
            "type": "Follow",
            "actor": VICTIM_URI,
            "object": alice_uri,
        },
    });
    assert_eq!(
        post_inbox(&services, &forged, &attacker_key, &attacker_priv).await,
        StatusCode::ACCEPTED
    );
    assert!(
        services
            .follows
            .is_following(&alice.id, &victim_id)
            .unwrap(),
        "forged Undo(Follow) must not drop the victim's edge"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn undo_accept_forged_by_third_party_keeps_edge() {
    let services = test_services();
    let (attacker_key, attacker_priv) = cache_remote(&services, ATTACKER_URI, "attacker");
    let (_victim_key, _victim_priv) = cache_remote(&services, VICTIM_URI, "victim");
    let alice = local_alice(&services);
    let alice_uri = alice_url(&alice);
    let victim_id = services
        .accounts
        .find_by_uri(VICTIM_URI)
        .unwrap()
        .unwrap()
        .id;
    services.follow(&alice.id, &victim_id, false).unwrap();

    // Only the Accepter (the embedded Accept actor AND Follow object) may
    // undo its grant; a third-party signer converges without effect.
    let forged = json!({
        "id": "https://attacker.invalid/activities/undo-accept-forged-1",
        "type": "Undo",
        "actor": ATTACKER_URI,
        "object": {
            "type": "Accept",
            "actor": VICTIM_URI,
            "object": {
                "type": "Follow",
                "actor": alice_uri,
                "object": VICTIM_URI,
            },
        },
    });
    let status = post_inbox(&services, &forged, &attacker_key, &attacker_priv).await;
    assert!(
        status == StatusCode::ACCEPTED || status == StatusCode::UNAUTHORIZED,
        "forged Undo(Accept) must not error, got {status}"
    );
    assert!(
        services
            .follows
            .is_following(&alice.id, &victim_id)
            .unwrap(),
        "third-party Undo(Accept) must not tear down the edge"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn undo_like_forged_keeps_victim_reaction() {
    let services = test_services();
    let (attacker_key, attacker_priv) = cache_remote(&services, ATTACKER_URI, "attacker");
    let (victim_key, victim_priv) = cache_remote(&services, VICTIM_URI, "victim");
    let alice = local_alice(&services);
    let note = services
        .create_note(CreateNoteInput::simple_public(&alice.id, "likeable"))
        .unwrap();
    let note_uri = alice_note_uri(&note, &services);
    let victim_id = services
        .accounts
        .find_by_uri(VICTIM_URI)
        .unwrap()
        .unwrap()
        .id;

    // Honest Like by the victim (control: the arm stores signer reactions).
    let like = activity(
        "https://victim.invalid/activities/like-1",
        "Like",
        VICTIM_URI,
        json!(note_uri.clone()),
    );
    assert_eq!(
        post_inbox(&services, &like, &victim_key, &victim_priv).await,
        StatusCode::ACCEPTED
    );
    assert!(
        services
            .reactions
            .get_reaction(&victim_id, &note.id)
            .unwrap()
            .is_some(),
        "control Like must land"
    );

    // Forged Undo(Like) naming the victim: the handler unlikes the
    // *signer's* reaction only (absent here), so the victim's survives.
    let forged = json!({
        "id": "https://attacker.invalid/activities/undo-like-forged-1",
        "type": "Undo",
        "actor": ATTACKER_URI,
        "object": {
            "type": "Like",
            "actor": VICTIM_URI,
            "object": note_uri,
        },
    });
    assert_eq!(
        post_inbox(&services, &forged, &attacker_key, &attacker_priv).await,
        StatusCode::ACCEPTED
    );
    assert!(
        services
            .reactions
            .get_reaction(&victim_id, &note.id)
            .unwrap()
            .is_some(),
        "forged Undo(Like) must not remove the victim's reaction"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn undo_block_forged_keeps_victim_block() {
    let services = test_services();
    let (attacker_key, attacker_priv) = cache_remote(&services, ATTACKER_URI, "attacker");
    let (victim_key, victim_priv) = cache_remote(&services, VICTIM_URI, "victim");
    let alice = local_alice(&services);
    let alice_uri = alice_url(&alice);
    let victim_id = services
        .accounts
        .find_by_uri(VICTIM_URI)
        .unwrap()
        .unwrap()
        .id;

    // Honest Block by the victim (control).
    let block_id = "https://victim.invalid/activities/block-1";
    let block = activity(block_id, "Block", VICTIM_URI, json!(alice_uri.clone()));
    assert_eq!(
        post_inbox(&services, &block, &victim_key, &victim_priv).await,
        StatusCode::ACCEPTED
    );
    assert!(services.blocks.is_blocking(&victim_id, &alice.id).unwrap());

    // Forged Undo(Block) from a third party: only the blocker's own edge is
    // ever removed (absent here), so the victim's block must survive.
    let forged = json!({
        "id": "https://attacker.invalid/activities/undo-block-forged-1",
        "type": "Undo",
        "actor": ATTACKER_URI,
        "object": {
            "id": block_id,
            "type": "Block",
            "actor": VICTIM_URI,
            "object": alice_uri,
        },
    });
    assert_eq!(
        post_inbox(&services, &forged, &attacker_key, &attacker_priv).await,
        StatusCode::ACCEPTED
    );
    assert!(
        services.blocks.is_blocking(&victim_id, &alice.id).unwrap(),
        "forged Undo(Block) must not lift the victim's block"
    );
}

// ---------------------------------------------------------------------------
// Block: a remote object is never blocked; a local blockee is the signer's.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn block_forged_remote_object_creates_no_block() {
    let services = test_services();
    let (attacker_key, attacker_priv) = cache_remote(&services, ATTACKER_URI, "attacker");
    let _ = cache_remote(&services, VICTIM_URI, "victim");
    let alice = local_alice(&services);
    let attacker_id = services
        .accounts
        .find_by_uri(ATTACKER_URI)
        .unwrap()
        .unwrap()
        .id;
    let victim_id = services
        .accounts
        .find_by_uri(VICTIM_URI)
        .unwrap()
        .unwrap()
        .id;

    let forged = activity(
        "https://attacker.invalid/activities/block-forged-1",
        "Block",
        ATTACKER_URI,
        json!(VICTIM_URI),
    );
    assert_eq!(
        post_inbox(&services, &forged, &attacker_key, &attacker_priv).await,
        StatusCode::ACCEPTED
    );
    assert!(
        !services
            .blocks
            .is_blocking(&attacker_id, &victim_id)
            .unwrap(),
        "Block of a remote object must create no row"
    );

    // Control: blocking the local user still works.
    let honest = activity(
        "https://attacker.invalid/activities/block-honest-1",
        "Block",
        ATTACKER_URI,
        json!(alice_url(&alice)),
    );
    assert_eq!(
        post_inbox(&services, &honest, &attacker_key, &attacker_priv).await,
        StatusCode::ACCEPTED
    );
    assert!(
        services
            .blocks
            .is_blocking(&attacker_id, &alice.id)
            .unwrap()
    );
}

// ---------------------------------------------------------------------------
// Create: the note author must be the signer (past the host gate).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_forged_attribution_401() {
    let services = test_services();
    let (attacker_key, attacker_priv) = cache_remote(&services, ATTACKER_URI, "attacker");
    let _ = cache_remote(&services, VICTIM_URI, "victim");
    // The forged object lives on the signer's host (so the object-host gate
    // passes) but names the victim as author: dispatch must reject.
    let forged_note_uri = "https://attacker.invalid/notes/forged-1";
    let forged = activity(
        "https://attacker.invalid/activities/create-forged-1",
        "Create",
        ATTACKER_URI,
        json!({
            "id": forged_note_uri,
            "type": "Note",
            "attributedTo": VICTIM_URI,
            "content": "forged",
            "to": ["https://www.w3.org/ns/activitystreams#Public"],
        }),
    );
    assert_eq!(
        post_inbox(&services, &forged, &attacker_key, &attacker_priv).await,
        StatusCode::UNAUTHORIZED,
        "Create naming another author must be rejected"
    );
    assert!(
        services
            .notes
            .find_by_uri(forged_note_uri)
            .unwrap()
            .is_none(),
        "forged Create must store nothing"
    );
}

// ---------------------------------------------------------------------------
// Announce / Like / EmojiReact: attributed to the signer, never the target.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn announce_attributes_renote_to_signer() {
    let services = test_services();
    let (attacker_key, attacker_priv) = cache_remote(&services, ATTACKER_URI, "attacker");
    let _ = cache_remote(&services, VICTIM_URI, "victim");
    let alice = local_alice(&services);
    let note = services
        .create_note(CreateNoteInput::simple_public(&alice.id, "original"))
        .unwrap();
    let note_uri = alice_note_uri(&note, &services);
    let attacker_id = services
        .accounts
        .find_by_uri(ATTACKER_URI)
        .unwrap()
        .unwrap()
        .id;
    let victim_row_id = services
        .accounts
        .find_by_uri(VICTIM_URI)
        .unwrap()
        .unwrap()
        .id;

    // Renoting someone else's note is legitimate; the renote belongs to the
    // signer and the victim gains no note.
    let announce = activity(
        "https://attacker.invalid/activities/announce-1",
        "Announce",
        ATTACKER_URI,
        json!(note_uri),
    );
    assert_eq!(
        post_inbox(&services, &announce, &attacker_key, &attacker_priv).await,
        StatusCode::ACCEPTED
    );
    let attacker_notes = services
        .notes
        .notes_by_author(&attacker_id, &TimelineCursor::with_limit(10))
        .unwrap();
    assert_eq!(
        attacker_notes.len(),
        1,
        "one renote attributed to the signer"
    );
    assert_eq!(
        attacker_notes[0].renote_id.as_deref(),
        Some(note.id.as_str())
    );
    assert!(
        services
            .notes
            .notes_by_author(&victim_row_id, &TimelineCursor::with_limit(10))
            .unwrap()
            .is_empty(),
        "Announce must not attribute anything to the victim"
    );
    let stored = services.notes.get_note(&note.id).unwrap().unwrap();
    assert_eq!(stored.text.as_deref(), Some("original"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn like_records_only_signer_reaction() {
    let services = test_services();
    let (attacker_key, attacker_priv) = cache_remote(&services, ATTACKER_URI, "attacker");
    let _ = cache_remote(&services, VICTIM_URI, "victim");
    let alice = local_alice(&services);
    let note = services
        .create_note(CreateNoteInput::simple_public(&alice.id, "likeable"))
        .unwrap();
    let note_uri = alice_note_uri(&note, &services);
    let attacker_id = services
        .accounts
        .find_by_uri(ATTACKER_URI)
        .unwrap()
        .unwrap()
        .id;
    let victim_id = services
        .accounts
        .find_by_uri(VICTIM_URI)
        .unwrap()
        .unwrap()
        .id;

    let like = activity(
        "https://attacker.invalid/activities/like-1",
        "Like",
        ATTACKER_URI,
        json!(note_uri),
    );
    assert_eq!(
        post_inbox(&services, &like, &attacker_key, &attacker_priv).await,
        StatusCode::ACCEPTED
    );
    assert!(
        services
            .reactions
            .get_reaction(&attacker_id, &note.id)
            .unwrap()
            .is_some(),
        "signer reaction must be recorded"
    );
    assert!(
        services
            .reactions
            .get_reaction(&victim_id, &note.id)
            .unwrap()
            .is_none(),
        "Like must never attribute a reaction to anyone but the signer"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn emoji_react_records_only_signer_reaction() {
    let services = test_services();
    let (attacker_key, attacker_priv) = cache_remote(&services, ATTACKER_URI, "attacker");
    let _ = cache_remote(&services, VICTIM_URI, "victim");
    let alice = local_alice(&services);
    let note = services
        .create_note(CreateNoteInput::simple_public(&alice.id, "reactable"))
        .unwrap();
    let note_uri = alice_note_uri(&note, &services);
    let attacker_id = services
        .accounts
        .find_by_uri(ATTACKER_URI)
        .unwrap()
        .unwrap()
        .id;
    let victim_id = services
        .accounts
        .find_by_uri(VICTIM_URI)
        .unwrap()
        .unwrap()
        .id;

    let react = json!({
        "id": "https://attacker.invalid/activities/emoji-1",
        "type": "EmojiReact",
        "actor": ATTACKER_URI,
        "object": note_uri,
        "content": "🎉",
    });
    assert_eq!(
        post_inbox(&services, &react, &attacker_key, &attacker_priv).await,
        StatusCode::ACCEPTED
    );
    assert_eq!(
        services
            .reactions
            .get_reaction(&attacker_id, &note.id)
            .unwrap()
            .map(|r| r.reaction),
        Some("🎉".to_string())
    );
    assert!(
        services
            .reactions
            .get_reaction(&victim_id, &note.id)
            .unwrap()
            .is_none(),
        "EmojiReact must never attribute a reaction to anyone but the signer"
    );
}

// ---------------------------------------------------------------------------
// Delete: known notes need author-or-same-host; Person needs self; clips
// need the collection owner; local collections are never mutated remotely.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_note_forged_401_and_survives() {
    let services = test_services();
    let (attacker_key, attacker_priv) = cache_remote(&services, ATTACKER_URI, "attacker");
    let _ = cache_remote(&services, VICTIM_URI, "victim");
    let alice = local_alice(&services);
    let note = services
        .create_note(CreateNoteInput::simple_public(&alice.id, "do not delete"))
        .unwrap();
    let note_uri = alice_note_uri(&note, &services);

    let forged = activity(
        "https://attacker.invalid/activities/delete-forged-1",
        "Delete",
        ATTACKER_URI,
        json!(note_uri.clone()),
    );
    assert_eq!(
        post_inbox(&services, &forged, &attacker_key, &attacker_priv).await,
        StatusCode::UNAUTHORIZED,
        "Delete of another host's note must be rejected"
    );
    assert!(
        services.notes.find_by_uri(&note_uri).unwrap().is_some(),
        "forged Delete must not remove the note"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_person_forged_by_third_party_ignored() {
    let services = test_services();
    let (attacker_key, attacker_priv) = cache_remote(&services, ATTACKER_URI, "attacker");
    let _ = cache_remote(&services, VICTIM_URI, "victim");
    let alice = local_alice(&services);

    // Only the actor itself may delete itself: a third-party Delete(Person)
    // naming the victim converges without effect.
    let forged = activity(
        "https://attacker.invalid/activities/delete-person-forged-1",
        "Delete",
        ATTACKER_URI,
        json!(VICTIM_URI),
    );
    assert_eq!(
        post_inbox(&services, &forged, &attacker_key, &attacker_priv).await,
        StatusCode::ACCEPTED
    );
    let victim_after = services.accounts.find_by_uri(VICTIM_URI).unwrap().unwrap();
    assert!(
        !victim_after.is_deleted && !victim_after.is_suspended,
        "forged Delete(Person) must not tombstone the victim"
    );

    // A Delete naming a local account is likewise never applied.
    let local_delete = activity(
        "https://attacker.invalid/activities/delete-person-local-1",
        "Delete",
        ATTACKER_URI,
        json!(alice_url(&alice)),
    );
    assert_eq!(
        post_inbox(&services, &local_delete, &attacker_key, &attacker_priv).await,
        StatusCode::ACCEPTED
    );
    let alice_after = services.accounts.find_by_id(&alice.id).unwrap().unwrap();
    assert!(!alice_after.is_deleted && !alice_after.is_suspended);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_clip_forged_local_ignored() {
    let services = test_services();
    let (attacker_key, attacker_priv) = cache_remote(&services, ATTACKER_URI, "attacker");
    let _ = cache_remote(&services, VICTIM_URI, "victim");

    // Local collections are never mutated remotely: accepted, no fetch, no
    // state change.
    let forged = activity(
        "https://attacker.invalid/activities/delete-clip-forged-1",
        "Delete",
        ATTACKER_URI,
        json!("https://pinetree.local/clips/xyz"),
    );
    assert_eq!(
        post_inbox(&services, &forged, &attacker_key, &attacker_priv).await,
        StatusCode::ACCEPTED
    );
    assert!(
        services
            .notes
            .pinnings_for_user(
                &services
                    .accounts
                    .find_by_uri(ATTACKER_URI)
                    .unwrap()
                    .unwrap()
                    .id
            )
            .unwrap()
            .is_empty()
    );
}

// ---------------------------------------------------------------------------
// Update: actor documents need object == signer; notes/questions need the
// author; collections need the collection owner.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_actor_forged_401() {
    let services = test_services();
    let (attacker_key, attacker_priv) = cache_remote(&services, ATTACKER_URI, "attacker");
    let _ = cache_remote(&services, VICTIM_URI, "victim");

    // The gate itself rejects an Update whose object is not the signer.
    let forged = activity(
        "https://attacker.invalid/activities/update-forged-1",
        "Update",
        ATTACKER_URI,
        json!({
            "id": VICTIM_URI,
            "type": "Person",
            "preferredUsername": "victim",
            "displayName": "pwned",
        }),
    );
    assert_eq!(
        post_inbox(&services, &forged, &attacker_key, &attacker_priv).await,
        StatusCode::UNAUTHORIZED,
        "Update of another actor must be rejected"
    );
    let victim_after = services.accounts.find_by_uri(VICTIM_URI).unwrap().unwrap();
    assert_eq!(victim_after.display_name, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_note_forged_keeps_text() {
    let services = test_services();
    let (attacker_key, attacker_priv) = cache_remote(&services, ATTACKER_URI, "attacker");
    let _ = cache_remote(&services, VICTIM_URI, "victim");
    let victim_id = services
        .accounts
        .find_by_uri(VICTIM_URI)
        .unwrap()
        .unwrap()
        .id;
    let victim_note_uri = "https://victim.invalid/notes/n1";
    cache_remote_note(
        &services,
        "setup-update-note-1",
        &victim_id,
        victim_note_uri,
        "v1",
    );

    // Gate passes embedded Note objects through; dispatch must still require
    // `attributedTo == signer`.
    let forged = activity(
        "https://attacker.invalid/activities/update-note-forged-1",
        "Update",
        ATTACKER_URI,
        json!({
            "id": victim_note_uri,
            "type": "Note",
            "attributedTo": VICTIM_URI,
            "content": "pwned",
        }),
    );
    assert_eq!(
        post_inbox(&services, &forged, &attacker_key, &attacker_priv).await,
        StatusCode::ACCEPTED
    );
    let stored = services
        .notes
        .find_by_uri(victim_note_uri)
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.text.as_deref(),
        Some("v1"),
        "forged Update(Note) must change nothing"
    );
    assert_eq!(stored.updated_at_ms, None);
    assert_eq!(stored.author_id, victim_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_question_forged_keeps_tallies() {
    let services = test_services();
    let (attacker_key, attacker_priv) = cache_remote(&services, ATTACKER_URI, "attacker");
    let _ = cache_remote(&services, VICTIM_URI, "victim");
    let victim_id = services
        .accounts
        .find_by_uri(VICTIM_URI)
        .unwrap()
        .unwrap()
        .id;
    let poll_uri = "https://victim.invalid/notes/poll-1";
    let mut input = CreateNoteInput::simple_public(&victim_id, "poll?");
    input.uri = Some(poll_uri.to_string());
    input.has_poll = true;
    input.poll = Some(PollInput {
        choices: vec!["alpha".to_string(), "beta".to_string()],
        multiple: false,
        expires_at_ms: None,
    });
    let seeded = services
        .handle_inbound_create_note("setup-update-question-1", input, Vec::new())
        .expect("seed poll")
        .expect("poll stored");
    assert_eq!(
        services
            .polls
            .get_poll(&seeded.id)
            .unwrap()
            .map(|p| p.votes),
        Some(vec![0, 0]),
        "poll setup must carry zeroed tallies"
    );

    // A forged tally refresh names the victim as author but is signed by the
    // attacker: dispatch re-validates the signer against the poll owner.
    let forged = activity(
        "https://attacker.invalid/activities/update-question-forged-1",
        "Update",
        ATTACKER_URI,
        json!({
            "id": poll_uri,
            "type": "Question",
            "attributedTo": VICTIM_URI,
        }),
    );
    assert_eq!(
        post_inbox(&services, &forged, &attacker_key, &attacker_priv).await,
        StatusCode::ACCEPTED
    );
    assert_eq!(
        services
            .polls
            .get_poll(&seeded.id)
            .unwrap()
            .map(|p| p.votes),
        Some(vec![0, 0]),
        "forged Update(Question) must not move tallies"
    );
}

// ---------------------------------------------------------------------------
// Add / Remove: local clips and someone else's featured collections are
// never mutated remotely; only the collection owner passes (verified by fetch).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_remove_clip_forged_local_untouched() {
    let services = test_services();
    let (attacker_key, attacker_priv) = cache_remote(&services, ATTACKER_URI, "attacker");
    let _ = cache_remote(&services, VICTIM_URI, "victim");
    let victim_id = services
        .accounts
        .find_by_uri(VICTIM_URI)
        .unwrap()
        .unwrap()
        .id;
    let victim_note_uri = "https://victim.invalid/notes/n1";
    cache_remote_note(
        &services,
        "setup-add-remove-1",
        &victim_id,
        victim_note_uri,
        "hello",
    );
    let attacker_id = services
        .accounts
        .find_by_uri(ATTACKER_URI)
        .unwrap()
        .unwrap()
        .id;
    let local_clip = "https://pinetree.local/clips/xyz".to_string();
    let pins = || {
        services
            .notes
            .pinnings_for_user(&attacker_id)
            .unwrap()
            .len()
    };

    // Remote actors never mutate our collections (no fetch, no state).
    for (n, added) in [(1, true), (2, false)] {
        let activity_type = if added { "Add" } else { "Remove" };
        let membership = json!({
            "id": format!("https://attacker.invalid/activities/clip-forged-{n}"),
            "type": activity_type,
            "actor": ATTACKER_URI,
            "target": local_clip,
            "object": victim_note_uri,
        });
        assert_eq!(
            post_inbox(&services, &membership, &attacker_key, &attacker_priv).await,
            StatusCode::ACCEPTED
        );
    }
    assert_eq!(pins(), 0, "clip Add/Remove must not touch pin rows");

    // Someone else's featured collection: same accept-without-change rule.
    let forged_featured = json!({
        "id": "https://attacker.invalid/activities/featured-forged-1",
        "type": "Add",
        "actor": ATTACKER_URI,
        "target": format!("{VICTIM_URI}/collections/featured"),
        "object": victim_note_uri,
    });
    assert_eq!(
        post_inbox(&services, &forged_featured, &attacker_key, &attacker_priv).await,
        StatusCode::ACCEPTED
    );
    assert_eq!(pins(), 0);
}

// ---------------------------------------------------------------------------
// Move: only the old account itself may announce its move.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn move_forged_401() {
    let services = test_services();
    let (attacker_key, attacker_priv) = cache_remote(&services, ATTACKER_URI, "attacker");
    let _ = cache_remote(&services, VICTIM_URI, "victim");
    let alice = local_alice(&services);

    // Object names the victim while the signer is the attacker.
    let forged = json!({
        "id": "https://attacker.invalid/activities/move-forged-1",
        "type": "Move",
        "actor": ATTACKER_URI,
        "object": VICTIM_URI,
        "target": alice_url(&alice),
    });
    assert_eq!(
        post_inbox(&services, &forged, &attacker_key, &attacker_priv).await,
        StatusCode::UNAUTHORIZED,
        "Move of someone else's account must be rejected"
    );
    for uri in [VICTIM_URI, ATTACKER_URI] {
        let row = services.accounts.find_by_uri(uri).unwrap().unwrap();
        assert_eq!(row.moved_to_uri, None, "{uri} must not gain a move target");
    }
}

// ---------------------------------------------------------------------------
// QuoteRequest: the instrument must be the signer's own note quoting the
// exact target.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quote_request_forged_instrument_401() {
    let services = test_services();
    let (attacker_key, attacker_priv) = cache_remote(&services, ATTACKER_URI, "attacker");
    let _ = cache_remote(&services, VICTIM_URI, "victim");
    let alice = local_alice(&services);
    let note = services
        .create_note(CreateNoteInput::simple_public(&alice.id, "quotable"))
        .unwrap();
    let note_uri = alice_note_uri(&note, &services);
    let instrument_uri = "https://attacker.invalid/notes/quote-1";

    // The instrument names the victim as its author while the signer is the
    // attacker: dispatch must reject before any quote row appears.
    let forged = json!({
        "id": "https://attacker.invalid/activities/quote-forged-1",
        "type": "QuoteRequest",
        "actor": ATTACKER_URI,
        "object": note_uri,
        "instrument": {
            "id": instrument_uri,
            "type": "Note",
            "attributedTo": VICTIM_URI,
            "content": "quoting",
            "quote": note_uri,
            "to": ["https://www.w3.org/ns/activitystreams#Public"],
        },
    });
    assert_eq!(
        post_inbox(&services, &forged, &attacker_key, &attacker_priv).await,
        StatusCode::UNAUTHORIZED,
        "QuoteRequest with another author's instrument must be rejected"
    );
    assert!(
        services
            .notes
            .find_note_by_quote_request(instrument_uri)
            .unwrap()
            .is_none(),
        "forged QuoteRequest must store no quote"
    );
}

// ---------------------------------------------------------------------------
// Flag: the reporter is always the signer; the target must be local.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flag_attributes_report_to_signer() {
    let services = test_services();
    let (attacker_key, attacker_priv) = cache_remote(&services, ATTACKER_URI, "attacker");
    let _ = cache_remote(&services, VICTIM_URI, "victim");
    let alice = local_alice(&services);
    let attacker_id = services
        .accounts
        .find_by_uri(ATTACKER_URI)
        .unwrap()
        .unwrap()
        .id;

    let flag = json!({
        "id": "https://attacker.invalid/activities/flag-1",
        "type": "Flag",
        "actor": ATTACKER_URI,
        "object": [alice_url(&alice)],
        "content": "spam",
    });
    assert_eq!(
        post_inbox(&services, &flag, &attacker_key, &attacker_priv).await,
        StatusCode::ACCEPTED
    );
    let rows = services
        .abuse_reports
        .list(
            &AbuseReportFilter {
                state: None,
                reporter_origin: "combined".to_string(),
                target_origin: "combined".to_string(),
            },
            &TimelineCursor::with_limit(10),
        )
        .expect("list reports");
    assert_eq!(rows.len(), 1, "one report row: {rows:?}");
    assert_eq!(rows[0].reporter, attacker_id, "reporter is the signer");
    assert_eq!(rows[0].target_account, alice.id);
    assert!(!rows[0].resolved && !rows[0].forwarded);
}
