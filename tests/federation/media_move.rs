//! Remote-media service test plus the account-move gate. Moved verbatim
//! from `tests/federation.rs`.

use super::common::{
    NODE_COUNTER, api_json, api_post, boot_node, pump_until, signed_post_to_inbox,
    spawn_png_server, test_client,
};

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use serde_json::{Value, json};

use pinetree::activitypub::objects::ParsedAttachment;
use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    AccountStore, ActivityDedup, BlobStore, Clock, FollowStore, NoopStreamingSink,
};
use pinetree::domain::id::SystemClock;
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::media::remote_cache::fetch_and_cache_remote_media;
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

// ---------------------------------------------------------------------------
// Test 2: remote media fetch/cache at the service layer (no federation
// servers needed — the attachment URL points at an inline PNG server).
// ---------------------------------------------------------------------------

#[test]
fn federation_remote_media_cache() {
    let n = NODE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-fed-media-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("media temp dir");
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let store = RedbStore::open_with_default_ids(dir.join("store.redb"), clock.clone())
        .expect("open media store");
    let blobs = Arc::new(FsBlobStore::new(dir.join("blobs")).expect("blob store"));
    let services = AppServices {
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
        streaming: Arc::new(NoopStreamingSink),
        dedup: store.clone() as Arc<dyn ActivityDedup>,
        federation_http: SafeHttpClient::new(SafeHttpConfig {
            allow_private_ips: true,
            ..Default::default()
        }),
        public_url: "http://127.0.0.1:9".to_string(),
        max_file_size: 4096,
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
    };
    let client = SafeHttpClient::new(SafeHttpConfig {
        allow_private_ips: true,
        ..Default::default()
    });

    // Strict client rejects loopback before fetching.
    let strict = SafeHttpClient::new(SafeHttpConfig {
        allow_private_ips: false,
        ..Default::default()
    });
    let blocked = ParsedAttachment {
        url: "http://127.0.0.1:9/secret.png".to_string(),
        media_type: Some("image/png".to_string()),
        name: Some("secret.png".to_string()),
        is_sensitive: false,
        blurhash: None,
        width: None,
        height: None,
    };
    assert!(fetch_and_cache_remote_media(&services, &strict, &blocked).is_err());

    let png: Vec<u8> = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 9, 9, 9, 9];
    let addr = spawn_png_server(png.clone(), 4);
    let good = ParsedAttachment {
        url: format!("http://{addr}/pic.png"),
        media_type: Some("image/png".to_string()),
        name: Some("Alt text".to_string()),
        is_sensitive: true,
        blurhash: None,
        width: Some(64),
        height: Some(64),
    };
    let meta = fetch_and_cache_remote_media(&services, &client, &good).expect("cache png");
    assert_eq!(meta.owner_id, None);
    assert!(meta.is_link);
    assert!(meta.is_sensitive);
    assert_eq!(meta.mime, "image/png");
    assert_eq!(meta.size_bytes, png.len() as u64);
    assert_eq!(
        blobs.get(&meta.blob_key).expect("blob").expect("bytes"),
        png
    );

    // Missing object rejected (404 from the inline server).
    let missing = ParsedAttachment {
        url: format!("http://{addr}/missing.png"),
        media_type: None,
        name: None,
        is_sensitive: false,
        blurhash: None,
        width: None,
        height: None,
    };
    assert!(fetch_and_cache_remote_media(&services, &client, &missing).is_err());
}

// ---------------------------------------------------------------------------
// Batch P gate 4: account move between two live pinetrees.
// bob@B moves to carol@A; dave@A (bob's follower) refollows carol.
// Covers: alias handshake, Move delivery, follower migration, replay
// convergence, forged-Move rejection.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn federation_account_move() {
    let a = boot_node("move-a");
    let b = boot_node("move-b");
    let client = test_client();
    let deadline = Duration::from_secs(25);

    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/signup", a.base_url),
            None,
            &json!({"username": "carol", "password": "s3cret-pw"}),
        ),
        "A signup carol",
    );
    let carol_id = v["id"].as_str().expect("carol id").to_string();
    let carol_token = v["token"].as_str().expect("carol token").to_string();
    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/signup", a.base_url),
            None,
            &json!({"username": "dave", "password": "s3cret-pw"}),
        ),
        "A signup dave",
    );
    let dave_id = v["id"].as_str().expect("dave id").to_string();
    let dave_token = v["token"].as_str().expect("dave token").to_string();
    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/signup", b.base_url),
            None,
            &json!({"username": "bob", "password": "s3cret-pw"}),
        ),
        "B signup bob",
    );
    let bob_id = v["id"].as_str().expect("bob id").to_string();
    let bob_token = v["token"].as_str().expect("bob token").to_string();
    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/signup", b.base_url),
            None,
            &json!({"username": "mallory", "password": "s3cret-pw"}),
        ),
        "B signup mallory",
    );
    let mallory_id = v["id"].as_str().expect("mallory id").to_string();

    let carol_uri = format!("{}/users/{}", a.base_url, carol_id);
    let bob_uri = format!("{}/users/{}", b.base_url, bob_id);
    let mallory_uri = format!("{}/users/{}", b.base_url, mallory_id);

    a.services
        .ensure_actor_keypair(&carol_id)
        .expect("carol key");
    a.services.ensure_actor_keypair(&dave_id).expect("dave key");
    b.services.ensure_actor_keypair(&bob_id).expect("bob key");
    b.services
        .ensure_actor_keypair(&mallory_id)
        .expect("mallory key");

    // Dave@A resolves + follows bob@B (remote follow handshake).
    let v = api_json(
        &api_post(
            &client,
            &format!("{}/api/users/show", a.base_url),
            Some(&dave_token),
            &json!({"username": "bob", "host": b.host}),
        ),
        "A resolves bob",
    );
    let bob_on_a = v["id"].as_str().expect("remote bob id").to_string();
    let r = api_post(
        &client,
        &format!("{}/api/following/create", a.base_url),
        Some(&dave_token),
        &json!({"userId": bob_on_a}),
    );
    assert!(
        (200..300).contains(&r.status),
        "dave follow bob: {}",
        r.status
    );
    pump_until(&a, &b, "B sees dave as follower", deadline, &mut || {
        b.services
            .accounts
            .find_by_uri(&format!("{}/users/{}", a.base_url, dave_id))
            .expect("lookup dave")
            .map(|acc| {
                b.services
                    .follows
                    .is_following(&acc.id, &bob_id)
                    .unwrap_or(false)
            })
            .unwrap_or(false)
    })
    .await;

    // Alias handshake: carol lists bob's URI as alsoKnownAs.
    let r = api_post(
        &client,
        &format!("{}/api/i/update", a.base_url),
        Some(&carol_token),
        &json!({"alsoKnownAs": [bob_uri]}),
    );
    assert!((200..300).contains(&r.status), "carol alias: {}", r.status);

    // Bob moves to carol@A.
    let r = api_post(
        &client,
        &format!("{}/api/i/move", b.base_url),
        Some(&bob_token),
        &json!({"moveToAccount": format!("carol@{}", a.host)}),
    );
    assert!(
        (200..300).contains(&r.status),
        "i/move: {} {}",
        r.status,
        String::from_utf8_lossy(&r.body)
    );
    let moved: Value = serde_json::from_slice(&r.body).expect("move json");
    assert_eq!(
        moved["movedTo"],
        json!(carol_uri),
        "movedTo packs destination"
    );

    // The Move reaches A: dave unfollows bob and follows carol.
    pump_until(&a, &b, "dave refollows carol", deadline, &mut || {
        a.services
            .follows
            .is_following(&dave_id, &carol_id)
            .unwrap_or(false)
    })
    .await;
    assert!(
        !a.services
            .follows
            .is_following(&dave_id, &bob_on_a)
            .unwrap_or(true),
        "stale dave→bob edge dropped"
    );
    let bob_on_b = b
        .services
        .accounts
        .find_by_id(&bob_id)
        .expect("bob row")
        .expect("bob present");
    assert_eq!(bob_on_b.moved_to_uri.as_deref(), Some(carol_uri.as_str()));

    // Replay convergence: the same Move delivered again changes nothing
    // (no duplicate edges, still exactly one dave→carol edge).
    let replay = json!({
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": format!("{}/moves/{}/replay-1", b.base_url, bob_id),
        "type": "Move",
        "actor": bob_uri,
        "object": bob_uri,
        "target": carol_uri,
    });
    let body = serde_json::to_vec(&replay).expect("encode move");
    let inbox_a = format!("{}/inbox", a.base_url);
    let r1 = signed_post_to_inbox(&client, &b.services, &bob_id, &inbox_a, &body, None);
    assert!((200..300).contains(&r1.status), "replay 1: {}", r1.status);
    let r2 = signed_post_to_inbox(&client, &b.services, &bob_id, &inbox_a, &body, None);
    assert!((200..300).contains(&r2.status), "replay 2: {}", r2.status);
    assert!(
        a.services
            .follows
            .is_following(&dave_id, &carol_id)
            .unwrap_or(false),
        "replay keeps the migrated edge"
    );

    // Forgery: mallory signs a Move claiming BOB moved → 401, no state.
    let forged = json!({
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": format!("{}/moves/{}/forged-1", b.base_url, mallory_id),
        "type": "Move",
        "actor": mallory_uri,
        "object": bob_uri,
        "target": mallory_uri,
    });
    let body = serde_json::to_vec(&forged).expect("encode forged");
    let r = signed_post_to_inbox(&client, &b.services, &mallory_id, &inbox_a, &body, None);
    assert_eq!(r.status, 401, "forged move rejected");
    let bob_on_a_row = a
        .services
        .accounts
        .find_by_id(&bob_on_a)
        .expect("bob row")
        .expect("bob present");
    // The legitimate Move already recorded carol; the forgery must not
    // overwrite it with mallory.
    assert_eq!(
        bob_on_a_row.moved_to_uri.as_deref(),
        Some(carol_uri.as_str()),
        "forgery changes nothing on the receiver"
    );
}
