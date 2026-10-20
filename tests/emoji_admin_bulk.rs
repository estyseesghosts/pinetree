//! Gap 1 contract tests (emoji completion): admin bulk emoji ops, remote
//! copy, and list-remote at the application-op level.
//!
//! Ground truth: Misskey `2026.10.1-alpha.0` (`./misskey`):
//! `add/remove/set-aliases-bulk`, `set-category/license-bulk` (null-reset),
//! `delete-bulk` (per-row delete + per-row `deleteCustomEmoji` log),
//! `copy` (re-download + `DUPLICATE_NAME` guard), `list-remote`. Pinetree
//! DIVERGENCEs under test: `copy` takes `host` + `name` (remote rows have no
//! global ids); bulk alias/category/license touches commit a per-row
//! `UpdateCustomEmoji` log row (the port requires a log per update call).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::api::misskey::packers::pack_remote_emoji_detailed;
use pinetree::application::commands::AppServices;
use pinetree::application::events::{EmojiBroadcastOp, StreamingEvent};
use pinetree::application::ports::{
    BlobStore, Clock, InsertMediaInput, JobKind, ModerationLogFilter, NoopStreamingSink, PortError,
    TimelineCursor,
};
use pinetree::domain::emoji::RemoteEmoji;
use pinetree::domain::id::{AidxGenerator, IdGenerator, ManualClock};
use pinetree::domain::media::MediaProperties;
use pinetree::domain::moderation::ModerationAction;
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

fn test_services(prefix: &str) -> AppServices {
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pinetree-emoji-bulk-test-{}-{prefix}-{}",
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
        blobs,
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
        public_url: "https://example.local".to_string(),
        max_file_size: 10 * 1024 * 1024,
        preserved_usernames: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        note_draft_limit: pinetree::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
        scheduled_note_limit: pinetree::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
        pin_limit: pinetree::domain::pinning::DEFAULT_PIN_LIMIT,
    }
}

/// Minimal 1x1 PNG (only the 8-byte magic matters to the sniffer).
const TINY_PNG: &[u8] = &[
    0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 0, 0, 0, 13, b'I', b'H', b'D', b'R', 0, 0,
    0, 1, 0, 0, 0, 1, 8, 6, 0, 0, 0, 0x1f, 0x15, 0xc4, 0x89, 0, 0, 0, 13, b'I', b'D', b'A',
    b'T', 0x78, 0x9c, 0x63, 0x60, 0x60, 0x60, 0x00, 0x00, 0x00, 0x05, 0x00, 0x01, 0x0d, 0x0a,
    0x2d, 0xb4, 0, 0, 0, 0, b'I', b'E', b'N', b'D', 0xae, 0x42, 0x60, 0x82,
];

fn make_file(services: &AppServices, name: &str) -> String {
    let blob_key = pinetree::media::ingest::sha256_hex(TINY_PNG);
    services.blobs.put(&blob_key, TINY_PNG).unwrap();
    services
        .media
        .insert_metadata(InsertMediaInput {
            id: None,
            owner_id: None,
            name: name.to_string(),
            mime: "image/png".to_string(),
            size_bytes: TINY_PNG.len() as u64,
            hash: pinetree::media::ingest::md5_hex(TINY_PNG),
            blob_key,
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: false,
            url: format!("https://example.local/files/{name}"),
            thumbnail_url: None,
            comment: None,
            source_url: None,
            max_file_size: 10 * 1024 * 1024,
        })
        .unwrap()
        .id
}

fn make_emoji(services: &AppServices, name: &str) -> String {
    let file_id = make_file(services, name);
    // Drain the Added broadcast job so later tests see only their own jobs.
    let emoji = services
        .create_emoji("mod1", name, &file_id, None, Vec::new(), None, false, false, Vec::new())
        .unwrap();
    drain_broadcasts(services);
    emoji.id
}

/// Claim every pending `StreamingPublish` job and decode its event.
fn drain_broadcasts(services: &AppServices) -> Vec<StreamingEvent> {
    let mut out = Vec::new();
    while let Some(job) = services.jobs.claim_next(60_000).unwrap() {
        assert_eq!(job.kind, JobKind::StreamingPublish);
        out.push(serde_json::from_slice::<StreamingEvent>(&job.payload).unwrap());
        services.jobs.complete(&job.id).unwrap();
    }
    out
}

fn broadcast_ops(events: &[StreamingEvent]) -> Vec<(EmojiBroadcastOp, usize)> {
    events
        .iter()
        .map(|e| match e {
            StreamingEvent::EmojiBroadcast { op, emojis } => (*op, emojis.len()),
            other => panic!("expected EmojiBroadcast, got {other:?}"),
        })
        .collect()
}

fn is_validation_invalid_param(err: &PortError) -> bool {
    matches!(err, PortError::Validation(msg) if msg.starts_with("INVALID_PARAM"))
}

// --- single-op broadcasts ---------------------------------------------------

#[test]
fn single_ops_emit_added_updated_deleted_broadcasts() {
    let services = test_services("single-broadcast");
    let file_id = make_file(&services, "blobcat");

    let emoji = services
        .create_emoji("mod1", "blobcat", &file_id, None, Vec::new(), None, false, false, Vec::new())
        .unwrap();
    assert_eq!(
        broadcast_ops(&drain_broadcasts(&services)),
        vec![(EmojiBroadcastOp::Added, 1)]
    );

    let updated = services
        .update_emoji(
            "mod1",
            Some(&emoji.id),
            None,
            None,
            Some(Some("cats".to_string())),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
    assert_eq!(updated.category.as_deref(), Some("cats"));
    let events = drain_broadcasts(&services);
    assert_eq!(broadcast_ops(&events), vec![(EmojiBroadcastOp::Updated, 1)]);
    match &events[0] {
        StreamingEvent::EmojiBroadcast { emojis, .. } => assert_eq!(emojis[0].id, emoji.id),
        _ => unreachable!(),
    }

    services.delete_emoji("mod1", &emoji.id).unwrap();
    let events = drain_broadcasts(&services);
    assert_eq!(broadcast_ops(&events), vec![(EmojiBroadcastOp::Deleted, 1)]);
    // The delete broadcast snapshots the row before it is gone.
    match &events[0] {
        StreamingEvent::EmojiBroadcast { emojis, .. } => assert_eq!(emojis[0].id, emoji.id),
        _ => unreachable!(),
    }

    // Unknown id stays NO_SUCH_EMOJI (upstream parity).
    assert!(matches!(
        services.delete_emoji("mod1", "nope"),
        Err(PortError::NotFound(code)) if code == "NO_SUCH_EMOJI"
    ));
}

// --- add-aliases-bulk --------------------------------------------------------

#[test]
fn add_aliases_bulk_unions_and_skips_unknown() {
    let services = test_services("add-aliases");
    let a = make_emoji(&services, "alpha");
    let b = make_emoji(&services, "beta");

    let out = services
        .add_emoji_aliases_bulk("mod1", &[a.clone(), "missing".to_string(), b.clone()], &[
            "x".to_string(),
            "y".to_string(),
        ])
        .unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].aliases, vec!["x".to_string(), "y".to_string()]);

    // Union: re-adding overlaps without duplicating.
    let out = services
        .add_emoji_aliases_bulk("mod1", &[a.clone()], &["y".to_string(), "z".to_string()])
        .unwrap();
    assert_eq!(
        out[0].aliases,
        vec!["x".to_string(), "y".to_string(), "z".to_string()]
    );

    // One Updated broadcast per touched row.
    assert_eq!(
        broadcast_ops(&drain_broadcasts(&services)),
        vec![
            (EmojiBroadcastOp::Updated, 1),
            (EmojiBroadcastOp::Updated, 1),
            (EmojiBroadcastOp::Updated, 1),
        ]
    );
    let _ = b;
}

// --- bulk guards --------------------------------------------------------------

#[test]
fn bulk_ids_cap_rejected() {
    let services = test_services("bulk-cap");
    let ids: Vec<String> = (0..101).map(|i| format!("id{i}")).collect();
    for op in [
        services.add_emoji_aliases_bulk("mod1", &ids, &["a".to_string()]),
        services.remove_emoji_aliases_bulk("mod1", &ids, &["a".to_string()]),
        services.set_emoji_aliases_bulk("mod1", &ids, &["a".to_string()]),
        services.set_emoji_category_bulk("mod1", &ids, None),
        services.set_emoji_license_bulk("mod1", &ids, None),
        services.delete_emojis_bulk("mod1", &ids),
    ] {
        assert!(
            op.as_ref().err().is_some_and(is_validation_invalid_param),
            "expected INVALID_PARAM, got {op:?}"
        );
    }
    // Exactly 100 ids is accepted (all unknown → empty result).
    let ids100: Vec<String> = (0..100).map(|i| format!("id{i}")).collect();
    assert!(
        services
            .delete_emojis_bulk("mod1", &ids100)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn bulk_alias_shapes_rejected() {
    let services = test_services("bulk-alias-shapes");
    let a = make_emoji(&services, "shapes");

    // Blank alias entries are rejected.
    let err = services
        .add_emoji_aliases_bulk("mod1", &[a.clone()], &["".to_string()])
        .unwrap_err();
    assert!(is_validation_invalid_param(&err), "got {err:?}");

    // The row validator caps aliases at 32 entries.
    let many: Vec<String> = (0..33).map(|i| format!("alias{i}")).collect();
    let err = services
        .set_emoji_aliases_bulk("mod1", &[a.clone()], &many)
        .unwrap_err();
    assert!(matches!(err, PortError::Validation(_)), "got {err:?}");
}

// --- remove / set aliases -----------------------------------------------------

#[test]
fn remove_aliases_bulk_subtracts() {
    let services = test_services("remove-aliases");
    let a = make_emoji(&services, "sub");
    services
        .set_emoji_aliases_bulk("mod1", &[a.clone()], &[
            "x".to_string(),
            "y".to_string(),
            "z".to_string(),
        ])
        .unwrap();
    drain_broadcasts(&services);

    let out = services
        .remove_emoji_aliases_bulk("mod1", &[a.clone(), "missing".to_string()], &[
            "y".to_string(),
        ])
        .unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].aliases, vec!["x".to_string(), "z".to_string()]);
    assert_eq!(
        broadcast_ops(&drain_broadcasts(&services)),
        vec![(EmojiBroadcastOp::Updated, 1)]
    );
}

#[test]
fn set_aliases_bulk_replaces() {
    let services = test_services("set-aliases");
    let a = make_emoji(&services, "repl");
    services
        .add_emoji_aliases_bulk("mod1", &[a.clone()], &["old".to_string()])
        .unwrap();
    drain_broadcasts(&services);

    let out = services
        .set_emoji_aliases_bulk("mod1", &[a.clone()], &["new".to_string()])
        .unwrap();
    assert_eq!(out[0].aliases, vec!["new".to_string()]);
}

// --- set-category / set-license -------------------------------------------------

#[test]
fn set_category_bulk_sets_and_null_resets() {
    let services = test_services("set-category");
    let a = make_emoji(&services, "cat1");
    let b = make_emoji(&services, "cat2");

    let out = services
        .set_emoji_category_bulk("mod1", &[a.clone(), b.clone()], Some("fun".to_string()))
        .unwrap();
    assert!(out.iter().all(|e| e.category.as_deref() == Some("fun")));

    // Absent category resets to null (upstream `ps.category ?? null`).
    let out = services
        .set_emoji_category_bulk("mod1", &[a.clone()], None)
        .unwrap();
    assert_eq!(out[0].category, None);
    // The untouched row keeps its category.
    assert_eq!(
        services.emojis.get_emoji(&b).unwrap().unwrap().category.as_deref(),
        Some("fun")
    );

    // >128 chars rejected by the row validator.
    let err = services
        .set_emoji_category_bulk("mod1", &[a.clone()], Some("c".repeat(129)))
        .unwrap_err();
    assert!(matches!(err, PortError::Validation(_)), "got {err:?}");
}

#[test]
fn set_license_bulk_sets_and_null_resets() {
    let services = test_services("set-license");
    let a = make_emoji(&services, "lic1");

    let out = services
        .set_emoji_license_bulk("mod1", &[a.clone()], Some("CC0".to_string()))
        .unwrap();
    assert_eq!(out[0].license.as_deref(), Some("CC0"));

    let out = services.set_emoji_license_bulk("mod1", &[a.clone()], None).unwrap();
    assert_eq!(out[0].license, None);
}

// --- delete-bulk ---------------------------------------------------------------

#[test]
fn delete_bulk_deletes_skips_unknown_and_logs() {
    let services = test_services("delete-bulk");
    let a = make_emoji(&services, "gone1");
    let b = make_emoji(&services, "gone2");

    let out = services
        .delete_emojis_bulk("mod1", &[a.clone(), "missing".to_string(), b.clone()])
        .unwrap();
    assert_eq!(out.len(), 2);
    assert!(services.emojis.get_emoji(&a).unwrap().is_none());
    assert!(services.emojis.get_emoji(&b).unwrap().is_none());

    // One Deleted broadcast per deleted row.
    assert_eq!(
        broadcast_ops(&drain_broadcasts(&services)),
        vec![
            (EmojiBroadcastOp::Deleted, 1),
            (EmojiBroadcastOp::Deleted, 1),
        ]
    );

    // Per-row deleteCustomEmoji mod-log rows (upstream parity).
    let logs = services
        .moderation
        .list_logs(
            &ModerationLogFilter { action: Some(ModerationAction::DeleteCustomEmoji), ..Default::default() },
            &TimelineCursor::with_limit(100),
        )
        .unwrap();
    assert!(logs.len() >= 2, "expected bulk delete log rows, got {}", logs.len());
}

// --- copy ------------------------------------------------------------------------

/// Serve `TINY_PNG` forever on loopback; returns the file URL.
fn serve_png() -> String {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut stream = stream;
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                TINY_PNG.len()
            );
            let _ = stream.write_all(header.as_bytes());
            let _ = stream.write_all(TINY_PNG);
        }
    });
    format!("http://{addr}/blobcat.png")
}

#[test]
fn copy_missing_remote_row_is_no_such_emoji() {
    let services = test_services("copy-missing");
    let err = services.copy_remote_emoji("mod1", "remote.example", "nope").unwrap_err();
    assert!(
        matches!(err, PortError::NotFound(ref code) if code == "NO_SUCH_EMOJI"),
        "got {err:?}"
    );
}

#[test]
fn copy_downloads_caches_and_adds_with_broadcast() {
    let services = test_services("copy-happy");
    let url = serve_png();
    services
        .emojis
        .upsert_remote_emoji(RemoteEmoji::new("remote.example", "blobcat", &url, 1_760_400_000_000).unwrap())
        .unwrap();

    let emoji = services.copy_remote_emoji("mod1", "remote.example", "blobcat").unwrap();
    assert_eq!(emoji.shortcode, "blobcat");
    assert!(emoji.media_id.is_some());

    // The stored bytes are the downloaded PNG behind the emoji's file row.
    let meta = services.media.get(emoji.media_id.as_deref().unwrap()).unwrap().unwrap();
    assert_eq!(meta.url, format!("https://example.local/files/{}", meta.id));

    // Added broadcast + AddCustomEmoji mod log (via create_emoji).
    assert_eq!(
        broadcast_ops(&drain_broadcasts(&services)),
        vec![(EmojiBroadcastOp::Added, 1)]
    );
    let logs = services
        .moderation
        .list_logs(
            &ModerationLogFilter { action: Some(ModerationAction::AddCustomEmoji), ..Default::default() },
            &TimelineCursor::with_limit(100),
        )
        .unwrap();
    assert!(!logs.is_empty());
}

#[test]
fn copy_duplicate_local_name_is_duplicate_name() {
    let services = test_services("copy-dupe");
    let _existing = make_emoji(&services, "taken");
    let url = serve_png();
    services
        .emojis
        .upsert_remote_emoji(RemoteEmoji::new("remote.example", "taken", &url, 1_760_400_000_000).unwrap())
        .unwrap();

    let err = services.copy_remote_emoji("mod1", "remote.example", "taken").unwrap_err();
    assert!(
        matches!(err, PortError::Conflict(ref code) if code == "DUPLICATE_NAME"),
        "got {err:?}"
    );
}

// --- list-remote -------------------------------------------------------------------

#[test]
fn list_remote_filters_by_host_query_and_date() {
    let services = test_services("list-remote");
    for (host, name, seen) in [
        ("a.example", "blobcat", 1_760_400_000_100),
        ("a.example", "blobdog", 1_760_400_000_200),
        ("b.example", "blobcat", 1_760_400_000_300),
    ] {
        services
            .emojis
            .upsert_remote_emoji(
                RemoteEmoji::new(host, name, &format!("https://{host}/e/{name}.png"), seen).unwrap(),
            )
            .unwrap();
    }
    let cursor = TimelineCursor::with_limit(10);

    // No host = every remote row (upstream `host: null` parity).
    assert_eq!(services.list_remote_emojis(None, None, &cursor).unwrap().len(), 3);
    assert_eq!(
        services.list_remote_emojis(None, Some("a.example"), &cursor).unwrap().len(),
        2
    );
    // Host match is case-insensitive.
    assert_eq!(
        services.list_remote_emojis(None, Some("A.EXAMPLE"), &cursor).unwrap().len(),
        2
    );
    // Name-or-`:name:` query over the shortcode.
    assert_eq!(
        services.list_remote_emojis(Some("blobcat"), None, &cursor).unwrap().len(),
        2
    );
    assert_eq!(
        services.list_remote_emojis(Some(":blobdog:"), None, &cursor).unwrap().len(),
        1
    );
    // Date bounds apply; since/until ids are accepted and ignored.
    let dated = TimelineCursor {
        since_date: Some(1_760_400_000_200),
        until_date: Some(1_760_400_000_300),
        limit: 10,
        ..Default::default()
    };
    assert_eq!(services.list_remote_emojis(None, None, &dated).unwrap().len(), 2);
    let with_ids = TimelineCursor {
        since_id: Some("x".to_string()),
        until_id: Some("y".to_string()),
        limit: 10,
        ..Default::default()
    };
    assert_eq!(services.list_remote_emojis(None, None, &with_ids).unwrap().len(), 3);
}

// --- pack_remote_emoji_detailed ------------------------------------------------------

#[test]
fn pack_remote_emoji_detailed_shape() {
    let services = test_services("pack-remote");
    let url = "https://remote.example/e/blobcat.png";
    let row = RemoteEmoji::new("remote.example", "blobcat", url, 1_760_400_000_000).unwrap();
    services.emojis.upsert_remote_emoji(row.clone()).unwrap();

    // Uncached: url falls back to the remote URL, host is set.
    let packed = pack_remote_emoji_detailed(&row, &services);
    assert_eq!(packed["name"], "blobcat");
    assert_eq!(packed["host"], "remote.example");
    assert_eq!(packed["url"], url);
    assert_eq!(packed["license"], serde_json::Value::Null);
    assert!(packed.get("category").is_none());
    assert_eq!(packed["localOnly"], false);
    assert_eq!(packed["isSensitive"], false);

    // Cached: url prefers the drive copy (same rule as the note registry).
    let file_id = make_file(&services, "cached");
    services.emojis.set_remote_emoji_media("remote.example", "blobcat", &file_id).unwrap();
    let row = services.emojis.get_remote_emoji("remote.example", "blobcat").unwrap().unwrap();
    let packed = pack_remote_emoji_detailed(&row, &services);
    assert_eq!(packed["url"], "https://example.local/files/cached");
    assert_eq!(packed["host"], "remote.example");
}
