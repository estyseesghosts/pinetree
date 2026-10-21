//! WP5 gaps: outbound/inbound emoji-reaction interop + poll fixtures.
//!
//! Hermetic (no live internet): pure-function assertions plus a Redb-backed
//! `AppServices` for the `host_caps` cache path. Live round-trips (real
//! Akkoma/Pleroma/Mastodon) still need the public-internet soak — see the
//! WP5 report.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::activitypub::objects::{
    extract_like_reaction, parse_question_poll, render_emoji_react_activity,
    render_like_activity_for,
};
use pinetree::application::commands::AppServices;
use pinetree::application::ports::{BlobStore, Clock, NoopStreamingSink};
use pinetree::domain::fed_policy::{
    ReactionStyle, is_custom_emoji_reaction, reaction_style_for_software,
};
use pinetree::domain::id::{AidxGenerator, IdGenerator, ManualClock};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::federation::host_caps::resolve_reaction_style;
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

fn test_services(tag: &str) -> AppServices {
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pinetree-gaps-fed-emoji-{}-{}-{}",
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

#[test]
fn akkoma_pleroma_get_emoji_react_style() {
    for name in ["akkoma", "pleroma", "Akkoma", "Pleroma", "akkoma "] {
        assert_eq!(
            reaction_style_for_software(Some(name)),
            ReactionStyle::EmojiReact,
            "{name:?}"
        );
    }
}

#[test]
fn mastodon_family_keeps_bare_like() {
    for name in [
        "mastodon",
        "hometown",
        "glitchsoc",
        "glitch",
        "pixelfed",
        "lemmy",
        "gotosocial",
        "MASTODON",
    ] {
        assert_eq!(
            reaction_style_for_software(Some(name)),
            ReactionStyle::PlainLike,
            "{name:?}"
        );
    }
}

#[test]
fn misskey_family_and_unknown_keep_misskey_like() {
    for name in [
        None,
        Some("misskey"),
        Some("sharkey"),
        Some("pinetree"),
        Some("catodon"),
        Some("firefish"),
        Some(""),
    ] {
        assert_eq!(
            reaction_style_for_software(name),
            ReactionStyle::MisskeyLike,
            "{name:?}"
        );
    }
}

#[test]
fn emoji_react_render_content_vs_tag() {
    let base = "https://pinetree.local";
    // Unicode reaction: content set, no tag.
    let unicode =
        render_emoji_react_activity(base, "r1", "https://pinetree.local/users/a", "https://r.example/n/1", "❤");
    assert_eq!(unicode["type"], "EmojiReact");
    assert_eq!(unicode["content"], "❤");
    assert!(
        unicode.get("tag").is_none(),
        "unicode reactions carry no tag"
    );
    assert!(unicode.get("_misskey_reaction").is_none());
    // Custom reaction: content AND an Emoji tag naming the shortcode.
    let custom =
        render_emoji_react_activity(base, "r2", "https://pinetree.local/users/a", "https://r.example/n/1", ":blobcat:");
    assert_eq!(custom["type"], "EmojiReact");
    assert_eq!(custom["content"], ":blobcat:");
    let tag = custom["tag"].as_array().expect("custom reaction has a tag");
    assert_eq!(tag.len(), 1);
    assert_eq!(tag[0]["type"], "Emoji");
    assert_eq!(tag[0]["name"], ":blobcat:");
    // The legacy renderers are unchanged: emoji-aware Like keeps
    // `_misskey_reaction`, plain Like drops it.
    let like = render_like_activity_for(
        base,
        "r3",
        "https://pinetree.local/users/a",
        "https://r.example/n/1",
        "❤",
        true,
    );
    assert_eq!(like["type"], "Like");
    assert_eq!(like["_misskey_reaction"], "❤");
    let plain = render_like_activity_for(
        base,
        "r4",
        "https://pinetree.local/users/a",
        "https://r.example/n/1",
        "❤",
        false,
    );
    assert_eq!(plain["type"], "Like");
    assert!(plain.get("_misskey_reaction").is_none());
    assert!(plain.get("content").is_none());
}

#[test]
fn custom_shortcode_detection() {
    assert!(is_custom_emoji_reaction(":blobcat:"));
    assert!(!is_custom_emoji_reaction("❤"));
    assert!(!is_custom_emoji_reaction(""));
}

#[test]
fn inbound_content_vs_tag_precedence() {
    // Misskey shape: `_misskey_reaction` wins.
    let misskey = serde_json::json!({
        "type": "Like",
        "actor": "https://mk.example/users/a",
        "object": "https://pinetree.local/notes/1",
        "content": "🍮",
        "_misskey_reaction": "🍮",
    });
    assert_eq!(extract_like_reaction(&misskey), "🍮");
    // Akkoma unicode: `EmojiReact{content}` with no tag.
    let akkoma_unicode = serde_json::json!({
        "type": "EmojiReact",
        "actor": "https://akkoma.example/users/b",
        "object": "https://pinetree.local/notes/1",
        "content": "❤",
    });
    assert_eq!(extract_like_reaction(&akkoma_unicode), "❤");
    // Akkoma custom: `content: ":name:"` verified against the `tag`.
    let akkoma_custom = serde_json::json!({
        "type": "EmojiReact",
        "actor": "https://akkoma.example/users/b",
        "object": "https://pinetree.local/notes/1",
        "content": ":blobcat:",
        "tag": [{ "type": "Emoji", "name": ":blobcat:" }],
    });
    assert_eq!(extract_like_reaction(&akkoma_custom), ":blobcat:");
    // Tag-only fallback (content absent): the Emoji tag name is used.
    let tag_only = serde_json::json!({
        "type": "EmojiReact",
        "actor": "https://pleroma.example/users/c",
        "object": "https://pinetree.local/notes/1",
        "tag": [{ "type": "Emoji", "name": ":party:" }],
    });
    assert_eq!(extract_like_reaction(&tag_only), ":party:");
}

#[test]
fn mastodon_glitch_poll_options_with_note_type_parse() {
    // Mastodon (pinned revision, 4.x): poll options serialize as
    // `{type: "Note", name, replies: {totalItems}}` under `oneOf`.
    let mastodon = serde_json::json!({
        "id": "https://mastodon.example/polls/1",
        "type": "Question",
        "oneOf": [
            { "type": "Note", "name": "yes", "replies": { "type": "Collection", "totalItems": 3 } },
            { "type": "Note", "name": "no", "replies": { "type": "Collection", "totalItems": 5 } },
        ],
        "closed": "2026-10-22T00:00:00.000Z",
    });
    let poll = parse_question_poll(mastodon.as_object().unwrap())
        .expect("mastodon Note-typed options must parse");
    assert_eq!(poll.choices, vec!["yes".to_string(), "no".to_string()]);
    assert_eq!(poll.votes, vec![3, 5]);
    assert!(!poll.multiple);
    assert!(poll.expires_at_ms.is_some());
    // Glitch-soc multi-choice: `anyOf` with `type: Note` options.
    let glitch = serde_json::json!({
        "id": "https://glitch.example/polls/2",
        "type": "Question",
        "anyOf": [
            { "type": "Note", "name": "a", "_misskey_votes": 1 },
            { "type": "Note", "name": "b", "_misskey_votes": 2 },
        ],
    });
    let poll =
        parse_question_poll(glitch.as_object().unwrap()).expect("glitch options must parse");
    assert!(poll.multiple);
    assert_eq!(poll.votes, vec![1, 2]);
    // Misskey (no option type) still parses.
    let misskey = serde_json::json!({
        "id": "https://mk.example/notes/q",
        "type": "Question",
        "oneOf": [
            { "name": "x", "replies": { "type": "Collection", "totalItems": 0 } },
            { "name": "y", "replies": { "type": "Collection", "totalItems": 0 } },
        ],
    });
    assert!(parse_question_poll(misskey.as_object().unwrap()).is_some());
    // Fewer than two named choices is still no poll (storage invariant).
    let short = serde_json::json!({
        "id": "https://mastodon.example/polls/9",
        "type": "Question",
        "oneOf": [{ "type": "Note", "name": "only" }],
    });
    assert!(parse_question_poll(short.as_object().unwrap()).is_none());
}

#[test]
fn host_caps_resolve_reaction_style_caches_probe() {
    let services = test_services("resolve-style");
    let now = 1_760_400_000_000;
    // Akkoma probe → EmojiReact, cached (second call must not probe).
    let fetch = |_: &str| -> Option<String> { Some("akkoma".to_string()) };
    assert_eq!(
        resolve_reaction_style(&services, "akk.example", now, &fetch),
        ReactionStyle::EmojiReact
    );
    let probe = |_: &str| -> Option<String> { panic!("probe on fresh cache hit") };
    assert_eq!(
        resolve_reaction_style(&services, "akk.example", now, &probe),
        ReactionStyle::EmojiReact
    );
    // Mastodon probe → PlainLike; failure → MisskeyLike, uncached.
    let fetch_masto = |_: &str| -> Option<String> { Some("mastodon".to_string()) };
    assert_eq!(
        resolve_reaction_style(&services, "masto.example", now, &fetch_masto),
        ReactionStyle::PlainLike
    );
    let fetch_none = |_: &str| -> Option<String> { None };
    assert_eq!(
        resolve_reaction_style(&services, "down.example", now, &fetch_none),
        ReactionStyle::MisskeyLike
    );
    assert!(
        services
            .host_caps
            .get_host_software("down.example")
            .unwrap()
            .is_none(),
        "probe failures are never cached"
    );
}
