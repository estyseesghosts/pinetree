//! WP6 domain-layering payoff: integration coverage for the pure rules moved
//! into `src/domain/*` (unit tests live inline in each domain module; this
//! suite locks the cross-module contracts the application ops share so no
//! adapter invents a per-caller variant).
//!
//! Groups (proposed conformance-group ids, see the WP6 report):
//! - D1 follow-request accept/reject + locked branching (`domain::follow`)
//! - D2 note edit/delete permission + tombstone transitions (`domain::note`)
//! - D3 reaction unreact/idempotency (`domain::reaction`)
//! - D4 rename/move state machine (`domain::account`)
//! - D5 MFM sanitize policy (`domain::note::sanitize_mfm`)
//! - D6 role_ids enforcement (`domain::emoji`)
//! - D7 pinned_page_id / verifiedLinks re-verify loop (`domain::account`)
//! - D8 clip notesCount derivation (`domain::clip`)
//! - D9 antenna exclude_sensitive + 7-day auto-deactivate (`domain::antenna`)
//! - D10 signin input shape + password policy (`domain::account`)
//! - D11 hashtag-trend aggregation (`domain::note`)

use std::collections::BTreeMap;

use pinetree::domain::account::{
    Account, needs_link_reverify, password_meets_policy, reset_verified_links_for_reverify,
    validate_move, validate_pinned_page_update, validate_rename, validate_signin_input,
};
use pinetree::domain::antenna::{
    ANTENNA_AUTO_DEACTIVATE_AFTER_MS, Antenna, AntennaNoteView, AntennaSrc,
    antenna_matches_with_sensitivity, deactivate_stale_antenna, should_auto_deactivate,
};
use pinetree::domain::clip::{Clip, count_clip_notes};
use pinetree::domain::emoji::{CustomEmoji, emoji_usable_by};
use pinetree::domain::follow::{
    FollowPath, FollowRequest, build_follow_from_request, decide_follow_path,
    require_follow_request,
};
use pinetree::domain::id::DomainError;
use pinetree::domain::note::{
    Note, Visibility, aggregate_hashtag_counts, can_delete_note, can_edit_note,
    rank_hashtag_trends, sanitize_mfm, tombstone_transition,
};
use pinetree::domain::reaction::{ReactOutcome, Reaction, can_unreact, decide_react};

fn note_by(author: &str) -> Note {
    Note {
        id: "nid00000000000001".to_string(),
        author_id: author.to_string(),
        text: Some("hello".to_string()),
        cw: None,
        visibility: Visibility::Public,
        visible_user_ids: Vec::new(),
        local_only: false,
        reply_id: None,
        renote_id: None,
        reply_user_id: None,
        file_ids: Vec::new(),
        mentions: Vec::new(),
        mentioned_remote_users: "[]".to_string(),
        tags: Vec::new(),
        emojis: Vec::new(),
        reactions: BTreeMap::new(),
        renote_count: 0,
        replies_count: 0,
        reaction_acceptance: None,
        uri: None,
        url: None,
        user_host: None,
        is_deleted: false,
        reply_parent_uri: None,
        quote: None,
        updated_at_ms: None,
        process_errors: None,
    }
}

fn follow_request() -> FollowRequest {
    FollowRequest {
        id: "rid00000000000001".to_string(),
        follower_id: "aid00000000000001".to_string(),
        followee_id: "aid00000000000002".to_string(),
        with_replies: false,
        notify: None,
    }
}

fn antenna(note: &Note) -> (Antenna, AntennaNoteView<'_>) {
    let antenna = Antenna {
        id: "a1".to_string(),
        owner_id: "owner".to_string(),
        name: "n".to_string(),
        src: AntennaSrc::All,
        user_list_id: None,
        keywords: Vec::new(),
        exclude_keywords: Vec::new(),
        users: Vec::new(),
        case_sensitive: false,
        with_replies: true,
        with_file: false,
        local_only: false,
        exclude_bots: false,
        exclude_notes_in_sensitive_channel: false,
        is_active: true,
        last_used_at_ms: 0,
        created_at_ms: 0,
    };
    let view = AntennaNoteView {
        note,
        author_acct: "u1".to_string(),
        author_is_bot: false,
        author_is_local: true,
        owner_is_author: false,
        owner_follows_author: false,
        owner_in_visible_ids: false,
        author_in_list: false,
    };
    (antenna, view)
}

// --- D1: follow-request accept/reject + locked branching -------------------

#[test]
fn d1_locked_followee_forces_request_path() {
    assert_eq!(decide_follow_path(true), FollowPath::Request);
    assert_eq!(decide_follow_path(false), FollowPath::Direct);
}

#[test]
fn d1_accept_builds_edge_and_reject_requires_request() {
    let req = follow_request();
    let edge = build_follow_from_request(Some(&req), "fid1".to_string()).unwrap();
    assert_eq!(edge.follower_id, req.follower_id);
    assert_eq!(edge.followee_id, req.followee_id);
    assert!(build_follow_from_request(None, "fid1".to_string()).is_err());
    assert!(require_follow_request(Some(&req)).is_ok());
    assert_eq!(
        require_follow_request(None),
        Err(DomainError::NotFound("NO_FOLLOW_REQUEST".to_string()))
    );
}

// --- D2: note edit/delete permission + tombstone ----------------------------

#[test]
fn d2_edit_delete_and_tombstone_rules() {
    let live = note_by("author");
    assert!(can_edit_note(&live, "author").is_ok());
    assert!(can_edit_note(&live, "stranger").is_err());
    assert!(can_delete_note(&live, "author", false).is_ok());
    assert!(can_delete_note(&live, "moderator", true).is_ok());
    assert!(can_delete_note(&live, "stranger", false).is_err());
    assert!(tombstone_transition(&live).is_ok());
    let mut dead = note_by("author");
    dead.is_deleted = true;
    assert!(can_edit_note(&dead, "author").is_err());
    assert!(can_delete_note(&dead, "author", false).is_err());
    assert!(tombstone_transition(&dead).is_err());
}

// --- D3: reaction unreact/idempotency ---------------------------------------

#[test]
fn d3_react_outcomes_and_unreact_gate() {
    assert_eq!(decide_react(None, "👍"), ReactOutcome::Created);
    assert_eq!(decide_react(Some("👍"), "👍"), ReactOutcome::Idempotent);
    assert_eq!(decide_react(Some("❤"), "👍"), ReactOutcome::Replaced);
    let row = Reaction {
        id: "r1".to_string(),
        user_id: "u1".to_string(),
        note_id: "n1".to_string(),
        reaction: "👍".to_string(),
    };
    assert!(can_unreact(Some(&row)).is_ok());
    assert_eq!(
        can_unreact(None),
        Err(DomainError::NotFound("NOT_REACTED".to_string()))
    );
}

// --- D4: rename/move state machine ------------------------------------------

#[test]
fn d4_rename_and_move_rules() {
    let local = Account::new_local("aid00000000000001".to_string(), "alice".to_string()).unwrap();
    assert!(validate_rename(&local, "bob", false).is_ok());
    assert!(validate_rename(&local, "bob", true).is_err());
    assert!(validate_move(&local, "https://new.example/users/alice", 7).is_ok());
    let mut moved =
        Account::new_local("aid00000000000002".to_string(), "carol".to_string()).unwrap();
    moved.moved_to_uri = Some("https://new.example/users/carol".to_string());
    assert!(validate_move(&moved, "https://other.example/u", 8).is_err());
}

// --- D5: MFM sanitize policy -------------------------------------------------

#[test]
fn d5_sanitize_keeps_mfm_drops_dangerous_html() {
    let plain = "**bold** @alice #tag :emoji:";
    assert_eq!(sanitize_mfm(plain), plain);
    assert_eq!(sanitize_mfm("a<script>alert(1)</script>b"), "ab");
    assert_eq!(
        sanitize_mfm("[x](javascript:alert(1))"),
        "[x](blocked:alert(1))"
    );
}

// --- D6: role_ids enforcement --------------------------------------------------

#[test]
fn d6_gated_emoji_requires_role_or_moderator() {
    let open = CustomEmoji {
        id: "e1".to_string(),
        shortcode: "x".to_string(),
        category: None,
        media_id: None,
        aliases: Vec::new(),
        license: None,
        is_sensitive: false,
        local_only: false,
        role_ids: Vec::new(),
        created_at_ms: 0,
    };
    assert!(emoji_usable_by(&open, &[], false));
    let gated = CustomEmoji {
        role_ids: vec!["r1".to_string()],
        ..open
    };
    assert!(emoji_usable_by(&gated, &["r1".to_string()], false));
    assert!(!emoji_usable_by(&gated, &[], false));
    assert!(emoji_usable_by(&gated, &[], true));
}

// --- D7: pinned_page_id / verifiedLinks re-verify loop -------------------------

#[test]
fn d7_pinned_page_shape_and_reverify_loop() {
    assert!(validate_pinned_page_update(None).is_ok());
    assert!(validate_pinned_page_update(Some("page1")).is_ok());
    assert!(validate_pinned_page_update(Some("")).is_err());
    assert!(reset_verified_links_for_reverify().is_empty());
    assert!(needs_link_reverify(None, 100));
    assert!(!needs_link_reverify(Some(100), 100));
}

// --- D8: clip notesCount ---------------------------------------------------------

#[test]
fn d8_notes_count_dedups_members() {
    assert_eq!(count_clip_notes(&[]), 0);
    assert_eq!(count_clip_notes(&["n1".to_string(), "n1".to_string()]), 1);
    let clip = Clip::new(
        "c1".to_string(),
        "u1".to_string(),
        "x".to_string(),
        None,
        true,
        0,
    );
    assert!(clip.last_clipped_at_ms.is_none());
}

// --- D9: antenna exclude_sensitive + 7-day auto-deactivate ------------------------

#[test]
fn d9_antenna_sensitivity_gate_and_stale_deactivation() {
    let n = note_by("u1");
    let (a, v) = antenna(&n);
    assert!(antenna_matches_with_sensitivity(&a, &v, true, false));
    assert!(!antenna_matches_with_sensitivity(&a, &v, true, true));
    assert_eq!(ANTENNA_AUTO_DEACTIVATE_AFTER_MS, 7 * 24 * 60 * 60 * 1000);
    assert!(should_auto_deactivate(0, 7 * 24 * 60 * 60 * 1000));
    assert!(!should_auto_deactivate(0, 7 * 24 * 60 * 60 * 1000 - 1));
    let (mut stale, _) = antenna(&n);
    stale.last_used_at_ms = 0;
    assert!(deactivate_stale_antenna(
        &mut stale,
        8 * 24 * 60 * 60 * 1000
    ));
    assert!(!stale.is_active);
}

// --- D10: signin input + password policy -------------------------------------------

#[test]
fn d10_signin_shape_and_password_policy() {
    assert!(validate_signin_input("alice", "s3cret!!").is_ok());
    assert!(validate_signin_input("", "s3cret!!").is_err());
    assert!(password_meets_policy("s3cret!!").is_ok());
    assert!(password_meets_policy("short").is_err());
}

// --- D11: hashtag-trend backing ------------------------------------------------------

#[test]
fn d11_trend_aggregation_skips_deleted_and_ranks() {
    let mut a = note_by("u1");
    a.tags = vec!["Rust".to_string(), "async".to_string()];
    let mut b = note_by("u2");
    b.tags = vec!["rust".to_string()];
    let mut dead = note_by("u3");
    dead.is_deleted = true;
    dead.tags = vec!["rust".to_string()];
    let counts = aggregate_hashtag_counts([&a, &b, &dead]);
    assert_eq!(counts.get("rust"), Some(&2));
    let ranked = rank_hashtag_trends(&counts, 1);
    assert_eq!(ranked, vec![("rust".to_string(), 2)]);
}
