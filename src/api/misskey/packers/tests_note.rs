#![allow(unused_imports)]
#![allow(dead_code)]

use crate::application::commands::AppServices;
use crate::application::commands::tests::UnavailableStores;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::application::ports::{
    AccountStore, ActivityDedup, ApiTokenRecord, AuthStore, BlobStore, BlockStore,
    CreateAccountInput, CreateNoteInput, FollowStore, HostCapsStore, HostSoftware,
    InsertMediaInput, JobQueue, LegacyAuthSessionRecord, MediaStore, MiAuthSessionRecord,
    MuteStore, NewJob, NoteStore, NotificationStore, PollChoiceCount, PollStore, PortError,
    ReactionStore, StreamingSink, UpdateAccountPatch,
};
use crate::domain::account::Account;
use crate::domain::blocking::Blocking;
use crate::domain::follow::Follow;
use crate::domain::id::{AidxGenerator, ManualClock};
use crate::domain::media::MediaMeta;
use crate::domain::muting::{Muting, RenoteMuting};
use crate::domain::note::{Note, Visibility};
use crate::domain::notification::Notification;
use crate::domain::poll::{Poll, PollVote};
use crate::domain::reaction::Reaction;
use serde_json::Value;

use super::helpers::assert_shape;
use super::helpers::load_props;
use super::helpers::test_account;
use super::helpers::test_media;
use super::helpers::test_note;
use super::helpers::test_services;
use super::support::Mem;
use crate::api::misskey::packers::pack_note;
use crate::api::misskey::packers::pack_note_reaction_with_note;
use crate::domain::quote::QuoteAuthorizationState;
use crate::domain::quote::QuoteReference;

#[test]
fn pack_note_reaction_with_note_shape() {
    use crate::domain::reaction::Reaction;

    let (services, mem, _) = test_services(1_700_000_000_000);
    let alice = test_account("aid00000000000001", "alice");
    mem.accounts
        .lock()
        .unwrap()
        .insert(alice.id.clone(), alice.clone());
    let note = test_note("nid00000000000001", &alice.id);
    let reaction = Reaction {
        id: "rid00000000000001".to_string(),
        user_id: alice.id.clone(),
        note_id: note.id.clone(),
        reaction: "👍".to_string(),
    };
    let packed = pack_note_reaction_with_note(&reaction, &alice, &note, None, &services).unwrap();
    assert_eq!(packed["id"], "rid00000000000001");
    assert!(packed["createdAt"].as_str().is_some());
    assert_eq!(packed["user"]["id"], alice.id);
    assert_eq!(packed["type"], "👍");
    assert_eq!(packed["note"]["id"], note.id);
}

#[test]
fn snapshot_note_matches_checkout() {
    let (services, mem, _) = test_services(1_700_000_000_000);
    let alice = test_account("aid00000000000001", "alice");
    let bob = test_account("aid00000000000002", "bob");
    mem.accounts
        .lock()
        .unwrap()
        .insert(alice.id.clone(), alice.clone());
    mem.accounts
        .lock()
        .unwrap()
        .insert(bob.id.clone(), bob.clone());
    let media = test_media("mid00000000000001", "aid00000000000001");
    mem.media.lock().unwrap().insert(media.id.clone(), media);

    let target = test_note("nid00000000000001", &bob.id);
    let mut reply = test_note("nid00000000000002", &alice.id);
    reply.reply_id = Some(target.id.clone());
    reply.file_ids = vec!["mid00000000000001".to_string()];
    mem.notes.lock().unwrap().insert(target.id.clone(), target);
    mem.notes
        .lock()
        .unwrap()
        .insert(reply.id.clone(), reply.clone());
    mem.reactions.lock().unwrap().insert(
        (bob.id.clone(), reply.id.clone()),
        Reaction {
            id: "rid00000000000001".to_string(),
            user_id: bob.id.clone(),
            note_id: reply.id.clone(),
            reaction: "❤️".to_string(),
        },
    );

    let packed = pack_note(&reply, Some(&bob.id), &services, 0).unwrap();
    let Some(props) = load_props("note.ts", "packedNoteSchema") else {
        return;
    };
    assert_shape(&packed, &props, "Note");
    // Value checks beyond key presence.
    assert_eq!(packed["reactionCount"], 3);
    assert_eq!(packed["myReaction"], "❤️");
    assert_eq!(packed["files"].as_array().unwrap().len(), 1);
    assert_eq!(packed["reply"]["id"], "nid00000000000001");
    // Nested reply packs only one level deep.
    assert_eq!(packed["reply"]["reply"], Value::Null);
    // Specified notes carry visibleUserIds; public notes do not.
    assert!(packed.get("visibleUserIds").is_none());
    let mut specified = reply.clone();
    specified.visibility = Visibility::Specified;
    specified.visible_user_ids = vec![bob.id.clone()];
    let packed = pack_note(&specified, Some(&bob.id), &services, 0).unwrap();
    assert_eq!(packed["visibleUserIds"], serde_json::json!([bob.id]));
}

// --- Quote-post API projection (docs/quote-posts.md) --------------------

fn quote_note(
    id: &str,
    author_id: &str,
    target_note_id: Option<&str>,
    target_uri: &str,
    state: QuoteAuthorizationState,
) -> Note {
    let mut n = test_note(id, author_id);
    n.renote_id = target_note_id.map(|t| t.to_string());
    n.quote = Some(QuoteReference {
        target_uri: target_uri.to_string(),
        target_note_id: target_note_id.map(|t| t.to_string()),
        authorization: state,
        authorization_uri: None,
        request_activity_uri: None,
        last_verified_at: None,
    });
    n
}

fn insert_all(mem: &Mem, accounts: Vec<Account>, notes: Vec<Note>) {
    for acc in accounts {
        mem.accounts.lock().unwrap().insert(acc.id.clone(), acc);
    }
    for note in notes {
        mem.notes.lock().unwrap().insert(note.id.clone(), note);
    }
}

#[test]
fn pack_quote_visible_target_projects_status_and_renote() {
    let (services, mem, _) = test_services(1_700_000_000_000);
    let alice = test_account("aid00000000000001", "alice");
    let bob = test_account("aid00000000000002", "bob");
    let mut target = test_note("nid00000000000009", &bob.id);
    target.visibility = Visibility::Public;
    let quote = quote_note(
        "nid00000000000010",
        &alice.id,
        Some(&target.id),
        "https://example.local/notes/nid00000000000009",
        QuoteAuthorizationState::Pending,
    );
    insert_all(&mem, vec![alice.clone(), bob], vec![target, quote.clone()]);
    let packed = pack_note(&quote, Some(&alice.id), &services, 0).unwrap();
    // Same renote shape regardless of wire format, plus the extension.
    assert_eq!(packed["renoteId"], "nid00000000000009");
    assert!(packed["renote"].is_object());
    assert_eq!(packed["quoteAuthorizationStatus"], "pending");
    assert!(packed.get("quoteTargetUri").is_none());
    assert!(packed.get("quoteTargetUnavailable").is_none());
}

#[test]
fn pack_quote_invisible_target_marks_unavailable_never_leaks_uri() {
    let (services, mem, _) = test_services(1_700_000_000_000);
    let alice = test_account("aid00000000000001", "alice");
    let bob = test_account("aid00000000000002", "bob");
    let stranger = test_account("aid00000000000003", "stranger");
    let mut target = test_note("nid00000000000009", &bob.id);
    target.visibility = Visibility::Followers;
    let quote = quote_note(
        "nid00000000000010",
        &alice.id,
        Some(&target.id),
        "https://example.local/notes/nid00000000000009",
        QuoteAuthorizationState::Unapproved,
    );
    insert_all(
        &mem,
        vec![alice, bob, stranger.clone()],
        vec![target, quote.clone()],
    );
    let packed = pack_note(&quote, Some(&stranger.id), &services, 0).unwrap();
    // Authorization never overrides visibility; rejected/unapproved
    // quotes still render their relation as unavailable.
    assert_eq!(packed["quoteAuthorizationStatus"], "unapproved");
    assert!(packed["renote"].is_null());
    assert_eq!(packed["quoteTargetUnavailable"], true);
    assert!(
        packed.get("quoteTargetUri").is_none(),
        "no URI leak for protected targets"
    );
}

#[test]
fn pack_quote_unresolved_target_uri_rules() {
    let (services, mem, _) = test_services(1_700_000_000_000);
    let alice = test_account("aid00000000000001", "alice");
    // Public quoting post: the relationship survives with its URI.
    let quote = quote_note(
        "nid00000000000010",
        &alice.id,
        None,
        "https://remote.example/notes/1",
        QuoteAuthorizationState::Unapproved,
    );
    assert!(quote.renote_id.is_none(), "no fabricated renoteId");
    insert_all(&mem, vec![alice.clone()], vec![quote.clone()]);
    let packed = pack_note(&quote, None, &services, 0).unwrap();
    assert_eq!(packed["renoteId"], Value::Null);
    assert_eq!(packed["quoteTargetUri"], "https://remote.example/notes/1");
    assert_eq!(packed["quoteAuthorizationStatus"], "unapproved");

    // Followers-only quoting post: conservative unavailable marker.
    let mut restricted = quote.clone();
    restricted.id = "nid00000000000011".to_string();
    restricted.visibility = Visibility::Followers;
    mem.notes
        .lock()
        .unwrap()
        .insert(restricted.id.clone(), restricted.clone());
    let packed = pack_note(&restricted, Some(&alice.id), &services, 0).unwrap();
    assert!(packed.get("quoteTargetUri").is_none());
    assert_eq!(packed["quoteTargetUnavailable"], true);
}

#[test]
fn pack_pure_renote_omits_quote_keys() {
    let (services, mem, _) = test_services(1_700_000_000_000);
    let alice = test_account("aid00000000000001", "alice");
    let bob = test_account("aid00000000000002", "bob");
    let target = test_note("nid00000000000009", &bob.id);
    let mut renote = test_note("nid00000000000010", &alice.id);
    renote.text = None;
    renote.renote_id = Some(target.id.clone());
    assert!(renote.quote.is_none());
    insert_all(&mem, vec![alice.clone(), bob], vec![target, renote.clone()]);
    let packed = pack_note(&renote, Some(&alice.id), &services, 0).unwrap();
    assert!(packed.get("quoteAuthorizationStatus").is_none());
    assert!(packed.get("quoteTargetUri").is_none());
    assert!(packed.get("quoteTargetUnavailable").is_none());
}

#[test]
fn pack_rejected_quote_still_renders_with_status() {
    let (services, mem, _) = test_services(1_700_000_000_000);
    let alice = test_account("aid00000000000001", "alice");
    let bob = test_account("aid00000000000002", "bob");
    let target = test_note("nid00000000000009", &bob.id);
    let quote = quote_note(
        "nid00000000000010",
        &alice.id,
        Some(&target.id),
        "https://example.local/notes/nid00000000000009",
        QuoteAuthorizationState::Rejected,
    );
    insert_all(&mem, vec![alice.clone(), bob], vec![target, quote.clone()]);
    let packed = pack_note(&quote, Some(&alice.id), &services, 0).unwrap();
    assert_eq!(packed["quoteAuthorizationStatus"], "rejected");
    assert!(
        packed["renote"].is_object(),
        "rejected quotes keep rendering"
    );
}
