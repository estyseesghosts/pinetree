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
use crate::api::misskey::packers::pack_following;
use crate::api::misskey::packers::pack_me_detailed;
use crate::api::misskey::packers::pack_user_detailed_not_me;
use crate::api::misskey::packers::pack_user_lite;

// --- Snapshot tests against the Misskey checkout ------------------------

#[test]
fn snapshot_user_lite_matches_checkout() {
    let account = test_account("aid00000000000001", "alice");
    let packed = pack_user_lite(&account, "https://example.local", 1_700_000_000_000);
    let Some(props) = load_props("user.ts", "packedUserLiteSchema") else {
        return;
    };
    assert_shape(&packed, &props, "UserLite");
    assert_eq!(packed["username"], "alice");
    assert_eq!(packed["host"], Value::Null);
    assert_eq!(
        packed["avatarUrl"],
        "https://example.local/identicon/aid00000000000001"
    );
}

#[test]
fn snapshot_user_detailed_matches_checkout() {
    let (services, mem, _) = test_services(1_700_000_000_000);
    let alice = test_account("aid00000000000001", "alice");
    let mut bob = test_account("aid00000000000002", "bob");
    bob.display_name = Some("Bob".to_string());
    bob.avatar_id = Some("mid00000000000001".to_string());
    mem.accounts
        .lock()
        .unwrap()
        .insert(alice.id.clone(), alice.clone());
    mem.accounts
        .lock()
        .unwrap()
        .insert(bob.id.clone(), bob.clone());
    mem.follows
        .lock()
        .unwrap()
        .insert((alice.id.clone(), bob.id.clone()));

    let packed = pack_user_detailed_not_me(&bob, Some(&alice.id), &services).unwrap();
    let Some(lite) = load_props("user.ts", "packedUserLiteSchema") else {
        return;
    };
    let Some(detailed) = load_props("user.ts", "packedUserDetailedNotMeOnlySchema") else {
        return;
    };
    let mut all = lite;
    all.extend(detailed);
    assert_shape(&packed, &all, "UserDetailedNotMe");
    assert_eq!(packed["name"], "Bob");
    assert_eq!(packed["isFollowing"], true);
    assert_eq!(packed["isFollowed"], false);
    assert_eq!(
        packed["avatarUrl"],
        "https://example.local/files/mid00000000000001"
    );
}

#[test]
fn snapshot_me_detailed_matches_checkout() {
    let (services, mem, _) = test_services(1_700_000_000_000);
    let alice = test_account("aid00000000000001", "alice");
    mem.accounts
        .lock()
        .unwrap()
        .insert(alice.id.clone(), alice.clone());

    let Some(lite) = load_props("user.ts", "packedUserLiteSchema") else {
        return;
    };
    let Some(detailed) = load_props("user.ts", "packedUserDetailedNotMeOnlySchema") else {
        return;
    };
    let Some(me_only) = load_props("user.ts", "packedMeDetailedOnlySchema") else {
        return;
    };

    let packed = pack_me_detailed(&alice, false, &services).unwrap();
    let mut all = lite.clone();
    all.extend(detailed.clone());
    all.extend(me_only.clone());
    // Secrets are `optional: true`; drop them from the required set.
    let required: Vec<_> = all
        .into_iter()
        .filter(|(name, _, optional, _)| {
            *optional == false
                && name != "email"
                && name != "emailVerified"
                && name != "securityKeysList"
        })
        .collect();
    assert_shape(&packed, &required, "MeDetailed");
    assert!(packed.get("email").is_none());

    let packed = pack_me_detailed(&alice, true, &services).unwrap();
    assert_eq!(packed["email"], Value::Null);
    assert_eq!(packed["securityKeysList"], Value::Array(Vec::new()));
    assert_eq!(packed["policies"]["driveCapacityMb"], 1024);
}

#[test]
fn me_detailed_packs_profile_store_defaults_and_values() {
    use crate::domain::account::ProfileField;

    let (services, mem, _) = test_services(1_700_000_000_000);
    let alice = test_account("aid00000000000001", "alice");
    mem.accounts
        .lock()
        .unwrap()
        .insert(alice.id.clone(), alice.clone());

    // Fresh accounts pack the upstream column defaults (were hardcoded
    // `false` before §0B).
    let packed = pack_me_detailed(&alice, false, &services).unwrap();
    assert_eq!(packed["preventAiLearning"], true);
    assert_eq!(packed["injectFeaturedNote"], true);
    assert_eq!(packed["receiveAnnouncementEmail"], true);
    assert_eq!(packed["chatScope"], "mutual");
    assert_eq!(packed["publicReactions"], true);
    assert_eq!(packed["followingVisibility"], "public");
    assert_eq!(packed["followersVisibility"], "public");
    assert_eq!(
        packed["emailNotificationTypes"],
        serde_json::json!(["follow", "receiveFollowRequest"])
    );
    assert_eq!(packed["mutedWords"], serde_json::json!([]));
    assert_eq!(packed["notificationRecieveConfig"], serde_json::json!({}));
    assert_eq!(packed["hideOnlineStatus"], false);
    assert_eq!(packed["onlineStatus"], "online");
    assert_eq!(packed["rejectQuotes"], false);
    assert_eq!(packed["defaultReaction"], Value::Null);

    // Stored values round-trip with Misskey shapes.
    let mut stored = alice.clone();
    stored.hide_online_status = true;
    stored.lang = Some("en".to_string());
    stored.fields = vec![ProfileField {
        name: "site".to_string(),
        value: "https://example.local".to_string(),
    }];
    stored.followed_message = Some("hi".to_string());
    stored.following_visibility = crate::domain::account::FollowVisibility::Followers;
    stored.chat_scope = crate::domain::account::ChatScope::None;
    stored.muted_words = vec![crate::domain::account::MutedWord::Single(
        "/x/i".to_string(),
    )];
    stored.muted_instances = vec!["spam.example".to_string()];
    stored.follow_approval_local_seconds = Some(60);
    stored.also_known_as = Some(vec!["https://example.local/users/".to_string()]);
    stored.reject_quotes = true;
    stored.default_reaction = Some("❤".to_string());
    let packed = pack_me_detailed(&stored, false, &services).unwrap();
    assert_eq!(packed["onlineStatus"], "unknown");
    assert_eq!(packed["lang"], "en");
    assert_eq!(
        packed["fields"],
        serde_json::json!([{"name": "site", "value": "https://example.local"}])
    );
    assert_eq!(packed["followedMessage"], "hi");
    assert_eq!(packed["followingVisibility"], "followers");
    assert_eq!(packed["chatScope"], "none");
    assert_eq!(packed["mutedWords"], serde_json::json!(["/x/i"]));
    assert_eq!(
        packed["mutedInstances"],
        serde_json::json!(["spam.example"])
    );
    assert_eq!(packed["followApprovalLocalSeconds"], 60);
    // Unresolvable alias URIs pack as null (upstream maps to ids).
    assert_eq!(packed["alsoKnownAs"], Value::Null);
    assert_eq!(packed["rejectQuotes"], true);
    assert_eq!(packed["defaultReaction"], "❤");

    // Remote viewers never see public reactions.
    let mut remote = test_account("aid00000000000002", "bob");
    remote.host = Some("remote.example".to_string());
    let packed = pack_user_detailed_not_me(&remote, Some(&alice.id), &services).unwrap();
    assert_eq!(packed["publicReactions"], false);
}

#[test]
fn ff_visibility_count_masking_cases() {
    use crate::domain::account::FollowVisibility;

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

    // Public: everyone (including anonymous) sees counts.
    let packed = pack_user_detailed_not_me(&alice, None, &services).unwrap();
    assert_eq!(packed["followersCount"], 3);
    assert_eq!(packed["followingCount"], 5);

    // Private: only self sees counts.
    let mut shy = alice.clone();
    shy.followers_visibility = FollowVisibility::Private;
    shy.following_visibility = FollowVisibility::Private;
    let packed = pack_user_detailed_not_me(&shy, Some(&bob.id), &services).unwrap();
    assert_eq!(packed["followersCount"], Value::Null);
    assert_eq!(packed["followingCount"], Value::Null);
    let packed = pack_user_detailed_not_me(&shy, Some(&alice.id), &services).unwrap();
    assert_eq!(packed["followersCount"], 3);
    assert_eq!(packed["followingCount"], 5);

    // Followers-only: self + followers see counts; strangers see null.
    let mut semi = alice.clone();
    semi.followers_visibility = FollowVisibility::Followers;
    semi.following_visibility = FollowVisibility::Followers;
    let packed = pack_user_detailed_not_me(&semi, Some(&bob.id), &services).unwrap();
    assert_eq!(packed["followersCount"], Value::Null);
    assert_eq!(packed["followingCount"], Value::Null);
    mem.follows
        .lock()
        .unwrap()
        .insert((bob.id.clone(), alice.id.clone()));
    let packed = pack_user_detailed_not_me(&semi, Some(&bob.id), &services).unwrap();
    assert_eq!(packed["followersCount"], 3);
    assert_eq!(packed["followingCount"], 5);
}

#[test]
fn public_reactions_pack_rule_local_profile_remote_false() {
    let (services, mem, _) = test_services(1_700_000_000_000);
    let alice = test_account("aid00000000000001", "alice");
    mem.accounts
        .lock()
        .unwrap()
        .insert(alice.id.clone(), alice.clone());

    // `publicReactions: isLocal ? profile : false`.
    let packed = pack_user_detailed_not_me(&alice, None, &services).unwrap();
    assert_eq!(packed["publicReactions"], true);
    let mut hidden = alice.clone();
    hidden.public_reactions = false;
    let packed = pack_user_detailed_not_me(&hidden, None, &services).unwrap();
    assert_eq!(packed["publicReactions"], false);
    let mut remote = alice.clone();
    remote.host = Some("remote.example".to_string());
    remote.public_reactions = true;
    let packed = pack_user_detailed_not_me(&remote, None, &services).unwrap();
    assert_eq!(packed["publicReactions"], false);
}

#[test]
fn pack_following_carries_both_sides_and_id_time() {
    use crate::domain::follow::Follow;
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
    let follow = Follow {
        id: "fid00000000000001".to_string(),
        follower_id: alice.id.clone(),
        followee_id: bob.id.clone(),
        with_replies: true,
        notify: None,
    };
    let packed = pack_following(&follow, None, &services).unwrap();
    assert_eq!(packed["id"], "fid00000000000001");
    assert_eq!(packed["followerId"], alice.id);
    assert_eq!(packed["followeeId"], bob.id);
    assert_eq!(packed["follower"]["id"], alice.id);
    assert_eq!(packed["followee"]["id"], bob.id);
    assert_eq!(packed["follower"]["username"], "alice");
    assert_eq!(packed["followee"]["username"], "bob");
    assert!(packed["createdAt"].as_str().is_some());
}
