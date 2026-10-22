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
use crate::api::misskey::packers::PACKER_VERSION;
use crate::api::misskey::packers::format_millis;
use crate::api::misskey::packers::pack_meta;

#[test]
fn snapshot_meta_matches_checkout() {
    let (services, _, _) = test_services(1_700_000_000_000);
    let Some(lite) = load_props("meta.ts", "packedMetaLiteSchema") else {
        return;
    };
    let Some(detailed_only) = load_props("meta.ts", "packedMetaDetailedOnlySchema") else {
        return;
    };

    let packed = pack_meta(&services, false).unwrap();
    assert_shape(&packed, &lite, "MetaLite");
    assert_eq!(packed["version"], PACKER_VERSION);
    assert_eq!(packed["uri"], "https://example.local");
    assert_eq!(packed["maxNoteTextLength"], 3000);

    let packed = pack_meta(&services, true).unwrap();
    let mut all = lite;
    all.extend(detailed_only);
    assert_shape(&packed, &all, "MetaDetailed");
    assert_eq!(packed["features"]["miauth"], true);
    assert_eq!(packed["features"]["localTimeline"], true);
    assert_eq!(packed["features"]["registration"], true);
}

#[test]
fn timestamps_are_iso8601_utc() {
    assert_eq!(format_millis(0), "1970-01-01T00:00:00.000Z");
    let s = format_millis(1_700_000_000_123);
    assert!(s.ends_with('Z'), "must be UTC: {s}");
    assert!(s.contains('T'), "must be ISO-8601: {s}");
}
