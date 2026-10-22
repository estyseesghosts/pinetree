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
use crate::api::misskey::packers::pack_drive_file;

#[test]
fn slice8_pack_drive_file_md5_stable_for_same_bytes() {
    // Slice 8 (D6, written before the fix): the client-visible `md5` is
    // the stored md5 field, byte-identical to the pre-slice value for
    // the same bytes — the blob-key migration must not move it, and the
    // SHA-256 address must never leak into the packed shape.
    let (services, _, _) = test_services(1_700_000_000_000);
    let media = test_media("mid00000000000001", "aid00000000000001");
    assert_eq!(media.hash, "d41d8cd98f00b204e9800998ecf8427e");
    let packed = pack_drive_file(&media, &services);
    assert_eq!(packed["md5"], "d41d8cd98f00b204e9800998ecf8427e");
    assert!(packed.get("blob_key").is_none());
    assert!(packed.get("sha256").is_none());
}

#[test]
fn snapshot_drive_file_matches_checkout() {
    let (services, _, _) = test_services(1_700_000_000_000);
    let media = test_media("mid00000000000001", "aid00000000000001");
    let packed = pack_drive_file(&media, &services);
    let Some(props) = load_props("drive-file.ts", "packedDriveFileSchema") else {
        return;
    };
    assert_shape(&packed, &props, "DriveFile");
    assert_eq!(packed["type"], "image/png");
    assert_eq!(packed["properties"]["width"], 1280);
}
