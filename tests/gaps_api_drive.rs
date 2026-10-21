//! WP2a gaps API: drive folders + folder-aware file bulk ops + small
//! social/read surfaces over existing stores.
//!
//! Real redb backend (same harness shape as `batch_q_phase1_app.rs`) for
//! media/notes/follows/lists/pages; an in-memory fake implements the
//! proposed [`DriveFolderStore`] until the parent lands the redb
//! implementation + `AppServices` field.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use pinetree::application::commands::AppServices;
use pinetree::application::commands::drive_folders::DriveFolderStore;
use pinetree::application::commands::pages::PageBody;
use pinetree::application::ports::{
    BlobStore, Clock, CreateNoteInput, IdGenerator, NewJob, NoopStreamingSink, PortError,
    TimelineCursor,
};
use pinetree::domain::drive_folder::DriveFolder;
use pinetree::domain::id::{AidxGenerator, ManualClock};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

fn test_services(prefix: &str) -> AppServices {
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pinetree-gaps-drive-{}-{prefix}-{}",
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
    }
}

/// In-memory [`DriveFolderStore`] fake (test-only stand-in for the parent's
/// redb implementation). Single-transaction semantics hold trivially: every
/// method is one lock acquisition.
#[derive(Default)]
struct MemFolderStore {
    folders: Mutex<HashMap<String, DriveFolder>>,
    placements: Mutex<HashMap<String, Option<String>>>,
}

impl DriveFolderStore for MemFolderStore {
    fn create_folder(
        &self,
        folder: DriveFolder,
        _jobs: Vec<NewJob>,
    ) -> Result<DriveFolder, PortError> {
        self.folders.lock().unwrap().insert(folder.id.clone(), folder.clone());
        Ok(folder)
    }
    fn get_folder(&self, id: &str) -> Result<Option<DriveFolder>, PortError> {
        Ok(self.folders.lock().unwrap().get(id).cloned())
    }
    fn update_folder(
        &self,
        folder: DriveFolder,
        _jobs: Vec<NewJob>,
    ) -> Result<DriveFolder, PortError> {
        self.folders.lock().unwrap().insert(folder.id.clone(), folder.clone());
        Ok(folder)
    }
    fn delete_folder(&self, id: &str, _jobs: Vec<NewJob>) -> Result<(), PortError> {
        self.folders.lock().unwrap().remove(id);
        Ok(())
    }
    fn child_folders(
        &self,
        owner_id: &str,
        parent_id: Option<&str>,
    ) -> Result<Vec<DriveFolder>, PortError> {
        let mut out: Vec<DriveFolder> = self
            .folders
            .lock()
            .unwrap()
            .values()
            .filter(|f| f.owner_id == owner_id && f.parent_id.as_deref() == parent_id)
            .cloned()
            .collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }
    fn find_folders_by_name(
        &self,
        owner_id: &str,
        name: &str,
        parent_id: Option<&str>,
    ) -> Result<Vec<DriveFolder>, PortError> {
        let mut out: Vec<DriveFolder> = self
            .folders
            .lock()
            .unwrap()
            .values()
            .filter(|f| {
                f.owner_id == owner_id && f.name == name && f.parent_id.as_deref() == parent_id
            })
            .cloned()
            .collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }
    fn count_child_folders(&self, folder_id: &str) -> Result<u64, PortError> {
        Ok(self
            .folders
            .lock()
            .unwrap()
            .values()
            .filter(|f| f.parent_id.as_deref() == Some(folder_id))
            .count() as u64)
    }
    fn set_file_folder(
        &self,
        file_id: &str,
        folder_id: Option<&str>,
        _jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        self.placements
            .lock()
            .unwrap()
            .insert(file_id.to_string(), folder_id.map(str::to_string));
        Ok(())
    }
    fn file_folder_id(&self, file_id: &str) -> Result<Option<Option<String>>, PortError> {
        Ok(self.placements.lock().unwrap().get(file_id).cloned())
    }
    fn count_files_in_folder(&self, folder_id: &str) -> Result<u64, PortError> {
        Ok(self
            .placements
            .lock()
            .unwrap()
            .values()
            .filter(|f| f.as_deref() == Some(folder_id))
            .count() as u64)
    }
}

fn upload(services: &AppServices, owner: &str, name: &str, bytes: &[u8]) -> String {
    services
        .upload_media(owner, name.to_string(), bytes, None, false, true)
        .unwrap()
        .id
}

#[test]
fn folders_crud_nested_and_guards() {
    let services = test_services("folders-crud");
    let store = MemFolderStore::default();
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();

    // create: default name + nested parents.
    let untitled = services
        .create_drive_folder(&store, &alice.id, None, None)
        .unwrap();
    assert_eq!(untitled.name, "Untitled");
    assert_eq!(untitled.parent_id, None);
    let root = services
        .create_drive_folder(&store, &alice.id, Some("docs".to_string()), None)
        .unwrap();
    let child = services
        .create_drive_folder(
            &store,
            &alice.id,
            Some("sub".to_string()),
            Some(root.id.clone()),
        )
        .unwrap();
    assert_eq!(child.parent_id.as_deref(), Some(root.id.as_str()));
    let grandchild = services
        .create_drive_folder(
            &store,
            &alice.id,
            Some("deep".to_string()),
            Some(child.id.clone()),
        )
        .unwrap();

    // show + find (folderId null = root) + child listing.
    assert_eq!(
        services.show_drive_folder(&store, &alice.id, &child.id).unwrap(),
        child
    );
    let found = services
        .find_drive_folders(&store, &alice.id, "sub", Some(&root.id))
        .unwrap();
    assert_eq!(found, vec![child.clone()]);
    assert!(
        services
            .find_drive_folders(&store, &alice.id, "sub", None)
            .unwrap()
            .is_empty()
    );
    let kids = services.child_drive_folders(&store, &alice.id, None).unwrap();
    assert_eq!(kids.len(), 2); // Untitled + docs
    let sub = services
        .child_drive_folders(&store, &alice.id, Some(&root.id))
        .unwrap();
    assert_eq!(sub, vec![child.clone()]);

    // Miss and foreign both read as NO_SUCH_FOLDER.
    assert_eq!(
        services.show_drive_folder(&store, &alice.id, "nope"),
        Err(PortError::NotFound("NO_SUCH_FOLDER".to_string()))
    );
    assert_eq!(
        services.show_drive_folder(&store, &bob.id, &root.id),
        Err(PortError::NotFound("NO_SUCH_FOLDER".to_string()))
    );
    // Foreign parent on create.
    assert_eq!(
        services.create_drive_folder(&store, &bob.id, Some("x".to_string()), Some(root.id.clone())),
        Err(PortError::NotFound("NO_SUCH_FOLDER".to_string()))
    );

    // Recursive nesting: while the chain root → child → grandchild holds,
    // moving the root under its grandchild fails, as does self-parenting.
    assert_eq!(
        services.update_drive_folder(
            &store,
            &alice.id,
            &root.id,
            None,
            Some(Some(grandchild.id.clone())),
        ),
        Err(PortError::Validation("RECURSIVE_NESTING".to_string()))
    );
    assert_eq!(
        services.update_drive_folder(
            &store,
            &alice.id,
            &child.id,
            None,
            Some(Some(child.id.clone())),
        ),
        Err(PortError::Validation("RECURSIVE_NESTING".to_string()))
    );

    // update: rename + move to root.
    let renamed = services
        .update_drive_folder(
            &store,
            &alice.id,
            &child.id,
            Some("renamed".to_string()),
            Some(None),
        )
        .unwrap();
    assert_eq!(renamed.name, "renamed");
    assert_eq!(renamed.parent_id, None);

    // Missing/foreign move target.
    assert_eq!(
        services.update_drive_folder(
            &store,
            &alice.id,
            &root.id,
            None,
            Some(Some("nope".to_string())),
        ),
        Err(PortError::NotFound("NO_SUCH_PARENT_FOLDER".to_string()))
    );

    // delete: non-empty refuses, empty succeeds.
    assert_eq!(
        services.delete_drive_folder(&store, &alice.id, &child.id),
        Err(PortError::Conflict(
            "HAS_CHILD_FILES_OR_FOLDERS".to_string()
        ))
    );
    services.delete_drive_folder(&store, &alice.id, &grandchild.id).unwrap();
    services.delete_drive_folder(&store, &alice.id, &child.id).unwrap();
    assert_eq!(
        services.show_drive_folder(&store, &alice.id, &child.id),
        Err(PortError::NotFound("NO_SUCH_FOLDER".to_string()))
    );
    // Foreign delete reads as NO_SUCH_FOLDER.
    assert_eq!(
        services.delete_drive_folder(&store, &bob.id, &root.id),
        Err(PortError::NotFound("NO_SUCH_FOLDER".to_string()))
    );
}

#[test]
fn files_move_find_existence_and_folder_leg() {
    let services = test_services("files-bulk");
    let store = MemFolderStore::default();
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();
    let folder = services
        .create_drive_folder(&store, &alice.id, Some("pics".to_string()), None)
        .unwrap();

    let a = upload(&services, &alice.id, "a.txt", b"aaa");
    let b = upload(&services, &alice.id, "a.txt", b"bbb");

    // find before the move: both at root.
    assert_eq!(
        services.find_drive_files(&store, &alice.id, "a.txt", None).unwrap().len(),
        2
    );
    // move-bulk one file into the folder.
    services
        .move_drive_files(&store, &alice.id, &[a.clone()], Some(&folder.id))
        .unwrap();
    let in_folder = services
        .find_drive_files(&store, &alice.id, "a.txt", Some(&folder.id))
        .unwrap();
    assert_eq!(in_folder.len(), 1);
    assert_eq!(in_folder[0].id, a);
    let at_root = services
        .find_drive_files(&store, &alice.id, "a.txt", None)
        .unwrap();
    assert_eq!(at_root.len(), 1);
    assert_eq!(at_root[0].id, b);

    // move-bulk skips missing/unowned ids; bogus folder errors.
    services
        .move_drive_files(&store, &alice.id, &[b.clone(), "ghost".to_string(), a.clone()], None)
        .unwrap();
    assert!(
        services
            .find_drive_files(&store, &alice.id, "a.txt", Some(&folder.id))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        services.move_drive_files(&store, &alice.id, &[a.clone()], Some("ghost")),
        Err(PortError::NotFound("NO_SUCH_FOLDER".to_string()))
    );
    // Bob's folder is foreign to alice.
    let bob_folder = services
        .create_drive_folder(&store, &bob.id, Some("b".to_string()), None)
        .unwrap();
    assert_eq!(
        services.move_drive_files(&store, &alice.id, &[a.clone()], Some(&bob_folder.id)),
        Err(PortError::NotFound("NO_SUCH_FOLDER".to_string()))
    );

    // check-existence is owner-scoped md5.
    let md5_a = services.media.get(&a).unwrap().unwrap().hash;
    assert!(services.drive_file_exists(&alice.id, &md5_a).unwrap());
    assert!(!services.drive_file_exists(&alice.id, "0".repeat(32).as_str()).unwrap());
    assert!(!services.drive_file_exists(&bob.id, &md5_a).unwrap());

    // drive/files/update folder leg.
    services
        .set_drive_file_folder(&store, &alice.id, &a, Some(&folder.id), false)
        .unwrap();
    assert_eq!(
        services
            .find_drive_files(&store, &alice.id, "a.txt", Some(&folder.id))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        services.set_drive_file_folder(&store, &alice.id, &a, Some("ghost"), false),
        Err(PortError::NotFound("NO_SUCH_FOLDER".to_string()))
    );
    assert_eq!(
        services.set_drive_file_folder(&store, &bob.id, &a, None, false),
        Err(PortError::Validation("ACCESS_DENIED".to_string()))
    );
    // Moderator leg bypasses the owner check.
    services
        .set_drive_file_folder(&store, &bob.id, &a, None, true)
        .unwrap();
}

#[test]
fn attached_notes_lists_referencing_notes() {
    let services = test_services("attached");
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();
    let file = upload(&services, &alice.id, "pic.png", b"png-bytes");

    let mut input = CreateNoteInput::simple_public(&alice.id, "with file");
    input.file_ids = vec![file.clone()];
    let note = services.create_note(input).unwrap();
    services
        .create_note(CreateNoteInput::simple_public(&alice.id, "no file"))
        .unwrap();

    let cursor = TimelineCursor {
        limit: 10,
        ..Default::default()
    };
    let notes = services.attached_notes(&alice.id, &file, false, &cursor).unwrap();
    assert_eq!(notes.len(), 1);
    assert_eq!(notes[0].id, note.id);

    // Missing file and foreign file (non-moderator) both 404.
    assert_eq!(
        services.attached_notes(&alice.id, "ghost", false, &cursor),
        Err(PortError::NotFound("NO_SUCH_FILE".to_string()))
    );
    assert_eq!(
        services.attached_notes(&bob.id, &file, false, &cursor),
        Err(PortError::NotFound("NO_SUCH_FILE".to_string()))
    );
    // Moderator sees the foreign file's notes.
    let mod_notes = services.attached_notes(&bob.id, &file, true, &cursor).unwrap();
    assert_eq!(mod_notes.len(), 1);

    // untilId cursor truncates.
    let paged = services
        .attached_notes(
            &alice.id,
            &file,
            false,
            &TimelineCursor {
                until_id: Some(note.id.clone()),
                limit: 10,
                ..Default::default()
            },
        )
        .unwrap();
    assert!(paged.is_empty());
}

#[test]
fn drive_stream_filters_by_type() {
    let services = test_services("stream");
    let alice = services.create_account("alice", None).unwrap();
    let mut png = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
    png.extend_from_slice(&[0x00, 0x01]);
    upload(&services, &alice.id, "a.png", &png);
    upload(&services, &alice.id, "b.txt", b"hello");

    let cursor = TimelineCursor {
        limit: 10,
        ..Default::default()
    };
    let all = services.stream_drive_files(&alice.id, &cursor, None, None).unwrap();
    assert_eq!(all.len(), 2);
    let images = services
        .stream_drive_files(&alice.id, &cursor, Some("image/"), None)
        .unwrap();
    assert_eq!(images.len(), 1);
    assert_eq!(images[0].mime, "image/png");
}

#[test]
fn following_list_returns_own_edges() {
    let services = test_services("following-list");
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();
    let carol = services.create_account("carol", None).unwrap();
    services.follow(&alice.id, &bob.id, false).unwrap();
    services.follow(&alice.id, &carol.id, true).unwrap();

    let cursor = TimelineCursor {
        limit: 10,
        ..Default::default()
    };
    let edges = services.following_list(&alice.id, false, &cursor).unwrap();
    assert_eq!(edges.len(), 2);
    // No notify preferences set on these edges.
    assert!(services.following_list(&alice.id, true, &cursor).unwrap().is_empty());
    // Limit applies.
    let one = services
        .following_list(
            &alice.id,
            false,
            &TimelineCursor {
                limit: 1,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(one.len(), 1);
    // Empty for a non-follower.
    assert!(services.following_list(&bob.id, false, &cursor).unwrap().is_empty());
}

#[test]
fn create_list_from_public_copies_members() {
    let services = test_services("create-from-public");
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();
    let carol = services.create_account("carol", None).unwrap();

    let source = services.create_user_list(&alice.id, "pub".to_string()).unwrap();
    services.push_list_member(&alice.id, &source.id, &carol.id).unwrap();
    services
        .update_user_list(&alice.id, &source.id, None, Some(true))
        .unwrap();

    let copy = services
        .create_list_from_public(&bob.id, "mine".to_string(), &source.id)
        .unwrap();
    assert_eq!(copy.owner_id, bob.id);
    assert!(!copy.is_public);
    let members = services.lists.members_of_list(&copy.id).unwrap();
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].member_id, carol.id);

    // Private source lists 404.
    let priv_list = services.create_user_list(&alice.id, "priv".to_string()).unwrap();
    assert_eq!(
        services.create_list_from_public(&bob.id, "x".to_string(), &priv_list.id),
        Err(PortError::NotFound("NO_SUCH_LIST".to_string()))
    );
    assert_eq!(
        services.create_list_from_public(&bob.id, "x".to_string(), "ghost"),
        Err(PortError::NotFound("NO_SUCH_LIST".to_string()))
    );
    // Name bound enforced (1..=100).
    assert!(
        services
            .create_list_from_public(&bob.id, "".to_string(), &source.id)
            .is_err()
    );
}

#[test]
fn pages_of_user_lists_public_pages() {
    let services = test_services("pages-of-user");
    let alice = services.create_account("alice", None).unwrap();
    services
        .create_page(
            &alice.id,
            PageBody {
                title: Some("hello".to_string()),
                name: Some("hello".to_string()),
                content: Some(serde_json::json!([{"type": "text", "text": "hi"}])),
                variables: Some(serde_json::json!([])),
                script: Some("".to_string()),
                ..Default::default()
            },
        )
        .unwrap();

    let cursor = TimelineCursor {
        limit: 10,
        ..Default::default()
    };
    let pages = services.pages_of_user(&alice.id, &cursor).unwrap();
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0].name, "hello");
    // Unknown users yield [] (upstream query parity).
    assert!(services.pages_of_user("ghost", &cursor).unwrap().is_empty());
}
