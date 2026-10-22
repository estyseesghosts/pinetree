use crate::application::commands::AppServices;

use crate::application::ports::{
    CreateDraftInput, CreateNoteInput, JobKind, NewJob, PortError, ScheduledDraftPayload,
    TimelineCursor, UpdateDraftPatch,
};
use crate::domain::draft::{NoteDraft, validate_draft_poll_expiry, validate_draft_schedule};
use crate::domain::id::DomainError;
use crate::domain::notification::NotificationKind;
use crate::domain::poll::PollInput;

use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::application::ports::{ActivityDedup, BlobStore, Clock, IdGenerator, NoopStreamingSink};
use crate::domain::id::{AidxGenerator, ManualClock};
use crate::domain::note::Visibility;
use crate::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use crate::jobs::worker::JobWorker;
use crate::media::storage::FsBlobStore;
use crate::media::transcode::TranscodeConfig;
use crate::persistence::redb::RedbStore;
use crate::services::email::{SmtpConfig, SmtpEmailSender};

fn draft_services(prefix: &str) -> (AppServices, Arc<ManualClock>) {
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pinetree-draft-test-{}-{prefix}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let clock: Arc<ManualClock> = Arc::new(ManualClock::new(1_760_400_000_000));
    let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
    let store = RedbStore::open(dir.join("test.redb"), clock.clone(), ids.clone()).unwrap();
    let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.join("blobs")).unwrap());
    let services = AppServices {
        clock: clock.clone() as Arc<dyn Clock>,
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
        public_url: "https://example.local".to_string(),
        max_file_size: 10 * 1024 * 1024,
        preserved_usernames: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        note_draft_limit: crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
        scheduled_note_limit: crate::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
        pin_limit: crate::domain::pinning::DEFAULT_PIN_LIMIT,
        default_like: None,
        cache_blobs: blobs.clone(),
        drive_capacity_per_user_bytes: 1 << 30,
        remote_cache_max_bytes: 512 << 20,
        remote_cache_avatar_bytes: 128 << 20,
        remote_cache_preview_bytes: 256 << 20,
        remote_cache_full_bytes: 128 << 20,
        remote_cache_mode: crate::config::RemoteCacheMode::OnDemand,
        remote_cache_avatar_ttl_days: 30,
        remote_cache_preview_ttl_days: 7,
        remote_cache_full_ttl_days: 3,
        remote_cache_warming_enabled: false,
    };
    (services, clock)
}

fn worker_for(services: &Arc<AppServices>) -> JobWorker {
    JobWorker {
        clock: services.clock.clone(),
        ids: services.ids.clone(),
        jobs: services.jobs.clone(),
        notifications: services.notifications.clone(),
        streaming: services.streaming.clone(),
        services: services.clone(),
        delivery_tracker: Arc::new(crate::federation::retry::DestinationTracker::new()),
        lease_duration_ms: 30_000,
        base_backoff_ms: 1_000,
        transcode_config: TranscodeConfig::default(),
        email_sender: Arc::new(SmtpEmailSender::new(SmtpConfig::default())),
    }
}

fn port_code(err: PortError) -> String {
    match err {
        PortError::Validation(m) | PortError::Conflict(m) | PortError::NotFound(m) => m,
        PortError::StorageUnavailable(m) => panic!("storage fault: {m}"),
    }
}

fn simple_draft() -> CreateDraftInput {
    CreateDraftInput {
        text: Some("draft body".to_string()),
        visibility: Visibility::Public,
        ..Default::default()
    }
}

fn scheduled_input(at_ms: i64) -> CreateDraftInput {
    CreateDraftInput {
        text: Some("scheduled body".to_string()),
        visibility: Visibility::Public,
        scheduled_at_ms: Some(at_ms),
        is_actually_scheduled: true,
        ..Default::default()
    }
}

#[test]
fn draft_crud_count_and_list_filters() {
    let (services, _) = draft_services("crud");
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();

    assert_eq!(services.count_drafts(&alice.id).unwrap(), 0);
    let d1 = services.create_draft(&alice.id, simple_draft()).unwrap();
    assert_eq!(d1.user_id, alice.id);
    assert!(!d1.is_actually_scheduled);
    let now = services.clock.now_ms();
    let d2 = services
        .create_draft(&alice.id, scheduled_input(now + 60_000))
        .unwrap();
    assert_eq!(services.count_drafts(&alice.id).unwrap(), 2);
    assert_eq!(services.count_drafts(&bob.id).unwrap(), 0);

    // List newest-first; `scheduled` tri-state filters.
    let all = services
        .list_drafts(&alice.id, &TimelineCursor::with_limit(10), None)
        .unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].id, d2.id, "newest first");
    let scheduled_only = services
        .list_drafts(&alice.id, &TimelineCursor::with_limit(10), Some(true))
        .unwrap();
    assert_eq!(scheduled_only.len(), 1);
    assert_eq!(scheduled_only[0].id, d2.id);
    let unscheduled_only = services
        .list_drafts(&alice.id, &TimelineCursor::with_limit(10), Some(false))
        .unwrap();
    assert_eq!(unscheduled_only.len(), 1);
    assert_eq!(unscheduled_only[0].id, d1.id);

    // Update merges the patch over the stored row.
    let updated = services
        .update_draft(
            &alice.id,
            &d1.id,
            UpdateDraftPatch {
                text: Some(Some("edited".to_string())),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(updated.text.as_deref(), Some("edited"));
    assert_eq!(updated.user_id, alice.id);

    // Foreign drafts are invisible to the updater.
    let err = services
        .update_draft(
            &bob.id,
            &d1.id,
            UpdateDraftPatch {
                text: Some(Some("hijack".to_string())),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(port_code(err), "NO_SUCH_NOTE_DRAFT");

    // Delete + count.
    services.delete_draft(&alice.id, &d1.id).unwrap();
    assert_eq!(services.count_drafts(&alice.id).unwrap(), 1);
    services.delete_draft(&alice.id, &d2.id).unwrap();
    assert_eq!(services.count_drafts(&alice.id).unwrap(), 0);
    let err = services.delete_draft(&alice.id, &d1.id).unwrap_err();
    assert_eq!(port_code(err), "NO_SUCH_NOTE_DRAFT");
}

#[test]
fn draft_schedule_gates_carry_exact_upstream_codes() {
    let (services, clock) = draft_services("schedule");
    let alice = services.create_account("alice", None).unwrap();
    let now = clock.now_ms();

    // Scheduled without a time.
    let err = services
        .create_draft(
            &alice.id,
            CreateDraftInput {
                is_actually_scheduled: true,
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(port_code(err), "SCHEDULED_AT_REQUIRED");

    // Non-future times (past and exactly-now).
    for at in [now - 1, now] {
        let err = services
            .create_draft(&alice.id, scheduled_input(at))
            .unwrap_err();
        assert_eq!(port_code(err), "SCHEDULED_AT_MUST_BE_IN_FUTURE");
    }

    // One scheduled draft fits the default `scheduledNoteLimit` of 1;
    // the second is rejected with the exact upstream code.
    services
        .create_draft(&alice.id, scheduled_input(now + 60_000))
        .unwrap();
    let err = services
        .create_draft(&alice.id, scheduled_input(now + 120_000))
        .unwrap_err();
    assert_eq!(port_code(err), "TOO_MANY_SCHEDULED_NOTES");

    // Unscheduled drafts are unaffected by the scheduled cap.
    let alice_plain = services.create_draft(&alice.id, simple_draft()).unwrap();

    // Update: flipping `isActuallyScheduled` without a time in the
    // same patch is REQUIRED even when the row already has one (the
    // patch is validated as a partial, upstream parity). A fresh
    // account avoids the scheduled cap tripping first (upstream
    // checks the cap before validating, same as here).
    let bob = services.create_account("bob", None).unwrap();
    let bob_plain = services.create_draft(&bob.id, simple_draft()).unwrap();
    let err = services
        .update_draft(
            &bob.id,
            &bob_plain.id,
            UpdateDraftPatch {
                is_actually_scheduled: Some(true),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(port_code(err), "SCHEDULED_AT_REQUIRED");

    // Update: scheduling past the cap is rejected too.
    let err = services
        .update_draft(
            &alice.id,
            &alice_plain.id,
            UpdateDraftPatch {
                scheduled_at_ms: Some(Some(now + 30_000)),
                is_actually_scheduled: Some(true),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(port_code(err), "TOO_MANY_SCHEDULED_NOTES");
}

#[test]
fn draft_limit_and_referential_gates() {
    let (mut services, _) = draft_services("limits");
    services.note_draft_limit = 2;
    let alice = services.create_account("alice", None).unwrap();
    services.create_draft(&alice.id, simple_draft()).unwrap();
    services.create_draft(&alice.id, simple_draft()).unwrap();
    let err = services
        .create_draft(&alice.id, simple_draft())
        .unwrap_err();
    assert_eq!(port_code(err), "TOO_MANY_DRAFTS");
    // Lift the cap for the referential checks below (they would
    // otherwise trip the draft limit first).
    services.note_draft_limit = 100;

    // Unknown files / targets are rejected with the note-create idents.
    let err = services
        .create_draft(
            &alice.id,
            CreateDraftInput {
                file_ids: vec!["mid00000000000001".to_string()],
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(port_code(err), "NO_SUCH_FILE");
    let err = services
        .create_draft(
            &alice.id,
            CreateDraftInput {
                reply_to: Some("nid00000000000001".to_string()),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(port_code(err), "NO_SUCH_REPLY_TARGET");
    let err = services
        .create_draft(
            &alice.id,
            CreateDraftInput {
                renote_of: Some("nid00000000000001".to_string()),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(port_code(err), "NO_SUCH_RENOTE_TARGET");

    // Blocked by the reply target's author.
    let bob = services.create_account("bob", None).unwrap();
    let target = crate::application::ports::CreateNoteInput::simple_public(&bob.id, "hi");
    let target = services.create_note(target).unwrap();
    services.block(&bob.id, &alice.id).unwrap();
    let err = services
        .create_draft(
            &alice.id,
            CreateDraftInput {
                reply_to: Some(target.id),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(port_code(err), "YOU_HAVE_BEEN_BLOCKED");
}

#[test]
fn scheduled_post_fires_through_the_worker() {
    let (services, clock) = draft_services("fire");
    let alice = services.create_account("alice", None).unwrap();
    let now = clock.now_ms();
    let draft = services
        .create_draft(&alice.id, scheduled_input(now + 60_000))
        .unwrap();
    let services = Arc::new(services);
    let worker = worker_for(&services);

    // Not due yet: the arming job is not claimable and nothing posts.
    worker.drain(100).unwrap();
    assert!(services.notes.get_draft(&draft.id).unwrap().is_some());

    // Past the scheduled time the worker posts the note, deletes the
    // draft, and emits `ScheduledNotePosted` carrying the note.
    clock.advance_ms(61_000);
    worker.drain(100).unwrap();
    assert!(services.notes.get_draft(&draft.id).unwrap().is_none());
    let notes = services
        .notes
        .notes_by_author(&alice.id, &TimelineCursor::with_limit(10))
        .unwrap();
    assert_eq!(notes.len(), 1);
    assert_eq!(notes[0].text.as_deref(), Some("scheduled body"));
    let notifs = services
        .notifications
        .list_notifications_filtered(
            &alice.id,
            &TimelineCursor::with_limit(10),
            &[NotificationKind::ScheduledNotePosted],
            &[],
        )
        .unwrap();
    assert_eq!(notifs.len(), 1);
    assert_eq!(notifs[0].note_id, Some(notes[0].id.clone()));
    // The kind round-trips through serde as the upstream camelCase type.
    let kind_json = serde_json::to_value(&notifs[0].kind).unwrap();
    assert_eq!(kind_json, serde_json::json!("scheduledNotePosted"));
}

#[test]
fn scheduled_post_failure_keeps_the_draft_and_notifies() {
    let (services, clock) = draft_services("fail");
    let alice = services.create_account("alice", None).unwrap();
    let now = clock.now_ms();
    // A draft replying to a note that is deleted before post time:
    // the post-time `create_note` rejects it, so the draft is kept
    // and `ScheduledNotePostFailed` carries the draft.
    let target = crate::application::ports::CreateNoteInput::simple_public(&alice.id, "target");
    let target = services.create_note(target).unwrap();
    let draft = services
        .create_draft(
            &alice.id,
            CreateDraftInput {
                text: Some("reply draft".to_string()),
                visibility: Visibility::Public,
                reply_to: Some(target.id.clone()),
                scheduled_at_ms: Some(now + 60_000),
                is_actually_scheduled: true,
                ..Default::default()
            },
        )
        .unwrap();
    services.delete_note(&alice.id, &target.id).unwrap();
    let services = Arc::new(services);
    let worker = worker_for(&services);
    clock.advance_ms(61_000);
    worker.drain(100).unwrap();
    assert!(services.notes.get_draft(&draft.id).unwrap().is_some());
    let notifs = services
        .notifications
        .list_notifications_filtered(
            &alice.id,
            &TimelineCursor::with_limit(10),
            &[NotificationKind::ScheduledNotePostFailed],
            &[],
        )
        .unwrap();
    assert_eq!(notifs.len(), 1);
    assert_eq!(notifs[0].draft_id, Some(draft.id.clone()));
    // The failed kind filters through the same exclude machinery.
    let excluded = services
        .notifications
        .list_notifications_filtered(
            &alice.id,
            &TimelineCursor::with_limit(10),
            &[],
            &[NotificationKind::ScheduledNotePostFailed],
        )
        .unwrap();
    assert!(excluded.is_empty());
}

#[test]
fn draft_update_reschedules_and_delete_clears() {
    let (services, clock) = draft_services("resched");
    let alice = services.create_account("alice", None).unwrap();
    let now = clock.now_ms();
    let draft = services
        .create_draft(&alice.id, scheduled_input(now + 60_000))
        .unwrap();
    // Move the arming to +120s.
    let merged = services
        .update_draft(
            &alice.id,
            &draft.id,
            UpdateDraftPatch {
                scheduled_at_ms: Some(Some(now + 120_000)),
                is_actually_scheduled: Some(true),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(merged.scheduled_at_ms, Some(now + 120_000));
    let services = Arc::new(services);
    let worker = worker_for(&services);
    // The stale +60s arming is gone: nothing posts at +61s.
    clock.advance_ms(61_000);
    worker.drain(100).unwrap();
    assert!(services.notes.get_draft(&draft.id).unwrap().is_some());
    assert!(
        services
            .notes
            .notes_by_author(&alice.id, &TimelineCursor::with_limit(10))
            .unwrap()
            .is_empty()
    );
    // The re-armed +120s job posts exactly once.
    clock.advance_ms(60_000);
    worker.drain(100).unwrap();
    assert!(services.notes.get_draft(&draft.id).unwrap().is_none());
    assert_eq!(
        services
            .notes
            .notes_by_author(&alice.id, &TimelineCursor::with_limit(10))
            .unwrap()
            .len(),
        1
    );

    // Delete clears the arming: a deleted draft never posts.
    let clock_now = clock.now_ms();
    let doomed = services
        .create_draft(&alice.id, scheduled_input(clock_now + 60_000))
        .unwrap();
    services.delete_draft(&alice.id, &doomed.id).unwrap();
    clock.advance_ms(61_000);
    worker.drain(100).unwrap();
    assert_eq!(
        services
            .notes
            .notes_by_author(&alice.id, &TimelineCursor::with_limit(10))
            .unwrap()
            .len(),
        1,
        "only the earlier note exists"
    );
}
