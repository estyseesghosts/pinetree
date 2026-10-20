use super::AppServices;

use crate::application::events::NotifyJobPayload;
use crate::application::ports::{JobKind, NewJob, PortError};
use crate::domain::notification::NotificationKind;

impl AppServices {
    /// Notifier-less delivery gate (Batch P; generalizes the
    /// `exportCompleted` gate to `test`): with no `notifierId` only a
    /// `never` receive-config on that kind drops the notification (Misskey
    /// `NotificationService.ts:97-104` parity); self-notify and mute checks
    /// do not apply. A missing notifiee fails open.
    pub fn system_notification_allowed(
        &self,
        notifiee_id: &str,
        kind: NotificationKind,
    ) -> Result<bool, PortError> {
        let Some(notifiee) = self.accounts.find_by_id(notifiee_id)? else {
            return Ok(true);
        };
        let Some(config) = notifiee
            .notification_recieve_config
            .get(kind.wire_type())
        else {
            return Ok(true);
        };
        Ok(config.kind.as_str() != "never")
    }

    /// Enqueue a `test` notification for `account_id` (Misskey
    /// `notifications/test-notification.ts`:
    /// `createNotification(user.id, 'test', {})`). Self-notified through
    /// the normal `Notify` path so type filters and grouped rendering
    /// exercise it like any other kind; the worker applies the
    /// notifier-less gate, so only a `never` test-config drops it.
    pub fn send_test_notification(&self, account_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(account_id)?;
        let payload = NotifyJobPayload {
            notifiee_id: account_id.to_string(),
            notifier_id: account_id.to_string(),
            kind: NotificationKind::Test,
            note_id: None,
            reaction: None,
            file_id: None,
            exported_entity: None,
        };
        let job = NewJob::new_json(
            JobKind::Notify,
            &payload,
            format!("notify:test:{account_id}:{}", self.ids.generate()),
            self.clock.now_ms(),
        )?;
        self.jobs.enqueue_jobs(vec![job])
    }
    /// Creation-time receive gate (Misskey `NotificationService`
    /// `#createNotificationInternal` parity, `notificationRecieveConfig`
    /// `UserProfile.ts:252-272` + `types.ts:28-57`).
    ///
    /// Called by the `Notify` job handler before inserting the row, so every
    /// creation path (local ops and inbound federation, which all enqueue
    /// `Notify` jobs) converges here. A `false` return converges quietly:
    /// the job completes with no row and no `main`-channel hints, exactly
    /// like upstream returning `null`.
    ///
    /// Rules, in upstream order:
    /// - self-notify (`notifiee == notifier`) drops;
    /// - `mutings.has(notifier)` drops (active mutes only);
    /// - `notificationRecieveConfig[kind]` (`None`/`all` = deliver):
    ///   `never` drops; `following` requires the notifiee to follow the
    ///   notifier; `follower` requires the notifier to follow the notifiee;
    ///   `mutualFollow` requires both; `followingOrFollower` requires
    ///   either; `list(userListId)` requires membership in the notifiee's
    ///   list (Batch O WP16; dangling/foreign lists deliver nothing, never
    ///   error — upstream missing-list parity).
    ///
    /// A missing notifiee account fails open (`true`): the row is harmless
    /// and this preserves the pre-§11 behavior for notifier-less and remote
    /// edge cases. Scheduler-direct inserts (`emit_scheduled_notification`)
    /// bypass this gate like upstream notifier-less notifications do.
    pub fn notification_delivery_allowed(
        &self,
        notifiee_id: &str,
        kind: NotificationKind,
        notifier_id: &str,
    ) -> Result<bool, PortError> {
        if notifiee_id == notifier_id {
            return Ok(false);
        }
        if self.mutes.is_muting(notifiee_id, notifier_id)? {
            return Ok(false);
        }
        let Some(notifiee) = self.accounts.find_by_id(notifiee_id)? else {
            return Ok(true);
        };
        let Some(config) = notifiee.notification_recieve_config.get(kind.wire_type()) else {
            return Ok(true);
        };
        match config.kind.as_str() {
            "all" => Ok(true),
            "never" => Ok(false),
            "following" => self.follows.is_following(notifiee_id, notifier_id),
            "follower" => self.follows.is_following(notifier_id, notifiee_id),
            "mutualFollow" => Ok(
                self.follows.is_following(notifiee_id, notifier_id)?
                    && self.follows.is_following(notifier_id, notifiee_id)?,
            ),
            "followingOrFollower" => Ok(
                self.follows.is_following(notifiee_id, notifier_id)?
                    || self.follows.is_following(notifier_id, notifiee_id)?,
            ),
            // Batch O WP16: list configs deliver when the notifier is a
            // member of the referenced list (dangling/foreign lists deliver
            // nothing, never error — upstream missing-list parity).
            "list" => match config.user_list_id.as_deref() {
                Some(list_id) => match self.lists.get_user_list(list_id) {
                    Ok(Some(list)) if list.owner_id == notifiee_id => Ok(self
                        .lists
                        .get_list_membership(list_id, notifier_id)?
                        .is_some()),
                    _ => Ok(false),
                },
                None => Ok(false),
            },
            // Stored configs are validated on write; an unknown surviving
            // value fails open rather than dropping notifications silently.
            _ => Ok(true),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::application::ports::{
        ActivityDedup, BlobStore, Clock, CreateAccountInput, IdGenerator, NoopStreamingSink,
        TimelineCursor, UpdateAccountPatch,
    };
    use crate::domain::account::NotificationRecieveConfig as ProfileNotifConfig;
    use crate::domain::id::{AidxGenerator, ManualClock};
    use crate::domain::note::Visibility;
    use crate::federation::delivery::{SafeHttpClient, SafeHttpConfig};
    use crate::jobs::worker::JobWorker;
    use crate::media::storage::FsBlobStore;
    use crate::persistence::redb::RedbStore;

    /// Same harness shape as the sibling command modules (redb-backed
    /// services + a real worker), plus the concrete clock so notification
    /// ids stay deterministic.
    fn notify_services(prefix: &str) -> (AppServices, Arc<ManualClock>) {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-notify-test-{}-{prefix}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let clock = Arc::new(ManualClock::new(1_760_400_000_000));
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
            blobs,
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
        }
    }

    fn set_receive_config(services: &AppServices, user_id: &str, kind: &str, config: ProfileNotifConfig) {
        let mut map = std::collections::BTreeMap::new();
        map.insert(kind.to_string(), config);
        services
            .update_profile(
                user_id,
                UpdateAccountPatch {
                    notification_recieve_config: Some(map),
                    ..Default::default()
                },
            )
            .unwrap();
    }

    fn notifs_for(services: &AppServices, user_id: &str) -> Vec<crate::domain::notification::Notification> {
        services
            .notifications
            .list_notifications_filtered(
                user_id,
                &TimelineCursor::with_limit(100),
                &[],
                &[],
            )
            .unwrap()
    }

    /// Drive one follow end-to-end (op enqueues `Notify`, worker inserts)
    /// and return the notifiee's stored notifications.
    fn follow_notifs(services: &Arc<AppServices>, follower_id: &str, followee_id: &str) -> usize {
        // `follow` to a locked account emits ReceiveFollowRequest instead;
        // both flow through the same gate, so the count is what matters.
        let _ = services.follow(follower_id, followee_id, false);
        worker_for(services).drain(100).unwrap();
        notifs_for(services, followee_id).len()
    }

    #[test]
    fn self_notify_drops_at_creation() {
        let (services, _) = notify_services("self-drop");
        let alice = services.create_account("alice", None).unwrap();
        let services = Arc::new(services);
        // A `Notify` job addressed to its own author converges quietly: no
        // row, no error, job completes.
        let job = services
            .notify_job(&alice.id, &alice.id, NotificationKind::Mention, None, None, "self")
            .unwrap();
        services.jobs.enqueue_jobs(vec![job]).unwrap();
        worker_for(&services).drain(100).unwrap();
        assert!(notifs_for(&services, &alice.id).is_empty());
    }

    #[test]
    fn muter_drops_notifier_at_creation() {
        let (services, _) = notify_services("mute-drop");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        services.mute(&alice.id, &bob.id, None).unwrap();
        let services = Arc::new(services);
        assert_eq!(follow_notifs(&services, &bob.id, &alice.id), 0);
        // Control: unmuted notifiers still deliver.
        let carol = services.create_account("carol", None).unwrap();
        assert_eq!(follow_notifs(&services, &carol.id, &alice.id), 1);
    }

    #[test]
    fn receive_config_never_drops() {
        let (services, _) = notify_services("never");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        set_receive_config(
            &services,
            &alice.id,
            "follow",
            ProfileNotifConfig { kind: "never".to_string(), user_list_id: None },
        );
        let services = Arc::new(services);
        assert_eq!(follow_notifs(&services, &bob.id, &alice.id), 0);
    }

    #[test]
    fn receive_config_relational_kinds() {
        let (services, _) = notify_services("relational");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let carol = services.create_account("carol", None).unwrap();
        let services = Arc::new(services);
        // bob follows alice; carol is a stranger either way.
        services.follow(&bob.id, &alice.id, false).unwrap();
        worker_for(&services).drain(100).unwrap();

        // `following`: notifiee must follow the notifier (bob→alice only
        // so far, so nobody passes for alice yet).
        set_receive_config(
            &services,
            &alice.id,
            "mention",
            ProfileNotifConfig { kind: "following".to_string(), user_list_id: None },
        );
        assert!(!services.notification_delivery_allowed(&alice.id, NotificationKind::Mention, &bob.id).unwrap());
        assert!(!services.notification_delivery_allowed(&alice.id, NotificationKind::Mention, &carol.id).unwrap());
        services.follow(&alice.id, &bob.id, false).unwrap();
        worker_for(&services).drain(100).unwrap();
        assert!(services.notification_delivery_allowed(&alice.id, NotificationKind::Mention, &bob.id).unwrap());

        // `follower`: notifier must follow the notifiee.
        set_receive_config(
            &services,
            &alice.id,
            "mention",
            ProfileNotifConfig { kind: "follower".to_string(), user_list_id: None },
        );
        assert!(services.notification_delivery_allowed(&alice.id, NotificationKind::Mention, &bob.id).unwrap());
        assert!(!services.notification_delivery_allowed(&alice.id, NotificationKind::Mention, &carol.id).unwrap());

        // `mutualFollow`: both directions required (alice↔bob now).
        set_receive_config(
            &services,
            &alice.id,
            "mention",
            ProfileNotifConfig { kind: "mutualFollow".to_string(), user_list_id: None },
        );
        assert!(services.notification_delivery_allowed(&alice.id, NotificationKind::Mention, &bob.id).unwrap());
        assert!(!services.notification_delivery_allowed(&alice.id, NotificationKind::Mention, &carol.id).unwrap());

        // `followingOrFollower`: either direction passes.
        set_receive_config(
            &services,
            &alice.id,
            "mention",
            ProfileNotifConfig { kind: "followingOrFollower".to_string(), user_list_id: None },
        );
        assert!(services.notification_delivery_allowed(&alice.id, NotificationKind::Mention, &bob.id).unwrap());
        assert!(!services.notification_delivery_allowed(&alice.id, NotificationKind::Mention, &carol.id).unwrap());
    }

    #[test]
    fn receive_config_list_without_lists_drops() {
        let (services, _) = notify_services("list-drop");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let carol = services.create_account("carol", None).unwrap();
        // Dangling list id: delivers nothing, never errors.
        set_receive_config(
            &services,
            &alice.id,
            "mention",
            ProfileNotifConfig { kind: "list".to_string(), user_list_id: Some("lst1".to_string()) },
        );
        let services = Arc::new(services);
        assert!(!services.notification_delivery_allowed(&alice.id, NotificationKind::Mention, &bob.id).unwrap());
        // Member delivers; non-member and foreign lists drop.
        let list = services.create_user_list(&alice.id, "l".to_string()).unwrap();
        services.push_list_member(&alice.id, &list.id, &bob.id).unwrap();
        set_receive_config(
            &services,
            &alice.id,
            "mention",
            ProfileNotifConfig { kind: "list".to_string(), user_list_id: Some(list.id.clone()) },
        );
        assert!(services.notification_delivery_allowed(&alice.id, NotificationKind::Mention, &bob.id).unwrap());
        assert!(!services.notification_delivery_allowed(&alice.id, NotificationKind::Mention, &carol.id).unwrap());
        assert!(!services.notification_delivery_allowed(&bob.id, NotificationKind::Mention, &bob.id).unwrap());
    }

    #[test]
    fn receive_config_end_to_end_through_mention_and_reaction() {
        let (services, _) = notify_services("e2e");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        // `never` on mention/reaction drops worker-created rows end to end.
        // (One update: the patch REPLACES the whole config map.)
        let mut map = std::collections::BTreeMap::new();
        for kind in ["mention", "reaction"] {
            map.insert(
                kind.to_string(),
                ProfileNotifConfig { kind: "never".to_string(), user_list_id: None },
            );
        }
        services
            .update_profile(
                &alice.id,
                UpdateAccountPatch { notification_recieve_config: Some(map), ..Default::default() },
            )
            .unwrap();
        // Mention via create_note, reaction via react: both enqueue Notify
        // jobs that the worker must now drop.
        let alice_note = services
            .create_note(crate::application::ports::CreateNoteInput::simple_public(
                &alice.id,
                "alice here",
            ))
            .unwrap();
        services
            .create_note(crate::application::ports::CreateNoteInput::simple_public(
                &bob.id,
                "hello @alice",
            ))
            .unwrap();
        // A reaction notifies the note's author (alice here).
        services.react(&bob.id, &alice_note.id, "👍", false).unwrap();
        let services = Arc::new(services);
        worker_for(&services).drain(100).unwrap();
        assert!(notifs_for(&services, &alice.id).is_empty());
        // Control: default config delivers both.
        services
            .update_profile(
                &alice.id,
                UpdateAccountPatch {
                    notification_recieve_config: Some(std::collections::BTreeMap::new()),
                    ..Default::default()
                },
            )
            .unwrap();
        services
            .create_note(crate::application::ports::CreateNoteInput::simple_public(
                &bob.id,
                "hello again @alice",
            ))
            .unwrap();
        services.react(&bob.id, &alice_note.id, "🎉", true).unwrap();
        worker_for(&services).drain(100).unwrap();
        assert_eq!(notifs_for(&services, &alice.id).len(), 2);
    }

    #[test]
    fn absent_or_all_config_delivers() {
        let (services, _) = notify_services("default-allow");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        // No config row at all, and an explicit `all`, both deliver.
        assert!(services.notification_delivery_allowed(&alice.id, NotificationKind::Follow, &bob.id).unwrap());
        set_receive_config(
            &services,
            &alice.id,
            "follow",
            ProfileNotifConfig { kind: "all".to_string(), user_list_id: None },
        );
        let services = Arc::new(services);
        assert_eq!(follow_notifs(&services, &bob.id, &alice.id), 1);
    }

    #[test]
    fn remote_author_note_carries_host_for_instance_mutes() {
        // §12 helper coverage: remote-authored notes expose `user_host` so
        // `mutedInstances` has something to match (wired in `queries.rs`).
        let (services, _) = notify_services("remote-host");
        let remote = services
            .accounts
            .create_account(CreateAccountInput {
                handle: "rina".to_string(),
                host: Some("remote.example".to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some("https://remote.example/users/rina".to_string()),
                inbox: Some("https://remote.example/inbox".to_string()),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            }, None)
            .unwrap();
        let note = services
            .create_note(crate::application::ports::CreateNoteInput {
                author_id: remote.id.clone(),
                text: Some("remote hello".to_string()),
                visibility: Visibility::Public,
                ..crate::application::ports::CreateNoteInput::simple_public(&remote.id, "x")
            })
            .unwrap();
        assert_eq!(note.user_host.as_deref(), Some("remote.example"));
    }
}
