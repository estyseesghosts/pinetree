use std::collections::{HashMap, HashSet};

use crate::application::commands::AppServices;
use crate::application::events::{MainEventKind, MainMessage};
use crate::application::ports::{PortError, TimelineCursor};
use crate::domain::account::{Account, FollowVisibility};
use crate::domain::follow::Follow;
use crate::domain::instance::UgcVisibilityForVisitor;
use crate::domain::note::Note;
use crate::domain::policy::{
    ViewerContext, is_eligible_for_federated_timeline, is_eligible_for_local_timeline, is_visible,
};
use crate::domain::reaction::Reaction;

impl AppServices {
    #[cfg(test)]
    fn post_note(
        &self,
        author_id: &str,
        text: &str,
        reply_to: Option<&str>,
    ) -> Result<Note, PortError> {
        let mut input = crate::application::ports::CreateNoteInput::simple_public(author_id, text);
        input.reply_to = reply_to.map(str::to_string);
        self.create_note(input)
    }
}

use super::*;
use std::sync::Arc;

use crate::application::ports::{
    AccountStore, CreateAccountInput, CreateNoteInput, NoopStreamingSink, UpdateAccountPatch,
};
use crate::domain::account::MutedWord as ProfileMuteWord;
use crate::domain::note::Visibility;

fn accounts() -> (AppServices, String, String) {
    let services = query_services("queries-tl");
    let alice = services.create_account("alice", None).unwrap();
    let bob = services.create_account("bob", None).unwrap();
    (services, alice.id, bob.id)
}

/// Redb-backed services for the timeline tests (same harness shape as
/// the command modules; `commands::tests` is private to that subtree).
fn query_services(prefix: &str) -> AppServices {
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::application::ports::{ActivityDedup, BlobStore, Clock, IdGenerator};
    use crate::domain::id::{AidxGenerator, ManualClock};
    use crate::federation::delivery::{SafeHttpClient, SafeHttpConfig};
    use crate::media::storage::FsBlobStore;
    use crate::persistence::redb::RedbStore;

    static N: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pinetree-query-test-{}-{prefix}-{}",
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
    }
}

fn remote_account(services: &AppServices, handle: &str, host: &str) -> String {
    services
        .accounts
        .create_account(
            CreateAccountInput {
                handle: handle.to_string(),
                host: Some(host.to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some(format!("https://{host}/users/{handle}")),
                inbox: Some(format!("https://{host}/inbox")),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            },
            None,
        )
        .unwrap()
        .id
}

fn reply_input(author_id: &str, text: &str, reply_to: &str) -> CreateNoteInput {
    let mut input = CreateNoteInput::simple_public(author_id, text);
    input.reply_to = Some(reply_to.to_string());
    input
}

fn ids(notes: Vec<Note>) -> Vec<String> {
    notes.into_iter().map(|n| n.id).collect()
}

fn channel_params(with_replies: bool) -> TimelineChannelParams {
    TimelineChannelParams {
        with_files: false,
        with_renotes: true,
        with_replies,
    }
}

/// `AccountStore` that reports one account as suspended while delegating
/// everything else to the inner store. Suspension has no public write
/// path yet (no moderation infra), so read-path filtering is covered
/// through this stub instead of fabricated rows.
struct SuspendOne {
    inner: Arc<dyn AccountStore>,
    suspended_id: String,
}

impl AccountStore for SuspendOne {
    fn create_account(
        &self,
        input: CreateAccountInput,
        log: Option<&crate::domain::moderation::ModerationLogEntry>,
    ) -> Result<Account, PortError> {
        self.inner.create_account(input, log)
    }
    fn find_by_id(&self, id: &str) -> Result<Option<Account>, PortError> {
        let mut account = self.inner.find_by_id(id)?;
        if id == self.suspended_id {
            if let Some(account) = account.as_mut() {
                account.is_suspended = true;
            }
        }
        Ok(account)
    }
    fn find_by_handle(&self, h: &str) -> Result<Option<Account>, PortError> {
        self.inner.find_by_handle(h)
    }
    fn find_by_username_and_host(
        &self,
        username: &str,
        host: Option<&str>,
    ) -> Result<Option<Account>, PortError> {
        self.inner.find_by_username_and_host(username, host)
    }
    fn is_username_available(
        &self,
        username: &str,
        preserved: &[String],
    ) -> Result<bool, PortError> {
        self.inner.is_username_available(username, preserved)
    }
    fn update_account(
        &self,
        id: &str,
        patch: UpdateAccountPatch,
        jobs: Vec<crate::application::ports::NewJob>,
    ) -> Result<Account, PortError> {
        self.inner.update_account(id, patch, jobs)
    }
    fn rename(&self, id: &str, new_handle: &str) -> Result<Account, PortError> {
        self.inner.rename(id, new_handle)
    }
    fn get_owner(&self, id: &str) -> Result<String, PortError> {
        self.inner.get_owner(id)
    }
    fn set_owner(&self, id: &str, node: &str, proof: &str) -> Result<(), PortError> {
        self.inner.set_owner(id, node, proof)
    }
    fn search_accounts(
        &self,
        query: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Account>, PortError> {
        self.inner.search_accounts(query, offset, limit)
    }
    fn all_accounts(
        &self,
        after: Option<&str>,
        max_rows: usize,
    ) -> Result<(Vec<Account>, Option<String>), PortError> {
        self.inner.all_accounts(after, max_rows)
    }
    fn remote_shared_inboxes(
        &self,
        after: Option<&str>,
        max_rows: usize,
    ) -> Result<(Vec<String>, Option<String>), PortError> {
        self.inner.remote_shared_inboxes(after, max_rows)
    }
}

/// Same services but with `suspended_id` reading back suspended.
fn with_suspended(services: &AppServices, suspended_id: &str) -> AppServices {
    AppServices {
        accounts: Arc::new(SuspendOne {
            inner: services.accounts.clone(),
            suspended_id: suspended_id.to_string(),
        }),
        ..services.clone()
    }
}

#[test]
fn home_timeline_keeps_replies_from_followed_users() {
    // §12 DIVERGENCE: home has no `withReplies` param and includes
    // replies from followed users (Misskey parity would exclude every
    // reply except self-replies-to-self, i.e. `replyUserId == me`).
    let (services, alice, bob) = accounts();
    let carol = services.create_account("carol", None).unwrap().id;
    services.follow(&bob, &alice, false).unwrap();
    let root = services.post_note(&carol, "carol root", None).unwrap();
    let reply = services
        .create_note(reply_input(&alice, "alice replies", &root.id))
        .unwrap();
    assert_ne!(reply.reply_user_id.as_deref(), Some(alice.as_str()));
    let cursor = TimelineCursor::with_limit(10);
    let home = ids(services.home_timeline(&bob, &cursor, false, true).unwrap());
    assert!(
        home.contains(&reply.id),
        "followed author's reply stays on home"
    );
    assert!(
        !home.contains(&root.id),
        "unfollowed root stays out of home scope"
    );
}

#[test]
fn local_timeline_self_reply_exception() {
    let (services, alice, bob) = accounts();
    let root = services.post_note(&alice, "root", None).unwrap();
    let other_reply = services
        .create_note(reply_input(&bob, "bob replies", &root.id))
        .unwrap();
    assert_ne!(other_reply.reply_user_id.as_deref(), Some(bob.as_str()));
    let self_reply = services
        .create_note(reply_input(&alice, "alice self-reply", &root.id))
        .unwrap();
    assert_eq!(self_reply.reply_user_id.as_deref(), Some(alice.as_str()));
    let cursor = TimelineCursor::with_limit(10);
    // `with_replies=false` drops other-authored replies, keeps roots
    // and self-replies (Misskey local-timeline parity).
    let no_replies = ids(services
        .local_timeline(Some(&alice), &cursor, false, true, false)
        .unwrap());
    assert!(no_replies.contains(&root.id));
    assert!(no_replies.contains(&self_reply.id));
    assert!(!no_replies.contains(&other_reply.id));
    // `with_replies=true` carries everything.
    let with_replies = ids(services
        .local_timeline(Some(&alice), &cursor, false, true, true)
        .unwrap());
    assert!(with_replies.contains(&root.id));
    assert!(with_replies.contains(&self_reply.id));
    assert!(with_replies.contains(&other_reply.id));
}

#[test]
fn hybrid_timeline_self_reply_exception() {
    let (services, alice, bob) = accounts();
    let carol = services.create_account("carol", None).unwrap().id;
    services.follow(&bob, &alice, false).unwrap();
    let root = services.post_note(&alice, "root", None).unwrap();
    let self_reply = services
        .create_note(reply_input(&alice, "self-reply", &root.id))
        .unwrap();
    // A stranger's reply: local-side only, dropped without `withReplies`.
    let stranger_reply = services
        .create_note(reply_input(&carol, "carol replies", &root.id))
        .unwrap();
    let cursor = TimelineCursor::with_limit(10);
    let merged = ids(services
        .hybrid_timeline(&bob, &cursor, false, true, false)
        .unwrap());
    assert!(merged.contains(&root.id));
    assert!(
        merged.contains(&self_reply.id),
        "self-reply survives {merged:?}"
    );
    assert!(!merged.contains(&stranger_reply.id));
    let merged_all = ids(services
        .hybrid_timeline(&bob, &cursor, false, true, true)
        .unwrap());
    assert!(merged_all.contains(&stranger_reply.id));
}

#[test]
fn global_timeline_is_public_only_and_carries_replies() {
    let (services, alice, bob) = accounts();
    let root = services.post_note(&alice, "public root", None).unwrap();
    let self_reply = services
        .create_note(reply_input(&alice, "self-reply", &root.id))
        .unwrap();
    let other_reply = services
        .create_note(reply_input(&bob, "bob replies", &root.id))
        .unwrap();
    // Followers-only and local-only notes never federate.
    let mut followers_input = CreateNoteInput::simple_public(&alice, "followers");
    followers_input.visibility = Visibility::Followers;
    let followers_note = services.create_note(followers_input).unwrap();
    let mut local_input = CreateNoteInput::simple_public(&alice, "local only");
    local_input.local_only = true;
    let local_note = services.create_note(local_input).unwrap();
    // Remote public notes federate.
    let rina = remote_account(&services, "rina", "remote.example");
    let remote_note = services.post_note(&rina, "remote hello", None).unwrap();
    assert_eq!(remote_note.user_host.as_deref(), Some("remote.example"));
    let cursor = TimelineCursor::with_limit(20);
    let global = ids(services
        .global_timeline(None, &cursor, false, true)
        .unwrap());
    assert!(global.contains(&root.id));
    assert!(
        global.contains(&self_reply.id),
        "global carries self-replies"
    );
    assert!(global.contains(&other_reply.id), "global carries replies");
    assert!(
        global.contains(&remote_note.id),
        "global carries remote public notes"
    );
    assert!(!global.contains(&followers_note.id));
    assert!(!global.contains(&local_note.id));
}

#[test]
fn filters_hide_authors_across_every_surface() {
    let (services, alice, bob) = accounts();
    let carol = services.create_account("carol", None).unwrap().id;
    let dave = services.create_account("dave", None).unwrap().id;
    // bob: blocked by alice. carol: muted by alice (mute keeps the
    // follow edge, so home scope still includes her notes). dave:
    // renote-muted by alice. rina: remote, on alice's muted instance.
    services.follow(&alice, &carol, false).unwrap();
    let blocked_note = services.post_note(&bob, "blocked says hi", None).unwrap();
    let muted_note = services.post_note(&carol, "muted says hi", None).unwrap();
    let target = services.post_note(&alice, "renote target", None).unwrap();
    let mut pure_input = CreateNoteInput::simple_public(&dave, "x");
    pure_input.text = None;
    pure_input.renote_of = Some(target.id.clone());
    let pure_renote = services.create_note(pure_input).unwrap();
    // A quote (renote + text) by the same renote-muted author stays
    // visible: renote-mutes hide pure renotes only.
    let mut quote_input = CreateNoteInput::simple_public(&dave, "quoting");
    quote_input.renote_of = Some(target.id.clone());
    let quote = services.create_note(quote_input).unwrap();
    let rina = remote_account(&services, "rina", "remote.example");
    let remote_note = services.post_note(&rina, "remote hello", None).unwrap();
    services.block(&alice, &bob).unwrap();
    services.mute(&alice, &carol, None).unwrap();
    services.renote_mute(&alice, &dave).unwrap();
    services
        .update_profile(
            &alice,
            UpdateAccountPatch {
                muted_instances: Some(vec!["remote.example".to_string()]),
                ..Default::default()
            },
        )
        .unwrap();
    let suspended = with_suspended(&services, &bob);

    let cursor = TimelineCursor::with_limit(20);
    for (label, author_note) in [
        ("blocked", &blocked_note),
        ("muted", &muted_note),
        ("renote-muted pure renote", &pure_renote),
        ("muted-instance remote", &remote_note),
    ] {
        // Local + global timelines hide the note for the viewer.
        for (timeline, notes) in [
            (
                "local",
                ids(services
                    .local_timeline(Some(&alice), &cursor, false, true, true)
                    .unwrap()),
            ),
            (
                "global",
                ids(services
                    .global_timeline(Some(&alice), &cursor, false, true)
                    .unwrap()),
            ),
        ] {
            assert!(
                !notes.contains(&author_note.id),
                "{label} note hidden on {timeline}"
            );
        }
        // `users/notes` hides it too.
        let listed = ids(services
            .user_notes(
                &author_note.author_id,
                Some(&alice),
                &cursor,
                true,
                true,
                false,
            )
            .unwrap());
        assert!(
            !listed.contains(&author_note.id),
            "{label} note hidden on users/notes"
        );
        // Streaming eligibility agrees on every channel.
        for channel in [
            TimelineChannel::Local,
            TimelineChannel::Global,
            TimelineChannel::Hybrid,
        ] {
            // Hybrid needs an authenticated viewer; alice qualifies
            // regardless of follow edges for local/global-side notes.
            assert!(
                !services
                    .note_eligible_for_channel(
                        channel,
                        Some(&alice),
                        author_note,
                        &channel_params(true)
                    )
                    .unwrap(),
                "{label} note ineligible on {channel:?}"
            );
        }
        // Capture agrees as well (the note is public, so only the
        // §0A filter can reject it).
        assert!(
            !services
                .capture_gate(Some(&alice), &author_note.id)
                .unwrap(),
            "{label} note uncapturable"
        );
    }
    // Renote-mutes hide ONLY pure renotes: dave's quote stays visible
    // on every surface.
    let local = ids(services
        .local_timeline(Some(&alice), &cursor, false, true, true)
        .unwrap());
    assert!(local.contains(&quote.id), "quotes survive renote-mutes");
    // Viewer-scoped filters do not apply anonymously — except
    // suspension, which hides for everyone. (Remote notes are never
    // local-timeline eligible, so the muted-instance control reads the
    // anonymous global timeline.)
    let anon = ids(services
        .local_timeline(None, &cursor, false, true, true)
        .unwrap());
    assert!(anon.contains(&blocked_note.id), "blocks are viewer-scoped");
    assert!(anon.contains(&muted_note.id), "mutes are viewer-scoped");
    let anon_global = ids(services
        .global_timeline(None, &cursor, false, true)
        .unwrap());
    assert!(
        anon_global.contains(&remote_note.id),
        "mutedInstances are viewer-scoped"
    );
    // Suspension hides for every viewer, including anonymous ones.
    for viewer in [Some(alice.as_str()), None] {
        let notes = ids(suspended
            .local_timeline(viewer, &cursor, false, true, true)
            .unwrap());
        assert!(
            !notes.contains(&blocked_note.id),
            "suspended author hidden for {viewer:?}"
        );
        assert!(
            !suspended.capture_gate(viewer, &blocked_note.id).unwrap(),
            "suspended author uncapturable for {viewer:?}"
        );
        let listed = suspended
            .user_notes(&bob, viewer, &cursor, true, true, false)
            .unwrap();
        assert!(listed.is_empty(), "suspended author hidden on users/notes");
    }
    // The suspended author's followers note is hidden even though
    // visibility alone would pass for the author themself... (control:
    // a non-suspended viewer-facing read still filters the row).
    assert!(
        suspended
            .notes_show(&blocked_note.id, Some(&alice))
            .unwrap_err()
            == PortError::NotFound("NO_SUCH_NOTE".to_string())
    );
}

#[test]
fn word_mutes_server_enforced_hard_drops_soft_keeps() {
    // Batch N WS8 (owner-approved DIVERGENCE from upstream, which
    // enforces word mutes only in the frontend): hard mutes drop the
    // note on every read surface; soft mutes do NOT drop (they pack
    // `isSoftMuted` when the note has no CW of its own — see the
    // packer tests). Matching is Misskey-exact (transitive,
    // self-exempt, case-sensitive AND-groups, `/regex/flags` bare
    // strings, CW-first haystack).
    let (services, alice, bob) = accounts();
    services
        .update_profile(
            &alice,
            UpdateAccountPatch {
                muted_words: Some(vec![ProfileMuteWord::Single("/spoiler/i".to_string())]),
                hard_muted_words: Some(vec![
                    ProfileMuteWord::Single("/secret/i".to_string()),
                    ProfileMuteWord::And(vec!["foo".to_string(), "bar".to_string()]),
                ]),
                ..Default::default()
            },
        )
        .unwrap();
    let plain = services.post_note(&bob, "just a note", None).unwrap();
    let soft_hit = services.post_note(&bob, "a spoiler here", None).unwrap();
    let hard_regex = services.post_note(&bob, "a secret here", None).unwrap();
    let hard_and = services.post_note(&bob, "foo and bar", None).unwrap();
    let partial_and = services.post_note(&bob, "only foo", None).unwrap();
    // A CW of its own does not save a hard-mute hit (drop), but keeps
    // the soft badge off (packed `isSoftMuted` requires no own CW).
    let mut cw_input = CreateNoteInput::simple_public(&bob, "a spoiler body");
    cw_input.cw = Some("cw here".to_string());
    let cw_soft = services.create_note(cw_input).unwrap();
    // Reply-transitive: bob replies to alice's note with a secret.
    let alice_root = services.post_note(&alice, "alice root", None).unwrap();
    let transitive = services
        .post_note(&bob, "holds a SECRET", Some(&alice_root.id))
        .unwrap();
    // Self-notes never match: alice's own secret stays visible to her.
    let own = services.post_note(&alice, "my own secret", None).unwrap();

    let cursor = TimelineCursor::with_limit(30);
    let local = ids(services
        .local_timeline(Some(&alice), &cursor, false, true, true)
        .unwrap());
    assert!(local.contains(&plain.id), "plain survives");
    assert!(local.contains(&soft_hit.id), "soft never drops");
    assert!(local.contains(&cw_soft.id), "soft with CW never drops");
    assert!(!local.contains(&hard_regex.id), "hard regex drops");
    assert!(!local.contains(&hard_and.id), "hard AND-group drops");
    assert!(
        local.contains(&partial_and.id),
        "partial AND-group survives"
    );
    assert!(
        !local.contains(&transitive.id),
        "reply-transitive hard drops"
    );
    assert!(local.contains(&own.id), "self-notes exempt");

    // notes/show fails closed on hard hits (like invisible notes).
    services
        .notes_show(&hard_regex.id, Some(&alice))
        .unwrap_err();
    services.notes_show(&soft_hit.id, Some(&alice)).unwrap();
    // Streaming gates agree.
    assert!(
        !services
            .note_eligible_for_channel(
                TimelineChannel::Local,
                Some(&alice),
                &hard_regex,
                &channel_params(true)
            )
            .unwrap(),
        "hard drops gate streaming"
    );
    assert!(
        services
            .note_eligible_for_channel(
                TimelineChannel::Local,
                Some(&alice),
                &soft_hit,
                &channel_params(true)
            )
            .unwrap(),
        "soft never gates streaming"
    );
    assert!(!services.capture_gate(Some(&alice), &hard_and.id).unwrap());
}
