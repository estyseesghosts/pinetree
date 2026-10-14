use std::collections::HashSet;
use std::sync::Arc;

use crate::activitypub::signatures::{
    generate_rsa_keypair_2048, private_key_to_pkcs8_pem, public_key_to_spki_pem,
};
use crate::application::events::{NotifyJobPayload, StreamingEvent};
use crate::application::ports::{
    AccountStore, ActorKeypairRecord, ActivityDedup, ApiTokenRecord, AuthStore, BlobStore,
    Clock, CreateAccountInput, CreateNoteInput, FollowStore, IdGenerator, InsertMediaInput,
    JobKind, JobQueue, MediaStore, NewJob, NoteStore, NotificationStore, PortError,
    ReactionStore, RemotePublicKeyRecord, StreamingSink, UpdateAccountPatch,
};
use crate::domain::account::Account;
use crate::domain::follow::Follow;
use crate::domain::media::{MediaMeta, MediaProperties};
use crate::domain::note::{Note, extract_mentions};
use crate::domain::notification::NotificationKind;
use crate::domain::policy::{ViewerContext, is_visible};
use crate::domain::reaction::{Reaction, coerce_reaction};
use crate::federation::delivery::SafeHttpClient;

/// Application services: the use-case layer over the storage ports.
///
/// Every user-scoped mutating use case calls `accounts.get_owner` on the
/// path so Phase 4 routing has an explicit hook (see `docs/phase-1-plan.md`).
#[derive(Clone)]
pub struct AppServices {
    pub clock: Arc<dyn Clock>,
    pub ids: Arc<dyn IdGenerator>,
    pub accounts: Arc<dyn AccountStore>,
    pub notes: Arc<dyn NoteStore>,
    pub follows: Arc<dyn FollowStore>,
    pub reactions: Arc<dyn ReactionStore>,
    pub jobs: Arc<dyn JobQueue>,
    pub media: Arc<dyn MediaStore>,
    pub blobs: Arc<dyn BlobStore>,
    pub notifications: Arc<dyn NotificationStore>,
    pub auth: Arc<dyn AuthStore>,
    pub streaming: Arc<dyn StreamingSink>,
    pub dedup: Arc<dyn ActivityDedup>,
    pub federation_http: SafeHttpClient,
    pub public_url: String,
    pub max_file_size: u64,
    pub preserved_usernames: Vec<String>,
    pub prohibited_words: Vec<String>,
    pub sensitive_words: Vec<String>,
}

impl AppServices {
    fn now_ms(&self) -> i64 {
        self.clock.now_ms()
    }

    fn notify_job(
        &self,
        notifiee_id: &str,
        notifier_id: &str,
        kind: NotificationKind,
        note_id: Option<String>,
        reaction: Option<String>,
        key_suffix: &str,
    ) -> Result<NewJob, PortError> {
        let payload = NotifyJobPayload {
            notifiee_id: notifiee_id.to_string(),
            notifier_id: notifier_id.to_string(),
            kind,
            note_id,
            reaction,
        };
        NewJob::new_json(
            JobKind::Notify,
            &payload,
            format!("notify:{key_suffix}:{}", self.ids.generate()),
            self.now_ms(),
        )
    }

    fn streaming_job(&self, event: &StreamingEvent, key: String) -> Result<NewJob, PortError> {
        NewJob::new_json(JobKind::StreamingPublish, event, key, self.now_ms())
    }

    pub fn create_account(
        &self,
        username: &str,
        password_hash: Option<&str>,
    ) -> Result<Account, PortError> {
        let input = CreateAccountInput {
            handle: username.to_string(),
            host: None,
            display_name: None,
            description: None,
            location: None,
            birthday: None,
            uri: None,
            inbox: None,
            shared_inbox: None,
            is_bot: false,
            is_cat: false,
            is_locked: false,
            preserved_usernames: self.preserved_usernames.clone(),
        };
        let account = self.accounts.create_account(input)?;
        if let Some(hash) = password_hash {
            self.auth.set_password_hash(&account.id, hash)?;
        }
        Ok(account)
    }

    #[allow(clippy::too_many_lines)]
    pub fn create_note(&self, input: CreateNoteInput) -> Result<Note, PortError> {
        self.accounts.get_owner(&input.author_id)?;

        // Resolve reply/renote targets so we know who to notify.
        let reply_target = match input.reply_to.as_deref() {
            None => None,
            Some(id) => match self.notes.get_note(id)? {
                None => {
                    return Err(PortError::NotFound("NO_SUCH_REPLY_TARGET".to_string()));
                }
                Some(note) => Some(note),
            },
        };
        let renote_target = match input.renote_of.as_deref() {
            None => None,
            Some(id) => match self.notes.get_note(id)? {
                None => {
                    return Err(PortError::NotFound("NO_SUCH_RENOTE_TARGET".to_string()));
                }
                Some(note) => Some(note),
            },
        };

        let mut jobs = Vec::new();
        let mut already_notified: HashSet<String> = HashSet::new();

        if let Some(target) = reply_target.as_ref() {
            if target.author_id != input.author_id {
                already_notified.insert(target.author_id.clone());
                jobs.push(self.notify_job(
                    &target.author_id,
                    &input.author_id,
                    NotificationKind::Reply,
                    Some("__PENDING_NOTE_ID__".to_string()),
                    None,
                    "reply",
                )?);
            }
        }
        if let Some(target) = renote_target.as_ref() {
            if target.author_id != input.author_id
                && !already_notified.contains(&target.author_id)
            {
                already_notified.insert(target.author_id.clone());
                jobs.push(self.notify_job(
                    &target.author_id,
                    &input.author_id,
                    NotificationKind::Renote,
                    Some("__PENDING_NOTE_ID__".to_string()),
                    None,
                    "renote",
                )?);
            }
        }

        // `@username` mentions (local only) become Mention notifications.
        if !input.no_extract_mentions {
            let mut text = input.text.clone().unwrap_or_default();
            if let Some(cw) = input.cw.as_ref() {
                text.push('\n');
                text.push_str(cw);
            }
            let mut seen: HashSet<String> = HashSet::new();
            for username in extract_mentions(&text) {
                let lower = username.to_lowercase();
                if !seen.insert(lower.clone()) {
                    continue;
                }
                if let Some(mentioned) = self.accounts.find_by_handle(&lower)? {
                    if mentioned.id != input.author_id
                        && !already_notified.contains(&mentioned.id)
                    {
                        already_notified.insert(mentioned.id.clone());
                        jobs.push(self.notify_job(
                            &mentioned.id,
                            &input.author_id,
                            NotificationKind::Mention,
                            Some("__PENDING_NOTE_ID__".to_string()),
                            None,
                            "mention",
                        )?);
                    }
                }
            }
        }

        jobs.push(self.streaming_job(
            &StreamingEvent::NoteCreated {
                note_id: "__PENDING_NOTE_ID__".to_string(),
            },
            format!("note-create:{}:{}", input.author_id, self.ids.generate()),
        )?);

        if !input.local_only {
            // MVP fan-out: the worker resolves recipient inboxes at delivery
            // time, so an empty inbox list completes as a Delivered no-op.
            // `__PENDING_NOTE_ID__` is substituted with the committed note id
            // inside the storage transaction (generic string replace in
            // `enqueue_jobs_in_txn`, which also covers structured payloads).
            jobs.push(
                crate::federation::outbox::FederateDeliverPayload::NoteCreated {
                    note_id: "__PENDING_NOTE_ID__".into(),
                    author_id: input.author_id.clone(),
                    inbox_override: None,
                }
                .into_job(
                    format!(
                        "federate:note:{}:{}",
                        input.author_id,
                        self.ids.generate()
                    ),
                    self.now_ms(),
                )?,
            );
        }

        let mut input_with_instance_words = input;
        input_with_instance_words.prohibited_words = self.prohibited_words.clone();
        input_with_instance_words.sensitive_words = self.sensitive_words.clone();
        self.notes
            .create_note(input_with_instance_words, jobs)
    }

    pub fn delete_note(&self, actor_id: &str, note_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(actor_id)?;
        let note = match self.notes.get_note(note_id)? {
            None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            Some(note) => note,
        };
        if note.author_id != actor_id {
            return Err(PortError::Validation("ACCESS_DENIED".to_string()));
        }
        let base = self.public_url.trim_end_matches('/');
        let mut jobs = vec![self.streaming_job(
            &StreamingEvent::NoteDeleted {
                note_id: note_id.to_string(),
            },
            format!("note-delete:{note_id}:{}", self.ids.generate()),
        )?];
        if !note.local_only {
            let note_uri = note
                .uri
                .clone()
                .unwrap_or_else(|| format!("{base}/notes/{}", note.id));
            // A pure renote (no text/CW/files) federates as Undo(Announce)
            // of the renote target rather than Delete(Tombstone).
            let was_announce_of_uri =
                if note.renote_id.is_some() && note.text.is_none() && note.cw.is_none() && note
                    .file_ids
                    .is_empty()
                {
                    match &note.renote_id {
                        Some(renote_id) => match self.notes.get_note(renote_id)? {
                            Some(target) => Some(target.uri.clone().unwrap_or_else(|| {
                                format!("{base}/notes/{}", target.id)
                            })),
                            None => None,
                        },
                        None => None,
                    }
                } else {
                    None
                };
            jobs.push(
                crate::federation::outbox::FederateDeliverPayload::NoteDeleted {
                    note_id: note.id.clone(),
                    author_id: actor_id.to_string(),
                    note_uri,
                    was_announce_of_uri,
                    inbox_override: None,
                }
                .into_job(
                    format!("federate:delete:{note_id}:{}", self.ids.generate()),
                    self.now_ms(),
                )?,
            );
        }
        self.notes.delete_note(note_id, jobs)
    }

    pub fn follow(
        &self,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
    ) -> Result<Follow, PortError> {
        self.accounts.get_owner(follower_id)?;
        let mut jobs = Vec::new();
        jobs.push(self.notify_job(
            followee_id,
            follower_id,
            NotificationKind::Follow,
            None,
            None,
            "follow",
        )?);
        jobs.push(self.streaming_job(
            &StreamingEvent::MeUpdated {
                account_id: followee_id.to_string(),
            },
            format!("follow:{follower_id}:{followee_id}:{}", self.ids.generate()),
        )?);
        // Federate the Follow when either side is remote. The storage follow
        // row id is generated inside the transaction, so the activity carries
        // a fresh id used for the Accept embedding (MVP: it may differ from
        // the storage row id).
        if let (Some(follower), Some(followee)) = (
            self.accounts.find_by_id(follower_id)?,
            self.accounts.find_by_id(followee_id)?,
        ) {
            let remote = if !followee.is_local() {
                Some(&followee)
            } else if !follower.is_local() {
                Some(&follower)
            } else {
                None
            };
            if let Some(remote_account) = remote {
                if let Some(inbox_url) =
                    crate::federation::outbox::preferred_inbox(remote_account)
                {
                    let follow_id = self.ids.generate();
                    jobs.push(
                        crate::federation::outbox::FederateDeliverPayload::Follow {
                            follow_id: follow_id.clone(),
                            follower_id: follower_id.to_string(),
                            followee_id: followee_id.to_string(),
                            inbox_url,
                        }
                        .into_job(
                            format!(
                                "federate:follow:{follower_id}:{followee_id}:{follow_id}"
                            ),
                            self.now_ms(),
                        )?,
                    );
                }
            }
        }
        self.follows
            .follow(follower_id, followee_id, with_replies, None, jobs)
    }

    pub fn unfollow(&self, follower_id: &str, followee_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(follower_id)?;
        let mut jobs = Vec::new();
        if let (Some(follower), Some(followee)) = (
            self.accounts.find_by_id(follower_id)?,
            self.accounts.find_by_id(followee_id)?,
        ) {
            let remote = if !followee.is_local() {
                Some(&followee)
            } else if !follower.is_local() {
                Some(&follower)
            } else {
                None
            };
            if let Some(remote_account) = remote {
                if let Some(inbox_url) =
                    crate::federation::outbox::preferred_inbox(remote_account)
                {
                    let follow_id = self
                        .follows
                        .get_follow(follower_id, followee_id)?
                        .map(|f| f.id)
                        .unwrap_or_else(|| self.ids.generate());
                    let undo_id = self.ids.generate();
                    jobs.push(
                        crate::federation::outbox::FederateDeliverPayload::Unfollow {
                            undo_id: undo_id.clone(),
                            follow_id,
                            follower_id: follower_id.to_string(),
                            followee_id: followee_id.to_string(),
                            inbox_url,
                        }
                        .into_job(
                            format!("federate:unfollow:{follower_id}:{followee_id}:{undo_id}"),
                            self.now_ms(),
                        )?,
                    );
                }
            }
        }
        self.follows.unfollow(follower_id, followee_id, jobs)
    }

    pub fn react(
        &self,
        user_id: &str,
        note_id: &str,
        reaction: &str,
        upsert: bool,
    ) -> Result<Reaction, PortError> {
        self.accounts.get_owner(user_id)?;
        let note = match self.notes.get_note(note_id)? {
            None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            Some(note) => note,
        };
        let follows_author = if user_id == note.author_id {
            true
        } else {
            self.follows.is_following(user_id, &note.author_id)?
        };
        let viewer = ViewerContext::local_user(user_id, follows_author);
        if !is_visible(&note, &viewer) {
            return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
        }
        if note.renote_id.is_some()
            && note.text.is_none()
            && note.cw.is_none()
            && note.file_ids.is_empty()
        {
            return Err(PortError::Validation(
                "CANNOT_REACT_TO_RENOTE".to_string(),
            ));
        }
        let coerced = coerce_reaction(reaction, note.reaction_acceptance, false);
        let mut jobs = Vec::new();
        jobs.push(self.streaming_job(
            &StreamingEvent::NoteReacted {
                note_id: note_id.to_string(),
                user_id: user_id.to_string(),
                reaction: coerced.clone(),
            },
            format!("react:{user_id}:{note_id}:{}", self.ids.generate()),
        )?);
        if note.author_id != user_id {
            jobs.push(self.notify_job(
                &note.author_id,
                user_id,
                NotificationKind::Reaction,
                Some(note_id.to_string()),
                Some(coerced.clone()),
                "reaction",
            )?);
        }
        // Federate the Like when the note author is remote.
        if let Some(author) = self.accounts.find_by_id(&note.author_id)? {
            if !author.is_local() {
                if let Some(inbox_url) =
                    crate::federation::outbox::preferred_inbox(&author)
                {
                    let base = self.public_url.trim_end_matches('/');
                    let reaction_id = self.ids.generate();
                    jobs.push(
                        crate::federation::outbox::FederateDeliverPayload::Like {
                            reaction_id: reaction_id.clone(),
                            actor_id: user_id.to_string(),
                            target_note_uri: note.uri.clone().unwrap_or_else(|| {
                                format!("{base}/notes/{note_id}")
                            }),
                            reaction: coerced.clone(),
                            inbox_url,
                        }
                        .into_job(
                            format!("federate:like:{user_id}:{note_id}:{reaction_id}"),
                            self.now_ms(),
                        )?,
                    );
                }
            }
        }
        self.reactions
            .react(user_id, note_id, &coerced, upsert, jobs)
    }

    pub fn unreact(&self, user_id: &str, note_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        let existing = self.reactions.get_reaction(user_id, note_id)?;
        let reaction = existing
            .as_ref()
            .map(|r| r.reaction.clone())
            .unwrap_or_default();
        let mut jobs = vec![self.streaming_job(
            &StreamingEvent::NoteUnreacted {
                note_id: note_id.to_string(),
                user_id: user_id.to_string(),
                reaction,
            },
            format!("unreact:{user_id}:{note_id}:{}", self.ids.generate()),
        )?];
        // Federate Undo(Like) when the note author is remote.
        if let Some(note) = self.notes.get_note(note_id)? {
            if let Some(author) = self.accounts.find_by_id(&note.author_id)? {
                if !author.is_local() {
                    if let Some(inbox_url) =
                        crate::federation::outbox::preferred_inbox(&author)
                    {
                        let base = self.public_url.trim_end_matches('/');
                        let reaction_id = existing
                            .as_ref()
                            .map(|r| r.id.clone())
                            .unwrap_or_else(|| self.ids.generate());
                        let undo_id = self.ids.generate();
                        // Best-effort: when no reaction row exists (storage
                        // reports NOT_REACTED) fall back to the Like-only
                        // heart for the Undo(Like) object.
                        let reaction_str = existing
                            .as_ref()
                            .map(|r| r.reaction.clone())
                            .unwrap_or_else(|| "❤".to_string());
                        jobs.push(
                            crate::federation::outbox::FederateDeliverPayload::UndoLike {
                                undo_id: undo_id.clone(),
                                reaction_id,
                                actor_id: user_id.to_string(),
                                target_note_uri: note.uri.clone().unwrap_or_else(|| {
                                    format!("{base}/notes/{note_id}")
                                }),
                                reaction: reaction_str,
                                inbox_url,
                            }
                            .into_job(
                                format!("federate:unlike:{user_id}:{note_id}:{undo_id}"),
                                self.now_ms(),
                            )?,
                        );
                    }
                }
            }
        }
        self.reactions.unreact(user_id, note_id, jobs)
    }

    pub fn update_profile(
        &self,
        account_id: &str,
        patch: UpdateAccountPatch,
    ) -> Result<Account, PortError> {
        self.accounts.get_owner(account_id)?;
        for field in [&patch.avatar_id, &patch.banner_id] {
            if let Some(Some(fid)) = field {
                match self.media.get(fid)? {
                    None => return Err(PortError::NotFound("NO_SUCH_FILE".to_string())),
                    Some(meta) => {
                        if meta.owner_id.as_deref() != Some(account_id) {
                            return Err(PortError::Validation("ACCESS_DENIED".to_string()));
                        }
                    }
                }
            }
        }
        // Fan out Update(Person) when remote followers exist. The
        // AccountStore trait cannot carry jobs, so the federated path commits
        // state+jobs atomically through the dedup record path with a fresh
        // activity id (replay is impossible; the dedup row itself is harmless).
        let mut federate_job = None;
        for f in self.follows.followers_of(account_id, None, 10_000)? {
            if let Some(follower) = self.accounts.find_by_id(&f.follower_id)? {
                if !follower.is_local() {
                    federate_job = Some(
                        crate::federation::outbox::FederateDeliverPayload::ActorUpdated {
                            update_id: self.ids.generate(),
                            account_id: account_id.to_string(),
                            inbox_override: None,
                        }
                        .into_job(
                            format!(
                                "federate:actor:{account_id}:{}",
                                self.ids.generate()
                            ),
                            self.now_ms(),
                        )?,
                    );
                    break;
                }
            }
        }
        if let Some(job) = federate_job {
            let activity_id = format!("local-update-{account_id}-{}", self.ids.generate());
            match self
                .dedup
                .record_update_account_activity(&activity_id, account_id, patch, vec![job])?
            {
                Some(account) => return Ok(account),
                // Dedup replay is impossible with a fresh activity id; refetch
                // defensively instead of failing the update.
                None => {
                    return self.accounts.find_by_id(account_id)?.ok_or_else(|| {
                        PortError::NotFound("NO_SUCH_USER".to_string())
                    });
                }
            }
        }
        self.accounts.update_account(account_id, patch)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn upload_media(
        &self,
        owner_id: &str,
        name: String,
        mime: Option<String>,
        bytes: &[u8],
        comment: Option<String>,
        is_sensitive: bool,
        force: bool,
    ) -> Result<MediaMeta, PortError> {
        self.accounts.get_owner(owner_id)?;
        if bytes.len() as u64 > self.max_file_size {
            return Err(PortError::Validation(
                "MAX_FILE_SIZE_EXCEEDED".to_string(),
            ));
        }
        let hash = md5_hex(bytes);
        if !force {
            if let Some(existing) = self.media.find_by_owner_and_hash(owner_id, &hash)? {
                return Ok(existing);
            }
        }
        let sniffed = sniff_mime(bytes, mime.as_deref());
        let media_id = self.ids.generate();
        let url = format!("{}/files/{media_id}", self.public_url.trim_end_matches('/'));
        self.blobs.put(&hash, bytes)?;
        self.media.insert_metadata(InsertMediaInput {
            id: Some(media_id),
            owner_id: Some(owner_id.to_string()),
            name,
            mime: sniffed,
            size_bytes: bytes.len() as u64,
            hash,
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive,
            is_link: false,
            url,
            thumbnail_url: None,
            comment,
            max_file_size: self.max_file_size,
        })
    }

    /// Re-export for callers that only need token bookkeeping through commands.
    pub fn store_token(&self, record: ApiTokenRecord) -> Result<(), PortError> {
        self.auth.store_token(record)
    }

    /// Return the RSA-2048 keypair for a local account, lazily generating and
    /// persisting one if none exists yet.
    pub fn ensure_actor_keypair(
        &self,
        account_id: &str,
    ) -> Result<ActorKeypairRecord, PortError> {
        if let Some(existing) = self.auth.get_actor_keypair(account_id)? {
            return Ok(existing);
        }
        self.rotate_actor_keypair(account_id)
    }

    /// Generate and store a fresh RSA-2048 keypair for `account_id` (key
    /// rotation or initial provisioning).
    pub fn rotate_actor_keypair(
        &self,
        account_id: &str,
    ) -> Result<ActorKeypairRecord, PortError> {
        let account = self
            .accounts
            .find_by_id(account_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
        if !account.is_local() {
            return Err(PortError::Validation(
                "cannot generate keypair for remote account".to_string(),
            ));
        }
        let (priv_key, pub_key) = generate_rsa_keypair_2048().map_err(PortError::Validation)?;
        let record = ActorKeypairRecord {
            account_id: account_id.to_string(),
            public_key_pem: public_key_to_spki_pem(&pub_key),
            private_key_pem: private_key_to_pkcs8_pem(&priv_key),
            created_at_ms: self.now_ms(),
        };
        self.auth.save_actor_keypair(record.clone())?;
        Ok(record)
    }

    /// Ensure the local `system_actor` service account and its RSA-2048 keypair
    /// exist (used for instance-signed ActivityPub GETs).
    pub fn ensure_system_actor(&self) -> Result<(Account, ActorKeypairRecord), PortError> {
        let account = match self.accounts.find_by_username_and_host("system_actor", None)? {
            Some(acc) => acc,
            None => {
                let mut input = CreateAccountInput::local("system_actor");
                input.is_bot = true;
                self.accounts.create_account(input)?
            }
        };
        let keypair = self.ensure_actor_keypair(&account.id)?;
        Ok((account, keypair))
    }

    /// Cache a remote actor's public key in storage.
    pub fn cache_remote_public_key(
        &self,
        key_id: &str,
        actor_uri: &str,
        account_id: Option<String>,
        public_key_pem: &str,
    ) -> Result<RemotePublicKeyRecord, PortError> {
        let record = RemotePublicKeyRecord {
            key_id: key_id.to_string(),
            actor_uri: actor_uri.to_string(),
            account_id,
            public_key_pem: public_key_pem.to_string(),
            fetched_at_ms: self.now_ms(),
        };
        self.auth.save_remote_public_key(record.clone())?;
        Ok(record)
    }

    // ------------------------------------------------------------------
    // Shared federation operations (Phase 2 Slices D1 & D3).
    //
    // These are the inbound/outbound contracts consumed by
    // `src/federation/inbox.rs` and the job worker. Inbound handlers commit
    // state+jobs atomically through the dedup record path and never enqueue
    // further `FederateDeliver` fan-out, except the auto-Accept in
    // `handle_inbound_follow` (prevents federation loops).
    // ------------------------------------------------------------------

    /// Build a `FederateDeliver` job for `payload` with a generated
    /// idempotency key under `key_prefix`.
    pub fn federate_job(
        &self,
        payload: &crate::federation::outbox::FederateDeliverPayload,
        key_prefix: &str,
    ) -> Result<NewJob, PortError> {
        payload.into_job(
            format!("{key_prefix}:{}", self.ids.generate()),
            self.now_ms(),
        )
    }

    /// Preferred remote inbox for `account` (`shared_inbox` first, then
    /// `inbox`; `None` for local accounts).
    pub fn remote_inbox_for_account(
        &self,
        account: &crate::domain::account::Account,
    ) -> Option<String> {
        crate::federation::outbox::preferred_inbox(account)
    }

    /// Record an inbound `Follow`, notifying the followee and auto-accepting
    /// when the followee is an unlocked local account and the follower is
    /// remote. A commit race that surfaces as `Conflict` (already following)
    /// is treated as success.
    pub fn handle_inbound_follow(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
    ) -> Result<Option<Follow>, PortError> {
        let mut jobs = Vec::new();
        jobs.push(self.notify_job(
            followee_id,
            follower_id,
            NotificationKind::Follow,
            None,
            None,
            "inbound-follow",
        )?);
        jobs.push(self.streaming_job(
            &StreamingEvent::MeUpdated {
                account_id: followee_id.to_string(),
            },
            format!(
                "inbound-follow:{follower_id}:{followee_id}:{}",
                self.ids.generate()
            ),
        )?);
        // Auto-Accept: the only inbound path that enqueues further federation
        // delivery.
        let base = self.public_url.trim_end_matches('/');
        if let (Some(follower), Some(followee)) = (
            self.accounts.find_by_id(follower_id)?,
            self.accounts.find_by_id(followee_id)?,
        ) {
            if followee.is_local() && !followee.is_locked && !follower.is_local() {
                if let Some(inbox_url) =
                    crate::federation::outbox::preferred_inbox(&follower)
                {
                    let follow_activity = serde_json::json!({
                        "id": activity_id,
                        "type": "Follow",
                        "actor": follower.actor_url(base),
                        "object": followee.actor_url(base),
                    });
                    let response_id = self.ids.generate();
                    jobs.push(
                        crate::federation::outbox::FederateDeliverPayload::FollowResponse {
                            response_id: response_id.clone(),
                            accept: true,
                            actor_id: followee_id.to_string(),
                            inbox_url,
                            follow_activity,
                        }
                        .into_job(
                            format!("federate:accept:{followee_id}:{response_id}"),
                            self.now_ms(),
                        )?,
                    );
                }
            }
        }
        match self.dedup.record_follow_activity(
            activity_id,
            follower_id,
            followee_id,
            with_replies,
            None,
            jobs,
        ) {
            Ok(v) => Ok(v),
            // Lost a commit race with a duplicate delivery: already following.
            Err(PortError::Conflict(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Record an inbound `Undo(Follow)`. A missing follow edge surfaces as
    /// `Validation(NOT_FOLLOWING)` and maps to `Ok(false)`.
    pub fn handle_inbound_unfollow(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
    ) -> Result<bool, PortError> {
        match self
            .dedup
            .record_unfollow_activity(activity_id, follower_id, followee_id, vec![])
        {
            Ok(v) => Ok(v),
            Err(PortError::Validation(msg)) if msg.contains("NOT_FOLLOWING") => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Record an inbound `Create(Note)` with best-effort reply/renote/mention
    /// notifications plus a streaming job. Replay returns `Ok(None)`.
    pub fn handle_inbound_create_note(
        &self,
        activity_id: &str,
        input: CreateNoteInput,
    ) -> Result<Option<Note>, PortError> {
        let mut jobs = Vec::new();
        if let Some(reply_id) = input.reply_to.as_deref() {
            if let Ok(Some(target)) = self.notes.get_note(reply_id) {
                if target.author_id != input.author_id {
                    if let Ok(job) = self.notify_job(
                        &target.author_id,
                        &input.author_id,
                        NotificationKind::Reply,
                        Some("__PENDING_NOTE_ID__".to_string()),
                        None,
                        "inbound-reply",
                    ) {
                        jobs.push(job);
                    }
                }
            }
        }
        if let Some(renote_id) = input.renote_of.as_deref() {
            if let Ok(Some(target)) = self.notes.get_note(renote_id) {
                if target.author_id != input.author_id {
                    if let Ok(job) = self.notify_job(
                        &target.author_id,
                        &input.author_id,
                        NotificationKind::Renote,
                        Some("__PENDING_NOTE_ID__".to_string()),
                        None,
                        "inbound-renote",
                    ) {
                        jobs.push(job);
                    }
                }
            }
        }
        if !input.no_extract_mentions {
            let mut text = input.text.clone().unwrap_or_default();
            if let Some(cw) = input.cw.as_ref() {
                text.push('\n');
                text.push_str(cw);
            }
            let mut seen: HashSet<String> = HashSet::new();
            for username in extract_mentions(&text) {
                let lower = username.to_lowercase();
                if !seen.insert(lower.clone()) {
                    continue;
                }
                if let Ok(Some(mentioned)) = self.accounts.find_by_handle(&lower) {
                    if mentioned.id != input.author_id {
                        if let Ok(job) = self.notify_job(
                            &mentioned.id,
                            &input.author_id,
                            NotificationKind::Mention,
                            Some("__PENDING_NOTE_ID__".to_string()),
                            None,
                            "inbound-mention",
                        ) {
                            jobs.push(job);
                        }
                    }
                }
            }
        }
        jobs.push(self.streaming_job(
            &StreamingEvent::NoteCreated {
                note_id: "__PENDING_NOTE_ID__".to_string(),
            },
            format!(
                "inbound-note-create:{}:{}",
                input.author_id,
                self.ids.generate()
            ),
        )?);
        let mut input_with_words = input;
        input_with_words.prohibited_words = self.prohibited_words.clone();
        input_with_words.sensitive_words = self.sensitive_words.clone();
        self.dedup
            .record_note_activity(activity_id, input_with_words, jobs)
    }

    /// Record an inbound `Delete(Tombstone)` / `Undo(Announce)`. A missing
    /// note maps to `Ok(false)`.
    pub fn handle_inbound_delete_note(
        &self,
        activity_id: &str,
        note_id: &str,
    ) -> Result<bool, PortError> {
        let job = self.streaming_job(
            &StreamingEvent::NoteDeleted {
                note_id: note_id.to_string(),
            },
            format!(
                "inbound-note-delete:{note_id}:{}",
                self.ids.generate()
            ),
        )?;
        match self
            .dedup
            .record_delete_note_activity(activity_id, note_id, vec![job])
        {
            Ok(v) => Ok(v),
            Err(PortError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Record an inbound `Like`. A commit race surfacing as `Conflict` maps
    /// to `Ok(None)`.
    pub fn handle_inbound_like(
        &self,
        activity_id: &str,
        user_id: &str,
        note_id: &str,
        reaction: &str,
    ) -> Result<Option<Reaction>, PortError> {
        let note = match self.notes.get_note(note_id)? {
            None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
            Some(note) => note,
        };
        let coerced = coerce_reaction(reaction, note.reaction_acceptance, false);
        let mut jobs = Vec::new();
        jobs.push(self.streaming_job(
            &StreamingEvent::NoteReacted {
                note_id: note_id.to_string(),
                user_id: user_id.to_string(),
                reaction: coerced.clone(),
            },
            format!(
                "inbound-react:{user_id}:{note_id}:{}",
                self.ids.generate()
            ),
        )?);
        if note.author_id != user_id {
            jobs.push(self.notify_job(
                &note.author_id,
                user_id,
                NotificationKind::Reaction,
                Some(note_id.to_string()),
                Some(coerced.clone()),
                "inbound-reaction",
            )?);
        }
        match self.dedup.record_react_activity(
            activity_id,
            user_id,
            note_id,
            &coerced,
            true,
            jobs,
        ) {
            Ok(v) => Ok(v),
            Err(PortError::Conflict(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Record an inbound `Undo(Like)`. A missing reaction surfaces as
    /// `Validation(NOT_REACTED)` and maps to `Ok(false)`.
    pub fn handle_inbound_unlike(
        &self,
        activity_id: &str,
        user_id: &str,
        note_id: &str,
    ) -> Result<bool, PortError> {
        let reaction = self
            .reactions
            .get_reaction(user_id, note_id)?
            .map(|r| r.reaction)
            .unwrap_or_default();
        let job = self.streaming_job(
            &StreamingEvent::NoteUnreacted {
                note_id: note_id.to_string(),
                user_id: user_id.to_string(),
                reaction,
            },
            format!(
                "inbound-unreact:{user_id}:{note_id}:{}",
                self.ids.generate()
            ),
        )?;
        match self
            .dedup
            .record_unreact_activity(activity_id, user_id, note_id, vec![job])
        {
            Ok(v) => Ok(v),
            Err(PortError::Validation(msg)) if msg.contains("NOT_REACTED") => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Record an inbound `Update(Person)` for a remote actor.
    pub fn handle_inbound_update_actor(
        &self,
        activity_id: &str,
        account_id: &str,
        patch: UpdateAccountPatch,
    ) -> Result<Option<Account>, PortError> {
        self.dedup
            .record_update_account_activity(activity_id, account_id, patch, vec![])
    }

    /// Record an inbound `Accept(Follow)`. The Accept's own id dedups; the
    /// underlying follow row insert is idempotent (`Conflict` maps to
    /// `Ok(None)`).
    pub fn handle_inbound_accept_follow(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
    ) -> Result<Option<Follow>, PortError> {
        match self.dedup.record_follow_activity(
            activity_id,
            follower_id,
            followee_id,
            with_replies,
            None,
            vec![],
        ) {
            Ok(v) => Ok(v),
            Err(PortError::Conflict(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Record an inbound `Reject(Follow)`: dedup the Reject id, then
    /// best-effort remove the follow edge. Returns `Ok(false)` only when the
    /// Reject itself was already seen.
    ///
    /// NOTE: the trailing `_with_replies` mirrors
    /// `handle_inbound_accept_follow` for call-site symmetry (the inbox
    /// dispatcher passes the Follow's flag through); it has no effect on the
    /// unfollow itself.
    pub fn handle_inbound_reject_follow(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
        _with_replies: bool,
    ) -> Result<bool, PortError> {
        if self.dedup.check_and_record(activity_id)? {
            return Ok(false);
        }
        match self.follows.unfollow(follower_id, followee_id, vec![]) {
            Ok(()) => Ok(true),
            Err(PortError::Validation(msg)) if msg.contains("NOT_FOLLOWING") => Ok(true),
            Err(e) => Err(e),
        }
    }
}

fn md5_hex(bytes: &[u8]) -> String {
    use md5::{Digest, Md5};
    let mut hasher = Md5::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Sniff the content type from magic bytes, falling back to the caller
/// provided `mime` or `application/octet-stream`.
fn sniff_mime(bytes: &[u8], fallback: Option<&str>) -> String {
    if bytes.starts_with(&[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A]) {
        return "image/png".to_string();
    }
    if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return "image/jpeg".to_string();
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return "image/gif".to_string();
    }
    if bytes.len() >= 12
        && bytes.starts_with(b"RIFF")
        && bytes.get(8..12) == Some(b"WEBP".as_slice())
    {
        return "image/webp".to_string();
    }
    if bytes.starts_with(b"%PDF") {
        return "application/pdf".to_string();
    }
    if bytes.len() >= 12 && bytes.get(4..8) == Some(b"ftyp".as_slice()) {
        return "video/mp4".to_string();
    }
    if std::str::from_utf8(bytes).is_ok() {
        return "text/plain".to_string();
    }
    fallback.unwrap_or("application/octet-stream").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activitypub::signatures::{rsa_sha256_sign, rsa_sha256_verify};
    use crate::application::ports::NoopStreamingSink;
    use crate::domain::id::{AidxGenerator, ManualClock};
    use crate::federation::delivery::SafeHttpConfig;
    use crate::media::storage::FsBlobStore;
    use crate::persistence::redb::RedbStore;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn test_services(prefix: &str) -> AppServices {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-cmd-test-{}-{prefix}-{}",
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
            reactions: store.clone(),
            jobs: store.clone(),
            media: store.clone(),
            blobs,
            notifications: store.clone(),
            auth: store.clone(),
            streaming: Arc::new(NoopStreamingSink),
            dedup: store.clone() as Arc<dyn ActivityDedup>,
            federation_http: SafeHttpClient::new(SafeHttpConfig {
                allow_private_ips: true,
                ..Default::default()
            }),
            public_url: "https://example.local".to_string(),
            max_file_size: 10 * 1024 * 1024,
            preserved_usernames: Vec::new(),
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
        }
    }

    #[test]
    fn actor_keypair_generation_rotation_and_remote_key_cache() {
        let services = test_services("keys");
        let alice = services.create_account("alice", None).unwrap();

        // Initial lazy generation creates and persists an RSA-2048 keypair.
        let kp1 = services.ensure_actor_keypair(&alice.id).unwrap();
        let kp1_again = services.ensure_actor_keypair(&alice.id).unwrap();
        assert_eq!(kp1, kp1_again, "ensure_actor_keypair is idempotent");
        assert!(kp1.public_key_pem.contains("BEGIN PUBLIC KEY"));
        assert!(kp1.private_key_pem.contains("BEGIN PRIVATE KEY"));

        let sig = rsa_sha256_sign(&kp1.private_key_pem, b"federation payload").unwrap();
        assert!(
            rsa_sha256_verify(&kp1.public_key_pem, b"federation payload", &sig).unwrap()
        );

        // Key rotation replaces the persisted keypair.
        let kp2 = services.rotate_actor_keypair(&alice.id).unwrap();
        assert_ne!(kp1.public_key_pem, kp2.public_key_pem);
        let fetched = services.auth.get_actor_keypair(&alice.id).unwrap().unwrap();
        assert_eq!(fetched.public_key_pem, kp2.public_key_pem);

        // System actor provisioning is idempotent and yields a valid keypair.
        let (sys1, sys_kp1) = services.ensure_system_actor().unwrap();
        let (sys2, sys_kp2) = services.ensure_system_actor().unwrap();
        assert_eq!(sys1.id, sys2.id);
        assert_eq!(sys_kp1, sys_kp2);
        assert!(sys1.is_bot);

        // Remote public key cache stores and retrieves by key_id.
        let cached = services
            .cache_remote_public_key(
                "https://remote.example/users/bob#main-key",
                "https://remote.example/users/bob",
                None,
                &kp1.public_key_pem,
            )
            .unwrap();
        let loaded = services
            .auth
            .get_remote_public_key("https://remote.example/users/bob#main-key")
            .unwrap()
            .unwrap();
        assert_eq!(loaded, cached);
    }
}
