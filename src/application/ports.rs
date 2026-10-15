use serde::{Deserialize, Serialize};

use crate::domain::account::Account;
use crate::domain::follow::{Follow, FollowNotify, FollowRequest};
pub use crate::domain::id::{Clock, DomainError, IdGenerator, PortError};
use crate::domain::media::{MediaMeta, MediaProperties};
use crate::domain::note::{Note, ReactionAcceptance, Visibility};
use crate::domain::notification::Notification;
use crate::domain::reaction::Reaction;

pub const MAX_JOB_PAYLOAD_BYTES: usize = 65_536;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateAccountInput {
    pub handle: String,
    pub host: Option<String>,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub location: Option<String>,
    pub birthday: Option<String>,
    pub uri: Option<String>,
    pub inbox: Option<String>,
    pub shared_inbox: Option<String>,
    pub is_bot: bool,
    pub is_cat: bool,
    pub is_locked: bool,
    pub preserved_usernames: Vec<String>,
}

impl CreateAccountInput {
    pub fn local(handle: impl Into<String>) -> Self {
        Self {
            handle: handle.into(),
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
            preserved_usernames: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateAccountPatch {
    pub display_name: Option<Option<String>>,
    pub description: Option<Option<String>>,
    pub location: Option<Option<String>>,
    pub birthday: Option<Option<String>>,
    pub avatar_id: Option<Option<String>>,
    pub banner_id: Option<Option<String>>,
    pub is_locked: Option<bool>,
    pub is_bot: Option<bool>,
    pub is_cat: Option<bool>,
    pub is_explorable: Option<bool>,
}

pub trait AccountStore: Send + Sync {
    fn create_account(&self, input: CreateAccountInput) -> Result<Account, PortError>;
    fn find_by_id(&self, id: &str) -> Result<Option<Account>, PortError>;
    fn find_by_handle(&self, normalized_handle: &str) -> Result<Option<Account>, PortError>;
    fn find_by_username_and_host(
        &self,
        username: &str,
        host: Option<&str>,
    ) -> Result<Option<Account>, PortError>;
    fn is_username_available(
        &self,
        username: &str,
        preserved_usernames: &[String],
    ) -> Result<bool, PortError>;
    fn update_account(&self, id: &str, patch: UpdateAccountPatch) -> Result<Account, PortError>;
    fn rename(&self, id: &str, new_handle: &str) -> Result<Account, PortError>;
    fn get_owner(&self, id: &str) -> Result<String, PortError>;
    fn set_owner(&self, id: &str, node: &str, migration_proof: &str) -> Result<(), PortError>;
    fn search_accounts(
        &self,
        query: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Account>, PortError>;
    fn find_by_uri(&self, uri: &str) -> Result<Option<Account>, PortError> {
        let _ = uri;
        Ok(None)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateNoteInput {
    pub author_id: String,
    pub text: Option<String>,
    pub cw: Option<String>,
    pub visibility: Visibility,
    pub local_only: bool,
    pub reply_to: Option<String>,
    pub renote_of: Option<String>,
    pub file_ids: Vec<String>,
    pub visible_users: Vec<String>,
    pub reaction_acceptance: Option<ReactionAcceptance>,
    pub no_extract_mentions: bool,
    pub no_extract_hashtags: bool,
    pub no_extract_emojis: bool,
    pub has_poll: bool,
    pub has_channel: bool,
    pub prohibited_words: Vec<String>,
    pub sensitive_words: Vec<String>,
    #[serde(default)]
    pub uri: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub extra_mentions: Vec<String>,
    #[serde(default)]
    pub extra_tags: Vec<String>,
}

impl CreateNoteInput {
    pub fn simple_public(author_id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            author_id: author_id.into(),
            text: Some(text.into()),
            cw: None,
            visibility: Visibility::Public,
            local_only: false,
            reply_to: None,
            renote_of: None,
            file_ids: Vec::new(),
            visible_users: Vec::new(),
            reaction_acceptance: None,
            no_extract_mentions: false,
            no_extract_hashtags: false,
            no_extract_emojis: false,
            has_poll: false,
            has_channel: false,
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
            uri: None,
            url: None,
            extra_mentions: Vec::new(),
            extra_tags: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineCursor {
    pub since_id: Option<String>,
    pub until_id: Option<String>,
    pub since_date: Option<i64>,
    pub until_date: Option<i64>,
    pub limit: usize,
}

impl TimelineCursor {
    pub fn with_limit(limit: usize) -> Self {
        Self {
            since_id: None,
            until_id: None,
            since_date: None,
            until_date: None,
            limit,
        }
    }
}

pub trait NoteStore: Send + Sync {
    fn create_note(&self, input: CreateNoteInput, jobs: Vec<NewJob>) -> Result<Note, PortError>;
    fn get_note(&self, id: &str) -> Result<Option<Note>, PortError>;
    fn get_note_including_deleted(&self, id: &str) -> Result<Option<Note>, PortError>;
    fn delete_note(&self, id: &str, jobs: Vec<NewJob>) -> Result<(), PortError>;
    fn notes_by_author(
        &self,
        author_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Note>, PortError>;
    fn list_all_notes(&self, cursor: &TimelineCursor) -> Result<Vec<Note>, PortError>;
    fn find_by_uri(&self, uri: &str) -> Result<Option<Note>, PortError> {
        let _ = uri;
        Ok(None)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobKind {
    StreamingPublish,
    Notify,
    FederateDeliver,
    TimelineMaterialize,
}

/// Maximum `fail()` retries before a job is auto-quarantined as
/// terminally failed (see `JobQueue::fail_permanent`). Mirrors the
/// `MAX_DELIVERY_ATTEMPTS` delivery budget so every job kind shares one
/// exhaustion bound.
pub const MAX_JOB_ATTEMPTS: u32 = 8;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewJob {
    pub kind: JobKind,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
    pub idempotency_key: String,
    pub not_before: i64,
}

impl NewJob {
    pub fn new_json<T: Serialize>(
        kind: JobKind,
        payload: &T,
        idempotency_key: impl Into<String>,
        not_before: i64,
    ) -> Result<Self, PortError> {
        let bytes = serde_json::to_vec(payload)
            .map_err(|e| PortError::Validation(format!("job payload serialize failed: {e}")))?;
        if bytes.len() > MAX_JOB_PAYLOAD_BYTES {
            return Err(PortError::Validation(format!(
                "job payload {} bytes exceeds {} byte cap",
                bytes.len(),
                MAX_JOB_PAYLOAD_BYTES
            )));
        }
        Ok(Self {
            kind,
            payload: bytes,
            idempotency_key: idempotency_key.into(),
            not_before,
        })
    }

    pub fn validate(&self) -> Result<(), PortError> {
        if self.idempotency_key.trim().is_empty() {
            return Err(PortError::Validation(
                "job idempotency_key is required".to_string(),
            ));
        }
        if self.payload.len() > MAX_JOB_PAYLOAD_BYTES {
            return Err(PortError::Validation(format!(
                "job payload {} bytes exceeds {} byte cap",
                self.payload.len(),
                MAX_JOB_PAYLOAD_BYTES
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub kind: JobKind,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
    pub idempotency_key: String,
    pub not_before: i64,
    pub attempts: u32,
    pub lease_expires: Option<i64>,
    /// Terminal failure flag. Quarantined jobs leave the claimable set but
    /// stay inspectable via `get_job`. Defaults to `false` so rows written
    /// before this field existed still decode.
    #[serde(default)]
    pub failed: bool,
    /// Human-readable quarantine reason (`bad-payload: …`,
    /// `permanent-failure: …`, `retry-exhausted`). `None` unless quarantined.
    #[serde(default)]
    pub fail_reason: Option<String>,
}

pub trait JobQueue: Send + Sync {
    /// Claim the next due job and hold it under a lease until
    /// `lease_duration_ms` past now. Reclaiming an expired lease (a job
    /// whose `lease_expires` is `Some(exp)` with `exp <= now`) counts as an
    /// attempt: the backend increments `attempts` (`saturating_add(1)`) and,
    /// when `attempts >= MAX_JOB_ATTEMPTS`, quarantines the job
    /// (`failed = true`, `fail_reason = "retry-exhausted"`) instead of
    /// returning it. Fresh jobs (`lease_expires == None`) never increment.
    fn claim_next(&self, lease_duration_ms: i64) -> Result<Option<Job>, PortError>;
    fn complete(&self, job_id: &str) -> Result<(), PortError>;
    fn fail(&self, job_id: &str, retry_at_ms: i64) -> Result<(), PortError>;
    /// Quarantine a job as terminally failed: it leaves the claimable set
    /// (never returned by `claim_next` again) but stays inspectable via
    /// `get_job` with `failed == true` and `fail_reason` recorded.
    /// Idempotent: repeating it (or calling it on a missing job) is a
    /// no-op success that keeps the first recorded reason.
    /// The provided default is a no-op so in-memory test doubles keep
    /// compiling; durable backends must override it.
    fn fail_permanent(&self, job_id: &str, reason: &str) -> Result<(), PortError> {
        let _ = (job_id, reason);
        Ok(())
    }
    fn get_job(&self, job_id: &str) -> Result<Option<Job>, PortError>;
    fn pending_count(&self) -> Result<usize, PortError>;
}

pub trait FollowStore: Send + Sync {
    fn follow(
        &self,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
        notify: Option<FollowNotify>,
        jobs: Vec<NewJob>,
    ) -> Result<Follow, PortError>;
    fn unfollow(
        &self,
        follower_id: &str,
        followee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError>;
    fn is_following(&self, follower_id: &str, followee_id: &str) -> Result<bool, PortError>;
    fn get_follow(
        &self,
        follower_id: &str,
        followee_id: &str,
    ) -> Result<Option<Follow>, PortError>;
    fn followers_of(
        &self,
        account_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Follow>, PortError>;
    fn following_of(
        &self,
        account_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Follow>, PortError>;

    // --- Pending follow requests (Phase 2.6 slice 4, decision D-a). A request
    // is NOT a follow edge: it never affects `is_following`, `followers_of`,
    // counters or visibility until accepted.
    /// Record a pending request. `Conflict(ALREADY_FOLLOWING)` if an edge
    /// exists, `Conflict(ALREADY_REQUESTED)` if a request exists.
    fn request_follow(
        &self,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
        notify: Option<FollowNotify>,
        jobs: Vec<NewJob>,
    ) -> Result<FollowRequest, PortError> {
        let _ = (follower_id, followee_id, with_replies, notify, jobs);
        unimplemented!("FollowStore::request_follow not implemented")
    }
    fn get_follow_request(
        &self,
        follower_id: &str,
        followee_id: &str,
    ) -> Result<Option<FollowRequest>, PortError> {
        let _ = (follower_id, followee_id);
        Ok(None)
    }
    /// Pending requests addressed to `account_id`, ordered by follower id.
    fn follow_requests_to(
        &self,
        account_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<FollowRequest>, PortError> {
        let _ = (account_id, cursor, limit);
        Ok(Vec::new())
    }
    /// Atomically delete the request, create the follow edge (counters
    /// included) and enqueue `jobs`. `NotFound(NO_FOLLOW_REQUEST)` if
    /// there is no request.
    fn accept_follow_request(
        &self,
        follower_id: &str,
        followee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<Follow, PortError> {
        let _ = (follower_id, followee_id, jobs);
        unimplemented!("FollowStore::accept_follow_request not implemented")
    }
    /// Atomically delete the request (reject or cancel) and enqueue `jobs`.
    /// `NotFound(NO_FOLLOW_REQUEST)` if there is no request.
    fn remove_follow_request(
        &self,
        follower_id: &str,
        followee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        let _ = (follower_id, followee_id, jobs);
        unimplemented!("FollowStore::remove_follow_request not implemented")
    }
}

pub trait ReactionStore: Send + Sync {
    fn react(
        &self,
        user_id: &str,
        note_id: &str,
        reaction: &str,
        upsert: bool,
        jobs: Vec<NewJob>,
    ) -> Result<Reaction, PortError>;
    fn unreact(
        &self,
        user_id: &str,
        note_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError>;
    fn get_reaction(
        &self,
        user_id: &str,
        note_id: &str,
    ) -> Result<Option<Reaction>, PortError>;
}

pub trait ActivityDedup: Send + Sync {
    fn check_and_record(&self, activity_id: &str) -> Result<bool, PortError>;
    /// Forget a recorded activity id so a later delivery with the same id is
    /// processed again. Used when `Undo(Follow)` retracts a Follow whose id
    /// the sender reuses for every re-follow of the same pair (Misskey's
    /// `/follows/{follower}/{followee}`). No silent default: backends and
    /// test doubles must implement this explicitly.
    fn forget_activity(&self, _activity_id: &str) -> Result<(), PortError> {
        unimplemented!("ActivityDedup::forget_activity not implemented")
    }
    fn record_note_activity(
        &self,
        activity_id: &str,
        input: CreateNoteInput,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Note>, PortError>;
    fn record_delete_note_activity(
        &self,
        activity_id: &str,
        note_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let _ = (activity_id, note_id, jobs);
        unimplemented!("ActivityDedup::record_delete_note_activity not implemented")
    }
    fn record_follow_activity(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
        notify: Option<FollowNotify>,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Follow>, PortError> {
        let _ = (
            activity_id,
            follower_id,
            followee_id,
            with_replies,
            notify,
            jobs,
        );
        unimplemented!("ActivityDedup::record_follow_activity not implemented")
    }
    /// Dedup + record a pending request from an inbound Follow to a locked
    /// account. `Ok(None)` when the activity id was already seen.
    fn record_follow_request_activity(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
        with_replies: bool,
        notify: Option<FollowNotify>,
        jobs: Vec<NewJob>,
    ) -> Result<Option<FollowRequest>, PortError> {
        let _ = (
            activity_id,
            follower_id,
            followee_id,
            with_replies,
            notify,
            jobs,
        );
        unimplemented!("ActivityDedup::record_follow_request_activity not implemented")
    }
    fn record_unfollow_activity(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let _ = (activity_id, follower_id, followee_id, jobs);
        unimplemented!("ActivityDedup::record_unfollow_activity not implemented")
    }
    fn record_reject_follow_activity(
        &self,
        activity_id: &str,
        follower_id: &str,
        followee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError>;
    fn record_react_activity(
        &self,
        activity_id: &str,
        user_id: &str,
        note_id: &str,
        reaction: &str,
        upsert: bool,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Reaction>, PortError> {
        let _ = (activity_id, user_id, note_id, reaction, upsert, jobs);
        unimplemented!("ActivityDedup::record_react_activity not implemented")
    }
    fn record_unreact_activity(
        &self,
        activity_id: &str,
        user_id: &str,
        note_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let _ = (activity_id, user_id, note_id, jobs);
        unimplemented!("ActivityDedup::record_unreact_activity not implemented")
    }
    fn record_update_account_activity(
        &self,
        activity_id: &str,
        account_id: &str,
        patch: UpdateAccountPatch,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Account>, PortError> {
        let _ = (activity_id, account_id, patch, jobs);
        unimplemented!("ActivityDedup::record_update_account_activity not implemented")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InsertMediaInput {
    pub id: Option<String>,
    pub owner_id: Option<String>,
    pub name: String,
    pub mime: String,
    pub size_bytes: u64,
    pub hash: String,
    pub blurhash: Option<String>,
    pub properties: MediaProperties,
    pub is_sensitive: bool,
    pub is_link: bool,
    pub url: String,
    pub thumbnail_url: Option<String>,
    pub comment: Option<String>,
    pub max_file_size: u64,
}

pub trait MediaStore: Send + Sync {
    fn insert_metadata(&self, input: InsertMediaInput) -> Result<MediaMeta, PortError>;
    fn get(&self, id: &str) -> Result<Option<MediaMeta>, PortError>;
    fn find_by_owner_and_hash(
        &self,
        owner_id: &str,
        hash: &str,
    ) -> Result<Option<MediaMeta>, PortError>;
    fn delete_metadata(&self, id: &str) -> Result<(), PortError>;
}

pub trait BlobStore: Send + Sync {
    fn put(&self, hash: &str, bytes: &[u8]) -> Result<(), PortError>;
    fn get(&self, hash: &str) -> Result<Option<Vec<u8>>, PortError>;
    fn delete(&self, hash: &str) -> Result<(), PortError>;
}

pub trait NotificationStore: Send + Sync {
    fn insert_notification(&self, notification: Notification) -> Result<Notification, PortError>;
    fn list_notifications(
        &self,
        notifiee_id: &str,
        limit: usize,
    ) -> Result<Vec<Notification>, PortError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiTokenRecord {
    pub token_hash: String,
    pub account_id: String,
    pub permissions: Vec<String>,
    pub is_first_party: bool,
    pub name: Option<String>,
    pub description: Option<String>,
    pub icon_url: Option<String>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MiAuthSessionRecord {
    pub session: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub icon_url: Option<String>,
    pub callback: Option<String>,
    pub permissions: Vec<String>,
    pub account_id: Option<String>,
    pub token: Option<String>,
    pub fetched: bool,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyAuthSessionRecord {
    pub id: String,
    pub token: String,
    pub app_secret: String,
    pub app_name: String,
    pub permissions: Vec<String>,
    pub account_id: Option<String>,
    pub access_token: Option<String>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorKeypairRecord {
    pub account_id: String,
    pub public_key_pem: String,
    pub private_key_pem: String,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemotePublicKeyRecord {
    pub key_id: String,
    pub actor_uri: String,
    pub account_id: Option<String>,
    pub public_key_pem: String,
    pub fetched_at_ms: i64,
}

pub trait AuthStore: Send + Sync {
    fn set_password_hash(&self, account_id: &str, password_hash: &str) -> Result<(), PortError>;
    fn get_password_hash(&self, account_id: &str) -> Result<Option<String>, PortError>;
    fn store_token(&self, record: ApiTokenRecord) -> Result<(), PortError>;
    fn get_token_by_hash(&self, token_hash: &str) -> Result<Option<ApiTokenRecord>, PortError>;
    fn save_miauth_session(&self, session: MiAuthSessionRecord) -> Result<(), PortError>;
    fn get_miauth_session(&self, session_id: &str) -> Result<Option<MiAuthSessionRecord>, PortError>;
    fn consume_miauth_session(
        &self,
        session_id: &str,
    ) -> Result<Option<MiAuthSessionRecord>, PortError>;
    fn save_legacy_session(&self, session: LegacyAuthSessionRecord) -> Result<(), PortError>;
    fn get_legacy_session(&self, token: &str) -> Result<Option<LegacyAuthSessionRecord>, PortError>;
    fn take_legacy_session(
        &self,
        token: &str,
    ) -> Result<Option<LegacyAuthSessionRecord>, PortError>;
    fn get_actor_keypair(
        &self,
        account_id: &str,
    ) -> Result<Option<ActorKeypairRecord>, PortError> {
        let _ = account_id;
        Ok(None)
    }
    fn save_actor_keypair(&self, record: ActorKeypairRecord) -> Result<(), PortError> {
        let _ = record;
        Ok(())
    }
    fn get_remote_public_key(
        &self,
        key_id: &str,
    ) -> Result<Option<RemotePublicKeyRecord>, PortError> {
        let _ = key_id;
        Ok(None)
    }
    fn save_remote_public_key(&self, record: RemotePublicKeyRecord) -> Result<(), PortError> {
        let _ = record;
        Ok(())
    }
}

pub trait StreamingSink: Send + Sync {
    fn publish(&self, event_type: &str, payload: &[u8]) -> Result<(), PortError>;
}

#[derive(Debug, Default, Clone)]
pub struct NoopActivityDedup;

impl ActivityDedup for NoopActivityDedup {
    fn check_and_record(&self, _activity_id: &str) -> Result<bool, PortError> {
        Ok(false)
    }

    fn record_note_activity(
        &self,
        _activity_id: &str,
        _input: CreateNoteInput,
        _jobs: Vec<NewJob>,
    ) -> Result<Option<Note>, PortError> {
        Ok(None)
    }

    fn record_reject_follow_activity(
        &self,
        _activity_id: &str,
        _follower_id: &str,
        _followee_id: &str,
        _jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        Ok(false)
    }
}

#[derive(Debug, Default, Clone)]
pub struct NoopStreamingSink;

impl StreamingSink for NoopStreamingSink {
    fn publish(&self, _event_type: &str, _payload: &[u8]) -> Result<(), PortError> {
        Ok(())
    }
}
