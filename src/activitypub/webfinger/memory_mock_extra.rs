//! In-memory store double for WebFinger/NodeInfo handler tests (second
//! half): job queue, poll, dedup, media, notification, auth, list,
//! registry, clip, and page store behavior, plus the shared `test_state`
//! fixture.

use std::sync::Arc;

use crate::application::commands::AppServices;
use crate::application::commands::tests::UnavailableStores;
use crate::application::ports::{
    ActivityDedup, ApiTokenRecord, AuthStore, BlobStore, CreateNoteInput, InsertMediaInput,
    JobQueue, LegacyAuthSessionRecord, MediaStore, MiAuthSessionRecord, NewJob, NotificationStore,
    PollChoiceCount, PollStore, PortError, StreamingSink, TimelineCursor,
};
use crate::domain::account::Account;
use crate::domain::id::{AidxGenerator, ManualClock};
use crate::domain::note::{Note, Visibility};
use crate::domain::notification::{Notification, NotificationKind};
use crate::domain::poll::{Poll, PollVote};
use crate::federation::delivery::{SafeHttpClient, SafeHttpConfig};

use super::memory_mock::{Mem, unavailable};

impl JobQueue for Mem {
    fn claim_next(
        &self,
        _lease_ms: i64,
    ) -> Result<Option<crate::application::ports::Job>, PortError> {
        Ok(None)
    }
    fn complete(&self, _job_id: &str) -> Result<(), PortError> {
        Ok(())
    }
    fn fail(&self, _job_id: &str, _retry_at_ms: i64) -> Result<(), PortError> {
        Ok(())
    }
    fn complete_with_followups(
        &self,
        _job_id: &str,
        _followups: Vec<NewJob>,
    ) -> Result<(), PortError> {
        Ok(())
    }
    fn enqueue_jobs(&self, _jobs: Vec<NewJob>) -> Result<(), PortError> {
        Ok(())
    }
    fn defer(&self, _job_id: &str, _not_before_ms: i64) -> Result<(), PortError> {
        Ok(())
    }
    fn get_job(&self, _job_id: &str) -> Result<Option<crate::application::ports::Job>, PortError> {
        Ok(None)
    }
    fn pending_count(&self) -> Result<usize, PortError> {
        Ok(0)
    }
}

impl PollStore for Mem {
    fn get_poll(&self, _note_id: &str) -> Result<Option<Poll>, PortError> {
        Ok(None)
    }
    fn votes_by_user_for_note(
        &self,
        _user_id: &str,
        _note_id: &str,
    ) -> Result<Vec<PollVote>, PortError> {
        Ok(Vec::new())
    }
    fn cast_vote(&self, _vote: PollVote, _jobs: Vec<NewJob>) -> Result<PollVote, PortError> {
        unavailable("cast_vote")
    }
    fn sync_poll_votes(
        &self,
        _note_id: &str,
        _counts_by_name: &[PollChoiceCount],
        _jobs: Vec<NewJob>,
    ) -> Result<Poll, PortError> {
        unavailable("sync_poll_votes")
    }
}

impl ActivityDedup for Mem {
    fn check_and_record(&self, _activity_id: &str) -> Result<bool, PortError> {
        Ok(false)
    }
    fn is_recorded(&self, _activity_id: &str) -> Result<bool, PortError> {
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

impl MediaStore for Mem {
    fn insert_metadata(
        &self,
        _input: InsertMediaInput,
    ) -> Result<crate::domain::media::MediaMeta, PortError> {
        unavailable("insert_metadata")
    }
    fn get(&self, _id: &str) -> Result<Option<crate::domain::media::MediaMeta>, PortError> {
        Ok(None)
    }
    fn find_by_owner_and_hash(
        &self,
        _owner: &str,
        _hash: &str,
    ) -> Result<Option<crate::domain::media::MediaMeta>, PortError> {
        Ok(None)
    }
    fn find_by_source_url(
        &self,
        _source_url: &str,
    ) -> Result<Vec<crate::domain::media::MediaMeta>, PortError> {
        // Test double stores no remote-cache rows.
        Ok(Vec::new())
    }
    fn delete_metadata(&self, _id: &str) -> Result<(), PortError> {
        unavailable("delete_metadata")
    }
    fn list_by_owner(
        &self,
        _owner_id: &str,
    ) -> Result<Vec<crate::domain::media::MediaMeta>, PortError> {
        // Test double stores no media rows.
        Ok(Vec::new())
    }
    fn list_all_media(
        &self,
        _cursor: &crate::application::ports::TimelineCursor,
    ) -> Result<Vec<crate::domain::media::MediaMeta>, PortError> {
        Ok(Vec::new())
    }
    fn commit_cache_eviction(
        &self,
        _blob_key: &str,
        _size_bytes: u64,
        _class: crate::application::ports::MediaClass,
        _index_keys: &[(String, String)],
    ) -> Result<crate::application::ports::CacheEvictionOutcome, PortError> {
        Ok(crate::application::ports::CacheEvictionOutcome::AlreadyAbsent)
    }
    fn cache_entries_for_blob_key(
        &self,
        _blob_key: &str,
    ) -> Result<Vec<crate::application::ports::RemoteCacheRecord>, PortError> {
        Ok(Vec::new())
    }
}

impl BlobStore for Mem {
    fn put(&self, _hash: &str, _bytes: &[u8]) -> Result<(), PortError> {
        Ok(())
    }
    fn get(&self, _hash: &str) -> Result<Option<Vec<u8>>, PortError> {
        Ok(None)
    }
    fn delete(&self, _hash: &str) -> Result<(), PortError> {
        Ok(())
    }
}

impl NotificationStore for Mem {
    fn insert_notification(&self, _n: Notification) -> Result<Notification, PortError> {
        unavailable("insert_notification")
    }
    fn list_notifications(
        &self,
        _notifiee: &str,
        _limit: usize,
    ) -> Result<Vec<Notification>, PortError> {
        Ok(Vec::new())
    }
    fn mark_all_read(&self, _notifiee: &str) -> Result<u64, PortError> {
        Ok(0)
    }
    fn list_notifications_filtered(
        &self,
        _notifiee: &str,
        _cursor: &TimelineCursor,
        _include: &[NotificationKind],
        _exclude: &[NotificationKind],
    ) -> Result<Vec<Notification>, PortError> {
        Ok(Vec::new())
    }
}

impl AuthStore for Mem {
    fn set_password_hash(&self, _id: &str, _hash: &str) -> Result<(), PortError> {
        Ok(())
    }
    fn get_password_hash(&self, _id: &str) -> Result<Option<String>, PortError> {
        Ok(None)
    }
    fn store_token(&self, _record: ApiTokenRecord) -> Result<(), PortError> {
        Ok(())
    }
    fn get_token_by_hash(&self, _hash: &str) -> Result<Option<ApiTokenRecord>, PortError> {
        Ok(None)
    }
    fn delete_token_by_hash(&self, _token_hash: &str) -> Result<(), PortError> {
        Ok(())
    }
    fn get_token_by_id(&self, _id: &str) -> Result<Option<ApiTokenRecord>, PortError> {
        Ok(None)
    }
    fn tokens_for_account(&self, _account_id: &str) -> Result<Vec<ApiTokenRecord>, PortError> {
        Ok(Vec::new())
    }
    fn delete_account_first_party_tokens(&self, _account_id: &str) -> Result<u64, PortError> {
        Ok(0)
    }
    fn save_miauth_session(&self, _s: MiAuthSessionRecord) -> Result<(), PortError> {
        Ok(())
    }
    fn get_miauth_session(&self, _id: &str) -> Result<Option<MiAuthSessionRecord>, PortError> {
        Ok(None)
    }
    fn consume_miauth_session(&self, _id: &str) -> Result<Option<MiAuthSessionRecord>, PortError> {
        Ok(None)
    }
    fn save_legacy_session(&self, _s: LegacyAuthSessionRecord) -> Result<(), PortError> {
        Ok(())
    }
    fn get_legacy_session(
        &self,
        _token: &str,
    ) -> Result<Option<LegacyAuthSessionRecord>, PortError> {
        Ok(None)
    }
    fn take_legacy_session(
        &self,
        _token: &str,
    ) -> Result<Option<LegacyAuthSessionRecord>, PortError> {
        Ok(None)
    }
}

impl crate::application::ports::UserListStore for Mem {
    fn create_user_list(
        &self,
        list: crate::domain::user_list::UserList,
    ) -> Result<crate::domain::user_list::UserList, PortError> {
        Ok(list)
    }
    fn update_user_list(
        &self,
        list: crate::domain::user_list::UserList,
    ) -> Result<Option<crate::domain::user_list::UserList>, PortError> {
        Ok(Some(list))
    }
    fn delete_user_list(&self, _owner_id: &str, _list_id: &str) -> Result<bool, PortError> {
        Ok(false)
    }
    fn get_user_list(
        &self,
        _list_id: &str,
    ) -> Result<Option<crate::domain::user_list::UserList>, PortError> {
        Ok(None)
    }
    fn lists_by_owner(
        &self,
        _owner_id: &str,
    ) -> Result<Vec<crate::domain::user_list::UserList>, PortError> {
        Ok(Vec::new())
    }
    fn public_lists_by_owner(
        &self,
        _owner_id: &str,
    ) -> Result<Vec<crate::domain::user_list::UserList>, PortError> {
        Ok(Vec::new())
    }
    fn add_list_member(
        &self,
        member: crate::domain::user_list::UserListMembership,
        _jobs: Vec<crate::application::ports::NewJob>,
    ) -> Result<crate::domain::user_list::UserListMembership, PortError> {
        Ok(member)
    }
    fn remove_list_member(
        &self,
        _list_id: &str,
        _member_id: &str,
        _jobs: Vec<crate::application::ports::NewJob>,
    ) -> Result<bool, PortError> {
        Ok(false)
    }
    fn update_list_membership(
        &self,
        _list_id: &str,
        _member_id: &str,
        _with_replies: bool,
    ) -> Result<Option<crate::domain::user_list::UserListMembership>, PortError> {
        Ok(None)
    }
    fn get_list_membership(
        &self,
        _list_id: &str,
        _member_id: &str,
    ) -> Result<Option<crate::domain::user_list::UserListMembership>, PortError> {
        Ok(None)
    }
    fn members_of_list(
        &self,
        _list_id: &str,
    ) -> Result<Vec<crate::domain::user_list::UserListMembership>, PortError> {
        Ok(Vec::new())
    }
    fn favorite_list(&self, _owner_id: &str, _list_id: &str) -> Result<bool, PortError> {
        Ok(true)
    }
    fn unfavorite_list(&self, _owner_id: &str, _list_id: &str) -> Result<bool, PortError> {
        Ok(false)
    }
    fn is_list_favorited(&self, _owner_id: &str, _list_id: &str) -> Result<bool, PortError> {
        Ok(false)
    }
    fn list_favorite_count(&self, _list_id: &str) -> Result<u64, PortError> {
        Ok(0)
    }
}

impl crate::application::ports::SigninStore for Mem {
    fn record_signin(
        &self,
        _record: crate::application::ports::SigninRecord,
    ) -> Result<(), PortError> {
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    fn list_signins(
        &self,
        _account_id: &str,
        _limit: usize,
        _since_id: Option<&str>,
        _until_id: Option<&str>,
        _since_date: Option<i64>,
        _until_date: Option<i64>,
    ) -> Result<Vec<crate::application::ports::SigninRecord>, PortError> {
        Ok(Vec::new())
    }
}

impl crate::application::ports::PasswordResetStore for Mem {
    fn create_reset_request(
        &self,
        _request: crate::application::ports::PasswordResetRequest,
    ) -> Result<(), PortError> {
        Ok(())
    }
    fn find_reset_request(
        &self,
        _token: &str,
    ) -> Result<Option<crate::application::ports::PasswordResetRequest>, PortError> {
        Ok(None)
    }
    fn delete_reset_request(&self, _id: &str) -> Result<(), PortError> {
        Ok(())
    }
}

impl crate::application::ports::RegistryStore for Mem {
    fn registry_set(
        &self,
        _item: crate::application::ports::RegistryItem,
    ) -> Result<(), PortError> {
        Ok(())
    }
    fn registry_get(
        &self,
        _user_id: &str,
        _domain: Option<&str>,
        _scope: &[String],
        _key: &str,
    ) -> Result<Option<crate::application::ports::RegistryItem>, PortError> {
        Ok(None)
    }
    fn registry_list(
        &self,
        _user_id: &str,
        _domain: Option<&str>,
        _scope: &[String],
    ) -> Result<Vec<crate::application::ports::RegistryItem>, PortError> {
        Ok(Vec::new())
    }
    fn registry_remove(
        &self,
        _user_id: &str,
        _domain: Option<&str>,
        _scope: &[String],
        _key: &str,
    ) -> Result<(), PortError> {
        Ok(())
    }
    fn registry_scopes(
        &self,
        _user_id: &str,
    ) -> Result<Vec<(Option<String>, Vec<String>)>, PortError> {
        Ok(Vec::new())
    }
    fn consume_totp_login(
        &self,
        _account_id: &str,
        _challenge: &str,
        _matched_step: u64,
        _now_ms: i64,
    ) -> Result<(), PortError> {
        Err(PortError::Validation(
            "INVALID_TOKEN no second-factor challenge is active".to_string(),
        ))
    }
}

impl crate::application::ports::ClipStore for Mem {
    fn create_clip(
        &self,
        _clip: crate::domain::clip::Clip,
    ) -> Result<crate::domain::clip::Clip, PortError> {
        unavailable("create_clip")
    }
    fn update_clip(
        &self,
        _clip: crate::domain::clip::Clip,
        _jobs: Vec<NewJob>,
    ) -> Result<Option<crate::domain::clip::Clip>, PortError> {
        Ok(None)
    }
    fn delete_clip(
        &self,
        _owner_id: &str,
        _clip_id: &str,
        _jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        Ok(false)
    }
    fn get_clip(&self, _clip_id: &str) -> Result<Option<crate::domain::clip::Clip>, PortError> {
        Ok(None)
    }
    fn clips_by_owner(&self, _owner_id: &str) -> Result<Vec<crate::domain::clip::Clip>, PortError> {
        Ok(Vec::new())
    }
    fn add_clip_note(
        &self,
        _row: crate::domain::clip::ClipNote,
        _now_ms: i64,
        _jobs: Vec<NewJob>,
    ) -> Result<crate::domain::clip::ClipNote, PortError> {
        unavailable("add_clip_note")
    }
    fn remove_clip_note(
        &self,
        _clip_id: &str,
        _note_id: &str,
        _now_ms: i64,
        _jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        Ok(false)
    }
    fn clip_note_ids(&self, _clip_id: &str) -> Result<Vec<String>, PortError> {
        Ok(Vec::new())
    }
    fn count_clip_notes(&self, _clip_id: &str) -> Result<u64, PortError> {
        Ok(0)
    }
    fn favorite_clip(&self, _fav: crate::domain::clip::ClipFavorite) -> Result<bool, PortError> {
        Ok(false)
    }
    fn unfavorite_clip(&self, _user_id: &str, _clip_id: &str) -> Result<bool, PortError> {
        Ok(false)
    }
    fn is_clip_favorited(&self, _user_id: &str, _clip_id: &str) -> Result<bool, PortError> {
        Ok(false)
    }
    fn clip_favorite_count(&self, _clip_id: &str) -> Result<u64, PortError> {
        Ok(0)
    }
    fn clip_favorites_by_user(
        &self,
        _user_id: &str,
    ) -> Result<Vec<crate::domain::clip::ClipFavorite>, PortError> {
        Ok(Vec::new())
    }
    fn follow_clip(&self, _follow: crate::domain::clip::ClipFollow) -> Result<bool, PortError> {
        Ok(false)
    }
    fn unfollow_clip(&self, _user_id: &str, _clip_id: &str) -> Result<bool, PortError> {
        Ok(false)
    }
    fn is_following_clip(&self, _user_id: &str, _clip_id: &str) -> Result<bool, PortError> {
        Ok(false)
    }
    fn followers_of_clip(
        &self,
        _clip_id: &str,
        _limit: usize,
    ) -> Result<Vec<crate::domain::clip::ClipFollow>, PortError> {
        Ok(Vec::new())
    }
    fn followed_clips_of(
        &self,
        _user_id: &str,
    ) -> Result<Vec<crate::domain::clip::ClipFollow>, PortError> {
        Ok(Vec::new())
    }
    fn record_clip_tombstone(&self, _clip_uri: &str, _deleted_at_ms: i64) -> Result<(), PortError> {
        Ok(())
    }
    fn clip_tombstone(&self, _clip_uri: &str) -> Result<Option<i64>, PortError> {
        Ok(None)
    }
}

impl crate::application::ports::PageStore for Mem {
    fn create_page(
        &self,
        _page: crate::domain::page::Page,
        _jobs: Vec<NewJob>,
    ) -> Result<crate::domain::page::Page, PortError> {
        unavailable("create_page")
    }
    fn update_page(
        &self,
        _page: crate::domain::page::Page,
        _jobs: Vec<NewJob>,
    ) -> Result<Option<crate::domain::page::Page>, PortError> {
        Ok(None)
    }
    fn delete_page(&self, _page_id: &str, _jobs: Vec<NewJob>) -> Result<bool, PortError> {
        Ok(false)
    }
    fn get_page(&self, _page_id: &str) -> Result<Option<crate::domain::page::Page>, PortError> {
        Ok(None)
    }
    fn get_page_by_name(
        &self,
        _owner_id: &str,
        _name: &str,
    ) -> Result<Option<crate::domain::page::Page>, PortError> {
        Ok(None)
    }
    fn get_page_by_uri(&self, _uri: &str) -> Result<Option<crate::domain::page::Page>, PortError> {
        Ok(None)
    }
    fn pages_by_owner(&self, _owner_id: &str) -> Result<Vec<crate::domain::page::Page>, PortError> {
        Ok(Vec::new())
    }
    fn like_page(
        &self,
        _like: crate::domain::page::PageLike,
        _jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        Ok(false)
    }
    fn unlike_page(&self, _user_id: &str, _page_id: &str) -> Result<bool, PortError> {
        Ok(false)
    }
    fn is_page_liked(&self, _user_id: &str, _page_id: &str) -> Result<bool, PortError> {
        Ok(false)
    }
    fn featured_pages(&self) -> Result<Vec<crate::domain::page::Page>, PortError> {
        Ok(Vec::new())
    }
}

impl StreamingSink for Mem {
    fn publish(&self, _event_type: &str, _payload: &[u8]) -> Result<(), PortError> {
        Ok(())
    }
}

pub(crate) fn test_state(public_url: &str) -> (Arc<AppServices>, Arc<Mem>) {
    let clock = Arc::new(ManualClock::new(1_760_400_000_123));
    let ids = Arc::new(AidxGenerator::with_node_id(clock.clone(), "test", 0));
    let mem = Arc::new(Mem::empty());
    let services = Arc::new(AppServices {
        clock: clock.clone(),
        ids: ids.clone(),
        accounts: mem.clone(),
        notes: mem.clone(),
        follows: mem.clone(),
        blocks: mem.clone(),
        mutes: mem.clone(),
        reactions: mem.clone(),
        favorites: mem.clone(),
        emojis: mem.clone(),
        antennas: mem.clone(),
        antenna_notes: mem.clone(),
        polls: mem.clone(),
        jobs: mem.clone(),
        media: mem.clone(),
        blobs: mem.clone(),
        notifications: mem.clone(),
        auth: mem.clone(),
        streaming: mem.clone(),
        dedup: mem.clone() as Arc<dyn ActivityDedup>,
        host_caps: mem.clone(),
        moderation: mem.clone(),
        instance_settings: mem.clone(),
        invites: mem.clone(),
        abuse_reports: mem.clone(),
        lists: mem.clone(),
        signin: mem.clone(),
        password_resets: mem.clone(),
        registry: mem.clone(),
        clips: mem.clone(),
        pages: mem.clone(),
        folders: Arc::new(UnavailableStores),
        system_webhooks: Arc::new(UnavailableStores),
        user_webhooks: Arc::new(UnavailableStores),
        abuse_recipients: Arc::new(UnavailableStores),
        memos: Arc::new(UnavailableStores),
        thread_mutes: Arc::new(UnavailableStores),
        federation_http: SafeHttpClient::new(SafeHttpConfig {
            allow_private_ips: true,
            ..Default::default()
        }),
        public_url: public_url.to_string(),
        max_file_size: 10 * 1024 * 1024,
        preserved_usernames: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        note_draft_limit: crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
        scheduled_note_limit: crate::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
        pin_limit: crate::domain::pinning::DEFAULT_PIN_LIMIT,
        default_like: None,
        cache_blobs: mem.clone(),
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
    });
    (services, mem)
}

pub(crate) fn alice(id: &str) -> Account {
    Account::new_local(id.to_string(), "alice".to_string()).unwrap()
}

pub(crate) fn local_note(id: &str, author_id: &str) -> Note {
    Note {
        id: id.to_string(),
        author_id: author_id.to_string(),
        text: Some("hello".to_string()),
        visibility: Visibility::Public,
        ..Default::default()
    }
}
