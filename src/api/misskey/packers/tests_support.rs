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

// In-memory stub stores: only the reads the packers touch are
// functional; everything else reports unavailability.
pub(crate) struct Mem {
    pub(crate) accounts: Mutex<HashMap<String, Account>>,
    pub(crate) notes: Mutex<HashMap<String, Note>>,
    pub(crate) media: Mutex<HashMap<String, MediaMeta>>,
    pub(crate) reactions: Mutex<HashMap<(String, String), Reaction>>,
    pub(crate) follows: Mutex<HashSet<(String, String)>>,
    pub(crate) polls: Mutex<HashMap<String, Poll>>,
    pub(crate) poll_votes: Mutex<HashMap<(String, String, usize), PollVote>>,
}

impl Mem {
    pub(crate) fn empty() -> Self {
        Self {
            accounts: Mutex::new(HashMap::new()),
            notes: Mutex::new(HashMap::new()),
            media: Mutex::new(HashMap::new()),
            reactions: Mutex::new(HashMap::new()),
            follows: Mutex::new(HashSet::new()),
            polls: Mutex::new(HashMap::new()),
            poll_votes: Mutex::new(HashMap::new()),
        }
    }
}

fn unavailable<T>(what: &str) -> Result<T, PortError> {
    Err(PortError::StorageUnavailable(format!("test stub: {what}")))
}

impl AccountStore for Mem {
    fn create_account(
        &self,
        _input: CreateAccountInput,
        _log: Option<&crate::domain::moderation::ModerationLogEntry>,
    ) -> Result<Account, PortError> {
        unavailable("create_account")
    }
    fn find_by_id(&self, id: &str) -> Result<Option<Account>, PortError> {
        Ok(self.accounts.lock().unwrap().get(id).cloned())
    }
    fn find_by_handle(&self, _handle: &str) -> Result<Option<Account>, PortError> {
        Ok(None)
    }
    fn find_by_username_and_host(
        &self,
        _username: &str,
        _host: Option<&str>,
    ) -> Result<Option<Account>, PortError> {
        Ok(None)
    }
    fn is_username_available(
        &self,
        _username: &str,
        _preserved: &[String],
    ) -> Result<bool, PortError> {
        Ok(true)
    }
    fn update_account(
        &self,
        _id: &str,
        _patch: UpdateAccountPatch,
        _jobs: Vec<NewJob>,
    ) -> Result<Account, PortError> {
        unavailable("update_account")
    }
    fn rename(&self, _id: &str, _new_handle: &str) -> Result<Account, PortError> {
        unavailable("rename")
    }
    fn get_owner(&self, _id: &str) -> Result<String, PortError> {
        Ok("test-node".to_string())
    }
    fn set_owner(&self, _id: &str, _node: &str, _proof: &str) -> Result<(), PortError> {
        Ok(())
    }
    fn search_accounts(
        &self,
        _query: &str,
        _offset: usize,
        _limit: usize,
    ) -> Result<Vec<Account>, PortError> {
        Ok(Vec::new())
    }
    fn all_accounts(
        &self,
        _after: Option<&str>,
        _max_rows: usize,
    ) -> Result<(Vec<Account>, Option<String>), PortError> {
        Ok((Vec::new(), None))
    }
    fn remote_shared_inboxes(
        &self,
        _after: Option<&str>,
        _max_rows: usize,
    ) -> Result<(Vec<String>, Option<String>), PortError> {
        Ok((Vec::new(), None))
    }
}

impl NoteStore for Mem {
    fn create_note(&self, _input: CreateNoteInput, _jobs: Vec<NewJob>) -> Result<Note, PortError> {
        unavailable("create_note")
    }
    fn get_note(&self, id: &str) -> Result<Option<Note>, PortError> {
        Ok(self.notes.lock().unwrap().get(id).cloned())
    }
    fn get_note_including_deleted(&self, id: &str) -> Result<Option<Note>, PortError> {
        self.get_note(id)
    }
    fn delete_note(&self, _id: &str, _jobs: Vec<NewJob>) -> Result<(), PortError> {
        unavailable("delete_note")
    }
    fn notes_by_author(
        &self,
        _author: &str,
        _cursor: &crate::application::ports::TimelineCursor,
    ) -> Result<Vec<Note>, PortError> {
        Ok(Vec::new())
    }
    fn list_all_notes(
        &self,
        _cursor: &crate::application::ports::TimelineCursor,
    ) -> Result<Vec<Note>, PortError> {
        Ok(Vec::new())
    }
    fn attach_remote_media(
        &self,
        _note_id: &str,
        _input: InsertMediaInput,
        _jobs: Vec<NewJob>,
    ) -> Result<MediaMeta, PortError> {
        unavailable("attach_remote_media")
    }
}

impl FollowStore for Mem {
    fn follow(
        &self,
        _a: &str,
        _b: &str,
        _with_replies: bool,
        _notify: Option<crate::domain::follow::FollowNotify>,
        _jobs: Vec<NewJob>,
    ) -> Result<Follow, PortError> {
        unavailable("follow")
    }
    fn unfollow(&self, _a: &str, _b: &str, _jobs: Vec<NewJob>) -> Result<(), PortError> {
        unavailable("unfollow")
    }
    fn is_following(&self, a: &str, b: &str) -> Result<bool, PortError> {
        Ok(self
            .follows
            .lock()
            .unwrap()
            .contains(&(a.to_string(), b.to_string())))
    }
    fn get_follow(&self, _a: &str, _b: &str) -> Result<Option<Follow>, PortError> {
        Ok(None)
    }
    fn followers_of(
        &self,
        _id: &str,
        _cursor: Option<&str>,
        _limit: usize,
    ) -> Result<Vec<Follow>, PortError> {
        Ok(Vec::new())
    }
    fn following_of(
        &self,
        _id: &str,
        _cursor: Option<&str>,
        _limit: usize,
    ) -> Result<Vec<Follow>, PortError> {
        Ok(Vec::new())
    }
}

impl BlockStore for Mem {
    fn block(&self, _blocking: Blocking, _jobs: Vec<NewJob>) -> Result<Blocking, PortError> {
        unavailable("block")
    }
    fn unblock(&self, _a: &str, _b: &str, _jobs: Vec<NewJob>) -> Result<(), PortError> {
        unavailable("unblock")
    }
    fn get_blocking(&self, _a: &str, _b: &str) -> Result<Option<Blocking>, PortError> {
        Ok(None)
    }
    fn is_blocking(&self, _a: &str, _b: &str) -> Result<bool, PortError> {
        Ok(false)
    }
    fn blocking_ids_for(&self, _id: &str) -> Result<Vec<String>, PortError> {
        Ok(Vec::new())
    }
    fn blocked_by_ids_for(&self, _id: &str) -> Result<Vec<String>, PortError> {
        Ok(Vec::new())
    }
    fn list_blockings(
        &self,
        _id: &str,
        _cursor: &crate::application::ports::TimelineCursor,
    ) -> Result<Vec<Blocking>, PortError> {
        Ok(Vec::new())
    }
}

impl HostCapsStore for Mem {
    fn get_host_software(&self, _host: &str) -> Result<Option<HostSoftware>, PortError> {
        Ok(None)
    }
    fn put_host_software(&self, _entry: HostSoftware) -> Result<(), PortError> {
        Ok(())
    }
}

impl crate::application::ports::ModerationLogStore for Mem {
    fn set_suspended(
        &self,
        _account_id: &str,
        _suspended: bool,
        _log: crate::domain::moderation::ModerationLogEntry,
        _jobs: Vec<NewJob>,
    ) -> Result<Account, PortError> {
        unavailable("set_suspended")
    }
    fn set_role(
        &self,
        _account_id: &str,
        _is_admin: bool,
        _is_moderator: bool,
        _log: crate::domain::moderation::ModerationLogEntry,
    ) -> Result<Account, PortError> {
        unavailable("set_role")
    }
    fn update_account_logged(
        &self,
        _account_id: &str,
        _patch: UpdateAccountPatch,
        _log: crate::domain::moderation::ModerationLogEntry,
        _jobs: Vec<NewJob>,
    ) -> Result<Account, PortError> {
        unavailable("update_account_logged")
    }
    fn delete_account(
        &self,
        _account_id: &str,
        _log: Option<crate::domain::moderation::ModerationLogEntry>,
        _jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        unavailable("delete_account")
    }
    fn append(
        &self,
        _entry: crate::domain::moderation::ModerationLogEntry,
    ) -> Result<(), PortError> {
        unavailable("append")
    }
    fn list_logs(
        &self,
        _filter: &crate::application::ports::ModerationLogFilter,
        _cursor: &crate::application::ports::TimelineCursor,
    ) -> Result<Vec<crate::domain::moderation::ModerationLogEntry>, PortError> {
        unavailable("list_logs")
    }
}

impl crate::application::ports::InstanceSettingsStore for Mem {
    fn get(&self) -> Result<crate::domain::instance::InstanceSettings, PortError> {
        // `pack_meta` reads the settings (WP2); answer the defaults.
        Ok(crate::domain::instance::InstanceSettings::default())
    }
    fn update(
        &self,
        _patch: crate::application::ports::InstanceSettingsPatch,
        _log: crate::domain::moderation::ModerationLogEntry,
    ) -> Result<crate::domain::instance::InstanceSettings, PortError> {
        unavailable("instance_settings update")
    }
}

impl crate::application::ports::InviteStore for Mem {
    fn create_invites(
        &self,
        _inputs: &[crate::application::ports::CreateInviteInput],
        _log: Option<&crate::domain::moderation::ModerationLogEntry>,
    ) -> Result<Vec<crate::domain::instance::InviteCode>, PortError> {
        unavailable("create_invites")
    }
    fn find_invite_by_id(
        &self,
        _id: &str,
    ) -> Result<Option<crate::domain::instance::InviteCode>, PortError> {
        unavailable("find_invite_by_id")
    }
    fn find_invite_by_code(
        &self,
        _code: &str,
    ) -> Result<Option<crate::domain::instance::InviteCode>, PortError> {
        unavailable("find_invite_by_code")
    }
    fn list_invites(
        &self,
        _filter: &crate::application::ports::InviteListFilter,
    ) -> Result<Vec<crate::domain::instance::InviteCode>, PortError> {
        unavailable("list_invites")
    }
    fn delete_invite(
        &self,
        _id: &str,
        _log: Option<&crate::domain::moderation::ModerationLogEntry>,
    ) -> Result<(), PortError> {
        unavailable("delete_invite")
    }
    fn consume_and_create_account(
        &self,
        _input: crate::application::ports::ConsumeInviteInput,
    ) -> Result<
        (
            crate::domain::account::Account,
            crate::domain::instance::InviteCode,
        ),
        PortError,
    > {
        unavailable("consume_and_create_account")
    }
}

impl crate::application::ports::AbuseReportStore for Mem {
    fn create(
        &self,
        _report: crate::domain::moderation::AbuseReport,
    ) -> Result<crate::domain::moderation::AbuseReport, PortError> {
        unavailable("create")
    }
    fn count_for_target(&self, _account_id: &str) -> Result<usize, PortError> {
        unavailable("count_for_target")
    }
    fn get(&self, _id: &str) -> Result<Option<crate::domain::moderation::AbuseReport>, PortError> {
        unavailable("get")
    }
    fn list(
        &self,
        _filter: &crate::application::ports::AbuseReportFilter,
        _cursor: &crate::application::ports::TimelineCursor,
    ) -> Result<Vec<crate::domain::moderation::AbuseReport>, PortError> {
        unavailable("list")
    }
    fn resolve(
        &self,
        _id: &str,
        _resolved_as: Option<crate::domain::moderation::AbuseReportResolution>,
        _assignee: Option<String>,
        _log: crate::domain::moderation::ModerationLogEntry,
        _jobs: Vec<crate::application::ports::NewJob>,
    ) -> Result<crate::domain::moderation::AbuseReport, PortError> {
        unavailable("resolve")
    }
    fn set_note(
        &self,
        _id: &str,
        _note: String,
        _log: Option<crate::domain::moderation::ModerationLogEntry>,
        _jobs: Vec<crate::application::ports::NewJob>,
    ) -> Result<crate::domain::moderation::AbuseReport, PortError> {
        unavailable("set_note")
    }
    fn mark_forwarded(
        &self,
        _id: &str,
        _log: crate::domain::moderation::ModerationLogEntry,
        _jobs: Vec<crate::application::ports::NewJob>,
    ) -> Result<crate::domain::moderation::AbuseReport, PortError> {
        unavailable("mark_forwarded")
    }
    fn create_flag_report(
        &self,
        _activity_id: &str,
        report: crate::domain::moderation::AbuseReport,
    ) -> Result<Option<crate::domain::moderation::AbuseReport>, PortError> {
        Ok(Some(report))
    }
}

impl MuteStore for Mem {
    fn mute(
        &self,
        _a: &str,
        _b: &str,
        _expires: Option<i64>,
        _jobs: Vec<NewJob>,
    ) -> Result<Option<Muting>, PortError> {
        unavailable("mute")
    }
    fn unmute(&self, _a: &str, _b: &str, _jobs: Vec<NewJob>) -> Result<(), PortError> {
        unavailable("unmute")
    }
    fn get_muting(&self, _a: &str, _b: &str) -> Result<Option<Muting>, PortError> {
        Ok(None)
    }
    fn is_muting(&self, _a: &str, _b: &str) -> Result<bool, PortError> {
        Ok(false)
    }
    fn muted_ids_for(&self, _id: &str) -> Result<Vec<String>, PortError> {
        Ok(Vec::new())
    }
    fn list_mutings(
        &self,
        _id: &str,
        _cursor: &crate::application::ports::TimelineCursor,
    ) -> Result<Vec<Muting>, PortError> {
        Ok(Vec::new())
    }
    fn renote_mute(
        &self,
        _a: &str,
        _b: &str,
        _jobs: Vec<NewJob>,
    ) -> Result<RenoteMuting, PortError> {
        unavailable("renote_mute")
    }
    fn renote_unmute(&self, _a: &str, _b: &str, _jobs: Vec<NewJob>) -> Result<(), PortError> {
        unavailable("renote_unmute")
    }
    fn get_renote_muting(&self, _a: &str, _b: &str) -> Result<Option<RenoteMuting>, PortError> {
        Ok(None)
    }
    fn is_renote_muting(&self, _a: &str, _b: &str) -> Result<bool, PortError> {
        Ok(false)
    }
    fn renote_muted_ids_for(&self, _id: &str) -> Result<Vec<String>, PortError> {
        Ok(Vec::new())
    }
    fn list_renote_mutings(
        &self,
        _id: &str,
        _cursor: &crate::application::ports::TimelineCursor,
    ) -> Result<Vec<RenoteMuting>, PortError> {
        Ok(Vec::new())
    }
}

impl ReactionStore for Mem {
    fn react(
        &self,
        _user: &str,
        _note: &str,
        _reaction: &str,
        _reaction_id: &str,
        _upsert: bool,
        _jobs: Vec<NewJob>,
    ) -> Result<Reaction, PortError> {
        unavailable("react")
    }
    fn unreact(&self, _user: &str, _note: &str, _jobs: Vec<NewJob>) -> Result<(), PortError> {
        unavailable("unreact")
    }
    fn get_reaction(&self, user: &str, note: &str) -> Result<Option<Reaction>, PortError> {
        Ok(self
            .reactions
            .lock()
            .unwrap()
            .get(&(user.to_string(), note.to_string()))
            .cloned())
    }
    fn reactions_by_user(&self, user: &str) -> Result<Vec<Reaction>, PortError> {
        Ok(self
            .reactions
            .lock()
            .unwrap()
            .values()
            .filter(|r| r.user_id == user)
            .cloned()
            .collect())
    }
}

impl crate::application::ports::FavoriteStore for Mem {
    fn favorite_note(
        &self,
        _favorite: crate::domain::favorite::NoteFavorite,
        _jobs: Vec<NewJob>,
    ) -> Result<crate::domain::favorite::NoteFavorite, PortError> {
        unavailable("favorite_note")
    }
    fn unfavorite_note(&self, _user_id: &str, _note_id: &str) -> Result<bool, PortError> {
        Ok(false)
    }
    fn is_favorited(&self, _user_id: &str, _note_id: &str) -> Result<bool, PortError> {
        Ok(false)
    }
    fn favorites_by_user(
        &self,
        _user_id: &str,
        _cursor: &crate::application::ports::TimelineCursor,
    ) -> Result<Vec<crate::domain::favorite::NoteFavorite>, PortError> {
        Ok(Vec::new())
    }
}

impl crate::application::ports::EmojiStore for Mem {
    fn create_emoji(
        &self,
        _emoji: crate::domain::emoji::CustomEmoji,
        _log: crate::domain::moderation::ModerationLogEntry,
        _jobs: Vec<NewJob>,
    ) -> Result<crate::domain::emoji::CustomEmoji, PortError> {
        unavailable("create_emoji")
    }
    fn update_emoji(
        &self,
        _emoji: crate::domain::emoji::CustomEmoji,
        _log: crate::domain::moderation::ModerationLogEntry,
        _jobs: Vec<NewJob>,
    ) -> Result<crate::domain::emoji::CustomEmoji, PortError> {
        unavailable("update_emoji")
    }
    fn delete_emoji(
        &self,
        _id: &str,
        _log: crate::domain::moderation::ModerationLogEntry,
        _jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        Ok(false)
    }
    fn get_emoji(&self, _id: &str) -> Result<Option<crate::domain::emoji::CustomEmoji>, PortError> {
        Ok(None)
    }
    fn get_emoji_by_name(
        &self,
        _shortcode: &str,
    ) -> Result<Option<crate::domain::emoji::CustomEmoji>, PortError> {
        Ok(None)
    }
    fn list_emojis(
        &self,
        _query: Option<&str>,
        _cursor: &crate::application::ports::TimelineCursor,
    ) -> Result<Vec<crate::domain::emoji::CustomEmoji>, PortError> {
        Ok(Vec::new())
    }
    fn all_local_emojis(&self) -> Result<Vec<crate::domain::emoji::CustomEmoji>, PortError> {
        Ok(Vec::new())
    }
    fn upsert_remote_emoji(
        &self,
        emoji: crate::domain::emoji::RemoteEmoji,
    ) -> Result<crate::domain::emoji::RemoteEmoji, PortError> {
        Ok(emoji)
    }
    fn get_remote_emoji(
        &self,
        _host: &str,
        _shortcode: &str,
    ) -> Result<Option<crate::domain::emoji::RemoteEmoji>, PortError> {
        Ok(None)
    }
    fn all_remote_emojis(&self) -> Result<Vec<crate::domain::emoji::RemoteEmoji>, PortError> {
        Ok(Vec::new())
    }
    fn list_remote_emojis(
        &self,
        _query: Option<&str>,
        _host: Option<&str>,
        _cursor: &crate::application::ports::TimelineCursor,
    ) -> Result<Vec<crate::domain::emoji::RemoteEmoji>, PortError> {
        Ok(Vec::new())
    }
    fn set_remote_emoji_media(
        &self,
        _host: &str,
        _shortcode: &str,
        _media_id: &str,
    ) -> Result<(), PortError> {
        Ok(())
    }
}

impl crate::application::ports::AntennaStore for Mem {
    fn create_antenna(
        &self,
        _antenna: crate::domain::antenna::Antenna,
        _jobs: Vec<NewJob>,
    ) -> Result<crate::domain::antenna::Antenna, PortError> {
        unavailable("create_antenna")
    }
    fn update_antenna(
        &self,
        _antenna: crate::domain::antenna::Antenna,
        _jobs: Vec<NewJob>,
    ) -> Result<crate::domain::antenna::Antenna, PortError> {
        unavailable("update_antenna")
    }
    fn delete_antenna(&self, _owner_id: &str, _id: &str) -> Result<bool, PortError> {
        Ok(false)
    }
    fn get_antenna(
        &self,
        _owner_id: &str,
        _id: &str,
    ) -> Result<Option<crate::domain::antenna::Antenna>, PortError> {
        Ok(None)
    }
    fn antennas_by_owner(
        &self,
        _owner_id: &str,
    ) -> Result<Vec<crate::domain::antenna::Antenna>, PortError> {
        Ok(Vec::new())
    }
    fn all_active_antennas(&self) -> Result<Vec<crate::domain::antenna::Antenna>, PortError> {
        Ok(Vec::new())
    }
    fn touch_antenna(&self, _owner_id: &str, _id: &str, _now_ms: i64) -> Result<(), PortError> {
        Ok(())
    }
}

impl crate::application::ports::AntennaNoteStore for Mem {
    fn append_antenna_note(&self, _antenna_id: &str, _note_id: &str) -> Result<(), PortError> {
        Ok(())
    }
    fn remove_antenna_note(&self, _antenna_id: &str, _note_id: &str) -> Result<(), PortError> {
        Ok(())
    }
    fn delete_antenna_notes(&self, _antenna_id: &str) -> Result<(), PortError> {
        Ok(())
    }
    fn antenna_notes(
        &self,
        _antenna_id: &str,
        _cursor: &crate::application::ports::TimelineCursor,
    ) -> Result<Vec<String>, PortError> {
        Ok(Vec::new())
    }
}

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
    fn get_poll(&self, note_id: &str) -> Result<Option<Poll>, PortError> {
        Ok(self.polls.lock().unwrap().get(note_id).cloned())
    }
    fn votes_by_user_for_note(
        &self,
        user_id: &str,
        note_id: &str,
    ) -> Result<Vec<PollVote>, PortError> {
        let mut out: Vec<PollVote> = self
            .poll_votes
            .lock()
            .unwrap()
            .iter()
            .filter(|((u, n, _), _)| u == user_id && n == note_id)
            .map(|(_, v)| v.clone())
            .collect();
        out.sort_by_key(|v| v.choice);
        Ok(out)
    }
    fn cast_vote(&self, vote: PollVote, _jobs: Vec<NewJob>) -> Result<PollVote, PortError> {
        let note = self
            .notes
            .lock()
            .unwrap()
            .get(&vote.note_id)
            .cloned()
            .ok_or_else(|| PortError::NotFound("NO_SUCH_NOTE".to_string()))?;
        if note.is_deleted {
            return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
        }
        let mut polls = self.polls.lock().unwrap();
        let poll = polls
            .get_mut(&vote.note_id)
            .ok_or_else(|| PortError::Validation("NO_POLL".to_string()))?;
        if vote.choice >= poll.choices.len() {
            return Err(PortError::Validation("INVALID_CHOICE".to_string()));
        }
        let mut votes = self.poll_votes.lock().unwrap();
        if poll.multiple {
            if votes.contains_key(&(vote.user_id.clone(), vote.note_id.clone(), vote.choice)) {
                return Err(PortError::Conflict("ALREADY_VOTED".to_string()));
            }
        } else if votes
            .keys()
            .any(|(u, n, _)| u == &vote.user_id && n == &vote.note_id)
        {
            return Err(PortError::Conflict("ALREADY_VOTED".to_string()));
        }
        if let Some(slot) = poll.votes.get_mut(vote.choice) {
            *slot = slot.saturating_add(1);
        }
        votes.insert(
            (vote.user_id.clone(), vote.note_id.clone(), vote.choice),
            vote.clone(),
        );
        Ok(vote)
    }
    fn sync_poll_votes(
        &self,
        note_id: &str,
        counts_by_name: &[PollChoiceCount],
        _jobs: Vec<NewJob>,
    ) -> Result<Poll, PortError> {
        let mut polls = self.polls.lock().unwrap();
        let poll = polls
            .get_mut(note_id)
            .ok_or_else(|| PortError::NotFound("NO_POLL".to_string()))?;
        for (i, choice) in poll.choices.iter().enumerate() {
            if let Some(count) = counts_by_name.iter().find(|c| &c.name == choice) {
                if let Some(slot) = poll.votes.get_mut(i) {
                    *slot = count.votes;
                }
            }
        }
        Ok(poll.clone())
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
    fn insert_metadata(&self, _input: InsertMediaInput) -> Result<MediaMeta, PortError> {
        unavailable("insert_metadata")
    }
    fn get(&self, id: &str) -> Result<Option<MediaMeta>, PortError> {
        Ok(self.media.lock().unwrap().get(id).cloned())
    }
    fn find_by_owner_and_hash(
        &self,
        _owner: &str,
        _hash: &str,
    ) -> Result<Option<MediaMeta>, PortError> {
        Ok(None)
    }
    fn find_by_source_url(&self, source_url: &str) -> Result<Vec<MediaMeta>, PortError> {
        let target = source_url.trim();
        if target.is_empty() {
            return Ok(Vec::new());
        }
        Ok(self
            .media
            .lock()
            .unwrap()
            .values()
            .filter(|m| m.source_url.as_deref().map(str::trim) == Some(target))
            .cloned()
            .collect())
    }
    fn delete_metadata(&self, _id: &str) -> Result<(), PortError> {
        unavailable("delete_metadata")
    }
    fn list_by_owner(&self, owner_id: &str) -> Result<Vec<MediaMeta>, PortError> {
        let mut out: Vec<MediaMeta> = self
            .media
            .lock()
            .unwrap()
            .values()
            .filter(|m| m.owner_id.as_deref() == Some(owner_id))
            .cloned()
            .collect();
        out.sort_by(|a, b| b.id.cmp(&a.id));
        Ok(out)
    }
    fn list_all_media(
        &self,
        _cursor: &crate::application::ports::TimelineCursor,
    ) -> Result<Vec<MediaMeta>, PortError> {
        let mut out: Vec<MediaMeta> = self.media.lock().unwrap().values().cloned().collect();
        out.sort_by(|a, b| b.id.cmp(&a.id));
        Ok(out)
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
        _cursor: &crate::application::ports::TimelineCursor,
        _include: &[crate::domain::notification::NotificationKind],
        _exclude: &[crate::domain::notification::NotificationKind],
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
