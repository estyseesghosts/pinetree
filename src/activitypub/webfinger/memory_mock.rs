//! In-memory store double for WebFinger/NodeInfo handler tests (first
//! half): the `Mem` struct plus account, note, follow, block, reaction,
//! favorite, emoji, and antenna store behavior.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::application::ports::{
    AccountStore, BlockStore, CreateAccountInput, CreateNoteInput, FollowStore, HostCapsStore,
    HostSoftware, InsertMediaInput, MuteStore, NewJob, NoteStore, ReactionStore, TimelineCursor,
    UpdateAccountPatch,
};
use crate::domain::account::Account;
use crate::domain::blocking::Blocking;
use crate::domain::follow::{Follow, FollowNotify};
use crate::domain::muting::{Muting, RenoteMuting};
use crate::domain::note::Note;
use crate::domain::reaction::Reaction;

use crate::application::ports::PortError;

pub(crate) struct Mem {
    accounts: Mutex<HashMap<String, Account>>,
    notes: Mutex<HashMap<String, Note>>,
}

impl Mem {
    pub(crate) fn empty() -> Self {
        Self {
            accounts: Mutex::new(HashMap::new()),
            notes: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn insert_account(&self, account: Account) {
        self.accounts
            .lock()
            .unwrap()
            .insert(account.id.clone(), account);
    }

    pub(crate) fn insert_note(&self, note: Note) {
        self.notes.lock().unwrap().insert(note.id.clone(), note);
    }
}

pub(crate) fn unavailable<T>(what: &str) -> Result<T, PortError> {
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
    fn find_by_handle(&self, handle: &str) -> Result<Option<Account>, PortError> {
        let cleaned = handle.trim().trim_start_matches('@').to_ascii_lowercase();
        self.find_by_username_and_host(&cleaned, None)
    }
    fn find_by_username_and_host(
        &self,
        username: &str,
        host: Option<&str>,
    ) -> Result<Option<Account>, PortError> {
        let lower = username.trim().to_ascii_lowercase();
        Ok(self
            .accounts
            .lock()
            .unwrap()
            .values()
            .find(|account| account.username_lower == lower && account.host.as_deref() == host)
            .cloned())
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
        Ok(self.accounts.lock().unwrap().values().cloned().collect())
    }
    fn all_accounts(
        &self,
        after: Option<&str>,
        max_rows: usize,
    ) -> Result<(Vec<Account>, Option<String>), PortError> {
        let mut out: Vec<Account> = self.accounts.lock().unwrap().values().cloned().collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        if let Some(a) = after {
            out.retain(|x| x.id.as_str() > a);
        }
        let next = if out.len() > max_rows {
            Some(out[max_rows - 1].id.clone())
        } else {
            None
        };
        out.truncate(max_rows);
        Ok((out, next))
    }
    fn remote_shared_inboxes(
        &self,
        _after: Option<&str>,
        _max_rows: usize,
    ) -> Result<(Vec<String>, Option<String>), PortError> {
        Ok((Vec::new(), None))
    }
    fn count_accounts(&self) -> Result<u64, PortError> {
        Ok(self.accounts.lock().unwrap().len() as u64)
    }
    fn count_local_accounts(&self) -> Result<u64, PortError> {
        Ok(self
            .accounts
            .lock()
            .unwrap()
            .values()
            .filter(|account| account.is_local() && !account.is_deleted && !account.is_suspended)
            .count() as u64)
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
        _cursor: &TimelineCursor,
    ) -> Result<Vec<Note>, PortError> {
        Ok(Vec::new())
    }
    fn list_all_notes(&self, _cursor: &TimelineCursor) -> Result<Vec<Note>, PortError> {
        Ok(self.notes.lock().unwrap().values().cloned().collect())
    }
    fn count_notes(&self) -> Result<u64, PortError> {
        Ok(self.notes.lock().unwrap().len() as u64)
    }
    fn count_local_notes(&self) -> Result<u64, PortError> {
        // Test-double equivalent of the maintained production counter:
        // live notes by local authors (the `Mem` tables are tiny).
        let author_ids: Vec<String> = self
            .notes
            .lock()
            .unwrap()
            .values()
            .filter(|note| !note.is_deleted)
            .map(|note| note.author_id.clone())
            .collect();
        let accounts = self.accounts.lock().unwrap();
        Ok(author_ids
            .iter()
            .filter(|id| {
                accounts
                    .get(id.as_str())
                    .is_some_and(|account| account.is_local())
            })
            .count() as u64)
    }
    fn attach_remote_media(
        &self,
        _note_id: &str,
        _input: InsertMediaInput,
        _jobs: Vec<NewJob>,
    ) -> Result<crate::domain::media::MediaMeta, PortError> {
        unavailable("attach_remote_media")
    }
}

impl FollowStore for Mem {
    fn follow(
        &self,
        _a: &str,
        _b: &str,
        _with_replies: bool,
        _notify: Option<FollowNotify>,
        _jobs: Vec<NewJob>,
    ) -> Result<Follow, PortError> {
        unavailable("follow")
    }
    fn unfollow(&self, _a: &str, _b: &str, _jobs: Vec<NewJob>) -> Result<(), PortError> {
        unavailable("unfollow")
    }
    fn is_following(&self, _a: &str, _b: &str) -> Result<bool, PortError> {
        Ok(false)
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
        _cursor: &TimelineCursor,
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
        _cursor: &TimelineCursor,
    ) -> Result<Vec<crate::domain::moderation::ModerationLogEntry>, PortError> {
        unavailable("list_logs")
    }
}

impl crate::application::ports::InstanceSettingsStore for Mem {
    fn get(&self) -> Result<crate::domain::instance::InstanceSettings, PortError> {
        unavailable("instance_settings get")
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
    fn count_for_target(&self, _account_id: &str) -> Result<usize, PortError> {
        unavailable("count_for_target")
    }
    fn create(
        &self,
        _report: crate::domain::moderation::AbuseReport,
    ) -> Result<crate::domain::moderation::AbuseReport, PortError> {
        unavailable("create")
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
    fn list_mutings(&self, _id: &str, _cursor: &TimelineCursor) -> Result<Vec<Muting>, PortError> {
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
        _cursor: &TimelineCursor,
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
    fn get_reaction(&self, _user: &str, _note: &str) -> Result<Option<Reaction>, PortError> {
        Ok(None)
    }
    fn reactions_by_user(&self, _user: &str) -> Result<Vec<Reaction>, PortError> {
        Ok(Vec::new())
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
