use std::sync::Arc;

use crate::application::ports::{
    AbuseReportStore, AccountStore, ActivityDedup, AntennaNoteStore, AntennaStore, AuthStore, BlobStore, BlockStore, ClipStore, Clock, EmojiStore, FavoriteStore, FollowStore, HostCapsStore,
    IdGenerator, InstanceSettingsStore, InviteStore, JobQueue, MediaStore, ModerationLogStore,
    MuteStore, NoteStore, NotificationStore, PageStore, PasswordResetStore, PollStore, ReactionStore, RegistryStore, SigninStore, StreamingSink, UserListStore,
};
use crate::federation::delivery::SafeHttpClient;

pub mod accounts;
pub mod abuse_reports;
pub mod antennas;
pub mod blocks;
pub mod drafts;
pub mod emoji;

pub mod emoji_transfer;
pub mod exports;
pub mod favorites;
pub mod follows;
pub mod helpers;
pub mod inbound;
pub mod invites;
pub mod keys;
pub mod media;
pub mod moderation;
pub mod notifications;
pub mod notes;
pub mod pins;
pub mod polls;
pub mod clips;
pub mod pages;
pub mod user_lists;
#[cfg(test)]
pub(crate) mod tests;

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
    pub blocks: Arc<dyn BlockStore>,
    pub mutes: Arc<dyn MuteStore>,
    pub reactions: Arc<dyn ReactionStore>,
    pub favorites: Arc<dyn FavoriteStore>,
    pub emojis: Arc<dyn EmojiStore>,
    pub antennas: Arc<dyn AntennaStore>,
    pub antenna_notes: Arc<dyn AntennaNoteStore>,
    pub polls: Arc<dyn PollStore>,
    pub jobs: Arc<dyn JobQueue>,
    pub media: Arc<dyn MediaStore>,
    pub blobs: Arc<dyn BlobStore>,
    pub notifications: Arc<dyn NotificationStore>,
    pub auth: Arc<dyn AuthStore>,
    pub streaming: Arc<dyn StreamingSink>,
    pub dedup: Arc<dyn ActivityDedup>,
    /// Per-host software-name cache (§6 `Like` emoji/plain split).
    pub host_caps: Arc<dyn HostCapsStore>,
    /// Moderation log + atomic moderation mutations (Batch M §3.2).
    pub moderation: Arc<dyn ModerationLogStore>,
    /// Instance registration policy and blocked hosts (Batch M §3.3).
    pub instance_settings: Arc<dyn InstanceSettingsStore>,
    /// Invite codes and the atomic signup claim (Batch M WP2).
    pub invites: Arc<dyn InviteStore>,
    /// Abuse reports (Batch M WP4).
    pub abuse_reports: Arc<dyn AbuseReportStore>,
    /// User lists (Batch O WP16).
    pub lists: Arc<dyn UserListStore>,
    /// Signin attempt history (Batch O WP9; `i/signin-history`).
    pub signin: Arc<dyn SigninStore>,
    /// Password-reset request rows (Batch P; `request-reset-password`).
    pub password_resets: Arc<dyn PasswordResetStore>,
    /// Client key-value registry (Batch P; `i/registry/*`).
    pub registry: Arc<dyn RegistryStore>,
    /// Clips (federated clips + pages handoff, Phase 1).
    pub clips: Arc<dyn ClipStore>,
    /// Pages (federated clips + pages handoff, Phase 3 local).
    pub pages: Arc<dyn PageStore>,
    pub federation_http: SafeHttpClient,
    pub public_url: String,
    pub max_file_size: u64,
    pub preserved_usernames: Vec<String>,
    pub prohibited_words: Vec<String>,
    pub sensitive_words: Vec<String>,
    /// Maximum note drafts per account (§5; Misskey `noteDraftLimit`,
    /// wired from [`crate::config::Config`]).
    pub note_draft_limit: usize,
    /// Maximum actually-scheduled drafts per account (§5; Misskey
    /// `scheduledNoteLimit`).
    pub scheduled_note_limit: usize,
    /// Maximum pinned notes per account (§7; Misskey `pinLimit`).
    pub pin_limit: usize,
}
