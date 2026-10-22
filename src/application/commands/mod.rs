use std::sync::Arc;

use crate::application::ports::{
    AbuseReportRecipientStore, AbuseReportStore, AccountStore, ActivityDedup, AntennaNoteStore,
    AntennaStore, AuthStore, BlobStore, BlockStore, ClipStore, Clock, DriveFolderStore, EmojiStore,
    FavoriteStore, FollowStore, HostCapsStore, IdGenerator, InstanceSettingsStore, InviteStore,
    JobQueue, MediaStore, ModerationLogStore, MuteStore, NoteStore, NoteThreadMuteStore,
    NotificationStore, PageStore, PasswordResetStore, PollStore, ReactionStore, RegistryStore,
    SigninStore, StreamingSink, SystemWebhookStore, UserListStore, UserMemoStore, UserWebhookStore,
};
use crate::federation::delivery::SafeHttpClient;

pub mod abuse_reports;
pub mod accounts;
pub mod admin_ext;
pub mod announcements;
pub mod antennas;
pub mod auth_2fa;
pub mod blocks;
pub mod drafts;
pub mod emoji;

pub mod clips;
pub mod drive_bulk;
pub mod drive_folders;
pub mod emoji_transfer;
pub mod exports;
pub mod favorites;
pub mod follows;
pub mod gallery;
pub mod hashtags;
pub mod helpers;
pub mod inbound;
pub mod invites;
pub mod keys;
pub mod media;
pub mod misc_endpoints;
pub mod moderation;
pub mod notes;
pub mod notifications;
pub mod pages;
pub mod pins;
pub mod polls;
#[cfg(test)]
pub(crate) mod tests;
pub mod user_lists;
pub mod webhooks;

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
    /// Remote-cache byte pool (`blobs_cache/`); `blobs` is the durable pool
    /// (`blobs_durable/`, never evicted).
    pub cache_blobs: Arc<dyn BlobStore>,
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
    /// Drive folders (gaps WP2a; real folder model + `folderId`).
    pub folders: Arc<dyn DriveFolderStore>,
    /// System webhooks (gaps WP3; admin-owned instance events).
    pub system_webhooks: Arc<dyn SystemWebhookStore>,
    /// Per-user webhooks (gaps WP3; owner-scoped).
    pub user_webhooks: Arc<dyn UserWebhookStore>,
    /// Abuse-report notification recipients (gaps WP3; email fan-out).
    pub abuse_recipients: Arc<dyn AbuseReportRecipientStore>,
    /// Per-user memos on other users (gaps WP2a; `users/update-memo`).
    pub memos: Arc<dyn UserMemoStore>,
    /// Thread mutes (gaps WP2a; `notes/thread-muting/*`).
    pub thread_mutes: Arc<dyn NoteThreadMuteStore>,
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
    /// Instance-wide default like emoji (`default_like` config key;
    /// Sharkey/Catodon `defaultLike` parity). `None` means ❤.
    pub default_like: Option<String>,
    /// Durable per-user quota + remote-cache budgets (from Config).
    pub drive_capacity_per_user_bytes: u64,
    pub remote_cache_max_bytes: u64,
    pub remote_cache_avatar_bytes: u64,
    pub remote_cache_preview_bytes: u64,
    pub remote_cache_full_bytes: u64,
    pub remote_cache_mode: crate::config::RemoteCacheMode,
    pub remote_cache_avatar_ttl_days: u64,
    pub remote_cache_preview_ttl_days: u64,
    pub remote_cache_full_ttl_days: u64,
    pub remote_cache_warming_enabled: bool,
}
