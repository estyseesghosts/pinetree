use std::sync::Arc;

use crate::application::ports::{
    AccountStore, ActivityDedup, AuthStore, BlobStore, Clock, FollowStore, IdGenerator, JobQueue,
    MediaStore, NoteStore, NotificationStore, ReactionStore, StreamingSink,
};
use crate::federation::delivery::SafeHttpClient;

pub mod accounts;
pub mod follows;
pub mod helpers;
pub mod inbound;
pub mod keys;
pub mod media;
pub mod notes;
#[cfg(test)]
mod tests;

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
