use crate::application::commands::AppServices;
use crate::application::ports::{Clock, IdGenerator, JobQueue, NotificationStore, StreamingSink};
use crate::federation::retry::DestinationTracker;
use crate::media::transcode::TranscodeConfig;
use crate::services::email::EmailSender;
use std::sync::Arc;

/// In-process durable-job worker (Phase 1, single node).
pub struct JobWorker {
    pub clock: Arc<dyn Clock>,
    pub ids: Arc<dyn IdGenerator>,
    pub jobs: Arc<dyn JobQueue>,
    pub notifications: Arc<dyn NotificationStore>,
    pub streaming: Arc<dyn StreamingSink>,
    pub services: Arc<AppServices>,
    pub delivery_tracker: Arc<DestinationTracker>,
    pub lease_duration_ms: i64,
    pub base_backoff_ms: i64,
    /// Transcode runtime config (toolchain path + quality/cap knobs from
    /// the `transcode` config section; see `crate::media::transcode`).
    /// The `TranscodeMedia` arm refuses stray jobs when disabled.
    pub transcode_config: TranscodeConfig,
    /// Outbound email transport (usually the configured
    /// `crate::services::email::SmtpEmailSender`). The `EmailSend` arm
    /// calls it on a blocking thread; an unconfigured transport fails
    /// sends loudly and the job quarantines.
    pub email_sender: Arc<dyn EmailSender>,
}
