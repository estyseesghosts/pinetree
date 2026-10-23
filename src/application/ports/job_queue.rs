use super::media_store::RemoteMediaAttachment;
use crate::domain::id::PortError;
use crate::domain::media::MediaClass;
use serde::{Deserialize, Serialize};

pub const MAX_JOB_PAYLOAD_BYTES: usize = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobKind {
    StreamingPublish,
    Notify,
    FederateDeliver,
    TimelineMaterialize,
    /// Slice 7 (H2+C3, decision D-d): fetch-and-cache remote note
    /// attachments off the inbox request. The payload is
    /// [`RemoteMediaFetchPayload`]; `note_id` may be
    /// `"__PENDING_NOTE_ID__"` at enqueue time and is substituted with the
    /// committed note id inside the storage transaction (same mechanism as
    /// the federated `NoteCreated` payload).
    RemoteMediaFetch,
    /// Quote-post authorization: re-verify an offered `QuoteAuthorization`
    /// stamp off the inbox request. The payload is
    /// [`QuoteStampReverifyPayload`]. Transient fetch failures retry with
    /// the worker's standard backoff (quarantined after
    /// [`MAX_JOB_ATTEMPTS`]); invalid stamp content completes without
    /// recording (a later re-delivered `Accept` can still be processed).
    QuoteStampReverify,
    /// Branched replies (`docs/replies.md` Slice B): resolve an unresolved
    /// reply parent URI off the inbox request. The payload is
    /// [`ReplyParentResolvePayload`]. The handler fetches/caches the parent
    /// through the SSRF-protected remote-note path, then links waiting
    /// children in bounded batches via the idempotent
    /// `NoteStore::link_pending_reply`. Fetch failures retry with the
    /// worker's standard backoff; after the budget is exhausted the job
    /// quarantines but the pending index entry is kept, so a later parent
    /// arrival (which enqueues a fresh job for the same URI) can still link.
    ReplyParentResolve,
    /// Poll expiry (`docs/misskey-parity-plan.md` §4; Misskey
    /// `endedPollNotificationQueue` parity, sans notifications): fires once
    /// at `expires_at` and publishes the `pollEnded` streaming event. The
    /// payload is [`PollExpiryPayload`]. Idempotent: a missing note/poll or
    /// a not-yet-expired poll completes quietly (a re-armed job covers
    /// clock skew). Reuses the worker's standard lease/backoff.
    PollExpiry,
    /// Scheduled-draft post (`docs/misskey-parity-plan.md` §5; Misskey
    /// `postScheduledNoteQueue` parity): fires once at the draft's
    /// `scheduled_at` and posts the draft as a note. The payload is
    /// [`ScheduledDraftPayload`]. Idempotent: a missing draft, an
    /// unscheduled draft, or a time mismatch (the draft was updated and
    /// re-armed) completes quietly. Success deletes the draft and emits
    /// `ScheduledNotePosted`; post failure keeps the draft and emits
    /// `ScheduledNotePostFailed`. Reuses the worker's standard
    /// lease/backoff.
    PostScheduledDraft,
    /// Profile export (`docs/misskey-parity-plan.md` §13; Misskey `db`
    /// queue `exportFollowing`/`exportBlocking`/`exportMuting`/
    /// `exportNotes`/`exportUserLists`/`exportAntennas`/`exportClips`/
    /// `exportFavorites` parity): builds the entity file off-request,
    /// stores it in the caller's drive, and reports completion with an
    /// `exportCompleted` notification. The payload is [`ExportJobPayload`]
    /// (jobs carry ids only — never file content, per the 64KiB cap).
    /// Idempotent: the export bytes are deterministic for the same state,
    /// the drive upload dedups on content (`force == false`), and the
    /// completion `Notify` job carries a deterministic key, so a replay
    /// overwrites the same notification row instead of duplicating it.
    /// A gone user converges quietly. Reuses the worker's standard
    /// lease/backoff.
    Export,
    /// Profile import (`docs/misskey-parity-plan.md` §13; Misskey `db`
    /// queue `importFollowing`/`importBlocking`/`importMuting`/
    /// `importUserLists`/`importAntennas` parity): reads the caller's drive
    /// file off-request and applies its rows. The payload is
    /// [`ImportJobPayload`] (ids only). Idempotent: every row resolves
    /// through the same application ops as the interactive endpoints, so a
    /// replay converges on `ALREADY_*` conflicts (skipped, never failed);
    /// only storage faults retry the job. A gone user or file converges
    /// quietly. Bounded work per run (one transaction per row, at most the
    /// 64KiB file the endpoint guards admit). Reuses the worker's standard
    /// lease/backoff.
    Import,
    /// Silent unfollow sweep (Batch M WP3; upstream
    /// `UserSuspendService.unFollowAll`, which enqueues one `unfollow`
    /// relationship job per outgoing follow of a suspended account). The
    /// payload is [`UnfollowAllPayload`]. One sweep job is enqueued in the
    /// suspend transaction and the worker expands it at run time: each run
    /// removes one bounded batch of the account's outgoing follow rows
    /// (each row deletion commits atomically with its own
    /// [`JobKind::FederateDeliver`] `Undo(Follow)` job), and returns a
    /// continuation job while more rows remain. Idempotent and crash-safe:
    /// a re-run sees the already-removed rows gone, the cursor advances
    /// over skipped rows, and each row's delete + delivery job is one
    /// transaction. Upstream `silent: true` parity: no `unfollow`
    /// streaming event, no notification. Reuses the worker's standard
    /// lease/backoff.
    UnfollowAll,
    /// Remote custom-emoji lazy fetch (Batch N WP8b): download one known
    /// remote-emoji image off-request after a user opts in via
    /// `showRemoteEmojis`. The payload is [`RemoteEmojiFetchPayload`].
    /// Idempotent: an already-cached row or a gone row converges quietly;
    /// fetch failures (non-image content type, oversize, network) converge
    /// quietly too — the packed URL keeps pointing at the remote host (the
    /// documented privacy trade-off). Only storage faults retry.
    RemoteEmojiFetch,
    /// Custom-emoji export (`export-custom-emojis`; Misskey `db` queue
    /// `exportCustomEmojis` parity): builds the instance zip (meta.json +
    /// per-emoji files) off-request, stores it in the caller's drive, and
    /// reports completion with an `exportCompleted` notification
    /// (`exportedEntity: "customEmoji"` verbatim). The payload is
    /// [`CustomEmojiTransferPayload`] (ids only — never file content).
    /// Idempotent: the drive upload dedups on content and the completion
    /// `Notify` job carries a deterministic key. A gone user converges
    /// quietly. Reuses the worker's standard lease/backoff.
    ExportCustomEmojis,
    /// Custom-emoji zip import (`admin/emoji/import-zip`; Misskey `db` queue
    /// `importCustomEmojis` parity): reads the caller's drive file
    /// off-request, validates the archive (zip-bomb, entry-count, size,
    /// traversal, and name-pattern guards), and applies its rows through the
    /// same `create_emoji` op as the interactive endpoint (name collisions
    /// replace, upstream delete-then-add parity). The payload is
    /// [`CustomEmojiTransferPayload`]. A gone user or file converges
    /// quietly; only storage faults retry the job.
    ImportCustomEmojis,
    /// Antenna fan-in (Batch N WS4; guide WP15): one bounded job per created
    /// note carrying the note id. The worker loads the active antennas,
    /// matches, and appends `antenna_notes` rows (trimmed to the newest 200
    /// per antenna in the same transaction). Idempotent: a gone note
    /// converges quietly; replays re-match harmlessly (rows are keyed
    /// `(antenna, note)`). Only storage faults retry.
    AntennaMatch,
    /// Storage overhaul (`docs/database.md`): evict cache bytes under
    /// pressure. The payload is [`CacheEvictPayload`]. Idempotent: the
    /// handler re-reads accounting and evicts LRU+TTL-first until the need
    /// fits or candidates run out; bytes only, metadata stays.
    CacheEvict,
    /// Storage overhaul: bounded idempotent storage reconciliation sweep.
    /// The payload is [`ReconcileStoragePayload`]. Converges quietly on
    /// re-run (orphan bytes removed, accounting adjusted, legacy rows
    /// flagged, sidecars healed).
    ReconcileStorage,
    /// Storage overhaul: recompute `DURABLE_BYTES_BY_OWNER` from a bounded
    /// metadata scan after the quota upgrade. The payload is
    /// [`BackfillDurablePayload`]. Idempotent: setters overwrite with the
    /// recomputed sums; a full batch returns a continuation cursor.
    BackfillDurableAccounting,
    /// Post-commit media transcode (gaps WP1): finish processing staged
    /// upload bytes off-request (ffmpeg stills/video, legacy pure-Rust
    /// dims otherwise) and re-commit the row under the same id. The payload
    /// is `crate::jobs::transcode::TranscodeJobPayload` (ids only, never
    /// file content, per the 64KiB cap). Idempotent: a missing row, or a
    /// row already in target shape with committed dims, converges quietly
    /// (`Noop`); the idempotency key is `transcode-media:{file_id}` so
    /// re-enqueues never stack duplicate ffmpeg runs. Reuses the worker's
    /// standard lease/backoff.
    TranscodeMedia,
    /// Outbound webhook delivery (gaps WP3): cooldown-aware signed POST of
    /// one subscriber event. The payload is
    /// `crate::application::commands::webhooks::WebhookDeliverPayload`
    /// (URL + secret ride along so the worker needs no store read; well
    /// under the 64KiB cap). Idempotent: the idempotency key is the
    /// per-(webhook, event, dedup) key minted by the `plan_*` helpers, so
    /// replays dedupe. Transient failures retry with the worker's standard
    /// backoff; SSRF-blocked URLs quarantine permanently, never retry.
    WebhookDeliver,
    /// Outbound email delivery (gaps WP3): plain-text submission through
    /// the configured SMTP relay. The payload is
    /// `crate::services::email::EmailSendPayload` (recipients + rendered
    /// text only, at most one recipient set per job). Idempotent: the
    /// idempotency key is the caller-supplied `dedup_key` scope (e.g.
    /// `email:abuse-report:{report}:{email}`), so a replay re-sends the
    /// same bytes under the same key. Reuses the worker's standard
    /// lease/backoff.
    EmailSend,
}

/// Maximum `fail()` retries before a job is auto-quarantined as
/// terminally failed (see `JobQueue::fail_permanent`). Every job kind,
/// including per-inbox federation delivery, shares this one exhaustion bound.
pub const MAX_JOB_ATTEMPTS: u32 = 8;

/// Maximum number of jobs one `JobQueue::enqueue_jobs` call (or the follow-up
/// batch of `complete_with_followups`) may insert in a single write
/// transaction. Keeps fan-out writes well under the FoundationDB ~10 MB
/// transaction cap; larger fan-outs are split across calls by the caller.
pub const MAX_JOBS_PER_TXN: usize = 500;

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
    /// Settle `job_id` as done AND insert `followups` in ONE write
    /// transaction: either both happen or neither does. Followups are
    /// validated before any write; at most [`MAX_JOBS_PER_TXN`] per call.
    fn complete_with_followups(
        &self,
        job_id: &str,
        followups: Vec<NewJob>,
    ) -> Result<(), PortError>;
    /// Insert `jobs` in ONE write transaction (all or nothing), at most
    /// [`MAX_JOBS_PER_TXN`] per call. Idempotent on idempotency key: jobs
    /// whose key already has a live (non-quarantined) row are skipped, so a
    /// re-run of the producing job (crash + lease-expiry reclaim) converges
    /// instead of duplicating; within-batch repeats keep the first.
    /// Quarantined rows do not suppress a same-key re-enqueue (a failed row
    /// is never reclaimed, so suppressing would lose the retry). Receivers
    /// of re-enqueued federation deliveries still dedupe by activity id.
    fn enqueue_jobs(&self, jobs: Vec<NewJob>) -> Result<(), PortError>;
    /// Release the lease on a claimed `job_id` and reschedule it at
    /// `not_before_ms` WITHOUT counting an attempt (used when a destination is
    /// in cooldown, which is not a failed delivery). Missing or quarantined
    /// jobs are a no-op success.
    fn defer(&self, job_id: &str, not_before_ms: i64) -> Result<(), PortError>;
    fn get_job(&self, job_id: &str) -> Result<Option<Job>, PortError>;
    fn pending_count(&self) -> Result<usize, PortError>;
    /// Remove every pending `PostScheduledDraft` job armed for `draft_id`
    /// (§5 `clearSchedule` parity: Misskey scans its delayed/waiting/active
    /// queues linearly — `TODO: 線形探索なのをどうにかする`). Called when a
    /// draft is updated or deleted so a stale arming can never post a
    /// superseded draft; the update path re-arms when the merged draft is
    /// still scheduled. Returns the number of jobs removed. The default is
    /// a no-op success so in-memory test doubles keep compiling; durable
    /// backends must override it.
    fn remove_scheduled_draft_jobs(&self, draft_id: &str) -> Result<usize, PortError> {
        let _ = draft_id;
        Ok(0)
    }
    /// True when any pending (claimable or leased, but not quarantined) job
    /// carries `key` as its idempotency key. Drives the durable replay gate
    /// for keyed fan-out (e.g. one transcode job per file id): the producer
    /// checks this before enqueueing so a re-run converges instead of
    /// stacking duplicates. Defaults to `false` so in-memory test doubles
    /// keep compiling; durable backends must override it.
    fn has_pending_with_key(&self, _key: &str) -> Result<bool, PortError> {
        Ok(false)
    }
}

/// Payload of a `JobKind::RemoteMediaFetch` job: the committed note to
/// attach to plus the (already count-truncated) remote attachments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteMediaFetchPayload {
    pub note_id: String,
    pub attachments: Vec<RemoteMediaAttachment>,
}

/// Payload of a `JobKind::QuoteStampReverify` job: re-dereference and verify
/// the stamp offered by an inbound `Accept(QuoteRequest)` whose first fetch
/// failed transiently. `accept_activity_id` is recorded only on successful
/// verification (with the state change, atomically), so retries and
/// re-deliveries converge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuoteStampReverifyPayload {
    pub accept_activity_id: String,
    pub request_uri: String,
    pub stamp_uri: String,
}

/// Payload of a `JobKind::ReplyParentResolve` job: resolve one pending
/// reply-parent URI (`docs/replies.md` Slice B). One URI-keyed job covers
/// all children waiting on that URI; the handler processes them in bounded
/// batches and enqueues a continuation when more remain. Duplicate jobs
/// for the same URI are harmless: linking is idempotent, so replays and
/// retries converge without double-counting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplyParentResolvePayload {
    pub parent_uri: String,
}

/// Payload of a `JobKind::PollExpiry` job: the note whose poll may have
/// expired. `not_before` is armed at the poll's `expires_at`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PollExpiryPayload {
    pub note_id: String,
}

/// Payload of a `JobKind::UnfollowAll` job (Batch M WP3): the suspended
/// account whose outgoing follows are silently removed, plus the walk
/// cursor. `after` is the last `followee_id` processed by the previous
/// batch (`None` = start); it advances monotonically so a skipped row can
/// never loop the sweep.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnfollowAllPayload {
    pub account_id: String,
    #[serde(default)]
    pub after: Option<String>,
}

/// Payload of a `JobKind::RemoteEmojiFetch` job (Batch N WP8b): one remote
/// emoji to fetch-and-cache after a user opted in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteEmojiFetchPayload {
    pub host: String,
    pub shortcode: String,
}

/// Payload of a `JobKind::ExportCustomEmojis` / `JobKind::ImportCustomEmojis`
/// job: the requesting user, plus (import only) the drive file holding the
/// zip. `#[serde(default)]` keeps payloads written before `file_id` existed
/// decodable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustomEmojiTransferPayload {
    pub user_id: String,
    #[serde(default)]
    pub file_id: Option<String>,
}

/// Payload of a `JobKind::AntennaMatch` job (Batch N WS4): the committed
/// note to fan into antenna feeds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AntennaMatchPayload {
    pub note_id: String,
}

/// Payload of a `JobKind::CacheEvict` job (storage overhaul): free at
/// least `need_bytes` for `class` (byte-weighted LRU + TTL, bytes only).
/// Small by construction (well under [`MAX_JOB_PAYLOAD_BYTES`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheEvictPayload {
    pub need_bytes: u64,
    pub class: MediaClass,
}

/// Payload of a `JobKind::ReconcileStorage` job (storage overhaul): one
/// bounded reconciler scan. `scan_cap` is clamped to `RECONCILE_SCAN_MAX`
/// by the handler; the scan converges over repeated runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconcileStoragePayload {
    #[serde(default)]
    pub scan_cap: Option<usize>,
}

/// Payload of a `JobKind::BackfillDurableAccounting` job (storage
/// overhaul): recompute durable owner bytes (and blob refcounts) in
/// bounded pages of at most 500 rows. `after` is the start cursor
/// (`None` = newest); the handler scans to the end and persists the sums
/// once, so re-runs converge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillDurablePayload {
    #[serde(default)]
    pub after: Option<String>,
    #[serde(default)]
    pub batch_size: Option<usize>,
}

/// Payload of a `JobKind::PostScheduledDraft` job: the draft to post.
/// `scheduled_at_ms` is the `scheduled_at` the job was armed for — the
/// handler converges quietly when the stored draft no longer matches
/// (updated/rescheduled or unscheduled meanwhile). `not_before` is armed
/// at `scheduled_at_ms` (Misskey `postScheduledNoteQueue` delay parity).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduledDraftPayload {
    pub draft_id: String,
    pub scheduled_at_ms: i64,
}

/// Exportable profile entity (§13; Misskey `export*` job parity). The wire
/// filename stem, extension, and `exportedEntity` tag are fixed per entity
/// (see the `Export*ProcessorService.ts` files).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ExportEntity {
    Following,
    Blocking,
    Muting,
    Notes,
    UserLists,
    Antennas,
    Clips,
    Favorites,
}

impl ExportEntity {
    /// Drive filename stem (`following-…`, `blocking-…`, `mute-…`,
    /// `notes-…`, `user-lists-…`, `antennas-…`, `clips-…`, `favorites-…`
    /// — verbatim upstream stems, including the `mute-` singular).
    pub fn file_stem(&self) -> &'static str {
        match self {
            ExportEntity::Following => "following",
            ExportEntity::Blocking => "blocking",
            ExportEntity::Muting => "mute",
            ExportEntity::Notes => "notes",
            ExportEntity::UserLists => "user-lists",
            ExportEntity::Antennas => "antennas",
            ExportEntity::Clips => "clips",
            ExportEntity::Favorites => "favorites",
        }
    }

    /// Drive file extension (CSV for the acct-list entities, JSON for the
    /// structured ones — verbatim upstream `ext`).
    pub fn file_ext(&self) -> &'static str {
        match self {
            ExportEntity::Following
            | ExportEntity::Blocking
            | ExportEntity::Muting
            | ExportEntity::UserLists => "csv",
            ExportEntity::Notes
            | ExportEntity::Antennas
            | ExportEntity::Clips
            | ExportEntity::Favorites => "json",
        }
    }

    /// Misskey `exportCompleted` `exportedEntity` tag (verbatim upstream
    /// values, including the `muting`/`note`/`userList` singulars).
    pub fn exported_entity(&self) -> &'static str {
        match self {
            ExportEntity::Following => "following",
            ExportEntity::Blocking => "blocking",
            ExportEntity::Muting => "muting",
            ExportEntity::Notes => "note",
            ExportEntity::UserLists => "userList",
            ExportEntity::Antennas => "antenna",
            ExportEntity::Clips => "clip",
            ExportEntity::Favorites => "favorite",
        }
    }
}

/// Importable profile entity (§13; Misskey `import*` job parity).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ImportEntity {
    Following,
    Blocking,
    Muting,
    UserLists,
    Antennas,
    /// pinetree-only (no upstream `i/import-clips` exists): consumes the
    /// pinetree `export-clips` format (DIVERGENCE, documented in the
    /// endpoint manifest §36).
    Clips,
}

/// Payload of a `JobKind::Export` job: which entity of `user_id` to export
/// into the caller's drive. `export_id` keys idempotency (deterministic
/// completion-notification key + content-deduped upload). `exclude_muting`
/// drops muted followees (`export-following` parity); `exclude_inactive`
/// is accepted and ignored — pinetree tracks no last-active timestamp
/// (DIVERGENCE, documented at the call sites).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportJobPayload {
    pub export_id: String,
    pub user_id: String,
    pub entity: ExportEntity,
    pub exclude_muting: bool,
    pub exclude_inactive: bool,
}

/// Payload of a `JobKind::Import` job: apply the caller's drive file
/// `file_id` as `entity` rows for `user_id`. `import_id` keys idempotency
/// (replays converge on per-row `ALREADY_*` conflicts). `with_replies`
/// seeds the follow `withReplies` default when a row carries none
/// (`import-following` parity).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportJobPayload {
    pub import_id: String,
    pub user_id: String,
    pub entity: ImportEntity,
    pub file_id: String,
    pub with_replies: Option<bool>,
}
