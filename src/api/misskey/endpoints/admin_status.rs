//! Admin status builders and pool presence.
//!
//! Split from `endpoints/mod.rs` (decomposition). Pure move, no behavior change.
#![allow(unused_imports)]
//! Misskey HTTP API surface (Slice D3).
//!
//! [`ApiState`] carries the application services, the in-process job
//! worker, and the rate limiter. [`build_router`] wires every Phase 1
//! route: `/healthz`, media/identicon file serving, the MiAuth web flow
//! (`GET`/`POST /miauth/{session}` plus the one-shot `check`
//! endpoints), web `signup`/`signin`, `drive/files/create` (multipart and
//! JSON), and the `POST /api/{endpoint}` JSON dispatcher.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{ConnectInfo, FromRequest, Multipart, Path, Query, Request, State},
    http::{
        HeaderMap, HeaderName, HeaderValue, StatusCode,
        header::{AUTHORIZATION, CONTENT_DISPOSITION, CONTENT_TYPE, COOKIE, LOCATION, SET_COOKIE},
    },
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use http_body_util::{BodyExt, Limited};
use serde_json::{Map, Value};

use crate::activitypub::federated_collections as ap_collections;
use crate::activitypub::{actor as ap_actor, objects as ap_objects, webfinger as ap_webfinger};
use crate::application::commands::AppServices;
use crate::application::commands::announcements::MemAnnouncementStore;
use crate::application::commands::gallery::FlashVisibility;
use crate::application::commands::gallery::IdCursor;
use crate::application::commands::gallery::MemGalleryStore;
use crate::application::commands::invites::SignupRejection;
use crate::application::commands::misc_endpoints::EmojisV2Page;
use crate::application::commands::misc_endpoints::ping;
use crate::application::commands::pins::FeaturedCache;
use crate::application::commands::webhooks::CreateSystemWebhookInput;
use crate::application::commands::webhooks::CreateUserWebhookInput;
use crate::application::commands::webhooks::UpdateSystemWebhookInput;
use crate::application::commands::webhooks::UpdateUserWebhookInput;
use crate::application::commands::webhooks::create_system_webhook;
use crate::application::commands::webhooks::create_user_webhook;
use crate::application::commands::webhooks::delete_system_webhook;
use crate::application::commands::webhooks::delete_user_webhook;
use crate::application::commands::webhooks::show_system_webhook;
use crate::application::commands::webhooks::show_user_webhook;
use crate::application::commands::webhooks::update_system_webhook;
use crate::application::commands::webhooks::update_user_webhook;
use crate::application::ports::{
    AbuseReportFilter, ApiTokenRecord, BlobStore, CreateDraftInput, CreateNoteInput, EditNoteInput,
    ExportEntity, ImportEntity, InviteListFilter, InviteSort, InviteTypeFilter, JobKind,
    LegacyAuthSessionRecord, MiAuthSessionRecord, ModerationLogFilter, NewJob,
    PasswordResetRequest, PortError, RegistryItem, SigninRecord, TimelineCursor,
    UpdateAccountPatch, UpdateDraftPatch,
};
use crate::domain::account::{
    Account, AvatarDecoration, ChatScope, FollowVisibility, MutedWord, NotificationRecieveConfig,
    ProfileField,
};
use crate::domain::follow::FollowNotify;
use crate::domain::instance::{InviteCode, UgcVisibilityForVisitor};
use crate::domain::moderation::{AbuseReportResolution, ModerationAction};
use crate::domain::note::{MAX_CW_API_CHARS, MAX_NOTE_TEXT_CHARS, ReactionAcceptance, Visibility};
use crate::domain::notification::NotificationKind;
use crate::federation::delivery::{
    fetch_and_upsert_remote_actor, fetch_remote_note, webfinger_discover_actor_uri,
};
use crate::federation::inbox as fed_inbox;
use crate::jobs::worker::JobWorker;
use crate::media::transcode::TranscodeConfig;
use crate::streaming::hub::{Hub, ONLINE_THRESHOLD_MS};
use crate::streaming::server::streaming_handler;

use crate::api::misskey::auth::{
    AuthenticatedCaller, SESSION_COOKIE_NAME, authenticate, extract_raw_token,
    generate_random_token, hash_password_async, hash_token, is_safe_callback_url, needs_rehash,
    render_miauth_page, require_admin, require_moderator, verify_password_async,
};
use crate::api::misskey::errors::ApiError;
use crate::api::misskey::packers::{
    pack_abuse_report, pack_antenna, pack_blocking, pack_clip, pack_drive_file, pack_drive_folder,
    pack_emoji_detailed, pack_emoji_simple, pack_following, pack_invite_code, pack_me_detailed,
    pack_meta, pack_moderation_log, pack_muting, pack_note, pack_note_draft,
    pack_note_reaction_with_note, pack_notification, pack_page, pack_remote_emoji_detailed,
    pack_renote_muting, pack_scheduled_note, pack_system_webhook, pack_user_detailed_not_me,
    pack_user_list, pack_user_lite, pack_user_relation, pack_user_webhook,
};
use crate::api::misskey::request::check_allowed_fields;
use crate::api::misskey::response::{RateLimiter, UploadSlots};
use crate::services::email::EmailError;
use crate::services::email::EmailMessage;
use crate::services::email::EmailSendPayload;
use crate::services::email::EmailSender;
use crate::services::email::SmtpConfig;
use crate::services::email::SmtpEmailSender;
use crate::services::email::password_reset_email;
use crate::services::email::verification_email;

use super::ApiState;
use super::middleware::{last_readiness, ops_now_ms, read_backup_context, read_ops_config, ready};
/// Shared API state: application services, the durable-job worker, the
/// Misskey rate limiter, the concurrent-upload slots, the live-streaming
/// broadcast hub, and the reverse proxies whose `X-Forwarded-For` is
/// honored ([`client_ip`]).

/// Newest backup statuses exposed on one status response (size limit).
pub const STATUS_BACKUP_LIST_CAP: usize = 20;
/// Cap on detailed-arm variable lists (`recentErrors`). The recent-error
/// ring already bounds to [`crate::observability::MAX_RECENT_ERRORS`]; this
/// tighter cap keeps one status body under ~8 KiB (each sample is two small
/// fields) with zero I/O: only a mutex snapshot plus clone, no table walk,
/// `check_integrity`, network fetch, or recursive hash per request.
pub const STATUS_DETAIL_CAP: usize = 32;
/// Page budget for the `admin/drive/show-file` url fallback walk: 20
/// pages × 500 rows bounds one lookup (same order as the old 10k clamp,
/// with correct empty-page termination).
pub const SHOW_FILE_WALK_PAGES: usize = 20;
/// Bounded `url` fallback walk for `admin/drive/show-file`: 500-row pages
/// newest-first, exclusive `until_id` advance, only an empty page ends the
/// walk, at most [`SHOW_FILE_WALK_PAGES`] pages. A short page (concurrent
/// deletes) never masquerades as end-of-scan.
pub(crate) fn walk_media_for_url(
    services: &AppServices,
    url: &str,
) -> Result<Option<crate::domain::media::MediaMeta>, ApiError> {
    let mut until_id: Option<String> = None;
    for _ in 0..SHOW_FILE_WALK_PAGES {
        let mut cursor = TimelineCursor::with_limit(500);
        cursor.until_id = until_id.clone();
        let page = services
            .media
            .list_all_media(&cursor)
            .map_err(ApiError::from_port_error)?;
        if page.is_empty() {
            break;
        }
        if let Some(hit) = page.iter().find(|m| m.url == url) {
            return Ok(Some(hit.clone()));
        }
        until_id = page.last().map(|m| m.id.clone());
    }
    Ok(None)
}
/// Safe subset of one [`crate::backup::BackupStatus`]: lifecycle, times,
/// sizes, and the redacted failure. No contents, no paths, no secrets
/// (the failure string is already redacted at write time).
pub(crate) fn pack_backup_status(status: &crate::backup::BackupStatus) -> Value {
    serde_json::json!({
        "jobId": status.job_id,
        "state": status.state,
        "requestedMs": status.requested_ms,
        "startedMs": status.started_ms,
        "finishedMs": status.finished_ms,
        "attempts": status.attempts,
        "outputFile": status.output_file,
        "bytesWritten": status.bytes_written,
        "tablesExported": status.tables_exported,
        "mediaFiles": status.media_files,
        "gateWaitMs": status.gate_wait_ms,
        "failureRedacted": status.failure_redacted,
    })
}
/// Pool-root presence without leaking layout: configured + is-dir only.
pub(crate) fn pack_pool_presence(dir: &PathBuf) -> Value {
    serde_json::json!({
        "configured": !dir.as_os_str().is_empty(),
        "present": !dir.as_os_str().is_empty() && dir.is_dir(),
    })
}
/// Basic admin status: always available to an authenticated admin,
/// detailed flag off or on. See the H4 field list in the module docs.
pub(crate) fn build_admin_status(state: &ApiState, detailed: bool) -> Value {
    let ops = read_ops_config();
    let now_ms = ops_now_ms();
    let uptime_ms = if ops.started_ms <= 0 {
        Value::Null
    } else {
        Value::from(now_ms.saturating_sub(ops.started_ms as u64))
    };
    let (ready_ok, ready_class) = last_readiness();
    let redb_readable = state.services.instance_settings.get().is_ok();
    let cache = state.services.media.cache_stats().ok();
    let pending_jobs = state.services.jobs.pending_count().ok();
    let (
        backups,
        last_backup_success_ms,
        last_backup_failure_ms,
        last_backup_duration_ms,
        last_backup_bytes_written,
    ) = match read_backup_context() {
        Some(context) => {
            let statuses = crate::backup::newest_backup_statuses(
                &context.backup_dir,
                STATUS_BACKUP_LIST_CAP * 4,
            );
            let last_success = statuses
                .iter()
                .filter(|s| s.state == crate::backup::BackupState::Completed)
                .filter_map(|s| s.finished_ms)
                .max();
            let last_failure = statuses
                .iter()
                .filter(|s| s.state == crate::backup::BackupState::Failed)
                .filter_map(|s| s.finished_ms)
                .max();
            let recent: Vec<Value> = statuses
                .iter()
                .take(STATUS_BACKUP_LIST_CAP)
                .map(pack_backup_status)
                .collect();
            // Duration of the most recently finished terminal run
            // (success or failure) and output bytes of the most recently
            // completed run: bounded scans over the same loaded statuses,
            // no extra I/O. `None` until a run finishes / completes.
            let mut last_duration_ms: Option<u64> = None;
            let mut last_finished_ms: Option<u64> = None;
            for candidate in statuses.iter().filter_map(|s| {
                s.finished_ms.map(|finished| {
                    (
                        finished,
                        s.started_ms.map(|started| finished.saturating_sub(started)),
                    )
                })
            }) {
                if last_finished_ms.is_none_or(|best| candidate.0 >= best) {
                    last_finished_ms = Some(candidate.0);
                    last_duration_ms = candidate.1;
                }
            }
            let last_completed_bytes: Option<u64> = statuses
                .iter()
                .filter(|s| s.state == crate::backup::BackupState::Completed)
                .filter_map(|s| s.finished_ms.map(|finished| (finished, s.bytes_written)))
                .max_by_key(|(finished, _)| *finished)
                .and_then(|(_, bytes)| bytes);
            (
                Value::Array(recent),
                last_success.map(Value::from).unwrap_or(Value::Null),
                last_failure.map(Value::from).unwrap_or(Value::Null),
                last_duration_ms.map(Value::from).unwrap_or(Value::Null),
                last_completed_bytes.map(Value::from).unwrap_or(Value::Null),
            )
        }
        None => (
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ),
    };
    let mut status = serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "commit": option_env!("PINETREE_COMMIT").unwrap_or("unknown"),
        "uptimeMs": uptime_ms,
        "safeMode": {
            "transcodeEnabled": ops.transcode_enabled,
            "detailedAdminStatus": ops.detailed_admin_status,
            "structuredLogging": crate::observability::structured_logging(),
        },
        "readiness": { "ready": ready_ok, "lastProbeErrorClass": ready_class },
        "redb": { "open": true, "readable": redb_readable },
        "pools": {
            "durable": pack_pool_presence(&ops.durable_dir),
            "cache": pack_pool_presence(&ops.cache_dir),
            "backup": pack_pool_presence(&ops.backup_dir),
        },
        "cache": match &cache {
            Some(stats) => serde_json::json!({
                "usedBytes": stats.total_bytes,
                "capBytes": state.services.remote_cache_max_bytes,
                "avatarBytes": stats.avatar_bytes,
                "previewBytes": stats.preview_bytes,
                "fullBytes": stats.full_bytes,
                "inFlightBytes": stats.in_flight_bytes,
                "hits": stats.hits,
                "misses": stats.misses,
                "bytesFetched": stats.bytes_fetched,
                "evictions": stats.evictions,
            }),
            None => Value::Null,
        },
        // Honest nulls (unknown is data, never a guess). Each unavailable
        // aggregate names the parent-owned port getter that would be needed
        // to compute it without a walk/scan per request; until those land,
        // the key stays an explicit null so UI/docs must say "unavailable":
        // - `durableUsedBytes`: needs a total-durable-bytes getter
        //   (`MediaStore::durable_bytes` is per-owner only).
        // - `freeDiskBytes`: needs a filesystem-space probe (no dependency
        //   for it; a pool walk per request is refused).
        // - `accountingDrift`: needs persisted reconciler drift counters
        //   (the reconciler only returns ephemeral reports today).
        // - `lastBackfill` / `lastReconcile`: need persisted run
        //   timestamps for the storage jobs (none are recorded yet).
        // - `jobBreakdown` / `oldestPendingMs`: need job enumeration by
        //   kind/age (`JobQueue` exposes `pending_count` only).
        // - `quarantine`: needs quarantine enumeration/reasons
        //   (`JobQueue` keeps quarantined rows inspectable via `get_job`
        //   by id only).
        // - `federationDelivery`: needs an aggregate export on the
        //   parent-owned `DestinationTracker` (per-host health only).
        // `lastBackup*` below ARE actionable: they derive from the same
        // bounded status-file load as `backups` (already capped), and the
        // cache `*Bytes`/`evictions` arms expose per-class use plus
        // pressure. No table walk, `check_integrity`, network fetch, or
        // recursive hash runs per request.
        "durableUsedBytes": Value::Null,
        "freeDiskBytes": Value::Null,
        "accountingDrift": Value::Null,
        "lastBackfill": Value::Null,
        "lastReconcile": Value::Null,
        "jobs": match pending_jobs {
            Some(pending) => serde_json::json!({ "pending": pending }),
            None => serde_json::json!({ "pending": Value::Null }),
        },
        "jobBreakdown": Value::Null,
        "oldestPendingMs": Value::Null,
        "quarantine": Value::Null,
        "federationDelivery": Value::Null,
        "http": {
            "rateLimitBuckets": state.rate_limiter.bucket_count(),
            "authFailureKeys": state.rate_limiter.failure_count(),
            "uploadSlotIps": state.upload_slots.slot_ip_count(),
        },
        // Bounded presence snapshot (no secrets, no I/O): live broadcast
        // receivers plus distinct streaming accounts seen within
        // `ONLINE_THRESHOLD_MS` (prunes stale entries on read).
        "webSocket": serde_json::json!({
            "connections": state.hub.receiver_count(),
            "onlineAccounts": state.hub.online_count(
                ops_now_ms() as i64,
                ONLINE_THRESHOLD_MS,
            ),
        }),
        "backups": backups,
        "lastBackupSuccessMs": last_backup_success_ms,
        "lastBackupFailureMs": last_backup_failure_ms,
        "lastBackupDurationMs": last_backup_duration_ms,
        "lastBackupBytesWritten": last_backup_bytes_written,
        "transcode": { "enabled": ops.transcode_enabled, "status": ops.ffmpeg_status },
    });
    if detailed {
        // Bounded detail arm (see `STATUS_DETAIL_CAP`): newest samples only,
        // categories only (never messages, bodies, or key material). No
        // `check_integrity`, table walk, or network fetch runs per request;
        // every source here is a mutex snapshot or a cloned snapshot value.
        let all_recent = crate::observability::recent_errors();
        let start = all_recent.len().saturating_sub(STATUS_DETAIL_CAP);
        let recent: Vec<Value> = all_recent[start..]
            .iter()
            .map(|sample| {
                serde_json::json!({
                    "recordedMs": sample.recorded_ms,
                    "category": sample.category,
                })
            })
            .collect();
        if let Some(map) = status.as_object_mut() {
            map.insert("recentErrors".to_string(), Value::Array(recent));
            map.insert(
                "errorCounts".to_string(),
                serde_json::to_value(crate::observability::error_counts()).unwrap_or(Value::Null),
            );
            map.insert(
                "configFingerprint".to_string(),
                ops.config_fingerprint.clone(),
            );
            map.insert(
                "startupProbes".to_string(),
                serde_json::json!({
                    "transcode": ops.ffmpeg_status,
                    "backupDirReady": !ops.backup_dir.as_os_str().is_empty()
                        && ops.backup_dir.is_dir(),
                }),
            );
        }
    }
    status
}
