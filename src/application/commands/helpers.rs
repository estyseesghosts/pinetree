use super::AppServices;

use std::collections::HashSet;

use crate::application::events::{NotifyJobPayload, StreamingEvent};
use crate::application::ports::{JobKind, NewJob, PortError};
use crate::domain::note::{Note, extract_mentions};
use crate::domain::notification::NotificationKind;
use crate::domain::policy::{ViewerContext, is_visible};

impl AppServices {
    pub(super) fn now_ms(&self) -> i64 {
        self.clock.now_ms()
    }

    pub(super) fn notify_job(
        &self,
        notifiee_id: &str,
        notifier_id: &str,
        kind: NotificationKind,
        note_id: Option<String>,
        reaction: Option<String>,
        key_suffix: &str,
    ) -> Result<NewJob, PortError> {
        self.notify_job_full(
            notifiee_id,
            notifier_id,
            kind,
            note_id,
            reaction,
            None,
            None,
            key_suffix,
        )
    }

    /// `notify_job` with clip/page refs (`ClipUpdated` carries `clip_id`,
    /// `PageLiked` carries `page_id`).
    pub fn notify_job_full(
        &self,
        notifiee_id: &str,
        notifier_id: &str,
        kind: NotificationKind,
        note_id: Option<String>,
        reaction: Option<String>,
        clip_id: Option<String>,
        page_id: Option<String>,
        key_suffix: &str,
    ) -> Result<NewJob, PortError> {
        let payload = NotifyJobPayload {
            notifiee_id: notifiee_id.to_string(),
            notifier_id: notifier_id.to_string(),
            kind,
            note_id,
            reaction,
            file_id: None,
            exported_entity: None,
            clip_id,
            page_id,
        };
        NewJob::new_json(
            JobKind::Notify,
            &payload,
            format!("notify:{key_suffix}:{}", self.ids.generate()),
            self.now_ms(),
        )
    }

    pub(super) fn streaming_job(
        &self,
        event: &StreamingEvent,
        key: String,
    ) -> Result<NewJob, PortError> {
        NewJob::new_json(JobKind::StreamingPublish, event, key, self.now_ms())
    }

    // ------------------------------------------------------------------
    // Shared federation operations (Phase 2 Slices D1 & D3).
    //
    // These are the inbound/outbound contracts consumed by
    // `src/federation/inbox.rs` and the job worker. Inbound handlers commit
    // state+jobs atomically through the dedup record path and never enqueue
    // further `FederateDeliver` fan-out, except the auto-Accept in
    // `handle_inbound_follow` (prevents federation loops).
    // ------------------------------------------------------------------

    /// Build a `FederateDeliver` job for `payload` with a generated
    /// idempotency key under `key_prefix`.
    pub fn federate_job(
        &self,
        payload: &crate::federation::outbox::FederateDeliverPayload,
        key_prefix: &str,
    ) -> Result<NewJob, PortError> {
        payload.into_job(
            format!("{key_prefix}:{}", self.ids.generate()),
            self.now_ms(),
        )
    }

    /// Preferred remote inbox for `account` (`shared_inbox` first, then
    /// `inbox`; `None` for local accounts).
    pub fn remote_inbox_for_account(
        &self,
        account: &crate::domain::account::Account,
    ) -> Option<String> {
        crate::federation::outbox::preferred_inbox(account)
    }
}

// ----------------------------------------------------------------------
// Shared note-creation side effects (local + inbound).
//
// `create_note` (Misskey API adapter path) and `handle_inbound_create_note`
// (federation inbox adapter path) each build reply, renote, and mention
// `Notify` jobs. That triple lives here as ONE application operation so
// the two adapters cannot drift apart: one `Notify` per user (a note that
// is both a reply and a mention emits a single notification), with the
// per-path strictness preserved below.
// ----------------------------------------------------------------------

/// Placeholder substituted with the committed note id inside the storage
/// transaction (the `Notify` rows commit atomically with the note).
pub(crate) const PENDING_NOTE_ID: &str = "__PENDING_NOTE_ID__";

/// Strictness of [`AppServices::note_interaction_jobs`]: the local API
/// path validates targets and propagates failures; the inbound federation
/// path is best-effort so one bad reference never fails inbox delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoteSideEffectMode {
    Local,
    Inbound,
}

/// Reply/renote resolutions plus the interaction `Notify` jobs for one
/// note creation. Targets are resolved exactly like the calling path
/// needs them (strict for local, best-effort for inbound); the local
/// quote-detection block reuses `renote_target`.
pub struct NoteSideEffects {
    pub jobs: Vec<NewJob>,
    pub reply_target: Option<Note>,
    pub renote_target: Option<Note>,
}

impl AppServices {
    /// Shared reply/renote/mention `Notify`-job builder for note creation:
    /// one application operation called by both the local `create_note`
    /// and the inbound `handle_inbound_create_note` paths.
    pub fn note_interaction_jobs(
        &self,
        author_id: &str,
        reply_to: Option<&str>,
        renote_of: Option<&str>,
        text: Option<&str>,
        cw: Option<&str>,
        no_extract_mentions: bool,
        mode: NoteSideEffectMode,
    ) -> Result<NoteSideEffects, PortError> {
        // Idempotency-key suffixes are load-bearing (dedupe keys): the
        // local and inbound paths keep their historical tags.
        let tag = |base: &str| match mode {
            NoteSideEffectMode::Local => base.to_string(),
            NoteSideEffectMode::Inbound => format!("inbound-{base}"),
        };
        let strict = mode == NoteSideEffectMode::Local;

        // Resolve reply/renote targets so we know who to notify. Local:
        // missing and invisible targets are indistinguishable so a reply
        // cannot probe for hidden notes; renote misses are
        // `NO_SUCH_RENOTE_TARGET`. Inbound: best-effort existence only —
        // deleted or invisible targets simply notify nobody.
        let reply_target = match reply_to {
            None => None,
            Some(id) => {
                if strict {
                    let note = match self.notes.get_note(id)? {
                        None => {
                            return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
                        }
                        Some(note) => {
                            let follows_author = note.author_id.as_str() == author_id
                                || self.follows.is_following(author_id, &note.author_id)?;
                            let viewer = ViewerContext::local_user(author_id, follows_author);
                            if note.is_deleted || !is_visible(&note, &viewer) {
                                return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
                            }
                            note
                        }
                    };
                    Some(note)
                } else {
                    match self.notes.get_note(id) {
                        Ok(Some(note)) => Some(note),
                        _ => None,
                    }
                }
            }
        };
        let renote_target = match renote_of {
            None => None,
            Some(id) => {
                if strict {
                    match self.notes.get_note(id)? {
                        None => {
                            return Err(PortError::NotFound("NO_SUCH_RENOTE_TARGET".to_string()));
                        }
                        Some(note) if note.is_deleted => {
                            return Err(PortError::NotFound("NO_SUCH_RENOTE_TARGET".to_string()));
                        }
                        Some(note) => Some(note),
                    }
                } else {
                    match self.notes.get_note(id) {
                        Ok(Some(note)) => Some(note),
                        _ => None,
                    }
                }
            }
        };

        let mut jobs = Vec::new();
        // One `Notify` per user: a note that is both a reply and a
        // mention must not emit two `notification` frames.
        let mut already_notified: HashSet<String> = HashSet::new();
        // Push a notification job, propagating storage faults on the
        // local path and skipping them on the inbound path.
        macro_rules! push_notify {
            ($notifiee:expr, $kind:expr, $suffix:expr) => {{
                let job = self.notify_job(
                    $notifiee,
                    author_id,
                    $kind,
                    Some(PENDING_NOTE_ID.to_string()),
                    None,
                    &tag($suffix),
                );
                if strict {
                    jobs.push(job?);
                } else if let Ok(job) = job {
                    jobs.push(job);
                }
            }};
        }

        if let Some(target) = reply_target.as_ref() {
            if target.author_id.as_str() != author_id
                && already_notified.insert(target.author_id.clone())
            {
                push_notify!(&target.author_id, NotificationKind::Reply, "reply");
            }
        }
        if let Some(target) = renote_target.as_ref() {
            if target.author_id.as_str() != author_id
                && !already_notified.contains(&target.author_id)
            {
                already_notified.insert(target.author_id.clone());
                push_notify!(&target.author_id, NotificationKind::Renote, "renote");
            }
        }

        // `@username` mentions (local only) become Mention notifications.
        if !no_extract_mentions {
            let mut buf = text.unwrap_or_default().to_string();
            if let Some(cw) = cw {
                buf.push('\n');
                buf.push_str(cw);
            }
            let mut seen: HashSet<String> = HashSet::new();
            for username in extract_mentions(&buf) {
                let lower = username.to_lowercase();
                if !seen.insert(lower.clone()) {
                    continue;
                }
                let mentioned = if strict {
                    self.accounts.find_by_handle(&lower)?
                } else {
                    match self.accounts.find_by_handle(&lower) {
                        Ok(mentioned) => mentioned,
                        Err(_) => continue,
                    }
                };
                if let Some(mentioned) = mentioned {
                    if mentioned.id.as_str() != author_id
                        && !already_notified.contains(&mentioned.id)
                    {
                        already_notified.insert(mentioned.id.clone());
                        push_notify!(&mentioned.id, NotificationKind::Mention, "mention");
                    }
                }
            }
        }

        Ok(NoteSideEffects {
            jobs,
            reply_target,
            renote_target,
        })
    }
}
