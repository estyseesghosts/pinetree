//! Batch M WP4 — abuse reports: `users/report-abuse`, admin listing,
//! resolve, note-update, and forward (Flag delivery).

use super::AppServices;

use crate::application::ports::{
    AbuseReportFilter, PortError, TimelineCursor,
};
use crate::domain::moderation::{AbuseReport, AbuseReportResolution, ModerationAction, ModerationLogEntry};
use crate::federation::outbox::FederateDeliverPayload;

impl AppServices {
    /// `users/report-abuse`: file a report. Returns the created report id.
    pub fn report_abuse(
        &self,
        reporter_id: &str,
        target_account_id: &str,
        comment: &str,
    ) -> Result<AbuseReport, PortError> {
        // Validation of comment length lives in domain.
        let target = self
            .accounts
            .find_by_id(target_account_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
        if reporter_id == target_account_id {
            return Err(PortError::Validation("CANNOT_REPORT_YOURSELF".to_string()));
        }
        // Upstream checks `roleService.isAdministrator` (admin-only); reporting
        // a moderator is legitimate there. Pinetree matches it.
        if target.is_admin {
            return Err(PortError::Validation("CANNOT_REPORT_THE_ADMIN".to_string()));
        }
        let report = AbuseReport::new(
            self.ids.generate(),
            target_account_id.to_string(),
            reporter_id.to_string(),
            comment.to_string(),
            self.clock.now_ms(),
        )?;
        self.abuse_reports.create(report)
    }

    pub fn list_abuse_reports(
        &self,
        filter: AbuseReportFilter,
        cursor: &TimelineCursor,
    ) -> Result<Vec<AbuseReport>, PortError> {
        self.abuse_reports.list(&filter, cursor)
    }

    pub fn get_abuse_report(&self, id: &str) -> Result<Option<AbuseReport>, PortError> {
        self.abuse_reports.get(id)
    }

    pub fn resolve_abuse_report(
        &self,
        moderator_id: &str,
        report_id: &str,
        resolved_as: Option<AbuseReportResolution>,
    ) -> Result<AbuseReport, PortError> {
        let report = self
            .abuse_reports
            .get(report_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_ABUSE_REPORT".to_string()))?;
        let target = self.accounts.find_by_id(&report.target_account)?;
        let log = ModerationLogEntry::new(
            self.ids.generate(),
            Some(moderator_id.to_string()),
            ModerationAction::ResolveAbuseReport,
            Some(report_id.to_string()),
            serde_json::json!({
                "reportId": report_id,
                "resolvedAs": resolved_as.map(|r| r.as_str()),
            }),
            self.clock.now_ms(),
        )?;
        let jobs = Vec::new();
        let _ = target;
        self.abuse_reports.resolve(
            report_id,
            resolved_as,
            Some(moderator_id.to_string()),
            log,
            jobs,
        )
    }

    pub fn update_abuse_report_note(
        &self,
        moderator_id: &str,
        report_id: &str,
        note: String,
    ) -> Result<AbuseReport, PortError> {
        let report = self
            .abuse_reports
            .get(report_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_ABUSE_REPORT".to_string()))?;
        let changed = report.moderation_note != note;
        let log = if changed {
            Some(ModerationLogEntry::new(
                self.ids.generate(),
                Some(moderator_id.to_string()),
                ModerationAction::UpdateAbuseReportNote,
                Some(report_id.to_string()),
                serde_json::json!({
                    "reportId": report_id,
                    "before": report.moderation_note,
                    "after": note,
                }),
                self.clock.now_ms(),
            )?)
        } else {
            None
        };
        self.abuse_reports.set_note(report_id, note, log, Vec::new())
    }

    /// Inbound `Flag` (upstream `ApInboxService.flag` parity): the first
    /// mentioned LOCAL user becomes the report target, the remote actor the
    /// reporter; `content` plus the JSON object-URI list become the comment
    /// (upstream `comment` shape). Unknown/remote-only targets converge
    /// quietly (`Ok(None)`, upstream `skip`). The dedup mark and the report
    /// commit in one transaction, so replays converge to `Ok(None)` with no
    /// duplicates. Instance blocks drop before any state write (WP5
    /// discipline — the caller checks first; this op re-checks the actor).
    /// No moderation-log row (upstream logs only resolve/forward/note-update
    /// via `report()`, which is log-free too).
    pub fn handle_inbound_flag(
        &self,
        activity_id: &str,
        actor_id: &str,
        object_uris: &[String],
        content: Option<&str>,
    ) -> Result<Option<AbuseReport>, PortError> {
        // Instance block (WP5): drop before any state write or dedup record.
        if self.inbound_host_blocked(actor_id) {
            return Ok(None);
        }
        let base = self.public_url.trim_end_matches('/').to_string();
        let prefix = format!("{base}/users/");
        let mut target_id: Option<String> = None;
        for uri in object_uris {
            let uri = uri.trim();
            if let Some(id) = uri.strip_prefix(&prefix) {
                let id = id.split(['/', '?', '#']).next().unwrap_or("").to_string();
                if id.is_empty() {
                    continue;
                }
                match self.accounts.find_by_id(&id)? {
                    Some(account) if account.is_local() => {
                        target_id = Some(account.id);
                        break;
                    }
                    _ => continue,
                }
            }
        }
        let Some(target_id) = target_id else {
            return Ok(None);
        };
        let comment = match content.map(str::trim).filter(|s| !s.is_empty()) {
            Some(content) => {
                let uris = serde_json::to_string_pretty(object_uris).unwrap_or_else(|_| "[]".to_string());
                format!("{content}\n{uris}")
            }
            None => serde_json::to_string_pretty(object_uris).unwrap_or_else(|_| "[]".to_string()),
        };
        // DIVERGENCE (documented): the report column is 1..=2048 chars
        // (upstream `report-abuse` paramDef). An overlong inbound comment
        // truncates instead of failing the inbox delivery (no retry storm).
        let comment: String = comment.chars().take(2048).collect();
        let report = AbuseReport::new(
            self.ids.generate(),
            target_id,
            actor_id.to_string(),
            comment,
            self.now_ms(),
        )?;
        self.abuse_reports.create_flag_report(activity_id, report)
    }

    pub fn forward_abuse_report(
        &self,
        moderator_id: &str,
        report_id: &str,
    ) -> Result<AbuseReport, PortError> {
        let report = self
            .abuse_reports
            .get(report_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_ABUSE_REPORT".to_string()))?;
        let target = self
            .accounts
            .find_by_id(&report.target_account)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
        if target.is_local() {
            return Err(PortError::Validation(
                "cannot forward a local report".to_string(),
            ));
        }
        // Upstream refuses a second forward ("already been forwarded").
        // Upstream throws a plain `Error` (→ 500); pinetree answers 400
        // `INVALID_PARAM` carrying the message (WP1 refusal-row precedent).
        if report.forwarded {
            return Err(PortError::Validation(
                "INVALID_PARAM already been forwarded".to_string(),
            ));
        }
        let inbox_url = self
            .remote_inbox_for_account(&target)
            .ok_or_else(|| PortError::Validation("no inbox".to_string()))?;
        let (system, _) = self.ensure_system_actor()?;
        let flag_id = self.ids.generate();
        let payload = FederateDeliverPayload::Flag {
            flag_id: flag_id.clone(),
            actor_id: system.id.clone(),
            target_account_id: report.target_account.clone(),
            content: report.comment.clone(),
            inbox_url,
        };
        let job = payload.into_job(
            format!("federate:flag:{}:{}", report.id, flag_id),
            self.now_ms(),
        )?;
        let log = ModerationLogEntry::new(
            self.ids.generate(),
            Some(moderator_id.to_string()),
            ModerationAction::ForwardAbuseReport,
            Some(report_id.to_string()),
            serde_json::json!({
                "reportId": report_id,
            }),
            self.clock.now_ms(),
        )?;
        self.abuse_reports.mark_forwarded(report_id, log, vec![job])
    }
}
