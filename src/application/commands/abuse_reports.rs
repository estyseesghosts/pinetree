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
        if target.has_moderator_powers() {
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
