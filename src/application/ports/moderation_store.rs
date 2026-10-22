use super::account_store::UpdateAccountPatch;
use super::job_queue::NewJob;
use super::note_store::TimelineCursor;
use crate::domain::account::Account;
use crate::domain::id::PortError;
use crate::domain::moderation::{
    AbuseReport, AbuseReportResolution, ModerationAction, ModerationLogEntry,
};
use crate::domain::webhook::AbuseReportRecipient;
use serde::{Deserialize, Serialize};

/// [`ModerationLogStore::list_logs`] filter (upstream
/// `admin/show-moderation-logs` params).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModerationLogFilter {
    /// Upstream `type`.
    pub action: Option<ModerationAction>,
    /// The moderator who performed the action (upstream `userId`).
    pub actor: Option<String>,
    /// The account or entity acted on (pinetree extra).
    pub target: Option<String>,
    /// Case-insensitive substring match on the details JSON (upstream
    /// `search`: `info::text ILIKE`).
    pub search: Option<String>,
}

pub trait ModerationLogStore: Send + Sync {
    /// WP3 suspension: flip `is_suspended`, delete follow requests in both
    /// directions when suspending (upstream `postSuspend`), append `log`,
    /// and enqueue `jobs` — one transaction (conformance H1/H2/H3). The
    /// refusal rule (never act on a moderator/admin) is application logic
    /// and lives in the command, not here.
    fn set_suspended(
        &self,
        account_id: &str,
        suspended: bool,
        log: ModerationLogEntry,
        jobs: Vec<NewJob>,
    ) -> Result<Account, PortError>;

    /// §3.1 roles: set the fixed built-in role flags and append `log` in
    /// one transaction. This is the ONLY write path for `is_admin`/
    /// `is_moderator`; `UpdateAccountPatch` deliberately cannot set them.
    fn set_role(
        &self,
        account_id: &str,
        is_admin: bool,
        is_moderator: bool,
        log: ModerationLogEntry,
    ) -> Result<Account, PortError>;

    /// Account patch + moderation-log row + jobs in one transaction
    /// (conformance H4). Used by every Batch M admin action that changes an
    /// account (`admin/reject-quotes`, `admin/update-user-note`, …).
    fn update_account_logged(
        &self,
        account_id: &str,
        patch: UpdateAccountPatch,
        log: ModerationLogEntry,
        jobs: Vec<NewJob>,
    ) -> Result<Account, PortError>;

    /// Batch M WP6 delete-account: tombstone the account (handle stays
    /// reserved), remove follows and follow requests in both directions,
    /// append `log` when given, and enqueue `jobs` — one transaction
    /// (conformance C3 + delete atomicity). `log` is `Some` for admin
    /// deletes and self-deletes (actor = the account itself).
    fn delete_account(
        &self,
        account_id: &str,
        log: Option<ModerationLogEntry>,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError>;

    /// Append one log row in its own transaction. Only for rows that ARE
    /// the action (nothing else commits with them); moderation actions use
    /// the compound methods above so a log row can never be lost against
    /// its action (H4).
    fn append(&self, entry: ModerationLogEntry) -> Result<(), PortError>;

    /// Newest-first page of log rows matching `filter`, cursor-paged by id
    /// (`TimelineCursor` since/until + limit).
    fn list_logs(
        &self,
        filter: &ModerationLogFilter,
        cursor: &TimelineCursor,
    ) -> Result<Vec<ModerationLogEntry>, PortError>;
}

/// Abuse reports (Batch M WP4; upstream `AbuseUserReport`). Whole-port
/// methods: state change + moderation-log row + outbound jobs commit in
/// ONE transaction (§2.3). `create` carries no log (upstream logs only
/// on resolve/forward/note-update); `resolve`/`set_note`/`mark_forwarded`
/// are compound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbuseReportFilter {
    /// Upstream `state`: "resolved" | "unresolved" | None (all).
    pub state: Option<String>,
    /// Upstream `reporterOrigin`: combined/local/remote.
    pub reporter_origin: String,
    /// Upstream `targetUserOrigin`: combined/local/remote.
    pub target_origin: String,
}

pub trait AbuseReportStore: Send + Sync {
    fn create(&self, report: AbuseReport) -> Result<AbuseReport, PortError>;
    fn get(&self, id: &str) -> Result<Option<AbuseReport>, PortError>;

    /// Batch M WP6: number of abuse reports whose target is `account_id`.
    fn count_for_target(&self, account_id: &str) -> Result<usize, PortError>;

    fn list(
        &self,
        filter: &AbuseReportFilter,
        cursor: &TimelineCursor,
    ) -> Result<Vec<AbuseReport>, PortError>;

    /// Resolve: flip resolved/resolved_as/assignee, append `log`, enqueue
    /// `jobs` — one transaction.
    fn resolve(
        &self,
        id: &str,
        resolved_as: Option<AbuseReportResolution>,
        assignee: Option<String>,
        log: ModerationLogEntry,
        jobs: Vec<NewJob>,
    ) -> Result<AbuseReport, PortError>;

    /// Set the moderation note. `log` is `Some` only when the note actually
    /// changed (upstream logs `updateAbuseReportNote` conditionally).
    fn set_note(
        &self,
        id: &str,
        note: String,
        log: Option<ModerationLogEntry>,
        jobs: Vec<NewJob>,
    ) -> Result<AbuseReport, PortError>;

    /// Mark forwarded; commits with `log` + `jobs` (the Flag delivery) in
    /// one transaction. Returns the updated report.
    fn mark_forwarded(
        &self,
        id: &str,
        log: ModerationLogEntry,
        jobs: Vec<NewJob>,
    ) -> Result<AbuseReport, PortError>;

    /// Inbound `Flag` (upstream `ApInboxService.flag` parity): atomically
    /// check-and-record `activity_id` and insert `report`. `Ok(None)` on
    /// replay (already seen). The target must be a local account id; the
    /// reporter may be remote (its account id is stored; host is derivable
    /// from the account row).
    fn create_flag_report(
        &self,
        activity_id: &str,
        report: AbuseReport,
    ) -> Result<Option<AbuseReport>, PortError>;
}

/// Per-pair memo text (`users/update-memo` parity: one free-form memo the
/// caller keeps about another account).
pub trait UserMemoStore: Send + Sync {
    /// Replace the memo `owner_id` keeps about `target_id` (`None` clears
    /// it). Upsert in one transaction; missing accounts are validated by
    /// the caller, not the store.
    fn set_memo(
        &self,
        owner_id: &str,
        target_id: &str,
        memo: Option<String>,
    ) -> Result<(), PortError>;
    /// The memo `owner_id` keeps about `target_id`, if any.
    fn get_memo(&self, owner_id: &str, target_id: &str) -> Result<Option<String>, PortError>;
}

/// Durable abuse-report notification recipients.
pub trait AbuseReportRecipientStore: Send + Sync {
    fn insert_recipient(
        &self,
        row: AbuseReportRecipient,
    ) -> Result<AbuseReportRecipient, PortError>;
    fn get_recipient(&self, id: &str) -> Result<Option<AbuseReportRecipient>, PortError>;
    fn list_recipients(&self) -> Result<Vec<AbuseReportRecipient>, PortError>;
    fn put_recipient(&self, row: AbuseReportRecipient) -> Result<AbuseReportRecipient, PortError>;
    fn delete_recipient(&self, id: &str) -> Result<bool, PortError>;
    fn active_recipient_emails(&self, limit: usize) -> Result<Vec<String>, PortError>;
}

// ---------------------------------------------------------------------------
// Gaps WP5: durable `Accept`-activity index (the process-local
// `ActivityIdIndex` in `crate::federation` closes the single-node gap;
// this trait is its durable replacement so a restart can still resolve a
// bare-IRI `Undo{object: <accept-id>}` to the follow pair it revokes).
// ---------------------------------------------------------------------------
