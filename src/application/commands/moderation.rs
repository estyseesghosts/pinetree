//! Batch M moderation use cases: built-in roles (§3.1) and the moderation
//! log read path (§3.2). WP3–WP6 moderation commands land here as the batch
//! progresses.

use super::AppServices;

use crate::application::events::StreamingEvent;
use crate::application::ports::{
    InstanceSettingsPatch, JobKind, ModerationLogFilter, NewJob, PortError, TimelineCursor,
    UnfollowAllPayload,
};
use crate::domain::account::Account;
use crate::domain::instance::{InstanceSettings, RegistrationPolicy};
use crate::domain::moderation::{ModerationAction, ModerationLogEntry};
use crate::federation::outbox::FederateDeliverPayload;

/// Outgoing-follow rows one `UnfollowAll` sweep run removes (Batch M WP3).
/// Bounded so one run stays far inside the FDB ~5 s / ~10 MiB transaction
/// limits; more rows are covered by the continuation job the run returns.
pub const UNFOLLOW_ALL_PAGE: usize = 100;

impl AppServices {
    /// §3.1 `set-role`: set the fixed built-in role flags. This is the
    /// only write path for `is_admin`/`is_moderator` (the CLI makes the
    /// first admin this way; `UpdateAccountPatch` cannot set roles).
    /// `actor` is the moderator taking the action, `None` for operator CLI
    /// use; the row logs as `setRole` in the same transaction (§2.3).
    pub fn set_role(
        &self,
        actor: Option<&str>,
        account_id: &str,
        is_admin: bool,
        is_moderator: bool,
    ) -> Result<Account, PortError> {
        self.accounts.get_owner(account_id)?;
        let target = self
            .accounts
            .find_by_id(account_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
        let log = ModerationLogEntry::new(
            self.ids.generate(),
            actor.map(str::to_string),
            ModerationAction::SetRole,
            Some(account_id.to_string()),
            serde_json::json!({
                "userId": account_id,
                "userUsername": target.username,
                "userHost": target.host,
                "isAdmin": is_admin,
                "isModerator": is_moderator,
            }),
            self.clock.now_ms(),
        )?;
        self.moderation
            .set_role(account_id, is_admin, is_moderator, log)
    }

    /// `admin/suspend-user` (Batch M WP3): suspend one account. Refused on
    /// moderator/admin targets (guide §3.1's blanket rule; upstream
    /// `suspend-user.ts:46` throws `cannot suspend moderator account`) and
    /// answered `NO_SUCH_USER` for a missing target (DIVERGENCE: upstream
    /// throws an untyped `user not found` there, which its API layer turns
    /// into `500 INTERNAL_ERROR`; pinetree answers the typed client error).
    ///
    /// One transaction (conformance H1): the `is_suspended` flag, the
    /// follow-request teardown in both directions (upstream `postSuspend`),
    /// the `suspend` moderation-log row, and the outbound jobs. For a LOCAL
    /// target those jobs are (1) the stream-close `UserSuspended` event,
    /// (2) ONE `SuspendBroadcast` sweep job the worker expands into
    /// per-inbox `Delete(Person)` deliveries (guide §11: the direct
    /// per-recipient enqueue is unbounded against `MAX_JOBS_PER_TXN` and
    /// the FDB transaction limits), and (3) ONE `UnfollowAll` sweep job
    /// whose worker runs silently remove the account's outgoing follows
    /// (upstream `unFollowAll`, `silent: true`) and deliver `Undo(Follow)`
    /// per remote followee. Remote targets take the flag + log + request
    /// teardown only — DIVERGENCE: upstream also runs `unFollowAll` for
    /// remote targets (deleting their edges to local accounts and
    /// delivering `Reject` per edge).
    ///
    /// Unlike `admin/reject-quotes`, a no-change call is NOT a no-op:
    /// upstream `suspend()` logs and re-runs its post-suspend work
    /// unconditionally, so re-suspending re-logs and re-sends the sweep.
    pub fn suspend_user(&self, moderator_id: &str, account_id: &str) -> Result<Account, PortError> {
        self.set_suspended_flag(moderator_id, account_id, true)
    }

    /// `admin/unsuspend-user` (Batch M WP3): mirror of [`Self::suspend_user`]
    /// (conformance H2). Restores the account but never restores what the
    /// suspend tore down: follow requests (both directions) and outgoing
    /// follows stay removed (upstream parity). For a LOCAL target the
    /// outbound job is ONE `SuspendBroadcast` sweep (`Undo(Delete(Person))`
    /// to the same knowable shared inboxes, upstream `postUnsuspend`).
    ///
    /// DIVERGENCE: the moderator/admin-target refusal is applied here too
    /// (guide §3.1 blanket rule); upstream `unsuspend-user.ts` has no such
    /// check — an apparent oversight, since it would otherwise let a
    /// moderator act on another moderator's account.
    pub fn unsuspend_user(
        &self,
        moderator_id: &str,
        account_id: &str,
    ) -> Result<Account, PortError> {
        self.set_suspended_flag(moderator_id, account_id, false)
    }

    fn set_suspended_flag(
        &self,
        moderator_id: &str,
        account_id: &str,
        suspended: bool,
    ) -> Result<Account, PortError> {
        let target = self
            .accounts
            .find_by_id(account_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
        // Ownership hook for Phase 4 routing (see `AppServices` docs).
        self.accounts.get_owner(account_id)?;
        if target.has_moderator_powers() {
            return Err(PortError::Validation(
                if suspended {
                    "cannot suspend moderator account"
                } else {
                    "cannot unsuspend moderator account"
                }
                .to_string(),
            ));
        }
        let action = if suspended {
            ModerationAction::Suspend
        } else {
            ModerationAction::Unsuspend
        };
        let log = ModerationLogEntry::new(
            self.ids.generate(),
            Some(moderator_id.to_string()),
            action,
            Some(account_id.to_string()),
            serde_json::json!({
                "userId": account_id,
                "userUsername": target.username,
                "userHost": target.host,
            }),
            self.clock.now_ms(),
        )?;
        let mut jobs = Vec::new();
        if target.is_local() {
            if suspended {
                jobs.push(self.streaming_job(
                    &StreamingEvent::UserSuspended {
                        account_id: account_id.to_string(),
                    },
                    format!("suspend-stream:{account_id}:{}", self.ids.generate()),
                )?);
            }
            let broadcast = FederateDeliverPayload::SuspendBroadcast {
                account_id: account_id.to_string(),
                activity_id: self.ids.generate(),
                undo: !suspended,
                inbox_override: None,
            };
            jobs.push(self.federate_job(
                &broadcast,
                &format!(
                    "sweep:suspend-broadcast:{}:{}",
                    account_id,
                    if suspended { "delete" } else { "undo" }
                ),
            )?);
            if suspended {
                jobs.push(NewJob::new_json(
                    JobKind::UnfollowAll,
                    &UnfollowAllPayload {
                        account_id: account_id.to_string(),
                        after: None,
                    },
                    format!("sweep:unfollow-all:{account_id}"),
                    self.now_ms(),
                )?);
            }
        }
        self.moderation.set_suspended(account_id, suspended, log, jobs)
    }

    /// `JobKind::UnfollowAll` handler (Batch M WP3): silently remove one
    /// bounded batch ([`UNFOLLOW_ALL_PAGE`]) of `account_id`'s outgoing
    /// follows, upstream `UserSuspendService.unFollowAll` +
    /// `UserFollowingService.unfollow(silent: true)` semantics. Each row
    /// deletion commits in ONE transaction with its own `Undo(Follow)`
    /// delivery job (remote followees only; local followees just lose the
    /// edge); no `unfollow` streaming event and no notification (`silent`).
    ///
    /// Returns the continuation job while more rows may remain (`None` =
    /// done). Crash-safe: a re-run sees the removed rows gone and the
    /// `after` cursor advances over skipped rows (a dangling edge whose
    /// followee account is gone is skipped and never loops the walk).
    /// Deterministic activity ids: a retry mints no second `Undo(Follow)`
    /// for the same edge.
    pub fn handle_unfollow_all(
        &self,
        payload_bytes: &[u8],
    ) -> Result<Option<NewJob>, PortError> {
        let payload: UnfollowAllPayload = serde_json::from_slice(payload_bytes)
            .map_err(|e| PortError::Validation(format!("unfollow-all payload invalid: {e}")))?;
        let UnfollowAllPayload { account_id, after } = payload;
        let batch = self
            .follows
            .following_of(&account_id, after.as_deref(), UNFOLLOW_ALL_PAGE)?;
        let done = batch.len() < UNFOLLOW_ALL_PAGE;
        let mut next_after = after;
        for follow in &batch {
            next_after = Some(follow.followee_id.clone());
            let mut jobs = Vec::new();
            if let Some(followee) = self.accounts.find_by_id(&follow.followee_id)? {
                if !followee.is_local() {
                    if let Some(inbox_url) = self.remote_inbox_for_account(&followee) {
                        let undo_id = format!("suspend-unfollow-{}", follow.id);
                        jobs.push(
                            FederateDeliverPayload::Unfollow {
                                undo_id: undo_id.clone(),
                                follow_id: follow.id.clone(),
                                follower_id: account_id.clone(),
                                followee_id: follow.followee_id.clone(),
                                inbox_url,
                            }
                            .into_job(
                                format!(
                                    "federate:unfollow:{account_id}:{}:{undo_id}",
                                    follow.followee_id
                                ),
                                self.now_ms(),
                            )?,
                        );
                    }
                }
            }
            match self.follows.unfollow(&account_id, &follow.followee_id, jobs) {
                Ok(()) => {}
                // A concurrently removed edge converges quietly; a gone
                // followee leaves the dangling row to the cursor (skipped,
                // never re-queued forever).
                Err(PortError::Validation(_) | PortError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        if done {
            return Ok(None);
        }
        // Key: account + cursor, stable and traceable (keys do not dedupe;
        // see `JobQueue::enqueue_jobs`).
        let key = format!(
            "sweep:unfollow-all:{account_id}:{}",
            next_after.as_deref().unwrap_or("")
        );
        let continuation = NewJob::new_json(
            JobKind::UnfollowAll,
            &UnfollowAllPayload {
                account_id,
                after: next_after,
            },
            key,
            self.now_ms(),
        )?;
        Ok(Some(continuation))
    }

    /// `admin/show-moderation-logs` read path: newest-first, cursor-paged,
    /// filtered (upstream `type`/`userId`/`search` params).
    pub fn list_moderation_logs(
        &self,
        filter: ModerationLogFilter,
        cursor: &TimelineCursor,
    ) -> Result<Vec<ModerationLogEntry>, PortError> {
        self.moderation.list_logs(&filter, cursor)
    }

    /// `admin/update-meta` subset (Batch M WP2): `disableRegistration`
    /// (maps to [`RegistrationPolicy`]) and `blockedHosts` (normalized:
    /// lowercased, trimmed, deduped, empties dropped — enforced from WP5,
    /// stored from here). The settings write and its
    /// `updateServerSettings` row commit in one transaction (§2.3); the
    /// row carries upstream's `{before, after}` payload over the changed
    /// fields (`admin/update-meta.ts:801`).
    pub fn update_instance_settings(
        &self,
        actor: &str,
        disable_registration: Option<bool>,
        blocked_hosts: Option<Vec<String>>,
        now_ms: i64,
    ) -> Result<InstanceSettings, PortError> {
        let current = self.instance_settings.get()?;
        let mut patch = InstanceSettingsPatch::default();
        if let Some(disabled) = disable_registration {
            patch.registration = Some(RegistrationPolicy::from_disable_registration(disabled));
        }
        if let Some(hosts) = blocked_hosts {
            patch.blocked_hosts = Some(InstanceSettings::normalize_blocked_hosts(&hosts));
        }
        let next_registration = patch.registration.unwrap_or(current.registration);
        let next_blocked_hosts = patch
            .blocked_hosts
            .clone()
            .unwrap_or_else(|| current.blocked_hosts.clone());
        let log = ModerationLogEntry::new(
            self.ids.generate(),
            Some(actor.to_string()),
            ModerationAction::UpdateServerSettings,
            None,
            serde_json::json!({
                "before": {
                    "disableRegistration": current.registration.disable_registration(),
                    "blockedHosts": current.blocked_hosts,
                },
                "after": {
                    "disableRegistration": next_registration.disable_registration(),
                    "blockedHosts": next_blocked_hosts,
                },
            }),
            now_ms,
        )?;
        self.instance_settings.update(patch, log)
    }
}
