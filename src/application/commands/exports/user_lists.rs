use crate::application::commands::AppServices;

use super::common::ImportRowOutcome;
use super::common::parse_acct_cell;
use super::common::skip_as_skipped;
use crate::application::ports::PortError;

impl AppServices {
    /// Apply user-list import rows (`listName,acct[,key=value]`, upstream
    /// `ImportUserListsProcessorService` parity): find-or-create the
    /// caller's list by name, resolve the acct locally (remote/unresolvable
    /// rows skip — no remote resolver), skip duplicates, apply
    /// `withReplies=`. Per-row try/catch parity: only storage faults abort
    /// (already-applied rows converge on conflicts on replay).
    pub(crate) fn apply_user_list_import(
        &self,
        user_id: &str,
        text: &str,
    ) -> Result<(usize, usize), PortError> {
        let mut applied = 0usize;
        let mut skipped = 0usize;
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let outcome = self.apply_user_list_row(user_id, line);
            match outcome {
                Ok(()) => applied += 1,
                Err(ImportRowOutcome::Skipped) => skipped += 1,
                Err(ImportRowOutcome::Abort(err)) => return Err(err),
            }
        }
        Ok((applied, skipped))
    }

    fn apply_user_list_row(&self, user_id: &str, line: &str) -> Result<(), ImportRowOutcome> {
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() < 2 || parts[0].trim().is_empty() {
            return Err(ImportRowOutcome::Skipped);
        }
        let list_name = parts[0].trim().to_string();
        let acct = parse_acct_cell(parts[1].trim()).ok_or(ImportRowOutcome::Skipped)?;
        let mut with_replies = false;
        for kv in parts.iter().skip(2) {
            let kv = kv.trim();
            if let Some(value) = kv.strip_prefix("withReplies=") {
                with_replies = value.trim() == "true";
            }
        }
        // Find-or-create the caller's list by name (upstream parity; names
        // are not unique, first match wins).
        let list = match self
            .lists
            .lists_by_owner(user_id)
            .map_err(skip_as_skipped)?
            .into_iter()
            .find(|l| l.name == list_name)
        {
            Some(list) => list,
            None => self
                .create_user_list(user_id, list_name)
                .map_err(skip_as_skipped)?,
        };
        let target_id = self
            .resolve_acct(&Some(acct))
            .map_err(skip_as_skipped)?
            .ok_or(ImportRowOutcome::Skipped)?;
        let target = self
            .accounts
            .find_by_id(&target_id)
            .map_err(skip_as_skipped)?
            .ok_or(ImportRowOutcome::Skipped)?;
        if target.id != user_id
            && self
                .blocks
                .is_blocking(&target.id, user_id)
                .map_err(skip_as_skipped)?
        {
            return Err(ImportRowOutcome::Skipped);
        }
        let member = crate::domain::user_list::UserListMembership {
            id: self.ids.generate(),
            list_id: list.id.clone(),
            member_id: target.id.clone(),
            with_replies,
            created_at_ms: self.now_ms(),
        };
        let stream_job = self
            .streaming_job(
                &crate::application::events::StreamingEvent::UserListMemberAdded {
                    list_id: list.id.clone(),
                    member_id: target.id.clone(),
                },
                format!(
                    "list-import:{}:{}:{}",
                    list.id,
                    target.id,
                    self.ids.generate()
                ),
            )
            .map_err(skip_as_skipped)?;
        // The op's full push (remote Follow + error mapping) is endpoint
        // shaped; the worker path inserts directly so a remote member's
        // Follow is still queued below.
        match self.lists.add_list_member(member, vec![stream_job]) {
            Ok(_) => {}
            Err(e @ PortError::StorageUnavailable(_)) => {
                return Err(ImportRowOutcome::Abort(e));
            }
            Err(_) => return Err(ImportRowOutcome::Skipped),
        }
        if !target.is_local() {
            if let Ok((proxy, _)) = self.ensure_system_actor() {
                if let Some(inbox_url) = crate::federation::outbox::preferred_inbox(&target) {
                    let follow_id = self.ids.generate();
                    let job = crate::federation::outbox::FederateDeliverPayload::Follow {
                        follow_id: follow_id.clone(),
                        follower_id: proxy.id,
                        followee_id: target.id.clone(),
                        inbox_url,
                    }
                    .into_job(
                        format!("federate:list-import-follow:{}:{follow_id}", list.id),
                        self.now_ms(),
                    )
                    .map_err(skip_as_skipped)?;
                    self.jobs.enqueue_jobs(vec![job]).map_err(skip_as_skipped)?;
                }
            }
        }
        Ok(())
    }
}
