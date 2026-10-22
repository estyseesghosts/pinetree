use super::codec::{decode_json, encode_json, map_redb_err};
use super::store::{CREDENTIALS, RedbStore};
use crate::application::ports::{
    ConsumeInviteInput, CreateInviteInput, InviteListFilter, InviteSort, InviteStore,
    InviteTypeFilter, PortError,
};
use crate::domain::account::Account;
use crate::domain::instance::InviteCode;
use crate::domain::moderation::ModerationLogEntry;
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};

/// Invite codes (Batch M WP2; upstream `RegistrationTicket`), keyed by
/// ticket id (aidx, time-ordered → reverse range reads newest-first).
pub(crate) const INVITES: TableDefinition<&str, &[u8]> = TableDefinition::new("invites");
/// Unique invite-code index (upstream `RegistrationTicket.code` unique
/// column): code → ticket id. A row present here means the code is
/// unconsumed; `consume_and_create_account` removes it atomically.
pub(crate) const INVITE_CODES: TableDefinition<&str, &str> = TableDefinition::new("invite_codes");
/// Row filter shared by the invite listings (keyset + `admin/invite/list`
/// `type` + creator scope).
fn matches_invite_filter(invite: &InviteCode, filter: &InviteListFilter, now_ms: i64) -> bool {
    if let Some(creator) = &filter.created_by {
        if invite.created_by.as_deref() != Some(creator.as_str()) {
            return false;
        }
    }
    match filter.filter {
        InviteTypeFilter::All => {}
        InviteTypeFilter::Unused => {
            if invite.is_used() {
                return false;
            }
        }
        InviteTypeFilter::Used => {
            if !invite.is_used() {
                return false;
            }
        }
        InviteTypeFilter::Expired => {
            if !invite.is_expired(now_ms) {
                return false;
            }
        }
    }
    if let Some(since_id) = &filter.since_id {
        if invite.id.as_str() <= since_id.as_str() {
            return false;
        }
    }
    if let Some(until_id) = &filter.until_id {
        if invite.id.as_str() >= until_id.as_str() {
            return false;
        }
    }
    if let Some(since_date_ms) = filter.since_date_ms {
        if invite.created_at_ms <= since_date_ms {
            return false;
        }
    }
    if let Some(until_date_ms) = filter.until_date_ms {
        if invite.created_at_ms >= until_date_ms {
            return false;
        }
    }
    true
}

impl InviteStore for RedbStore {
    fn create_invites(
        &self,
        inputs: &[CreateInviteInput],
        log: Option<&ModerationLogEntry>,
    ) -> Result<Vec<InviteCode>, PortError> {
        if inputs.is_empty() || inputs.len() > 100 {
            return Err(PortError::Validation(
                "INVALID_PARAM invite count must be 1..=100".to_string(),
            ));
        }
        let now_ms = self.clock.now_ms();
        let write_txn = self.begin_write()?;
        let mut created = Vec::with_capacity(inputs.len());
        {
            let mut invites_table = write_txn.open_table(INVITES).map_err(map_redb_err)?;
            let mut codes_table = write_txn.open_table(INVITE_CODES).map_err(map_redb_err)?;
            for input in inputs {
                let id = self.ids.generate();
                let invite = InviteCode::new(
                    id.clone(),
                    input.code.clone(),
                    input.created_by.clone(),
                    now_ms,
                    input.expires_at_ms,
                )?;
                if codes_table
                    .get(invite.code.as_str())
                    .map_err(map_redb_err)?
                    .is_some()
                {
                    return Err(PortError::Conflict(format!(
                        "invite code {} already exists",
                        invite.code
                    )));
                }
                let bytes = encode_json(&invite)?;
                invites_table
                    .insert(id.as_str(), bytes.as_slice())
                    .map_err(map_redb_err)?;
                codes_table
                    .insert(invite.code.as_str(), id.as_str())
                    .map_err(map_redb_err)?;
                created.push(invite);
            }
        }
        if let Some(entry) = log {
            self.append_log_in_txn(&write_txn, entry)?;
        }
        self.commit_write(write_txn)?;
        Ok(created)
    }

    fn find_invite_by_id(&self, id: &str) -> Result<Option<InviteCode>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(INVITES).map_err(map_redb_err)?;
        match table.get(id).map_err(map_redb_err)? {
            Some(guard) => Ok(Some(decode_json(guard.value())?)),
            None => Ok(None),
        }
    }

    fn find_invite_by_code(&self, code: &str) -> Result<Option<InviteCode>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let codes_table = read_txn.open_table(INVITE_CODES).map_err(map_redb_err)?;
        let Some(id_guard) = codes_table.get(code).map_err(map_redb_err)? else {
            // Consumed or never issued: the unique-index row is the source
            // of truth for "still claimable".
            return Ok(None);
        };
        let id = id_guard.value().to_string();
        drop(id_guard);
        let table = read_txn.open_table(INVITES).map_err(map_redb_err)?;
        match table.get(id.as_str()).map_err(map_redb_err)? {
            Some(guard) => Ok(Some(decode_json(guard.value())?)),
            None => Ok(None),
        }
    }

    fn list_invites(&self, filter: &InviteListFilter) -> Result<Vec<InviteCode>, PortError> {
        if filter.limit == 0 {
            return Ok(Vec::new());
        }
        // Bounded scan (conformance F2 parity): filtered listings stop
        // after this many rows even when more exist.
        const SCAN_CAP: usize = 10_000;
        let now_ms = self.clock.now_ms();
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(INVITES).map_err(map_redb_err)?;
        let mut out: Vec<InviteCode> = Vec::new();
        let mut scanned = 0usize;
        match filter.sort {
            // Id order == created order (aidx); scan in the requested
            // direction so a bounded scan sees the right end first.
            InviteSort::CreatedAsc => {
                for entry in table.range(""..).map_err(map_redb_err)? {
                    scanned += 1;
                    if scanned > SCAN_CAP {
                        break;
                    }
                    let (_, v) = entry.map_err(map_redb_err)?;
                    let invite: InviteCode = decode_json(v.value())?;
                    if matches_invite_filter(&invite, filter, now_ms) {
                        out.push(invite);
                    }
                }
            }
            InviteSort::CreatedDesc => {
                for entry in table.range(""..).map_err(map_redb_err)?.rev() {
                    scanned += 1;
                    if scanned > SCAN_CAP {
                        break;
                    }
                    let (_, v) = entry.map_err(map_redb_err)?;
                    let invite: InviteCode = decode_json(v.value())?;
                    if matches_invite_filter(&invite, filter, now_ms) {
                        out.push(invite);
                    }
                }
            }
            InviteSort::UsedAsc | InviteSort::UsedDesc => {
                for entry in table.range(""..).map_err(map_redb_err)?.rev() {
                    scanned += 1;
                    if scanned > SCAN_CAP {
                        break;
                    }
                    let (_, v) = entry.map_err(map_redb_err)?;
                    let invite: InviteCode = decode_json(v.value())?;
                    if matches_invite_filter(&invite, filter, now_ms) {
                        out.push(invite);
                    }
                }
                out.sort_by(|a, b| {
                    // `+usedAt` = used_at DESC with unused LAST
                    // (`NULLS LAST`); `-usedAt` = used_at ASC with unused
                    // FIRST (`NULLS FIRST`). Option ordering already puts
                    // None first ascending, so only the descending case
                    // inverts. Id breaks ties deterministically.
                    let by_used = match filter.sort {
                        InviteSort::UsedDesc => b.used_at_ms.cmp(&a.used_at_ms),
                        _ => a.used_at_ms.cmp(&b.used_at_ms),
                    };
                    by_used.then_with(|| a.id.cmp(&b.id))
                });
            }
        }
        Ok(out
            .into_iter()
            .skip(filter.offset)
            .take(filter.limit)
            .collect())
    }

    fn delete_invite(&self, id: &str, log: Option<&ModerationLogEntry>) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        let code = {
            let mut invites_table = write_txn.open_table(INVITES).map_err(map_redb_err)?;
            let Some(guard) = invites_table.get(id).map_err(map_redb_err)? else {
                return Err(PortError::NotFound("NO_SUCH_INVITE_CODE".to_string()));
            };
            let invite: InviteCode = decode_json(guard.value())?;
            drop(guard);
            invites_table.remove(id).map_err(map_redb_err)?;
            invite.code
        };
        {
            let mut codes_table = write_txn.open_table(INVITE_CODES).map_err(map_redb_err)?;
            codes_table.remove(code.as_str()).map_err(map_redb_err)?;
        }
        if let Some(entry) = log {
            self.append_log_in_txn(&write_txn, entry)?;
        }
        self.commit_write(write_txn)
    }

    fn consume_and_create_account(
        &self,
        input: ConsumeInviteInput,
    ) -> Result<(Account, InviteCode), PortError> {
        let account = self.build_account(input.account)?;
        let now_ms = self.clock.now_ms();
        // One transaction: the claim (mark used + drop the unique-index
        // row) and the account creation commit together or not at all
        // (conformance I1/I2). A failure here (bad username, duplicate
        // handle, injected fault) aborts everything below.
        let write_txn = self.begin_write()?;
        let consumed = {
            let mut codes_table = write_txn.open_table(INVITE_CODES).map_err(map_redb_err)?;
            let mut invites_table = write_txn.open_table(INVITES).map_err(map_redb_err)?;
            let id = match codes_table.get(input.code.as_str()).map_err(map_redb_err)? {
                Some(guard) => guard.value().to_string(),
                None => return Err(PortError::NotFound("NO_SUCH_INVITE_CODE".to_string())),
            };
            let mut invite: InviteCode =
                match invites_table.get(id.as_str()).map_err(map_redb_err)? {
                    Some(guard) => decode_json(guard.value())?,
                    None => return Err(PortError::NotFound("NO_SUCH_INVITE_CODE".to_string())),
                };
            // Missing, used, and expired codes fail the same way upstream's
            // signup gate does (`SignupApiService`: `ticket == null ||
            // ticket.usedById != null` → 400).
            if invite.is_used() || invite.is_expired(now_ms) {
                return Err(PortError::NotFound("NO_SUCH_INVITE_CODE".to_string()));
            }
            invite.consume(&account.id, now_ms)?;
            let bytes = encode_json(&invite)?;
            invites_table
                .insert(id.as_str(), bytes.as_slice())
                .map_err(map_redb_err)?;
            codes_table
                .remove(input.code.as_str())
                .map_err(map_redb_err)?;
            invite
        };
        self.create_account_in_txn(&write_txn, &account, None)?;
        if let Some(hash) = &input.password_hash {
            let mut credentials = write_txn.open_table(CREDENTIALS).map_err(map_redb_err)?;
            credentials
                .insert(account.id.as_str(), hash.as_str())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)?;
        Ok((account, consumed))
    }
}
