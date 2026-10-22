use super::codec::{decode_json, encode_json, map_redb_err};
use super::store::{CREDENTIALS, RedbStore};
use crate::application::ports::{
    ActorKeypairRecord, ApiTokenRecord, AuthStore, LegacyAuthSessionRecord,
    MAX_SIGNIN_ROWS_PER_USER, MiAuthSessionRecord, PasswordResetRequest, PasswordResetStore,
    PortError, RemotePublicKeyRecord, SigninRecord, SigninStore,
};
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};

pub(crate) const API_TOKENS: TableDefinition<&str, &[u8]> = TableDefinition::new("api_tokens");
/// Token-id index (Batch O WP9): opaque token id → token hash. Rows minted
/// before ids existed are backfilled at open; every stored row resolves.
pub(crate) const API_TOKENS_BY_ID: TableDefinition<&str, &str> =
    TableDefinition::new("api_tokens_by_id");
/// Signin attempt rows (Batch O WP9; Misskey `MiSignin`), keyed by row id
/// (aidx, time-ordered → reverse range reads newest-first).
pub(crate) const SIGNINS: TableDefinition<&str, &[u8]> = TableDefinition::new("signins");
/// Signin rows by `(account_id, signin_id)` (history read + per-user trim).
pub(crate) const SIGNINS_BY_USER: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("signins_by_user");
pub(crate) const MIAUTH_SESSIONS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("miauth_sessions");
pub(crate) const LEGACY_SESSIONS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("legacy_sessions");
/// Password-reset request rows (Batch P; Misskey `MiPasswordResetRequest`),
/// keyed by token (the lookup path) …
pub(crate) const PASSWORD_RESETS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("password_resets");
/// … plus id → token so `delete_reset_request(id)` needs no scan.
pub(crate) const PASSWORD_RESET_IDS: TableDefinition<&str, &str> =
    TableDefinition::new("password_reset_ids");
pub(crate) const ACTOR_KEYPAIRS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("actor_keypairs");
pub(crate) const REMOTE_PUBLIC_KEYS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("remote_public_keys");
impl AuthStore for RedbStore {
    fn set_password_hash(&self, account_id: &str, password_hash: &str) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(CREDENTIALS).map_err(map_redb_err)?;
            table
                .insert(account_id, password_hash)
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn get_password_hash(&self, account_id: &str) -> Result<Option<String>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(CREDENTIALS).map_err(map_redb_err)?;
        match table.get(account_id).map_err(map_redb_err)? {
            Some(g) => Ok(Some(g.value().to_string())),
            None => Ok(None),
        }
    }

    fn store_token(&self, record: ApiTokenRecord) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(API_TOKENS).map_err(map_redb_err)?;
            let encoded = encode_json(&record)?;
            table
                .insert(record.token_hash.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
            if !record.id.is_empty() {
                let mut by_id = write_txn
                    .open_table(API_TOKENS_BY_ID)
                    .map_err(map_redb_err)?;
                by_id
                    .insert(record.id.as_str(), record.token_hash.as_str())
                    .map_err(map_redb_err)?;
            }
        }
        self.commit_write(write_txn)
    }

    fn get_token_by_hash(&self, token_hash: &str) -> Result<Option<ApiTokenRecord>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(API_TOKENS).map_err(map_redb_err)?;
        match table.get(token_hash).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn get_token_by_id(&self, id: &str) -> Result<Option<ApiTokenRecord>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_id = read_txn
            .open_table(API_TOKENS_BY_ID)
            .map_err(map_redb_err)?;
        let hash: Option<String> = match by_id.get(id).map_err(map_redb_err)? {
            Some(g) => Some(g.value().to_string()),
            None => None,
        };
        drop(by_id);
        match hash {
            Some(hash) => {
                let table = read_txn.open_table(API_TOKENS).map_err(map_redb_err)?;
                match table.get(hash.as_str()).map_err(map_redb_err)? {
                    Some(g) => Ok(Some(decode_json(g.value())?)),
                    None => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    fn tokens_for_account(&self, account_id: &str) -> Result<Vec<ApiTokenRecord>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(API_TOKENS).map_err(map_redb_err)?;
        let mut out = Vec::new();
        for row in table.iter().map_err(map_redb_err)? {
            let (_, v) = row.map_err(map_redb_err)?;
            let record: ApiTokenRecord = decode_json(v.value())?;
            if record.account_id == account_id {
                out.push(record);
            }
        }
        out.sort_by(|a, b| {
            a.created_at_ms
                .cmp(&b.created_at_ms)
                .then_with(|| a.id.cmp(&b.id))
        });
        Ok(out)
    }

    fn delete_account_first_party_tokens(&self, account_id: &str) -> Result<u64, PortError> {
        let write_txn = self.begin_write()?;
        let removed = {
            let mut table = write_txn.open_table(API_TOKENS).map_err(map_redb_err)?;
            let mut by_id = write_txn
                .open_table(API_TOKENS_BY_ID)
                .map_err(map_redb_err)?;
            let mut doomed: Vec<(String, String)> = Vec::new();
            for row in table.iter().map_err(map_redb_err)? {
                let (k, v) = row.map_err(map_redb_err)?;
                let record: ApiTokenRecord = decode_json(v.value())?;
                if record.account_id == account_id && record.is_first_party {
                    doomed.push((k.value().to_string(), record.id));
                }
            }
            let count = doomed.len() as u64;
            for (hash, id) in doomed {
                table.remove(hash.as_str()).map_err(map_redb_err)?;
                if !id.is_empty() {
                    by_id.remove(id.as_str()).map_err(map_redb_err)?;
                }
            }
            count
        };
        self.commit_write(write_txn)?;
        Ok(removed)
    }

    fn delete_token_by_hash(&self, token_hash: &str) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(API_TOKENS).map_err(map_redb_err)?;
            let id: Option<String> = match table.get(token_hash).map_err(map_redb_err)? {
                Some(g) => {
                    let record: ApiTokenRecord = decode_json(g.value())?;
                    if record.id.is_empty() {
                        None
                    } else {
                        Some(record.id)
                    }
                }
                None => None,
            };
            table.remove(token_hash).map_err(map_redb_err)?;
            if let Some(id) = id {
                let mut by_id = write_txn
                    .open_table(API_TOKENS_BY_ID)
                    .map_err(map_redb_err)?;
                by_id.remove(id.as_str()).map_err(map_redb_err)?;
            }
        }
        self.commit_write(write_txn)
    }

    fn save_miauth_session(&self, session: MiAuthSessionRecord) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn
                .open_table(MIAUTH_SESSIONS)
                .map_err(map_redb_err)?;
            let encoded = encode_json(&session)?;
            table
                .insert(session.session.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn get_miauth_session(
        &self,
        session_id: &str,
    ) -> Result<Option<MiAuthSessionRecord>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(MIAUTH_SESSIONS).map_err(map_redb_err)?;
        let sess_opt: Option<MiAuthSessionRecord> =
            match table.get(session_id).map_err(map_redb_err)? {
                Some(g) => Some(decode_json(g.value())?),
                None => None,
            };
        drop(table);
        drop(read_txn);
        // Batch N WS7: expired rows read as missing (deleted on read, so a
        // re-poll after the approval window converges to `ok: false`).
        if let Some(sess) = sess_opt {
            if sess
                .expires_at_ms
                .is_some_and(|exp| exp <= self.clock.now_ms())
            {
                let write_txn = self.begin_write()?;
                {
                    let mut table = write_txn
                        .open_table(MIAUTH_SESSIONS)
                        .map_err(map_redb_err)?;
                    let _ = table.remove(session_id);
                }
                self.commit_write(write_txn)?;
                return Ok(None);
            }
            return Ok(Some(sess));
        }
        Ok(None)
    }

    fn consume_miauth_session(
        &self,
        session_id: &str,
    ) -> Result<Option<MiAuthSessionRecord>, PortError> {
        let write_txn = self.begin_write()?;
        let consumed = {
            let mut table = write_txn
                .open_table(MIAUTH_SESSIONS)
                .map_err(map_redb_err)?;
            let sess_opt: Option<MiAuthSessionRecord> =
                match table.get(session_id).map_err(map_redb_err)? {
                    Some(g) => Some(decode_json(g.value())?),
                    None => None,
                };
            match sess_opt {
                // Expired rows are dropped, never vended (one-shot stays
                // one-shot; the poller sees `ok: false`).
                Some(sess)
                    if sess
                        .expires_at_ms
                        .is_some_and(|exp| exp <= self.clock.now_ms()) =>
                {
                    let _ = table.remove(session_id);
                    None
                }
                Some(mut sess)
                    if !sess.fetched && sess.token.is_some() && sess.account_id.is_some() =>
                {
                    sess.fetched = true;
                    let encoded = encode_json(&sess)?;
                    table
                        .insert(session_id, encoded.as_slice())
                        .map_err(map_redb_err)?;
                    Some(sess)
                }
                _ => None,
            }
        };
        self.commit_write(write_txn)?;
        Ok(consumed)
    }

    fn save_legacy_session(&self, session: LegacyAuthSessionRecord) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn
                .open_table(LEGACY_SESSIONS)
                .map_err(map_redb_err)?;
            let encoded = encode_json(&session)?;
            table
                .insert(session.token.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn get_legacy_session(
        &self,
        token: &str,
    ) -> Result<Option<LegacyAuthSessionRecord>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(LEGACY_SESSIONS).map_err(map_redb_err)?;
        let sess_opt: Option<LegacyAuthSessionRecord> =
            match table.get(token).map_err(map_redb_err)? {
                Some(g) => Some(decode_json(g.value())?),
                None => None,
            };
        drop(table);
        drop(read_txn);
        // Batch N WS7: expired rows read as missing (deleted on read).
        if let Some(sess) = sess_opt {
            if sess
                .expires_at_ms
                .is_some_and(|exp| exp <= self.clock.now_ms())
            {
                let write_txn = self.begin_write()?;
                {
                    let mut table = write_txn
                        .open_table(LEGACY_SESSIONS)
                        .map_err(map_redb_err)?;
                    let _ = table.remove(token);
                }
                self.commit_write(write_txn)?;
                return Ok(None);
            }
            return Ok(Some(sess));
        }
        Ok(None)
    }

    fn take_legacy_session(
        &self,
        token: &str,
    ) -> Result<Option<LegacyAuthSessionRecord>, PortError> {
        let write_txn = self.begin_write()?;
        let removed = {
            let mut table = write_txn
                .open_table(LEGACY_SESSIONS)
                .map_err(map_redb_err)?;
            let existing: Option<LegacyAuthSessionRecord> =
                match table.get(token).map_err(map_redb_err)? {
                    Some(g) => Some(decode_json(g.value())?),
                    None => None,
                };
            match existing {
                // Expired rows are dropped, never vended (Batch N WS7).
                Some(s)
                    if s.expires_at_ms
                        .is_some_and(|exp| exp <= self.clock.now_ms()) =>
                {
                    let _ = table.remove(token);
                    None
                }
                Some(s) if s.account_id.is_some() && s.access_token.is_some() => {
                    table.remove(token).map_err(map_redb_err)?;
                    Some(s)
                }
                Some(s) => Some(s),
                None => None,
            }
        };
        self.commit_write(write_txn)?;
        Ok(removed)
    }

    fn get_actor_keypair(&self, account_id: &str) -> Result<Option<ActorKeypairRecord>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(ACTOR_KEYPAIRS).map_err(map_redb_err)?;
        match table.get(account_id).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn save_actor_keypair(&self, record: ActorKeypairRecord) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(ACTOR_KEYPAIRS).map_err(map_redb_err)?;
            let encoded = encode_json(&record)?;
            table
                .insert(record.account_id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn save_actor_keypair_if_absent(
        &self,
        record: ActorKeypairRecord,
    ) -> Result<ActorKeypairRecord, PortError> {
        // redb serialises write transactions, so the check and the insert are one atomic step.
        let write_txn = self.begin_write()?;
        let stored = {
            let mut table = write_txn.open_table(ACTOR_KEYPAIRS).map_err(map_redb_err)?;
            let existing: Option<ActorKeypairRecord> = match table
                .get(record.account_id.as_str())
                .map_err(map_redb_err)?
            {
                Some(g) => Some(decode_json(g.value())?),
                None => None,
            };
            match existing {
                Some(existing) => existing,
                None => {
                    let encoded = encode_json(&record)?;
                    table
                        .insert(record.account_id.as_str(), encoded.as_slice())
                        .map_err(map_redb_err)?;
                    record
                }
            }
        };
        self.commit_write(write_txn)?;
        Ok(stored)
    }

    fn get_remote_public_key(
        &self,
        key_id: &str,
    ) -> Result<Option<RemotePublicKeyRecord>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn
            .open_table(REMOTE_PUBLIC_KEYS)
            .map_err(map_redb_err)?;
        match table.get(key_id).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn save_remote_public_key(&self, record: RemotePublicKeyRecord) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn
                .open_table(REMOTE_PUBLIC_KEYS)
                .map_err(map_redb_err)?;
            let encoded = encode_json(&record)?;
            table
                .insert(record.key_id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }
}

impl SigninStore for RedbStore {
    fn record_signin(&self, record: SigninRecord) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut signins = write_txn.open_table(SIGNINS).map_err(map_redb_err)?;
            let mut by_user = write_txn
                .open_table(SIGNINS_BY_USER)
                .map_err(map_redb_err)?;
            let encoded = encode_json(&record)?;
            signins
                .insert(record.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
            by_user
                .insert((record.account_id.as_str(), record.id.as_str()), &1u8)
                .map_err(map_redb_err)?;
            // Trim oldest-first to the per-user cap in the same transaction.
            let owned: Vec<String> = {
                let mut ids = Vec::new();
                for entry in by_user.iter().map_err(map_redb_err)? {
                    let (k, _) = entry.map_err(map_redb_err)?;
                    let (owner, sid) = k.value();
                    if owner == record.account_id.as_str() {
                        ids.push(sid.to_string());
                    }
                }
                ids
            };
            if owned.len() > MAX_SIGNIN_ROWS_PER_USER {
                // aidx ids are time-ordered: ascending = oldest-first.
                let mut sorted = owned;
                sorted.sort();
                for drop_id in sorted.iter().take(sorted.len() - MAX_SIGNIN_ROWS_PER_USER) {
                    by_user
                        .remove((record.account_id.as_str(), drop_id.as_str()))
                        .map_err(map_redb_err)?;
                    signins.remove(drop_id.as_str()).map_err(map_redb_err)?;
                }
            }
        }
        self.commit_write(write_txn)
    }

    fn list_signins(
        &self,
        account_id: &str,
        limit: usize,
        since_id: Option<&str>,
        until_id: Option<&str>,
        since_date: Option<i64>,
        until_date: Option<i64>,
    ) -> Result<Vec<SigninRecord>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let signins = read_txn.open_table(SIGNINS).map_err(map_redb_err)?;
        let by_user = read_txn.open_table(SIGNINS_BY_USER).map_err(map_redb_err)?;
        let mut rows: Vec<SigninRecord> = Vec::new();
        for entry in by_user.iter().map_err(map_redb_err)? {
            let (k, _) = entry.map_err(map_redb_err)?;
            let (owner, sid) = k.value();
            if owner != account_id {
                continue;
            }
            if let Some(since) = since_id {
                if sid <= since {
                    continue;
                }
            }
            if let Some(until) = until_id {
                if sid >= until {
                    continue;
                }
            }
            let Some(g) = signins.get(sid).map_err(map_redb_err)? else {
                continue;
            };
            let record: SigninRecord = decode_json(g.value())?;
            if let Some(since_ms) = since_date {
                if record.created_at_ms <= since_ms {
                    continue;
                }
            }
            if let Some(until_ms) = until_date {
                if record.created_at_ms >= until_ms {
                    continue;
                }
            }
            rows.push(record);
        }
        // Newest-first (aidx ids descending = creation order).
        rows.sort_by(|a, b| b.id.cmp(&a.id));
        rows.truncate(limit);
        Ok(rows)
    }
}

// --- Batch P: password-reset + registry stores ------------------------------

impl PasswordResetStore for RedbStore {
    fn create_reset_request(&self, request: PasswordResetRequest) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut by_token = write_txn
                .open_table(PASSWORD_RESETS)
                .map_err(map_redb_err)?;
            let mut by_id = write_txn
                .open_table(PASSWORD_RESET_IDS)
                .map_err(map_redb_err)?;
            let encoded = encode_json(&request)?;
            by_token
                .insert(request.token.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
            by_id
                .insert(request.id.as_str(), request.token.as_str())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn find_reset_request(&self, token: &str) -> Result<Option<PasswordResetRequest>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_token = read_txn.open_table(PASSWORD_RESETS).map_err(map_redb_err)?;
        let Some(g) = by_token.get(token).map_err(map_redb_err)? else {
            return Ok(None);
        };
        Ok(Some(decode_json(g.value())?))
    }

    fn delete_reset_request(&self, id: &str) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut by_token = write_txn
                .open_table(PASSWORD_RESETS)
                .map_err(map_redb_err)?;
            let mut by_id = write_txn
                .open_table(PASSWORD_RESET_IDS)
                .map_err(map_redb_err)?;
            let token: Option<String> = by_id
                .get(id)
                .map_err(map_redb_err)?
                .map(|g| g.value().to_string());
            if let Some(token) = token {
                by_token.remove(token.as_str()).map_err(map_redb_err)?;
                by_id.remove(id).map_err(map_redb_err)?;
            }
        }
        self.commit_write(write_txn)
    }
}
