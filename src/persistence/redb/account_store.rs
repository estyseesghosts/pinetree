use super::codec::{decode_json, encode_json, map_redb_err};
use super::store::{ACCOUNTS, COUNTER_ACCOUNTS_LOCAL, HANDLE_INDEX, RedbStore};
use crate::application::ports::{
    AccountStore, CreateAccountInput, NewJob, PortError, UpdateAccountPatch,
};
use crate::domain::account::{Account, validate_local_username};
use crate::domain::moderation::ModerationLogEntry;
use redb::{
    ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition, WriteTransaction,
};

pub(crate) const USED_USERNAMES: TableDefinition<&str, u8> = TableDefinition::new("used_usernames");
pub(crate) const ACCOUNT_OWNERS: TableDefinition<&str, &str> =
    TableDefinition::new("account_owners");
/// Unique index of accounts by canonical ActivityPub `uri` → account id
/// (WP5 federation). Written in the same transaction as the account row;
/// rebuildable derived state, backfilled at open for older databases.
/// The row is kept on soft-delete (scan parity: `find_by_uri` never
/// filtered on flags).
pub(crate) const ACCOUNTS_BY_URI: TableDefinition<&str, &str> =
    TableDefinition::new("accounts_by_uri");
/// Full-handle alias `handle` → account id (WP5 federation), where
/// `handle` is the lowercased username for local accounts and
/// `username@host` (both lowercased) for remote ones. Maintained in the
/// same transaction as the account row. `HANDLE_INDEX` stays the primary
/// `(username, host)` lookup (exact parity, untouched); this table serves
/// the WP5 full-handle actor-resolution path the parent wires next.
pub(crate) const ACCOUNTS_BY_HANDLE: TableDefinition<&str, &str> =
    TableDefinition::new("accounts_by_handle");
impl RedbStore {
    pub(crate) fn handle_key(username_lower: &str, host: Option<&str>) -> String {
        match host {
            Some(h) if !h.is_empty() => {
                format!("remote:{}:{}", h.to_ascii_lowercase(), username_lower)
            }
            _ => format!("local:{username_lower}"),
        }
    }

    /// Canonical full handle for `ACCOUNTS_BY_HANDLE`: the lowercased
    /// username for local accounts, `username@host` (both lowercased) for
    /// remote ones.
    pub(crate) fn canonical_handle(account: &Account) -> String {
        match account
            .host
            .as_deref()
            .map(str::trim)
            .filter(|h| !h.is_empty())
        {
            Some(host) => format!("{}@{}", account.username_lower, host.to_ascii_lowercase()),
            None => account.username_lower.clone(),
        }
    }

    pub(crate) fn update_account_in_txn(
        &self,
        write_txn: &WriteTransaction,
        id: &str,
        patch: UpdateAccountPatch,
        jobs: Vec<NewJob>,
    ) -> Result<Account, PortError> {
        for job in &jobs {
            job.validate()?;
        }
        let updated = {
            let mut accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
            let mut acc: Account = match accounts_table.get(id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound(format!("account {id} not found"))),
            };
            if let Some(v) = patch.display_name {
                acc.display_name = v;
            }
            if let Some(v) = patch.description {
                acc.description = v;
            }
            if let Some(v) = patch.followed_message {
                acc.followed_message = v;
            }
            if let Some(v) = patch.location {
                acc.location = v;
            }
            if let Some(v) = patch.birthday {
                acc.birthday = v;
            }
            if let Some(v) = patch.lang {
                acc.lang = v;
            }
            if let Some(v) = patch.avatar_id {
                acc.avatar_id = v;
            }
            if let Some(v) = patch.avatar_decorations {
                acc.avatar_decorations = v;
            }
            if let Some(v) = patch.banner_id {
                acc.banner_id = v;
            }
            if let Some(v) = patch.fields {
                acc.fields = v;
            }
            if let Some(v) = patch.is_locked {
                acc.is_locked = v;
            }
            if let Some(v) = patch.is_bot {
                acc.is_bot = v;
            }
            if let Some(v) = patch.is_cat {
                acc.is_cat = v;
            }
            if let Some(v) = patch.is_explorable {
                acc.is_explorable = v;
            }
            if let Some(v) = patch.hide_online_status {
                acc.hide_online_status = v;
            }
            if let Some(v) = patch.public_reactions {
                acc.public_reactions = v;
            }
            if let Some(v) = patch.careful_bot {
                acc.careful_bot = v;
            }
            if let Some(v) = patch.auto_accept_followed {
                acc.auto_accept_followed = v;
            }
            if let Some(v) = patch.follow_approval_local_seconds {
                acc.follow_approval_local_seconds = v;
            }
            if let Some(v) = patch.follow_approval_remote_seconds {
                acc.follow_approval_remote_seconds = v;
            }
            if let Some(v) = patch.no_crawle {
                acc.no_crawle = v;
            }
            if let Some(v) = patch.prevent_ai_learning {
                acc.prevent_ai_learning = v;
            }
            if let Some(v) = patch.require_signin_to_view_contents {
                acc.require_signin_to_view_contents = v;
            }
            if let Some(v) = patch.make_notes_followers_only_before {
                acc.make_notes_followers_only_before = v;
            }
            if let Some(v) = patch.make_notes_hidden_before {
                acc.make_notes_hidden_before = v;
            }
            if let Some(v) = patch.inject_featured_note {
                acc.inject_featured_note = v;
            }
            if let Some(v) = patch.receive_announcement_email {
                acc.receive_announcement_email = v;
            }
            if let Some(v) = patch.always_mark_nsfw {
                acc.always_mark_nsfw = v;
            }
            if let Some(v) = patch.auto_sensitive {
                acc.auto_sensitive = v;
            }
            if let Some(v) = patch.following_visibility {
                acc.following_visibility = v;
            }
            if let Some(v) = patch.followers_visibility {
                acc.followers_visibility = v;
            }
            if let Some(v) = patch.chat_scope {
                acc.chat_scope = v;
            }
            if let Some(v) = patch.pinned_page_id {
                acc.pinned_page_id = v;
            }
            if let Some(v) = patch.muted_words {
                acc.enable_word_mute = !v.is_empty();
                acc.muted_words = v;
            }
            if let Some(v) = patch.hard_muted_words {
                acc.hard_muted_words = v;
            }
            if let Some(v) = patch.muted_instances {
                acc.muted_instances = v;
            }
            if let Some(v) = patch.notification_recieve_config {
                acc.notification_recieve_config = v;
            }
            if let Some(v) = patch.email_notification_types {
                acc.email_notification_types = v;
            }
            if let Some(v) = patch.also_known_as {
                acc.also_known_as = v;
            }
            if let Some(v) = patch.reject_quotes {
                acc.reject_quotes = v;
            }
            if let Some(v) = patch.default_reaction {
                acc.default_reaction = v;
            }
            // DIVERGENCE (Batch N WP8b): per-user remote-emoji opt-in.
            if let Some(v) = patch.show_remote_emojis {
                acc.show_remote_emojis = v;
            }
            if let Some(v) = patch.moderator_note {
                acc.moderator_note = v;
            }
            // Batch P email identity loop (`i/update-email`, `verify-email`).
            if let Some(v) = patch.email {
                acc.email = v;
            }
            if let Some(v) = patch.email_verified {
                acc.email_verified = v;
            }
            if let Some(v) = patch.email_verify_code {
                acc.email_verify_code = v;
            }
            // Batch P `i/move`: moved marker is set once, never cleared.
            if let Some(v) = patch.moved_to_uri {
                acc.moved_to_uri = Some(v);
            }
            if let Some(v) = patch.moved_at_ms {
                acc.moved_at_ms = Some(v);
            }
            // `update.ts` resets `verifiedLinks` on every profile update;
            // re-verification happens off-request (see `verifyLink`).
            // Only applied when the caller sets the field (profile updates);
            // remote-actor refreshes leave it untouched.
            if let Some(v) = patch.verified_links {
                acc.verified_links = v;
            }
            acc.validate()?;
            let bytes = encode_json(&acc)?;
            accounts_table
                .insert(acc.id.as_str(), bytes.as_slice())
                .map_err(map_redb_err)?;
            acc
        };
        self.enqueue_jobs_in_txn(write_txn, jobs, None)?;
        Ok(updated)
    }

    /// Validate + construct one account row from `input` (no writes).
    /// The local-username and preserved-name rules run here so every
    /// creation path (CLI, signup, `admin/accounts/create`, invite claim)
    /// shares them.
    pub(crate) fn build_account(&self, input: CreateAccountInput) -> Result<Account, PortError> {
        let username = input.handle.trim().to_string();
        let username_lower = username.to_ascii_lowercase();
        let is_local = input.host.is_none();

        if is_local {
            validate_local_username(&username)?;
            for reserved in &input.preserved_usernames {
                if reserved.eq_ignore_ascii_case(&username_lower) {
                    return Err(PortError::Conflict(format!(
                        "username {username} is reserved"
                    )));
                }
            }
        }

        let account_id = self.ids.generate();
        let account = Account {
            id: account_id.clone(),
            username,
            username_lower: username_lower.clone(),
            host: input.host.clone(),
            display_name: input.display_name,
            description: input.description,
            location: input.location,
            birthday: input.birthday,
            uri: input.uri,
            inbox: input.inbox,
            shared_inbox: input.shared_inbox,
            followers_count: 0,
            following_count: 0,
            notes_count: 0,
            avatar_id: None,
            banner_id: None,
            is_locked: input.is_locked,
            is_bot: input.is_bot,
            is_suspended: false,
            is_admin: false,
            is_moderator: false,
            is_explorable: true,
            is_deleted: false,
            is_cat: input.is_cat,
            // §0B: fresh accounts start at the upstream column defaults.
            hide_online_status: false,
            avatar_decorations: Vec::new(),
            // Wave 6 atomicity: see `moved_to_uri` below.
            also_known_as: input.also_known_as,
            require_signin_to_view_contents: false,
            make_notes_followers_only_before: None,
            make_notes_hidden_before: None,
            chat_scope: crate::domain::account::ChatScope::default(),
            followed_message: None,
            lang: None,
            fields: Vec::new(),
            verified_links: Vec::new(),
            public_reactions: true,
            following_visibility: crate::domain::account::FollowVisibility::default(),
            followers_visibility: crate::domain::account::FollowVisibility::default(),
            auto_accept_followed: false,
            follow_approval_local_seconds: None,
            follow_approval_remote_seconds: None,
            no_crawle: false,
            prevent_ai_learning: true,
            always_mark_nsfw: false,
            auto_sensitive: false,
            careful_bot: false,
            inject_featured_note: true,
            receive_announcement_email: true,
            pinned_page_id: None,
            enable_word_mute: false,
            muted_words: Vec::new(),
            hard_muted_words: Vec::new(),
            muted_instances: Vec::new(),
            notification_recieve_config: std::collections::BTreeMap::new(),
            email_notification_types: vec![
                "follow".to_string(),
                "receiveFollowRequest".to_string(),
            ],
            reject_quotes: false,
            default_reaction: None,
            show_remote_emojis: false,
            email: None,
            email_verified: false,
            email_verify_code: None,
            // Wave 6 atomicity: handshake columns seed in the creating
            // commit (single txn with the row + indexes); the old
            // create-then-update window is gone.
            moved_to_uri: input.moved_to_uri,
            moved_at_ms: input.moved_at_ms,
            moderator_note: None,
        };
        account.validate()?;
        Ok(account)
    }

    /// Account uniqueness checks + row/index inserts (+ optional `log`)
    /// inside an open write transaction: the account row, its handle
    /// index, the owner mapping, and the moderation-log row commit
    /// together (§2.3).
    pub(crate) fn create_account_in_txn(
        &self,
        write_txn: &WriteTransaction,
        account: &Account,
        log: Option<&ModerationLogEntry>,
    ) -> Result<(), PortError> {
        let is_local = account.host.is_none();
        let username_lower = account.username_lower.as_str();
        let handle_key = Self::handle_key(&account.username_lower, account.host.as_deref());
        {
            let mut accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
            let mut handle_table = write_txn.open_table(HANDLE_INDEX).map_err(map_redb_err)?;
            let used_table = write_txn.open_table(USED_USERNAMES).map_err(map_redb_err)?;
            let mut owners_table = write_txn.open_table(ACCOUNT_OWNERS).map_err(map_redb_err)?;

            if is_local
                && used_table
                    .get(username_lower)
                    .map_err(map_redb_err)?
                    .is_some()
            {
                return Err(PortError::Conflict(format!(
                    "handle {username_lower} was previously used"
                )));
            }
            if handle_table
                .get(handle_key.as_str())
                .map_err(map_redb_err)?
                .is_some()
            {
                return Err(PortError::Conflict(format!(
                    "handle {username_lower} is already taken"
                )));
            }

            let encoded = encode_json(account)?;
            accounts_table
                .insert(account.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
            handle_table
                .insert(handle_key.as_str(), account.id.as_str())
                .map_err(map_redb_err)?;
            // WP5 federation: the URI + full-handle rows commit in the
            // same transaction as the account row.
            if let Some(uri) = account
                .uri
                .as_deref()
                .map(str::trim)
                .filter(|u| !u.is_empty())
            {
                let mut by_uri = write_txn
                    .open_table(ACCOUNTS_BY_URI)
                    .map_err(map_redb_err)?;
                by_uri
                    .insert(uri, account.id.as_str())
                    .map_err(map_redb_err)?;
            }
            let mut by_handle = write_txn
                .open_table(ACCOUNTS_BY_HANDLE)
                .map_err(map_redb_err)?;
            by_handle
                .insert(
                    Self::canonical_handle(account).as_str(),
                    account.id.as_str(),
                )
                .map_err(map_redb_err)?;
            owners_table
                .insert(account.id.as_str(), self.local_node_id.as_str())
                .map_err(map_redb_err)?;
            // Wave 4 scans: a fresh local account joins the maintained
            // local-user counter (NodeInfo `users.total` predicate: local,
            // live, unsuspended — all true at creation).
            if is_local {
                self.counter_add_in_txn(&write_txn, COUNTER_ACCOUNTS_LOCAL, 1)?;
            }
        }
        if let Some(entry) = log {
            self.append_log_in_txn(write_txn, entry)?;
        }
        Ok(())
    }
}
impl AccountStore for RedbStore {
    fn create_account(
        &self,
        input: CreateAccountInput,
        log: Option<&ModerationLogEntry>,
    ) -> Result<Account, PortError> {
        let account = self.build_account(input)?;
        let write_txn = self.begin_write()?;
        self.create_account_in_txn(&write_txn, &account, log)?;
        self.commit_write(write_txn)?;
        Ok(account)
    }

    fn find_by_id(&self, id: &str) -> Result<Option<Account>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
        match table.get(id).map_err(map_redb_err)? {
            Some(guard) => Ok(Some(decode_json(guard.value())?)),
            None => Ok(None),
        }
    }

    fn find_by_handle(&self, normalized_handle: &str) -> Result<Option<Account>, PortError> {
        let cleaned = normalized_handle.trim().trim_start_matches('@');
        self.find_by_username_and_host(cleaned, None)
    }

    fn find_by_username_and_host(
        &self,
        username: &str,
        host: Option<&str>,
    ) -> Result<Option<Account>, PortError> {
        let lower = username.trim().to_ascii_lowercase();
        let key = Self::handle_key(&lower, host);
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let handle_table = read_txn.open_table(HANDLE_INDEX).map_err(map_redb_err)?;
        let accounts_table = read_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
        let Some(id_guard) = handle_table.get(key.as_str()).map_err(map_redb_err)? else {
            return Ok(None);
        };
        let account_id = id_guard.value().to_string();
        match accounts_table
            .get(account_id.as_str())
            .map_err(map_redb_err)?
        {
            Some(acc_guard) => Ok(Some(decode_json(acc_guard.value())?)),
            None => Ok(None),
        }
    }

    fn is_username_available(
        &self,
        username: &str,
        preserved_usernames: &[String],
    ) -> Result<bool, PortError> {
        validate_local_username(username)?;
        let lower = username.to_ascii_lowercase();
        for p in preserved_usernames {
            if p.eq_ignore_ascii_case(&lower) {
                return Ok(false);
            }
        }
        let key = Self::handle_key(&lower, None);
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let handle_table = read_txn.open_table(HANDLE_INDEX).map_err(map_redb_err)?;
        let used_table = read_txn.open_table(USED_USERNAMES).map_err(map_redb_err)?;
        if handle_table
            .get(key.as_str())
            .map_err(map_redb_err)?
            .is_some()
        {
            return Ok(false);
        }
        if used_table
            .get(lower.as_str())
            .map_err(map_redb_err)?
            .is_some()
        {
            return Ok(false);
        }
        Ok(true)
    }

    fn update_account(
        &self,
        id: &str,
        patch: UpdateAccountPatch,
        jobs: Vec<NewJob>,
    ) -> Result<Account, PortError> {
        let write_txn = self.begin_write()?;
        let updated = self.update_account_in_txn(&write_txn, id, patch, jobs)?;
        self.commit_write(write_txn)?;
        Ok(updated)
    }

    fn rename(&self, id: &str, new_handle: &str) -> Result<Account, PortError> {
        validate_local_username(new_handle)?;
        let new_lower = new_handle.to_ascii_lowercase();
        let new_key = Self::handle_key(&new_lower, None);

        let write_txn = self.begin_write()?;
        let updated = {
            let mut accounts_table = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
            let mut handle_table = write_txn.open_table(HANDLE_INDEX).map_err(map_redb_err)?;
            let mut used_table = write_txn.open_table(USED_USERNAMES).map_err(map_redb_err)?;

            let mut acc: Account = match accounts_table.get(id).map_err(map_redb_err)? {
                Some(g) => decode_json(g.value())?,
                None => return Err(PortError::NotFound(format!("account {id} not found"))),
            };
            if acc.host.is_some() {
                return Err(PortError::Validation(
                    "cannot rename remote account".to_string(),
                ));
            }
            if acc.username_lower == new_lower {
                acc.username = new_handle.to_string();
                let bytes = encode_json(&acc)?;
                accounts_table
                    .insert(acc.id.as_str(), bytes.as_slice())
                    .map_err(map_redb_err)?;
                acc
            } else {
                if used_table
                    .get(new_lower.as_str())
                    .map_err(map_redb_err)?
                    .is_some()
                {
                    return Err(PortError::Conflict(format!(
                        "handle {new_lower} was previously used"
                    )));
                }
                if handle_table
                    .get(new_key.as_str())
                    .map_err(map_redb_err)?
                    .is_some()
                {
                    return Err(PortError::Conflict(format!(
                        "handle {new_lower} is already taken"
                    )));
                }
                let old_lower = acc.username_lower.clone();
                let old_key = Self::handle_key(&old_lower, None);
                handle_table
                    .remove(old_key.as_str())
                    .map_err(map_redb_err)?;
                used_table
                    .insert(old_lower.as_str(), &1u8)
                    .map_err(map_redb_err)?;

                acc.username = new_handle.to_string();
                acc.username_lower = new_lower;
                acc.validate()?;
                let bytes = encode_json(&acc)?;
                accounts_table
                    .insert(acc.id.as_str(), bytes.as_slice())
                    .map_err(map_redb_err)?;
                handle_table
                    .insert(new_key.as_str(), acc.id.as_str())
                    .map_err(map_redb_err)?;
                // WP5 federation: renames move the full-handle alias in
                // the same transaction (the URI row is immutable).
                let mut by_handle = write_txn
                    .open_table(ACCOUNTS_BY_HANDLE)
                    .map_err(map_redb_err)?;
                by_handle.remove(old_lower.as_str()).map_err(map_redb_err)?;
                by_handle
                    .insert(acc.username_lower.as_str(), acc.id.as_str())
                    .map_err(map_redb_err)?;
                acc
            }
        };
        self.commit_write(write_txn)?;
        Ok(updated)
    }

    fn get_owner(&self, id: &str) -> Result<String, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let owners = read_txn.open_table(ACCOUNT_OWNERS).map_err(map_redb_err)?;
        if let Some(guard) = owners.get(id).map_err(map_redb_err)? {
            return Ok(guard.value().to_string());
        }
        let accounts = read_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
        if accounts.get(id).map_err(map_redb_err)?.is_some() {
            return Ok(self.local_node_id.clone());
        }
        Err(PortError::NotFound(format!("account {id} not found")))
    }

    fn set_owner(&self, id: &str, node: &str, migration_proof: &str) -> Result<(), PortError> {
        if migration_proof.trim().is_empty() {
            return Err(PortError::Validation(
                "migration_proof is required".to_string(),
            ));
        }
        let write_txn = self.begin_write()?;
        {
            let accounts = write_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
            if accounts.get(id).map_err(map_redb_err)?.is_none() {
                return Err(PortError::NotFound(format!("account {id} not found")));
            }
            let mut owners = write_txn.open_table(ACCOUNT_OWNERS).map_err(map_redb_err)?;
            owners.insert(id, node).map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)
    }

    fn search_accounts(
        &self,
        query: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Account>, PortError> {
        let q = query.trim().to_lowercase();
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
        let mut matches = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (_, val) = entry.map_err(map_redb_err)?;
            let acc: Account = decode_json(val.value())?;
            if acc.is_deleted || acc.is_suspended {
                continue;
            }
            let name_match = acc
                .display_name
                .as_ref()
                .map(|n| n.to_lowercase().contains(&q))
                .unwrap_or(false);
            if q.is_empty() || acc.username_lower.contains(&q) || name_match {
                matches.push(acc);
            }
        }
        matches.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(matches.into_iter().skip(offset).take(limit).collect())
    }

    fn find_by_email_verify_code(&self, code: &str) -> Result<Option<Account>, PortError> {
        // Codes are single-use and short-lived; the accounts table is small
        // on a single node, so a bounded cursor walk (same shape as the
        // export sweeps) is the honest lookup — no secondary index to keep
        // in sync with the clear-on-verify write.
        let mut after: Option<String> = None;
        loop {
            let (page, next) = self.all_accounts(after.as_deref(), 500)?;
            if page.is_empty() {
                return Ok(None);
            }
            for acc in page {
                if acc.email_verify_code.as_deref() == Some(code) {
                    return Ok(Some(acc));
                }
            }
            let Some(cursor) = next else {
                return Ok(None);
            };
            after = Some(cursor);
        }
    }

    fn count_accounts(&self) -> Result<u64, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
        Ok(table.len().map_err(map_redb_err)?)
    }

    fn count_local_accounts(&self) -> Result<u64, PortError> {
        // Wave 4 liveness: O(1) maintained counter (bumped on create,
        // suspend/unsuspend, and delete in the same transactions; seeded
        // once at open). The old NodeInfo path decoded the whole table.
        self.counter_get(COUNTER_ACCOUNTS_LOCAL)
    }

    fn all_accounts(
        &self,
        after: Option<&str>,
        max_rows: usize,
    ) -> Result<(Vec<Account>, Option<String>), PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
        let mut out = Vec::new();
        let mut last: Option<String> = None;
        let mut scanned = 0usize;
        for entry in table.iter().map_err(map_redb_err)? {
            let (k, v) = entry.map_err(map_redb_err)?;
            let id = k.value().to_string();
            if let Some(a) = after {
                if id.as_str() <= a {
                    continue;
                }
            }
            if scanned >= max_rows {
                break;
            }
            let acc: Account = decode_json(v.value())?;
            out.push(acc);
            last = Some(id);
            scanned += 1;
        }
        let next = if scanned >= max_rows { last } else { None };
        Ok((out, next))
    }

    fn remote_shared_inboxes(
        &self,
        after: Option<&str>,
        max_rows: usize,
    ) -> Result<(Vec<String>, Option<String>), PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
        let start = after.as_deref().unwrap_or("");
        let mut out = Vec::new();
        let mut next: Option<String> = None;
        let mut scanned = 0usize;
        for entry in table.range(start..).map_err(map_redb_err)? {
            let (k, v) = entry.map_err(map_redb_err)?;
            let id = k.value();
            // The cursor is exclusive: skip the row it names.
            if Some(id) == after.as_deref() {
                continue;
            }
            scanned += 1;
            let account: Account = decode_json(v.value())?;
            if account.host.is_some() {
                if let Some(inbox) = account
                    .shared_inbox
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    out.push(inbox.to_string());
                }
            }
            if scanned >= max_rows {
                next = Some(id.to_string());
                break;
            }
        }
        Ok((out, next))
    }

    fn find_by_uri(&self, uri: &str) -> Result<Option<Account>, PortError> {
        let target = uri.trim();
        if target.is_empty() {
            return Ok(None);
        }
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_uri = read_txn.open_table(ACCOUNTS_BY_URI).map_err(map_redb_err)?;
        let Some(id) = by_uri
            .get(target)
            .map_err(map_redb_err)?
            .map(|guard| guard.value().to_string())
        else {
            return Ok(None);
        };
        let table = read_txn.open_table(ACCOUNTS).map_err(map_redb_err)?;
        match table.get(id.as_str()).map_err(map_redb_err)? {
            Some(guard) => Ok(Some(decode_json(guard.value())?)),
            None => Ok(None),
        }
    }
}
