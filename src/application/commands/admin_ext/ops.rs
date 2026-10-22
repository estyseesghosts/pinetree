use crate::application::commands::AppServices;

use crate::application::ports::PortError;
use crate::domain::account::Account;
use crate::domain::admin::{
    FederationInstanceOverride, KEY_PROXY_ACCOUNT, QUEUE_CONTROL_UNSUPPORTED_REASON,
    SCOPE_ADMIN_META, SCOPE_FEDERATION_INSTANCES,
};

impl AppServices {
    pub fn proxy_account_get(&self) -> Result<Option<String>, PortError> {
        Ok(self
            .global_get(SCOPE_ADMIN_META, KEY_PROXY_ACCOUNT)?
            .and_then(|value| value.as_str().map(str::to_string)))
    }

    /// `admin/update-proxy-account`: set (or clear with `None`) the proxy
    /// account. The target must be a local account.
    pub fn proxy_account_set(&self, user_id: Option<&str>) -> Result<(), PortError> {
        match user_id {
            None => self.global_remove(SCOPE_ADMIN_META, KEY_PROXY_ACCOUNT),
            Some(id) => {
                let account = self
                    .accounts
                    .find_by_id(id)?
                    .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
                if !account.is_local() {
                    return Err(PortError::Validation(
                        "INVALID_PARAM proxy account must be local".to_string(),
                    ));
                }
                self.global_set(
                    SCOPE_ADMIN_META,
                    KEY_PROXY_ACCOUNT,
                    serde_json::Value::String(id.to_string()),
                )
            }
        }
    }

    // --- stats / user ips / find-by-email / reset-password --------------------------

    /// `admin/stats` (upstream `stats.ts` shape, reduced): instance totals
    /// computable from existing stores. DIVERGENCE: no per-day deltas
    /// (notes/drive deltas need time-series ports — proposed).
    pub fn admin_stats_overview(&self) -> Result<serde_json::Value, PortError> {
        let users = self
            .accounts
            .count_accounts()
            .map_err(|_| PortError::StorageUnavailable("stats need count_accounts".to_string()))?;
        let pending_jobs = self.jobs.pending_count()?;
        let federation = self.federation_instances()?;
        let local_emojis = self
            .emojis
            .all_local_emojis()
            .map(|rows| rows.len())
            .unwrap_or(0);
        Ok(serde_json::json!({
            "usersCount": users,
            "pendingJobs": pending_jobs,
            "knownRemoteHosts": federation.len(),
            "localEmojis": local_emojis,
        }))
    }

    /// `admin/server-info` (upstream `server-info.ts` shape, reduced to the
    /// public `server-info` packer inputs): machine/cpu/mem from stable
    /// Rust + `/proc`; `fs` totals 0 (same DIVERGENCE as `server-info`).
    pub fn admin_server_info(&self) -> serde_json::Value {
        let machine = format!("{} {}", std::env::consts::OS, std::env::consts::ARCH);
        let cores = std::thread::available_parallelism()
            .map(|n| n.get() as u64)
            .unwrap_or(1);
        serde_json::json!({
            "machine": machine,
            "cpu": { "cores": cores },
            "mem": {},
            "fs": { "total": 0, "used": 0 },
        })
    }

    /// `admin/reset-password` (upstream file of the same name): admin sets
    /// a user's password hash directly (the dispatcher hashes the plaintext
    /// off-request, mirroring `admin/accounts/create`). No moderation-log
    /// row: no dedicated action type exists in `domain/moderation.rs`
    /// (proposed: `resetPassword` in the subagent report).
    pub fn admin_reset_password(
        &self,
        account_id: &str,
        password_hash: &str,
    ) -> Result<(), PortError> {
        self.accounts
            .find_by_id(account_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
        self.accounts.get_owner(account_id)?;
        self.auth.set_password_hash(account_id, password_hash)
    }

    /// `admin/get-user-ips` + `federation/get-user-ips` (upstream files of
    /// the same names): recent signin IPs for one account, newest-first
    /// (signin rows exist only from deploy forward, trimmed per account).
    pub fn get_user_ips(
        &self,
        account_id: &str,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>, PortError> {
        self.accounts
            .find_by_id(account_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
        self.accounts.get_owner(account_id)?;
        let limit = limit.clamp(1, 100);
        let rows = self
            .signin
            .list_signins(account_id, limit, None, None, None, None)?;
        Ok(rows
            .into_iter()
            .map(|row| {
                serde_json::json!({
                    "ip": row.ip,
                    "createdAt": format_millis(row.created_at_ms),
                    "success": row.success,
                })
            })
            .collect())
    }

    /// `admin/accounts/find-by-email` (upstream file of the same name):
    /// exact (case-insensitive) email match over a bounded account scan
    /// (admin/show-users caps the same walk at 10 000 rows).
    pub fn find_account_by_email(&self, email: &str) -> Result<Option<Account>, PortError> {
        let wanted = email.trim().to_ascii_lowercase();
        if wanted.is_empty() {
            return Ok(None);
        }
        let mut cursor = None;
        loop {
            let (page, next) = self.accounts.all_accounts(cursor.as_deref(), 1000)?;
            for account in page {
                if account
                    .email
                    .as_deref()
                    .is_some_and(|stored| stored.to_ascii_lowercase() == wanted)
                {
                    return Ok(Some(account));
                }
            }
            match next {
                Some(cursor_value) => cursor = Some(cursor_value),
                None => return Ok(None),
            }
        }
    }

    // --- federation instances -------------------------------------------------------
    //
    /// `federation/instances` (upstream `instances.ts`): observed remote
    /// hosts derived from cached remote accounts, merged with moderator
    /// overrides. Bounded scan (same 10 000-row cap as `admin/show-users`).
    pub fn federation_instances(&self) -> Result<Vec<serde_json::Value>, PortError> {
        use std::collections::BTreeMap;
        let mut counts: BTreeMap<String, usize> = BTreeMap::new();
        let mut cursor = None;
        let mut scanned = 0usize;
        loop {
            let (page, next) = self.accounts.all_accounts(cursor.as_deref(), 1000)?;
            for account in &page {
                if let Some(host) = account.host.clone() {
                    if !account.is_deleted {
                        *counts.entry(host.to_ascii_lowercase()).or_default() += 1;
                    }
                }
            }
            scanned += page.len();
            match next {
                Some(cursor_value) if scanned < 10_000 => cursor = Some(cursor_value),
                _ => break,
            }
        }
        let overrides: std::collections::HashMap<String, FederationInstanceOverride> = self
            .global_list::<FederationInstanceOverride>(SCOPE_FEDERATION_INSTANCES)?
            .into_iter()
            .map(|(_, record)| (record.host.clone(), record))
            .collect();
        let mut out = Vec::new();
        for (host, users_count) in &counts {
            let sponsored = overrides.get(host);
            out.push(serde_json::json!({
                "host": host,
                "usersCount": users_count,
                "isSuspended": sponsored.is_some_and(|o| o.is_suspended),
                "isBlocked": sponsored.is_some_and(|o| o.is_blocked),
                "isSilenced": sponsored.is_some_and(|o| o.is_silenced),
            }));
        }
        for (host, record) in &overrides {
            if !counts.contains_key(host) {
                out.push(serde_json::json!({
                    "host": host,
                    "usersCount": 0,
                    "isSuspended": record.is_suspended,
                    "isBlocked": record.is_blocked,
                    "isSilenced": record.is_silenced,
                }));
            }
        }
        Ok(out)
    }

    /// `federation/show-instance` (upstream file of the same name): one
    /// observed host merged with its override, or `None` when unknown.
    pub fn federation_show_instance(
        &self,
        host: &str,
    ) -> Result<Option<serde_json::Value>, PortError> {
        let wanted = host.trim().to_ascii_lowercase();
        if wanted.is_empty() {
            return Ok(None);
        }
        for row in self.federation_instances()? {
            if row.get("host").and_then(|h| h.as_str()) == Some(wanted.as_str()) {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    /// `admin/federation/update-instance` + `federation/update-instance`:
    /// store the moderator override row (observed traffic is untouched).
    /// Unknown hosts are accepted (the override applies when the host is
    /// first observed — upstream parity).
    pub fn federation_update_instance(
        &self,
        host: &str,
        is_suspended: Option<bool>,
        is_blocked: Option<bool>,
        is_silenced: Option<bool>,
        moderation_note: Option<String>,
    ) -> Result<FederationInstanceOverride, PortError> {
        let host = host.trim().to_ascii_lowercase();
        if host.is_empty() || host.contains(' ') || host.contains('/') {
            return Err(PortError::Validation(
                "INVALID_PARAM invalid host".to_string(),
            ));
        }
        let mut record: FederationInstanceOverride = self
            .global_get(SCOPE_FEDERATION_INSTANCES, &host)?
            .and_then(|value| serde_json::from_value(value).ok())
            .unwrap_or(FederationInstanceOverride {
                host: host.clone(),
                is_suspended: false,
                is_blocked: false,
                is_silenced: false,
                moderation_note: None,
                updated_at_ms: 0,
            });
        if let Some(value) = is_suspended {
            record.is_suspended = value;
        }
        if let Some(value) = is_blocked {
            record.is_blocked = value;
        }
        if let Some(value) = is_silenced {
            record.is_silenced = value;
        }
        if moderation_note.is_some() {
            record.moderation_note = moderation_note;
        }
        record.updated_at_ms = self.now_ms();
        self.global_set(
            SCOPE_FEDERATION_INSTANCES,
            &host,
            serde_json::to_value(&record).map_err(|e| {
                PortError::Validation(format!("INVALID_PARAM instance encode: {e}"))
            })?,
        )?;
        Ok(record)
    }

    /// `admin/federation/refresh-remote-instance-metadata` (upstream file of
    /// the same name): touch the override's `updated_at_ms` so caches and
    /// workers re-read it. DIVERGENCE: no live metadata refetch (no
    /// instance-metadata fetcher in this build); the dispatcher answers 204.
    pub fn federation_refresh_metadata(&self, host: &str) -> Result<(), PortError> {
        let host = host.trim().to_ascii_lowercase();
        if host.is_empty() {
            return Err(PortError::Validation(
                "INVALID_PARAM invalid host".to_string(),
            ));
        }
        if let Some(value) = self.global_get(SCOPE_FEDERATION_INSTANCES, &host)? {
            let mut record: FederationInstanceOverride =
                serde_json::from_value(value).map_err(|e| {
                    PortError::Validation(format!("INVALID_PARAM instance decode: {e}"))
                })?;
            record.updated_at_ms = self.now_ms();
            self.global_set(
                SCOPE_FEDERATION_INSTANCES,
                &host,
                serde_json::to_value(&record).map_err(|e| {
                    PortError::Validation(format!("INVALID_PARAM instance encode: {e}"))
                })?,
            )?;
        }
        Ok(())
    }

    // --- queue -------------------------------------------------------------------------
    //
    /// `queue/stats` + `admin/queue/stats`: the single unified job queue
    /// has no BullMQ lanes, so `deliver`/`inbox` report the same pending
    /// count and `active`/`delayed` are 0. DIVERGENCE recorded (upstream
    /// reports per-queue Bull numbers).
    pub fn queue_stats(&self) -> Result<serde_json::Value, PortError> {
        let waiting = self.jobs.pending_count()?;
        Ok(serde_json::json!({
            "deliver": { "active": 0, "waiting": waiting, "delayed": 0 },
            "inbox": { "active": 0, "waiting": 0, "delayed": 0 },
        }))
    }

    /// `admin/queue/deliver-delayed`, `admin/queue/inbox-delayed`,
    /// `admin/queue/clear`: loud 400s — no enumeration/clear ports exist
    /// for the unified queue (proposed in the subagent report).
    pub fn queue_delayed_unsupported(&self) -> Result<(), PortError> {
        Err(PortError::Validation(format!(
            "INVALID_PARAM {QUEUE_CONTROL_UNSUPPORTED_REASON}"
        )))
    }
}
/// Millisecond timestamps as ISO-8601 UTC strings (same rendering as the
/// API packers; local copy keeps `application` free of adapter imports).

fn format_millis(ms: i64) -> String {
    let dt: chrono::DateTime<chrono::Utc> =
        chrono::DateTime::from_timestamp_millis(ms).unwrap_or_default();
    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}
