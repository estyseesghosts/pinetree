//! WP2-part-C admin + federation + queue use cases.
//!
//! Storage (binding constraint — no new ports/tables): instance-global
//! collections (relays, ads, announcements, roles + memberships,
//! abuse-report notification recipients, proxy account, federation
//! overrides) live in the existing [`RegistryStore`] under the reserved
//! [`GLOBAL_SETTINGS_NAMESPACE`] user id (see
//! `crate::domain::admin`). Per-account reads (signin IPs) use the
//! existing [`SigninStore`]; counts use existing store methods. Role
//! policies are stored verbatim and NOT enforced (TODOS P2: no
//! role-policy engine).

use super::AppServices;

use crate::application::ports::{PortError, RegistryItem};
use crate::domain::admin::{
    AbuseRecipientRecord, AdRecord, AnnouncementRecord, FederationInstanceOverride, RelayRecord,
    RoleRecord, GLOBAL_SETTINGS_NAMESPACE, KEY_PROXY_ACCOUNT, QUEUE_CONTROL_UNSUPPORTED_REASON,
    SCOPE_ADMIN_ABUSE_RECIPIENTS, SCOPE_ADMIN_ADS,
    SCOPE_ADMIN_ANNOUNCEMENTS, SCOPE_ADMIN_META, SCOPE_ADMIN_RELAYS, SCOPE_ADMIN_ROLES,
    SCOPE_ADMIN_ROLE_MEMBERS, SCOPE_FEDERATION_INSTANCES, validate_ad, validate_announcement,
    validate_recipient, validate_relay_inbox, validate_role,
};
use crate::domain::account::Account;

fn global_scope(scope: &str) -> Vec<String> {
    vec![scope.to_string()]
}

impl AppServices {
    // --- global registry helpers ------------------------------------------

    fn global_get(&self, scope: &str, key: &str) -> Result<Option<serde_json::Value>, PortError> {
        Ok(self
            .registry
            .registry_get(GLOBAL_SETTINGS_NAMESPACE, None, &global_scope(scope), key)?
            .map(|item| item.value))
    }

    fn global_set(&self, scope: &str, key: &str, value: serde_json::Value) -> Result<(), PortError> {
        self.registry.registry_set(RegistryItem {
            id: self.ids.generate(),
            user_id: GLOBAL_SETTINGS_NAMESPACE.to_string(),
            domain: None,
            scope: global_scope(scope),
            key: key.to_string(),
            value,
            updated_at_ms: self.now_ms(),
        })
    }

    fn global_remove(&self, scope: &str, key: &str) -> Result<(), PortError> {
        self.registry.registry_remove(
            GLOBAL_SETTINGS_NAMESPACE,
            None,
            &global_scope(scope),
            key,
        )
    }

    fn global_list<T>(&self, scope: &str) -> Result<Vec<(String, T)>, PortError>
    where
        T: serde::de::DeserializeOwned,
    {
        let items =
            self.registry
                .registry_list(GLOBAL_SETTINGS_NAMESPACE, None, &global_scope(scope))?;
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            if let Ok(record) = serde_json::from_value::<T>(item.value) {
                out.push((item.key, record));
            }
        }
        Ok(out)
    }

    // --- relays (`admin/relays/*`) ------------------------------------------

    /// `admin/relays/add` (upstream `relays/add.ts`): idempotent by inbox.
    pub fn relay_add(&self, inbox: &str) -> Result<RelayRecord, PortError> {
        validate_relay_inbox(inbox)?;
        if let Some(value) = self.global_get(SCOPE_ADMIN_RELAYS, inbox)? {
            return serde_json::from_value(value)
                .map_err(|e| PortError::Validation(format!("INVALID_PARAM relay decode: {e}")));
        }
        let record = RelayRecord {
            inbox: inbox.to_string(),
            status: "requesting".to_string(),
        };
        self.global_set(
            SCOPE_ADMIN_RELAYS,
            inbox,
            serde_json::to_value(&record)
                .map_err(|e| PortError::Validation(format!("INVALID_PARAM relay encode: {e}")))?,
        )?;
        Ok(record)
    }

    /// `admin/relays/list` (upstream `relays/list.ts`).
    pub fn relay_list(&self) -> Result<Vec<RelayRecord>, PortError> {
        let mut rows: Vec<RelayRecord> = self
            .global_list::<RelayRecord>(SCOPE_ADMIN_RELAYS)?
            .into_iter()
            .map(|(_, record)| record)
            .collect();
        rows.sort_by(|a, b| a.inbox.cmp(&b.inbox));
        Ok(rows)
    }

    /// `admin/relays/remove` (upstream `relays/remove.ts`): missing inbox
    /// is a no-op success.
    pub fn relay_remove(&self, inbox: &str) -> Result<(), PortError> {
        validate_relay_inbox(inbox)?;
        self.global_remove(SCOPE_ADMIN_RELAYS, inbox)
    }

    // --- announcements (`admin/announcements/*`) -----------------------------

    /// `admin/announcements/create`.
    pub fn admin_announcement_create(
        &self,
        title: &str,
        text: &str,
        image_url: Option<String>,
    ) -> Result<AnnouncementRecord, PortError> {
        validate_announcement(title, text)?;
        let now = self.now_ms();
        let record = AnnouncementRecord {
            id: self.ids.generate(),
            title: title.to_string(),
            text: text.to_string(),
            image_url,
            created_at_ms: now,
            updated_at_ms: now,
        };
        self.global_set(
            SCOPE_ADMIN_ANNOUNCEMENTS,
            &record.id.clone(),
            serde_json::to_value(&record)
                .map_err(|e| PortError::Validation(format!("INVALID_PARAM announcement encode: {e}")))?,
        )?;
        Ok(record)
    }

    /// `admin/announcements/list`: newest-first.
    pub fn admin_announcement_list(&self) -> Result<Vec<AnnouncementRecord>, PortError> {
        let mut rows: Vec<AnnouncementRecord> = self
            .global_list::<AnnouncementRecord>(SCOPE_ADMIN_ANNOUNCEMENTS)?
            .into_iter()
            .map(|(_, record)| record)
            .collect();
        rows.sort_by(|a, b| {
            b.created_at_ms
                .cmp(&a.created_at_ms)
                .then_with(|| b.id.cmp(&a.id))
        });
        Ok(rows)
    }

    /// `admin/announcements/update`.
    pub fn admin_announcement_update(
        &self,
        id: &str,
        title: &str,
        text: &str,
        image_url: Option<String>,
    ) -> Result<AnnouncementRecord, PortError> {
        validate_announcement(title, text)?;
        let mut record: AnnouncementRecord = self
            .global_get(SCOPE_ADMIN_ANNOUNCEMENTS, id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_ANNOUNCEMENT".to_string()))
            .and_then(|value| {
                serde_json::from_value(value)
                    .map_err(|_| PortError::NotFound("NO_SUCH_ANNOUNCEMENT".to_string()))
            })?;
        record.title = title.to_string();
        record.text = text.to_string();
        record.image_url = image_url;
        record.updated_at_ms = self.now_ms();
        self.global_set(
            SCOPE_ADMIN_ANNOUNCEMENTS,
            id,
            serde_json::to_value(&record)
                .map_err(|e| PortError::Validation(format!("INVALID_PARAM announcement encode: {e}")))?,
        )?;
        Ok(record)
    }

    /// `admin/announcements/delete`: missing id is a no-op success.
    pub fn admin_announcement_delete(&self, id: &str) -> Result<(), PortError> {
        self.global_remove(SCOPE_ADMIN_ANNOUNCEMENTS, id)
    }

    // --- ads (`admin/ad/*`) ----------------------------------------------------

    /// `admin/ad/create`.
    #[allow(clippy::too_many_arguments)]
    pub fn ad_create(
        &self,
        url: &str,
        memo: &str,
        place: &str,
        ratio: f64,
        priority: &str,
        expires_at_ms: i64,
        starts_at_ms: i64,
        image_url: &str,
        day_of_week: i64,
    ) -> Result<AdRecord, PortError> {
        validate_ad(url, memo, place, image_url)?;
        let record = AdRecord {
            id: self.ids.generate(),
            url: url.to_string(),
            memo: memo.to_string(),
            place: place.to_string(),
            ratio,
            priority: priority.to_string(),
            expires_at_ms,
            starts_at_ms,
            image_url: image_url.to_string(),
            day_of_week,
        };
        self.global_set(
            SCOPE_ADMIN_ADS,
            &record.id.clone(),
            serde_json::to_value(&record)
                .map_err(|e| PortError::Validation(format!("INVALID_PARAM ad encode: {e}")))?,
        )?;
        Ok(record)
    }

    /// `admin/ad/list`: insertion order by id.
    pub fn ad_list(&self) -> Result<Vec<AdRecord>, PortError> {
        let mut rows: Vec<AdRecord> = self
            .global_list::<AdRecord>(SCOPE_ADMIN_ADS)?
            .into_iter()
            .map(|(_, record)| record)
            .collect();
        rows.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(rows)
    }

    /// `admin/ad/delete`: missing id is a no-op success.
    pub fn ad_delete(&self, id: &str) -> Result<(), PortError> {
        self.global_remove(SCOPE_ADMIN_ADS, id)
    }

    /// `admin/ad/update`: replace mutable fields of one row.
    #[allow(clippy::too_many_arguments)]
    pub fn ad_update(
        &self,
        id: &str,
        url: &str,
        memo: &str,
        place: &str,
        ratio: f64,
        priority: &str,
        expires_at_ms: i64,
        starts_at_ms: i64,
        image_url: &str,
        day_of_week: i64,
    ) -> Result<AdRecord, PortError> {
        validate_ad(url, memo, place, image_url)?;
        self.global_get(SCOPE_ADMIN_ADS, id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_AD".to_string()))
            .and_then(|value| {
                serde_json::from_value::<AdRecord>(value)
                    .map_err(|_| PortError::NotFound("NO_SUCH_AD".to_string()))
            })?;
        let record = AdRecord {
            id: id.to_string(),
            url: url.to_string(),
            memo: memo.to_string(),
            place: place.to_string(),
            ratio,
            priority: priority.to_string(),
            expires_at_ms,
            starts_at_ms,
            image_url: image_url.to_string(),
            day_of_week,
        };
        self.global_set(
            SCOPE_ADMIN_ADS,
            id,
            serde_json::to_value(&record)
                .map_err(|e| PortError::Validation(format!("INVALID_PARAM ad encode: {e}")))?,
        )?;
        Ok(record)
    }

    /// `admin/ad/refresh`: prune expired rows (`expires_at_ms <= now`).
    /// Returns the number of rows pruned.
    pub fn ad_refresh(&self) -> Result<usize, PortError> {
        let now = self.now_ms();
        let rows = self.global_list::<AdRecord>(SCOPE_ADMIN_ADS)?;
        let mut pruned = 0usize;
        for (key, record) in rows {
            if record.expires_at_ms <= now {
                self.global_remove(SCOPE_ADMIN_ADS, &key)?;
                pruned += 1;
            }
        }
        Ok(pruned)
    }

    // --- roles (`admin/roles/*`) -----------------------------------------------
    //
    /// Custom roles, stored verbatim. [`ROLE_POLICY_UNENFORCED_NOTE`]:
    /// assignment gates visibility only; no permission engine reads these.

    /// `admin/roles/create`.
    #[allow(clippy::too_many_arguments)]
    pub fn role_create(
        &self,
        name: &str,
        description: &str,
        color: Option<String>,
        icon_url: Option<String>,
        target: &str,
        cond_formula: serde_json::Value,
        is_public: bool,
        as_badge: bool,
        can_edit_members_by_moderator: bool,
        display_order: i64,
        policies: serde_json::Value,
    ) -> Result<RoleRecord, PortError> {
        validate_role(name, target)?;
        let record = RoleRecord {
            id: self.ids.generate(),
            name: name.to_string(),
            description: description.to_string(),
            color,
            icon_url,
            target: target.to_string(),
            cond_formula,
            is_public,
            as_badge,
            can_edit_members_by_moderator,
            display_order,
            policies,
        };
        self.global_set(
            SCOPE_ADMIN_ROLES,
            &record.id.clone(),
            serde_json::to_value(&record)
                .map_err(|e| PortError::Validation(format!("INVALID_PARAM role encode: {e}")))?,
        )?;
        Ok(record)
    }

    /// `admin/roles/list`: `display_order`, then id.
    pub fn role_list(&self) -> Result<Vec<RoleRecord>, PortError> {
        let mut rows: Vec<RoleRecord> = self
            .global_list::<RoleRecord>(SCOPE_ADMIN_ROLES)?
            .into_iter()
            .map(|(_, record)| record)
            .collect();
        rows.sort_by(|a, b| {
            a.display_order
                .cmp(&b.display_order)
                .then_with(|| a.id.cmp(&b.id))
        });
        Ok(rows)
    }

    /// `admin/roles/show`: verbatim `NO_SUCH_ROLE` on miss.
    pub fn role_show(&self, role_id: &str) -> Result<RoleRecord, PortError> {
        self.global_get(SCOPE_ADMIN_ROLES, role_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_ROLE".to_string()))
            .and_then(|value| {
                serde_json::from_value(value)
                    .map_err(|_| PortError::NotFound("NO_SUCH_ROLE".to_string()))
            })
    }

    /// `admin/roles/delete`: drops the role + its membership row. Missing
    /// id is a no-op success.
    pub fn role_delete(&self, role_id: &str) -> Result<(), PortError> {
        self.global_remove(SCOPE_ADMIN_ROLES, role_id)?;
        self.global_remove(SCOPE_ADMIN_ROLE_MEMBERS, role_id)
    }

    /// `admin/roles/update`: replace mutable fields of one role.
    #[allow(clippy::too_many_arguments)]
    pub fn role_update(
        &self,
        role_id: &str,
        name: &str,
        description: &str,
        color: Option<String>,
        icon_url: Option<String>,
        target: &str,
        cond_formula: serde_json::Value,
        is_public: bool,
        as_badge: bool,
        can_edit_members_by_moderator: bool,
        display_order: i64,
        policies: serde_json::Value,
    ) -> Result<RoleRecord, PortError> {
        validate_role(name, target)?;
        self.role_show(role_id)?;
        let record = RoleRecord {
            id: role_id.to_string(),
            name: name.to_string(),
            description: description.to_string(),
            color,
            icon_url,
            target: target.to_string(),
            cond_formula,
            is_public,
            as_badge,
            can_edit_members_by_moderator,
            display_order,
            policies,
        };
        self.global_set(
            SCOPE_ADMIN_ROLES,
            role_id,
            serde_json::to_value(&record)
                .map_err(|e| PortError::Validation(format!("INVALID_PARAM role encode: {e}")))?,
        )?;
        Ok(record)
    }

    fn role_member_ids(&self, role_id: &str) -> Result<Vec<String>, PortError> {
        let value = self.global_get(SCOPE_ADMIN_ROLE_MEMBERS, role_id)?;
        match value {
            None => Ok(Vec::new()),
            Some(value) => {
                let ids: Vec<String> = serde_json::from_value(value).map_err(|e| {
                    PortError::Validation(format!("INVALID_PARAM role members decode: {e}"))
                })?;
                Ok(ids)
            }
        }
    }

    fn role_set_members(&self, role_id: &str, user_ids: &[String]) -> Result<(), PortError> {
        self.global_set(
            SCOPE_ADMIN_ROLE_MEMBERS,
            role_id,
            serde_json::to_value(user_ids)
                .map_err(|e| PortError::Validation(format!("INVALID_PARAM role members encode: {e}")))?,
        )
    }

    /// `admin/roles/assign`: role + target must exist; idempotent.
    pub fn role_assign(&self, role_id: &str, user_id: &str) -> Result<(), PortError> {
        self.role_show(role_id)?;
        self.accounts
            .find_by_id(user_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
        let mut ids = self.role_member_ids(role_id)?;
        if !ids.iter().any(|id| id == user_id) {
            ids.push(user_id.to_string());
            self.role_set_members(role_id, &ids)?;
        }
        Ok(())
    }

    /// `admin/roles/unassign`: idempotent; missing role/member is a no-op.
    pub fn role_unassign(&self, role_id: &str, user_id: &str) -> Result<(), PortError> {
        let mut ids = self.role_member_ids(role_id)?;
        let before = ids.len();
        ids.retain(|id| id != user_id);
        if ids.len() != before {
            self.role_set_members(role_id, &ids)?;
        }
        Ok(())
    }

    /// `admin/roles/users` (`admin/roles/user` shape): member accounts of
    /// one role, id-ordered, offset/limit paged.
    pub fn role_users(
        &self,
        role_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Account>, PortError> {
        self.role_show(role_id)?;
        let mut ids = self.role_member_ids(role_id)?;
        ids.sort();
        let mut out = Vec::new();
        for id in ids.into_iter().skip(offset).take(limit) {
            if let Some(account) = self.accounts.find_by_id(&id)? {
                out.push(account);
            }
        }
        Ok(out)
    }

    // --- abuse-report notification recipients ------------------------------------
    //
    /// `abuse-report-notification-recipient/*` (upstream family of the same
    /// name): global recipient rows.

    /// `create`.
    pub fn abuse_recipient_create(
        &self,
        is_active: bool,
        name: &str,
        method: &str,
        user_id: Option<String>,
        email: Option<String>,
        webhook_url: Option<String>,
    ) -> Result<AbuseRecipientRecord, PortError> {
        validate_recipient(name, method, email.as_deref(), webhook_url.as_deref())?;
        if let Some(uid) = &user_id {
            self.accounts
                .find_by_id(uid)?
                .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
        }
        let record = AbuseRecipientRecord {
            id: self.ids.generate(),
            is_active,
            name: name.to_string(),
            method: method.to_string(),
            user_id,
            email,
            webhook_url,
        };
        self.global_set(
            SCOPE_ADMIN_ABUSE_RECIPIENTS,
            &record.id.clone(),
            serde_json::to_value(&record).map_err(|e| {
                PortError::Validation(format!("INVALID_PARAM recipient encode: {e}"))
            })?,
        )?;
        Ok(record)
    }

    /// `list`: id-ordered.
    pub fn abuse_recipient_list(&self) -> Result<Vec<AbuseRecipientRecord>, PortError> {
        let mut rows: Vec<AbuseRecipientRecord> =
            self.global_list::<AbuseRecipientRecord>(SCOPE_ADMIN_ABUSE_RECIPIENTS)?
                .into_iter()
                .map(|(_, record)| record)
                .collect();
        rows.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(rows)
    }

    /// `show`: verbatim miss code.
    pub fn abuse_recipient_show(&self, id: &str) -> Result<AbuseRecipientRecord, PortError> {
        self.global_get(SCOPE_ADMIN_ABUSE_RECIPIENTS, id)?
            .ok_or_else(|| {
                PortError::NotFound("NO_SUCH_ABUSE_REPORT_NOTIFICATION_RECIPIENT".to_string())
            })
            .and_then(|value| {
                serde_json::from_value(value).map_err(|_| {
                    PortError::NotFound("NO_SUCH_ABUSE_REPORT_NOTIFICATION_RECIPIENT".to_string())
                })
            })
    }

    /// `update`.
    pub fn abuse_recipient_update(
        &self,
        id: &str,
        is_active: bool,
        name: &str,
        method: &str,
        user_id: Option<String>,
        email: Option<String>,
        webhook_url: Option<String>,
    ) -> Result<AbuseRecipientRecord, PortError> {
        validate_recipient(name, method, email.as_deref(), webhook_url.as_deref())?;
        self.abuse_recipient_show(id)?;
        if let Some(uid) = &user_id {
            self.accounts
                .find_by_id(uid)?
                .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
        }
        let record = AbuseRecipientRecord {
            id: id.to_string(),
            is_active,
            name: name.to_string(),
            method: method.to_string(),
            user_id,
            email,
            webhook_url,
        };
        self.global_set(
            SCOPE_ADMIN_ABUSE_RECIPIENTS,
            id,
            serde_json::to_value(&record).map_err(|e| {
                PortError::Validation(format!("INVALID_PARAM recipient encode: {e}"))
            })?,
        )?;
        Ok(record)
    }

    /// `delete`: missing id is a no-op success.
    pub fn abuse_recipient_delete(&self, id: &str) -> Result<(), PortError> {
        self.global_remove(SCOPE_ADMIN_ABUSE_RECIPIENTS, id)
    }

    // --- proxy account / retention (global singletons) ---------------------------

    /// `admin/update-proxy-account` read half (`admin/proxy-account` show):
    /// the stored proxy account id, if any.
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
        let users = self.accounts.count_accounts().map_err(|_| {
            PortError::StorageUnavailable("stats need count_accounts".to_string())
        })?;
        let pending_jobs = self.jobs.pending_count()?;
        let federation = self.federation_instances()?;
        let local_emojis = self.emojis.all_local_emojis().map(|rows| rows.len()).unwrap_or(0);
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
        let rows = self.signin.list_signins(account_id, limit, None, None, None, None)?;
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
            return Err(PortError::Validation("INVALID_PARAM invalid host".to_string()));
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
            serde_json::to_value(&record)
                .map_err(|e| PortError::Validation(format!("INVALID_PARAM instance encode: {e}")))?,
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
            return Err(PortError::Validation("INVALID_PARAM invalid host".to_string()));
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
