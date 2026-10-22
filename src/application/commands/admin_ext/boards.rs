use crate::application::commands::AppServices;

use crate::application::ports::{PortError, RegistryItem};
use crate::domain::account::Account;
use crate::domain::admin::{
    AbuseRecipientRecord, AdRecord, AnnouncementRecord, GLOBAL_SETTINGS_NAMESPACE, RelayRecord,
    RoleRecord, SCOPE_ADMIN_ABUSE_RECIPIENTS, SCOPE_ADMIN_ADS, SCOPE_ADMIN_ANNOUNCEMENTS,
    SCOPE_ADMIN_RELAYS, SCOPE_ADMIN_ROLE_MEMBERS, SCOPE_ADMIN_ROLES, validate_ad,
    validate_announcement, validate_recipient, validate_relay_inbox, validate_role,
};

fn global_scope(scope: &str) -> Vec<String> {
    vec![scope.to_string()]
}

impl AppServices {
    pub(crate) fn global_get(
        &self,
        scope: &str,
        key: &str,
    ) -> Result<Option<serde_json::Value>, PortError> {
        Ok(self
            .registry
            .registry_get(GLOBAL_SETTINGS_NAMESPACE, None, &global_scope(scope), key)?
            .map(|item| item.value))
    }

    pub(crate) fn global_set(
        &self,
        scope: &str,
        key: &str,
        value: serde_json::Value,
    ) -> Result<(), PortError> {
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

    pub(crate) fn global_remove(&self, scope: &str, key: &str) -> Result<(), PortError> {
        self.registry
            .registry_remove(GLOBAL_SETTINGS_NAMESPACE, None, &global_scope(scope), key)
    }

    pub(crate) fn global_list<T>(&self, scope: &str) -> Result<Vec<(String, T)>, PortError>
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
            serde_json::to_value(&record).map_err(|e| {
                PortError::Validation(format!("INVALID_PARAM announcement encode: {e}"))
            })?,
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
            serde_json::to_value(&record).map_err(|e| {
                PortError::Validation(format!("INVALID_PARAM announcement encode: {e}"))
            })?,
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
            serde_json::to_value(user_ids).map_err(|e| {
                PortError::Validation(format!("INVALID_PARAM role members encode: {e}"))
            })?,
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
        let mut rows: Vec<AbuseRecipientRecord> = self
            .global_list::<AbuseRecipientRecord>(SCOPE_ADMIN_ABUSE_RECIPIENTS)?
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
}
