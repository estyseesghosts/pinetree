//! WP2-part-C misc endpoints: `ping`, `retention`, `reset-db`
//! (refused), `sw/*` push subscriptions, `v2/admin/emoji/list`, and the
//! `endpoint`/`endpoints` self-description.
//!
//! Storage (binding constraint — no new ports/tables): push subscriptions
//! live in the existing [`RegistryStore`] under the owning account
//! (`scope ["sw"]`); retention policy is a global registry singleton
//! under [`GLOBAL_SETTINGS_NAMESPACE`].

use super::AppServices;

use crate::application::ports::{PortError, RegistryItem, TimelineCursor};
use crate::domain::admin::{
    GLOBAL_SETTINGS_NAMESPACE, KEY_RETENTION, PushSubscription, RESET_DB_REFUSED_REASON,
    SCOPE_ADMIN_META, SCOPE_SW, validate_push_subscription,
};

/// `ping` (upstream `ping.ts`): connectivity probe, no credential.
/// Answers `{}` (empty object).
pub fn ping() -> serde_json::Value {
    serde_json::json!({})
}

impl AppServices {
    // --- retention ---------------------------------------------------------

    /// `retention` (read half): the stored retention policy object, or `{}`
    /// when none was set. DIVERGENCE: upstream has no `retention` read
    /// endpoint in the pinned checkout family; this get/set pair is the
    /// pinetree retention-policy surface until the drive-retention job
    /// lands (proposed in the subagent report).
    pub fn retention_get(&self) -> Result<serde_json::Value, PortError> {
        Ok(self
            .registry
            .registry_get(
                GLOBAL_SETTINGS_NAMESPACE,
                None,
                &vec![SCOPE_ADMIN_META.to_string()],
                KEY_RETENTION,
            )?
            .map(|item| item.value)
            .unwrap_or_else(|| serde_json::json!({})))
    }

    /// `retention` (write half): replace the stored policy object (must be
    /// a JSON object; empty `{}` clears back to default).
    pub fn retention_set(&self, policy: serde_json::Value) -> Result<serde_json::Value, PortError> {
        if !policy.is_object() {
            return Err(PortError::Validation(
                "INVALID_PARAM policy must be a JSON object".to_string(),
            ));
        }
        self.registry.registry_set(RegistryItem {
            id: self.ids.generate(),
            user_id: GLOBAL_SETTINGS_NAMESPACE.to_string(),
            domain: None,
            scope: vec![SCOPE_ADMIN_META.to_string()],
            key: KEY_RETENTION.to_string(),
            value: policy.clone(),
            updated_at_ms: self.now_ms(),
        })?;
        Ok(policy)
    }

    /// `reset-db`: always refused (development-only destructive reset with
    /// no single-node equivalent). The dispatcher maps this to a loud 400.
    pub fn refuse_reset_db(&self) -> Result<(), PortError> {
        Err(PortError::Validation(format!(
            "INVALID_PARAM {RESET_DB_REFUSED_REASON}"
        )))
    }

    // --- push subscriptions (`sw/*`) ------------------------------------------

    fn sw_key(endpoint: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        endpoint.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }

    /// `sw/register` (upstream `sw/register.ts`): store one subscription
    /// for the caller, keyed by endpoint hash. Idempotent overwrite.
    pub fn sw_register(
        &self,
        account_id: &str,
        endpoint: &str,
        auth: &str,
        publickey: &str,
    ) -> Result<PushSubscription, PortError> {
        validate_push_subscription(endpoint, auth, publickey)?;
        let record = PushSubscription {
            endpoint: endpoint.to_string(),
            auth: auth.to_string(),
            publickey: publickey.to_string(),
        };
        self.registry.registry_set(RegistryItem {
            id: self.ids.generate(),
            user_id: account_id.to_string(),
            domain: None,
            scope: vec![SCOPE_SW.to_string()],
            key: Self::sw_key(endpoint),
            value: serde_json::to_value(&record)
                .map_err(|e| PortError::Validation(format!("INVALID_PARAM sw encode: {e}")))?,
            updated_at_ms: self.now_ms(),
        })?;
        Ok(record)
    }

    /// `sw/show-registration` (upstream `sw/show-registration.ts`): fetch
    /// one subscription by endpoint. Missing → `NO_SUCH_SUBSCRIPTION`.
    pub fn sw_show(&self, account_id: &str, endpoint: &str) -> Result<PushSubscription, PortError> {
        self.registry
            .registry_get(
                account_id,
                None,
                &vec![SCOPE_SW.to_string()],
                &Self::sw_key(endpoint),
            )?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_SUBSCRIPTION".to_string()))
            .and_then(|item| {
                serde_json::from_value(item.value)
                    .map_err(|_| PortError::NotFound("NO_SUCH_SUBSCRIPTION".to_string()))
            })
    }

    /// `sw/unregister` (upstream `sw/unregister.ts`): drop one
    /// subscription by endpoint. Missing is a no-op success.
    pub fn sw_unregister(&self, account_id: &str, endpoint: &str) -> Result<(), PortError> {
        self.registry.registry_remove(
            account_id,
            None,
            &vec![SCOPE_SW.to_string()],
            &Self::sw_key(endpoint),
        )
    }

    /// `sw/update-registration` (upstream `sw/update-registration.ts`):
    /// replace the auth/publickey of an existing subscription.
    pub fn sw_update(
        &self,
        account_id: &str,
        endpoint: &str,
        auth: &str,
        publickey: &str,
    ) -> Result<PushSubscription, PortError> {
        validate_push_subscription(endpoint, auth, publickey)?;
        self.sw_show(account_id, endpoint)?;
        self.sw_register(account_id, endpoint, auth, publickey)
    }

    // --- `v2/admin/emoji/list` ---------------------------------------------------
    //
    /// `v2/admin/emoji/list` (upstream `v2/admin/emoji/list.ts`): local
    /// emoji by default; a `host` filter (non-local value) selects the
    /// remote-emoji list instead. Same cursor shape as `admin/emoji/list`.
    pub fn list_emojis_v2(
        &self,
        query: Option<&str>,
        host: Option<&str>,
        cursor: &TimelineCursor,
    ) -> Result<EmojisV2Page, PortError> {
        let local_host = self.public_url_host();
        let remote = match host {
            Some(h) if !h.trim().is_empty() && Some(h) != local_host.as_deref() => true,
            _ => false,
        };
        if remote {
            let rows = self.emojis.list_remote_emojis(query, host, cursor)?;
            Ok(EmojisV2Page::Remote(
                rows.into_iter()
                    .map(|row| {
                        serde_json::json!({
                            "id": format!("{}:{}", row.host, row.shortcode),
                            "name": row.shortcode,
                            "host": row.host,
                            "url": row.remote_url,
                        })
                    })
                    .collect(),
            ))
        } else {
            let rows = self.emojis.list_emojis(query, cursor)?;
            Ok(EmojisV2Page::Local(
                rows.into_iter()
                    .map(|row| {
                        serde_json::json!({
                            "id": row.id,
                            "name": row.shortcode,
                            "host": null,
                        })
                    })
                    .collect(),
            ))
        }
    }

    fn public_url_host(&self) -> Option<String> {
        let after_scheme = self.public_url.split_once("://").map(|(_, rest)| rest)?;
        let host = after_scheme
            .split('/')
            .next()
            .unwrap_or("")
            .split(':')
            .next()
            .unwrap_or("");
        if host.is_empty() {
            None
        } else {
            Some(host.to_ascii_lowercase())
        }
    }

    // --- `endpoint` / `endpoints` -------------------------------------------------
    //
    /// `endpoints` (upstream `endpoints.ts`): names of every Misskey API
    /// endpoint this server answers (i.e. every dispatcher arm). The list
    /// is static and MUST be kept in sync with the dispatcher by the parent
    /// merge (the authoritative arm list lives in `endpoints/mod.rs`).
    pub fn endpoint_names(&self) -> Vec<&'static str> {
        ENDPOINT_NAMES.to_vec()
    }

    /// `endpoint` (upstream `endpoint.ts`): self-description for one
    /// endpoint. Returns the name back; unknown names are
    /// `NO_SUCH_ENDPOINT`. DIVERGENCE: no per-param schema (param defs live
    /// in the dispatcher field checks, not in a registry).
    pub fn endpoint_describe(&self, name: &str) -> Result<serde_json::Value, PortError> {
        if ENDPOINT_NAMES.contains(&name) {
            Ok(serde_json::json!({ "name": name }))
        } else {
            Err(PortError::NotFound("NO_SUCH_ENDPOINT".to_string()))
        }
    }
}

/// `v2/admin/emoji/list` result page (local or remote rows, packed by the
/// dispatcher into `EmojiDetailed`).
#[derive(Debug)]
pub enum EmojisV2Page {
    Local(Vec<serde_json::Value>),
    Remote(Vec<serde_json::Value>),
}

/// Static dispatcher-arm inventory for `endpoints` (parent-owned
/// dispatcher is authoritative; this list mirrors it for WP2-part-C arms —
/// parent extends it to the full arm list at merge).
pub const ENDPOINT_NAMES: &[&str] = &[
    // WP2-part-C: auth/2fa + apps.
    "i/2fa/register",
    "i/2fa/key-done",
    "i/2fa/done",
    "i/2fa/password-less",
    "i/2fa/unregister",
    "i/2fa/update-key",
    "i/2fa/remove-key",
    "i/2fa/authenticate",
    "my/apps",
    "app/create",
    "app/show",
    // WP2-part-C: misc.
    "ping",
    "retention",
    "reset-db",
    "sw/register",
    "sw/show-registration",
    "sw/unregister",
    "sw/update-registration",
    "v2/admin/emoji/list",
    "endpoint",
    "endpoints",
    // WP2-part-C: admin families.
    "admin/roles/create",
    "admin/roles/delete",
    "admin/roles/list",
    "admin/roles/show",
    "admin/roles/update",
    "admin/roles/assign",
    "admin/roles/unassign",
    "admin/roles/users",
    "admin/queue/stats",
    "admin/queue/deliver-delayed",
    "admin/queue/inbox-delayed",
    "admin/queue/clear",
    "admin/relays/add",
    "admin/relays/list",
    "admin/relays/remove",
    "admin/ad/create",
    "admin/ad/list",
    "admin/ad/update",
    "admin/ad/delete",
    "admin/ad/refresh",
    "admin/announcements/create",
    "admin/announcements/list",
    "admin/announcements/update",
    "admin/announcements/delete",
    "admin/federation/list",
    "admin/federation/update-instance",
    "admin/federation/refresh-remote-instance-metadata",
    "admin/federation/show-instance",
    "admin/stats",
    "admin/get-user-ips",
    "admin/accounts/find-by-email",
    "admin/server-info",
    "admin/update-proxy-account",
    "admin/proxy-account",
    "admin/unset-mfa",
    "admin/reset-password",
    "federation/instances",
    "federation/show-instance",
    "federation/update-instance",
    "federation/refresh-remote-instance-metadata",
    "federation/find-by-email",
    "federation/get-user-ips",
    "queue/stats",
    "queue/deliver-delayed",
    "abuse-report-notification-recipient/create",
    "abuse-report-notification-recipient/list",
    "abuse-report-notification-recipient/show",
    "abuse-report-notification-recipient/update",
    "abuse-report-notification-recipient/delete",
];
