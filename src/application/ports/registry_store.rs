use crate::domain::id::PortError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One client registry item (Batch P; Misskey `MiRegistryItem`: `id`,
/// `userId`, `domain` (nullable; an access-token id when written with a
/// token-scoped call, else the explicit `domain` param or null), `scope`
/// (string array), `key`, `value` (arbitrary JSON), `updatedAt`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistryItem {
    pub id: String,
    pub user_id: String,
    pub domain: Option<String>,
    pub scope: Vec<String>,
    pub key: String,
    pub value: Value,
    pub updated_at_ms: i64,
}

/// Client key-value registry (Batch P; `i/registry/*`). Upstream
/// `RegistryApiService` semantics: one row per
/// (user, domain, scope, key); `set` upserts; reads with a non-null
/// domain are token/app-scoped, null-domain rows are shared. Scope
/// segments are validated by the caller (`^[a-zA-Z0-9_]+$`).
pub trait RegistryStore: Send + Sync {
    /// Upsert one item (insert or replace value + `updated_at_ms`).
    fn registry_set(&self, item: RegistryItem) -> Result<(), PortError>;
    /// Fetch one item by (user, domain, scope, key).
    fn registry_get(
        &self,
        user_id: &str,
        domain: Option<&str>,
        scope: &[String],
        key: &str,
    ) -> Result<Option<RegistryItem>, PortError>;
    /// All items of one (user, domain, scope).
    fn registry_list(
        &self,
        user_id: &str,
        domain: Option<&str>,
        scope: &[String],
    ) -> Result<Vec<RegistryItem>, PortError>;
    /// Delete one item (missing rows are not an error, like upstream
    /// `remove`).
    fn registry_remove(
        &self,
        user_id: &str,
        domain: Option<&str>,
        scope: &[String],
        key: &str,
    ) -> Result<(), PortError>;
    /// Distinct (domain, scope) pairs holding at least one item for the
    /// user (`scopes-with-domain`).
    fn registry_scopes(
        &self,
        user_id: &str,
    ) -> Result<Vec<(Option<String>, Vec<String>)>, PortError>;
    /// Atomic second-factor consume: validate the challenge, enforce the
    /// cross-challenge replay guard, then consume the challenge and record
    /// the step in one write transaction.
    fn consume_totp_login(
        &self,
        account_id: &str,
        challenge: &str,
        matched_step: u64,
        now_ms: i64,
    ) -> Result<(), PortError>;
}
