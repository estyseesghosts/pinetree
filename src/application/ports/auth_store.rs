use crate::domain::id::PortError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiTokenRecord {
    /// Opaque token id (Misskey `MiAccessToken.id`, aidx). Assigned at mint;
    /// rows written before Batch O backfill it at open. Addressed by
    /// `i/revoke-token {tokenId}` and listed by `i/apps`.
    #[serde(default)]
    pub id: String,
    pub token_hash: String,
    pub account_id: String,
    pub permissions: Vec<String>,
    pub is_first_party: bool,
    pub name: Option<String>,
    pub description: Option<String>,
    pub icon_url: Option<String>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MiAuthSessionRecord {
    pub session: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub icon_url: Option<String>,
    pub callback: Option<String>,
    pub permissions: Vec<String>,
    pub account_id: Option<String>,
    pub token: Option<String>,
    pub fetched: bool,
    pub created_at_ms: i64,
    /// Approval deadline (Batch N WS7 hardening; upstream has no expiry).
    /// `None` = no deadline (rows written before expiry existed). New rows
    /// always carry `Some`; expired rows read as missing (deleted on read).
    #[serde(default)]
    pub expires_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyAuthSessionRecord {
    pub id: String,
    pub token: String,
    pub app_secret: String,
    pub app_name: String,
    pub permissions: Vec<String>,
    pub account_id: Option<String>,
    pub access_token: Option<String>,
    pub created_at_ms: i64,
    /// Approval deadline (Batch N WS7 hardening; upstream has no expiry).
    /// `None` = no deadline (rows written before expiry existed).
    #[serde(default)]
    pub expires_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorKeypairRecord {
    pub account_id: String,
    pub public_key_pem: String,
    pub private_key_pem: String,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemotePublicKeyRecord {
    pub key_id: String,
    pub actor_uri: String,
    pub account_id: Option<String>,
    pub public_key_pem: String,
    pub fetched_at_ms: i64,
}

pub trait AuthStore: Send + Sync {
    fn set_password_hash(&self, account_id: &str, password_hash: &str) -> Result<(), PortError>;
    fn get_password_hash(&self, account_id: &str) -> Result<Option<String>, PortError>;
    fn store_token(&self, record: ApiTokenRecord) -> Result<(), PortError>;
    fn get_token_by_hash(&self, token_hash: &str) -> Result<Option<ApiTokenRecord>, PortError>;
    /// Delete the token row addressed by `token_hash` (Phase 3
    /// `i/revoke-token`). A missing row is a no-op success.
    fn delete_token_by_hash(&self, token_hash: &str) -> Result<(), PortError>;
    /// Fetch one grant by its opaque id (Batch O `i/revoke-token`
    /// `{tokenId}`, `i/apps`). Rows minted before ids existed are backfilled
    /// at open, so every stored row resolves.
    fn get_token_by_id(&self, id: &str) -> Result<Option<ApiTokenRecord>, PortError>;
    /// All grants ever minted for `account_id`, oldest-first (Batch O
    /// `i/apps`, `i/regenerate-token`). No pagination: callers bound the
    /// account's live grants by construction (signin mints one row per
    /// login; see `MAX_SIGNIN_ROWS_PER_USER` for the analogous cap).
    fn tokens_for_account(&self, account_id: &str) -> Result<Vec<ApiTokenRecord>, PortError>;
    /// Delete every first-party (native) grant of `account_id` (Batch O
    /// `i/regenerate-token`: the single native token rotation). App grants
    /// (`is_first_party: false`) keep working. Returns rows removed.
    fn delete_account_first_party_tokens(&self, account_id: &str) -> Result<u64, PortError>;
    fn save_miauth_session(&self, session: MiAuthSessionRecord) -> Result<(), PortError>;
    fn get_miauth_session(
        &self,
        session_id: &str,
    ) -> Result<Option<MiAuthSessionRecord>, PortError>;
    fn consume_miauth_session(
        &self,
        session_id: &str,
    ) -> Result<Option<MiAuthSessionRecord>, PortError>;
    fn save_legacy_session(&self, session: LegacyAuthSessionRecord) -> Result<(), PortError>;
    fn get_legacy_session(&self, token: &str)
    -> Result<Option<LegacyAuthSessionRecord>, PortError>;
    fn take_legacy_session(
        &self,
        token: &str,
    ) -> Result<Option<LegacyAuthSessionRecord>, PortError>;
    fn get_actor_keypair(&self, account_id: &str) -> Result<Option<ActorKeypairRecord>, PortError> {
        let _ = account_id;
        Ok(None)
    }
    fn save_actor_keypair(&self, record: ActorKeypairRecord) -> Result<(), PortError> {
        let _ = record;
        Ok(())
    }
    /// Store `record` only if the account has no keypair yet, and return the keypair that is stored
    /// afterwards (the existing one when another caller won). Concurrent first fetches of an actor
    /// must all see one key, so stores should make this atomic; the default is get-then-save.
    fn save_actor_keypair_if_absent(
        &self,
        record: ActorKeypairRecord,
    ) -> Result<ActorKeypairRecord, PortError> {
        if let Some(existing) = self.get_actor_keypair(&record.account_id)? {
            return Ok(existing);
        }
        self.save_actor_keypair(record.clone())?;
        Ok(record)
    }
    fn get_remote_public_key(
        &self,
        key_id: &str,
    ) -> Result<Option<RemotePublicKeyRecord>, PortError> {
        let _ = key_id;
        Ok(None)
    }
    fn save_remote_public_key(&self, record: RemotePublicKeyRecord) -> Result<(), PortError> {
        let _ = record;
        Ok(())
    }
}

/// One signin attempt row (Batch O WP9; Misskey `MiSignin`: `id`, `userId`,
/// `ip varchar(128)`, `headers jsonb`, `success bool`, cascade-delete on
/// user). Written for password-signin success/failure (known account only,
/// like upstream `fail()`) and signup success. MiAuth and legacy-session
/// approvals are not signins upstream and write no row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SigninRecord {
    pub id: String,
    pub account_id: String,
    pub ip: String,
    pub headers: String,
    pub success: bool,
    pub created_at_ms: i64,
}

/// Bound on stored signin rows per account (Batch O WP9). Upstream keeps
/// every row; pinetree trims oldest-first in the same transaction so the
/// read path stays bounded (no FDB impact: the write is one row + bounded
/// deletes). No backfill possible — rows exist only from deploy forward.
pub const MAX_SIGNIN_ROWS_PER_USER: usize = 100;

pub trait SigninStore: Send + Sync {
    /// Insert one attempt row and trim the account's history to
    /// [`MAX_SIGNIN_ROWS_PER_USER`] oldest-first, atomically.
    fn record_signin(&self, record: SigninRecord) -> Result<(), PortError>;
    /// Newest-first page for `i/signin-history`. `limit` is clamped by the
    /// caller (1..=100, default 10). Cursor pairs mirror upstream
    /// (`sinceId`/`untilId` aidx ids, `sinceDate`/`untilDate` ms).
    #[allow(clippy::too_many_arguments)]
    fn list_signins(
        &self,
        account_id: &str,
        limit: usize,
        since_id: Option<&str>,
        until_id: Option<&str>,
        since_date: Option<i64>,
        until_date: Option<i64>,
    ) -> Result<Vec<SigninRecord>, PortError>;
}

/// One password-reset request row (Batch P; Misskey
/// `MiPasswordResetRequest`: `id` (aid, carries the 30-minute TTL —
/// `reset-password` rejects tokens older than 30 min by id timestamp),
/// `userId`, `token` (64-char). No expiry sweeper: rows are single-use
/// (deleted on consume) and unreadable rows fail closed (`NO_SUCH_CODE`
/// posture like `verify-email`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PasswordResetRequest {
    pub id: String,
    pub user_id: String,
    pub token: String,
}

/// Password-reset tokens (Batch P; `request-reset-password` /
/// `reset-password`). Single-use rows keyed by token.
pub trait PasswordResetStore: Send + Sync {
    /// Insert one request row (caller mints id + 64-char token).
    fn create_reset_request(&self, request: PasswordResetRequest) -> Result<(), PortError>;
    /// Look up by token (`None` when unknown or already consumed).
    fn find_reset_request(&self, token: &str) -> Result<Option<PasswordResetRequest>, PortError>;
    /// Delete one row by id (consume on successful reset; best-effort on
    /// expiry — missing rows are not an error).
    fn delete_reset_request(&self, id: &str) -> Result<(), PortError>;
}
