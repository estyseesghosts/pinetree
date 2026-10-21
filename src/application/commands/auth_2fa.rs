//! WP2-part-C auth use cases: TOTP two-factor enrol/verify, the
//! developer app registry, and the WebAuthn decision record.
//!
//! Storage (binding constraint — no new ports/tables): per-account state
//! lives in the existing [`RegistryStore`] (`scope ["2fa"]` / `["apps"]`,
//! domain `None`). `signin-with-passkey` stays a loud 400: pinetree stores
//! no WebAuthn credentials and runs no ceremony, so graduating it would
//! need an RP/challenge store + attestation verification (proposed in the
//! subagent report; dispatcher stub unchanged).
//!
//! Error-code rule: Misskey code strings are preserved verbatim in the
//! `PortError` payload (`INCORRECT_PASSWORD`, `INVALID_TOKEN`,
//! `NO_SUCH_APP`); the dispatcher maps them via `from_port_error`. UUIDs
//! for codes pinetree mints here need parent pinning against the Misskey
//! checkout (misskey symlink is broken in this worktree).

use super::AppServices;

use crate::application::ports::{PortError, RegistryItem};
use crate::domain::admin::{
    AppRecord, TotpState, SCOPE_2FA, SCOPE_APPS, KEY_TOTP,
    SECURITY_KEY_UNSUPPORTED_REASON,
    validate_app_callback, validate_app_name, validate_totp_token_format,
};

const TOTP_STEP_SECS: u64 = 30;
const TOTP_DIGITS: u32 = 6;
const B32_ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

/// Enrolment payload (`i/2fa/register`, `i/2fa/update-key` res shape).
/// DIVERGENCE: `qr` carries the otpauth URL text, not a PNG data URL
/// (no image encoder vendored); clients render their own QR code.
#[derive(Debug, Clone)]
pub struct TotpEnrolment {
    pub qr: String,
    pub url: String,
    pub secret: String,
    pub label: String,
}

impl AppServices {
    // --- TOTP helpers ----------------------------------------------------

    fn totp_row(&self, account_id: &str) -> Result<Option<TotpState>, PortError> {
        let item = self.registry.registry_get(
            account_id,
            None,
            &vec![SCOPE_2FA.to_string()],
            KEY_TOTP,
        )?;
        match item {
            None => Ok(None),
            Some(item) => serde_json::from_value(item.value)
                .map(Some)
                .map_err(|e| PortError::Validation(format!("INVALID_PARAM corrupt 2fa row: {e}"))),
        }
    }

    fn store_totp_row(&self, account_id: &str, state: &TotpState) -> Result<(), PortError> {
        self.registry.registry_set(RegistryItem {
            id: self.ids.generate(),
            user_id: account_id.to_string(),
            domain: None,
            scope: vec![SCOPE_2FA.to_string()],
            key: KEY_TOTP.to_string(),
            value: serde_json::to_value(state)
                .map_err(|e| PortError::Validation(format!("INVALID_PARAM 2fa encode: {e}")))?,
            updated_at_ms: self.now_ms(),
        })
    }

    fn remove_totp_row(&self, account_id: &str) -> Result<(), PortError> {
        self.registry.registry_remove(
            account_id,
            None,
            &vec![SCOPE_2FA.to_string()],
            KEY_TOTP,
        )
    }

    fn check_password(&self, account_id: &str, password: &str) -> Result<(), PortError> {
        let stored = self.auth.get_password_hash(account_id)?;
        let stored = stored.as_deref().unwrap_or("");
        if !crate::api::misskey::auth::verify_password(password, stored) {
            return Err(PortError::Validation("INCORRECT_PASSWORD".to_string()));
        }
        Ok(())
    }

    fn enrol_totp(&self, account_id: &str, label: String) -> Result<TotpEnrolment, PortError> {
        let secret_bytes: [u8; 20] = rand::random();
        let secret = base32_encode(&secret_bytes);
        self.store_totp_row(
            account_id,
            &TotpState {
                secret_b32: secret.clone(),
                verified: false,
                passwordless: false,
            },
        )?;
        let issuer = host_of_public_url(&self.public_url);
        let url = format!(
            "otpauth://totp/{issuer}:{label}?secret={secret}&issuer={issuer}&digits=6&period=30&algorithm=SHA1"
        );
        Ok(TotpEnrolment {
            qr: url.clone(),
            url,
            secret,
            label,
        })
    }

    fn username_label(&self, account_id: &str) -> Result<String, PortError> {
        let account = self
            .accounts
            .find_by_id(account_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
        Ok(account.username)
    }

    /// `i/2fa/register` (upstream `register.ts`): password check, then mint
    /// a pending secret. Refused when a verified secret already exists.
    pub fn register_2fa(&self, account_id: &str, password: &str) -> Result<TotpEnrolment, PortError> {
        self.check_password(account_id, password)?;
        if self.totp_row(account_id)?.is_some_and(|s| s.verified) {
            return Err(PortError::Validation(
                "INVALID_PARAM 2fa already registered".to_string(),
            ));
        }
        let label = self.username_label(account_id)?;
        self.enrol_totp(account_id, label)
    }

    /// `i/2fa/key-done` (upstream `key-done.ts`): password + token verify,
    /// marks the pending secret verified.
    pub fn confirm_2fa_key(&self, account_id: &str, password: &str, token: &str) -> Result<(), PortError> {
        self.check_password(account_id, password)?;
        validate_totp_token_format(token)?;
        let mut state = self
            .totp_row(account_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_2FA".to_string()))?;
        if !verify_totp(&state.secret_b32, self.now_ms(), token) {
            return Err(PortError::Validation("INVALID_TOKEN".to_string()));
        }
        state.verified = true;
        self.store_totp_row(account_id, &state)
    }

    /// `i/2fa/done` (upstream `done.ts`): token-only confirm of a pending
    /// secret (the `register` response flow).
    pub fn done_2fa(&self, account_id: &str, token: &str) -> Result<(), PortError> {
        validate_totp_token_format(token)?;
        let mut state = self
            .totp_row(account_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_2FA".to_string()))?;
        if !verify_totp(&state.secret_b32, self.now_ms(), token) {
            return Err(PortError::Validation("INVALID_TOKEN".to_string()));
        }
        state.verified = true;
        self.store_totp_row(account_id, &state)
    }

    /// `i/2fa/password-less` (upstream `password-less.ts`): toggle login
    /// without password for a verified secret.
    pub fn set_2fa_passwordless(&self, account_id: &str, value: bool) -> Result<(), PortError> {
        let mut state = self.verified_totp(account_id)?;
        state.passwordless = value;
        self.store_totp_row(account_id, &state)
    }

    /// `i/2fa/unregister` (upstream `unregister.ts`): password check, drop
    /// the whole row.
    pub fn unregister_2fa(&self, account_id: &str, password: &str) -> Result<(), PortError> {
        self.check_password(account_id, password)?;
        if self.totp_row(account_id)?.is_none() {
            return Err(PortError::NotFound("NO_SUCH_2FA".to_string()));
        }
        self.remove_totp_row(account_id)
    }

    /// `i/2fa/update-key` (upstream `update-key.ts`): password check, rotate
    /// to a fresh pending secret (must be re-confirmed via `done`).
    pub fn rotate_2fa_key(
        &self,
        account_id: &str,
        password: &str,
    ) -> Result<TotpEnrolment, PortError> {
        self.check_password(account_id, password)?;
        self.verified_totp(account_id)?;
        let label = self.username_label(account_id)?;
        self.enrol_totp(account_id, label)
    }

    /// `i/2fa/authenticate` (signin-flow second step): verify `token`
    /// against the verified secret. No session is minted here; the
    /// dispatcher mints the token on success like the password path.
    pub fn authenticate_2fa(&self, account_id: &str, token: &str) -> Result<(), PortError> {
        validate_totp_token_format(token)?;
        let state = self.verified_totp(account_id)?;
        if !verify_totp(&state.secret_b32, self.now_ms(), token) {
            return Err(PortError::Validation("INVALID_TOKEN".to_string()));
        }
        Ok(())
    }

    /// `i/2fa/remove-key` (security-key removal): loud 400 — pinetree stores
    /// no WebAuthn credentials, so there is nothing to remove. Same reason
    /// as the `signin-with-passkey` stub.
    pub fn remove_2fa_security_key(
        &self,
        _account_id: &str,
        _credential_id: &str,
    ) -> Result<(), PortError> {
        Err(PortError::Validation(format!(
            "INVALID_PARAM {SECURITY_KEY_UNSUPPORTED_REASON}"
        )))
    }

    /// `admin/unset-mfa` (covers the `mfa` scope item): admin clears a
    /// user's TOTP row without their password. Missing row is a no-op
    /// success (upstream `unset-mfa` parity posture).
    pub fn admin_unset_2fa(&self, account_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(account_id)?;
        self.accounts
            .find_by_id(account_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
        self.remove_totp_row(account_id)
    }

    fn verified_totp(&self, account_id: &str) -> Result<TotpState, PortError> {
        match self.totp_row(account_id)? {
            Some(state) if state.verified => Ok(state),
            _ => Err(PortError::NotFound("NO_SUCH_2FA".to_string())),
        }
    }

    // --- App registry -----------------------------------------------------

    /// `app/create` (upstream `app/create.ts`): owner-scoped app row.
    /// DIVERGENCE: rows are stored per-creator in the registry until the
    /// global app table lands, so `show` is owner-scoped too.
    pub fn create_app(
        &self,
        account_id: &str,
        name: &str,
        description: Option<String>,
        callback_url: Option<&str>,
        permission: Vec<String>,
    ) -> Result<AppRecord, PortError> {
        validate_app_name(name)?;
        validate_app_callback(callback_url)?;
        for perm in &permission {
            if perm.trim().is_empty() || perm.chars().count() > 64 {
                return Err(PortError::Validation(
                    "INVALID_PARAM invalid permission entry".to_string(),
                ));
            }
        }
        let record = AppRecord {
            id: self.ids.generate(),
            name: name.to_string(),
            secret: random_secret(32),
            callback_url: callback_url.map(str::to_string),
            permission,
            description,
            created_at_ms: self.now_ms(),
        };
        self.registry.registry_set(RegistryItem {
            id: self.ids.generate(),
            user_id: account_id.to_string(),
            domain: None,
            scope: vec![SCOPE_APPS.to_string()],
            key: record.id.clone(),
            value: serde_json::to_value(&record)
                .map_err(|e| PortError::Validation(format!("INVALID_PARAM app encode: {e}")))?,
            updated_at_ms: self.now_ms(),
        })?;
        Ok(record)
    }

    /// `app/show` (upstream `app/show.ts`): owner-scoped lookup by id.
    /// Missing → verbatim `NO_SUCH_APP`.
    pub fn show_app(&self, account_id: &str, app_id: &str) -> Result<AppRecord, PortError> {
        let item = self
            .registry
            .registry_get(account_id, None, &vec![SCOPE_APPS.to_string()], app_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_APP".to_string()))?;
        serde_json::from_value(item.value)
            .map_err(|_| PortError::NotFound("NO_SUCH_APP".to_string()))
    }

    /// `my/apps` (upstream `my/apps.ts`): the caller's own app rows.
    pub fn list_my_apps(&self, account_id: &str) -> Result<Vec<AppRecord>, PortError> {
        let items =
            self.registry
                .registry_list(account_id, None, &vec![SCOPE_APPS.to_string()])?;
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            if let Ok(record) = serde_json::from_value::<AppRecord>(item.value) {
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
}

fn host_of_public_url(public_url: &str) -> String {
    let after_scheme = public_url.split_once("://").map(|(_, rest)| rest).unwrap_or(public_url);
    let host = after_scheme
        .split('/')
        .next()
        .unwrap_or("")
        .split('@')
        .next_back()
        .unwrap_or("");
    let host = host.split(':').next().unwrap_or("").trim();
    if host.is_empty() {
        "instance".to_string()
    } else {
        host.to_string()
    }
}

fn random_secret(len: usize) -> String {
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let buf: Vec<u8> = (0..len).map(|_| rand::random::<u8>()).collect();
    buf.iter().map(|b| CHARS[(*b as usize) % CHARS.len()] as char).collect()
}

/// RFC 4648 base32 encode (no padding), used for TOTP secrets.
pub fn base32_encode(bytes: &[u8]) -> String {
    let mut out = String::new();
    let mut acc: u32 = 0;
    let mut bits: u8 = 0;
    for &b in bytes {
        acc = (acc << 8) | u32::from(b);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(B32_ALPHABET[((acc >> bits) & 31) as usize] as char);
        }
    }
    if bits > 0 {
        out.push(B32_ALPHABET[((acc << (5 - bits)) & 31) as usize] as char);
    }
    out
}

/// RFC 4648 base32 decode (accepts padded or unpadded, case-insensitive).
pub fn base32_decode(input: &str) -> Result<Vec<u8>, PortError> {
    let mut out = Vec::new();
    let mut acc: u32 = 0;
    let mut bits: u8 = 0;
    for ch in input.chars() {
        if ch == '=' {
            break;
        }
        let val = B32_ALPHABET
            .iter()
            .position(|&c| c as char == ch.to_ascii_uppercase())
            .ok_or_else(|| PortError::Validation("INVALID_PARAM bad base32 secret".to_string()))?
            as u32;
        acc = (acc << 5) | val;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push(((acc >> bits) & 0xff) as u8);
        }
    }
    Ok(out)
}

/// RFC 6238 TOTP (SHA-1, 30 s step, 6 digits), ±1 step window.
/// Public so integration tests can mint tokens without duplicating crypto.
pub fn totp_token(secret_b32: &str, unix_secs: u64) -> Result<String, PortError> {
    let key_bytes = base32_decode(secret_b32)?;
    if key_bytes.is_empty() {
        return Err(PortError::Validation("INVALID_PARAM empty totp secret".to_string()));
    }
    let step = unix_secs / u64::from(TOTP_STEP_SECS);
    Ok(hotp(&key_bytes, step))
}

/// Verify `token` at `now_ms` with a ±1 step window.
pub fn verify_totp(secret_b32: &str, now_ms: i64, token: &str) -> bool {
    if validate_totp_token_format(token).is_err() {
        return false;
    }
    let now_secs = now_ms.div_euclid(1000) as u64;
    let step = now_secs / u64::from(TOTP_STEP_SECS);
    let key_bytes = match base32_decode(secret_b32) {
        Ok(bytes) if !bytes.is_empty() => bytes,
        _ => return false,
    };
    [step.saturating_sub(1), step, step.saturating_add(1)]
        .iter()
        .any(|&s| hotp(&key_bytes, s) == token)
}

fn hotp(key: &[u8], counter: u64) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, key);
    let tag = ring::hmac::sign(&key, &counter.to_be_bytes());
    let bytes = tag.as_ref();
    let offset = (bytes[bytes.len() - 1] & 0x0f) as usize;
    let code = ((u32::from(bytes[offset] & 0x7f) << 24)
        | (u32::from(bytes[offset + 1]) << 16)
        | (u32::from(bytes[offset + 2]) << 8)
        | u32::from(bytes[offset + 3]))
        % 10u32.pow(TOTP_DIGITS);
    let width = TOTP_DIGITS as usize;
    format!("{code:0>width$}")
}
