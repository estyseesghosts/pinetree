//! Misskey auth: tokens, passwords, MiAuth helpers (Slice D2).
//!
//! Tokens are stored hashed (SHA-256 hex). Passwords are stored as Argon2id
//! PHC strings (`$argon2id$...`); legacy `salt:hash` SHA-256 hex values are
//! still verified and upgraded on the next successful signin (slice 9, H3).
//! Credentials arrive via `Authorization: Bearer`, the JSON body `"i"`
//! parameter, or the `pinetree_session` cookie.

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::domain::account::Account;

/// Argon2id cost parameters (canonical home: [`crate::domain::password`]).
/// Re-exported here so existing adapter callers keep working.
pub use crate::domain::password::{
    ARGON2_M_COST_KIB, ARGON2_P_COST, ARGON2_T_COST, hash_password, needs_rehash, verify_password,
};

use super::errors::ApiError;

/// Session cookie set by web sign-in for the MiAuth approval flow.
pub const SESSION_COOKIE_NAME: &str = "pinetree_session";

/// SHA-256 hex of a raw token (the form stored in `AuthStore`).
pub fn hash_token(raw_token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_token.as_bytes());
    hex::encode(hasher.finalize())
}

fn password_task_failed() -> ApiError {
    ApiError::from_port_error(PortError::StorageUnavailable(
        "password hashing task failed".into(),
    ))
}

/// [`hash_password`] on the blocking pool, so Argon2 never stalls the async
/// executor.
pub async fn hash_password_async(password: String) -> Result<String, ApiError> {
    tokio::task::spawn_blocking(move || hash_password(&password))
        .await
        .map_err(|_| password_task_failed())
}

/// [`verify_password`] on the blocking pool.
pub async fn verify_password_async(password: String, stored: String) -> Result<bool, ApiError> {
    tokio::task::spawn_blocking(move || verify_password(&password, &stored))
        .await
        .map_err(|_| password_task_failed())
}

/// 32-char secure random hex token (16 random bytes).
pub fn generate_random_token() -> String {
    let bytes: Vec<u8> = (0..16).map(|_| rand::random::<u8>()).collect();
    hex::encode(bytes)
}

/// Extract the raw token from an `Authorization` header value
/// (`Bearer <token>`, case-sensitive scheme per RFC 6750 §2.1).
pub fn token_from_authorization_header(value: &str) -> Option<String> {
    value
        .strip_prefix("Bearer ")
        .filter(|token| !token.is_empty())
        .map(|token| token.to_string())
}

/// Extract the raw credential from a JSON body (`"i"` parameter).
pub fn token_from_body(body: &Value) -> Option<String> {
    body.get("i")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .map(|token| token.to_string())
}

/// Extract the raw credential from a `Cookie` header value
/// (`pinetree_session=<token>`).
pub fn token_from_cookie_header(cookie_header: &str) -> Option<String> {
    cookie_header.split(';').find_map(|part| {
        let part = part.trim();
        part.strip_prefix(&format!("{SESSION_COOKIE_NAME}="))
            .filter(|token| !token.is_empty())
            .map(|token| token.to_string())
    })
}

/// Credential extraction order: `Authorization: Bearer` first, then the
/// JSON body `"i"` parameter, then the session cookie.
pub fn extract_raw_token(
    authorization_header: Option<&str>,
    body: Option<&Value>,
    cookie_header: Option<&str>,
) -> Option<String> {
    if let Some(header) = authorization_header {
        if let Some(token) = token_from_authorization_header(header) {
            return Some(token);
        }
    }
    if let Some(body) = body {
        if let Some(token) = token_from_body(body) {
            return Some(token);
        }
    }
    if let Some(cookies) = cookie_header {
        if let Some(token) = token_from_cookie_header(cookies) {
            return Some(token);
        }
    }
    None
}

/// Authenticated API caller: the resolved [`Account`], the token's
/// permission set, whether the credential is first-party (password
/// session, full permissions), and the presented raw token.
pub struct AuthenticatedCaller {
    pub account: Account,
    pub permissions: Vec<String>,
    pub is_first_party: bool,
    pub raw_token: String,
}

impl AuthenticatedCaller {
    /// Requires token permission `perm`; first-party callers pass through.
    pub fn require_permission(&self, perm: &str) -> Result<(), ApiError> {
        if self.is_first_party || self.permissions.iter().any(|p| p == perm) {
            Ok(())
        } else {
            Err(ApiError::permission_denied())
        }
    }

    /// Requires a first-party (secure) credential.
    pub fn require_secure(&self) -> Result<(), ApiError> {
        if self.is_first_party {
            Ok(())
        } else {
            Err(ApiError::permission_denied())
        }
    }
}

/// Batch M §3.1 guard: the caller must hold moderator powers (moderator or
/// admin — an admin implies moderator). Role checks live here, not in token
/// scopes: scopes alone are not authorization.
pub fn require_moderator(caller: &AuthenticatedCaller) -> Result<(), ApiError> {
    if caller.account.has_moderator_powers() {
        Ok(())
    } else {
        Err(ApiError::permission_denied())
    }
}

/// Batch M §3.1 guard: the caller must be an admin.
pub fn require_admin(caller: &AuthenticatedCaller) -> Result<(), ApiError> {
    if caller.account.is_admin {
        Ok(())
    } else {
        Err(ApiError::permission_denied())
    }
}

/// Resolve `raw_token` to its caller: token-hash lookup, account load,
/// suspended/deleted gating. Storage faults surface as 500 `ApiError`s.
pub fn authenticate(
    services: &AppServices,
    raw_token: &str,
) -> Result<AuthenticatedCaller, ApiError> {
    let record = services
        .auth
        .get_token_by_hash(&hash_token(raw_token))
        .map_err(ApiError::from_port_error)?;
    let record = record.ok_or_else(ApiError::authentication_failed)?;
    let account = services
        .accounts
        .find_by_id(&record.account_id)
        .map_err(ApiError::from_port_error)?
        .ok_or_else(ApiError::authentication_failed)?;
    if account.is_suspended {
        return Err(ApiError::user_suspended());
    }
    if account.is_deleted {
        return Err(ApiError::authentication_failed());
    }
    Ok(AuthenticatedCaller {
        account,
        permissions: record.permissions,
        is_first_party: record.is_first_party,
        raw_token: raw_token.to_string(),
    })
}

/// Schemes that must never appear in a MiAuth callback URL
/// (`pages/miauth.vue:52-66`).
const BLOCKED_CALLBACK_SCHEMES: &[&str] = &[
    "javascript:",
    "file:",
    "data:",
    "mailto:",
    "tel:",
    "vbscript:",
];

/// Whether `url` is safe to redirect to after MiAuth approval: blocks the
/// dangerous schemes above, allows `http(s)://` and any other custom app
/// scheme not on the blocklist.
pub fn is_safe_callback_url(url: &str) -> bool {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return false;
    }
    // Scheme = text before the first `:`, provided no `/`, `?`, or `#`
    // comes first (otherwise there is no scheme).
    let mut scheme_end = None;
    for (i, c) in trimmed.char_indices() {
        if c == ':' {
            scheme_end = Some(i);
            break;
        }
        if c == '/' || c == '?' || c == '#' {
            break;
        }
    }
    let scheme_end = match scheme_end {
        Some(end) => end,
        None => return false,
    };
    let scheme = trimmed[..scheme_end].to_ascii_lowercase();
    if scheme.is_empty() || scheme.contains(char::is_whitespace) {
        return false;
    }
    let with_colon = format!("{scheme}:");
    !BLOCKED_CALLBACK_SCHEMES.contains(&with_colon.as_str())
}

fn escape_html(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

/// Server-rendered MiAuth approval page: login/signup context plus an
/// "Approve" form posting to `POST /miauth/{session}`.
pub fn render_miauth_page(
    session: &str,
    name: Option<&str>,
    icon: Option<&str>,
    callback: Option<&str>,
    permission: Option<&str>,
    error_msg: Option<&str>,
) -> String {
    let session = escape_html(session);
    let app_name = escape_html(name.unwrap_or("An app"));
    let icon_img = match icon {
        Some(url) => format!(
            "<img src=\"{}\" alt=\"app icon\" class=\"icon\" />",
            escape_html(url)
        ),
        None => String::new(),
    };
    let callback_row = match callback {
        Some(url) => {
            let safe = is_safe_callback_url(url);
            format!(
                "<p class=\"callback\">After approval you will be redirected to:<br /><code>{}</code>{}</p>",
                escape_html(url),
                if safe {
                    String::new()
                } else {
                    " <strong>(this callback URL looks unsafe and will not be used)</strong>"
                        .to_string()
                }
            )
        }
        None => String::new(),
    };
    let permissions: Vec<String> = permission
        .unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(escape_html)
        .collect();
    let permission_list = if permissions.is_empty() {
        "<li>Basic access (no extra permissions)</li>".to_string()
    } else {
        permissions
            .iter()
            .map(|p| format!("<li><code>{p}</code></li>"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let error_banner = match error_msg {
        Some(msg) => format!("<p class=\"error\">{}</p>", escape_html(msg)),
        None => String::new(),
    };
    format!(
        "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\" />\n<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n<title>MiAuth — {app_name}</title>\n<style>\nbody {{ font-family: sans-serif; background: #f4f4f6; margin: 0; display: grid; place-content: center; min-height: 100vh; }}\n.card {{ background: #fff; border-radius: 12px; padding: 32px; max-width: 480px; width: calc(100vw - 64px); box-shadow: 0 8px 16px rgba(0,0,0,.1); }}\n.icon {{ width: 64px; height: 64px; border-radius: 12px; }}\n.error {{ color: #b00020; }}\n.callback code {{ font-size: 90%; word-break: break-all; }}\n.actions {{ display: flex; gap: 12px; margin-top: 16px; }}\nbutton {{ padding: 10px 20px; border-radius: 8px; border: none; cursor: pointer; }}\nbutton.approve {{ background: #86b300; color: #fff; }}\nbutton.deny {{ background: #eee; }}\n</style>\n</head>\n<body>\n<div class=\"card\">\n{icon_img}\n<h1>{app_name} wants to access your account</h1>\n{error_banner}\n<p>This app requests the following permissions:</p>\n<ul>\n{permission_list}\n</ul>\n{callback_row}\n<form method=\"post\" action=\"/miauth/{session}\">\n<div class=\"actions\">\n<button class=\"approve\" type=\"submit\" name=\"decision\" value=\"approve\">Approve</button>\n<button class=\"deny\" type=\"submit\" name=\"decision\" value=\"deny\">Deny</button>\n</div>\n</form>\n</div>\n</body>\n</html>"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use argon2::{
        Algorithm, Argon2, Params, Version,
        password_hash::{PasswordHasher, SaltString, rand_core::OsRng},
    };

    #[test]
    fn token_hash_is_stable_sha256_hex() {
        assert_eq!(hash_token("hello"), hash_token("hello"));
        assert_ne!(hash_token("hello"), hash_token("world"));
        assert_eq!(hash_token("").len(), 64);
    }

    #[test]
    fn password_roundtrip_and_salt_uniqueness() {
        let a = hash_password("s3cret!");
        let b = hash_password("s3cret!");
        assert_ne!(a, b, "salts must differ");
        assert!(verify_password("s3cret!", &a));
        assert!(verify_password("s3cret!", &b));
        assert!(!verify_password("wrong", &a));
        assert!(!verify_password("s3cret!", "not-a-hash"));
    }

    /// Pre-slice-9 `salt:sha256` hash, built the way the legacy hasher did.
    fn legacy_hash(salt_hex: &str, password: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(salt_hex.as_bytes());
        hasher.update(b":");
        hasher.update(password.as_bytes());
        format!("{salt_hex}:{}", hex::encode(hasher.finalize()))
    }

    #[test]
    fn slice9_legacy_salt_hash_verifies_and_needs_rehash() {
        let stored = legacy_hash("00112233445566778899aabbccddeeff", "legacy-pw");
        assert!(verify_password("legacy-pw", &stored));
        assert!(!verify_password("wrong", &stored));
        assert!(needs_rehash(&stored));
    }

    #[test]
    fn slice9_new_hashes_are_argon2id_phc() {
        let stored = hash_password("s3cret!");
        assert!(
            stored.starts_with("$argon2id$v=19$m=19456,t=4,p=1$"),
            "unexpected PHC prefix: {stored}"
        );
        assert!(verify_password("s3cret!", &stored));
        assert!(!verify_password("wrong", &stored));
        assert!(!needs_rehash(&stored));
    }

    #[test]
    fn slice9_outdated_argon2_params_need_rehash() {
        let params = Params::new(8, 1, 1, None).expect("valid test params");
        let salt = SaltString::generate(&mut OsRng);
        let weak = Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
            .hash_password(b"pw", &salt)
            .expect("weak hash")
            .to_string();
        assert!(needs_rehash(&weak));
        assert!(verify_password("pw", &weak));
    }

    #[tokio::test]
    async fn slice9_async_password_wrappers_round_trip() {
        let stored = hash_password_async("async-pw".to_string())
            .await
            .expect("hash task");
        assert!(stored.starts_with("$argon2id$"));
        assert!(
            verify_password_async("async-pw".to_string(), stored.clone())
                .await
                .expect("verify task")
        );
        assert!(
            !verify_password_async("nope".to_string(), stored)
                .await
                .expect("verify task")
        );
    }

    #[test]
    fn generated_tokens_are_32_chars() {
        let token = generate_random_token();
        assert_eq!(token.len(), 32);
        assert!(token.bytes().all(|b| b.is_ascii_hexdigit()));
        assert_ne!(token, generate_random_token());
    }

    #[test]
    fn credential_extraction_order() {
        let body = serde_json::json!({"i": "body-token"});
        assert_eq!(
            extract_raw_token(
                Some("Bearer header-token"),
                Some(&body),
                Some("pinetree_session=cookie-token")
            ),
            Some("header-token".to_string())
        );
        assert_eq!(
            extract_raw_token(None, Some(&body), Some("pinetree_session=cookie-token")),
            Some("body-token".to_string())
        );
        assert_eq!(
            extract_raw_token(None, None, Some("other=1; pinetree_session=cookie-token")),
            Some("cookie-token".to_string())
        );
        assert_eq!(extract_raw_token(None, None, None), None);
        // Scheme is case-sensitive: lowercase bearer is not a token.
        assert_eq!(token_from_authorization_header("bearer x"), None);
    }

    #[test]
    fn callback_url_blocklist() {
        for bad in [
            "javascript:alert(1)",
            "JaVaScRiPt:alert(1)",
            "file:///etc/passwd",
            "data:text/html,hi",
            "mailto:a@b.c",
            "tel:+123",
            "vbscript:msgbox(1)",
            "",
            "/relative/path",
        ] {
            assert!(!is_safe_callback_url(bad), "expected unsafe: {bad}");
        }
        for good in [
            "https://app.example/callback",
            "http://localhost:3000/cb",
            "myapp://callback",
        ] {
            assert!(is_safe_callback_url(good), "expected safe: {good}");
        }
    }

    #[test]
    fn miauth_page_posts_to_session() {
        let html = render_miauth_page(
            "sess123",
            Some("Cool App"),
            None,
            Some("https://app.example/cb"),
            Some("read:account,write:notes"),
            None,
        );
        assert!(html.contains("POST") || html.contains("post"));
        assert!(html.contains("/miauth/sess123"));
        assert!(html.contains("Cool App"));
        assert!(html.contains("write:notes"));
        // Untrusted app names are escaped.
        let evil = render_miauth_page("s", Some("<script>"), None, None, None, None);
        assert!(!evil.contains("<script>"));
    }
}
