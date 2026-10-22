use std::time::Duration;

use super::super::errors::{
    DEFAULT_DNS_TIMEOUT, DEFAULT_HTTP_TIMEOUT, DEFAULT_TOTAL_TIMEOUT, MAX_REDIRECTS,
};

/// Configuration for the untrusted-peer federation HTTP client.
#[derive(Debug, Clone)]
pub struct SafeHttpConfig {
    /// Per-operation socket timeout (connect, and each read/write).
    pub timeout: Duration,
    /// Overall deadline for one plain-HTTP request (connect + write + read).
    pub total_timeout: Duration,
    /// Bound on one DNS lookup.
    pub dns_timeout: Duration,
    pub max_redirects: usize,
    /// SSRF exemption ONLY for local federation testing (env
    /// `PINETREE_ALLOW_PRIVATE_IPS`): skips private/reserved-IP rejection.
    /// NEVER affects TLS certificate verification, which stays enforced.
    pub allow_private_ips: bool,
    pub allowed_hosts: Vec<String>,
    pub user_agent: String,
}

impl Default for SafeHttpConfig {
    fn default() -> Self {
        let allow_private = std::env::var("PINETREE_ALLOW_PRIVATE_IPS")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        Self {
            timeout: DEFAULT_HTTP_TIMEOUT,
            total_timeout: DEFAULT_TOTAL_TIMEOUT,
            dns_timeout: DEFAULT_DNS_TIMEOUT,
            max_redirects: MAX_REDIRECTS,
            allow_private_ips: allow_private,
            allowed_hosts: Vec::new(),
            user_agent: "Pinetree/0.1.0 (+https://misskey-hub.net)".to_string(),
        }
    }
}

/// Raw HTTP response returned by [`SafeHttpClient`].
#[derive(Debug, Clone)]
pub struct SafeHttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub final_url: String,
}

impl SafeHttpResponse {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}
