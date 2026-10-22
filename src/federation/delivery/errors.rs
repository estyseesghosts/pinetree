//! Federation transport errors and classification (moved from `src/federation/delivery.rs`).
//! Safe federation HTTP client, SSRF protection, and WebFinger + signed-GET
//! remote actor/note resolver (Phase 2 Slice C1).
//!
//! Enforces:
//! - Scheme check (`http` or `https` only)
//! - SSRF / private-IP rejection (loopback, RFC1918, link-local, CGNAT,
//!   multicast, unspecified, IPv6 unique-local/link-local/loopback, plus
//!   IPv4-mapped IPv6 addresses) unless explicitly allowed by an allowlist
//!   or `allow_private_ips` flag for local federation testing.
//!   `allow_private_ips` is an SSRF exemption ONLY: it never affects TLS
//!   certificate verification, which is always enforced on HTTPS requests.
//! - Connect and read timeouts (default 10s), an overall deadline for the
//!   plain-HTTP path (default 20s), and a bounded DNS lookup (default 5s)
//! - Typed [`SendError`]s: permanent kinds (SSRF, invalid URL, NXDOMAIN) are
//!   never retried; transient kinds (timeouts, 5xx/429 at the retry layer)
//!   are retried with backoff by the delivery worker
//! - Redirect limit (default ≤ 3 hops, rejecting `https` → `http` downgrades
//!   and re-validating every redirect target against SSRF rules)
//! - Response size limit (default 64 KiB for ActivityPub JSON, configurable
//!   cap for media downloads).

use std::net::ToSocketAddrs;
use std::time::Duration;

use super::client::{SafeHttpClient, SafeHttpConfig};
use super::url::resolve_and_validate_url;

/// Maximum size of an ActivityPub / WebFinger JSON document (64 KiB).
pub const MAX_AP_JSON_BYTES: usize = 65_536;

/// Default connect/read timeout for federation HTTP calls (10s).
pub const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// Default overall deadline for one plain-HTTP request: connect, write and
/// read together (20s).
pub const DEFAULT_TOTAL_TIMEOUT: Duration = Duration::from_secs(20);

/// Default bound on one DNS lookup (5s).
pub const DEFAULT_DNS_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum HTTP redirects followed on outbound federation requests.
pub const MAX_REDIRECTS: usize = 3;

/// Why a federation send failed. The kind drives the retry decision: see
/// [`SendErrorKind::is_transient`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendErrorKind {
    /// Destination resolved to a private/reserved address (SSRF guard). Permanent.
    SsrfBlocked,
    /// Not an `http(s)` URL, or an unparseable authority. Permanent.
    InvalidUrl,
    /// DNS reports that the name does not exist (NXDOMAIN). Permanent.
    DnsNotFound,
    /// Redirect refused (HTTPS downgrade, loop, unsupported target). Permanent.
    Redirect,
    /// Response or request exceeded its size cap. Permanent.
    TooLarge,
    /// DNS lookup failed for a reason other than NXDOMAIN. Transient.
    Dns,
    /// DNS lookup, connect, or the overall request deadline elapsed. Transient.
    Timeout,
    /// TCP connect refused or unreachable. Transient.
    Connect,
    /// Socket read or write failed mid-request. Transient.
    Io,
    /// TLS handshake or certificate failure. Transient.
    Tls,
    /// Malformed or truncated HTTP response from the peer. Transient.
    Protocol,
    /// Local failure (signing, key lookup, spawning curl). Transient.
    Internal,
}

impl SendErrorKind {
    /// `true` when retrying later may succeed.
    pub fn is_transient(self) -> bool {
        matches!(
            self,
            Self::Dns
                | Self::Timeout
                | Self::Connect
                | Self::Io
                | Self::Tls
                | Self::Protocol
                | Self::Internal
        )
    }
}

/// Typed failure from [`SafeHttpClient::send`] and the resolver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendError {
    pub kind: SendErrorKind,
    pub message: String,
}

impl SendError {
    pub fn new(kind: SendErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SendError {}

/// Callers that only need text (media and actor fetches) keep their
/// `Result<_, String>` signatures through `?`.
impl From<SendError> for String {
    fn from(err: SendError) -> String {
        err.message
    }
}

pub(crate) fn is_timeout_kind(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    )
}

pub(crate) fn io_send_error(context: String, err: &std::io::Error, timed_out: bool) -> SendError {
    let kind = if timed_out || is_timeout_kind(err.kind()) {
        SendErrorKind::Timeout
    } else {
        SendErrorKind::Io
    };
    SendError::new(kind, format!("{context}: {err}"))
}

/// Classify a failed response read. Size-cap messages are permanent; anything
/// else from the peer's bytes is a protocol fault and transient.
pub(crate) fn response_error(message: String, timed_out: bool) -> SendError {
    if timed_out {
        SendError::new(SendErrorKind::Timeout, message)
    } else if message.contains("exceeded") || message.contains("exceeds") {
        SendError::new(SendErrorKind::TooLarge, message)
    } else {
        SendError::new(SendErrorKind::Protocol, message)
    }
}

/// Map a curl exit code to a kind (see curl's exit-code table).
pub(crate) fn curl_error(code: Option<i32>, detail: &str) -> SendError {
    let kind = match code {
        Some(28) => SendErrorKind::Timeout,
        Some(6) => SendErrorKind::Dns,
        Some(7) => SendErrorKind::Connect,
        Some(35 | 51 | 58 | 60 | 77) => SendErrorKind::Tls,
        Some(63) => SendErrorKind::TooLarge,
        _ => SendErrorKind::Io,
    };
    let code_text = code.map_or_else(|| "signal".to_string(), |c| c.to_string());
    SendError::new(kind, format!("curl failed (exit {code_text}): {detail}"))
}

#[cfg(test)]
mod slice10_transport_tests {
    use super::super::url::classify_dns_error;
    use super::*;
    use std::io::{self, ErrorKind};
    use std::net::TcpListener;
    use std::time::{Duration, Instant};

    fn strict_cfg() -> SafeHttpConfig {
        SafeHttpConfig {
            allow_private_ips: false,
            allowed_hosts: Vec::new(),
            ..Default::default()
        }
    }

    #[test]
    fn slice10_ssrf_and_invalid_url_are_permanent() {
        let ssrf =
            resolve_and_validate_url("http://127.0.0.1:3000/inbox", &strict_cfg()).unwrap_err();
        assert_eq!(ssrf.kind, SendErrorKind::SsrfBlocked);
        assert!(!ssrf.kind.is_transient());

        let metadata =
            resolve_and_validate_url("http://169.254.169.254/latest", &strict_cfg()).unwrap_err();
        assert_eq!(metadata.kind, SendErrorKind::SsrfBlocked);

        let bad_scheme =
            resolve_and_validate_url("ftp://93.184.216.34/file", &strict_cfg()).unwrap_err();
        assert_eq!(bad_scheme.kind, SendErrorKind::InvalidUrl);
        assert!(!bad_scheme.kind.is_transient());
    }

    #[test]
    fn slice10_dns_nxdomain_is_permanent_other_dns_failures_transient() {
        let nxdomain = io::Error::new(
            ErrorKind::Other,
            "failed to lookup address information: Name or service not known",
        );
        let nx = classify_dns_error("gone.example", &nxdomain);
        assert_eq!(nx.kind, SendErrorKind::DnsNotFound);
        assert!(!nx.kind.is_transient());

        let macos = io::Error::new(
            ErrorKind::Other,
            "nodename nor servname provided, or not known",
        );
        assert_eq!(
            classify_dns_error("gone.example", &macos).kind,
            SendErrorKind::DnsNotFound
        );

        let temporary = io::Error::new(
            ErrorKind::Other,
            "failed to lookup address information: Temporary failure in name resolution",
        );
        let flaky = classify_dns_error("flaky.example", &temporary);
        assert_eq!(flaky.kind, SendErrorKind::Dns);
        assert!(flaky.kind.is_transient());
    }

    #[test]
    fn slice10_transient_and_permanent_kinds_partition() {
        for kind in [
            SendErrorKind::Dns,
            SendErrorKind::Timeout,
            SendErrorKind::Connect,
            SendErrorKind::Io,
            SendErrorKind::Tls,
            SendErrorKind::Protocol,
            SendErrorKind::Internal,
        ] {
            assert!(kind.is_transient(), "{kind:?} must be transient");
        }
        for kind in [
            SendErrorKind::SsrfBlocked,
            SendErrorKind::InvalidUrl,
            SendErrorKind::DnsNotFound,
            SendErrorKind::Redirect,
            SendErrorKind::TooLarge,
        ] {
            assert!(!kind.is_transient(), "{kind:?} must be permanent");
        }
    }

    #[test]
    fn slice10_curl_exit_codes_map_to_kinds() {
        assert_eq!(
            curl_error(Some(28), "timed out").kind,
            SendErrorKind::Timeout
        );
        assert_eq!(curl_error(Some(7), "refused").kind, SendErrorKind::Connect);
        assert_eq!(curl_error(Some(60), "cert").kind, SendErrorKind::Tls);
        let too_big = curl_error(Some(63), "max-filesize");
        assert_eq!(too_big.kind, SendErrorKind::TooLarge);
        assert!(!too_big.kind.is_transient());
        assert!(curl_error(Some(28), "x").kind.is_transient());
        assert_eq!(curl_error(None, "killed").kind, SendErrorKind::Io);
    }

    #[test]
    fn slice10_response_errors_classify_size_timeout_and_protocol() {
        assert_eq!(
            response_error("response body exceeded max size of 5 bytes".into(), false).kind,
            SendErrorKind::TooLarge
        );
        assert_eq!(
            response_error("truncated chunked body".into(), false).kind,
            SendErrorKind::Protocol
        );
        assert_eq!(
            response_error("read response body: x".into(), true).kind,
            SendErrorKind::Timeout
        );
    }

    #[test]
    fn slice10_send_error_text_is_unchanged_for_string_callers() {
        let err = SendError::new(SendErrorKind::Timeout, "connect x: timed out");
        assert_eq!(err.to_string(), "connect x: timed out");
        let as_text: String = err.into();
        assert_eq!(as_text, "connect x: timed out");
    }

    #[test]
    fn slice10_total_deadline_bounds_a_silent_peer() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind silent peer");
        let addr = listener.local_addr().expect("silent addr");
        // Accept, then stay silent far longer than the deadline.
        std::thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                std::thread::sleep(Duration::from_secs(3));
                drop(stream);
            }
        });
        let client = SafeHttpClient::new(SafeHttpConfig {
            allow_private_ips: true,
            timeout: Duration::from_secs(10),
            total_timeout: Duration::from_millis(300),
            ..Default::default()
        });
        let started = Instant::now();
        let err = client
            .send("GET", &format!("http://{addr}/silent"), &[], None, 1024)
            .unwrap_err();
        assert_eq!(err.kind, SendErrorKind::Timeout, "got {err}");
        assert!(err.kind.is_transient());
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "overall deadline not enforced: {:?}",
            started.elapsed()
        );
    }
}
