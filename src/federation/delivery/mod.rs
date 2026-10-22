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

pub(crate) mod client;
pub(crate) mod errors;
pub(crate) mod resolve;
pub(crate) mod sign;
pub(crate) mod url;

pub use self::client::{SafeHttpClient, SafeHttpConfig, SafeHttpResponse};
pub use self::errors::{
    DEFAULT_DNS_TIMEOUT, DEFAULT_HTTP_TIMEOUT, DEFAULT_TOTAL_TIMEOUT, MAX_AP_JSON_BYTES,
    MAX_REDIRECTS, SendError, SendErrorKind,
};
pub use self::resolve::{
    fetch_and_upsert_remote_actor, fetch_quote_stamp_doc, fetch_remote_note,
    resolve_remote_public_key, upsert_remote_actor_from_doc, webfinger_discover_actor_uri,
};
pub use self::sign::{format_http_date, signed_get_json, signed_post_activity};
pub use self::url::{
    BoundedDnsResolver, DEFAULT_MAX_DNS_IN_FLIGHT, DnsResolver, DnsResolverPolicy, ParsedUrl,
    SystemResolver, configure_dns_pool, is_private_or_reserved_ip, parse_http_url,
    resolve_and_validate_url,
};

pub use self::client::set_extra_ca;
