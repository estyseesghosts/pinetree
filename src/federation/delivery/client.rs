//! Safe federation HTTP client: curl-backed send, deadlines, framing
//! (moved from `src/federation/delivery.rs`).
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

mod config;
mod framing;
mod send;
#[cfg(test)]
mod tests;

pub use self::config::{SafeHttpConfig, SafeHttpResponse};
pub use self::send::{SafeHttpClient, set_extra_ca};
