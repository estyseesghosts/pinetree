//! SSRF-safe URL parsing and validation (moved from `src/federation/delivery.rs`).
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

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs};
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

use super::client::SafeHttpConfig;
use super::errors::{SendError, SendErrorKind};

/// Decomposed HTTP(S) URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedUrl {
    pub scheme: String,
    pub host: String,
    pub port: u16,
    pub host_header: String,
    pub path_and_query: String,
}

/// Parse an `http://` or `https://` URL into its components.
pub fn parse_http_url(raw: &str) -> Result<ParsedUrl, String> {
    let trimmed = raw.trim();
    let (scheme_raw, rest) = trimmed
        .split_once("://")
        .ok_or_else(|| format!("URL missing scheme: {trimmed}"))?;
    let scheme = scheme_raw.to_ascii_lowercase();
    if scheme != "http" && scheme != "https" {
        return Err(format!("unsupported URL scheme: {scheme}"));
    }
    let rest_no_frag = rest.split('#').next().unwrap_or(rest);
    let (authority, path_part) = match rest_no_frag.find(['/', '?']) {
        Some(idx) => (&rest_no_frag[..idx], &rest_no_frag[idx..]),
        None => (rest_no_frag, "/"),
    };
    if authority.is_empty() || authority.contains('@') {
        return Err(format!("invalid URL authority: {authority}"));
    }
    let path_and_query = if path_part.starts_with('?') {
        format!("/{path_part}")
    } else if path_part.is_empty() {
        "/".to_string()
    } else {
        path_part.to_string()
    };

    let default_port = if scheme == "https" { 443 } else { 80 };
    let (host, port) = if authority.starts_with('[') {
        // IPv6 literal `[::1]:8080`
        let close = authority
            .find(']')
            .ok_or_else(|| format!("malformed IPv6 host in URL: {authority}"))?;
        let h = &authority[1..close];
        let after = &authority[close + 1..];
        let p = if let Some(p_str) = after.strip_prefix(':') {
            p_str
                .parse::<u16>()
                .map_err(|_| format!("invalid port in URL: {authority}"))?
        } else if after.is_empty() {
            default_port
        } else {
            return Err(format!("invalid authority in URL: {authority}"));
        };
        (h.to_ascii_lowercase(), p)
    } else if let Some((h, p_str)) = authority.rsplit_once(':') {
        let p = p_str
            .parse::<u16>()
            .map_err(|_| format!("invalid port in URL: {authority}"))?;
        (h.to_ascii_lowercase(), p)
    } else {
        (authority.to_ascii_lowercase(), default_port)
    };

    if host.is_empty() || port == 0 {
        return Err(format!("empty host or zero port in URL: {trimmed}"));
    }
    let host_header = if (scheme == "http" && port == 80) || (scheme == "https" && port == 443) {
        if host.contains(':') {
            format!("[{host}]")
        } else {
            host.clone()
        }
    } else if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };

    Ok(ParsedUrl {
        scheme,
        host,
        port,
        host_header,
        path_and_query,
    })
}

/// Returns `true` if `ip` is a loopback, private (RFC1918 / RFC4193),
/// link-local, CGNAT (`100.64.0.0/10`), multicast, broadcast, documentation,
/// or unspecified address.
pub fn is_private_or_reserved_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_private_or_reserved_ipv4(v4),
        IpAddr::V6(v6) => {
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return is_private_or_reserved_ipv4(mapped);
            }
            is_private_or_reserved_ipv6(v6)
        }
    }
}

pub(crate) fn is_private_or_reserved_ipv4(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    ip.is_loopback() // 127.0.0.0/8
        || ip.is_private() // 10/8, 172.16/12, 192.168/16
        || ip.is_link_local() // 169.254.0.0/16
        || ip.is_broadcast()
        || ip.is_multicast()
        || ip.is_unspecified()
        || ip.is_documentation()
        || o[0] == 0 // 0.0.0.0/8
        || (o[0] == 100 && (o[1] & 0b1100_0000) == 64) // 100.64.0.0/10 CGNAT
        || (o[0] == 192 && o[1] == 0 && o[2] == 0) // 192.0.0.0/24 IETF
        || (o[0] == 198 && (o[1] == 18 || o[1] == 19)) // 198.18.0.0/15 benchmarking
        || o[0] >= 240 // 240.0.0.0/4 reserved
}

pub(crate) fn is_private_or_reserved_ipv6(ip: Ipv6Addr) -> bool {
    let segs = ip.segments();
    ip.is_loopback() // ::1
        || ip.is_unspecified() // ::
        || ip.is_multicast() // ff00::/8
        || (segs[0] & 0xfe00) == 0xfc00 // fc00::/7 unique local
        || (segs[0] & 0xffc0) == 0xfe80 // fe80::/10 link-local
        || (segs[0] == 0x2001 && segs[1] == 0x0db8) // 2001:db8::/32 documentation
}

/// True when `host` (`host` or `host:port`, optionally bracketed IPv6) is an
/// IP literal rather than a DNS name.
pub(crate) fn is_ip_literal_host(host: &str) -> bool {
    let h = host.trim();
    let bare = h
        .strip_prefix('[')
        .and_then(|rest| rest.split(']').next())
        .unwrap_or_else(|| h.split(':').next().unwrap_or(h));
    bare.parse::<std::net::IpAddr>().is_ok()
}

/// Resolve a host on a helper thread and wait at most `timeout`. A lookup
/// that outlives the deadline is abandoned; its thread ends on its own when
/// the system resolver returns.
pub(crate) fn resolve_host_with_timeout(
    host: &str,
    port: u16,
    timeout: Duration,
) -> Result<Vec<SocketAddr>, SendError> {
    let (tx, rx) = std::sync::mpsc::channel::<std::io::Result<Vec<SocketAddr>>>();
    let lookup = format!("{host}:{port}");
    let _lookup_thread = std::thread::Builder::new()
        .name("pinetree-dns".to_string())
        .spawn(move || {
            let result = lookup
                .to_socket_addrs()
                .map(|addrs| addrs.collect::<Vec<SocketAddr>>());
            let _ = tx.send(result);
        })
        .map_err(|e| {
            SendError::new(
                SendErrorKind::Internal,
                format!("spawn DNS lookup for {host}: {e}"),
            )
        })?;
    match rx.recv_timeout(timeout) {
        Ok(Ok(addrs)) => Ok(addrs),
        Ok(Err(err)) => Err(classify_dns_error(host, &err)),
        Err(RecvTimeoutError::Timeout) => Err(SendError::new(
            SendErrorKind::Timeout,
            format!("DNS resolution timed out for {host} after {timeout:?}"),
        )),
        Err(RecvTimeoutError::Disconnected) => Err(SendError::new(
            SendErrorKind::Dns,
            format!("DNS resolution failed for {host}"),
        )),
    }
}

/// Map a resolver failure to a kind. NXDOMAIN is permanent; every other
/// resolver failure (temporary failure, server unreachable) is transient.
pub(crate) fn classify_dns_error(host: &str, err: &std::io::Error) -> SendError {
    const NXDOMAIN_MARKERS: [&str; 5] = [
        "name or service not known",
        "nodename nor servname",
        "no such host",
        "host not found",
        "unknown host",
    ];
    let text = err.to_string().to_ascii_lowercase();
    let kind = if NXDOMAIN_MARKERS.iter().any(|m| text.contains(m)) {
        SendErrorKind::DnsNotFound
    } else {
        SendErrorKind::Dns
    };
    SendError::new(kind, format!("DNS resolution failed for {host}: {err}"))
}

/// Resolve a URL's host and validate that none of its resolved socket
/// addresses violate SSRF rules.
pub fn resolve_and_validate_url(
    url: &str,
    cfg: &SafeHttpConfig,
) -> Result<(ParsedUrl, Vec<SocketAddr>), SendError> {
    let parsed = parse_http_url(url).map_err(|e| SendError::new(SendErrorKind::InvalidUrl, e))?;
    let host_allowed = cfg.allow_private_ips
        || cfg.allowed_hosts.iter().any(|h| {
            h.eq_ignore_ascii_case(&parsed.host) || h.eq_ignore_ascii_case(&parsed.host_header)
        });

    // Check literal IP first so we reject private literals before touching DNS.
    if let Ok(ip) = parsed.host.parse::<IpAddr>() {
        if !host_allowed && is_private_or_reserved_ip(ip) {
            return Err(SendError::new(
                SendErrorKind::SsrfBlocked,
                format!("SSRF blocked private/reserved IP {ip} for {url}"),
            ));
        }
        return Ok((parsed.clone(), vec![SocketAddr::new(ip, parsed.port)]));
    }

    let addrs = resolve_host_with_timeout(&parsed.host, parsed.port, cfg.dns_timeout)?;
    if addrs.is_empty() {
        return Err(SendError::new(
            SendErrorKind::Dns,
            format!("DNS resolution returned no addresses for {}", parsed.host),
        ));
    }
    if !host_allowed {
        for addr in &addrs {
            if is_private_or_reserved_ip(addr.ip()) {
                return Err(SendError::new(
                    SendErrorKind::SsrfBlocked,
                    format!(
                        "SSRF blocked resolved private/reserved IP {} for {}",
                        addr.ip(),
                        url
                    ),
                ));
            }
        }
    }
    Ok((parsed, addrs))
}

pub(crate) fn resolve_redirect_url(base: &ParsedUrl, location: &str) -> Result<String, String> {
    let loc = location.trim();
    if loc.starts_with("http://") || loc.starts_with("https://") {
        return Ok(loc.to_string());
    }
    if loc.starts_with('/') {
        return Ok(format!("{}://{}{}", base.scheme, base.host_header, loc));
    }
    Err(format!("unsupported relative redirect Location: {loc}"))
}
