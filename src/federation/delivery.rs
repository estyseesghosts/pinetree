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
//! - Connect and read timeouts (default 10s)
//! - Redirect limit (default ≤ 3 hops, rejecting `https` → `http` downgrades
//!   and re-validating every redirect target against SSRF rules)
//! - Response size limit (default 64 KiB for ActivityPub JSON, configurable
//!   cap for media downloads).

use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream, ToSocketAddrs};
use std::process::{Command, Stdio};
use std::time::Duration;

use chrono::Utc;
use serde_json::Value;

use crate::activitypub::actor::{ParsedRemoteActor, parse_remote_actor, parse_url_host};
use crate::activitypub::objects::{AP_CONTENT_TYPE, ParsedRemoteNote, parse_remote_note};
use crate::activitypub::signatures::{actor_uri_from_key_id, sign_request_headers};
use crate::application::commands::AppServices;
use crate::application::ports::{CreateAccountInput, PortError, UpdateAccountPatch};
use crate::domain::account::Account;

/// Maximum size of an ActivityPub / WebFinger JSON document (64 KiB).
pub const MAX_AP_JSON_BYTES: usize = 65_536;
/// Default connect/read timeout for federation HTTP calls (10s).
pub const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(10);
/// Maximum HTTP redirects followed on outbound federation requests.
pub const MAX_REDIRECTS: usize = 3;

/// Configuration for the untrusted-peer federation HTTP client.
#[derive(Debug, Clone)]
pub struct SafeHttpConfig {
    pub timeout: Duration,
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
            max_redirects: MAX_REDIRECTS,
            allow_private_ips: allow_private,
            allowed_hosts: Vec::new(),
            user_agent: "Pinetree/0.1.0 (+https://misskey-hub.net)".to_string(),
        }
    }
}

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

fn is_private_or_reserved_ipv4(ip: Ipv4Addr) -> bool {
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

fn is_private_or_reserved_ipv6(ip: Ipv6Addr) -> bool {
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
fn is_ip_literal_host(host: &str) -> bool {
    let h = host.trim();
    let bare = h
        .strip_prefix('[')
        .and_then(|rest| rest.split(']').next())
        .unwrap_or_else(|| h.split(':').next().unwrap_or(h));
    bare.parse::<std::net::IpAddr>().is_ok()
}

/// Resolve a URL's host and validate that none of its resolved socket
/// addresses violate SSRF rules.
pub fn resolve_and_validate_url(
    url: &str,
    cfg: &SafeHttpConfig,
) -> Result<(ParsedUrl, Vec<SocketAddr>), String> {
    let parsed = parse_http_url(url)?;
    let host_allowed = cfg.allow_private_ips
        || cfg
            .allowed_hosts
            .iter()
            .any(|h| h.eq_ignore_ascii_case(&parsed.host) || h.eq_ignore_ascii_case(&parsed.host_header));

    // Check literal IP first so we reject private literals before touching DNS.
    if let Ok(ip) = parsed.host.parse::<IpAddr>() {
        if !host_allowed && is_private_or_reserved_ip(ip) {
            return Err(format!("SSRF blocked private/reserved IP {ip} for {url}"));
        }
        return Ok((parsed.clone(), vec![SocketAddr::new(ip, parsed.port)]));
    }

    let lookup_host = format!("{}:{}", parsed.host, parsed.port);
    let addrs: Vec<SocketAddr> = lookup_host
        .to_socket_addrs()
        .map_err(|e| format!("DNS resolution failed for {}: {e}", parsed.host))?
        .collect();
    if addrs.is_empty() {
        return Err(format!("DNS resolution returned no addresses for {}", parsed.host));
    }
    if !host_allowed {
        for addr in &addrs {
            if is_private_or_reserved_ip(addr.ip()) {
                return Err(format!(
                    "SSRF blocked resolved private/reserved IP {} for {}",
                    addr.ip(),
                    url
                ));
            }
        }
    }
    Ok((parsed, addrs))
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

/// Untrusted-peer HTTP client with SSRF, timeout, redirect, and size-limit guards.
#[derive(Debug, Clone, Default)]
pub struct SafeHttpClient {
    pub config: SafeHttpConfig,
}

/// Build the `curl` argument list for one HTTPS federation request.
///
/// This is the single place the HTTPS argument list is constructed, so tests
/// can assert on the real flags (rather than a copy). TLS certificate
/// verification is always on: the list never contains `-k`/`--insecure`,
/// regardless of `allow_private_ips` (which exempts SSRF checks only).
fn curl_args(
    method: &str,
    full_url: &str,
    resolve: &str,
    headers: &[(&str, &str)],
    body_present: bool,
    timeout_secs: u64,
    max_bytes: usize,
) -> Vec<String> {
    let mut args = vec![
        "-sS".to_string(),
        "-i".to_string(),
        "--http1.1".to_string(),
        "--max-redirs".to_string(),
        "0".to_string(),
        "--max-time".to_string(),
        timeout_secs.to_string(),
        "--max-filesize".to_string(),
        max_bytes.to_string(),
        "--resolve".to_string(),
        resolve.to_string(),
        "-X".to_string(),
        method.to_ascii_uppercase(),
    ];
    for (k, v) in headers {
        args.push("-H".to_string());
        args.push(format!("{k}: {v}"));
    }
    if body_present {
        args.push("--data-binary".to_string());
        args.push("@-".to_string());
    }
    args.push(full_url.to_string());
    args
}

impl SafeHttpClient {
    pub fn new(config: SafeHttpConfig) -> Self {
        Self { config }
    }

    /// Perform an HTTP GET or POST with redirect following (GET only) and
    /// response size enforcement.
    pub fn send(
        &self,
        method: &str,
        url: &str,
        extra_headers: &[(&str, &str)],
        body: Option<&[u8]>,
        max_response_bytes: usize,
    ) -> Result<SafeHttpResponse, String> {
        let mut current_url = url.to_string();
        let mut hops = 0usize;

        loop {
            let (parsed, addrs) = resolve_and_validate_url(&current_url, &self.config)?;
            let resp = self.send_single_hop(
                method,
                &current_url,
                &parsed,
                &addrs,
                extra_headers,
                body,
                max_response_bytes,
            )?;

            if matches!(resp.status, 301 | 302 | 303 | 307 | 308)
                && method.eq_ignore_ascii_case("GET")
            {
                if hops >= self.config.max_redirects {
                    return Err(format!(
                        "exceeded max redirects ({}) for {url}",
                        self.config.max_redirects
                    ));
                }
                let loc = resp
                    .header("location")
                    .ok_or_else(|| "redirect response missing Location header".to_string())?;
                let next_url = resolve_redirect_url(&parsed, loc)?;
                let next_parsed = parse_http_url(&next_url)?;
                if parsed.scheme == "https" && next_parsed.scheme == "http" {
                    return Err(format!(
                        "refusing HTTPS to HTTP redirect downgrade: {current_url} -> {next_url}"
                    ));
                }
                current_url = next_url;
                hops += 1;
                continue;
            }

            return Ok(resp);
        }
    }

    fn send_single_hop(
        &self,
        method: &str,
        full_url: &str,
        parsed: &ParsedUrl,
        addrs: &[SocketAddr],
        extra_headers: &[(&str, &str)],
        body: Option<&[u8]>,
        max_response_bytes: usize,
    ) -> Result<SafeHttpResponse, String> {
        if parsed.scheme == "https" {
            return self.send_https_via_curl(
                method,
                full_url,
                parsed,
                addrs,
                extra_headers,
                body,
                max_response_bytes,
            );
        }

        let addr = addrs[0];
        let mut stream = TcpStream::connect_timeout(&addr, self.config.timeout)
            .map_err(|e| format!("connect {addr}: {e}"))?;
        let _ = stream.set_read_timeout(Some(self.config.timeout));
        let _ = stream.set_write_timeout(Some(self.config.timeout));

        let mut req_buf = Vec::with_capacity(512 + body.map(|b| b.len()).unwrap_or(0));
        write!(
            &mut req_buf,
            "{} {} HTTP/1.1\r\n",
            method.to_ascii_uppercase(),
            parsed.path_and_query
        )
        .unwrap();
        let mut has_host = false;
        let mut has_ua = false;
        let mut has_conn = false;
        let mut has_len = false;
        for (k, v) in extra_headers {
            if k.eq_ignore_ascii_case("host") {
                has_host = true;
            } else if k.eq_ignore_ascii_case("user-agent") {
                has_ua = true;
            } else if k.eq_ignore_ascii_case("connection") {
                has_conn = true;
            } else if k.eq_ignore_ascii_case("content-length") {
                has_len = true;
            }
            write!(&mut req_buf, "{k}: {v}\r\n").unwrap();
        }
        if !has_host {
            write!(&mut req_buf, "Host: {}\r\n", parsed.host_header).unwrap();
        }
        if !has_ua {
            write!(&mut req_buf, "User-Agent: {}\r\n", self.config.user_agent).unwrap();
        }
        if !has_conn {
            write!(&mut req_buf, "Connection: close\r\n").unwrap();
        }
        if let Some(b) = body {
            if !has_len {
                write!(&mut req_buf, "Content-Length: {}\r\n", b.len()).unwrap();
            }
        }
        write!(&mut req_buf, "\r\n").unwrap();
        if let Some(b) = body {
            req_buf.extend_from_slice(b);
        }

        stream
            .write_all(&req_buf)
            .map_err(|e| format!("write request to {addr}: {e}"))?;
        stream
            .flush()
            .map_err(|e| format!("flush request to {addr}: {e}"))?;

        read_http_response(&mut stream, max_response_bytes, full_url)
    }

    #[allow(clippy::too_many_arguments)]
    fn send_https_via_curl(
        &self,
        method: &str,
        full_url: &str,
        parsed: &ParsedUrl,
        addrs: &[SocketAddr],
        extra_headers: &[(&str, &str)],
        body: Option<&[u8]>,
        max_response_bytes: usize,
    ) -> Result<SafeHttpResponse, String> {
        let addr = addrs[0];
        let timeout_secs = self.config.timeout.as_secs().max(1);
        let args = curl_args(
            method,
            full_url,
            &format!("{}:{}:{}", parsed.host, parsed.port, addr.ip()),
            extra_headers,
            body.is_some(),
            timeout_secs,
            max_response_bytes,
        );
        let mut cmd = Command::new("curl");
        for a in &args {
            cmd.arg(a);
        }
        if body.is_some() {
            cmd.stdin(Stdio::piped());
        } else {
            cmd.stdin(Stdio::null());
        }
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

        let mut child = cmd.spawn().map_err(|e| format!("spawn curl: {e}"))?;
        if let Some(b) = body {
            if let Some(mut stdin) = child.stdin.take() {
                let _ = stdin.write_all(b);
            }
        }
        let output = child
            .wait_with_output()
            .map_err(|e| format!("wait curl: {e}"))?;
        if !output.status.success() {
            return Err(format!(
                "curl failed ({}): {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        let mut cursor = std::io::Cursor::new(output.stdout);
        read_http_response(&mut cursor, max_response_bytes, full_url)
    }
}

fn resolve_redirect_url(base: &ParsedUrl, location: &str) -> Result<String, String> {
    let loc = location.trim();
    if loc.starts_with("http://") || loc.starts_with("https://") {
        return Ok(loc.to_string());
    }
    if loc.starts_with('/') {
        return Ok(format!("{}://{}{}", base.scheme, base.host_header, loc));
    }
    Err(format!("unsupported relative redirect Location: {loc}"))
}

fn read_http_response(
    reader: &mut impl Read,
    max_body_bytes: usize,
    final_url: &str,
) -> Result<SafeHttpResponse, String> {
    const MAX_HEADER_BYTES: usize = 32 * 1024;
    let mut raw = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];
    let header_end: usize;

    loop {
        let n = reader
            .read(&mut chunk)
            .map_err(|e| format!("read response headers: {e}"))?;
        if n == 0 {
            return Err("unexpected EOF before HTTP response headers completed".to_string());
        }
        raw.extend_from_slice(&chunk[..n]);
        if let Some(pos) = find_subslice(&raw, b"\r\n\r\n") {
            header_end = pos + 4;
            break;
        }
        if raw.len() > MAX_HEADER_BYTES {
            return Err("HTTP response headers exceeded 32 KiB limit".to_string());
        }
    }

    let header_str = std::str::from_utf8(&raw[..header_end - 4])
        .map_err(|_| "HTTP response headers are not valid UTF-8".to_string())?;
    let mut lines = header_str.split("\r\n");
    let status_line = lines.next().ok_or("empty HTTP status line")?;
    let mut status_parts = status_line.split_whitespace();
    let _http_ver = status_parts.next().ok_or("malformed HTTP status line")?;
    let status: u16 = status_parts
        .next()
        .ok_or("HTTP status line missing code")?
        .parse()
        .map_err(|_| format!("invalid HTTP status code in {status_line}"))?;

    let mut headers = Vec::new();
    let mut content_length: Option<usize> = None;
    let mut is_chunked = false;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let name = k.trim().to_string();
            let val = v.trim().to_string();
            if name.eq_ignore_ascii_case("content-length") {
                if let Ok(len) = val.parse::<usize>() {
                    if len > max_body_bytes {
                        return Err(format!(
                            "response Content-Length {len} exceeds limit {max_body_bytes}"
                        ));
                    }
                    content_length = Some(len);
                }
            } else if name.eq_ignore_ascii_case("transfer-encoding")
                && val.to_ascii_lowercase().contains("chunked")
            {
                is_chunked = true;
            }
            headers.push((name, val));
        }
    }

    let mut body_raw = raw[header_end..].to_vec();
    if body_raw.len() > max_body_bytes + 512 && !is_chunked {
        return Err(format!(
            "response body exceeded max size of {max_body_bytes} bytes"
        ));
    }

    loop {
        if let Some(expected) = content_length {
            if body_raw.len() >= expected {
                body_raw.truncate(expected);
                break;
            }
        }
        let n = reader
            .read(&mut chunk)
            .map_err(|e| format!("read response body: {e}"))?;
        if n == 0 {
            break;
        }
        body_raw.extend_from_slice(&chunk[..n]);
        let cap = if is_chunked {
            max_body_bytes + 4096
        } else {
            max_body_bytes
        };
        if body_raw.len() > cap {
            return Err(format!(
                "response body exceeded max size of {max_body_bytes} bytes"
            ));
        }
    }

    let body = if is_chunked {
        let decoded = decode_chunked(&body_raw)?;
        if decoded.len() > max_body_bytes {
            return Err(format!(
                "decoded chunked body exceeded max size of {max_body_bytes} bytes"
            ));
        }
        decoded
    } else {
        body_raw
    };

    Ok(SafeHttpResponse {
        status,
        headers,
        body,
        final_url: final_url.to_string(),
    })
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn decode_chunked(mut input: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    loop {
        let Some(crlf) = find_subslice(input, b"\r\n") else {
            break;
        };
        let size_line = std::str::from_utf8(&input[..crlf])
            .map_err(|_| "invalid chunk size line".to_string())?;
        let hex_part = size_line.split(';').next().unwrap_or("").trim();
        let chunk_size = usize::from_str_radix(hex_part, 16)
            .map_err(|_| format!("invalid chunk size: {hex_part}"))?;
        input = &input[crlf + 2..];
        if chunk_size == 0 {
            break;
        }
        if input.len() < chunk_size + 2 {
            return Err("truncated chunked body".to_string());
        }
        out.extend_from_slice(&input[..chunk_size]);
        input = &input[chunk_size + 2..];
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Signed ActivityPub GET / POST + WebFinger & Remote Actor/Note Resolver
// ---------------------------------------------------------------------------

/// Format a millisecond timestamp as an HTTP `Date` IMF-fixdate string
/// (`"Wed, 14 Oct 2026 12:00:00 GMT"`).
pub fn format_http_date(ms: i64) -> String {
    let dt = chrono::DateTime::<Utc>::from_timestamp_millis(ms).unwrap_or_else(Utc::now);
    dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

/// Perform an HTTP-signed ActivityPub `GET` using the local `system_actor` key.
pub fn signed_get_json(
    services: &AppServices,
    client: &SafeHttpClient,
    url: &str,
    accept: &str,
) -> Result<Value, String> {
    let parsed = parse_http_url(url)?;
    let (sys_account, sys_kp) = services
        .ensure_system_actor()
        .map_err(|e| format!("ensure_system_actor: {e}"))?;
    let base = services.public_url.trim_end_matches('/');
    let key_id = format!("{}/users/{}#main-key", base, sys_account.id);
    let date = format_http_date(services.clock.now_ms());
    let sig = sign_request_headers(
        "GET",
        &parsed.path_and_query,
        &parsed.host_header,
        &date,
        None,
        &key_id,
        &sys_kp.private_key_pem,
    )?;

    let headers = [
        ("Host", parsed.host_header.as_str()),
        ("Date", sig.date.as_str()),
        ("Signature", sig.signature.as_str()),
        ("Accept", accept),
    ];
    let resp = client.send("GET", url, &headers, None, MAX_AP_JSON_BYTES)?;
    if !(200..300).contains(&resp.status) {
        return Err(format!("GET {url} returned HTTP {}", resp.status));
    }
    serde_json::from_slice(&resp.body).map_err(|e| format!("parse JSON from {url}: {e}"))
}

/// Perform an HTTP-signed ActivityPub `POST` of a JSON activity to a remote inbox.
pub fn signed_post_activity(
    services: &AppServices,
    client: &SafeHttpClient,
    sender_account_id: &str,
    inbox_url: &str,
    body_bytes: &[u8],
) -> Result<SafeHttpResponse, String> {
    let parsed = parse_http_url(inbox_url)?;
    let kp = services
        .ensure_actor_keypair(sender_account_id)
        .map_err(|e| format!("ensure_actor_keypair: {e}"))?;
    let base = services.public_url.trim_end_matches('/');
    let key_id = format!("{base}/users/{sender_account_id}#main-key");
    let date = format_http_date(services.clock.now_ms());
    let sig = sign_request_headers(
        "POST",
        &parsed.path_and_query,
        &parsed.host_header,
        &date,
        Some(body_bytes),
        &key_id,
        &kp.private_key_pem,
    )?;
    let digest = sig.digest.unwrap_or_default();
    let headers = [
        ("Host", parsed.host_header.as_str()),
        ("Date", sig.date.as_str()),
        ("Digest", digest.as_str()),
        ("Signature", sig.signature.as_str()),
        ("Content-Type", AP_CONTENT_TYPE),
        ("Accept", "application/activity+json, application/ld+json"),
    ];
    client.send("POST", inbox_url, &headers, Some(body_bytes), MAX_AP_JSON_BYTES)
}

/// Discover a remote user's canonical actor URI via WebFinger
/// (`/.well-known/webfinger?resource=acct:{username}@{host}`).
pub fn webfinger_discover_actor_uri(
    client: &SafeHttpClient,
    username: &str,
    host: &str,
    scheme_override: Option<&str>,
) -> Result<String, String> {
    let resource = format!("acct:{}@{}", username.trim(), host.trim());
    let encoded_resource = resource.replace('@', "%40").replace(':', "%3A");
    let schemes: Vec<&str> = match scheme_override {
        Some(s) => vec![s],
        // IP-literal hosts (common in test rigs and LAN deployments, often
        // with a port) never serve public TLS: go straight to `http` instead
        // of burning a full timeout on a doomed `https` attempt.
        None if is_ip_literal_host(host) => vec!["http"],
        None => vec!["https", "http"],
    };
    let mut last_err = String::new();
    for scheme in schemes {
        let wf_url = format!(
            "{scheme}://{}/.well-known/webfinger?resource={encoded_resource}",
            host.trim()
        );
        let headers = [(
            "Accept",
            "application/jrd+json, application/json;q=0.9, application/xrd+xml;q=0.8",
        )];
        match client.send("GET", &wf_url, &headers, None, MAX_AP_JSON_BYTES) {
            Ok(resp) if (200..300).contains(&resp.status) => {
                if let Ok(jrd) = serde_json::from_slice::<Value>(&resp.body) {
                    if let Some(links) = jrd.get("links").and_then(Value::as_array) {
                        for link in links {
                            let rel = link.get("rel").and_then(Value::as_str).unwrap_or("");
                            let typ = link.get("type").and_then(Value::as_str).unwrap_or("");
                            if rel == "self"
                                && (typ.contains("activity+json") || typ.contains("ld+json"))
                            {
                                if let Some(href) = link.get("href").and_then(Value::as_str) {
                                    return Ok(href.trim().to_string());
                                }
                            }
                        }
                    }
                }
                last_err = format!("WebFinger JRD at {wf_url} had no ActivityPub self link");
            }
            Ok(resp) => {
                last_err = format!("WebFinger {wf_url} returned HTTP {}", resp.status);
            }
            Err(e) => {
                last_err = e;
            }
        }
    }
    Err(last_err)
}

/// Fetch a remote ActivityPub Actor by URI, verify its origin host, and
/// upsert/cache the remote [`Account`] and its public key in storage.
pub fn fetch_and_upsert_remote_actor(
    services: &AppServices,
    client: &SafeHttpClient,
    actor_uri: &str,
) -> Result<(Account, ParsedRemoteActor), PortError> {
    let doc = signed_get_json(
        services,
        client,
        actor_uri,
        "application/activity+json, application/ld+json;q=0.9, application/json;q=0.8",
    )
    .map_err(PortError::Validation)?;
    upsert_remote_actor_from_doc(services, actor_uri, &doc)
}

/// Upsert a remote [`Account`] and cache its public key from an already-fetched
/// Actor JSON document, enforcing same-origin authority between `expected_uri`
/// and `doc["id"]`.
pub fn upsert_remote_actor_from_doc(
    services: &AppServices,
    expected_uri: &str,
    doc: &Value,
) -> Result<(Account, ParsedRemoteActor), PortError> {
    let parsed = parse_remote_actor(doc).map_err(PortError::Validation)?;
    let (_, expected_host) = parse_url_host(expected_uri)
        .ok_or_else(|| PortError::Validation(format!("invalid expected URI: {expected_uri}")))?;
    if parsed.host != expected_host {
        return Err(PortError::Validation(format!(
            "actor id host {} does not match requested URI host {}",
            parsed.host, expected_host
        )));
    }

    let description = parsed
        .summary_mfm
        .clone()
        .or_else(|| {
            parsed
                .summary_html
                .as_deref()
                .map(crate::activitypub::objects::sanitize_html_to_mfm)
        })
        .filter(|s| !s.is_empty());

    let account = if let Some(existing) = services
        .accounts
        .find_by_uri(&parsed.uri)?
        .or(services
            .accounts
            .find_by_username_and_host(&parsed.username, Some(&parsed.host))?)
    {
        services.accounts.update_account(
            &existing.id,
            UpdateAccountPatch {
                display_name: Some(parsed.display_name.clone()),
                description: Some(description),
                location: Some(parsed.location.clone()),
                birthday: None,
                avatar_id: None,
                banner_id: None,
                is_locked: Some(parsed.manually_approves_followers),
                is_bot: Some(parsed.is_bot()),
                is_cat: Some(parsed.is_cat),
                is_explorable: Some(parsed.discoverable),
            },
        )?
    } else {
        services.accounts.create_account(CreateAccountInput {
            handle: parsed.username.clone(),
            host: Some(parsed.host.clone()),
            display_name: parsed.display_name.clone(),
            description,
            location: parsed.location.clone(),
            birthday: None,
            uri: Some(parsed.uri.clone()),
            inbox: Some(parsed.inbox.clone()),
            shared_inbox: parsed.shared_inbox.clone(),
            is_bot: parsed.is_bot(),
            is_cat: parsed.is_cat,
            is_locked: parsed.manually_approves_followers,
            preserved_usernames: Vec::new(),
        })?
    };

    // Cache both `{actor}#main-key` and `{actor}/publickey` so signature
    // lookups succeed without another roundtrip.
    services.cache_remote_public_key(
        &parsed.public_key_id,
        &parsed.uri,
        Some(account.id.clone()),
        &parsed.public_key_pem,
    )?;
    let alt_key_id = format!("{}/publickey", parsed.uri.trim_end_matches('/'));
    if alt_key_id != parsed.public_key_id {
        services.cache_remote_public_key(
            &alt_key_id,
            &parsed.uri,
            Some(account.id.clone()),
            &parsed.public_key_pem,
        )?;
    }

    Ok((account, parsed))
}

/// Resolve a signer `keyId` to `(actor_uri, public_key_pem)`, consulting the
/// local cache first and fetching the remote actor document on miss (or when
/// `force_refresh` is true).
pub fn resolve_remote_public_key(
    services: &AppServices,
    client: &SafeHttpClient,
    key_id: &str,
    force_refresh: bool,
) -> Result<(String, String), PortError> {
    if !force_refresh {
        if let Some(cached) = services.auth.get_remote_public_key(key_id)? {
            return Ok((cached.actor_uri, cached.public_key_pem));
        }
    }
    let actor_uri = actor_uri_from_key_id(key_id);
    let (_account, parsed) = fetch_and_upsert_remote_actor(services, client, &actor_uri)?;
    Ok((parsed.uri, parsed.public_key_pem))
}

/// Fetch and parse a remote ActivityPub `Note` by URI, verifying that its
/// `attributedTo` actor shares the same origin host as `note_uri`.
pub fn fetch_remote_note(
    services: &AppServices,
    client: &SafeHttpClient,
    note_uri: &str,
) -> Result<ParsedRemoteNote, PortError> {
    let doc = signed_get_json(
        services,
        client,
        note_uri,
        "application/activity+json, application/ld+json;q=0.9, application/json;q=0.8",
    )
    .map_err(PortError::Validation)?;
    let obj = if doc.get("type").and_then(Value::as_str) == Some("Create") {
        doc.get("object").cloned().unwrap_or(doc)
    } else {
        doc
    };
    let parsed = parse_remote_note(&obj).map_err(PortError::Validation)?;
    let (_, note_host) = parse_url_host(&parsed.uri)
        .ok_or_else(|| PortError::Validation("invalid remote note URI".to_string()))?;
    let (_, req_host) = parse_url_host(note_uri)
        .ok_or_else(|| PortError::Validation("invalid requested note URI".to_string()))?;
    let (_, author_host) = parse_url_host(&parsed.attributed_to)
        .ok_or_else(|| PortError::Validation("invalid remote note attributedTo URI".to_string()))?;
    if note_host != req_host || note_host != author_host {
        return Err(PortError::Validation(format!(
            "remote note origin mismatch: req={req_host}, note={note_host}, author={author_host}"
        )));
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    #[test]
    fn ssrf_blocks_loopback_private_link_local_and_mapped_ipv6() {
        for blocked in [
            "127.0.0.1",
            "10.0.0.1",
            "172.16.5.4",
            "192.168.1.1",
            "169.254.169.254",
            "100.64.0.1",
            "0.0.0.0",
            "::1",
            "fe80::1",
            "fc00::1",
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.1",
        ] {
            let ip: IpAddr = blocked.parse().unwrap();
            assert!(
                is_private_or_reserved_ip(ip),
                "expected {blocked} to be blocked as private/reserved"
            );
        }

        for public_ip in ["1.1.1.1", "8.8.8.8", "93.184.216.34", "2606:4700:4700::1111"] {
            let ip: IpAddr = public_ip.parse().unwrap();
            assert!(
                !is_private_or_reserved_ip(ip),
                "expected {public_ip} to be allowed as public"
            );
        }

        let strict_cfg = SafeHttpConfig {
            allow_private_ips: false,
            allowed_hosts: Vec::new(),
            ..Default::default()
        };
        assert!(resolve_and_validate_url("http://127.0.0.1:3000/inbox", &strict_cfg).is_err());
        assert!(resolve_and_validate_url("http://169.254.169.254/latest", &strict_cfg).is_err());
        assert!(resolve_and_validate_url("ftp://93.184.216.34/file", &strict_cfg).is_err());
    }

    #[test]
    fn redirect_limit_and_size_limit_enforced() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            for _ in 0..6 {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                let mut buf = [0u8; 1024];
                let n = stream.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                if req.starts_with("GET /loop") {
                    let resp = format!(
                        "HTTP/1.1 302 Found\r\nLocation: http://{addr}/loop\r\nContent-Length: 0\r\n\r\n"
                    );
                    let _ = stream.write_all(resp.as_bytes());
                } else if req.starts_with("GET /huge") {
                    let body = vec![b'x'; 2048];
                    let header = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(header.as_bytes());
                    let _ = stream.write_all(&body);
                }
            }
        });

        let client = SafeHttpClient::new(SafeHttpConfig {
            allow_private_ips: true,
            max_redirects: 2,
            ..Default::default()
        });

        let loop_err = client
            .send("GET", &format!("http://{addr}/loop"), &[], None, 1024)
            .unwrap_err();
        assert!(
            loop_err.contains("exceeded max redirects"),
            "got {loop_err}"
        );

        let size_err = client
            .send("GET", &format!("http://{addr}/huge"), &[], None, 512)
            .unwrap_err();
        assert!(size_err.contains("exceeds limit"), "got {size_err}");

        drop(handle);
    }

    #[test]
    fn curl_args_never_disable_tls_and_keep_ssrf_pins() {
        let headers = [
            ("Host", "example.social"),
            ("Content-Type", "application/activity+json"),
        ];
        let args = curl_args(
            "POST",
            "https://example.social/inbox",
            "example.social:443:93.184.216.34",
            &headers,
            true,
            10,
            65_536,
        );
        assert!(
            !args.iter().any(|a| a == "-k" || a == "--insecure"),
            "curl args must never disable TLS verification: {args:?}"
        );
        let resolve_pos = args
            .iter()
            .position(|a| a == "--resolve")
            .expect("curl args must pin DNS with --resolve");
        assert_eq!(args[resolve_pos + 1], "example.social:443:93.184.216.34");
        let redirs_pos = args
            .iter()
            .position(|a| a == "--max-redirs")
            .expect("curl args must cap redirects");
        assert_eq!(args[redirs_pos + 1], "0");

        // GET without body: same TLS and pinning guarantees.
        let get_args = curl_args(
            "GET",
            "https://example.social/users/alice",
            "example.social:443:93.184.216.34",
            &headers[..1],
            false,
            10,
            65_536,
        );
        assert!(
            !get_args.iter().any(|a| a == "-k" || a == "--insecure"),
            "curl GET args must never disable TLS verification: {get_args:?}"
        );
        assert!(get_args.iter().any(|a| a == "--resolve"));
        let get_redirs = get_args
            .iter()
            .position(|a| a == "--max-redirs")
            .expect("curl GET args must cap redirects");
        assert_eq!(get_args[get_redirs + 1], "0");
    }
}
