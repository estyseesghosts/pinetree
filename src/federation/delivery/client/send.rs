use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use super::super::errors::{
    SendError, SendErrorKind, curl_error, io_send_error, is_timeout_kind, response_error,
};
use super::super::url::{
    BoundedDnsResolver, ParsedUrl, default_dns_resolver, parse_http_url,
    resolve_and_validate_url_with_resolver, resolve_redirect_url,
};
use super::config::{SafeHttpConfig, SafeHttpResponse};
use super::framing::{read_http_response, remaining_budget};

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
///
/// Framing: `--raw` disables curl's Transfer-Encoding decoding so
/// `read_http_response` sees the real wire framing on both the plain-HTTP
/// and curl-HTTPS paths (curl would otherwise de-chunk before our parser
/// runs, double-decoding chunked bodies to empty/error).
///
/// Config isolation: `-q` ignores `~/.curlrc` (a local operator config must
/// never change federation framing/TLS), and `--noproxy "*"` bypasses any
/// proxy env (`http_proxy`/`https_proxy`/`all_proxy`). Proxies are NOT a
/// supported config: federation pins DNS via `--resolve` to the
/// SSRF-validated address, which a proxy would silently bypass.
pub(crate) fn curl_args(
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
        "-q".to_string(),
        "--noproxy".to_string(),
        "*".to_string(),
        "--raw".to_string(),
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

static EXTRA_CA: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();

/// Bench only (`--federation-extra-ca=PATH`): trust the CA bundle at `path` for outbound HTTPS,
/// for a lab whose servers present a private CA. curl's `--cacert` replaces its default roots,
/// so the file must hold every CA the node needs. Verification stays on. First call wins.
pub fn set_extra_ca(path: std::path::PathBuf) {
    let _ = EXTRA_CA.set(path);
}

/// Insert `--cacert PATH` before the URL (the last argument).
pub(crate) fn with_extra_ca(mut args: Vec<String>, ca: Option<&std::path::Path>) -> Vec<String> {
    if let Some(ca) = ca {
        let url = args.pop();
        args.push("--cacert".to_string());
        args.push(ca.display().to_string());
        args.extend(url);
    }
    args
}

impl SafeHttpClient {
    pub fn new(config: SafeHttpConfig) -> Self {
        Self { config }
    }

    /// Perform an HTTP GET or POST with redirect following (GET only) and
    /// response size enforcement, using the process-wide bounded DNS pool.
    pub fn send(
        &self,
        method: &str,
        url: &str,
        extra_headers: &[(&str, &str)],
        body: Option<&[u8]>,
        max_response_bytes: usize,
    ) -> Result<SafeHttpResponse, SendError> {
        self.send_with_resolver(
            method,
            url,
            extra_headers,
            body,
            max_response_bytes,
            default_dns_resolver(),
        )
    }

    /// [`send`](Self::send) over an explicit bounded resolver pool (Phase 4
    /// P1 test seam: scripted resolvers stall, fail, or return canned
    /// addresses without touching the network). Every hop — including
    /// redirect targets — is re-resolved through the same pool and
    /// re-validated against SSRF rules.
    pub fn send_with_resolver(
        &self,
        method: &str,
        url: &str,
        extra_headers: &[(&str, &str)],
        body: Option<&[u8]>,
        max_response_bytes: usize,
        resolver: &BoundedDnsResolver,
    ) -> Result<SafeHttpResponse, SendError> {
        let mut current_url = url.to_string();
        let mut hops = 0usize;

        loop {
            let (parsed, addrs) =
                resolve_and_validate_url_with_resolver(&current_url, &self.config, resolver)?;
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
                    return Err(SendError::new(
                        SendErrorKind::Redirect,
                        format!(
                            "exceeded max redirects ({}) for {url}",
                            self.config.max_redirects
                        ),
                    ));
                }
                let loc = resp.header("location").ok_or_else(|| {
                    SendError::new(
                        SendErrorKind::Protocol,
                        "redirect response missing Location header",
                    )
                })?;
                let next_url = resolve_redirect_url(&parsed, loc)
                    .map_err(|e| SendError::new(SendErrorKind::Redirect, e))?;
                let next_parsed = parse_http_url(&next_url)
                    .map_err(|e| SendError::new(SendErrorKind::InvalidUrl, e))?;
                if parsed.scheme == "https" && next_parsed.scheme == "http" {
                    return Err(SendError::new(
                        SendErrorKind::Redirect,
                        format!(
                            "refusing HTTPS to HTTP redirect downgrade: {current_url} -> {next_url}"
                        ),
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
    ) -> Result<SafeHttpResponse, SendError> {
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

        // One deadline spans connect, write and read (`total_timeout`).
        let deadline = Instant::now() + self.config.total_timeout;
        let addr = addrs
            .first()
            .copied()
            .ok_or_else(|| SendError::new(SendErrorKind::Dns, "no addresses to connect to"))?;
        let connect_budget = remaining_budget(deadline).ok_or_else(|| {
            SendError::new(
                SendErrorKind::Timeout,
                format!("connect {addr}: deadline exceeded"),
            )
        })?;
        let stream = TcpStream::connect_timeout(&addr, connect_budget.min(self.config.timeout))
            .map_err(|e| {
                let kind = if is_timeout_kind(e.kind()) {
                    SendErrorKind::Timeout
                } else {
                    SendErrorKind::Connect
                };
                SendError::new(kind, format!("connect {addr}: {e}"))
            })?;
        let mut conn = DeadlineStream {
            stream,
            deadline,
            per_op: self.config.timeout,
            timed_out: false,
        };

        let mut req_buf = Vec::with_capacity(512 + body.map(|b| b.len()).unwrap_or(0));
        write!(
            &mut req_buf,
            "{} {} HTTP/1.1\r\n",
            method.to_ascii_uppercase(),
            parsed.path_and_query
        )
        .expect("writing an HTTP request line into a Vec<u8> never fails");
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
            write!(&mut req_buf, "{k}: {v}\r\n").expect("Vec<u8> writes are infallible");
        }
        if !has_host {
            write!(&mut req_buf, "Host: {}\r\n", parsed.host_header)
                .expect("Vec<u8> writes are infallible");
        }
        if !has_ua {
            write!(&mut req_buf, "User-Agent: {}\r\n", self.config.user_agent)
                .expect("Vec<u8> writes are infallible");
        }
        if !has_conn {
            write!(&mut req_buf, "Connection: close\r\n").expect("Vec<u8> writes are infallible");
        }
        if let Some(b) = body {
            if !has_len {
                write!(&mut req_buf, "Content-Length: {}\r\n", b.len())
                    .expect("Vec<u8> writes are infallible");
            }
        }
        write!(&mut req_buf, "\r\n").expect("Vec<u8> writes are infallible");
        if let Some(b) = body {
            req_buf.extend_from_slice(b);
        }

        let written = conn.write_all_deadline(&req_buf);
        if let Err(e) = written {
            return Err(io_send_error(
                format!("write request to {addr}"),
                &e,
                conn.timed_out,
            ));
        }
        conn.stream
            .flush()
            .map_err(|e| io_send_error(format!("flush request to {addr}"), &e, conn.timed_out))?;

        let parsed_response = read_http_response(&mut conn, max_response_bytes, full_url);
        let timed_out = conn.timed_out;
        parsed_response.map_err(|msg| response_error(msg, timed_out))
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
    ) -> Result<SafeHttpResponse, SendError> {
        let addr = addrs[0];
        let timeout_secs = self.config.timeout.as_secs().max(1);
        let args = with_extra_ca(
            curl_args(
                method,
                full_url,
                &format!("{}:{}:{}", parsed.host, parsed.port, addr.ip()),
                extra_headers,
                body.is_some(),
                timeout_secs,
                max_response_bytes,
            ),
            EXTRA_CA.get().map(|p| p.as_path()),
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

        let mut child = cmd
            .spawn()
            .map_err(|e| SendError::new(SendErrorKind::Internal, format!("spawn curl: {e}")))?;
        if let Some(b) = body {
            if let Some(mut stdin) = child.stdin.take() {
                let _ = stdin.write_all(b);
            }
        }
        let output = child
            .wait_with_output()
            .map_err(|e| SendError::new(SendErrorKind::Io, format!("wait curl: {e}")))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            return Err(curl_error(
                output.status.code(),
                &format!("{}: {stderr}", output.status),
            ));
        }
        let mut cursor = std::io::Cursor::new(output.stdout);
        read_http_response(&mut cursor, max_response_bytes, full_url)
            .map_err(|msg| response_error(msg, false))
    }
}

/// Plain-HTTP stream bounded by one overall deadline. Each read and write
/// re-arms its socket timeout with the time left, so a peer that trickles
/// bytes cannot stretch a request past `SafeHttpConfig::total_timeout`.
struct DeadlineStream {
    stream: TcpStream,
    deadline: Instant,
    per_op: Duration,
    /// Set when the deadline or a socket timeout fired, so the caller reports
    /// `Timeout` rather than a generic protocol error.
    timed_out: bool,
}

impl DeadlineStream {
    fn time_left(&mut self) -> std::io::Result<Duration> {
        match remaining_budget(self.deadline) {
            Some(left) => Ok(left.min(self.per_op)),
            None => {
                self.timed_out = true;
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "federation request deadline exceeded",
                ))
            }
        }
    }

    fn write_all_deadline(&mut self, buf: &[u8]) -> std::io::Result<()> {
        for chunk in buf.chunks(16 * 1024) {
            let left = self.time_left()?;
            self.stream.set_write_timeout(Some(left))?;
            let res = self.stream.write_all(chunk);
            if let Err(e) = &res {
                self.timed_out |= is_timeout_kind(e.kind());
            }
            res?;
        }
        Ok(())
    }
}

impl Read for DeadlineStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let left = self.time_left()?;
        self.stream.set_read_timeout(Some(left))?;
        let res = self.stream.read(buf);
        if let Err(e) = &res {
            self.timed_out |= is_timeout_kind(e.kind());
        }
        res
    }
}
