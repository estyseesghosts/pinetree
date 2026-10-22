use std::net::IpAddr;

use super::super::errors::SendErrorKind;
use super::super::url::{is_private_or_reserved_ip, resolve_and_validate_url};
use super::config::SafeHttpConfig;
use super::framing::{decode_chunked, read_http_response};
use super::send::{SafeHttpClient, curl_args, with_extra_ca};
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

    for public_ip in [
        "1.1.1.1",
        "8.8.8.8",
        "93.184.216.34",
        "2606:4700:4700::1111",
    ] {
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
        loop_err.to_string().contains("exceeded max redirects"),
        "got {loop_err}"
    );
    assert_eq!(loop_err.kind, SendErrorKind::Redirect);

    let size_err = client
        .send("GET", &format!("http://{addr}/huge"), &[], None, 512)
        .unwrap_err();
    assert!(
        size_err.to_string().contains("exceeds limit"),
        "got {size_err}"
    );
    assert_eq!(size_err.kind, SendErrorKind::TooLarge);

    drop(handle);
}

#[test]
fn extra_ca_is_added_before_the_url_and_is_off_by_default() {
    let args = || {
        curl_args(
            "GET",
            "https://mastodon.lab/x",
            "mastodon.lab:443:127.0.0.1",
            &[],
            false,
            10,
            1024,
        )
    };
    assert_eq!(with_extra_ca(args(), None), args());
    let with = with_extra_ca(args(), Some(std::path::Path::new("/ca.crt")));
    let n = with.len();
    assert_eq!(
        &with[n - 3..],
        ["--cacert", "/ca.crt", "https://mastodon.lab/x"]
    );
    assert!(!with.iter().any(|a| a == "-k" || a == "--insecure"));
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

    // Slice 1 framing: curl must not de-chunk (our parser does), and
    // operator config / proxy env must never change federation behavior
    // (proxies are NOT a supported config: DNS is pinned via --resolve).
    for (label, a) in [("POST", &args), ("GET", &get_args)] {
        assert!(
            a.iter().any(|x| x == "--raw"),
            "{label} curl args must pass --raw so read_http_response sees real framing: {a:?}"
        );
        assert!(
            a.iter().any(|x| x == "-q"),
            "{label} curl args must pass -q to ignore .curlrc: {a:?}"
        );
        let noproxy_pos = a
            .iter()
            .position(|x| x == "--noproxy")
            .expect("--noproxy must bypass proxy env");
        assert_eq!(a[noproxy_pos + 1], "*");
    }
}

#[test]
fn decode_chunked_hardening_rejects_garbage_without_panicking() {
    // Overflow: 16 f's parse to u64::MAX; `+ 2` must be checked, not panic.
    assert!(decode_chunked(b"ffffffffffffffff\r\n").is_err());
    // Truncated body: declared 5, only 2 bytes present.
    assert!(decode_chunked(b"5\r\nhe").is_err());
    // Missing CRLF trailer after chunk data.
    assert!(decode_chunked(b"5\r\nhelloXX0\r\n\r\n").is_err());
    // Chunk-extension garbage with control bytes must be rejected.
    assert!(decode_chunked(b"5;ext=\x01\x02\r\nhello\r\n0\r\n\r\n").is_err());
    // Empty and invalid hex.
    assert!(decode_chunked(b"\r\n\r\n").is_err());
    assert!(decode_chunked(b"zz\r\nhello\r\n0\r\n\r\n").is_err());
    // Missing terminal zero chunk is truncation, not a body.
    assert!(decode_chunked(b"5\r\nhello\r\n").is_err());
    // Valid chunked with a token extension still decodes.
    let decoded = decode_chunked(b"5;ext=foo\r\nhello\r\n0\r\n\r\n")
        .expect("valid token extension must decode");
    assert_eq!(decoded, b"hello");
}

#[test]
fn read_http_response_skips_100_continue_and_decodes_chunked() {
    let raw = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5;ext=foo\r\nhello\r\n0\r\n\r\n";
    let mut cursor = std::io::Cursor::new(raw.to_vec());
    let resp = read_http_response(&mut cursor, 65_536, "https://example.social/x")
        .expect("100 Continue prefix must be skipped");
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body, b"hello");
}

#[test]
fn read_http_response_skips_proxy_connect_block() {
    let raw = b"HTTP/1.1 200 Connection established\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
    let mut cursor = std::io::Cursor::new(raw.to_vec());
    let resp = read_http_response(&mut cursor, 65_536, "https://example.social/y")
        .expect("proxy CONNECT block must be skipped");
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body, b"hello");
}
