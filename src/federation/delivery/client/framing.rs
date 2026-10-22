use std::io::Read;
use std::time::{Duration, Instant};

use super::config::SafeHttpResponse;

/// Time left before `deadline`, or `None` once it has passed.
pub(crate) fn remaining_budget(deadline: Instant) -> Option<Duration> {
    let left = deadline.saturating_duration_since(Instant::now());
    (!left.is_zero()).then_some(left)
}

pub(crate) fn read_http_response(
    reader: &mut impl Read,
    max_body_bytes: usize,
    final_url: &str,
) -> Result<SafeHttpResponse, String> {
    const MAX_HEADER_BYTES: usize = 32 * 1024;
    const MAX_INTERIM_BLOCKS: usize = 8;
    let mut raw = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];

    // Skip interim header blocks (1xx, plus proxy CONNECT
    // `200 Connection established`) and return the final block.
    let (status, headers, content_length, is_chunked, body_raw) = {
        let mut final_block: Option<(u16, Vec<(String, String)>, Option<usize>, bool, Vec<u8>)> =
            None;
        for _ in 0..=MAX_INTERIM_BLOCKS {
            let header_end: usize = loop {
                let n = reader
                    .read(&mut chunk)
                    .map_err(|e| format!("read response headers: {e}"))?;
                if n == 0 {
                    // No more bytes: if we already buffered a terminator,
                    // fall through to parse; otherwise the headers are
                    // truncated.
                    if let Some(pos) = find_subslice(&raw, b"\r\n\r\n") {
                        break pos + 4;
                    }
                    return Err("unexpected EOF before HTTP response headers completed".to_string());
                }
                raw.extend_from_slice(&chunk[..n]);
                if let Some(pos) = find_subslice(&raw, b"\r\n\r\n") {
                    break pos + 4;
                }
                if raw.len() > MAX_HEADER_BYTES {
                    return Err("HTTP response headers exceeded 32 KiB limit".to_string());
                }
            };
            // Enforce the per-block cap even when the whole block arrived
            // in previously buffered bytes.
            if header_end > MAX_HEADER_BYTES + 4 {
                return Err("HTTP response headers exceeded 32 KiB limit".to_string());
            }

            let header_str = std::str::from_utf8(
                raw.get(..header_end.saturating_sub(4))
                    .ok_or("HTTP response headers are not valid UTF-8".to_string())?,
            )
            .map_err(|_| "HTTP response headers are not valid UTF-8".to_string())?;
            let mut lines = header_str.split("\r\n");
            let status_line = lines.next().ok_or("empty HTTP status line")?.to_string();
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

            // Interim blocks carry no body: discard the block and parse the
            // next one from the already-buffered remainder.
            let is_interim = (100..200).contains(&status)
                || (status == 200
                    && status_line
                        .to_ascii_lowercase()
                        .contains("connection established"));
            if is_interim {
                raw = raw.get(header_end..).unwrap_or(&[]).to_vec();
                if raw.len() > MAX_HEADER_BYTES {
                    return Err("HTTP response headers exceeded 32 KiB limit".to_string());
                }
                continue;
            }

            let body_raw = raw.get(header_end..).unwrap_or(&[]).to_vec();
            final_block = Some((status, headers, content_length, is_chunked, body_raw));
            break;
        }
        final_block.ok_or_else(|| "too many interim HTTP response header blocks".to_string())?
    };
    let mut body_raw = body_raw;
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

pub(crate) fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

pub(crate) fn decode_chunked(mut input: &[u8]) -> Result<Vec<u8>, String> {
    let total_budget = input.len();
    let mut out = Vec::new();
    let mut saw_terminal = false;
    loop {
        if input.is_empty() {
            break;
        }
        let Some(crlf) = find_subslice(input, b"\r\n") else {
            return Err("truncated chunked body: size line without CRLF".to_string());
        };
        let line_bytes = input
            .get(..crlf)
            .ok_or_else(|| "invalid chunk size line".to_string())?;
        let size_line =
            std::str::from_utf8(line_bytes).map_err(|_| "invalid chunk size line".to_string())?;
        let (hex_raw, ext_raw) = match size_line.split_once(';') {
            Some((h, e)) => (h, Some(e)),
            None => (size_line, None),
        };
        let hex_part = hex_raw.trim();
        if hex_part.is_empty() {
            return Err("invalid chunk size: empty".to_string());
        }
        if !hex_part.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(format!("invalid chunk size: {hex_part}"));
        }
        // Chunk extensions (after ';') must be visible ASCII (plus SP/TAB);
        // control bytes, DEL, or non-ASCII garbage are rejected.
        if let Some(ext) = ext_raw {
            if ext.is_empty() {
                return Err("invalid chunk extension: empty".to_string());
            }
            for c in ext.chars() {
                if c == '\t' || c == ' ' {
                    continue;
                }
                if !c.is_ascii() || (c as u32) < 0x21 || (c as u32) > 0x7e {
                    return Err("invalid chunk extension".to_string());
                }
            }
        }
        let chunk_size = usize::from_str_radix(hex_part, 16)
            .map_err(|_| format!("invalid chunk size: {hex_part}"))?;
        input = input
            .get(crlf + 2..)
            .ok_or_else(|| "truncated chunked body".to_string())?;
        if chunk_size == 0 {
            saw_terminal = true;
            break;
        }
        // Overflow-safe framing: `chunk_size + 2` (data + trailing CRLF)
        // must fit and be fully present in the remaining input. The decoded
        // output can never exceed the encoded input length, so a chunk
        // larger than the remaining budget is truncated/garbage.
        let framed = chunk_size
            .checked_add(2)
            .ok_or_else(|| "invalid chunk size: overflow".to_string())?;
        if framed > input.len() {
            return Err("truncated chunked body".to_string());
        }
        if chunk_size > total_budget {
            return Err("truncated chunked body".to_string());
        }
        let next_len = out
            .len()
            .checked_add(chunk_size)
            .ok_or_else(|| "invalid chunk size: overflow".to_string())?;
        if next_len > total_budget {
            return Err("chunked body exceeds input budget".to_string());
        }
        let data = input
            .get(..chunk_size)
            .ok_or_else(|| "truncated chunked body".to_string())?;
        let trailer = input
            .get(chunk_size..framed)
            .ok_or_else(|| "truncated chunked body".to_string())?;
        if trailer != b"\r\n" {
            return Err("invalid chunk trailer: missing CRLF".to_string());
        }
        out.extend_from_slice(data);
        input = input
            .get(framed..)
            .ok_or_else(|| "truncated chunked body".to_string())?;
    }
    if !saw_terminal {
        return Err("truncated chunked body: missing terminal chunk".to_string());
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Signed ActivityPub GET / POST + WebFinger & Remote Actor/Note Resolver
// ---------------------------------------------------------------------------
