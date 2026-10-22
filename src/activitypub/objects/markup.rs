//! MFM ↔ HTML conversion and sanitization (Slice B2): local MFM / plain
//! note text becomes safe ActivityPub HTML, and untrusted inbound HTML or
//! MFM is sanitized back into safe plain/MFM text.

// ---------------------------------------------------------------------------
// Slice B2: MFM ↔ HTML conversion and sanitization
// ---------------------------------------------------------------------------

pub fn html_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(ch),
        }
    }
    out
}

fn html_unescape(input: &str) -> String {
    input
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
        .replace("&#x27;", "'")
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
}

/// Convert local MFM / plain note text into safe ActivityPub HTML (`<p>`,
/// `<br>`, `<a class="u-url mention">`, `#hashtag` links).
pub fn mfm_to_html(text: &str, public_url: &str) -> String {
    let base = public_url.trim_end_matches('/');
    let mut rendered_lines = Vec::new();
    for line in text.split('\n') {
        rendered_lines.push(render_mfm_line(line, base));
    }
    format!("<p><span>{}</span></p>", rendered_lines.join("<br>"))
}

fn render_mfm_line(line: &str, base_url: &str) -> String {
    let chars: Vec<char> = line.chars().collect();
    let mut out = String::with_capacity(line.len() + 16);
    let mut i = 0;
    while i < chars.len() {
        let boundary = i == 0 || !(chars[i - 1].is_alphanumeric() || chars[i - 1] == '_');
        if chars[i] == '@' && boundary {
            // Parse @user or @user@host
            let mut j = i + 1;
            while j < chars.len()
                && (chars[j].is_ascii_alphanumeric()
                    || chars[j] == '_'
                    || chars[j] == '.'
                    || chars[j] == '-')
            {
                j += 1;
            }
            if j > i + 1 {
                let mut host_end = j;
                if j < chars.len() && chars[j] == '@' {
                    let mut k = j + 1;
                    while k < chars.len()
                        && (chars[k].is_ascii_alphanumeric()
                            || chars[k] == '.'
                            || chars[k] == '-'
                            || chars[k] == ':')
                    {
                        k += 1;
                    }
                    // Trim trailing dot
                    while k > j + 1 && chars[k - 1] == '.' {
                        k -= 1;
                    }
                    if k > j + 1 {
                        host_end = k;
                    }
                }
                let mention: String = chars[i..host_end].iter().collect();
                let username: String = chars[i + 1..j].iter().collect();
                let href = if host_end > j {
                    let host: String = chars[j + 1..host_end].iter().collect();
                    format!("https://{host}/@{username}")
                } else {
                    format!("{base_url}/@{username}")
                };
                out.push_str(&format!(
                    "<a href=\"{}\" class=\"u-url mention\">{}</a>",
                    html_escape(&href),
                    html_escape(&mention)
                ));
                i = host_end;
                continue;
            }
        } else if chars[i] == '#' && boundary {
            let mut j = i + 1;
            while j < chars.len() && (chars[j].is_alphanumeric() || chars[j] == '_') {
                j += 1;
            }
            if j > i + 1 {
                let tag: String = chars[i + 1..j].iter().collect();
                let href = format!("{base_url}/tags/{}", html_escape(&tag.to_lowercase()));
                out.push_str(&format!(
                    "<a href=\"{href}\" rel=\"tag\">#{}</a>",
                    html_escape(&tag)
                ));
                i = j;
                continue;
            }
        }
        match chars[i] {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            ch => out.push(ch),
        }
        i += 1;
    }
    out
}

/// Sanitize untrusted inbound HTML or MFM text into safe plain/MFM text.
///
/// - Completely drops `<script>...</script>`, `<style>...</style>`,
///   `<iframe>...</iframe>`, `<object>...</object>` blocks and their inner text.
/// - Converts `<br>`, `<br/>`, `</p>`, `</div>`, `</li>` into newlines.
/// - Strips all other HTML tags (including any `on*` event handler attributes
///   or `javascript:` / `data:` / `vbscript:` links).
/// - Neutralizes any `javascript:`, `vbscript:`, or `data:text/html` URL schemes
///   inside MFM links `[label](url)`.
pub fn sanitize_html_to_mfm(input: &str) -> String {
    let mut s = input.to_string();
    for tag in ["script", "style", "iframe", "object"] {
        s = strip_element_with_body(&s, tag);
    }

    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'<' {
            if let Some(end_rel) = s[i..].find('>') {
                let tag_body = s[i + 1..i + end_rel].trim().to_ascii_lowercase();
                let tag_name = tag_body
                    .trim_start_matches('/')
                    .split(|c: char| c.is_ascii_whitespace() || c == '/')
                    .next()
                    .unwrap_or("");
                let is_closing = tag_body.starts_with('/');
                match (is_closing, tag_name) {
                    (false, "br") => out.push('\n'),
                    (true, "p" | "div" | "li" | "blockquote" | "pre") => out.push('\n'),
                    _ => {}
                }
                i += end_rel + 1;
                continue;
            }
        }
        let Some(rest) = s.get(i..) else { break };
        let Some(ch) = rest.chars().next() else { break };
        out.push(ch);
        i += ch.len_utf8();
    }

    let unescaped = html_unescape(&out);
    let cleaned = sanitize_mfm_text(&unescaped);
    cleaned.trim().to_string()
}

/// Sanitize inbound MFM source text (`source.content` or `_misskey_summary`),
/// stripping dangerous HTML tags (`<script>`, `<style>`, `<iframe>`, `<object>`,
/// `on*=` attributes) and neutralizing `javascript:`/`vbscript:`/`data:` URLs.
pub fn sanitize_mfm_text(input: &str) -> String {
    let mut s = input.to_string();
    for tag in ["script", "style", "iframe", "object"] {
        s = strip_element_with_body(&s, tag);
    }
    // Strip any raw HTML tags with event handlers or dangerous schemes.
    let mut cleaned = String::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        if s.as_bytes()[i] == b'<' {
            if let Some(end_rel) = s[i..].find('>') {
                let inner = s[i + 1..i + end_rel].to_ascii_lowercase();
                let tag_name = inner
                    .trim_start_matches('/')
                    .split(|c: char| c.is_ascii_whitespace() || c == '/')
                    .next()
                    .unwrap_or("");
                if matches!(
                    tag_name,
                    "script"
                        | "style"
                        | "iframe"
                        | "object"
                        | "embed"
                        | "form"
                        | "input"
                        | "button"
                        | "svg"
                        | "math"
                        | "img"
                        | "a"
                        | "div"
                        | "span"
                        | "p"
                        | "br"
                ) || inner.contains("on")
                    || inner.contains("javascript:")
                    || inner.contains("data:")
                    || inner.contains("vbscript:")
                {
                    if tag_name == "br" {
                        cleaned.push('\n');
                    }
                    i += end_rel + 1;
                    continue;
                }
            }
        }
        let Some(rest) = s.get(i..) else { break };
        let Some(ch) = rest.chars().next() else { break };
        cleaned.push(ch);
        i += ch.len_utf8();
    }

    // Neutralize dangerous schemes in MFM links `[text](javascript:...)`.
    let mut final_out = String::with_capacity(cleaned.len());
    let lower = cleaned.to_ascii_lowercase();
    let mut pos = 0;
    while pos < cleaned.len() {
        let rest_lower = &lower[pos..];
        if rest_lower.starts_with("javascript:")
            || rest_lower.starts_with("vbscript:")
            || rest_lower.starts_with("data:text/html")
        {
            final_out.push_str("blocked:");
            if let Some(colon) = cleaned[pos..].find(':') {
                pos += colon + 1;
                continue;
            }
        }
        let Some(rest) = cleaned.get(pos..) else {
            break;
        };
        let Some(ch) = rest.chars().next() else { break };
        final_out.push(ch);
        pos += ch.len_utf8();
    }
    // Collapse 3+ consecutive newlines down to 2.
    while final_out.contains("\n\n\n") {
        final_out = final_out.replace("\n\n\n", "\n\n");
    }
    final_out.trim().to_string()
}

fn strip_element_with_body(input: &str, tag: &str) -> String {
    let open_pat = format!("<{tag}");
    let close_pat = format!("</{tag}>");
    let mut s = input.to_string();
    loop {
        let lower = s.to_ascii_lowercase();
        let Some(start) = lower.find(&open_pat) else {
            break;
        };
        if let Some(close_rel) = lower[start..].find(&close_pat) {
            let end = start + close_rel + close_pat.len();
            s.replace_range(start..end, "");
        } else if let Some(gt_rel) = lower[start..].find('>') {
            let end = start + gt_rel + 1;
            s.replace_range(start..end, "");
        } else {
            s.replace_range(start..s.len(), "");
            break;
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mfm_to_html_and_sanitization_strip_xss_payloads() {
        let html = mfm_to_html(
            "Hello @alice@remote.example #fediverse\nLine 2",
            "https://pinetree.local",
        );
        assert!(html.contains("<a href=\"https://remote.example/@alice\" class=\"u-url mention\">@alice@remote.example</a>"));
        assert!(html.contains(
            "<a href=\"https://pinetree.local/tags/fediverse\" rel=\"tag\">#fediverse</a>"
        ));
        assert!(html.contains("<br>"));

        let dirty = "<p>Safe text<script>alert('xss')</script><style>body{display:none}</style><iframe src=\"https://evil.example\"></iframe><img src=x onerror=\"alert(1)\"><a href=\"javascript:alert(2)\">click</a><br>Second line</p>";
        let cleaned = sanitize_html_to_mfm(dirty);
        assert_eq!(cleaned, "Safe textclick\nSecond line");
        assert!(!cleaned.contains("script"));
        assert!(!cleaned.contains("alert"));
        assert!(!cleaned.contains("onerror"));
        assert!(!cleaned.contains("javascript:"));

        let dirty_mfm = "Hi [bad](javascript:alert(1)) <script>evil()</script> world";
        let cleaned_mfm = sanitize_mfm_text(dirty_mfm);
        assert!(!cleaned_mfm.contains("javascript:"));
        assert!(!cleaned_mfm.contains("<script>"));
        assert!(cleaned_mfm.contains("Hi [bad](blocked:alert(1))"));
    }
}
