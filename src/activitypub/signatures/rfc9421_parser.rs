//! RFC 9421 Structured-Fields subset parser: the `Signature-Input` /
//! `Signature` dictionary readers.

use super::keys::base64_decode;

// ---------------------------------------------------------------------------
// Structured-Fields subset parser (Signature-Input / Signature dictionaries)
// ---------------------------------------------------------------------------

/// A parsed signature-parameter value (supported Structured Fields subset).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SigParamValue {
    Str(String),
    Int(i64),
    Token(String),
    Bytes(Vec<u8>),
    Bool(bool),
}

pub(crate) fn param_value_to_sf(value: &SigParamValue) -> String {
    match value {
        SigParamValue::Str(s) => {
            format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
        }
        SigParamValue::Int(i) => i.to_string(),
        SigParamValue::Token(t) => t.clone(),
        SigParamValue::Bytes(b) => format!(":{}:", super::keys::base64_encode(b)),
        SigParamValue::Bool(true) => "?1".to_string(),
        SigParamValue::Bool(false) => "?0".to_string(),
    }
}

/// Parsed RFC 9421 `Signature-Input` entry (first dictionary member).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedRfc9421Signature {
    /// Dictionary label matching the `Signature` header entry.
    pub label: String,
    /// Key identifier used to resolve the verification key.
    pub key_id: String,
    /// Effective algorithm (`rsa-v1_5-sha256`; `rsa-sha256` is aliased).
    pub algorithm: String,
    /// `created` timestamp (seconds since epoch).
    pub created: Option<i64>,
    /// `expires` timestamp (seconds since epoch), when sent.
    pub expires: Option<i64>,
    /// Covered components in received order.
    pub covered: Vec<String>,
    /// All signature parameters in received order.
    pub params: Vec<(String, SigParamValue)>,
}

struct SfCursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> SfCursor<'a> {
    fn new(s: &'a str) -> Self {
        Self {
            bytes: s.as_bytes(),
            pos: 0,
        }
    }

    fn eof(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn bump(&mut self) {
        self.pos += 1;
    }

    fn skip_ows(&mut self) {
        while matches!(self.peek(), Some(b' ') | Some(b'\t')) {
            self.bump();
        }
    }

    fn expect_byte(&mut self, want: u8, what: &str) -> Result<(), String> {
        match self.peek() {
            Some(got) if got == want => {
                self.bump();
                Ok(())
            }
            _ => Err(format!("malformed Signature-Input: expected {what}")),
        }
    }

    /// Dictionary key / parameter name: `lcalpha *( lcalpha / DIGIT /
    /// "_" / "-" / "." / "*" )`.
    fn parse_key(&mut self) -> Result<String, String> {
        let start = self.pos;
        match self.peek() {
            Some(b'a'..=b'z') => self.bump(),
            _ => return Err("malformed Signature-Input: expected lowercase key".to_string()),
        }
        while matches!(
            self.peek(),
            Some(b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'.' | b'*')
        ) {
            self.bump();
        }
        String::from_utf8(self.bytes[start..self.pos].to_vec())
            .map_err(|_| "malformed Signature-Input: non-UTF8 key".to_string())
    }

    /// `sf-string`: `"` with `\`-escapes for `"` and `\` only.
    fn parse_string(&mut self) -> Result<String, String> {
        self.expect_byte(b'"', "`\"`")?;
        let mut out = String::new();
        loop {
            match self.peek() {
                None => return Err("malformed Signature-Input: unterminated string".to_string()),
                Some(b'"') => {
                    self.bump();
                    return Ok(out);
                }
                Some(b'\\') => {
                    self.bump();
                    match self.peek() {
                        Some(b'"') => out.push('"'),
                        Some(b'\\') => out.push('\\'),
                        _ => {
                            return Err("malformed Signature-Input: bad string escape".to_string());
                        }
                    }
                    self.bump();
                }
                Some(c) => {
                    if c < 0x20 || c == 0x7f {
                        return Err(
                            "malformed Signature-Input: control character in string".to_string()
                        );
                    }
                    out.push(c as char);
                    self.bump();
                }
            }
        }
    }

    /// `sf-binary`: `:base64:` (strict alphabet, no whitespace inside).
    fn parse_binary(&mut self) -> Result<Vec<u8>, String> {
        self.expect_byte(b':', "`:`")?;
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c == b':' {
                break;
            }
            if !matches!(c, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'+' | b'/' | b'=') {
                return Err("malformed Signature-Input: bad byte-sequence character".to_string());
            }
            self.bump();
        }
        self.expect_byte(b':', "closing `:`")?;
        let raw = std::str::from_utf8(&self.bytes[start..self.pos - 1])
            .map_err(|_| "malformed Signature-Input: non-UTF8 byte sequence".to_string())?;
        base64_decode(raw).map_err(|e| format!("malformed Signature-Input: {e}"))
    }

    /// Integer or decimal number; decimals are outside the supported subset.
    fn parse_number(&mut self) -> Result<SigParamValue, String> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.bump();
        }
        let digits_start = self.pos;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.bump();
        }
        if self.pos == digits_start {
            return Err("malformed Signature-Input: expected number".to_string());
        }
        if self.peek() == Some(b'.') {
            return Err("malformed Signature-Input: decimals are not supported".to_string());
        }
        std::str::from_utf8(&self.bytes[start..self.pos])
            .map_err(|_| "malformed Signature-Input: non-UTF8 number".to_string())?
            .parse::<i64>()
            .map(SigParamValue::Int)
            .map_err(|_| "malformed Signature-Input: integer out of range".to_string())
    }

    /// Token: `ALPHA / "*"` followed by `tchar / ":" / "/"` characters.
    fn parse_token(&mut self) -> Result<String, String> {
        let start = self.pos;
        match self.peek() {
            Some(b'A'..=b'Z' | b'a'..=b'z' | b'*') => self.bump(),
            _ => return Err("malformed Signature-Input: expected token".to_string()),
        }
        while let Some(c) = self.peek() {
            if matches!(c, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b':' | b'/')
                || matches!(
                    c,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
            {
                self.bump();
            } else {
                break;
            }
        }
        String::from_utf8(self.bytes[start..self.pos].to_vec())
            .map_err(|_| "malformed Signature-Input: non-UTF8 token".to_string())
    }

    fn parse_param_value(&mut self) -> Result<SigParamValue, String> {
        match self.peek() {
            Some(b'"') => self.parse_string().map(SigParamValue::Str),
            Some(b':') => self.parse_binary().map(SigParamValue::Bytes),
            Some(b'?') => {
                self.bump();
                match self.peek() {
                    Some(b'1') => {
                        self.bump();
                        Ok(SigParamValue::Bool(true))
                    }
                    Some(b'0') => {
                        self.bump();
                        Ok(SigParamValue::Bool(false))
                    }
                    _ => Err("malformed Signature-Input: bad boolean".to_string()),
                }
            }
            Some(b'0'..=b'9' | b'-') => self.parse_number(),
            Some(b'(') => Err(
                "malformed Signature-Input: inner-list parameter values are not supported"
                    .to_string(),
            ),
            Some(_) => self.parse_token().map(SigParamValue::Token),
            None => Err("malformed Signature-Input: truncated parameter value".to_string()),
        }
    }
}

/// Parse an RFC 9421 `Signature-Input` dictionary value. The FIRST member is
/// returned; extra members are parsed (labels must be unique) and ignored.
/// Duplicate covered components, component parameters, empty covered lists,
/// missing `keyid`, and unsupported `alg` values are all errors.
pub fn parse_signature_input(header_value: &str) -> Result<ParsedRfc9421Signature, String> {
    let mut cursor = SfCursor::new(header_value);
    let mut members: Vec<(String, Vec<String>, Vec<(String, SigParamValue)>)> = Vec::new();
    loop {
        cursor.skip_ows();
        if cursor.eof() {
            break;
        }
        if !members.is_empty() {
            cursor.expect_byte(b',', "`,`")?;
            cursor.skip_ows();
            if cursor.eof() {
                return Err("malformed Signature-Input: trailing comma".to_string());
            }
        }
        let label = cursor.parse_key()?;
        cursor.skip_ows();
        cursor.expect_byte(b'=', "`=`")?;
        cursor.skip_ows();
        cursor.expect_byte(b'(', "`(`")?;
        let mut covered = Vec::new();
        loop {
            cursor.skip_ows();
            if cursor.peek() == Some(b')') {
                cursor.bump();
                break;
            }
            if cursor.eof() {
                return Err("malformed Signature-Input: unterminated covered list".to_string());
            }
            let item = cursor.parse_string()?;
            cursor.skip_ows();
            if cursor.peek() == Some(b';') {
                return Err(format!(
                    "unsupported parameters on covered component {item:?}"
                ));
            }
            covered.push(item);
        }
        let mut params: Vec<(String, SigParamValue)> = Vec::new();
        loop {
            cursor.skip_ows();
            if cursor.peek() != Some(b';') {
                break;
            }
            cursor.bump();
            cursor.skip_ows();
            let key = cursor.parse_key()?;
            if params.iter().any(|(k, _)| k == &key) {
                return Err(format!("duplicate Signature-Input parameter: {key}"));
            }
            cursor.skip_ows();
            let value = if cursor.peek() == Some(b'=') {
                cursor.bump();
                cursor.skip_ows();
                cursor.parse_param_value()?
            } else {
                SigParamValue::Bool(true)
            };
            params.push((key, value));
        }
        members.push((label, covered, params));
    }
    if members.is_empty() {
        return Err("malformed Signature-Input: empty dictionary".to_string());
    }
    let mut seen_labels: Vec<&str> = Vec::with_capacity(members.len());
    for (label, _, _) in &members {
        if seen_labels.contains(&label.as_str()) {
            return Err(format!("duplicate Signature-Input label: {label}"));
        }
        seen_labels.push(label.as_str());
    }
    let (label, covered, params) = &members[0];
    if covered.is_empty() {
        return Err("malformed Signature-Input: no covered components".to_string());
    }
    let mut seen: Vec<&str> = Vec::with_capacity(covered.len());
    for component in covered {
        if seen.contains(&component.as_str()) {
            return Err(format!("duplicate covered component: {component}"));
        }
        seen.push(component.as_str());
    }
    let key_id = params
        .iter()
        .find(|(k, _)| k == "keyid")
        .and_then(|(_, v)| match v {
            SigParamValue::Str(s) if !s.trim().is_empty() => Some(s.clone()),
            _ => None,
        })
        .ok_or("Signature-Input missing keyid".to_string())?;
    let mut algorithm = "rsa-v1_5-sha256".to_string();
    if let Some((_, value)) = params.iter().find(|(k, _)| k == "alg") {
        let name = match value {
            SigParamValue::Str(s) | SigParamValue::Token(s) => s.to_lowercase(),
            _ => return Err("malformed Signature-Input: non-string alg".to_string()),
        };
        if name == "rsa-v1_5-sha256" || name == "rsa-sha256" {
            algorithm = "rsa-v1_5-sha256".to_string();
        } else {
            return Err(format!("unsupported signature algorithm: {name}"));
        }
    }
    let int_param = |name: &str| -> Result<Option<i64>, String> {
        match params.iter().find(|(k, _)| k == name) {
            None => Ok(None),
            Some((_, SigParamValue::Int(i))) => Ok(Some(*i)),
            Some(_) => Err(format!("malformed Signature-Input: non-integer {name}")),
        }
    };
    Ok(ParsedRfc9421Signature {
        label: label.clone(),
        key_id,
        algorithm,
        created: int_param("created")?,
        expires: int_param("expires")?,
        covered: covered.clone(),
        params: params.clone(),
    })
}

/// Parse an RFC 9421 `Signature` dictionary value and return the raw
/// signature bytes for `label`. A leading `Signature ` prefix (as in an
/// `Authorization: Signature ...` value) is tolerated.
pub fn parse_http_signature_value(header_value: &str, label: &str) -> Result<Vec<u8>, String> {
    let mut value = header_value.trim();
    if value.len() >= 9
        && value[..9].eq_ignore_ascii_case("signature")
        && (value.len() == 9 || value.as_bytes()[9].is_ascii_whitespace())
    {
        value = value[9..].trim_start();
    }
    let mut cursor = SfCursor::new(value);
    let mut found: Option<Vec<u8>> = None;
    let mut labels: Vec<String> = Vec::new();
    loop {
        cursor.skip_ows();
        if cursor.eof() {
            break;
        }
        if !labels.is_empty() {
            cursor.expect_byte(b',', "`,`")?;
            cursor.skip_ows();
            if cursor.eof() {
                return Err("malformed Signature: trailing comma".to_string());
            }
        }
        let entry_label = cursor.parse_key()?;
        if labels.contains(&entry_label) {
            return Err(format!("duplicate Signature label: {entry_label}"));
        }
        labels.push(entry_label.clone());
        cursor.skip_ows();
        cursor.expect_byte(b'=', "`=`")?;
        cursor.skip_ows();
        let bytes = cursor.parse_binary()?;
        // Unknown trailing parameters on a Signature entry are not part of
        // the supported profile; the dictionary shape has none.
        cursor.skip_ows();
        if cursor.peek() == Some(b';') {
            return Err("malformed Signature: entry parameters are not supported".to_string());
        }
        if entry_label == label {
            found = Some(bytes);
        }
    }
    found.ok_or_else(|| format!("Signature entry {label:?} not present"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activitypub::signatures::legacy::parse_http_date;
    use crate::activitypub::signatures::rfc9421::build_rfc9421_signature_base;
    use axum::http::{HeaderMap, HeaderName, HeaderValue};

    use std::str::FromStr;

    fn test_headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (k, v) in pairs {
            map.insert(
                HeaderName::from_str(k).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        map
    }

    fn date_fixture() -> (&'static str, i64) {
        let s = "Wed, 14 Oct 2026 12:00:00 GMT";
        (s, parse_http_date(s).unwrap())
    }

    #[test]
    fn rfc9421_malformed_inputs_rejected() {
        // Signature-Input values that must all fail to parse.
        let bad_inputs = [
            "",
            "not-a-dictionary",
            "sig1=",
            "sig1=()",
            "sig1=();created=1",
            "sig1=(\"@method\" \"@authority\");created=notanumber",
            "sig1=(\"@method\" \"@authority\");created=\"123\"",
            "sig1=(\"@method\" \"@authority\");created=1.5",
            "sig1=(\"@method\" \"@authority\" \"@method\");created=1;keyid=\"k\"",
            "sig1=(\"@method\" \"@authority\";key \"@path\");created=1;keyid=\"k\"",
            "sig1=(\"@method\" \"@authority\");created=1;created=2;keyid=\"k\"",
            "sig1=(\"@method\" \"@authority\");created=1;keyid=\"k\",sig1=(\"@method\");created=1",
            "a=(\"@method\" \"@authority\");created=1;keyid=\"k\",b=(\"@method\" \"@authority\");created=1;keyid=\"k\",b=(\"@method\" \"@authority\");created=1;keyid=\"k\"",
            "sig1=(\"@method\" \"@authority\");created=1;keyid=\"k\";alg=\"hmac-sha256\"",
            "sig1=(\"@method\" \"@authority\");created=1;keyid=\"k\";alg=\"ed25519\"",
            "sig1=(\"@method\" \"@authority\");created=1;keyid=\"k\";alg=\"rsa-pss-sha512\"",
            "sig1=(\"@method\" \"@authority\");created=1;keyid=\"\"",
            "sig1=(\"@method\" \"@authority\");created=1,",
            "SIG1=(\"@method\" \"@authority\");created=1;keyid=\"k\"",
        ];
        for bad in bad_inputs {
            assert!(parse_signature_input(bad).is_err(), "accepted: {bad}");
        }
        // `created` is optional at parse time (verification requires it);
        // exotic-but-well-formed covered sets parse and fail at base build.
        let no_created =
            parse_signature_input("sig1=(\"@method\" \"@authority\" \"@path\");keyid=\"k\"")
                .unwrap();
        assert_eq!(no_created.created, None);
        let sparse = parse_signature_input("sig1=(\"@method\");created=1;keyid=\"k\"").unwrap();
        assert_eq!(sparse.covered, vec!["@method".to_string()]);
        let (date, _) = date_fixture();
        let dummy = test_headers(&[("date", date), ("host", "example.local")]);
        assert!(
            build_rfc9421_signature_base("GET", "/x", "https", "example.local", &dummy, &sparse)
                .is_err()
        );
        for bad_covered in ["@status", "@unknown-component", "@Method"] {
            let parsed = parse_signature_input(&format!(
                "sig1=(\"@method\" \"@authority\" \"@path\" \"{bad_covered}\");created=1;keyid=\"k\""
            ))
            .unwrap();
            assert!(
                build_rfc9421_signature_base(
                    "GET",
                    "/x",
                    "https",
                    "example.local",
                    &dummy,
                    &parsed
                )
                .is_err(),
                "built base for {bad_covered}"
            );
        }
        // Well-formed parses with expected fields.
        let parsed = parse_signature_input(
            "sig1=(\"@method\" \"@authority\" \"@path\" \"content-digest\");created=1700000000;keyid=\"https://x/users/1#k\"",
        )
        .unwrap();
        assert_eq!(parsed.label, "sig1");
        assert_eq!(parsed.created, Some(1_700_000_000));
        assert_eq!(parsed.expires, None);
        // hs2019-style alias and token alg parse; unknown algs do not.
        let aliased = parse_signature_input(
            "sig1=(\"@method\" \"@authority\" \"@path\");created=1;keyid=\"k\";alg=\"rsa-sha256\"",
        )
        .unwrap();
        assert_eq!(aliased.algorithm, "rsa-v1_5-sha256");
        // Signature dictionary values that must all fail.
        assert!(parse_http_signature_value("sig1=:AAA", "sig1").is_err());
        assert!(parse_http_signature_value("sig1=\"AAA=\"", "sig1").is_err());
        assert!(parse_http_signature_value("other=:QUJD:", "sig1").is_err());
        assert!(parse_http_signature_value("sig1=:QUJD:,sig1=:QUJD:", "sig1").is_err());
        assert_eq!(
            parse_http_signature_value("sig1=:QUJD:,other=:REVG:", "sig1").unwrap(),
            b"ABC"
        );
        // Leading `Signature ` prefix tolerated (Authorization shape).
        assert_eq!(
            parse_http_signature_value("Signature sig1=:QUJD:", "sig1").unwrap(),
            b"ABC"
        );
    }
}
