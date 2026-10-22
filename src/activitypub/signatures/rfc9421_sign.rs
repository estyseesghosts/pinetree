//! RFC 9421 outbound request signer (mirrors `sign_request_headers` for the
//! legacy scheme).

use super::digest::compute_content_digest_header;
use super::keys::rsa_sha256_sign_base64;

// ---------------------------------------------------------------------------
// RFC 9421 request signer (tests + future outbound use; mirrors
// `sign_request_headers` for the legacy scheme)
// ---------------------------------------------------------------------------

/// Headers produced by [`sign_rfc9421_headers`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rfc9421SignedHeaders {
    /// Full value for the `Content-Digest` header (`None` for bodyless requests).
    pub content_digest: Option<String>,
    /// Full value for the `Signature-Input` header.
    pub signature_input: String,
    /// Full value for the `Signature` header.
    pub signature: String,
}

/// Sign an outgoing request under this crate's RFC 9421 profile: covers
/// `@method`, `@authority`, `@path` (plus `content-digest` when `body` is
/// present) with `created`, `keyid`, `alg="rsa-v1_5-sha256"` parameters.
pub fn sign_rfc9421_headers(
    method: &str,
    path_and_query: &str,
    _scheme: &str,
    authority: &str,
    body: Option<&[u8]>,
    key_id: &str,
    private_key_pem: &str,
    created_secs: i64,
) -> Result<Rfc9421SignedHeaders, String> {
    let authority = authority.trim().to_lowercase();
    if authority.is_empty() {
        return Err("RFC 9421 signing needs a non-empty authority".to_string());
    }
    let mut covered = vec!["@method", "@authority", "@path"];
    let content_digest = body.map(compute_content_digest_header);
    if body.is_some() {
        covered.push("content-digest");
    }
    let mut lines = Vec::with_capacity(covered.len() + 1);
    for component in &covered {
        let value = match *component {
            "@method" => method.to_uppercase(),
            "@authority" => authority.clone(),
            "@path" => path_and_query.to_string(),
            _ => content_digest.clone().unwrap_or_default(),
        };
        lines.push(format!("{component}: {value}"));
    }
    let mut inner = String::from("(");
    for (i, component) in covered.iter().enumerate() {
        if i > 0 {
            inner.push(' ');
        }
        inner.push_str(&format!("\"{component}\""));
    }
    let escaped_key = key_id.replace('\\', "\\\\").replace('"', "\\\"");
    inner.push_str(&format!(
        ");created={created_secs};keyid=\"{escaped_key}\";alg=\"rsa-v1_5-sha256\""
    ));
    lines.push(format!("\"@signature-params\": {inner}"));
    let base = lines.join("\n");
    let signature = format!(
        "sig1=:{}:",
        rsa_sha256_sign_base64(private_key_pem, base.as_bytes())?
    );
    Ok(Rfc9421SignedHeaders {
        content_digest,
        signature_input: format!("sig1={inner}"),
        signature,
    })
}
