//! Slice A3: RSA-2048 keys, PEM codecs, PKCS#1 v1.5 `rsa-sha256`,
//! `Digest` header helpers, and legacy HTTP Signatures (draft-cavage).
//!
//! RSA math, key generation, and PEM/DER codecs come from the maintained
//! RustCrypto `rsa` crate (pure Rust); base64 comes from the `base64` crate.
//! This module keeps the HTTP-signature layer plus thin crate-backed wrappers
//! with the pre-Slice-6 public surface (`sha2` for hashing, `rand` for key
//! generation, `axum::http` for headers, `chrono` for `Date` skew checks).

mod digest;
mod keys;
mod legacy;
mod rfc9421;
mod rfc9421_parser;
mod rfc9421_sign;
mod sig_mode;

pub use digest::{
    compute_content_digest_header, compute_digest_header, verify_content_digest_header,
    verify_digest_header,
};
pub use keys::{
    RsaPrivateKey, RsaPublicKey, base64_decode, base64_encode, generate_rsa_keypair,
    generate_rsa_keypair_2048, parse_private_key_pem, parse_public_key_pem,
    private_key_to_pkcs1_pem, private_key_to_pkcs8_pem, public_key_to_spki_pem, rsa_sha256_sign,
    rsa_sha256_sign_base64, rsa_sha256_verify,
};
pub use legacy::{
    ParsedSignature, SignedRequestHeaders, actor_uri_from_key_id, build_signing_string,
    has_legacy_signature, parse_first_legacy_signature, parse_http_date, parse_signature_header,
    sign_request_headers, verify_http_signature,
};
pub use rfc9421::{build_rfc9421_signature_base, verify_rfc9421_signature};
pub use rfc9421_parser::{
    ParsedRfc9421Signature, SigParamValue, parse_http_signature_value, parse_signature_input,
};
pub use rfc9421_sign::{Rfc9421SignedHeaders, sign_rfc9421_headers};
pub use sig_mode::{
    VerifyOutcome, apply_sig_mode, rfc9421_mode, scheme_of_public_url, set_rfc9421_mode,
};
