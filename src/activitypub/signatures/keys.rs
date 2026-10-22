//! RSA key material: key generation, base64 + PEM codecs, and the
//! PKCS#1 v1.5 `rsa-sha256` sign/verify primitives everything else builds on.
//!
//! RSA math, key generation, and PEM/DER codecs come from the maintained
//! RustCrypto `rsa` crate (pure Rust); base64 comes from the `base64` crate.

use base64::Engine as _;
use rsa::pkcs1::{DecodeRsaPrivateKey, DecodeRsaPublicKey, EncodeRsaPrivateKey, LineEnding};
use rsa::pkcs1v15::{Signature, VerifyingKey};
use rsa::pkcs8::{DecodePrivateKey, EncodePrivateKey};
use sha2::{Digest as _, Sha256};
use signature::Verifier;
use spki::{DecodePublicKey, EncodePublicKey};

/// RSA private key (RustCrypto `rsa` crate type, re-exported so existing
/// `signatures::RsaPrivateKey` import paths keep working).
pub use rsa::RsaPrivateKey;
/// RSA public key (RustCrypto `rsa` crate type, re-exported so existing
/// `signatures::RsaPublicKey` import paths keep working).
pub use rsa::RsaPublicKey;

// ---------------------------------------------------------------------------
// RSA key generation (via the `rsa` crate)
// ---------------------------------------------------------------------------

/// Generate a random RSA keypair of `bits` bits (default 2048 via
/// [`generate_rsa_keypair_2048`]). Returns `(private_key, public_key)`.
pub fn generate_rsa_keypair(bits: usize) -> Result<(RsaPrivateKey, RsaPublicKey), String> {
    let priv_key = RsaPrivateKey::new(&mut rand_core::OsRng, bits)
        .map_err(|e| format!("RSA key generation failed: {e}"))?;
    let pub_key = RsaPublicKey::from(&priv_key);
    Ok((priv_key, pub_key))
}

/// Generate a random 2048-bit RSA keypair.
pub fn generate_rsa_keypair_2048() -> Result<(RsaPrivateKey, RsaPublicKey), String> {
    generate_rsa_keypair(2048)
}

// ---------------------------------------------------------------------------
// Base64 (RFC 4648 standard alphabet, via the `base64` crate)
// ---------------------------------------------------------------------------

/// Standard-alphabet base64 encoding (with padding).
pub fn base64_encode(data: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(data)
}

/// Standard-alphabet base64 decoding (with padding). ASCII whitespace is
/// ignored for backwards compatibility with the pre-Slice-6 codec.
pub fn base64_decode(s: &str) -> Result<Vec<u8>, String> {
    let clean: String = s.chars().filter(|c| !c.is_whitespace()).collect();
    base64::engine::general_purpose::STANDARD
        .decode(clean.as_str())
        .map_err(|e| format!("invalid base64: {e}"))
}

// ---------------------------------------------------------------------------
// PEM encoding / decoding (via `rsa`'s pkcs1/pkcs8/spki support)
// ---------------------------------------------------------------------------

/// Encode a public key as `-----BEGIN PUBLIC KEY-----` (SPKI) PEM, the form
/// Misskey (`publicKeyPem`) and Node `crypto` expect, with LF line endings.
pub fn public_key_to_spki_pem(key: &RsaPublicKey) -> String {
    key.to_public_key_pem(LineEnding::LF)
        .expect("in-memory RSA public key always encodes as SPKI PEM")
        .as_str()
        .to_owned()
}

/// Encode a private key as `-----BEGIN PRIVATE KEY-----` (PKCS#8) PEM.
pub fn private_key_to_pkcs8_pem(key: &RsaPrivateKey) -> String {
    key.to_pkcs8_pem(LineEnding::LF)
        .expect("in-memory RSA private key always encodes as PKCS#8 PEM")
        .as_str()
        .to_owned()
}

/// Encode a private key as `-----BEGIN RSA PRIVATE KEY-----` (PKCS#1) PEM.
pub fn private_key_to_pkcs1_pem(key: &RsaPrivateKey) -> String {
    key.to_pkcs1_pem(LineEnding::LF)
        .expect("in-memory RSA private key always encodes as PKCS#1 PEM")
        .as_str()
        .to_owned()
}

/// Parse a `PUBLIC KEY` (SPKI) or `RSA PUBLIC KEY` (raw PKCS#1) PEM.
pub fn parse_public_key_pem(pem: &str) -> Result<RsaPublicKey, String> {
    // Check the longer `RSA PUBLIC KEY` label first: it contains `PUBLIC KEY`
    // as a substring.
    if pem.contains("RSA PUBLIC KEY") {
        RsaPublicKey::from_pkcs1_pem(pem)
            .map_err(|e| format!("parse PKCS#1 RSA public key PEM: {e}"))
    } else if pem.contains("PUBLIC KEY") {
        RsaPublicKey::from_public_key_pem(pem)
            .map_err(|e| format!("parse SPKI public key PEM: {e}"))
    } else {
        Err("unsupported public key PEM (expected PUBLIC KEY or RSA PUBLIC KEY)".to_string())
    }
}

/// Parse a `PRIVATE KEY` (PKCS#8) or `RSA PRIVATE KEY` (PKCS#1) PEM.
pub fn parse_private_key_pem(pem: &str) -> Result<RsaPrivateKey, String> {
    // Check the longer `RSA PRIVATE KEY` label first: it contains
    // `PRIVATE KEY` as a substring.
    if pem.contains("RSA PRIVATE KEY") {
        RsaPrivateKey::from_pkcs1_pem(pem)
            .map_err(|e| format!("parse PKCS#1 RSA private key PEM: {e}"))
    } else if pem.contains("PRIVATE KEY") {
        RsaPrivateKey::from_pkcs8_pem(pem).map_err(|e| format!("parse PKCS#8 private key PEM: {e}"))
    } else {
        Err("unsupported private key PEM (expected PRIVATE KEY or RSA PRIVATE KEY)".to_string())
    }
}

// ---------------------------------------------------------------------------
// PKCS#1 v1.5 rsa-sha256 sign & verify (via `rsa::pkcs1v15`)
// ---------------------------------------------------------------------------

/// Sign `message` with PKCS#1 v1.5 + SHA-256. Returns the raw signature bytes.
///
/// Slice 12 (M6, decision D-f(a)): signing runs on `ring` (constant-time
/// RSA with blinding) because RUSTSEC-2023-0071 (Marvin timing attack) is
/// unpatched in the `rsa` crate. The `rsa` crate still parses the stored
/// PKCS#1/PKCS#8 PEM (so existing keys load unchanged) and verifies; its
/// private-key operation is never used. `ring` requires >= 2048-bit keys.
pub fn rsa_sha256_sign(private_key_pem: &str, message: &[u8]) -> Result<Vec<u8>, String> {
    let key = parse_private_key_pem(private_key_pem)?;
    let der = key
        .to_pkcs1_der()
        .map_err(|e| format!("encode PKCS#1 DER: {e}"))?;
    let pair = ring::signature::RsaKeyPair::from_der(der.as_bytes())
        .map_err(|e| format!("load RSA key into ring (needs >= 2048 bits): {e}"))?;
    let mut sig = vec![0u8; pair.public().modulus_len()];
    pair.sign(
        &ring::signature::RSA_PKCS1_SHA256,
        &ring::rand::SystemRandom::new(),
        message,
        &mut sig,
    )
    .map_err(|e| format!("ring RSA sign: {e}"))?;
    Ok(sig)
}

/// Sign `message` and return the base64-encoded signature.
pub fn rsa_sha256_sign_base64(private_key_pem: &str, message: &[u8]) -> Result<String, String> {
    Ok(base64_encode(&rsa_sha256_sign(private_key_pem, message)?))
}

/// Verify a PKCS#1 v1.5 + SHA-256 signature. Returns `Ok(false)` for a
/// well-formed but non-matching signature; `Err` for undecodable keys.
pub fn rsa_sha256_verify(
    public_key_pem: &str,
    message: &[u8],
    signature: &[u8],
) -> Result<bool, String> {
    let key = parse_public_key_pem(public_key_pem)?;
    let verifying_key = VerifyingKey::<Sha256>::new(key);
    let Ok(sig) = Signature::try_from(signature) else {
        return Ok(false);
    };
    Ok(verifying_key.verify(message, &sig).is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::pkcs1::EncodeRsaPublicKey;

    /// ring refuses < 2048-bit keys, so signing tests share one cached
    /// 2048-bit keypair instead of generating a key per test.
    fn test_keypair() -> (RsaPrivateKey, RsaPublicKey) {
        static KEY: std::sync::OnceLock<(RsaPrivateKey, RsaPublicKey)> = std::sync::OnceLock::new();
        KEY.get_or_init(|| generate_rsa_keypair_2048().unwrap())
            .clone()
    }

    /// Crate-backed equivalent of the old textbook-fixture DER roundtrip:
    /// every PEM encoding parses back to the same key, and mismatched labels
    /// are rejected.
    #[test]
    fn key_pem_roundtrip_all_encodings() {
        let (priv_key, pub_key) = test_keypair();
        // PKCS#8 private roundtrip.
        let pkcs8_pem = private_key_to_pkcs8_pem(&priv_key);
        assert!(pkcs8_pem.starts_with("-----BEGIN PRIVATE KEY-----\n"));
        let priv_back = parse_private_key_pem(&pkcs8_pem).unwrap();
        assert_eq!(private_key_to_pkcs8_pem(&priv_back), pkcs8_pem);
        // PKCS#1 private parses to the same key.
        let pkcs1_pem = private_key_to_pkcs1_pem(&priv_key);
        assert!(pkcs1_pem.starts_with("-----BEGIN RSA PRIVATE KEY-----\n"));
        let priv_from_pkcs1 = parse_private_key_pem(&pkcs1_pem).unwrap();
        assert_eq!(private_key_to_pkcs8_pem(&priv_from_pkcs1), pkcs8_pem);
        // SPKI public roundtrip.
        let spki_pem = public_key_to_spki_pem(&pub_key);
        assert!(spki_pem.starts_with("-----BEGIN PUBLIC KEY-----\n"));
        let pub_back = parse_public_key_pem(&spki_pem).unwrap();
        assert_eq!(public_key_to_spki_pem(&pub_back), spki_pem);
        // Raw PKCS#1 public label parses to the same key.
        let raw_pub_pem = pub_back
            .to_pkcs1_pem(LineEnding::LF)
            .expect("PKCS#1 public PEM encoding");
        assert!(raw_pub_pem.starts_with("-----BEGIN RSA PUBLIC KEY-----\n"));
        let pub_from_pkcs1 = parse_public_key_pem(&raw_pub_pem).unwrap();
        assert_eq!(public_key_to_spki_pem(&pub_from_pkcs1), spki_pem);
        // PEM wrappers reject the wrong key kind.
        assert!(parse_public_key_pem(&pkcs8_pem).is_err());
        assert!(parse_private_key_pem(&spki_pem).is_err());
    }

    /// Regression coverage for the old hand-rolled CRT signing path (which
    /// underflowed for keys with `q > p` whenever `m2 > m1 + p`, failing a
    /// few percent of signatures). CRT now lives inside the `rsa` crate;
    /// this test keeps the coverage: fresh keys each sign/verify 200
    /// messages without failure.
    #[test]
    fn sign_verify_with_q_larger_than_p() {
        for _ in 0..1 {
            let (priv_key, pub_key) = test_keypair();
            let priv_pem = private_key_to_pkcs8_pem(&priv_key);
            let pub_pem = public_key_to_spki_pem(&pub_key);
            for i in 0..200 {
                let msg = format!("message {i}");
                let sig =
                    rsa_sha256_sign(&priv_pem, msg.as_bytes()).expect("signing must not fail");
                assert!(rsa_sha256_verify(&pub_pem, msg.as_bytes(), &sig).unwrap());
            }
        }
    }

    #[test]
    fn keygen_1024_pem_roundtrip_and_sign_verify() {
        let (priv_key, pub_key) = test_keypair();
        let priv_pem = private_key_to_pkcs8_pem(&priv_key);
        let pub_pem = public_key_to_spki_pem(&pub_key);
        let priv_back = parse_private_key_pem(&priv_pem).unwrap();
        let pub_back = parse_public_key_pem(&pub_pem).unwrap();
        // `rsa` key types carry no `PartialEq`; compare canonical PEMs.
        assert_eq!(private_key_to_pkcs8_pem(&priv_back), priv_pem);
        assert_eq!(public_key_to_spki_pem(&pub_back), pub_pem);
        let msg = b"hello federation";
        let sig = rsa_sha256_sign(&priv_pem, msg).unwrap();
        assert_eq!(sig.len(), 256);
        assert!(rsa_sha256_verify(&pub_pem, msg, &sig).unwrap());
        // Tampered message / signature / wrong key all fail.
        assert!(!rsa_sha256_verify(&pub_pem, b"hello federatioN", &sig).unwrap());
        let mut bad_sig = sig.clone();
        bad_sig[10] ^= 0x01;
        assert!(!rsa_sha256_verify(&pub_pem, msg, &bad_sig).unwrap());
        assert!(!rsa_sha256_verify(&pub_pem, msg, &sig[..64]).unwrap());
        let (_, other_pub) = generate_rsa_keypair(1024).unwrap();
        let other_pem = public_key_to_spki_pem(&other_pub);
        assert!(!rsa_sha256_verify(&other_pem, msg, &sig).unwrap());
        // Base64 helper agrees.
        assert_eq!(
            rsa_sha256_sign_base64(&priv_pem, msg).unwrap(),
            base64_encode(&sig)
        );
    }

    #[test]
    fn base64_known_answers() {
        assert_eq!(base64_encode(b"hello"), "aGVsbG8=");
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_decode("aGVsbG8=").unwrap(), b"hello");
        assert_eq!(base64_decode("aGVs\nbG8=").unwrap(), b"hello");
        assert_eq!(base64_decode("").unwrap(), b"");
        assert!(base64_decode("aGVsbG8").is_err());
        assert!(base64_decode("aGVsbG8!").is_err());
        // Roundtrip incl. all byte values.
        let all: Vec<u8> = (0..=255).collect();
        assert_eq!(base64_decode(&base64_encode(&all)).unwrap(), all);
    }

    #[test]
    fn node_crypto_rsa2048_fixture_interop() {
        let node_pub_pem = "-----BEGIN PUBLIC KEY-----\n\
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtB8BOMPCFjO+KBuZFVM+\n\
UlZ98+5yiaTRFJYTMzNHtP+J1jphiTe6VRLLTY92mVM+p8ziIz7hklh7ta/Y+7Hr\n\
EfOksEZG+7dvSopGJf9YBXmCeYNGOjAEjYLhx+I88CQsw3Xvu/nnzXJDOQe07hWm\n\
vF7E1AyimnOVi3bjQNrqyCQ35U9hDJZFUAfc8oHeYjqdu7N/jy0VYt+U+vCQod8a\n\
z+Fufip7bv/hSnVBLOUb61zd44ksDf1rpiA3Q617oYh5/18U5jycaTbxeWU/iNfM\n\
HVN0N9bZ8OpLCRlVWl4RN5uYhxbjrovKswWWlHX8kC/7nkp3NXHo6VY5FEVtocVH\n\
eQIDAQAB\n\
-----END PUBLIC KEY-----\n";
        let node_priv_pem = "-----BEGIN PRIVATE KEY-----\n\
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC0HwE4w8IWM74o\n\
G5kVUz5SVn3z7nKJpNEUlhMzM0e0/4nWOmGJN7pVEstNj3aZUz6nzOIjPuGSWHu1\n\
r9j7sesR86SwRkb7t29KikYl/1gFeYJ5g0Y6MASNguHH4jzwJCzDde+7+efNckM5\n\
B7TuFaa8XsTUDKKac5WLduNA2urIJDflT2EMlkVQB9zygd5iOp27s3+PLRVi35T6\n\
8JCh3xrP4W5+Kntu/+FKdUEs5RvrXN3jiSwN/WumIDdDrXuhiHn/XxTmPJxpNvF5\n\
ZT+I18wdU3Q31tnw6ksJGVVaXhE3m5iHFuOui8qzBZaUdfyQL/ueSnc1cejpVjkU\n\
RW2hxUd5AgMBAAECggEASXhMG0deJ4g7jEiVjKlDxnoZLiKY/aQI1IjSlBPar+17\n\
37I4jqv4iTL9UkmuaqQV7oQXyaCgif0lmNp9e0z7QXtMNcq0sYAFGTaFnoIk+Taz\n\
DbWKM7naYY59YfbaPGnFCIVWoCozbw2odd664lJajINrbcMpoC59VUHNOGsTUUcl\n\
Zu6XJb41vJR984iBWVl3H4IBQ0fq8omwSdDfnVesTUFAI5+UWbVsFSkR2GxeafeW\n\
wCcxb/HZGmHdZA0r+wcE5DHlvKWEuNyTLeaL+5Ls4i4tF1u5M4VObR6p8pOXCuDr\n\
bhlrMC29ppcytPA6gp3rfYjQB3KGSCez85QOZEoiMQKBgQDfFkkAYRDJXOFmiKMI\n\
5Es9iEKNDRtnJGZHsY7EUaY0KjwARnf0B107GOcpDP/rAvDcE1AgrH8WWp2U/F7c\n\
Tn0ivFoC71mcgqDW8CpxWm6gFfJxIDcQHab4pCePZTSokCGvQHkQ8SCF9CiYPF8e\n\
N36OYIfCcqzw10S61wdoneWpywKBgQDOsfNbnEM4MqP3slJ4xfZEMJiCmn9A0XLN\n\
FKlg64WwZYuPf0tVGLUsFk7v2bxKjPdIB/CN0k/bmBgbTsCVNaxN2JQgCWVN1Lkn\n\
RK74TDk1U54AoEEg5wFDboKxwila4O+QzEv9Kvc4wKfP6m3UKwcsjCHOJ8IeKNdr\n\
g6/sV7h7SwKBgQDMcmLyuwIkWD+ltSEc/APMX30GQXXUn7IwkBPT7TQRaIdWhT5X\n\
YQxbJaZro6NCII7WUcjEOPG7bc4vBN52rmPWGqGvQOVyHvECKlXrTkJF6fcnH6v2\n\
1AX6po4ux0RsGrMUJlUNZr40pR8y7VkKmzcbinHAjVHa4p31US1T/ijE4wKBgBsK\n\
kOoFUY4Cogz/2WZPj95v9SPxDoDuZfSKpXY2mfDv+Ag4ASUK7HjuGjmq5MVkF93m\n\
nAQR5by280BvVyauNPKiSQ6/6+74y5hllfaKEln1rJAsZxjk3/dwd8zFuB9Zmj35\n\
XF+sxVc5bTr4UpIVZwTe+HU+IllLwpMgUV28RDhtAoGBAKGCggYfQ1Q54vmljH1c\n\
me9QQFofDzsoq8Gj1xuhDFFNmHqW1z0DQkkf1gdKgx9E/uE4ceXTLE9z313cof8v\n\
mzqBLgLPPqB2yu9VRXgr43i6mJ3jiW6NK+tPIj7U7GNdGRd8LAmjP0W/8S6CFoYk\n\
/Xx9/OtP1rd1WHkDZbdd3gi9\n\
-----END PRIVATE KEY-----\n";
        let msg = b"(request-target): post /inbox\ndate: Wed, 14 Oct 2026 12:00:00 GMT\nhost: example.local\ndigest: SHA-256=47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=";
        let expected_sig = "HsFrxxUwBWUhCpsXx9sn2QIFZin4n1iGmxhpTX/VBfjg9wZaykhLUf4W2Y1oAGGiGPiri4AqpZ+EDqg5AobkK5ygwrbuqViCr/+7tK6RC02FAJKV+5OfQpq4OGAK9WoFm6HJ6pylZZuTHXi84fVv/u1mvQIXLGWSJ1wZL9cEE+LXlqCB4uk5ZKDZQpUfcy3djn26gTXzQRaeYSzxhyNzOmv7v6eXAu5X+Tte3Vbtj3le1UUTDhbT2MpnCuefFfRXra5tjVOMi0UtBsa97HaxgDgAmPCx6vjVtVJPgXjaQVV2bTsfXKwFVKyKLyusPTkxvTTuMaTX0lixrg8xhb09XA==";

        let our_sig = rsa_sha256_sign_base64(node_priv_pem, msg).unwrap();
        assert_eq!(our_sig, expected_sig);
        let sig_bytes = base64_decode(expected_sig).unwrap();
        assert!(rsa_sha256_verify(node_pub_pem, msg, &sig_bytes).unwrap());
    }

    /// Slice 12 (D-f(a)): the ring-backed signer produces exactly the bytes
    /// the `rsa` crate's (deterministic) PKCS#1 v1.5 signer would, and
    /// both stored PEM encodings (PKCS#1, PKCS#8) load and sign.
    #[test]
    fn slice12_ring_signature_matches_rsa_crate_and_both_pem_forms_load() {
        use rsa::pkcs1v15::SigningKey;
        use signature::{SignatureEncoding, Signer};
        let (priv_key, pub_key) = test_keypair();
        let msg = b"slice12 fixture message";
        let expected = SigningKey::<Sha256>::new(priv_key.clone())
            .sign(msg)
            .to_bytes()
            .as_ref()
            .to_vec();
        let pub_pem = public_key_to_spki_pem(&pub_key);
        for pem in [
            private_key_to_pkcs8_pem(&priv_key),
            private_key_to_pkcs1_pem(&priv_key),
        ] {
            let sig = rsa_sha256_sign(&pem, msg).expect("ring signing");
            assert_eq!(sig, expected, "PKCS#1 v1.5 is deterministic");
            assert!(rsa_sha256_verify(&pub_pem, msg, &sig).unwrap());
        }
    }

    /// ring rejects undersized keys; the error is explicit, not a panic.
    #[test]
    fn slice12_undersized_key_is_a_clean_error() {
        let (small, _) = generate_rsa_keypair(1024).unwrap();
        let err = rsa_sha256_sign(&private_key_to_pkcs8_pem(&small), b"x").unwrap_err();
        assert!(err.contains("2048"), "{err}");
    }
}
