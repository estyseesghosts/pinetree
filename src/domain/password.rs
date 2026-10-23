//! Password hashing and verification (domain layer).
//!
//! Argon2id PHC strings (`$argon2id$...`) are the current hash format;
//! legacy `salt:hash` SHA-256 hex values still verify and are upgraded on
//! the next successful sign-in. This lives in `domain` (not the Misskey
//! HTTP adapter) so application use cases — self-service account deletion,
//! TOTP enrol/rotate/unregister — can verify passwords without importing
//! the adapter crate path (dependency direction `domain <- application
//! <- adapters`). The Misskey adapter re-exports thin delegates over these
//! functions for its async/blocking-pool wrappers.

use argon2::{
    Algorithm, Argon2, Params, Version,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng},
};
use sha2::{Digest, Sha256};

/// Argon2id cost parameters for new password hashes: the OWASP baseline of
/// 19 MiB memory, 4 passes, 1 lane.
pub const ARGON2_M_COST_KIB: u32 = 19_456;
pub const ARGON2_T_COST: u32 = 4;
pub const ARGON2_P_COST: u32 = 1;

/// Every Argon2id PHC string this module writes starts with this prefix.
const ARGON2ID_PREFIX: &str = "$argon2id$";
/// Argon2 version 0x13 is written as `v=19` in the PHC string.
const ARGON2_VERSION_NUMBER: u32 = 19;

fn argon2_current() -> Argon2<'static> {
    let params = Params::new(ARGON2_M_COST_KIB, ARGON2_T_COST, ARGON2_P_COST, None)
        .expect("argon2 cost parameters are valid constants");
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
}

/// Argon2id PHC hash of `password` with a fresh random salt. Blocking and
/// CPU-heavy: async callers hash on the blocking pool.
pub fn hash_password(password: &str) -> String {
    let salt = SaltString::generate(&mut OsRng);
    argon2_current()
        .hash_password(password.as_bytes(), &salt)
        .expect("argon2 hashing with fixed valid parameters")
        .to_string()
}

/// Verify `password` against a stored hash: Argon2id PHC strings are checked
/// with their own embedded parameters, anything else as a legacy
/// `salt:hash` value.
pub fn verify_password(password: &str, stored: &str) -> bool {
    if stored.starts_with(ARGON2ID_PREFIX) {
        return match PasswordHash::new(stored) {
            Ok(parsed) => Argon2::default()
                .verify_password(password.as_bytes(), &parsed)
                .is_ok(),
            Err(_) => false,
        };
    }
    verify_legacy_password(password, stored)
}

/// True when a successful login should replace `stored`: a legacy
/// `salt:hash` value, an unparseable value, or an Argon2id hash whose
/// version or cost parameters differ from the current constants.
pub fn needs_rehash(stored: &str) -> bool {
    if !stored.starts_with(ARGON2ID_PREFIX) {
        return true;
    }
    let parsed = match PasswordHash::new(stored) {
        Ok(parsed) => parsed,
        Err(_) => return true,
    };
    if parsed.version != Some(ARGON2_VERSION_NUMBER) {
        return true;
    }
    match Params::try_from(&parsed) {
        Ok(params) => {
            params.m_cost() != ARGON2_M_COST_KIB
                || params.t_cost() != ARGON2_T_COST
                || params.p_cost() != ARGON2_P_COST
        }
        Err(_) => true,
    }
}

/// Legacy `salt:hash` (salted SHA-256 hex) check, constant-time.
fn verify_legacy_password(password: &str, stored: &str) -> bool {
    let (salt, expected) = match stored.split_once(':') {
        Some(parts) => parts,
        None => return false,
    };
    let expected_bytes = match hex::decode(expected) {
        Ok(bytes) => bytes,
        Err(_) => return false,
    };
    if hex::decode(salt).is_err() {
        return false;
    }
    let actual = password_digest(salt, password);
    actual.len() == expected_bytes.len()
        && actual
            .iter()
            .zip(expected_bytes.iter())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
}

fn password_digest(salt_hex: &str, password: &str) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(salt_hex.as_bytes());
    hasher.update(b":");
    hasher.update(password.as_bytes());
    hasher.finalize().to_vec()
}
