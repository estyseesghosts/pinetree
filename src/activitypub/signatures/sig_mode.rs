//! Inbound signature mode gating (Batch E): the process-wide
//! [`Rfc9421Mode`] cell, the pure [`apply_sig_mode`] decision function,
//! and the small presence/scheme helpers the gate needs.
//!
//! Mode plumbing: the configured [`Rfc9421Mode`] reaches the inbox and fetch
//! gate through the process-wide [`rfc9421_mode`]/[`set_rfc9421_mode`] cell
//! below (default `permissive`, matching `Config::default()`). This is
//! interim plumbing forced by batch file ownership (`src/config.rs` and
//! `src/main.rs` belong to another batch, and `AppServices` cannot gain a
//! field without breaking sibling-owned literal constructors): the parent
//! wires one line in `main.rs` after config load,
//! `set_rfc9421_mode(cfg.federation.rfc9421_mode)`, and may later move the
//! mode into `AppServices`. The pure decision function [`apply_sig_mode`]
//! takes the mode as a parameter so every mode combination is hermetically
//! testable without the global.

use std::sync::atomic::{AtomicU8, Ordering};

use crate::config::Rfc9421Mode;

/// Process-wide effective inbound signature mode (see module docs).
static INBOUND_RFC9421_MODE: AtomicU8 = AtomicU8::new(0);

/// Read the effective inbound signature mode (default `permissive`).
pub fn rfc9421_mode() -> Rfc9421Mode {
    match INBOUND_RFC9421_MODE.load(Ordering::SeqCst) {
        1 => Rfc9421Mode::Enforce,
        _ => Rfc9421Mode::Permissive,
    }
}

/// Set the effective inbound signature mode (startup wiring + tests).
pub fn set_rfc9421_mode(mode: Rfc9421Mode) {
    INBOUND_RFC9421_MODE.store(
        match mode {
            Rfc9421Mode::Enforce => 1,
            Rfc9421Mode::Permissive => 0,
        },
        Ordering::SeqCst,
    );
}

/// Scheme implied by a configured `public_url` (for `@scheme`/`@target-uri`
/// reconstruction). Anything that is not explicit `http://` is `https://`.
pub fn scheme_of_public_url(public_url: &str) -> &'static str {
    if public_url.trim_start().starts_with("http://") {
        "http"
    } else {
        "https"
    }
}

/// Outcome of one signature-scheme verification attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VerifyOutcome {
    /// Cryptographically valid; carries the key owner's actor URI.
    Verified(String),
    /// Absent-unusable or invalid; carries a log-safe detail string.
    Failed(String),
}

/// Pure mode gate combining the legacy and RFC 9421 attempt outcomes.
///
/// * `permissive`: accept when EITHER scheme verifies (dual-sign interop);
///   when both verify they must agree on the owner (actor-mismatch fails
///   closed). RFC 9421 fields, when present, are always verified — never
///   merely logged — but a valid legacy signature still authenticates.
/// * `enforce`: require a valid RFC 9421 signature; legacy-only, unsigned,
///   and RFC 9421-invalid requests are rejected even with a valid legacy
///   signature. A legacy signature that verifies for a DIFFERENT owner
///   than the RFC 9421 signature also fails closed.
pub fn apply_sig_mode(
    mode: Rfc9421Mode,
    legacy: Option<VerifyOutcome>,
    rfc9421: Option<VerifyOutcome>,
) -> Result<String, String> {
    match mode {
        Rfc9421Mode::Permissive => match (legacy, rfc9421) {
            (Some(VerifyOutcome::Verified(a)), Some(VerifyOutcome::Verified(b))) => {
                if a == b {
                    Ok(a)
                } else {
                    Err(format!(
                        "legacy and RFC 9421 signatures disagree on owner ({a:?} vs {b:?})"
                    ))
                }
            }
            (Some(VerifyOutcome::Verified(a)), _) => Ok(a),
            (_, Some(VerifyOutcome::Verified(b))) => Ok(b),
            (Some(VerifyOutcome::Failed(a)), Some(VerifyOutcome::Failed(b))) => {
                Err(format!("no valid signature (legacy: {a}; rfc9421: {b})"))
            }
            (Some(VerifyOutcome::Failed(a)), None) => Err(a),
            (None, Some(VerifyOutcome::Failed(b))) => Err(b),
            (None, None) => Err("no signature".to_string()),
        },
        Rfc9421Mode::Enforce => match (legacy, rfc9421) {
            (Some(VerifyOutcome::Verified(a)), Some(VerifyOutcome::Verified(b))) => {
                if a == b {
                    Ok(b)
                } else {
                    Err(format!(
                        "legacy and RFC 9421 signatures disagree on owner ({a:?} vs {b:?})"
                    ))
                }
            }
            (_, Some(VerifyOutcome::Verified(b))) => Ok(b),
            (_, Some(VerifyOutcome::Failed(detail))) => Err(detail),
            (_, None) => {
                Err("RFC 9421 signature required (federation.rfc9421_mode=enforce)".to_string())
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_sig_mode_matrix() {
        use crate::config::Rfc9421Mode;
        let alice = VerifyOutcome::Verified("https://x/users/alice".to_string());
        let bob = VerifyOutcome::Verified("https://x/users/bob".to_string());
        let bad_l = VerifyOutcome::Failed("legacy bad".to_string());
        let bad_r = VerifyOutcome::Failed("rfc9421 bad".to_string());
        // Permissive: either valid signature authenticates.
        assert_eq!(
            apply_sig_mode(Rfc9421Mode::Permissive, Some(alice.clone()), None),
            Ok("https://x/users/alice".to_string())
        );
        assert_eq!(
            apply_sig_mode(Rfc9421Mode::Permissive, None, Some(alice.clone())),
            Ok("https://x/users/alice".to_string())
        );
        assert_eq!(
            apply_sig_mode(
                Rfc9421Mode::Permissive,
                Some(alice.clone()),
                Some(alice.clone())
            ),
            Ok("https://x/users/alice".to_string())
        );
        assert_eq!(
            apply_sig_mode(
                Rfc9421Mode::Permissive,
                Some(bad_l.clone()),
                Some(alice.clone())
            ),
            Ok("https://x/users/alice".to_string())
        );
        assert_eq!(
            apply_sig_mode(
                Rfc9421Mode::Permissive,
                Some(alice.clone()),
                Some(bad_r.clone())
            ),
            Ok("https://x/users/alice".to_string())
        );
        // Permissive: dual-signer disagreement fails closed.
        assert!(
            apply_sig_mode(
                Rfc9421Mode::Permissive,
                Some(alice.clone()),
                Some(bob.clone())
            )
            .is_err()
        );
        assert!(
            apply_sig_mode(
                Rfc9421Mode::Permissive,
                Some(bad_l.clone()),
                Some(bad_r.clone())
            )
            .is_err()
        );
        assert!(apply_sig_mode(Rfc9421Mode::Permissive, Some(bad_l.clone()), None).is_err());
        assert!(apply_sig_mode(Rfc9421Mode::Permissive, None, None).is_err());
        // Enforce: RFC 9421 required; legacy-only rejected.
        assert_eq!(
            apply_sig_mode(Rfc9421Mode::Enforce, None, Some(alice.clone())),
            Ok("https://x/users/alice".to_string())
        );
        assert_eq!(
            apply_sig_mode(
                Rfc9421Mode::Enforce,
                Some(alice.clone()),
                Some(alice.clone())
            ),
            Ok("https://x/users/alice".to_string())
        );
        assert!(apply_sig_mode(Rfc9421Mode::Enforce, Some(alice.clone()), None).is_err());
        assert!(apply_sig_mode(Rfc9421Mode::Enforce, None, None).is_err());
        assert!(
            apply_sig_mode(
                Rfc9421Mode::Enforce,
                Some(bad_l.clone()),
                Some(bad_r.clone())
            )
            .is_err()
        );
        assert!(
            apply_sig_mode(
                Rfc9421Mode::Enforce,
                Some(alice.clone()),
                Some(bad_r.clone())
            )
            .is_err()
        );
        assert!(
            apply_sig_mode(Rfc9421Mode::Enforce, Some(alice.clone()), Some(bob.clone())).is_err()
        );
    }
}
