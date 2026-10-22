//! Batch E (`docs/prod-ready.md` §6): RFC 9421 signature mode + verifier
//! integration coverage.
//!
//! Hermetic by construction: every test signs with in-process keys (one
//! static 2048-bit fixture pair plus one runtime-generated pair), resolves
//! keys from a pre-seeded local cache, and dispatches activities whose type
//! the inbox accepts-and-ignores without fetching. No test performs network
//! I/O; a test that accidentally needed the network would fail (the harness
//! points at reserved `.example`/`.test` names with no route).
//!
//! Requirement map (§6 item 7 plus items 3–6):
//!
//! * legacy-only accept: `legacy_only_post_accepts`,
//!   `inbox_accepts_legacy_only_under_permissive` (both in `legacy`).
//! * RFC-9421-only accept: `rfc9421_only_post_accepts`,
//!   `inbox_accepts_rfc9421_only_under_permissive`,
//!   `inbox_accepts_rfc9421_only_under_enforce` (all in `rfc9421`).
//! * valid dual-signature accept: `dual_signatures_agree_on_owner`,
//!   `inbox_accepts_dual_signed_post` (both in `dual`).
//! * invalid signature reject: `corrupted_and_wrong_key_signatures_rejected`
//!   (in `rejection`).
//! * tampered body reject: `tampered_body_rejected`,
//!   `inbox_rejects_tampered_body` (both in `rejection`).
//! * malformed/missing covered components reject:
//!   `malformed_and_missing_components_rejected` (in `rejection`).
//! * replay/stale timestamps reject: `stale_and_future_timestamps_rejected`,
//!   `inbox_rejects_stale_signatures` (both in `rejection`).
//! * key rotation: `rotated_key_replaces_old_key` (in `rejection`; crypto
//!   layer — the inbox/fetch refresh-on-rotation path re-fetches the actor
//!   document over the network, so it cannot run hermetically).
//! * actor mismatch reject: `signer_disagreement_fails_closed`,
//!   `inbox_rejects_actor_mismatch` (both in `rejection`).
//! * mode selection: `mode_selection_matrix`,
//!   `inbox_rejects_legacy_only_under_enforce` (in `legacy`),
//!   `fetch_gate_mode_selection` (in `modes`).
//! * actor-owner/object-authority gates still run after verification (§6
//!   item 5): `inbox_rejects_actor_mismatch` (actor `==` signer) and
//!   `inbox_rejects_foreign_object_create` (Create object same-host rule,
//!   both in `rejection`).
//! * legacy strength unweakened (§6 item 6): legacy assertions appear in
//!   every relevant test; no existing `signatures.rs` test is modified.
//!
//! Layout: shared fixtures live in `federation_sig_mode_e5/common.rs`; the
//! `#[test]`s keep their exact names, split by signature mode (`legacy`,
//! `rfc9421`, `dual`, plus cross-scheme `rejection` and the `modes` matrix).

#[path = "federation_sig_mode_e5/common.rs"]
mod common;
#[path = "federation_sig_mode_e5/dual.rs"]
mod dual;
#[path = "federation_sig_mode_e5/legacy.rs"]
mod legacy;
#[path = "federation_sig_mode_e5/modes.rs"]
mod modes;
#[path = "federation_sig_mode_e5/rejection.rs"]
mod rejection;
#[path = "federation_sig_mode_e5/rfc9421.rs"]
mod rfc9421;
