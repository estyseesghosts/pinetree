# Phase 2.6 Slice 12 — RSA advisory decision (M6, D-f)

Date: 2026-10-15. Status: implemented, full `cargo test` green.

## Advisory status
RUSTSEC-2023-0071 (Marvin timing attack, `rsa` crate): fetched the advisory text from rustsec/advisory-db (raw GitHub, 2026-10-15): `patched = []`; it states rsa 0.9.10 (latest stable) and 0.10.0-rc.18 are still affected as of 2026-09-12 and that the crypto-bigint migration did not close it. The local registry has 0.10.0-rc.19; I did not test it independently, and rc.19 is not stated as patched. `cargo audit` is not installed here, so no local scan output. Conclusion: upgrading `rsa` does not fix it.

## Decision D-f: (a) sign with `ring`
`rsa_sha256_sign` (`src/activitypub/signatures.rs`) now parses the stored PEM with the `rsa` crate (PKCS#1 or PKCS#8, unchanged), re-encodes to PKCS#1 DER and signs with `ring::signature::RsaKeyPair` / `RSA_PKCS1_SHA256` (constant-time, blinded). `rsa` is kept for keygen, PEM/SPKI codecs and verification (public-key operation, no secret timing). `ring` 0.17 chosen over `aws-lc-rs`: no `cmake` in this sandbox. Constraint: ring rejects keys < 2048 bits; production keys are 2048 (`generate_rsa_keypair_2048`), undersized keys now return a clean error. Test keys moved to one cached 2048-bit keypair.

## Tests
- `slice12_ring_signature_matches_rsa_crate_and_both_pem_forms_load`: ring signature is byte-identical to the `rsa` crate's deterministic PKCS#1 v1.5 signature; both PKCS#1 and PKCS#8 PEMs load, sign and verify.
- `slice12_undersized_key_is_a_clean_error`.
- Existing sign/verify, HTTP-signature and federation tests pass unchanged apart from key size (`sig.len()` 128 → 256).
- Failing-first: not reconstructed. The equality test passes against both implementations by design (it is a regression guard); the undersized-key test would fail against the old signer (which accepted 1024-bit keys) but I did not execute that.

## Verification
Full `cargo test`: lib 193, client_smoke 1 (SMOKE_OK), conformance 51, crash_recovery 2, federation 2 (live two-node pinetree signing over ring), federation_interop 1, minrepro 1 = 251 passed, 0 failed (slice 11: 249).

## Not verified
`scripts/smoke-phase2.sh` / live Misskey+Sharkey signature acceptance (Node 22 + Docker unavailable); ring output is standard PKCS#1 v1.5 SHA-256, and byte-equality with the previous signer is tested. Private-key parsing still passes through the `rsa` crate (non-timing-sensitive local parse, but note it touches secret bytes once at load).
