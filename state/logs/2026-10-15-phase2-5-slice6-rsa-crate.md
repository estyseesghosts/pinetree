# 2026-10-15 — Phase 2.5 Slice 6: Replace the custom RSA

- Crate choice: RustCrypto `rsa 0.9` (pure Rust, no C/CMake toolchain — fits the
  single-node story) + `signature 2` + `base64 0.23`, with `spki 0.7/pem` and
  `rand_core 0.6` as bridge deps (`rsa::spki` is not re-exported; repo `rand`
  0.10's `ThreadRng` does not satisfy rsa 0.9's `rand_core` 0.6 bounds, so
  keygen uses `rand_core::OsRng`). `aws-lc-rs` considered and rejected
  (build-toolchain weight, unjustified for this MVP).
- Pre-work gap (logged, not blocking): the live RUSTSEC advisory DB could not be
  consulted from this environment (web search unavailable). Follow-up: add
  `cargo audit`/`cargo deny` to CI and re-check the `rsa` timing/Marvin status
  there. Six deleted custom-crypto unit tests is the visible surface change.
- `signatures.rs` (2104 → ~1030 lines): custom `BigUint`/`SignedBig`,
  primality/keygen, RSA structs, DER/PEM codecs, PKCS#1 v1.5 math, custom base64
  all deleted. Same public surface preserved (`generate_rsa_keypair_2048`,
  SPKI/PKCS#8/PKCS#1 PEM encode + parse, `rsa_sha256_sign[_base64]/verify`,
  digest + HTTP-signature layer verbatim) via `pub use rsa::{RsaPublicKey,
  RsaPrivateKey}` and thin wrappers. `keys.rs` and all callers compile unchanged.
- Tests: custom-internals tests deleted (5× biguint, deterministic-keygen,
  textbook-DER); CRT regression rewritten crate-backed (3 keys × 200
  sign/verify); PEM roundtrips, HTTP-sig tests, and the Node
  `rsa2048_fixture_interop` known-answer test kept.
- Verified: `cargo build` clean (only the 3 pre-existing warnings);
  full `cargo test` green — 108 lib + 35 conformance (+1 client-smoke,
  2 crash-recovery, 2 federation incl. signed-POST end-to-end, 1 interop,
  1 minrepro), 0 failed. Stored PKCS#8/SPKI keys still parse (fixture test).
