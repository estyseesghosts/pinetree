//! Phase 3 profile contract suite: run the live binary + real misskey-js
//! 2026.10.1-alpha.0 `APIClient` + `Stream` suite (`scripts/smoke-phase3.sh`)
//! so `cargo test` verifies every profile endpoint/channel in
//! `docs/phase-3-profile.md` against the actual client library.
//!
//! Skips (passes vacuously) when the git-ignored `./misskey` checkout is
//! absent, e.g. in clean clones without symlinks — same rule as the
//! packer snapshot tests.

#[test]
fn client_profile_live_binary_and_misskey_js() {
    let checkout = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("misskey/packages/misskey-js/src/streaming.ts");
    if !checkout.is_file() {
        eprintln!("skipping client_profile: ./misskey checkout absent");
        return;
    }
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/scripts/smoke-phase3.sh");
    let status = std::process::Command::new("bash")
        .arg(script)
        .status()
        .expect("run scripts/smoke-phase3.sh");
    assert!(
        status.success(),
        "scripts/smoke-phase3.sh failed with {status}"
    );
}
