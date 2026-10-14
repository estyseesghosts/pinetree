//! Slice E1 cargo integration test: run the live binary + misskey-js
//! 2026.10.1-alpha.0 client smoke suite (`scripts/smoke-phase1.sh`) so
//! `cargo test` automatically verifies register -> auth -> post -> timeline
//! -> follow -> react against a real server, including a SIGKILL restart.

#[test]
fn client_smoke_live_binary_and_misskey_js() {
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/scripts/smoke-phase1.sh");
    let status = std::process::Command::new("bash")
        .arg(script)
        .status()
        .expect("run scripts/smoke-phase1.sh");
    assert!(
        status.success(),
        "scripts/smoke-phase1.sh failed with {status}"
    );
}
