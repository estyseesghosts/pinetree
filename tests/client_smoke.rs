//! Slice E1 cargo integration test: run the live binary + misskey-js
//! 2026.10.1-alpha.0 client smoke suite (`scripts/smoke-phase1.sh`) so
//! `cargo test` automatically verifies register -> auth -> post -> timeline
//! -> follow -> react against a real server, including a SIGKILL restart.

#[test]
fn client_smoke_live_binary_and_misskey_js() {
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/scripts/smoke-phase1.sh");
    // On a Windows host bare `bash` resolves to the WSL stub, which cannot
    // run this suite (Windows paths, native binaries). Prefer native Git
    // Bash exactly like contributors invoke the scripts by hand; fall back
    // to `bash` everywhere else.
    #[cfg(windows)]
    let shell = {
        let git_bash = r"C:\Program Files\Git\bin\bash.exe";
        if std::path::Path::new(git_bash).is_file() {
            git_bash.to_string()
        } else {
            "bash".to_string()
        }
    };
    #[cfg(not(windows))]
    let shell = "bash".to_string();
    let status = std::process::Command::new(&shell)
        .arg(script)
        .status()
        .expect("run scripts/smoke-phase1.sh");
    assert!(
        status.success(),
        "scripts/smoke-phase1.sh failed with {status} (shell: {shell})"
    );
}
