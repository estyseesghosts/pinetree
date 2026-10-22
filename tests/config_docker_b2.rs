//! Batch B (prod-ready.md §3) config/Docker policy tests.
//!
//! Locks in: transcode disabled by default (Rust `Config::default` + shipped
//! Docker default), fail-closed FFmpeg probe logic, the explicit-JSON unsafe
//! local-HTTP opt-in, the placeholder canonical-URL forcing function, and the
//! two-target Dockerfile shape.
//!
//! What this file does NOT cover (needs a Docker host; see
//! `scripts/tests/test-docker-ffmpeg-b2.sh`): building both images, the five
//! end-to-end Docker scenarios, and the real worker-path transcode smoke.

use std::path::PathBuf;

use pinetree::config::{Config, PublicHostClass, TranscodeSection, UnsafeSection};

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn disabled_transcode() -> TranscodeSection {
    TranscodeSection { enabled: false, ..TranscodeSection::default() }
}

fn config_with_url(url: &str, allow_local_http: bool) -> Config {
    Config {
        public_url: url.to_string(),
        transcode: disabled_transcode(),
        r#unsafe: UnsafeSection { allow_local_http_for_testing: allow_local_http },
        ..Config::default()
    }
}

// ---------------------------------------------------------------------------
// B1: FFmpeg policy.
// ---------------------------------------------------------------------------

#[test]
fn default_rust_config_disables_transcoding() {
    let cfg = Config::default();
    assert!(!cfg.transcode.enabled, "new installs must start without FFmpeg");
    assert!(cfg.validate().is_ok());
}

#[test]
fn disabled_transcode_needs_no_toolchain() {
    // Disabled + bogus binary: no probe runs, ranges still hold.
    let cfg = Config {
        transcode: TranscodeSection {
            enabled: false,
            ffmpeg_path: "/nonexistent/pinetree-ffmpeg-xyz".to_string(),
            ..TranscodeSection::default()
        },
        ..Config::default()
    };
    assert!(cfg.validate().is_ok());
    assert!(cfg.transcode.to_runtime().validate().is_ok());
}

#[test]
fn enabled_transcode_with_broken_toolchain_fails_the_probe() {
    // Enabled + missing binary: the runtime probe (what the main startup
    // gate runs) fails with an actionable message naming the binary — the
    // failure the gate turns into a non-zero exit before serve mode.
    let section = TranscodeSection {
        enabled: true,
        ffmpeg_path: "/nonexistent/pinetree-ffmpeg-xyz".to_string(),
        ..TranscodeSection::default()
    };
    assert!(section.enabled);
    let err = section
        .to_runtime()
        .validate()
        .expect_err("missing ffmpeg must fail the probe");
    assert!(err.contains("pinetree-ffmpeg-xyz"), "{err}");
    assert!(err.contains("could not be executed"), "{err}");
    // Static `Config::validate()` stays hermetic (ranges only); the
    // environment probe belongs to startup, never to config parsing.
    let cfg = Config { transcode: section, ..Config::default() };
    assert!(cfg.validate().is_ok());
}

#[test]
fn empty_ffmpeg_path_is_rejected() {
    let section = TranscodeSection {
        enabled: true,
        ffmpeg_path: String::new(),
        ..TranscodeSection::default()
    };
    assert!(section.to_runtime().validate().is_err());
}

// ---------------------------------------------------------------------------
// B1/B4: shipped Docker configs.
// ---------------------------------------------------------------------------

#[test]
fn docker_default_config_disables_transcode_and_unsafe_http() {
    let path = manifest_dir().join("docker/pinetree.container.json");
    let bytes = std::fs::read(&path).expect("read docker default config");
    let cfg: Config = serde_json::from_slice(&bytes).expect("parse docker default config");
    assert!(!cfg.transcode.enabled, "default image must not require FFmpeg");
    assert!(!cfg.r#unsafe.allow_local_http_for_testing);
    // The placeholder canonical URL refuses to start until the operator sets
    // a real one (fail closed, with an actionable message).
    let err = cfg.validate().expect_err("placeholder public_url must fail closed");
    assert!(err.contains("placeholder"), "{err}");
    // With a canonical https URL the same file validates.
    let mut canonical = cfg.clone();
    canonical.public_url = "https://social.example.com".to_string();
    canonical.data_dir = PathBuf::from("/tmp/pinetree-b2-default");
    assert!(canonical.validate().is_ok());
}

#[test]
fn transcode_enabled_example_enables_explicitly() {
    let path = manifest_dir().join("docker/pinetree.transcode-enabled.example.json");
    let bytes = std::fs::read(&path).expect("read enabled example config");
    let cfg: Config = serde_json::from_slice(&bytes).expect("parse enabled example config");
    assert!(cfg.transcode.enabled, "example must opt in explicitly");
    assert!(!cfg.r#unsafe.allow_local_http_for_testing);
    // The example still forces a canonical URL (never ships a usable default).
    assert!(cfg.validate().is_err());
    let mut canonical = cfg.clone();
    canonical.public_url = "https://social.example.com".to_string();
    canonical.data_dir = PathBuf::from("/tmp/pinetree-b2-enabled");
    // Static ranges hold without probing any binary (probe is startup-only).
    assert!(canonical.validate().is_ok());
}

// ---------------------------------------------------------------------------
// B3: unsafe local-HTTP opt-in.
// ---------------------------------------------------------------------------

#[test]
fn https_accepted_and_http_rejected_by_default() {
    assert!(config_with_url("https://social.example.com/", false).validate().is_ok());
    for url in ["http://localhost:3000/", "http://127.0.0.1:3000/", "http://example.com/"] {
        let err = config_with_url(url, false).validate().expect_err("http off by default");
        assert!(err.contains("unsafe.allow_local_http_for_testing"), "{err}");
    }
}

#[test]
fn enabled_flag_accepts_only_local_test_hosts() {
    // Smoke rigs (127.0.0.1, 127.0.1.x loopback) + localhost + RFC 1918 +
    // reserved .test bench names.
    for url in [
        "http://localhost:3000/",
        "http://app.localhost/",
        "http://127.0.0.1:3000/",
        "http://127.0.1.20/",
        "http://[::1]:3000/",
        "http://10.0.0.5/",
        "http://192.168.1.10:3000/",
        "http://172.16.0.2/",
        "http://pt-a.test/",
    ] {
        assert!(config_with_url(url, true).validate().is_ok(), "{url}");
    }
    // Arbitrary public HTTP is never covered by the flag.
    for url in [
        "http://example.com/",
        "http://mastodon.social/",
        "http://8.8.8.8/",
        "http://169.254.169.254/",
        "http://[fd00::1]/",
    ] {
        let err = config_with_url(url, true).validate().expect_err("public http rejected");
        assert!(err.contains("non-local host"), "{err}");
    }
}

#[test]
fn unsafe_json_key_round_trips_and_rejects_typos() {
    let cfg: Config = serde_json::from_str("{}").expect("empty object parses");
    assert!(!cfg.r#unsafe.allow_local_http_for_testing);
    // The JSON key is exactly "unsafe" (raw-identifier field).
    let json = serde_json::to_value(Config::default()).expect("serialize");
    assert!(json.get("unsafe").is_some(), "{json}");
    assert_eq!(json["unsafe"]["allow_local_http_for_testing"], false);
    let on: Config =
        serde_json::from_str(r#"{"unsafe": {"allow_local_http_for_testing": true}}"#)
            .expect("parse");
    assert!(on.r#unsafe.allow_local_http_for_testing);
    assert!(serde_json::from_str::<Config>(r#"{"unsafe": {"allow_local_http": true}}"#).is_err());
    assert!(serde_json::from_str::<Config>(r#"{"unsaf": {}}"#).is_err());
}

#[test]
fn host_class_labels_are_stable_and_secret_free() {
    assert_eq!(PublicHostClass::Loopback.as_str(), "loopback");
    assert_eq!(PublicHostClass::LocalhostName.as_str(), "localhost");
    assert_eq!(PublicHostClass::PrivateLan.as_str(), "private-lan");
    assert_eq!(PublicHostClass::TestDomain.as_str(), "test-domain");
    assert_eq!(PublicHostClass::Public.as_str(), "public");
    assert!(!format!("{:?}", PublicHostClass::Loopback).contains("127"));
}

// ---------------------------------------------------------------------------
// B4: two-target Dockerfile shape (static guard; real builds need a host).
// ---------------------------------------------------------------------------

#[test]
fn dockerfile_has_shared_base_and_two_final_targets() {
    let path = manifest_dir().join("Dockerfile");
    let body = std::fs::read_to_string(&path).expect("read Dockerfile");
    assert!(body.contains("AS runtime-base"), "shared base stage");
    assert!(body.contains("AS runtime-ffmpeg"), "ffmpeg target");
    assert!(body.contains("AS runtime"), "minimal target");
    // The ffmpeg package must appear only in the ffmpeg target's install
    // list: every bare `ffmpeg` list line comes after the ffmpeg stage.
    let stage_at = body.find("AS runtime-ffmpeg").expect("ffmpeg stage");
    let (before, after) = body.split_at(stage_at);
    for line in before.lines() {
        let item = line.trim().trim_end_matches('\\').trim();
        assert_ne!(item, "ffmpeg", "minimal stages must not install FFmpeg");
    }
    assert!(
        after.lines().any(|line| line.trim().trim_end_matches('\\').trim() == "ffmpeg"),
        "ffmpeg target must install the FFmpeg package"
    );
    // Both final targets drop privileges, keep tini, and probe liveness.
    assert!(body.matches("USER pinetree").count() >= 2, "non-root in both targets");
    assert!(body.matches("tini").count() >= 2, "tini in both targets");
    assert!(body.contains("/healthz"), "liveness probe kept (readiness upgrade is Batch G/H)");
}
