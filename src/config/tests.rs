use super::*;

#[test]
fn default_config_is_valid() {
    assert!(Config::default().validate().is_ok());
}

#[test]
fn rejects_unknown_keys() {
    // Typo safety: `deny_unknown_fields` fails the whole file.
    let raw = r#"{"public_url": "https://example.local", "pubic_url": "x"}"#;
    assert!(serde_json::from_str::<Config>(raw).is_err());
}

#[test]
fn missing_keys_take_defaults() {
    let cfg: Config = serde_json::from_str("{}").expect("empty object parses");
    assert_eq!(cfg.public_url, Config::default().public_url);
    assert_eq!(cfg.listen, Config::default().listen);
    assert_eq!(
        cfg.note_draft_limit,
        crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT
    );
    assert!(cfg.trusted_proxies.is_empty());
    // Email transport dormant by default (empty host/from, port 25,
    // signup gate off); transcode OFF with the runtime defaults (Batch B
    // policy: `{}`-JSON must not opt into transcoding — the section
    // default carries `enabled: false`, flipped Batch H);
    // federation signatures permissive; diagnostics detail off with
    // structured lines on; backup dir unset with the 64 GiB cap.
    assert!(cfg.smtp_host.is_empty());
    assert_eq!(cfg.smtp_port, 25);
    assert!(cfg.smtp_username.is_empty());
    assert!(cfg.smtp_password.is_empty());
    assert!(cfg.smtp_from.is_empty());
    assert!(!cfg.email_required_for_signup);
    assert!(!cfg.smtp_config().is_configured());
    assert!(!cfg.transcode.enabled);
    assert_eq!(cfg.transcode.ffmpeg_path, "ffmpeg");
    assert_eq!(cfg.transcode.webp_quality, 80);
    assert_eq!(cfg.transcode.vp9_crf, 30);
    assert_eq!(cfg.transcode.vp9_cpu_used, 2);
    assert_eq!(cfg.transcode.opus_bitrate_kbps, 96);
    assert_eq!(
        cfg.transcode.wall_time_secs,
        crate::media::transcode::DEFAULT_WALL_TIME_SECS
    );
    assert_eq!(
        cfg.transcode.max_output_bytes,
        crate::media::transcode::DEFAULT_MAX_OUTPUT_BYTES
    );
    assert_eq!(
        cfg.transcode.max_memory_bytes,
        crate::media::transcode::DEFAULT_MAX_MEMORY_BYTES
    );
    assert_eq!(
        cfg.transcode.max_file_size,
        crate::media::transcode::DEFAULT_MAX_FILE_SIZE
    );
    assert_eq!(cfg.federation.rfc9421_mode, Rfc9421Mode::Permissive);
    assert!(!cfg.diagnostics.detailed_admin_status);
    assert!(cfg.diagnostics.structured_logging);
    assert!(cfg.backup.dir.is_none());
    assert_eq!(cfg.backup.max_bundle_bytes, 64 << 30);
    // Storage overhaul defaults: 1 GiB durable quota, 512 MiB global
    // cache (128/256/128 reserved), on-demand, 30/7/3-day TTLs.
    assert_eq!(cfg.drive_capacity_per_user_bytes, 1 << 30);
    assert_eq!(cfg.remote_cache_max_bytes, 512 << 20);
    assert_eq!(cfg.remote_cache_avatar_bytes, 128 << 20);
    assert_eq!(cfg.remote_cache_preview_bytes, 256 << 20);
    assert_eq!(cfg.remote_cache_full_bytes, 128 << 20);
    assert_eq!(cfg.remote_cache_mode, RemoteCacheMode::OnDemand);
    assert_eq!(cfg.remote_cache_avatar_ttl_days, 30);
    assert_eq!(cfg.remote_cache_preview_ttl_days, 7);
    assert_eq!(cfg.remote_cache_full_ttl_days, 3);
    assert!(!cfg.remote_cache_warming_enabled);
}

#[test]
fn smtp_keys_round_trip() {
    let dir =
        std::env::temp_dir().join(format!("pinetree-config-smtp-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("pinetree.json");
    let body = r#"{
            "smtp_host": "relay.local",
            "smtp_port": 2525,
            "smtp_username": "pinetree",
            "smtp_password": "secret",
            "smtp_from": "noreply@example.com",
            "email_required_for_signup": true,
            "transcode": {"enabled": false}
        }"#;
    std::fs::write(&path, body).unwrap();
    let cfg = Config::load_from_file(&path).expect("load");
    assert!(cfg.validate().is_ok());
    assert_eq!(cfg.smtp_host, "relay.local");
    assert_eq!(cfg.smtp_port, 2525);
    assert_eq!(cfg.smtp_config().smtp_from, "noreply@example.com");
    assert!(cfg.smtp_config().is_configured());
    assert!(cfg.email_required_for_signup);
    // Serde round-trip keeps the keys (typo safety works both ways).
    let json = serde_json::to_value(&cfg).expect("serialize");
    assert_eq!(json["smtp_host"], "relay.local");
    assert_eq!(json["smtp_port"], 2525);
    assert_eq!(json["email_required_for_signup"], true);
    let back: Config = serde_json::from_value(json).expect("deserialize");
    assert_eq!(back.smtp_config(), cfg.smtp_config());
    std::fs::remove_file(&path).ok();
}

#[test]
fn smtp_keys_reject_bad_values() {
    // Disabled transcode keeps these assertions hermetic (no ffmpeg
    // probe); the toolchain gate has its own test below.
    let dormant = || TranscodeSection {
        enabled: false,
        ..Default::default()
    };
    // Zero port is always rejected (the default is 25, so zero means
    // an explicit bad value).
    let zero_port = Config {
        smtp_port: 0,
        transcode: dormant(),
        ..Config::default()
    };
    assert!(zero_port.validate().is_err());
    // Half-filled transport (host without from) is rejected.
    let half = Config {
        smtp_host: "relay.local".to_string(),
        transcode: dormant(),
        ..Config::default()
    };
    assert!(half.validate().is_err());
    // Bad `from` shape is rejected via the sender's validator.
    let bad_from = Config {
        smtp_host: "relay.local".to_string(),
        smtp_from: "not-an-email".to_string(),
        transcode: dormant(),
        ..Config::default()
    };
    assert!(bad_from.validate().is_err());
    // Split auth (username without password) is rejected.
    let split_auth = Config {
        smtp_host: "relay.local".to_string(),
        smtp_from: "noreply@example.com".to_string(),
        smtp_username: "pinetree".to_string(),
        transcode: dormant(),
        ..Config::default()
    };
    assert!(split_auth.validate().is_err());
    // Unknown SMTP keys stay hard errors.
    assert!(serde_json::from_str::<Config>(r#"{"smtp_hosts": "x"}"#).is_err());
    // A fully-configured transport validates.
    let full = Config {
        smtp_host: "relay.local".to_string(),
        smtp_from: "noreply@example.com".to_string(),
        transcode: dormant(),
        ..Config::default()
    };
    assert!(full.validate().is_ok());
}

#[test]
fn transcode_section_defaults_match_runtime() {
    // The section default converts field-for-field to the runtime
    // default for every quality/cap knob (single source of truth).
    // The master switch differs by Batch B policy: the section default
    // is disabled (fresh installs start without FFmpeg) while the
    // runtime `TranscodeConfig::default()` keeps `enabled: true`.
    let runtime = TranscodeSection::default().to_runtime();
    let expected = crate::media::transcode::TranscodeConfig::default();
    assert!(!TranscodeSection::default().enabled);
    assert!(!runtime.enabled);
    assert_eq!(runtime.ffmpeg_path, expected.ffmpeg_path);
    assert_eq!(runtime.webp_quality, expected.webp_quality);
    assert_eq!(runtime.vp9_crf, expected.vp9_crf);
    assert_eq!(runtime.vp9_cpu_used, expected.vp9_cpu_used);
    assert_eq!(runtime.opus_bitrate_kbps, expected.opus_bitrate_kbps);
    assert_eq!(runtime.wall_time_secs, expected.wall_time_secs);
    assert_eq!(runtime.max_output_bytes, expected.max_output_bytes);
    assert_eq!(runtime.max_memory_bytes, expected.max_memory_bytes);
    assert_eq!(runtime.max_file_size, expected.max_file_size);
}

#[test]
fn transcode_keys_round_trip() {
    // Disabled so no test machine needs an ffmpeg binary for this.
    let raw = r#"{
            "transcode": {
                "enabled": false,
                "ffmpeg_path": "/opt/bin/ffmpeg",
                "webp_quality": 90,
                "vp9_crf": 32,
                "vp9_cpu_used": 3,
                "opus_bitrate_kbps": 128,
                "wall_time_secs": 60,
                "max_output_bytes": 33554432,
                "max_memory_bytes": 536870912,
                "max_file_size": 16777216
            }
        }"#;
    let cfg: Config = serde_json::from_str(raw).expect("parse");
    assert!(cfg.validate().is_ok());
    assert!(!cfg.transcode.enabled);
    assert_eq!(cfg.transcode.ffmpeg_path, "/opt/bin/ffmpeg");
    assert_eq!(cfg.transcode.webp_quality, 90);
    assert_eq!(cfg.transcode.opus_bitrate_kbps, 128);
    let runtime = cfg.transcode.to_runtime();
    assert!(!runtime.enabled);
    assert_eq!(runtime.vp9_crf, 32);
    // Serde round-trip keeps the nested keys.
    let json = serde_json::to_value(&cfg).expect("serialize");
    assert_eq!(json["transcode"]["webp_quality"], 90);
    assert_eq!(json["transcode"]["enabled"], false);
    let back: Config = serde_json::from_value(json).expect("deserialize");
    assert_eq!(back.transcode, cfg.transcode);
    // Unknown nested keys stay hard errors.
    assert!(serde_json::from_str::<Config>(r#"{"transcode": {"webp_qualit": 90}}"#).is_err());
}

#[test]
fn transcode_keys_reject_bad_values() {
    // Every range knob rejects zero/out-of-range without probing any
    // binary (disabled, so validation stays hermetic).
    for section in [
        TranscodeSection {
            webp_quality: 101,
            enabled: false,
            ..Default::default()
        },
        TranscodeSection {
            vp9_crf: 64,
            enabled: false,
            ..Default::default()
        },
        TranscodeSection {
            vp9_cpu_used: 6,
            enabled: false,
            ..Default::default()
        },
        TranscodeSection {
            opus_bitrate_kbps: 0,
            enabled: false,
            ..Default::default()
        },
        TranscodeSection {
            wall_time_secs: 0,
            enabled: false,
            ..Default::default()
        },
        TranscodeSection {
            max_output_bytes: 0,
            enabled: false,
            ..Default::default()
        },
        TranscodeSection {
            max_memory_bytes: 0,
            enabled: false,
            ..Default::default()
        },
        TranscodeSection {
            max_file_size: 0,
            enabled: false,
            ..Default::default()
        },
    ] {
        let cfg = Config {
            transcode: section,
            ..Config::default()
        };
        assert!(cfg.validate().is_err());
    }
    // Enabled with an unexecutable binary fails the STARTUP probe
    // (`TranscodeConfig::validate`), not `Config::validate()` (which
    // stays hermetic so defaults validate anywhere). No binary runs
    // for a missing path — the spawn error is the gate. The section
    // default is disabled (Batch B/H), so this case opts in
    // explicitly.
    let missing_toolchain = Config {
        transcode: TranscodeSection {
            enabled: true,
            ffmpeg_path: "/nonexistent/pinetree-ffmpeg-xyz".to_string(),
            ..Default::default()
        },
        ..Config::default()
    };
    assert!(missing_toolchain.validate().is_ok());
    assert!(missing_toolchain.transcode.to_runtime().validate().is_err());
    // Disabled with an unexecutable path validates (no probe).
    let disabled_toolchain = Config {
        transcode: TranscodeSection {
            enabled: false,
            ffmpeg_path: "/nonexistent/pinetree-ffmpeg-xyz".to_string(),
            ..Default::default()
        },
        ..Config::default()
    };
    assert!(disabled_toolchain.validate().is_ok());
}

#[test]
fn federation_keys_round_trip() {
    let cfg: Config = serde_json::from_str("{}").expect("empty object parses");
    assert_eq!(cfg.federation.rfc9421_mode, Rfc9421Mode::Permissive);
    let enforced: Config = serde_json::from_str(
        r#"{"federation": {"rfc9421_mode": "enforce"}, "transcode": {"enabled": false}}"#,
    )
    .expect("parse");
    assert_eq!(enforced.federation.rfc9421_mode, Rfc9421Mode::Enforce);
    assert!(enforced.validate().is_ok());
    // Serde round-trip keeps the nested key.
    let json = serde_json::to_value(&enforced).expect("serialize");
    assert_eq!(json["federation"]["rfc9421_mode"], "enforce");
    let back: Config = serde_json::from_value(json).expect("deserialize");
    assert_eq!(back.federation, enforced.federation);
    // Unknown modes and unknown nested keys stay hard errors.
    assert!(
        serde_json::from_str::<Config>(r#"{"federation": {"rfc9421_mode": "strict"}}"#).is_err()
    );
    assert!(
        serde_json::from_str::<Config>(r#"{"federation": {"rfc9421_modes": "enforce"}}"#).is_err()
    );
}

#[test]
fn file_round_trip() {
    let dir = std::env::temp_dir().join(format!("pinetree-config-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("pinetree.json");
    let body = r#"{
            "public_url": "https://social.example.com/",
            "listen": "0.0.0.0:3000",
            "max_file_size": 1048576,
            "trusted_proxies": ["10.0.0.1"],
            "note_draft_limit": 20,
            "scheduled_note_limit": 2,
            "pin_limit": 8,
            "preserved_usernames": ["admin"],
            "prohibited_words": ["spam"],
            "sensitive_words": ["nsfw"],
            "default_like": "👍"
        }"#;
    std::fs::write(&path, body).unwrap();
    let cfg = Config::load_from_file(&path).expect("load");
    assert_eq!(cfg.public_url, "https://social.example.com/");
    assert_eq!(cfg.normalized_public_url(), "https://social.example.com");
    assert_eq!(cfg.listen.to_string(), "0.0.0.0:3000");
    assert_eq!(cfg.max_file_size, 1048576);
    assert_eq!(
        cfg.trusted_proxies,
        vec!["10.0.0.1".parse::<IpAddr>().unwrap()]
    );
    assert_eq!(cfg.note_draft_limit, 20);
    assert_eq!(cfg.scheduled_note_limit, 2);
    assert_eq!(cfg.pin_limit, 8);
    assert_eq!(cfg.preserved_usernames, vec!["admin".to_string()]);
    assert_eq!(cfg.default_like.as_deref(), Some("👍"));
    std::fs::remove_file(&path).ok();
}

#[test]
fn default_like_bounds() {
    assert!(
        Config {
            default_like: None,
            ..Config::default()
        }
        .validate()
        .is_ok()
    );
    assert!(
        Config {
            default_like: Some("👍".to_string()),
            ..Config::default()
        }
        .validate()
        .is_ok()
    );
    assert!(
        Config {
            default_like: Some(":blobcat:".to_string()),
            ..Config::default()
        }
        .validate()
        .is_ok()
    );
    assert!(
        Config {
            default_like: Some(String::new()),
            ..Config::default()
        }
        .validate()
        .is_err()
    );
    assert!(
        Config {
            default_like: Some("r".repeat(261)),
            ..Config::default()
        }
        .validate()
        .is_err()
    );
}

#[test]
fn storage_keys_round_trip() {
    let dir = std::env::temp_dir().join(format!(
        "pinetree-config-storage-test-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("pinetree.json");
    let body = r#"{
            "drive_capacity_per_user_bytes": 1073741824,
            "remote_cache_max_bytes": 536870912,
            "remote_cache_avatar_bytes": 134217728,
            "remote_cache_preview_bytes": 268435456,
            "remote_cache_full_bytes": 134217728,
            "remote_cache_mode": "warm",
            "remote_cache_avatar_ttl_days": 30,
            "remote_cache_preview_ttl_days": 7,
            "remote_cache_full_ttl_days": 3,
            "remote_cache_warming_enabled": true
        }"#;
    std::fs::write(&path, body).unwrap();
    let cfg = Config::load_from_file(&path).expect("load");
    assert!(cfg.validate().is_ok());
    assert_eq!(cfg.drive_capacity_per_user_bytes, 1 << 30);
    assert_eq!(cfg.remote_cache_mode, RemoteCacheMode::Warm);
    assert!(cfg.remote_cache_warming_enabled);
    // Serde round-trip keeps the keys (typo safety works both ways).
    let json = serde_json::to_value(&cfg).expect("serialize");
    assert_eq!(json["remote_cache_mode"], "warm");
    assert_eq!(json["remote_cache_max_bytes"], 536870912);
    let back: Config = serde_json::from_value(json).expect("deserialize");
    assert_eq!(back.remote_cache_mode, RemoteCacheMode::Warm);
    std::fs::remove_file(&path).ok();
}

#[test]
fn storage_keys_reject_bad_values() {
    for cfg in [
        Config {
            drive_capacity_per_user_bytes: 0,
            ..Config::default()
        },
        Config {
            remote_cache_max_bytes: 0,
            ..Config::default()
        },
        Config {
            remote_cache_avatar_bytes: 0,
            ..Config::default()
        },
        Config {
            remote_cache_preview_bytes: 0,
            ..Config::default()
        },
        Config {
            remote_cache_full_bytes: 0,
            ..Config::default()
        },
        // Reserved class budgets live inside the global cap.
        Config {
            remote_cache_avatar_bytes: (512 << 20) + 1,
            ..Config::default()
        },
        Config {
            remote_cache_avatar_ttl_days: 0,
            ..Config::default()
        },
        Config {
            remote_cache_preview_ttl_days: 0,
            ..Config::default()
        },
        Config {
            remote_cache_full_ttl_days: 0,
            ..Config::default()
        },
    ] {
        assert!(cfg.validate().is_err(), "{cfg:?}");
    }
    // Unknown storage keys stay hard errors.
    assert!(serde_json::from_str::<Config>(r#"{"remote_cache_max_byte": 1}"#).is_err());
    // Mode values are snake_case.
    let cfg: Config = serde_json::from_str(r#"{"remote_cache_mode": "on_demand"}"#).unwrap();
    assert_eq!(cfg.remote_cache_mode, RemoteCacheMode::OnDemand);
    assert!(serde_json::from_str::<Config>(r#"{"remote_cache_mode": "eager"}"#).is_err());
}

#[test]
fn storage_pool_dirs_default_distinct_and_sum_guard() {
    let cfg = Config::default();
    assert_eq!(cfg.durable_pool_dir(), cfg.data_dir.join("blobs_durable"));
    assert_eq!(cfg.cache_pool_dir(), cfg.data_dir.join("blobs_cache"));
    assert!(cfg.validate().is_ok());
    // Sum of reserved class budgets over the global cap is rejected,
    // even when each class fits individually.
    let over = Config {
        remote_cache_max_bytes: 512 << 20,
        remote_cache_avatar_bytes: 256 << 20,
        remote_cache_preview_bytes: 256 << 20,
        remote_cache_full_bytes: 256 << 20,
        ..Config::default()
    };
    assert!(over.validate().is_err());
    // Explicit pool dirs must resolve distinct.
    let same = Config {
        durable_dir: Some(PathBuf::from("/tmp/pool")),
        cache_dir: Some(PathBuf::from("/tmp/pool")),
        ..Config::default()
    };
    assert!(same.validate().is_err());
    let split = Config {
        durable_dir: Some(PathBuf::from("/tmp/durable")),
        cache_dir: Some(PathBuf::from("/tmp/cache")),
        ..Config::default()
    };
    assert!(split.validate().is_ok());
    assert_eq!(split.durable_pool_dir(), PathBuf::from("/tmp/durable"));
    assert_eq!(split.cache_pool_dir(), PathBuf::from("/tmp/cache"));
}

#[test]
fn load_missing_file_names_path() {
    let err = Config::load_from_file(std::path::Path::new("/nonexistent/pinetree.json"))
        .expect_err("missing file errors");
    assert!(err.contains("/nonexistent/pinetree.json"), "{err}");
}

#[test]
fn batch_b_default_disables_transcode_and_unsafe_http() {
    let cfg = Config::default();
    assert!(
        !cfg.transcode.enabled,
        "new installs must start without FFmpeg"
    );
    assert!(!cfg.r#unsafe.allow_local_http_for_testing);
    assert!(cfg.validate().is_ok());
    // Serialized defaults carry both policies.
    let json = serde_json::to_value(&cfg).expect("serialize");
    assert_eq!(json["transcode"]["enabled"], false);
    assert_eq!(json["unsafe"]["allow_local_http_for_testing"], false);
    let back: Config = serde_json::from_value(json).expect("deserialize");
    assert!(!back.transcode.enabled);
    assert!(!back.r#unsafe.allow_local_http_for_testing);
}

#[test]
fn batch_b_unsafe_section_round_trip_and_typo_safety() {
    let cfg: Config = serde_json::from_str("{}").expect("empty object parses");
    assert!(!cfg.r#unsafe.allow_local_http_for_testing);
    let on: Config = serde_json::from_str(r#"{"unsafe": {"allow_local_http_for_testing": true}}"#)
        .expect("parse");
    assert!(on.r#unsafe.allow_local_http_for_testing);
    let json = serde_json::to_value(&on).expect("serialize");
    assert_eq!(json["unsafe"]["allow_local_http_for_testing"], true);
    let back: Config = serde_json::from_value(json).expect("deserialize");
    assert!(back.r#unsafe.allow_local_http_for_testing);
    assert!(serde_json::from_str::<Config>(r#"{"unsafe": {"allow_local_http": true}}"#).is_err());
    assert!(serde_json::from_str::<Config>(r#"{"unsaf": {}}"#).is_err());
}

#[test]
fn batch_b_http_public_url_gating() {
    fn with_url(url: &str, allow: bool) -> Config {
        Config {
            public_url: url.to_string(),
            r#unsafe: UnsafeSection {
                allow_local_http_for_testing: allow,
            },
            transcode: TranscodeSection {
                enabled: false,
                ..TranscodeSection::default()
            },
            ..Config::default()
        }
    }
    // Default HTTPS accepted with the flag off.
    assert!(
        with_url("https://social.example.com/", false)
            .validate()
            .is_ok()
    );
    // Default HTTP rejected with the flag off (message names the flag).
    for url in [
        "http://localhost:3000/",
        "http://127.0.0.1:3000/",
        "http://example.com/",
    ] {
        let err = with_url(url, false)
            .validate()
            .expect_err("http rejected by default");
        assert!(err.contains("unsafe.allow_local_http_for_testing"), "{err}");
    }
    // Flag on: local/test hosts accepted...
    for url in [
        "http://localhost:3000/",
        "http://LOCALHOST/",
        "http://app.localhost/",
        "http://127.0.0.1:3000/",
        "http://127.0.1.20/",
        "http://[::1]:3000/",
        "http://10.0.0.5/",
        "http://192.168.1.10:3000/",
        "http://172.16.0.2/",
        "http://pt-a.test/",
    ] {
        assert!(with_url(url, true).validate().is_ok(), "{url}");
    }
    // ...but public HTTP stays rejected, with the host class reported.
    for url in [
        "http://example.com/",
        "http://mastodon.social/",
        "http://8.8.8.8/",
        "http://169.254.169.254/",
        "http://[fd00::1]/",
    ] {
        let err = with_url(url, true)
            .validate()
            .expect_err("public http rejected");
        assert!(err.contains("non-local host"), "{err}");
    }
}

#[test]
fn batch_b_placeholder_public_url_refuses_to_start() {
    let cfg = Config {
        public_url: "https://REPLACE-ME.invalid".to_string(),
        ..Config::default()
    };
    let err = cfg.validate().expect_err("placeholder must fail closed");
    assert!(err.contains("placeholder"), "{err}");
    assert!(err.contains("public_url"), "{err}");
}

#[test]
fn batch_b_public_host_class_cases() {
    fn class(url: &str) -> PublicHostClass {
        Config {
            public_url: url.to_string(),
            ..Config::default()
        }
        .public_host_class()
    }
    assert_eq!(class("https://example.com/"), PublicHostClass::Public);
    assert_eq!(class("http://127.0.0.1:3000/"), PublicHostClass::Loopback);
    assert_eq!(class("http://127.0.1.20/"), PublicHostClass::Loopback);
    assert_eq!(class("http://[::1]:3000/"), PublicHostClass::Loopback);
    assert_eq!(
        class("http://localhost:3000/"),
        PublicHostClass::LocalhostName
    );
    assert_eq!(
        class("http://APP.LOCALHOST/"),
        PublicHostClass::LocalhostName
    );
    assert_eq!(class("http://10.1.2.3/"), PublicHostClass::PrivateLan);
    assert_eq!(
        class("http://192.168.0.1:8080/x"),
        PublicHostClass::PrivateLan
    );
    assert_eq!(class("http://172.31.255.255/"), PublicHostClass::PrivateLan);
    assert_eq!(class("http://172.15.0.1/"), PublicHostClass::Public);
    assert_eq!(class("http://pt-a.test/"), PublicHostClass::TestDomain);
    assert_eq!(class("http://8.8.8.8/"), PublicHostClass::Public);
    assert_eq!(class("http://example.com/"), PublicHostClass::Public);
    assert!(PublicHostClass::Loopback.is_local_test());
    assert!(PublicHostClass::TestDomain.is_local_test());
    assert!(!PublicHostClass::Public.is_local_test());
    assert_eq!(PublicHostClass::PrivateLan.as_str(), "private-lan");
}

#[test]
fn rejects_bad_values() {
    for cfg in [
        Config {
            public_url: "ftp://example.local".into(),
            ..Config::default()
        },
        Config {
            public_url: "https://".into(),
            ..Config::default()
        },
        Config {
            max_file_size: 0,
            ..Config::default()
        },
        Config {
            note_draft_limit: 0,
            ..Config::default()
        },
        Config {
            scheduled_note_limit: 0,
            ..Config::default()
        },
        Config {
            pin_limit: 0,
            ..Config::default()
        },
        Config {
            mode: DeploymentMode::Cluster,
            ..Config::default()
        },
    ] {
        assert!(cfg.validate().is_err(), "{cfg:?}");
    }
    let empty_dir = Config {
        data_dir: PathBuf::from(""),
        ..Config::default()
    };
    assert!(empty_dir.validate().is_err());
}

#[test]
fn batch_h_diagnostics_section_round_trip_and_typo_safety() {
    // Defaults: detail off, structured lines on.
    let cfg: Config = serde_json::from_str("{}").expect("empty object parses");
    assert!(!cfg.diagnostics.detailed_admin_status);
    assert!(cfg.diagnostics.structured_logging);
    assert!(cfg.validate().is_ok());
    // Explicit opt-in round-trips through file + serde value.
    let on: Config = serde_json::from_str(
        r#"{"diagnostics": {"detailed_admin_status": true, "structured_logging": false}}"#,
    )
    .expect("parse");
    assert!(on.diagnostics.detailed_admin_status);
    assert!(!on.diagnostics.structured_logging);
    assert!(on.validate().is_ok());
    let json = serde_json::to_value(&on).expect("serialize");
    assert_eq!(json["diagnostics"]["detailed_admin_status"], true);
    assert_eq!(json["diagnostics"]["structured_logging"], false);
    let back: Config = serde_json::from_value(json).expect("deserialize");
    assert_eq!(back.diagnostics, on.diagnostics);
    // Typo safety at both levels.
    assert!(
        serde_json::from_str::<Config>(r#"{"diagnostics": {"detailed_admin": true}}"#).is_err()
    );
    assert!(serde_json::from_str::<Config>(r#"{"diagnostic": {}}"#).is_err());
}

#[test]
fn batch_h_backup_section_round_trip_and_guards() {
    // Defaults: dir unset (resolves under `data_dir`), 64 GiB cap.
    let cfg: Config = serde_json::from_str("{}").expect("empty object parses");
    assert!(cfg.backup.dir.is_none());
    assert_eq!(cfg.backup.max_bundle_bytes, 64 << 30);
    assert_eq!(cfg.backup_dir(), cfg.data_dir.join("backups"));
    assert!(cfg.validate().is_ok());
    // Explicit dir round-trips (null stays unset).
    let explicit: Config = serde_json::from_str(
        r#"{"backup": {"dir": "/var/lib/pinetree/backups", "max_bundle_bytes": 1073741824}}"#,
    )
    .expect("parse");
    assert_eq!(
        explicit.backup.dir,
        Some(PathBuf::from("/var/lib/pinetree/backups"))
    );
    assert_eq!(explicit.backup.max_bundle_bytes, 1 << 30);
    assert_eq!(
        explicit.backup_dir(),
        PathBuf::from("/var/lib/pinetree/backups")
    );
    assert!(explicit.validate().is_ok());
    let nulled: Config = serde_json::from_str(r#"{"backup": {"dir": null}}"#).expect("parse");
    assert!(nulled.backup.dir.is_none());
    // Guards: zero cap rejected, empty dir rejected.
    let zero_cap = Config {
        backup: BackupSection {
            max_bundle_bytes: 0,
            ..BackupSection::default()
        },
        ..Config::default()
    };
    assert!(zero_cap.validate().is_err());
    let empty_dir = Config {
        backup: BackupSection {
            dir: Some(PathBuf::from("")),
            ..BackupSection::default()
        },
        ..Config::default()
    };
    assert!(empty_dir.validate().is_err());
    // Typo safety at both levels.
    assert!(serde_json::from_str::<Config>(r#"{"backup": {"directory": "x"}}"#).is_err());
    assert!(serde_json::from_str::<Config>(r#"{"backups": {}}"#).is_err());
}

#[test]
fn batch_h_config_summary_carries_no_secrets() {
    let cfg = Config {
        smtp_username: "pinetree".to_string(),
        smtp_password: "hunter2".to_string(),
        smtp_host: "relay.local".to_string(),
        smtp_from: "noreply@example.com".to_string(),
        preserved_usernames: vec!["admin".to_string()],
        trusted_proxies: vec!["10.0.0.1".parse().expect("ip")],
        ..Config::default()
    };
    assert!(cfg.validate().is_ok());
    let summary = cfg.config_summary();
    let text = serde_json::to_string(&summary).expect("serialize");
    for secret in [
        "hunter2",
        "pinetree",
        "relay.local",
        "noreply@example.com",
        "10.0.0.1",
    ] {
        assert!(!text.contains(secret), "summary leaks {secret:?}: {text}");
    }
    // ...but the safe flags survive.
    assert_eq!(summary["email_configured"], true);
    assert_eq!(summary["transcode_enabled"], false);
    assert_eq!(summary["detailed_admin_status"], false);
    assert_eq!(summary["structured_logging"], true);
    assert_eq!(summary["trusted_proxies"], 1);
}

#[test]
fn batch_d6_rate_limit_section_round_trip_and_guards() {
    // Defaults mirror the documented response constants.
    let cfg: Config = serde_json::from_str("{}").expect("empty object parses");
    assert_eq!(cfg.rate_limit.max_buckets, 32_768, "default max_buckets");
    assert_eq!(
        cfg.rate_limit.max_auth_failures, 32_768,
        "default max_auth_failures"
    );
    assert_eq!(
        cfg.rate_limit.prune_interval_ms, 60_000,
        "default prune_interval_ms"
    );
    assert_eq!(
        cfg.rate_limit.max_buckets,
        crate::api::misskey::response::MAX_RATE_LIMIT_BUCKETS,
        "default tracks response constant"
    );
    assert_eq!(
        cfg.rate_limit.max_auth_failures,
        crate::api::misskey::response::MAX_AUTH_FAILURE_BUCKETS,
        "default tracks response constant"
    );
    assert_eq!(
        cfg.rate_limit.prune_interval_ms,
        crate::api::misskey::response::PRUNE_INTERVAL_MS,
        "default tracks response constant"
    );
    assert!(cfg.validate().is_ok(), "defaults validate");
    // Explicit values round-trip through file + serde value.
    let custom: Config = serde_json::from_str(
        r#"{"rate_limit": {"max_buckets": 1024, "max_auth_failures": 512, "prune_interval_ms": 5000}}"#,
    )
    .expect("parse custom rate_limit");
    assert_eq!(custom.rate_limit.max_buckets, 1024, "custom max_buckets");
    assert_eq!(
        custom.rate_limit.max_auth_failures, 512,
        "custom max_auth_failures"
    );
    assert_eq!(
        custom.rate_limit.prune_interval_ms, 5000,
        "custom prune_interval_ms"
    );
    assert!(custom.validate().is_ok(), "custom values validate");
    let json = serde_json::to_value(&custom).expect("serialize custom");
    assert_eq!(
        json["rate_limit"]["max_buckets"], 1024,
        "round-trip buckets"
    );
    assert_eq!(
        json["rate_limit"]["max_auth_failures"], 512,
        "round-trip failures"
    );
    assert_eq!(
        json["rate_limit"]["prune_interval_ms"], 5000,
        "round-trip prune"
    );
    let back: Config = serde_json::from_value(json).expect("deserialize round-trip");
    assert_eq!(back.rate_limit, custom.rate_limit, "round-trip equality");
    // Typo safety at both levels.
    assert!(
        serde_json::from_str::<Config>(r#"{"rate_limit": {"max_bucket": 1}}"#).is_err(),
        "nested typo rejected"
    );
    assert!(
        serde_json::from_str::<Config>(r#"{"rate_limits": {}}"#).is_err(),
        "section typo rejected"
    );
    // Guards: every bound must be positive (zero / negative rejected).
    for section in [
        RateLimitSection {
            max_buckets: 0,
            ..RateLimitSection::default()
        },
        RateLimitSection {
            max_auth_failures: 0,
            ..RateLimitSection::default()
        },
        RateLimitSection {
            prune_interval_ms: 0,
            ..RateLimitSection::default()
        },
        RateLimitSection {
            prune_interval_ms: -1,
            ..RateLimitSection::default()
        },
    ] {
        let cfg = Config {
            rate_limit: section,
            ..Config::default()
        };
        assert!(cfg.validate().is_err(), "rate_limit guard {section:?}");
    }
}

#[test]
fn batch_d6_uploads_section_round_trip_and_guards() {
    // Defaults mirror the documented response constants.
    let cfg: Config = serde_json::from_str("{}").expect("empty object parses");
    assert_eq!(cfg.uploads.per_ip, 2, "default per_ip");
    assert_eq!(cfg.uploads.global, 16, "default global");
    assert_eq!(cfg.uploads.max_ips, 4_096, "default max_ips");
    assert_eq!(
        cfg.uploads.per_ip,
        crate::api::misskey::response::MAX_CONCURRENT_UPLOADS_PER_IP,
        "default tracks response constant"
    );
    assert_eq!(
        cfg.uploads.global,
        crate::api::misskey::response::MAX_CONCURRENT_UPLOADS_GLOBAL,
        "default tracks response constant"
    );
    assert_eq!(
        cfg.uploads.max_ips,
        crate::api::misskey::response::MAX_UPLOAD_SLOT_IPS,
        "default tracks response constant"
    );
    assert!(cfg.validate().is_ok(), "defaults validate");
    // Explicit values round-trip through file + serde value.
    let custom: Config =
        serde_json::from_str(r#"{"uploads": {"per_ip": 4, "global": 32, "max_ips": 128}}"#)
            .expect("parse custom uploads");
    assert_eq!(custom.uploads.per_ip, 4, "custom per_ip");
    assert_eq!(custom.uploads.global, 32, "custom global");
    assert_eq!(custom.uploads.max_ips, 128, "custom max_ips");
    assert!(custom.validate().is_ok(), "custom values validate");
    let json = serde_json::to_value(&custom).expect("serialize custom");
    assert_eq!(json["uploads"]["per_ip"], 4, "round-trip per_ip");
    assert_eq!(json["uploads"]["global"], 32, "round-trip global");
    assert_eq!(json["uploads"]["max_ips"], 128, "round-trip max_ips");
    let back: Config = serde_json::from_value(json).expect("deserialize round-trip");
    assert_eq!(back.uploads, custom.uploads, "round-trip equality");
    // Typo safety at both levels.
    assert!(
        serde_json::from_str::<Config>(r#"{"uploads": {"perip": 1}}"#).is_err(),
        "nested typo rejected"
    );
    assert!(
        serde_json::from_str::<Config>(r#"{"upload": {}}"#).is_err(),
        "section typo rejected"
    );
    // Guards: every bound must be positive.
    for section in [
        UploadsSection {
            per_ip: 0,
            ..UploadsSection::default()
        },
        UploadsSection {
            global: 0,
            ..UploadsSection::default()
        },
        UploadsSection {
            max_ips: 0,
            ..UploadsSection::default()
        },
    ] {
        let cfg = Config {
            uploads: section,
            ..Config::default()
        };
        assert!(cfg.validate().is_err(), "uploads guard {section:?}");
    }
}

#[test]
fn batch_d6_defaults_preserve_batch_policies() {
    // D6 must not flip the Batch B/H/P policies pinned by earlier audits.
    let cfg: Config = serde_json::from_str("{}").expect("empty object parses");
    assert!(!cfg.transcode.enabled, "transcode stays disabled");
    assert!(
        !cfg.r#unsafe.allow_local_http_for_testing,
        "unsafe HTTP stays disabled"
    );
    assert!(
        !cfg.diagnostics.detailed_admin_status,
        "detailed status stays off"
    );
    assert!(
        cfg.diagnostics.structured_logging,
        "structured logging stays on"
    );
    assert_eq!(
        cfg.federation.rfc9421_mode,
        Rfc9421Mode::Permissive,
        "rfc9421 stays permissive"
    );
    assert!(cfg.validate().is_ok(), "defaults validate");
}
