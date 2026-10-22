//! Batch D6: JSON-configurable rate-limit + upload bounds.
//!
//! Proves the `rate_limit { max_buckets, max_auth_failures,
//! prune_interval_ms }` and `uploads { per_ip, global, max_ips }`
//! sections parse with sane defaults, reject typos/zeros, and plumb
//! through to `RateLimiter` / `UploadSlots` without changing defaults.

use std::sync::Arc;

use pinetree::api::misskey::response::RateLimitBounds;
use pinetree::api::misskey::response::RateLimiter;
use pinetree::api::misskey::response::UploadSlotBounds;
use pinetree::api::misskey::response::UploadSlots;
use pinetree::config::Config;
use pinetree::domain::id::ManualClock;

#[test]
fn defaults_match_documented_constants() {
    let cfg = Config::default();
    assert_eq!(
        cfg.rate_limit.max_buckets,
        pinetree::api::misskey::response::MAX_RATE_LIMIT_BUCKETS,
        "default max_buckets tracks constant"
    );
    assert_eq!(
        cfg.rate_limit.max_auth_failures,
        pinetree::api::misskey::response::MAX_AUTH_FAILURE_BUCKETS,
        "default max_auth_failures tracks constant"
    );
    assert_eq!(
        cfg.rate_limit.prune_interval_ms,
        pinetree::api::misskey::response::PRUNE_INTERVAL_MS,
        "default prune_interval_ms tracks constant"
    );
    assert_eq!(
        cfg.uploads.per_ip,
        pinetree::api::misskey::response::MAX_CONCURRENT_UPLOADS_PER_IP,
        "default per_ip tracks constant"
    );
    assert_eq!(
        cfg.uploads.global,
        pinetree::api::misskey::response::MAX_CONCURRENT_UPLOADS_GLOBAL,
        "default global tracks constant"
    );
    assert_eq!(
        cfg.uploads.max_ips,
        pinetree::api::misskey::response::MAX_UPLOAD_SLOT_IPS,
        "default max_ips tracks constant"
    );
    assert!(cfg.validate().is_ok(), "defaults validate");
    // Empty JSON keeps the same defaults.
    let empty: Config = serde_json::from_str("{}").expect("empty object parses");
    assert_eq!(
        empty.rate_limit, cfg.rate_limit,
        "empty keeps rate defaults"
    );
    assert_eq!(empty.uploads, cfg.uploads, "empty keeps upload defaults");
}

#[test]
fn custom_bounds_plumb_to_limiter() {
    let cfg: Config = serde_json::from_str(
        r#"{"rate_limit": {"max_buckets": 8, "max_auth_failures": 8, "prune_interval_ms": 1000}}"#,
    )
    .expect("parse custom rate_limit");
    assert!(cfg.validate().is_ok(), "custom rate_limit validates");
    let bounds = RateLimitBounds::new(
        cfg.rate_limit.max_buckets,
        cfg.rate_limit.max_auth_failures,
        cfg.rate_limit.prune_interval_ms,
    );
    let clock = Arc::new(ManualClock::new(0));
    let limiter = RateLimiter::new_with_bounds(clock, bounds).with_limit(
        "notes/create",
        10_000,
        60_000,
        None,
    );
    for index in 0..32 {
        let key = format!("churn-{index}");
        limiter
            .check(&key, "notes/create")
            .expect("fresh churn key passes");
    }
    assert!(
        limiter.bucket_count() <= 8,
        "custom max_buckets enforced: {} > 8",
        limiter.bucket_count()
    );
    assert_eq!(limiter.bounds(), bounds, "bounds round-trip");
}

#[test]
fn with_config_alias_preserves_limits() {
    let clock = Arc::new(ManualClock::new(0));
    let bounds = RateLimitBounds::new(16, 16, 1000);
    let limiter = RateLimiter::with_misskey_defaults(clock).with_config(bounds);
    assert_eq!(limiter.bounds(), bounds, "with_config installs bounds");
    assert!(
        limiter.check("alice", "notes/create").is_ok(),
        "misskey defaults survive with_config"
    );
}

#[test]
fn custom_bounds_plumb_to_upload_slots() {
    let cfg: Config =
        serde_json::from_str(r#"{"uploads": {"per_ip": 1, "global": 4, "max_ips": 8}}"#)
            .expect("parse custom uploads");
    assert!(cfg.validate().is_ok(), "custom uploads validates");
    let bounds = UploadSlotBounds::new(cfg.uploads.per_ip, cfg.uploads.global, cfg.uploads.max_ips);
    let slots = UploadSlots::from_bounds(bounds);
    assert_eq!(slots.bounds(), bounds, "bounds round-trip");
    // Per-IP cap from JSON enforced.
    let first = slots.acquire("10.0.0.1").expect("first slot");
    assert!(
        slots.acquire("10.0.0.1").is_err(),
        "per_ip=1 rejects second concurrent upload"
    );
    drop(first);
    slots.acquire("10.0.0.1").expect("slot freed after drop");
    // Distinct-IP table respects max_ips at rest (sequential acquire+drop).
    for index in 0..32 {
        let ip = format!("192.0.2.{index}");
        let permit = slots.acquire(&ip).expect("sequential acquire passes");
        drop(permit);
    }
    assert!(
        slots.slot_ip_count() < 8 + 8,
        "IP map respects custom max_ips: {}",
        slots.slot_ip_count()
    );
}

#[test]
fn zero_bounds_rejected() {
    let bad_rate: Config =
        serde_json::from_str(r#"{"rate_limit": {"max_buckets": 0}}"#).expect("parse zero buckets");
    assert!(bad_rate.validate().is_err(), "zero max_buckets rejected");
    let bad_uploads: Config =
        serde_json::from_str(r#"{"uploads": {"per_ip": 0}}"#).expect("parse zero per_ip");
    assert!(bad_uploads.validate().is_err(), "zero per_ip rejected");
    assert!(
        serde_json::from_str::<Config>(r#"{"rate_limit": {"max_bucket": 1}}"#).is_err(),
        "rate_limit typo rejected"
    );
    assert!(
        serde_json::from_str::<Config>(r#"{"uploads": {"perip": 1}}"#).is_err(),
        "uploads typo rejected"
    );
}
