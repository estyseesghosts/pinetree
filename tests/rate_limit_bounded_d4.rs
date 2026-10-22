//! Batch D: bounded rate-limiter and upload-slot maps.
//!
//! Proves that high-cardinality churn over client/account keys and IPs
//! cannot grow the in-memory maps without bound, that expired entries are
//! pruned, that actively abusive keys keep throttling through eviction,
//! and that per-IP upload caps survive races between acquires, drops, and
//! idle-entry eviction.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pinetree::api::misskey::response::{
    MAX_AUTH_FAILURE_BUCKETS, MAX_RATE_LIMIT_BUCKETS, MAX_UPLOAD_SLOT_IPS, RateLimiter, UploadSlots,
};
use pinetree::domain::id::ManualClock;

fn bucket_limiter_at(
    now_ms: i64,
    max_per_window: u64,
    window_ms: u64,
) -> (RateLimiter, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::new(now_ms));
    let limiter =
        RateLimiter::new(clock.clone()).with_limit("notes/create", max_per_window, window_ms, None);
    (limiter, clock)
}

#[test]
fn ordinary_buckets_stay_bounded_and_expire_after_window() {
    let (limiter, clock) = bucket_limiter_at(0, 10_000, 60_000);
    for index in 0..MAX_RATE_LIMIT_BUCKETS + 500 {
        let key = format!("churn-key-{index}");
        limiter
            .check(&key, "notes/create")
            .expect("fresh churn key passes");
    }
    assert!(
        limiter.bucket_count() <= MAX_RATE_LIMIT_BUCKETS,
        "map exceeds documented bound: {} > {MAX_RATE_LIMIT_BUCKETS}",
        limiter.bucket_count()
    );

    // Past the window every churn entry is dead: the next insert prunes
    // them instead of growing the map.
    clock.advance_ms(61_000);
    limiter
        .check("post-window-key", "notes/create")
        .expect("fresh key passes after window");
    assert!(
        limiter.bucket_count() <= 16,
        "expired entries were not pruned: {} retained",
        limiter.bucket_count()
    );
    // An old key starts a fresh window rather than inheriting stale state.
    limiter
        .check("churn-key-0", "notes/create")
        .expect("expired key restarts cleanly");
}

#[test]
fn active_abuse_key_survives_eviction_pressure() {
    // One-hour window so nothing expires mid-test.
    let (limiter, _clock) = bucket_limiter_at(0, 2, 3_600_000);
    limiter
        .check("victim", "notes/create")
        .expect("first passes");
    limiter
        .check("victim", "notes/create")
        .expect("second passes");
    assert!(
        limiter.check("victim", "notes/create").is_err(),
        "victim must be at cap before churn"
    );

    for index in 0..MAX_RATE_LIMIT_BUCKETS + 5_000 {
        let key = format!("other-{index}");
        let _ = limiter.check(&key, "notes/create");
    }
    assert!(
        limiter.bucket_count() <= MAX_RATE_LIMIT_BUCKETS,
        "map exceeds documented bound: {} > {MAX_RATE_LIMIT_BUCKETS}",
        limiter.bucket_count()
    );
    assert!(
        limiter.check("victim", "notes/create").is_err(),
        "eviction must not un-throttle a key currently at its cap"
    );
}

#[test]
fn min_interval_state_survives_prune_while_window_expired() {
    let clock = Arc::new(ManualClock::new(0));
    // Window (60 s) shorter than the minimum interval (90 s): after the
    // window lapses the entry is still live via the interval, so expiry
    // pruning must retain it. (Under hard cap pressure the stalest entries
    // may be evicted by age regardless; that path is covered by
    // `active_abuse_key_survives_eviction_pressure` for at-cap keys.)
    let limiter =
        RateLimiter::new(clock.clone()).with_limit("notes/delete", 300, 60_000, Some(90_000));
    limiter
        .check("victim", "notes/delete")
        .expect("first passes");
    clock.advance_ms(61_000);

    // Below-cap churn triggers the periodic prune without any eviction.
    for index in 0..5_000 {
        let key = format!("filler-{index}");
        limiter
            .check(&key, "notes/delete")
            .expect("fresh filler passes");
    }
    assert!(
        limiter.bucket_count() <= 5_002,
        "map grew unexpectedly: {}",
        limiter.bucket_count()
    );
    let err = limiter
        .check("victim", "notes/delete")
        .expect_err("interval state must survive expiry pruning");
    assert_eq!(err.code, "RATE_LIMIT_EXCEEDED");
    clock.advance_ms(30_000);
    limiter
        .check("victim", "notes/delete")
        .expect("interval elapsed: request passes");
}

#[test]
fn auth_failures_stay_bounded_expire_and_keep_blocking_abuse() {
    let clock = Arc::new(ManualClock::new(0));
    let limiter = RateLimiter::new(clock.clone()).with_auth_limit("auth/signin", 3, 5, 60_000);
    for _ in 0..3 {
        limiter.record_auth_failure("auth/signin", "victim-account", "victim-ip");
    }
    assert!(
        limiter
            .check_auth_failures("auth/signin", "victim-account", "victim-ip")
            .is_err(),
        "victim must be blocked before churn"
    );

    for index in 0..MAX_AUTH_FAILURE_BUCKETS + 5_000 {
        limiter.record_auth_failure(
            "auth/signin",
            &format!("account-{index}"),
            &format!("ip-{index}"),
        );
    }
    assert!(
        limiter.failure_count() <= MAX_AUTH_FAILURE_BUCKETS,
        "failure map exceeds documented bound: {} > {MAX_AUTH_FAILURE_BUCKETS}",
        limiter.failure_count()
    );
    assert!(
        limiter
            .check_auth_failures("auth/signin", "victim-account", "victim-ip")
            .is_err(),
        "eviction must not un-throttle an actively blocked key"
    );

    // Past the window the whole map is dead: the next record prunes it and
    // the victim is unblocked (no lockout beyond the window).
    clock.advance_ms(61_000);
    limiter.record_auth_failure("auth/signin", "fresh-account", "fresh-ip");
    assert!(
        limiter.failure_count() <= 16,
        "expired failure entries were not pruned: {} retained",
        limiter.failure_count()
    );
    assert!(
        limiter
            .check_auth_failures("auth/signin", "victim-account", "victim-ip")
            .is_ok(),
        "window expiry must clear the block"
    );
}

#[test]
fn upload_slots_cap_per_ip_and_release_on_drop() {
    let slots = UploadSlots::new(2, 10);
    let first = slots.acquire("10.0.0.1").expect("first slot");
    let second = slots.acquire("10.0.0.1").expect("second slot");
    let err = slots
        .acquire("10.0.0.1")
        .expect_err("third exceeds per-IP cap");
    assert_eq!(err.code, "RATE_LIMIT_EXCEEDED");
    assert_eq!(err.http_status, 429);
    let _other = slots.acquire("10.0.0.2").expect("other IP unaffected");
    drop(first);
    let _again = slots.acquire("10.0.0.1").expect("slot freed by drop");
    drop(second);
    drop(_other);
    drop(_again);
}

#[test]
fn upload_slot_ips_stay_bounded_under_churn() {
    let slots = UploadSlots::with_defaults();
    for index in 0..MAX_UPLOAD_SLOT_IPS + 2_000 {
        let ip = format!("192.0.2.{index}");
        let permit = slots.acquire(&ip).expect("sequential acquire passes");
        drop(permit);
    }
    assert!(
        slots.slot_ip_count() < MAX_UPLOAD_SLOT_IPS,
        "IP map exceeds documented bound: {} >= {MAX_UPLOAD_SLOT_IPS}",
        slots.slot_ip_count()
    );
    // Eviction removed only idle entries: the table still works.
    slots
        .acquire("198.51.100.7")
        .expect("acquire works after churn");
}

#[tokio::test]
async fn upload_slots_same_ip_race_respects_cap() {
    let slots = Arc::new(UploadSlots::new(2, 64));
    let current = Arc::new(AtomicUsize::new(0));
    let observed_max = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for _ in 0..32 {
        let slots = slots.clone();
        let current = current.clone();
        let observed_max = observed_max.clone();
        handles.push(tokio::spawn(async move {
            if let Ok(_permit) = slots.acquire("203.0.113.9") {
                let held = current.fetch_add(1, Ordering::SeqCst) + 1;
                observed_max.fetch_max(held, Ordering::SeqCst);
                tokio::task::yield_now().await;
                current.fetch_sub(1, Ordering::SeqCst);
            }
        }));
    }
    for handle in handles {
        handle.await.expect("worker joins");
    }
    assert_eq!(current.load(Ordering::SeqCst), 0, "permits leak");
    assert!(
        observed_max.load(Ordering::SeqCst) <= 2,
        "per-IP cap violated under race: max {}",
        observed_max.load(Ordering::SeqCst)
    );
    assert!(
        observed_max.load(Ordering::SeqCst) >= 1,
        "no acquire ever succeeded"
    );
}

#[tokio::test]
async fn upload_slots_eviction_race_never_splits_per_ip_cap() {
    // Holds one permit on the victim IP while another task churns enough
    // distinct IPs to force idle-entry eviction scans. If eviction ever
    // removed the victim entry, the follow-up acquire would mint a second
    // semaphore and succeed despite per-IP cap 1 — the naive-remove bug.
    let slots = Arc::new(UploadSlots::new(1, 10_000));
    let held = slots
        .acquire("victim-ip")
        .expect("victim holds the only slot");

    let churner = {
        let slots = slots.clone();
        tokio::spawn(async move {
            for index in 0..MAX_UPLOAD_SLOT_IPS + 500 {
                let ip = format!("churn-ip-{index}");
                if let Ok(permit) = slots.acquire(&ip) {
                    drop(permit);
                }
            }
        })
    };
    // Interleave racing acquires on the victim IP while eviction runs.
    for _ in 0..200 {
        assert!(
            slots.acquire("victim-ip").is_err(),
            "victim cap split across semaphores during eviction race"
        );
        tokio::task::yield_now().await;
    }
    churner.await.expect("churner joins");
    assert!(
        slots.acquire("victim-ip").is_err(),
        "victim slot must still be held after churn"
    );
    assert!(
        slots.slot_ip_count() < MAX_UPLOAD_SLOT_IPS + 8,
        "IP map unbounded: {}",
        slots.slot_ip_count()
    );
    drop(held);
    slots
        .acquire("victim-ip")
        .expect("slot released after holder drops");
}
