//! Wave 1 L3: reservation release underflow surfaces instead of clamping.
//!
//! Fails on old code: the second `release_cache_reservation(100)` clamped
//! `in_flight` at 0 via `saturating_sub` and returned `Ok`, hiding the
//! double-release.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::ports::{MediaStore, PortError};
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::persistence::redb::RedbStore;

static SEQ: AtomicU64 = AtomicU64::new(0);

fn open_store() -> Arc<RedbStore> {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-l3-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_760_400_000_000));
    RedbStore::open_with_default_ids(dir.join("store.redb"), clock).unwrap()
}

#[test]
fn double_release_returns_err_and_leaves_counter_unchanged() {
    let store = open_store();
    assert!(store.reserve_cache_bytes(100).unwrap());
    assert_eq!(store.cache_stats().unwrap().in_flight_bytes, 100);
    store.release_cache_reservation(100).unwrap();
    assert_eq!(store.cache_stats().unwrap().in_flight_bytes, 0);
    let err = store
        .release_cache_reservation(100)
        .expect_err("double release must surface, not clamp");
    assert!(
        matches!(err, PortError::StorageUnavailable(_)),
        "unexpected: {err:?}"
    );
    assert_eq!(
        store.cache_stats().unwrap().in_flight_bytes,
        0,
        "failed release must not move the counter"
    );
}

#[test]
fn oversized_release_returns_err_and_keeps_remainder() {
    let store = open_store();
    assert!(store.reserve_cache_bytes(40).unwrap());
    let err = store
        .release_cache_reservation(100)
        .expect_err("release larger than outstanding must surface");
    assert!(
        matches!(err, PortError::StorageUnavailable(_)),
        "unexpected: {err:?}"
    );
    assert_eq!(store.cache_stats().unwrap().in_flight_bytes, 40);
    store.release_cache_reservation(40).unwrap();
    assert_eq!(store.cache_stats().unwrap().in_flight_bytes, 0);
}
