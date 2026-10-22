//! Concurrent upload slots (split from `response.rs`).
//!
//! Pure move, no behavior change.

#![allow(unused_imports)]
//! Misskey API fixed-window rate limiting (Slice D1; Batch D bounded).
//!
//! Keyed by `(account_id_or_ip, endpoint_bucket)` with a window count
//! (`max_per_window` requests per `window_ms`) plus an optional
//! `min_interval_ms` between requests in the same bucket. Uses an
//! injected [`Clock`] so tests control time. Exceeding either limit
//! returns `Err(ApiError::rate_limit_exceeded(retry_secs))`.
//!
//! Batch D bound: both in-memory maps ([`RateLimiter::state`] ordinary
//! buckets and [`RateLimiter::failures`] auth-failure state) and the
//! [`UploadSlots`] per-IP table are hard-bounded
//! ([`MAX_RATE_LIMIT_BUCKETS`], [`MAX_AUTH_FAILURE_BUCKETS`],
//! [`MAX_UPLOAD_SLOT_IPS`]). Expired entries are pruned by periodic
//! time-gated full scans (at most one per [`PRUNE_INTERVAL_MS`] per map),
//! under cap pressure, and — for auth-failure entries — also on the read
//! path; if the map is still full, the stalest entries are evicted with
//! keys that would currently refuse a request evicted last, so eviction
//! can never silently un-throttle a key that is currently being abusive.
//!
//! TODO(config): wire these bounds through the JSON `Config` once the
//! shared config schema is settled (config.rs is owned by another agent).
//! Until then these hardcoded, documented constants are the bounds; do
//! not add env-var/CLI-only knobs for them.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::super::errors::ApiError;
use crate::application::ports::Clock;

pub use super::super::request::check_allowed_fields;

/// Slice 6 (H1, decision D-c): concurrent-upload slots for
/// `drive/files/create`. Uploads buffer up to `max_file_size` + 1 MiB in
/// memory, so one client must not stack unbounded concurrent uploads: at
/// most [`MAX_CONCURRENT_UPLOADS_PER_IP`] concurrent uploads per client IP
/// (keyed by the same [`client_ip`](super::endpoints::client_ip) string the
/// rate limiter falls back to), plus a [`MAX_CONCURRENT_UPLOADS_GLOBAL`]
/// global cap bounding worst-case transient memory on a single node. The
/// permit is held for the whole upload handler; exhaustion answers
/// Misskey-shaped `RATE_LIMIT_EXCEEDED` (429) with a short `Retry-After`
/// hint so clients back off instead of hot-looping.
pub const MAX_CONCURRENT_UPLOADS_PER_IP: usize = 2;
pub const MAX_CONCURRENT_UPLOADS_GLOBAL: usize = 16;
pub const UPLOAD_BUSY_RETRY_AFTER_SECS: u64 = 5;
/// Batch D bound: at most this many distinct client-IP entries are retained
/// in the upload-slot map. Idle entries (no live or in-flight permits) are
/// evicted under pressure; entries with active permits are never evicted,
/// so the per-IP cap cannot split across two semaphores. If every retained
/// entry is active — only possible under extreme concurrent-upload churn —
/// one extra entry is allowed rather than breaking an active cap, so this
/// is a soft bound under load and a hard bound at rest.
///
/// TODO(config): make this JSON-configurable once the shared config schema
/// is settled (config.rs is owned by another agent).
pub const MAX_UPLOAD_SLOT_IPS: usize = 4_096;
/// Held for the duration of one upload handler; dropping it frees both
/// the per-IP and the global slot.
///
/// Field order matters: fields drop in declaration order, so the two
/// semaphore permits release before `_guard` decrements the IP's active
/// count. That ordering is what makes map eviction safe — an entry with
/// `active == 0` provably has no live permit referencing its semaphore.
#[derive(Debug)]
pub struct UploadPermit {
    _ip: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
    _guard: IpActiveGuard,
}
/// Decrements its IP entry's active count on drop. Always the last field of
/// [`UploadPermit`] so the count reaches zero only after both semaphore
/// permits are released.
#[derive(Debug)]
pub(crate) struct IpActiveGuard {
    inner: Arc<SlotInner>,
    ip: String,
}
impl Drop for IpActiveGuard {
    fn drop(&mut self) {
        let mut slots = self.inner.slots.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = slots.get_mut(&self.ip) {
            entry.active = entry.active.saturating_sub(1);
        }
        // Deliberately no removal here: eviction happens only in
        // `acquire` under the same lock that hands out permits, so a
        // racing acquire can never observe a removed entry while a permit
        // for that IP is still alive (a naive `remove(ip)` on drop would
        // let the next acquire mint a second semaphore and double the
        // effective per-IP cap).
    }
}

/// One retained per-IP upload semaphore plus its outstanding-permit count.
#[derive(Debug)]
pub(crate) struct IpSlot {
    semaphore: Arc<Semaphore>,
    /// Permits checked out plus acquires currently in flight between the
    /// count increment and the semaphore `try_acquire`. The map entry may
    /// be evicted only while this is zero.
    active: usize,
}
#[derive(Debug)]
pub(crate) struct SlotInner {
    per_ip: usize,
    global: Arc<Semaphore>,
    slots: Mutex<HashMap<String, IpSlot>>,
}
/// Concurrent-upload slots; see [`MAX_CONCURRENT_UPLOADS_PER_IP`].
///
/// The per-IP table is bounded by [`MAX_UPLOAD_SLOT_IPS`] without ever
/// breaking the per-IP cap under races: every live permit for an IP shares
/// the single semaphore in that IP's map entry, entries are evicted only
/// while their `active` count is zero, and the count itself is incremented
/// under the map lock before any permit is handed out.
pub struct UploadSlots {
    inner: Arc<SlotInner>,
}
impl UploadSlots {
    pub fn new(per_ip: usize, global: usize) -> Self {
        Self {
            inner: Arc::new(SlotInner {
                per_ip,
                global: Arc::new(Semaphore::new(global)),
                slots: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Production defaults: 2 concurrent uploads per IP, 16 globally.
    pub fn with_defaults() -> Self {
        Self::new(MAX_CONCURRENT_UPLOADS_PER_IP, MAX_CONCURRENT_UPLOADS_GLOBAL)
    }

    /// Retained per-IP slot entries, bounded by [`MAX_UPLOAD_SLOT_IPS`]
    /// at rest. Diagnostic/test hook.
    pub fn slot_ip_count(&self) -> usize {
        self.inner
            .slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    /// Decrement the in-flight count after a failed acquire (the entry is
    /// kept: only idle entries are evicted, and only under cap pressure in
    /// [`UploadSlots::acquire`]).
    fn release_inflight(&self, ip: &str) {
        let mut slots = self.inner.slots.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = slots.get_mut(ip) {
            entry.active = entry.active.saturating_sub(1);
        }
    }

    /// Take one upload slot for `ip`, or fail 429 `RATE_LIMIT_EXCEEDED`.
    /// Non-blocking: never waits, so a saturated server sheds load fast.
    pub fn acquire(&self, ip: &str) -> Result<UploadPermit, ApiError> {
        let busy = || ApiError::rate_limit_exceeded(UPLOAD_BUSY_RETRY_AFTER_SECS);
        let slot = {
            let mut slots = self.inner.slots.lock().unwrap_or_else(|e| e.into_inner());
            if !slots.contains_key(ip) && slots.len() >= MAX_UPLOAD_SLOT_IPS {
                // Evict idle entries (no live or in-flight permits). If
                // every entry is active, inserting one extra entry keeps
                // the per-IP caps correct; the bound is re-established as
                // soon as permits drain.
                slots.retain(|_, entry| entry.active > 0);
            }
            let per_ip = self.inner.per_ip;
            let entry = slots.entry(ip.to_string()).or_insert_with(|| IpSlot {
                semaphore: Arc::new(Semaphore::new(per_ip)),
                active: 0,
            });
            entry.active += 1;
            entry.semaphore.clone()
        };
        let global = match self.inner.global.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                self.release_inflight(ip);
                return Err(busy());
            }
        };
        match slot.try_acquire_owned() {
            Ok(ip_permit) => Ok(UploadPermit {
                _ip: ip_permit,
                _global: global,
                _guard: IpActiveGuard {
                    inner: self.inner.clone(),
                    ip: ip.to_string(),
                },
            }),
            Err(_) => {
                drop(global);
                self.release_inflight(ip);
                Err(busy())
            }
        }
    }
}

#[test]
fn upload_slots_cap_per_ip_and_release_on_drop() {
    // Slice 6 (H1, decision D-c): at most `per_ip` concurrent uploads
    // per client IP; the permit releases when the handler drops it.
    let slots = UploadSlots::new(1, 10);
    let first = slots.acquire("1.1.1.1").expect("first slot");
    let err = slots.acquire("1.1.1.1").unwrap_err();
    assert_eq!(err.code, "RATE_LIMIT_EXCEEDED");
    assert_eq!(err.http_status, 429);
    // A different IP is unaffected by the first IP's usage.
    let _other = slots.acquire("2.2.2.2").expect("other ip slot");
    drop(first);
    let _again = slots.acquire("1.1.1.1").expect("slot after release");
}

#[test]
fn upload_slots_global_cap() {
    // Slice 6 (H1, decision D-c): the global cap binds even when no
    // single IP is at its own cap.
    let slots = UploadSlots::new(10, 1);
    let _first = slots.acquire("1.1.1.1").expect("first slot");
    let err = slots.acquire("2.2.2.2").unwrap_err();
    assert_eq!(err.code, "RATE_LIMIT_EXCEEDED");
    assert_eq!(err.http_status, 429);
}
