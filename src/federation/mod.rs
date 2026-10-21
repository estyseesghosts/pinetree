pub mod inbox;
pub mod outbox;
pub mod delivery;
pub mod host_caps;
pub mod retry;
/// Generic authenticated-fetch gate: HTTP-signature verification on
/// inbound GETs plus the serve/403/404 decision interface. Clip/page tier
/// enforcement and (later) private-community membership checks are
/// resource callbacks above this gate — never below it.
pub mod fetch_gate;

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use crate::domain::fed_policy::{
    ACTIVITY_INDEX_CAP, REPLY_COALESCE_TTL_MS, reply_resolve_key, stamp_retry_due,
};

/// Inbound `Accept`-activity idempotency index (WP5 `Undo(Accept)` by-ID).
///
/// A bare-IRI `Undo{object: <accept-id>}` carries no embedded `Follow`, so
/// the pair it revokes can only be resolved when the original `Accept` was
/// seen. The inbox records every embedded `Accept(Follow)` here
/// (`accept_id → (follower_actor_uri, followee_actor_uri)`); the `Undo`
/// path consults it. Bounded ([`ACTIVITY_INDEX_CAP`]), process-local, and
/// explicitly best-effort: a restart drops the map and a rare bare-IRI undo
/// converges without effect until the next embedded `Accept` re-seeds it.
/// (Durable per-activity storage needs a ports/redb schema change — proposed
/// in the WP5 report — this index closes the single-node gap meanwhile.)
pub struct ActivityIdIndex {
    inner: Mutex<HashMap<String, (String, String)>>,
}

impl ActivityIdIndex {
    fn global() -> &'static ActivityIdIndex {
        static INDEX: LazyLock<ActivityIdIndex> = LazyLock::new(|| ActivityIdIndex {
            inner: Mutex::new(HashMap::new()),
        });
        &INDEX
    }

    /// Record an `Accept(Follow)` grant. No-op on empty ids.
    pub fn record_accept(follower_actor_uri: &str, followee_actor_uri: &str, accept_id: &str) {
        let (follower, followee, id) = (
            follower_actor_uri.trim(),
            followee_actor_uri.trim(),
            accept_id.trim(),
        );
        if follower.is_empty() || followee.is_empty() || id.is_empty() {
            return;
        }
        let mut inner = Self::global().inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.len() >= ACTIVITY_INDEX_CAP && !inner.contains_key(id) {
            // Arbitrary-single eviction (the map is a convergence aid, not
            // an LRU: any evicted entry degrades to the pre-index
            // accept-without-effect, never to a wrong unfollow).
            if let Some(first) = inner.keys().next().cloned() {
                inner.remove(&first);
            }
        }
        inner.insert(id.to_string(), (follower.to_string(), followee.to_string()));
    }

    /// Look up a previously recorded `Accept(Follow)` by activity id.
    pub fn lookup_accept(accept_id: &str) -> Option<(String, String)> {
        Self::global()
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(accept_id.trim())
            .cloned()
    }

    /// Forget one entry (used after a successful by-ID undo so a replayed
    /// `Undo` cannot unfollow a later re-follow granted under the same pair
    /// — the next embedded `Accept` re-seeds when the follow is re-granted).
    pub fn forget(accept_id: &str) {
        Self::global()
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(accept_id.trim());
    }

    #[cfg(test)]
    fn clear_for_tests() {
        Self::global().inner.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }
}

/// Reply-parent resolver coalescer (WP5 reply debt: N-waiters → 1 job).
///
/// The job queue performs no idempotency-key dedup, so N replies to one
/// uncached parent would enqueue N identical `reply-resolve:{uri}` jobs.
/// The inbox consults this process-local coalescer before enqueueing: the
/// first waiter inside [`REPLY_COALESCE_TTL_MS`] enqueues, later waiters
/// skip (their pending-index entries are still recorded — the single job
/// resolves all of them). A waiter past the TTL enqueues a fresh trigger,
/// so a lost job cannot strand waiters forever. Restart-safe by the same
/// argument: worst case is one extra trigger job, never a lost link.
pub struct ReplyCoalescer {
    inner: Mutex<HashMap<String, i64>>,
}

impl ReplyCoalescer {
    fn global() -> &'static ReplyCoalescer {
        static COALESCE: LazyLock<ReplyCoalescer> = LazyLock::new(|| ReplyCoalescer {
            inner: Mutex::new(HashMap::new()),
        });
        &COALESCE
    }

    /// True when the caller should enqueue a resolver job for `parent_uri`
    /// now (first waiter in the window, or window expired). `now_ms` is the
    /// caller's clock; tests pass synthetic times.
    pub fn should_enqueue(parent_uri: &str, now_ms: i64) -> bool {
        let key = reply_resolve_key(parent_uri);
        let mut inner = Self::global().inner.lock().unwrap_or_else(|e| e.into_inner());
        // Opportunistic prune: keep the map proportional to live parents.
        if inner.len() > ACTIVITY_INDEX_CAP {
            let cutoff = now_ms.saturating_sub(REPLY_COALESCE_TTL_MS);
            inner.retain(|_, last| *last >= cutoff);
        }
        match inner.get(&key).copied() {
            Some(last) if now_ms.saturating_sub(last) < REPLY_COALESCE_TTL_MS => false,
            _ => {
                inner.insert(key, now_ms);
                true
            }
        }
    }

    #[cfg(test)]
    fn clear_for_tests() {
        Self::global().inner.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }
}

/// Negative stamp-fetch cache (WP5 stamp debt: dedup transient failures).
///
/// A stamp IRI whose fetch recently failed maps to
/// `(last_attempt_ms, consecutive_failures)`; verification callers consult
/// [`should_fetch`] before hitting the network and record the outcome after.
/// Success removes the entry. Backoff follows
/// [`stamp_retry_due`]: 60s × failures, capped at one hour. Process-local
/// and best-effort like the sibling caches above.
pub struct StampNegativeCache {
    inner: Mutex<HashMap<String, (i64, u32)>>,
}

impl StampNegativeCache {
    fn global() -> &'static StampNegativeCache {
        static NEG: LazyLock<StampNegativeCache> = LazyLock::new(|| StampNegativeCache {
            inner: Mutex::new(HashMap::new()),
        });
        &NEG
    }

    /// True when a stamp fetch may proceed now (no recent failure, or the
    /// backoff window for the recorded failures has elapsed).
    pub fn should_fetch(stamp_uri: &str, now_ms: i64) -> bool {
        let key = stamp_uri.trim().to_string();
        let inner = Self::global().inner.lock().unwrap_or_else(|e| e.into_inner());
        match inner.get(&key).copied() {
            None => true,
            Some((last, failures)) => stamp_retry_due(failures, Some(last), now_ms),
        }
    }

    /// Record a fetch outcome: `true` clears the negative entry, `false`
    /// inserts/advances it.
    pub fn record(stamp_uri: &str, now_ms: i64, success: bool) {
        let key = stamp_uri.trim().to_string();
        if key.is_empty() {
            return;
        }
        let mut inner = Self::global().inner.lock().unwrap_or_else(|e| e.into_inner());
        if success {
            inner.remove(&key);
        } else {
            let failures = inner.get(&key).map(|(_, f)| f.saturating_add(1)).unwrap_or(1);
            if inner.len() >= ACTIVITY_INDEX_CAP && !inner.contains_key(&key) {
                if let Some(first) = inner.keys().next().cloned() {
                    inner.remove(&first);
                }
            }
            inner.insert(key, (now_ms, failures));
        }
    }

    #[cfg(test)]
    fn clear_for_tests() {
        Self::global().inner.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }
}

/// Test-only: drop every federation-layer ephemeral cache entry. Each
/// gaps test calls this first so the process-global maps cannot leak
/// across tests sharing one binary.
#[cfg(test)]
pub fn clear_ephemeral_caches_for_tests() {
    ActivityIdIndex::clear_for_tests();
    ReplyCoalescer::clear_for_tests();
    StampNegativeCache::clear_for_tests();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_index_round_trip_and_forget() {
        ActivityIdIndex::clear_for_tests();
        ActivityIdIndex::record_accept(
            "https://local.example/users/alice",
            "https://remote.example/users/bob",
            "https://remote.example/accepts/1",
        );
        assert_eq!(
            ActivityIdIndex::lookup_accept("https://remote.example/accepts/1"),
            Some((
                "https://local.example/users/alice".to_string(),
                "https://remote.example/users/bob".to_string()
            ))
        );
        assert_eq!(
            ActivityIdIndex::lookup_accept("https://remote.example/accepts/unknown"),
            None
        );
        ActivityIdIndex::forget("https://remote.example/accepts/1");
        assert_eq!(
            ActivityIdIndex::lookup_accept("https://remote.example/accepts/1"),
            None
        );
        // Empty ids never record.
        ActivityIdIndex::record_accept("", "https://remote.example/users/bob", "https://x/1");
        assert_eq!(ActivityIdIndex::lookup_accept("https://x/1"), None);
        ActivityIdIndex::clear_for_tests();
    }

    #[test]
    fn reply_coalescer_n_waiters_one_job() {
        ReplyCoalescer::clear_for_tests();
        let now = 1_760_400_000_000;
        let uri = "https://remote.example/notes/parent";
        assert!(ReplyCoalescer::should_enqueue(uri, now));
        // N further waiters inside the window share the trigger.
        for dt in [1, 1_000, REPLY_COALESCE_TTL_MS - 1] {
            assert!(!ReplyCoalescer::should_enqueue(uri, now + dt));
        }
        // Past the window a fresh trigger is allowed (job-loss cover).
        assert!(ReplyCoalescer::should_enqueue(uri, now + REPLY_COALESCE_TTL_MS));
        // A different parent is independent.
        assert!(ReplyCoalescer::should_enqueue(
            "https://remote.example/notes/other",
            now
        ));
        ReplyCoalescer::clear_for_tests();
    }

    #[test]
    fn stamp_negative_cache_backoff() {
        StampNegativeCache::clear_for_tests();
        let now = 1_760_400_000_000;
        let stamp = "https://target.example/stamps/1";
        assert!(StampNegativeCache::should_fetch(stamp, now));
        StampNegativeCache::record(stamp, now, false);
        assert!(!StampNegativeCache::should_fetch(stamp, now + 59_999));
        assert!(StampNegativeCache::should_fetch(stamp, now + 60_000));
        StampNegativeCache::record(stamp, now + 60_000, true);
        assert!(StampNegativeCache::should_fetch(stamp, now + 60_001));
        StampNegativeCache::clear_for_tests();
    }
}
