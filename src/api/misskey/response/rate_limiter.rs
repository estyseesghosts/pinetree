//! Fixed-window rate limiting (split from `response.rs`).
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
//! Batch D6: the bounds are JSON-configurable via `Config::rate_limit`
//! (`rate_limit { max_buckets, max_auth_failures, prune_interval_ms }`;
//! see `crate::config::RateLimitSection`). The constants below remain
//! the defaults so `RateLimiter::new` preserves current behavior; use
//! `RateLimiter::with_bounds` / `with_config` to install file values.
//! There are no env-var/CLI-only knobs for these bounds.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::super::errors::ApiError;
use crate::application::ports::Clock;
use crate::domain::id::ManualClock;

pub use super::super::request::check_allowed_fields;

pub(crate) const MINUTE_MS: u64 = 60_000;
pub(crate) const HOUR_MS: u64 = 3_600_000;
/// Slice 9 (H4): progressive delay after failed auth attempts. The delay
/// before the next attempt is `AUTH_BACKOFF_BASE_MS` after the first
/// failure in a window, doubling per further failure, capped at
/// [`AUTH_BACKOFF_CAP_MS`].
pub const AUTH_BACKOFF_BASE_MS: u64 = 1_000;
pub const AUTH_BACKOFF_CAP_MS: u64 = 60_000;
/// Batch D bounds on client-keyed in-memory state.
///
/// `MAX_RATE_LIMIT_BUCKETS` caps ordinary `(key, bucket)` window entries,
/// `MAX_AUTH_FAILURE_BUCKETS` caps `(key, bucket)` auth-failure entries,
/// and [`MAX_UPLOAD_SLOT_IPS`] caps distinct per-IP upload-slot entries.
/// All three comfortably exceed single-node legitimate cardinality (tens of
/// distinct local callers) while keeping worst-case memory bounded: each
/// entry is two short strings plus a few integers, so even a full map is
/// on the order of megabytes. Eviction always prefers stale entries and
/// evicts active abuse keys last; see the module docs.
///
/// Batch D6: these constants are the defaults for the JSON-configurable
/// `rate_limit` section (`crate::config::RateLimitSection`). Prefer
/// `RateLimitBounds::default()` (which mirrors them) over the raw
/// constants in new code.
pub const MAX_RATE_LIMIT_BUCKETS: usize = 32_768;
pub const MAX_AUTH_FAILURE_BUCKETS: usize = 32_768;
/// At most one full expiry scan per map per this interval of clock time.
/// Steady-state cost is negligible (a ≤32k-entry scan once a minute), and
/// churn with a frozen clock pays for at most one scan: entries can only
/// expire as the clock advances. Cap-pressure pruning and eviction apply
/// regardless of time, so the bounds hold even if the clock stalls.
pub const PRUNE_INTERVAL_MS: i64 = 60_000;
/// JSON-configurable memory bounds for [`RateLimiter`] (Batch D6).
///
/// Mirrors `crate::config::RateLimitSection` field for field without
/// importing the config crate (keeps the adapter layer decoupled).
/// `Default` mirrors the documented constants above so existing callers
/// keep current behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimitBounds {
    /// Cap on ordinary `(key, bucket)` window entries.
    pub max_buckets: usize,
    /// Cap on `(key, bucket)` auth-failure entries.
    pub max_auth_failures: usize,
    /// At most one full expiry scan per map per this interval (ms).
    pub prune_interval_ms: i64,
}

impl Default for RateLimitBounds {
    fn default() -> Self {
        Self {
            max_buckets: MAX_RATE_LIMIT_BUCKETS,
            max_auth_failures: MAX_AUTH_FAILURE_BUCKETS,
            prune_interval_ms: PRUNE_INTERVAL_MS,
        }
    }
}

impl RateLimitBounds {
    /// Explicit bounds; all three must be `> 0` (checked by
    /// `Config::validate` on the JSON path).
    pub fn new(max_buckets: usize, max_auth_failures: usize, prune_interval_ms: i64) -> Self {
        Self {
            max_buckets,
            max_auth_failures,
            prune_interval_ms,
        }
    }
}
/// Configured limits for one endpoint bucket.
#[derive(Debug, Clone, Copy)]
pub struct BucketLimit {
    pub max_per_window: u64,
    pub window_ms: u64,
    pub min_interval_ms: Option<u64>,
}
#[derive(Debug, Clone, Copy)]
pub(crate) struct BucketState {
    window_start_ms: i64,
    count: u64,
    last_ms: Option<i64>,
}
/// Failure-counted throttle for one auth bucket: a per-account limit and a
/// per-IP limit, each a fixed window of *failed* attempts.
#[derive(Debug, Clone, Copy)]
pub struct AuthLimit {
    pub account: BucketLimit,
    pub ip: BucketLimit,
}
#[derive(Debug, Clone, Copy)]
pub(crate) struct FailureState {
    window_start_ms: i64,
    failures: u64,
    last_failure_ms: i64,
}
/// Fixed-window rate limiter keyed by `(account_id_or_ip, endpoint_bucket)`.
pub struct RateLimiter {
    clock: Arc<dyn Clock>,
    limits: HashMap<String, BucketLimit>,
    state: Mutex<HashMap<(String, String), BucketState>>,
    auth_limits: HashMap<String, AuthLimit>,
    failures: Mutex<HashMap<(String, String), FailureState>>,
    bucket_prune_at: AtomicI64,
    failure_prune_at: AtomicI64,
    bounds: RateLimitBounds,
}
impl RateLimiter {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self::new_with_bounds(clock, RateLimitBounds::default())
    }

    /// Constructor with explicit memory bounds (Batch D6 JSON path).
    pub fn new_with_bounds(clock: Arc<dyn Clock>, bounds: RateLimitBounds) -> Self {
        Self {
            clock,
            limits: HashMap::new(),
            state: Mutex::new(HashMap::new()),
            auth_limits: HashMap::new(),
            failures: Mutex::new(HashMap::new()),
            bucket_prune_at: AtomicI64::new(i64::MIN),
            failure_prune_at: AtomicI64::new(i64::MIN),
            bounds,
        }
    }

    /// Builder installing JSON `rate_limit` bounds (Batch D6).
    /// Preserves all endpoint limits; only the memory caps change.
    pub fn with_bounds(mut self, bounds: RateLimitBounds) -> Self {
        self.bounds = bounds;
        self
    }

    /// Alias for [`RateLimiter::with_bounds`] (Batch D6 `with_config`
    /// wiring point for the `Config::rate_limit` section).
    pub fn with_config(mut self, bounds: RateLimitBounds) -> Self {
        self.bounds = bounds;
        self
    }

    /// Active memory bounds (diagnostic/test hook).
    pub fn bounds(&self) -> RateLimitBounds {
        self.bounds
    }

    /// Misskey Phase 1 defaults (`docs/endpoint-manifest.md` §7 plus the
    /// `notes/reactions/delete` 60/h + 3 s and `drive/files/create` 120/h
    /// buckets from `docs/phase-1-plan.md`), plus the slice 9 auth buckets.
    pub fn with_misskey_defaults(clock: Arc<dyn Clock>) -> Self {
        Self::new(clock)
            .with_limit("i/update", 20, HOUR_MS, None)
            .with_limit("notes/create", 300, HOUR_MS, None)
            .with_limit("notes/edit", 300, HOUR_MS, None)
            .with_limit("notes/delete", 300, HOUR_MS, Some(1_000))
            .with_limit("following/create", 100, HOUR_MS, None)
            .with_limit("following/delete", 100, HOUR_MS, None)
            .with_limit("following/invalidate", 100, HOUR_MS, None)
            .with_limit("users/search-by-username-and-host", 60, HOUR_MS, None)
            .with_limit("ap/show", 60, HOUR_MS, None)
            .with_limit("ap/get", 30, HOUR_MS, None)
            // Batch O WP9 self-service + WP12 preview + WP13 drive extras.
            .with_limit("i/change-password", 10, HOUR_MS, None)
            .with_limit("i/regenerate-token", 10, HOUR_MS, None)
            .with_limit("i/apps", 60, HOUR_MS, None)
            .with_limit("i/authorized-apps", 60, HOUR_MS, None)
            .with_limit("i/signin-history", 60, HOUR_MS, None)
            .with_limit("url-preview", 60, HOUR_MS, None)
            .with_limit("drive/files/upload-from-url", 60, HOUR_MS, None)
            .with_limit("drive/files/find-by-hash", 60, HOUR_MS, None)
            // Upstream `users/lists/push.ts` 1h/30 rate bucket.
            .with_limit("users/lists/push", 30, HOUR_MS, None)
            // Federated clips + pages handoff (upstream buckets):
            // `clips/add-note` 20/h, `pages/create` 10/h, `pages/update`
            // 300/h.
            .with_limit("clips/add-note", 20, HOUR_MS, None)
            .with_limit("pages/create", 10, HOUR_MS, None)
            .with_limit("pages/update", 300, HOUR_MS, None)
            .with_limit("blocking/create", 20, HOUR_MS, None)
            .with_limit("blocking/delete", 100, HOUR_MS, None)
            .with_limit("mute/create", 20, HOUR_MS, None)
            .with_limit("renote-mute/create", 20, HOUR_MS, None)
            .with_limit("notes/reactions/delete", 60, HOUR_MS, Some(3_000))
            .with_limit("drive/files/create", 120, HOUR_MS, None)
            // Draft endpoints (§5; same 300/h cost class as `notes/*`).
            .with_limit("notes/drafts/create", 300, HOUR_MS, None)
            .with_limit("notes/drafts/update", 300, HOUR_MS, None)
            .with_limit("notes/drafts/delete", 300, HOUR_MS, None)
            // Profile import/export (§13; Misskey `secure: true` 1/hr
            // parity — each endpoint gets its own 1/hr bucket. Upstream
            // durations differ for three exports (`export-notes`,
            // `export-clips`, `export-favorites` are 1/day and
            // `export-user-lists` is 1/min); pinetree normalizes all 13 to
            // 1/hr, documented in the endpoint manifest §20.
            .with_limit("i/export-following", 1, HOUR_MS, None)
            .with_limit("i/export-blocking", 1, HOUR_MS, None)
            .with_limit("i/export-mute", 1, HOUR_MS, None)
            .with_limit("i/export-notes", 1, HOUR_MS, None)
            .with_limit("i/export-user-lists", 1, HOUR_MS, None)
            .with_limit("i/export-antennas", 1, HOUR_MS, None)
            .with_limit("i/export-clips", 1, HOUR_MS, None)
            .with_limit("i/export-favorites", 1, HOUR_MS, None)
            .with_limit("i/import-following", 1, HOUR_MS, None)
            .with_limit("i/import-blocking", 1, HOUR_MS, None)
            .with_limit("i/import-muting", 1, HOUR_MS, None)
            .with_limit("i/import-user-lists", 1, HOUR_MS, None)
            .with_limit("i/import-antennas", 1, HOUR_MS, None)
            // Custom-emoji export (upstream `export-custom-emojis.ts`
            // `1hour/1` parity). `admin/emoji/import-zip` has no upstream
            // bucket; it rides the default open bucket like the other
            // `admin/emoji/*` arms.
            .with_limit("export-custom-emojis", 1, HOUR_MS, None)
            // Sharkey `notes/like.ts` 2-calls-per-second bucket parity.
            .with_limit("notes/like", 2, 1_000, None)
            // Every signup attempt from one IP counts (see `check`).
            .with_limit("auth/signup", 20, HOUR_MS, None)
            // MiAuth check polls: every request counts per IP.
            .with_limit("auth/miauth-check", 600, MINUTE_MS, None)
            // Failed signin attempts: 10 per account and 30 per IP per minute.
            .with_auth_limit("auth/signin", 10, 30, MINUTE_MS)
            .with_auth_limit("auth/miauth", 10, 30, MINUTE_MS)
            // Batch O WP9: wrong-password on self-service counts like a
            // failed signin (same 10/account + 30/IP per minute class).
            .with_auth_limit("auth/change-password", 10, 30, MINUTE_MS)
            .with_auth_limit("auth/regenerate-token", 10, 30, MINUTE_MS)
            // Batch P: email/move/translate/notification/discovery buckets
            // (upstream windows: update-email + request-reset-password 3/h,
            // i/move 5/day, fetch-external-resources 50/h, fetch-rss
            // 300/min, test-notification 10/min, notes/versions 10/5s).
            .with_limit("i/update-email", 3, HOUR_MS, None)
            .with_limit("request-reset-password", 3, HOUR_MS, None)
            .with_limit("i/move", 5, 86_400_000, None)
            .with_limit("fetch-external-resources", 50, HOUR_MS, None)
            .with_limit("fetch-rss", 300, MINUTE_MS, None)
            .with_limit("notifications/test-notification", 10, MINUTE_MS, None)
            .with_limit("notes/versions", 10, 5_000, None)
            // Batch Q Phase 1: upstream `i/notifications.ts:23-26` and
            // `i/notifications-grouped.ts:28-31` buckets (30 per 30 s).
            .with_limit("i/notifications", 30, 30_000, None)
            .with_limit("i/notifications-grouped", 30, 30_000, None)
            .with_auth_limit("auth/update-email", 10, 30, MINUTE_MS)
            // Phase 2 P0: the TOTP endpoints were referenced by
            // `check_auth_failures` but never registered, so every 2FA
            // check was a silent no-op. Register the same 10/account +
            // 30/IP per minute class as the other auth buckets, plus
            // ordinary per-endpoint buckets for the i/2fa/* arms
            // (`remove-key` needs none: it is an unconditional 400 stub).
            .with_auth_limit("auth/2fa", 10, 30, MINUTE_MS)
            .with_auth_limit("auth/2fa-authenticate", 10, 30, MINUTE_MS)
            .with_limit("i/2fa/register", 10, HOUR_MS, None)
            .with_limit("i/2fa/key-done", 10, HOUR_MS, None)
            .with_limit("i/2fa/done", 10, HOUR_MS, None)
            .with_limit("i/2fa/password-less", 10, HOUR_MS, None)
            .with_limit("i/2fa/unregister", 10, HOUR_MS, None)
            .with_limit("i/2fa/update-key", 10, HOUR_MS, None)
            .with_limit("i/2fa/authenticate", 30, MINUTE_MS, None)
    }

    /// Failure-counted auth bucket: at most `max_per_account` failures per
    /// account key and `max_per_ip` per IP key within `window_ms`.
    pub fn with_auth_limit(
        mut self,
        bucket: impl Into<String>,
        max_per_account: u64,
        max_per_ip: u64,
        window_ms: u64,
    ) -> Self {
        let limit = |max_per_window| BucketLimit {
            max_per_window,
            window_ms,
            min_interval_ms: None,
        };
        self.auth_limits.insert(
            bucket.into(),
            AuthLimit {
                account: limit(max_per_account),
                ip: limit(max_per_ip),
            },
        );
        self
    }

    /// Refuse an auth attempt before any credential check. Only failures are
    /// counted, so a successful signin is never blocked by earlier successes.
    /// Blocked when either key is over its window cap or still inside its
    /// progressive delay. No lockout: every block expires with the window or
    /// the delay. Buckets without an auth limit always pass.
    pub fn check_auth_failures(
        &self,
        bucket: &str,
        account_key: &str,
        ip_key: &str,
    ) -> Result<(), ApiError> {
        let limit = match self.auth_limits.get(bucket) {
            None => return Ok(()),
            Some(limit) => *limit,
        };
        let now = self.clock.now_ms();
        self.prune_failures_if_due(now);
        let mut states = self.failures.lock().unwrap_or_else(|e| e.into_inner());
        let account_key_pair = (account_key.to_string(), bucket.to_string());
        let ip_key_pair = (ip_key.to_string(), bucket.to_string());
        let account_state = states.get(&account_key_pair).copied();
        let ip_state = states.get(&ip_key_pair).copied();
        let account = failure_retry_secs(account_state, limit.account, now);
        let ip = failure_retry_secs(ip_state, limit.ip, now);
        // Opportunistic expiry on the read path: an entry whose window has
        // fully elapsed is dropped instead of lingering until cap pressure.
        // An entry merely past its progressive backoff is retained: its
        // failure count must survive until the window ends for the cap.
        if !failure_key_retained(&limit, account_state, now) {
            states.remove(&account_key_pair);
        }
        if !failure_key_retained(&limit, ip_state, now) {
            states.remove(&ip_key_pair);
        }
        match account.into_iter().chain(ip).max() {
            Some(retry_secs) => Err(ApiError::rate_limit_exceeded(retry_secs)),
            None => Ok(()),
        }
    }

    /// Count one failed auth attempt against both the account and IP keys.
    pub fn record_auth_failure(&self, bucket: &str, account_key: &str, ip_key: &str) {
        let limit = match self.auth_limits.get(bucket) {
            None => return,
            Some(limit) => *limit,
        };
        let now = self.clock.now_ms();
        self.prune_failures_if_due(now);
        let mut states = self.failures.lock().unwrap_or_else(|e| e.into_inner());
        // Bound the map before growing it: prune expired entries, then
        // evict stalest-first with actively blocking keys evicted last.
        if states.len() >= self.bounds.max_auth_failures {
            prune_failure_states(&mut states, &self.auth_limits, now);
            if states.len() >= self.bounds.max_auth_failures {
                evict_stalest_failures(
                    &mut states,
                    &self.auth_limits,
                    now,
                    self.bounds.max_auth_failures,
                );
            }
        }
        for (key, bucket_limit) in [(account_key, limit.account), (ip_key, limit.ip)] {
            let entry_key = (key.to_string(), bucket.to_string());
            let next = match states.get(&entry_key).copied() {
                Some(state)
                    if (now.saturating_sub(state.window_start_ms) as u64)
                        < bucket_limit.window_ms =>
                {
                    FailureState {
                        window_start_ms: state.window_start_ms,
                        failures: state.failures + 1,
                        last_failure_ms: now,
                    }
                }
                _ => FailureState {
                    window_start_ms: now,
                    failures: 1,
                    last_failure_ms: now,
                },
            };
            states.insert(entry_key, next);
        }
    }

    pub fn with_limit(
        mut self,
        bucket: impl Into<String>,
        max_per_window: u64,
        window_ms: u64,
        min_interval_ms: Option<u64>,
    ) -> Self {
        self.limits.insert(
            bucket.into(),
            BucketLimit {
                max_per_window,
                window_ms,
                min_interval_ms,
            },
        );
        self
    }

    /// Debug-only: multiplies every endpoint bucket's `max_per_window` by
    /// `factor`. Auth failure throttles and `min_interval_ms` are unchanged.
    /// Used by `--debug-weak-rate-limits` for load tests; never in production.
    pub fn with_weakened_limits(mut self, factor: u64) -> Self {
        for limit in self.limits.values_mut() {
            limit.max_per_window = limit.max_per_window.saturating_mul(factor);
        }
        self
    }

    /// Record one request from `account_id_or_ip` to `endpoint_bucket`.
    /// Buckets without a configured limit always pass.
    pub fn check(&self, account_id_or_ip: &str, endpoint_bucket: &str) -> Result<(), ApiError> {
        let limit = match self.limits.get(endpoint_bucket) {
            None => return Ok(()),
            Some(limit) => *limit,
        };
        let now = self.clock.now_ms();
        self.prune_buckets_if_due(now);
        let key = (account_id_or_ip.to_string(), endpoint_bucket.to_string());
        let mut states = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // Bound the map before growing it: prune expired entries, then
        // evict stalest-first with keys at their window cap evicted last.
        // Only new keys can grow the map, so existing keys skip the scan
        // and stay on the fast path.
        if !states.contains_key(&key) && states.len() >= self.bounds.max_buckets {
            prune_bucket_states(&mut states, &self.limits, now);
            if states.len() >= self.bounds.max_buckets {
                evict_stalest_buckets(&mut states, &self.limits, now, self.bounds.max_buckets);
            }
        }
        let mut entry = states.get(&key).copied().unwrap_or(BucketState {
            window_start_ms: now,
            count: 0,
            last_ms: None,
        });
        // Roll the fixed window when it has elapsed.
        if now.saturating_sub(entry.window_start_ms) as u64 >= limit.window_ms {
            entry.window_start_ms = now;
            entry.count = 0;
        }
        // Short-term minimum interval first (Misskey `BRIEF_REQUEST_INTERVAL`).
        if let (Some(min), Some(last)) = (limit.min_interval_ms, entry.last_ms) {
            let elapsed = now.saturating_sub(last) as u64;
            if elapsed < min {
                return Err(ApiError::rate_limit_exceeded(ceil_secs(min - elapsed)));
            }
        }
        if entry.count >= limit.max_per_window {
            let reset_in =
                (entry.window_start_ms as u64).saturating_add(limit.window_ms) as i64 - now;
            return Err(ApiError::rate_limit_exceeded(ceil_secs(
                reset_in.max(0) as u64
            )));
        }
        entry.count += 1;
        entry.last_ms = Some(now);
        states.insert(key, entry);
        Ok(())
    }

    /// Full expiry scan of the ordinary-bucket map, at most once per
    /// bound interval of clock time. Lock-free gate: concurrent
    /// callers may rarely prune twice, which is harmless and idempotent.
    fn prune_buckets_if_due(&self, now: i64) {
        let interval = self.bounds.prune_interval_ms;
        if now.saturating_sub(self.bucket_prune_at.load(Ordering::Relaxed)) < interval {
            return;
        }
        let mut states = self.state.lock().unwrap_or_else(|e| e.into_inner());
        prune_bucket_states(&mut states, &self.limits, now);
        self.bucket_prune_at.store(now, Ordering::Relaxed);
    }

    /// Full expiry scan of the auth-failure map, at most once per
    /// bound interval of clock time. See [`RateLimiter::prune_buckets_if_due`].
    fn prune_failures_if_due(&self, now: i64) {
        let interval = self.bounds.prune_interval_ms;
        if now.saturating_sub(self.failure_prune_at.load(Ordering::Relaxed)) < interval {
            return;
        }
        let mut states = self.failures.lock().unwrap_or_else(|e| e.into_inner());
        prune_failure_states(&mut states, &self.auth_limits, now);
        self.failure_prune_at.store(now, Ordering::Relaxed);
    }

    /// Retained ordinary bucket entries, bounded by the configured
    /// `max_buckets` (default [`MAX_RATE_LIMIT_BUCKETS`]).
    /// Diagnostic/test hook.
    pub fn bucket_count(&self) -> usize {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Retained auth-failure entries, bounded by the configured
    /// `max_auth_failures` (default [`MAX_AUTH_FAILURE_BUCKETS`]).
    /// Diagnostic/test hook.
    pub fn failure_count(&self) -> usize {
        self.failures
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }
}

/// An ordinary bucket entry is live while its fixed window has not elapsed,
/// or while its `min_interval_ms` state can still reject a request. Anything
/// else is expired and safe to drop: the next request for that key simply
/// starts a fresh window, which is exactly what the rollover path in
/// [`RateLimiter::check`] would do.
pub(crate) fn bucket_state_is_live(
    limits: &HashMap<String, BucketLimit>,
    key: &(String, String),
    state: &BucketState,
    now: i64,
) -> bool {
    let limit = match limits.get(&key.1) {
        None => return false,
        Some(limit) => *limit,
    };
    if (now.saturating_sub(state.window_start_ms) as u64) < limit.window_ms {
        return true;
    }
    match (limit.min_interval_ms, state.last_ms) {
        (Some(min_interval), Some(last)) => (now.saturating_sub(last) as u64) < min_interval,
        _ => false,
    }
}
/// Drop every expired ordinary bucket entry. Called by the periodic
/// time-gated scan and under cap pressure; see
/// [`RateLimiter::prune_buckets_if_due`].
pub(crate) fn prune_bucket_states(
    states: &mut HashMap<(String, String), BucketState>,
    limits: &HashMap<String, BucketLimit>,
    now: i64,
) {
    states.retain(|key, state| bucket_state_is_live(limits, key, state, now));
}
/// Eviction protection class of an ordinary bucket entry. Only the last
/// class — a key currently at its window cap inside a live window, i.e. an
/// actively abusive key — is strongly protected. Interval-only entries
/// (window gone, `min_interval_ms` still pending) are never *pruned* early,
/// but under hard cap pressure they compete by age like any other idle
/// key: with every entry live, something must go, and the stalest entry is
/// the throttle-safest victim (a hot abuse key always has a fresh window).
pub(crate) fn bucket_eviction_rank(
    limits: &HashMap<String, BucketLimit>,
    key: &(String, String),
    state: &BucketState,
    now: i64,
) -> (bool, i64) {
    let at_cap_live = match limits.get(&key.1) {
        None => false,
        Some(limit) => {
            (now.saturating_sub(state.window_start_ms) as u64) < limit.window_ms
                && state.count >= limit.max_per_window
        }
    };
    (at_cap_live, state.window_start_ms)
}
/// Evict stalest-first until the map fits under `max_buckets`.
/// Entries for unknown buckets and idle keys go first (oldest window
/// first); keys currently at their cap inside a live window are evicted
/// only if nothing else remains, so eviction under churn discards idle
/// keys while an actively abusive key keeps throttling.
/// Eviction runs in a single pass down to a below-cap target so sustained
/// churn amortizes to O(1) per insert instead of one O(map) scan per key.
pub(crate) fn evict_stalest_buckets(
    states: &mut HashMap<(String, String), BucketState>,
    limits: &HashMap<String, BucketLimit>,
    now: i64,
    max_buckets: usize,
) {
    if states.len() < max_buckets {
        return;
    }
    let target = max_buckets * 7 / 8;
    let mut ranked: Vec<(bool, i64, (String, String))> = states
        .iter()
        .map(|(key, state)| {
            let (at_cap_live, window_start) = bucket_eviction_rank(limits, key, state, now);
            (at_cap_live, window_start, key.clone())
        })
        .collect();
    ranked.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let excess = states.len().saturating_sub(target).max(1);
    for (_, _, key) in ranked.into_iter().take(excess) {
        states.remove(&key);
    }
}
/// Whether an auth-failure entry must be retained: its window is still open
/// under either of the bucket's limits (account or IP — the map key does
/// not record which role a key was inserted under, so openness under either
/// limit retains it). An entry merely past its progressive backoff is still
/// retained: dropping it would reset its failure count and let an attacker
/// stretch failures past the window cap by waiting out each backoff.
/// Deletion is safe only once the window has fully elapsed, at which point
/// a fresh attempt starts a new window anyway.
pub(crate) fn failure_key_retained(
    limit: &AuthLimit,
    state: Option<FailureState>,
    now: i64,
) -> bool {
    match state {
        None => false,
        Some(state) => {
            let age = now.saturating_sub(state.window_start_ms).max(0) as u64;
            age < limit.account.window_ms || age < limit.ip.window_ms
        }
    }
}
/// An auth-failure entry is live while [`failure_key_retained`] holds for
/// its bucket: the map key does not record whether the key was inserted in
/// the account or IP role, so liveness under either limit retains it — the
/// safe direction, since it can only keep throttling, never drop it early.
pub(crate) fn failure_state_is_live(
    auth_limits: &HashMap<String, AuthLimit>,
    key: &(String, String),
    state: &FailureState,
    now: i64,
) -> bool {
    match auth_limits.get(&key.1) {
        None => false,
        Some(limit) => failure_key_retained(limit, Some(*state), now),
    }
}
/// Whether an auth-failure entry would currently refuse an attempt under
/// either of the bucket's limits. Eviction protection is keyed on this —
/// not mere window-openness — so discarding entries under cap pressure can
/// never silently un-throttle an actively blocked key in favor of a live
/// but non-blocking one.
pub(crate) fn failure_is_blocking(
    auth_limits: &HashMap<String, AuthLimit>,
    key: &(String, String),
    state: &FailureState,
    now: i64,
) -> bool {
    match auth_limits.get(&key.1) {
        None => false,
        Some(limit) => {
            failure_retry_secs(Some(*state), limit.account, now).is_some()
                || failure_retry_secs(Some(*state), limit.ip, now).is_some()
        }
    }
}
/// Drop every expired auth-failure entry. Called by the periodic
/// time-gated scan and under cap pressure; see
/// [`RateLimiter::prune_failures_if_due`].
pub(crate) fn prune_failure_states(
    states: &mut HashMap<(String, String), FailureState>,
    auth_limits: &HashMap<String, AuthLimit>,
    now: i64,
) {
    states.retain(|key, state| failure_state_is_live(auth_limits, key, state, now));
}
/// Evict until the map fits under `max_auth_failures`.
/// Non-blocking entries go first (fewest failures, oldest first);
/// entries that would currently refuse an attempt are evicted last.
/// Single-pass down to a below-cap target; see [`evict_stalest_buckets`].
pub(crate) fn evict_stalest_failures(
    states: &mut HashMap<(String, String), FailureState>,
    auth_limits: &HashMap<String, AuthLimit>,
    now: i64,
    max_auth_failures: usize,
) {
    if states.len() < max_auth_failures {
        return;
    }
    let target = max_auth_failures * 7 / 8;
    let mut ranked: Vec<(bool, u64, i64, (String, String))> = states
        .iter()
        .map(|(key, state)| {
            (
                failure_is_blocking(auth_limits, key, state, now),
                state.failures,
                state.last_failure_ms,
                key.clone(),
            )
        })
        .collect();
    ranked.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
    let excess = states.len().saturating_sub(target).max(1);
    for (_, _, _, key) in ranked.into_iter().take(excess) {
        states.remove(&key);
    }
}
pub(crate) fn ceil_secs(ms: u64) -> u64 {
    ms.div_ceil(1000).max(1)
}
/// Progressive delay after `failures` (>= 1) failed attempts in a window.
pub(crate) fn auth_backoff_ms(failures: u64) -> u64 {
    let shift = failures.saturating_sub(1).min(16);
    (AUTH_BACKOFF_BASE_MS << shift).min(AUTH_BACKOFF_CAP_MS)
}
/// Retry hint in seconds for one failure-counted key, or `None` when the key
/// may attempt now: its window has expired, or it is under the cap and past
/// its progressive delay.
pub(crate) fn failure_retry_secs(
    state: Option<FailureState>,
    limit: BucketLimit,
    now: i64,
) -> Option<u64> {
    let state = state?;
    let window_end = state.window_start_ms.saturating_add(limit.window_ms as i64);
    if now >= window_end {
        return None;
    }
    if state.failures >= limit.max_per_window {
        return Some(ceil_secs((window_end - now) as u64));
    }
    let backoff = auth_backoff_ms(state.failures);
    let elapsed = now.saturating_sub(state.last_failure_ms).max(0) as u64;
    if elapsed < backoff {
        Some(ceil_secs(backoff - elapsed))
    } else {
        None
    }
}
fn limiter_at(now_ms: i64) -> (RateLimiter, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::new(now_ms));
    let limiter = RateLimiter::new(clock.clone()).with_limit("notes/create", 2, 60_000, None);
    (limiter, clock)
}

#[test]
fn window_count_and_rollover() {
    let (limiter, clock) = limiter_at(0);
    assert!(limiter.check("alice", "notes/create").is_ok());
    assert!(limiter.check("alice", "notes/create").is_ok());
    let err = limiter.check("alice", "notes/create").unwrap_err();
    assert_eq!(err.code, "RATE_LIMIT_EXCEEDED");
    // Other actors and unconfigured buckets are unaffected.
    assert!(limiter.check("bob", "notes/create").is_ok());
    assert!(limiter.check("alice", "meta").is_ok());
    // Window rolls over after 60 s.
    clock.advance_ms(60_000);
    assert!(limiter.check("alice", "notes/create").is_ok());
}

#[test]
fn min_interval_enforced() {
    let clock = Arc::new(ManualClock::new(0));
    let limiter =
        RateLimiter::new(clock.clone()).with_limit("notes/delete", 300, HOUR_MS, Some(1_000));
    assert!(limiter.check("alice", "notes/delete").is_ok());
    let err = limiter.check("alice", "notes/delete").unwrap_err();
    assert_eq!(err.code, "RATE_LIMIT_EXCEEDED");
    clock.advance_ms(1_000);
    assert!(limiter.check("alice", "notes/delete").is_ok());
}

fn auth_limiter_at(now_ms: i64) -> (RateLimiter, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::new(now_ms));
    // 3 failures per account and 5 per IP within 60 s.
    let limiter = RateLimiter::new(clock.clone()).with_auth_limit("auth/signin", 3, 5, 60_000);
    (limiter, clock)
}

#[test]
fn slice9_auth_failures_backoff_then_count_cap_then_window_expiry() {
    let (limiter, clock) = auth_limiter_at(0);
    let (acct, ip) = ("acct:alice", "ip:1.1.1.1");
    assert!(limiter.check_auth_failures("auth/signin", acct, ip).is_ok());
    // Failure 1 (t=0): progressive delay of 1 s before the next attempt.
    limiter.record_auth_failure("auth/signin", acct, ip);
    let err = limiter
        .check_auth_failures("auth/signin", acct, ip)
        .unwrap_err();
    assert_eq!(err.code, "RATE_LIMIT_EXCEEDED");
    assert_eq!(err.http_status, 429);
    clock.advance_ms(1_000);
    // Failure 2 (t=1 s): the delay doubles to 2 s.
    assert!(limiter.check_auth_failures("auth/signin", acct, ip).is_ok());
    limiter.record_auth_failure("auth/signin", acct, ip);
    assert!(
        limiter
            .check_auth_failures("auth/signin", acct, ip)
            .is_err()
    );
    clock.advance_ms(2_000);
    // Failure 3 (t=3 s) reaches the per-account cap of 3.
    assert!(limiter.check_auth_failures("auth/signin", acct, ip).is_ok());
    limiter.record_auth_failure("auth/signin", acct, ip);
    clock.advance_ms(30_000);
    assert!(
        limiter
            .check_auth_failures("auth/signin", acct, ip)
            .is_err(),
        "count cap holds until the window ends"
    );
    // The window expires at 60 s: no lockout beyond the window.
    clock.advance_ms(30_000);
    assert!(limiter.check_auth_failures("auth/signin", acct, ip).is_ok());
}

#[test]
fn slice9_many_accounts_from_one_ip_trip_the_ip_bucket() {
    let (limiter, clock) = auth_limiter_at(0);
    for n in 0..5 {
        limiter.record_auth_failure("auth/signin", &format!("acct:user{n}"), "ip:2.2.2.2");
    }
    // A fresh account from the same IP is refused although it has no
    // failures of its own.
    let err = limiter
        .check_auth_failures("auth/signin", "acct:fresh", "ip:2.2.2.2")
        .unwrap_err();
    assert_eq!(err.code, "RATE_LIMIT_EXCEEDED");
    // Other IPs are unaffected.
    assert!(
        limiter
            .check_auth_failures("auth/signin", "acct:fresh", "ip:3.3.3.3")
            .is_ok()
    );
    clock.advance_ms(60_000);
    assert!(
        limiter
            .check_auth_failures("auth/signin", "acct:fresh", "ip:2.2.2.2")
            .is_ok()
    );
}
