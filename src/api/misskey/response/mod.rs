//! Misskey API rate limiting and upload slots (split from `response.rs`).
//!
//! Pure move, no behavior change.

mod rate_limiter;
mod upload_slots;

pub use super::request::check_allowed_fields;
pub use rate_limiter::{
    AUTH_BACKOFF_BASE_MS, AUTH_BACKOFF_CAP_MS, AuthLimit, BucketLimit, MAX_AUTH_FAILURE_BUCKETS,
    MAX_RATE_LIMIT_BUCKETS, PRUNE_INTERVAL_MS, RateLimitBounds, RateLimiter,
};
pub use upload_slots::{
    MAX_CONCURRENT_UPLOADS_GLOBAL, MAX_CONCURRENT_UPLOADS_PER_IP, MAX_UPLOAD_SLOT_IPS,
    UPLOAD_BUSY_RETRY_AFTER_SECS, UploadPermit, UploadSlotBounds, UploadSlots,
};
