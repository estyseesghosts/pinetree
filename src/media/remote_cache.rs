//! Remote media URL validation, downloading, and caching (Phase 2 Slice C2).
//!
//! Validates remote attachment URLs (scheme `http`/`https`, SSRF check,
//! size limit, MIME sniffing) before fetching/caching; stores metadata in
//! `MediaStore` (`owner_id = None`, `is_link = true`) and raw bytes in the
//! remote-cache `BlobStore` pool (`services.cache_blobs`, never the durable
//! `services.blobs` pool) outside the database.
//!
//! Phase 2.6 slice 7 (H2+C3, decision D-d): the inbox no longer fetches
//! attachments inline. It enqueues one durable [`JobKind::RemoteMediaFetch`]
//! job per note (atomically with `record_note_activity`); the worker runs
//! [`process_remote_media_fetch`], which is idempotent per (remote URL,
//! note) and best-effort per attachment.
//!
//! Remote-media cache plan (`docs/remote-media-cache-plan.md` §§4–5):
//! the default is fetch-on-first-media-request ([`fetch_on_first_request`],
//! used by the `GET /files/:id` handler when cache bytes are absent); the
//! job system remains for opt-in warming. One global byte budget plus
//! per-class caps bound the cache; eviction drops bytes only (metadata and
//! the `(source_url, variant)` index row stay, so bytes re-fetch while the
//! source lives). In-flight reservations (not post-hoc totals) keep
//! concurrent fetches under the cap.

mod evict;
mod fetch;
mod store;
#[cfg(test)]
mod support;

pub use evict::ensure_cache_budget;
pub use fetch::{
    AVATAR_VARIANT, DEFAULT_VARIANT, fetch_and_cache_remote_media, fetch_on_first_request,
    fetch_on_first_request_as, validate_remote_media_url,
};
pub use store::{
    MAX_REMOTE_ATTACHMENTS_PER_NOTE, MAX_REMOTE_MEDIA_TOTAL_BYTES_PER_NOTE,
    build_remote_media_fetch_job, process_remote_media_fetch,
};
