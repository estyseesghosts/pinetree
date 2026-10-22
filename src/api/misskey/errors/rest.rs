//! `rest` error constructors (split from `errors.rs`).
//!
#![allow(unused_imports)]
use super::codes::*;
use super::error::ApiError;

impl ApiError {
    /// Misskey `fetch-rss.ts` `INVALID_URL` (HTTP 400 upstream).
    pub fn fetch_rss_invalid_url() -> Self {
        Self::client("Invalid URL.", "INVALID_URL", FETCH_RSS_INVALID_URL_ID, 400)
    }
    /// Misskey `fetch-rss.ts` `FETCH_RSS_FAILED` (HTTP 422 upstream).
    pub fn fetch_rss_failed() -> Self {
        Self::client(
            "Failed to fetch RSS.",
            "FETCH_RSS_FAILED",
            FETCH_RSS_FAILED_ID,
            422,
        )
    }
    /// Misskey `fetch-rss.ts` `FETCH_RSS_UNAVAILABLE` (HTTP 503 upstream).
    pub fn fetch_rss_unavailable() -> Self {
        Self::client(
            "RSS fetching is temporarily unavailable.",
            "FETCH_RSS_UNAVAILABLE",
            FETCH_RSS_UNAVAILABLE_ID,
            503,
        )
    }
    /// Gaps final wiring: `gallery/posts/like` + `flash/like`
    /// `ALREADY_LIKED` (kept out of the shared lookup so the pages
    /// `ALREADY_LIKED` UUID is untouched; pinetree-minted —
    /// `// TODO(final): verify against misskey`).
    pub fn gallery_already_liked() -> Self {
        Self::client(
            "The item has already been liked.",
            "ALREADY_LIKED",
            GALLERY_ALREADY_LIKED_ID,
            400,
        )
    }
    /// Gaps final wiring: `gallery/posts/unlike` + `flash/unlike`
    /// `NOT_LIKED` (same non-shared treatment as the like twin).
    pub fn gallery_not_liked() -> Self {
        Self::client(
            "You have not liked that item.",
            "NOT_LIKED",
            GALLERY_NOT_LIKED_ID,
            400,
        )
    }
}
