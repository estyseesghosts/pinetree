//! Phase 2 Deliverable 3: end-to-end federation between two live `pinetree`
//! HTTP servers (`pinetree-a`, `pinetree-b`) on loopback.
//!
//! Harness choice: in-process `axum::serve` on `127.0.0.1:0` (no subprocess
//! precedent exists for live servers in cargo tests). Each node gets its own
//! `RedbStore` temp dir (`pinetree-fed-{a|b}-{pid}-{n}`), `FsBlobStore`,
//! `SystemClock` (real time, so HTTP `Date` skew checks pass), default
//! `AidxGenerator` ids, `SafeHttpClient` with `allow_private_ips: true`
//! (required for loopback federation), a `JobWorker` (50 ms base backoff so
//! retry phases settle fast), and `RateLimiter::with_misskey_defaults`.
//!
//! The federation route handlers (`webfinger`, `actor`, `notes`, `inbox`)
//! exist in `src/` but are not yet wired into `build_router` (done
//! concurrently), so this file wires them onto the router itself. If route
//! wiring later lands in `src/`, these explicit routes keep working
//! unchanged. Happy-path federation goes through the Misskey HTTP API
//! (`signup`, `users/show` remote resolution, `following/create|delete`,
//! `notes/create`) and `AppServices` + worker drains; raw signed POSTs are
//! used only for auth-negative and dedup-replay tests.
//!
//! Timing: `#[tokio::test(flavor = "multi_thread", worker_threads = 16)]`
//! because `SafeHttpClient::send` blocks on I/O while the servers live on
//! the same runtime. All
//! cross-node assertions use poll-until-deadline helpers, never
//! fixed sleeps (except the short backoff wait in the retry phase).
//!
//! Layout: the shared harness lives in `federation/common.rs`; the seven
//! `#[test]`s keep their exact names in scenario files (`end_to_end`,
//! `media_move`, `clips`, `pages`). The tests are scenario-based — the
//! inbox activities (Create/Update/Delete/Like/Announce/Follow) all run
//! inside the single `federation_two_node_end_to_end` matrix, and the
//! collections tests cover `federated_collections`, so activity-named files
//! would mislabel them.

#[path = "federation/clips.rs"]
mod clips;
#[path = "federation/common.rs"]
mod common;
#[path = "federation/end_to_end.rs"]
mod end_to_end;
#[path = "federation/media_move.rs"]
mod media_move;
#[path = "federation/pages.rs"]
mod pages;
