# 2026-10-15 — Phase 2.5 Slice 7: Decouple ActivityPub from the Misskey adapter

- `activitypub/{actor,objects,webfinger}.rs` (15 handlers) and
  `federation/inbox.rs` (`inbox_handler`, `user_inbox_handler`, `process_inbox`)
  now take `State<Arc<AppServices>>` / `&AppServices` instead of `ApiState`.
  `ApiState` stays for the Misskey router; `main.rs` wiring unchanged.
- Router (`endpoints/mod.rs::build_router`, signature unchanged):
  AP/inbox routes form a sub-router with `.with_state(services)` merged into the
  Misskey router (both `Router<()>` after state baked in). `DefaultBodyLimit`
  note: `disable()` now applies to Misskey routes only; AP routes fall back to
  axum's 2 MiB default — harmless (inbox enforces its own 64 KiB cap; other AP
  routes are GETs; drive uploads stay on the Misskey router).
- `format_millis` + `PACKER_VERSION` moved to neutral `activitypub/mod.rs`;
  `packers.rs` keeps `pub use` re-exports so all Misskey-side/test imports work.
- Grep: zero `api::misskey` imports outside `src/api` (sole hit is a pre-existing
  `//!` doc comment in `webfinger.rs:4`, allowed). Test `ApiState`
  constructions in webfinger/objects replaced with `Arc<AppServices>`.
- Verified: full `cargo test` green — 108 lib + 35 conformance (+1 client-smoke,
  2 crash-recovery, 2 federation, 1 interop, 1 minrepro), 0 failed.
