//! Profile import/export (§13; Misskey `i/export-*` + `i/import-*` parity).
//!
//! Reference pins: Misskey `2026.10.1-alpha.0` (`./misskey`). Verified this
//! session against `i/export-following|blocking|mute|notes|antennas|clips|
//! favorites|user-lists.ts`, `i/import-blocking|muting|following|antennas|
//! user-lists.ts`, `QueueService.ts:322-587` (`createExport*`/`createImport*`
//! job shapes), and every `Export*`/`Import*ProcessorService.ts`.
//!
//! Shape of the slice:
//! - The endpoint arms enqueue one [`JobKind::Export`]/[`JobKind::Import`]
//!   job (ids only — never file content, per the 64KiB job-payload cap) and
//!   answer `204 No Content` immediately (upstream returns void too).
//! - The worker runs [`AppServices::run_export_job`]/[`run_import_job`].
//!   Export completion fans out through `Disposition::Fanout`, so the export
//!   job's completion and the `exportCompleted` `Notify` enqueue commit in
//!   ONE storage transaction (`complete_with_followups`).
//! - Exports land in the caller's drive via the existing `upload_media`
//!   path; imports read the file back via media metadata + `blobs.get`.
//!
//! Divergences (all documented here and in the endpoint manifest §20):
//! - `excludeInactive` is accepted and ignored: pinetree tracks no
//!   last-active timestamp, and a zero-notes heuristic would mislabel
//!   lurkers as inactive.
//! - Local accounts export as a bare `username` (upstream
//!   `getFullApAccount` always appends `@host`, even for locals); imports
//!   accept both the bare and the `user@host` form.
//! - Remote users are resolved from the local account store only: pinetree
//!   has no remote-user resolver, so unresolvable rows are skipped (upstream
//!   fetches them over federation).
//! - User lists have a subsystem and export/import real rows. Clips have
//!   one too (federated clips + pages handoff, Phase 1): `export-clips`
//!   writes the upstream `serializeClip` shape, and the pinetree-only
//!   `i/import-clips` consumes that same shape (find-or-create by name;
//!   upstream has no import-clips endpoint). Favorites (Batch N WS2) and
//!   antennas (Batch N WS4) have subsystems and export/import real rows.
//! - Drive upload dedups on content (`force == false`, unlike upstream
//!   `force: true`) so a crashed-and-retried export converges on the same
//!   file instead of orphaning a duplicate.

mod antennas;
mod blocking;
mod build;
mod clips;
mod common;
mod favorites;
mod following;
mod jobs;
mod muting;
mod notes;
mod user_lists;

pub use common::IMPORT_MAX_FILE_BYTES;
