# 2026-10-21 gaps final wiring 1/5: ports traits + PollEnded

First of the parent-merge slices. Subagent `ses_e9fb3b40dffeQqIm2VCVFWLTUq`
(ports only); parent reviewed, style-audited, and commits. Worker/redb/
endpoints/docs agents still running against these committed traits.

## What landed (all additive, zero deletions)

- `JobKind::{TranscodeMedia, WebhookDeliver, EmailSend}` with payload +
  idempotency-key docs.
- Moved verbatim into ports: `DriveFolderStore` (10 methods),
  `SystemWebhookStore`, `UserWebhookStore`,
  `AbuseReportRecipientStore`.
- `FollowStore::update_follow_options` (default `StorageUnavailable`) +
  `follow_requests_from` (default empty).
- New required traits: `UserMemoStore`, `NoteThreadMuteStore`
  (idempotent-bool semantics), `ActivityIndex` (durable Undo-by-ID).
- Defaults added: `ClipStore::clip_ids_for_note` (empty),
  `JobQueue::has_pending_with_key` (false),
  `MediaStore::count_drive_files` (`StorageUnavailable`).
- `NotificationKind::PollEnded` + `"pollEnded"` wire mapping.
- Reconciliation: WP3 email-recipient store vs WP2c admin recipient
  record are different shapes/consumers — kept separate (no merge).
- `ModerationAction::ResetPassword` NOT added (lives in
  `domain/moderation.rs`; needs its `as_str`/`parse` + test table —
  folded into the worker/endpoints slice).

## Deliberately deferred

- `AppServices` fields: ~70 struct literals repo-wide; adding 7
  fields now would churn sibling-owned files mid-flight. Ops take
  explicit `&dyn Store` params, so endpoints/worker wire without them;
  fields land with the redb slice (same commit updates all literals).
- `ChannelStore`/`ChatStore`/`GalleryStore`/`AnnouncementStore`:
  out of ports per SKIP order (ops work via explicit params).

## Verification

- `cargo check --offline --lib`: exactly 2 errors, both
  sibling-pending `worker.rs` E0004s (3 JobKind arms + PollEnded
  notify) — the worker agent's scope. Nothing attributable to this
  slice.
- Style: 265+/5+ lines, zero `unwrap`, zero >100-col new lines (9
  hits all pre-existing), one-module-per-`use`, `///` docs on every
  new item, no abbreviations/unsafe/todo.

## Style compliance note (per `rust_style_guide.html`)

`cargo fmt` unavailable in this container (no rustfmt on Win32, no
`cargo fmt` under WSL) — wrapping kept manual at 100 cols throughout
the final wiring; the full gate re-checks.
