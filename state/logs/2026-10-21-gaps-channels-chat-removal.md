# 2026-10-21 gaps: channels/chat code removed (owner order)

Supersedes `state/logs/2026-10-21-gaps-wp2b.md` for the channels/chat
families (gallery/flash/hashtags/announcements parts of that log stand).

## Reason

Owner decision: Misskey's channels/chat implementations conflict with
pinetree's intended direction. Removal is mandatory, not deferral. Both
families stay out of scope: REST keeps 404ing (`UNKNOWN_API_ENDPOINT`
only for truly-unknown still holds — these are now truly-unknown),
streaming stays `UNSUPPORTED_CHANNEL` (already the documented decision).

## What was removed

- `src/application/commands/channels.rs` (ChannelStore seam, 14 ops)
- `src/application/commands/chat.rs` (ChatStore seam, 19 ops)
- `src/domain/channel.rs`, `src/domain/chat_room.rs`
- `tests/gaps_api_channels.rs`, `tests/gaps_api_chat.rs`
- `pub mod` lines in `src/application/commands/mod.rs`,
  `src/domain/mod.rs`
- `docs/gaps.md` scope lines updated (non-goals + WP2 strike-through).

## What was kept (relocated, not removed)

- `IdCursor` pager (generic `since_id/until_id/limit`) moved verbatim
  into `src/application/commands/gallery.rs` (its remaining consumer;
  chat/channels were the only other users). Import updated in
  `tests/gaps_api_gallery.rs`. Verified no other references to the
  removed modules remain (`grep` for `channels::|chat::|channel::|
  chat_room::` clean).

## Verification

- Removal introduces zero new compile errors: `cargo check --offline
  --lib` shows only the 2 pre-existing sibling-pending E0004s (ports
  agent's new `JobKind::{TranscodeMedia,WebhookDeliver,EmailSend}` +
  `NotificationKind::PollEnded` variants awaiting worker arms — not
  mine).
- Gallery/hashtags/announcements suites re-run deferred to the final
  gate (lib currently blocked by the sibling E0004s); the gallery
  change is a verbatim relocation + import swap, and those suites were
  3/3 + 3/3 + 2/2 green before removal.

## Style compliance (per `rust_style_guide.html`)

Relocated `IdCursor` keeps its `///` docs, `Default` impl, `new`-style
`with_limit` constructor (naming-convention compliant); no line exceeds
100 cols; no new imports beyond the same module.
