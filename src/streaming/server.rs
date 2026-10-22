//! Authenticated WebSocket `/streaming` (Phase 3, profile §2).
//!
//! Wire protocol (misskey-js `Stream` parity):
//! - Upgrade: token from `?i=` or `Authorization: Bearer`. No token means
//!   anonymous (non-credential channels only). Bad token → HTTP 401,
//!   suspended user → 403, token without `read:account` → 401.
//! - Client→server JSON `{type, body}`: `connect {channel,id,params?}`,
//!   `disconnect {id}`, `channel`/`ch` (only `readNotification` is
//!   interpreted), top-level `readNotification`, `subNote`/`s`/`sr` `{id}`,
//!   `unsubNote`/`un` `{id}`. Anything else is ignored.
//! - Server→client: `channel {id,type,body}` for channel events,
//!   top-level `noteUpdated {id,type,body}` for capture events,
//!   `connected {id}` when `connect` carried `pong: true`, and
//!   `error {code: UNSUPPORTED_CHANNEL, channel, id}` for unknown channels.
//! - Channels (upstream `stream/channels/*` parity): `main`,
//!   `homeTimeline`, `localTimeline`, `hybridTimeline`, `globalTimeline`,
//!   `antenna` + `userList` (credential, owner-checked), `hashtag`
//!   (`q: string[][]`), `roleTimeline` (`roleId`), `drive` (credential),
//!   `serverStats` + `queueStats` (snapshot `stats` on connect, `statsLog`
//!   on `requestLog`), `admin` (credential + moderator), `clip`
//!   (`clipId`, `updated`/`deleted` with null bodies). `reversi`,
//!   `reversiGame`, `channel`, `chatUser`, `chatRoom` stay
//!   `UNSUPPORTED_CHANNEL` by decision (no channels/chat/Reversi model —
//!   the REST surfaces ship, the streams do not).
//! - Main events: every upstream `MainEventTypes` key is forwarded
//!   (`notification`, `mention`, `reply`, `renote`, `follow`, `followed`,
//!   `unfollow`, `meUpdated`, `readAllNotifications`,
//!   `notificationFlushed`, `unreadNotification`, `unreadAntenna`,
//!   `readAntenna`, `readAllAnnouncements`, `announcementCreated`,
//!   `myTokenRegenerated`, `signin`, `registryUpdated`, `driveFileCreated`,
//!   `urlUploadFinished`, `pageEvent`, `receiveFollowRequest`); producers
//!   live in the owning commands (see the WP4 report for the still-missing
//!   ones).
//! - DIVERGENCEs: `serverStats`/`queueStats` emit one snapshot on connect
//!   (+ `statsLog` on demand) instead of upstream's 1 s Xev broadcast
//!   (no in-process broadcast bus); `roleTimeline` accepts any `roleId`
//!   but no producer publishes to it (no role model); drive-folder bodies
//!   are `{id}` until the WP2 folder packer lands; `reacted.emoji` is
//!   `{name, url} | null` resolved from the emoji stores.
//! - Limits: 32 channels/connection, 1536 captured notes (LRU-evicts the
//!   oldest, like Misskey), 2 min idle reap with 60 s pings.
//!
//! Storage reads run on the blocking pool; the hub fan-out itself never
//! blocks. Timeline filtering mirrors the REST eligibility
//! (`note_eligible_for_channel`); capture uses the light gate
//! (`capture_gate`).

mod connection;
mod handshake;
mod subscribe;

pub use handshake::{streaming_handler, token_for_upgrade};
