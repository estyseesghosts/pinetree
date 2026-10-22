# Current state

Status: living page, as of 2026-10-20. Built from the code inventory [`inventory-pinetree-vs-misskey-2026-10-20.md`](../inventory-pinetree-vs-misskey-2026-10-20.md), which reads the source code directly. Where an older document disagrees, the inventory wins. Disagreements are listed under [Known doubts](#known-doubts).

Meaning of the words used: **works** means the feature runs and has been checked. **partly** means some of it works, with limits noted. **not yet** means planned or missing. **won't do (for now)** means left out on purpose. A short plain table is in [Feature matrix](Feature-Matrix.md).

## Phases in one line each

- Phase 0 (contracts) and Phase 1 (single-machine server): done.
- Phase 2 (talking to other servers): done, tested live with Misskey and Sharkey in a local lab.
- Phase 3 (app compatibility and live updates): done on 2026-10-16.
- Quote posts and reply threads: done on 2026-10-16.
- A batch of everyday features (favorites, custom emoji, antennas, abuse reports, and more): done on 2026-10-20.
- Phase 4 (many machines as one server, on FoundationDB): not started in code. Empty placeholders only.

[Phase](Glossary.md) and other terms are explained in the [Glossary](Glossary.md).

## Accounts and signing in

- **Sign up and sign in: works.** Passwords are stored in a scrambled form that is hard to reverse.
- **Sign in from a Misskey app (MiAuth): works.** Requests have stricter checks than a basic flow: a time limit on the login handshake and an explicit list of permissions the app asks for.
- **Access tokens with permissions, and revoking them: works.**
- **Editing your profile: partly.** Name, bio, location, birthday, avatar, banner and flags work. Some profile fields, decorations and mute settings are not included yet.
- **Suspending and deleting accounts: works.**
- **Moving an account to a new address: works** between pinetree servers. The account keeps its ID.
- **Two-factor sign-in, passkeys, and listing apps: not yet.**

## Posting

- **Writing posts: works.** Text, content warnings, visibility (public, home only, followers only, specific people), replies, boosts, attached files, and "local only" posts.
- **Showing and deleting posts: works.** A deleted post leaves a marker so other servers know it is gone.
- **Editing posts: works,** keeping an edit history. This is a Sharkey-style feature; the Misskey row in the [Feature matrix](Feature-Matrix.md) is not confirmed.
- **Drafts and scheduled posts: works.**
- **Emoji reactions: works,** including custom emoji. Custom emoji from other servers are opt-in.
- **Boosts (renotes), and undoing them: works.**
- **Quote posts: works.** A quoted post needs approval. Public quotes are approved automatically; a manual-approval option is not offered yet. See [Quote posts](../quote-posts.md).
- **Reply threads: works.** A reply whose parent post is not known yet is kept hidden until the parent is found. This is on purpose, so nothing is shown that might be unsafe. See [Replies](../replies.md).
- **Polls: works** for creating, voting and closing. Polls that arrive from other servers in unusual shapes may show up without the poll. See [Federation](#talking-to-other-servers).
- **Favorites and pinned posts: works.**
- **Word mutes: works,** enforced by the server, not just by the app. See [word mutes](../word-mutes.md).

## Following and blocking

- **Following and unfollowing: works.** Follower and following lists work.
- **Locked accounts: works.** Follow requests can be listed, accepted, rejected, or cancelled.
- **Blocking, muting, and muting boosts: works.**
- **Per-follow settings and a list of requests you sent: not yet.**

## Timelines and search

- **Home, local, hybrid and global timelines: works,** read a page at a time.
- **A timeline of one of your user lists: works.**
- **Hashtag timelines: won't do (for now).** Looking up one exact hashtag works. Trending hashtags do not.
- **Mentions: works.**
- **Search for posts and people: partly.** It matches part of a word, and only looks at a recent window of posts kept in memory.
- **Recommendations: partly.** A simple scoring method, not a learned one.

## Notifications

- **Notification list, grouped list, and marking as read: works.**
- **Live notifications: partly.** The main live channel sends the most common events, but not all of them (see [Streaming](#live-updates-streaming)).

## Lists, antennas, clips, and pages

- **User lists: works.** Add and remove members and read a timeline of the list. Remote accounts as list members are still an open decision.
- **Antennas: works.** An antenna is a filter you set up; posts that match it collect into its own feed. There is a cap on how many you can have.
- **Clips (your saved collections of posts): works** on the local server. Sharing them with other servers is partly done.
- **Pages: works** on the local server. Sharing pages with other servers is partly done; see [federated collections](../federated-collections.md).
- **Channels, galleries, and flash-style posts: won't do (for now).**

## Drive (files and pictures)

- **Uploading files, from your device or from a web address: works.** Duplicate files are recognised.
- **Listing, viewing, editing and deleting files: works.**
- **Folders: won't do (for now).** Files sit in one flat list. Apps that expect folders will show them as missing.
- **Finding or moving many files at once, and thumbnails: not yet.**
- **Pictures and files sent from other servers: partly.** They are checked for size and type, and fetched safely. They are never cleaned up yet, so disk use only grows. The planned fix is in the [remote media cache plan](../remote-media-cache-plan.md).
- **Serving files: works.** Safe picture and video types show in the browser; anything else is offered as a download.

## Live updates (streaming)

Streaming means the server pushes new posts and events to an app as they happen, instead of the app asking again and again.

- **Live home, local, hybrid and global timelines: works.**
- **Live user list and antenna feeds: works.**
- **Live note updates (new reactions, deletions, poll votes, edits): works.**
- **Live main channel (notifications, follows, and similar events): partly.** Many event types are not sent yet.
- **Live channels for hashtags, drive, clips, admin and stats: not yet.**
- **Live channels for chat, community channels, and games: won't do (for now).**
- **Connection limits and idle cut-off: partly.** Built, but not yet tested under live load.

## Talking to other servers

- **Being found by other servers (WebFinger, NodeInfo): works.**
- **Checking the identity of remote accounts and their signatures: works.** Messages with a bad signature are refused.
- **Following, accepting, rejecting and undoing follows: works,** tested live with real Misskey and Sharkey.
- **Posts, edits and deletions: works.** Edits that arrive without the full post are not applied yet. See [Known doubts](#known-doubts).
- **Reactions (likes): works.** Servers in the Mastodon family receive a plain "like" instead of an emoji reaction.
- **Boosts and quote posts: works.**
- **Polls from other servers: partly.** See Posting above.
- **Blocks, reports and account moves: works.**
- **Real strangers' servers on the open internet: partly.** Tests ran on a local lab only.
- **Mastodon and Pleroma/Akkoma: partly.** Exchanges have been seen in test runs. The full test set runs against Misskey and Sharkey.
- **Newer message signature standard (RFC 9421): works (single-node).** rsa-v1_5-sha256 profile, permissive default + enforce opt-in; `tests/federation_sig_mode_e5.rs` green; live interop pending.
- **Built-in sign-in through other apps (OAuth): not yet,** only if an app needs it.
- **Mastodon's own app interface: won't do.** See [Intentional differences](Intentional-Differences.md).
- **Relays (shared forwarding servers): won't do (for now).**

Safety for incoming messages from other servers: they are size-limited, time-limited, redirect-limited, checked for unsafe internal addresses, and checked for valid signatures. This is done for every incoming message. Details: [Glossary](Glossary.md).

## Running it

- **Running on a single machine: works.** Uses one data file.
- **Settings file: works** for most settings. A few settings are still only set by environment variables or command-line flags.
- **Limits on how often each app can call (rate limits): works,** answering "slow down, try again later" in the same way Misskey does.
- **Error answers: works.** Unknown addresses get "not found"; unknown fields get a clear error.
- **Health check: works.** `/healthz` liveness + `/ready` bounded readiness (redb read + storage roots); Docker probes `/ready`.
- **Logs and metrics: works (single-node).** Structured levels + correlation IDs + redaction; admin status/diagnostics with honest nulls until ports expose aggregates.
- **Backups: works (single-node).** Versioned ZIP online snapshot + offline restore drill.
- **Many machines as one server, on FoundationDB: not yet.** See [Roadmap](Roadmap.md).

## What is missing, in one list

For the full list with evidence, see [TODOS](../TODOS.md). The big items are: many-machine operation, shared file storage, cleanup of old remote pictures, health and metrics, and the client features listed as not yet above.

## Known doubts

Where sources disagree, this page follows the 2026-10-20 inventory. These are the points that are still not settled:

1. **The older feature matrix (dated 2026-10-16) said polls, mutes and blocks, note editing, and admin roles were missing.** The inventory shows them working, at least in part. The [Feature matrix](Feature-Matrix.md) here follows the inventory.
2. **The [Roadmap](Roadmap.md) table only lists Batch N as shipped.** The inventory and TODOS also show Batches O and P as shipped. A spot check on 2026-10-20 found their endpoints in the code, but each feature has not been individually re-tested. This page treats them as shipped, not yet re-verified.
3. **Federating clips and pages:** resolved in the code. Both federation switches default to OFF (`src/domain/instance.rs:128-130`), so nothing federates until an admin turns them on. One recent commit message says pages federate by default; the code does not match that message, and the code wins.
4. **Announcements:** the inventory lists them as out of scope, but TODOS lists the missing "badge never clears" problem as open. Both are true; the feature is a won't-do-for-now with a visible gap.
5. **Profile editing:** the inventory says the full set of profile fields is there; this page earlier listed a subset. Marked partly.
6. **Public-internet federation:** not tested. All federation tests are on a local lab.
7. **Mastodon-family servers:** the test set is Misskey and Sharkey. Mastodon fixtures are still pending.
8. **Quote approvals:** approval is automatic for public and home-visible quotes. There is no manual review queue yet, so the setting is not offered to users.

## Read more

- [Feature matrix](Feature-Matrix.md) and [Roadmap](Roadmap.md) in this wiki.
- Contract detail: [endpoint manifest](../endpoint-manifest.md), [Phase 3 profile](../phase-3-profile.md), [quote posts](../quote-posts.md), [replies](../replies.md), [word mutes](../word-mutes.md), [federated collections](../federated-collections.md).
- Inventory: [inventory 2026-10-20](../inventory-pinetree-vs-misskey-2026-10-20.md).
