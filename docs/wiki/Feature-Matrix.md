# Feature matrix

Status: living page, as of 2026-10-20. Compares pinetree with the features a Misskey-style app expects. Based on the 2026-10-20 code inventory ([inventory](../inventory-pinetree-vs-misskey-2026-10-20.md)). Where that inventory is uncertain, the row says so.

Key:

- **WORKS:** runs and has been checked.
- **PARTLY:** some of it works; the notes say what is missing.
- **NOT YET:** planned or missing; not a deliberate choice.
- **WON'T DO (intentional):** left out on purpose. See [Intentional differences](Intentional-Differences.md).

| Feature | Works? | Notes (plain) | Detail doc |
|---|---|---|---|
| **Accounts** | | | |
| Sign up and sign in | WORKS | Passwords stored in a hard-to-reverse scrambled form | [endpoint manifest](../endpoint-manifest.md) |
| Sign in from a Misskey app (MiAuth) | WORKS | Stricter than basic: short time limit, must name permissions | [endpoint manifest](../endpoint-manifest.md) |
| Access tokens and revoking them | WORKS | Each token has permissions | [endpoint manifest](../endpoint-manifest.md) |
| Edit your profile | PARTLY | Name, bio, location, birthday, pictures, flags. Not every profile field yet | [endpoint manifest](../endpoint-manifest.md) |
| Two-factor sign-in, passkeys, app list | NOT YET | Missing | [TODOS](../TODOS.md) |
| Suspend and delete accounts | WORKS | | [endpoint manifest](../endpoint-manifest.md) |
| Move account to a new address | WORKS | Tested between pinetree servers; account keeps its ID | [identity rules](../identity-rules.md) |
| Admin tools (reports, suspend, invites) | PARTLY | Reports, suspend and invites work. Roles and many admin screens are missing | [endpoint manifest](../endpoint-manifest.md) |
| **Posting** | | | |
| Write posts (text, warnings, visibility, replies, boosts, files, local-only) | WORKS | Visibility: public, home, followers, specific people | [visibility policy](../visibility-policy.md) |
| Show and delete posts | WORKS | Deleted posts leave a "deleted" marker | [domain types](../domain-types.md) |
| Edit posts | WORKS | Keeps edit history. A Sharkey-style feature; Misskey row not confirmed | [endpoint manifest](../endpoint-manifest.md) |
| Drafts and scheduled posts | WORKS | | [endpoint manifest](../endpoint-manifest.md) |
| Emoji reactions | WORKS | Custom emoji included; remote custom emoji are opt-in | [endpoint manifest](../endpoint-manifest.md) |
| Boosts (renotes) and undo | WORKS | | [endpoint manifest](../endpoint-manifest.md) |
| Quote posts | WORKS | Public quotes approved automatically; manual approval not offered yet | [quote posts](../quote-posts.md) |
| Reply threads | WORKS | Replies to unknown parents stay hidden until the parent is found | [replies](../replies.md) |
| Polls (create, vote, close) | WORKS | Polls from other servers in unusual shapes may arrive without the poll | [endpoint manifest](../endpoint-manifest.md) |
| Favorites and pinned posts | WORKS | | [endpoint manifest](../endpoint-manifest.md) |
| Word mutes | WORKS | Enforced by the server, not only by the app | [word mutes](../word-mutes.md) |
| **Following and blocking** | | | |
| Follow and unfollow; follower lists | WORKS | | [endpoint manifest](../endpoint-manifest.md) |
| Follow requests (locked accounts) | WORKS | List, accept, reject, cancel | [endpoint manifest](../endpoint-manifest.md) |
| Block, mute, mute boosts | WORKS | | [endpoint manifest](../endpoint-manifest.md) |
| Per-follow settings; list of sent requests | NOT YET | | [TODOS](../TODOS.md) |
| **Timelines and search** | | | |
| Home, local, hybrid, global timelines | WORKS | Read a page at a time | [endpoint manifest](../endpoint-manifest.md) |
| User-list timeline | WORKS | | [endpoint manifest](../endpoint-manifest.md) |
| Mentions | WORKS | | [endpoint manifest](../endpoint-manifest.md) |
| Hashtag timeline and trends | WON'T DO (intentional) | Exact-tag lookup works; trends and hashtag pages do not | [Intentional differences](Intentional-Differences.md) |
| Search posts and people | PARTLY | Matches part of a word; searches a recent in-memory window | [endpoint manifest](../endpoint-manifest.md) |
| Recommendations | PARTLY | Simple scoring, not learned | [endpoint manifest](../endpoint-manifest.md) |
| **Notifications** | | | |
| Notification list, grouped list, mark as read | WORKS | | [endpoint manifest](../endpoint-manifest.md) |
| Live notifications | PARTLY | Most common events sent; not all | [endpoint manifest](../endpoint-manifest.md) |
| **Lists, antennas, clips, pages** | | | |
| User lists | WORKS | Remote accounts as members: open decision | [endpoint manifest](../endpoint-manifest.md) |
| Antennas (filtered feeds) | WORKS | Capped in number | [endpoint manifest](../endpoint-manifest.md) |
| Clips (saved collections) | WORKS | Local. Sharing with other servers: partly | [federated collections](../federated-collections.md) |
| Pages | WORKS | Local. Sharing with other servers: partly | [federated collections](../federated-collections.md) |
| Channels, galleries, flash posts | WON'T DO (intentional) | | [Intentional differences](Intentional-Differences.md) |
| **Drive (files and pictures)** | | | |
| Upload files (device or web address) | WORKS | Duplicates recognised | [endpoint manifest](../endpoint-manifest.md) |
| List, view, edit, delete files | WORKS | | [endpoint manifest](../endpoint-manifest.md) |
| Folders | WON'T DO (intentional) | Files sit in one flat list | [Intentional differences](Intentional-Differences.md) |
| Find or move many files; thumbnails | NOT YET | | [TODOS](../TODOS.md) |
| Pictures from other servers | PARTLY | Checked and fetched safely; never cleaned up yet | [remote media cache plan](../remote-media-cache-plan.md) |
| Serving files | WORKS | Safe types show inline; others download | [endpoint manifest](../endpoint-manifest.md) |
| **Live updates (streaming)** | | | |
| Live home, local, hybrid, global timelines | WORKS | | [endpoint manifest](../endpoint-manifest.md) |
| Live user list and antenna feeds | WORKS | | [endpoint manifest](../endpoint-manifest.md) |
| Live note updates (reactions, deletes, votes, edits) | WORKS | | [endpoint manifest](../endpoint-manifest.md) |
| Live main channel (all event types) | PARTLY | Many event types not sent yet | [endpoint manifest](../endpoint-manifest.md) |
| Live hashtag, drive, clip, admin, stats channels | NOT YET | Answered with "channel not supported" | [TODOS](../TODOS.md) |
| Live chat, community channel, games | WON'T DO (intentional) | | [Intentional differences](Intentional-Differences.md) |
| **Talking to other servers** | | | |
| Being found (WebFinger, NodeInfo) | WORKS | | [ActivityPub collections](../activitypub-collections.md) |
| Remote identity and signature checks | WORKS | Messages with bad signatures refused | [identity rules](../identity-rules.md) |
| Follow, accept, reject, undo | WORKS | Live-tested with Misskey and Sharkey (local lab) | [endpoint manifest](../endpoint-manifest.md) |
| Posts, edits, deletions | WORKS | Edits without the full post not applied yet | [TODOS](../TODOS.md) |
| Reactions (likes) | WORKS | Mastodon-family servers get a plain like | [Intentional differences](Intentional-Differences.md) |
| Boosts and quote posts | WORKS | | [quote posts](../quote-posts.md) |
| Polls from other servers | PARTLY | Unusual poll shapes arrive without the poll | [TODOS](../TODOS.md) |
| Blocks, reports, account moves | WORKS | | [endpoint manifest](../endpoint-manifest.md) |
| Open internet with strangers' servers | PARTLY | Only tested on a local lab | [testing guide](../testing.md) |
| Mastodon, Pleroma, Akkoma | PARTLY | Exchanges seen in test runs; full tests are Misskey and Sharkey | [testing guide](../testing.md) |
| RFC 9421 message signatures | NOT YET | | [TODOS](../TODOS.md) |
| OAuth sign-in | NOT YET | Only if an app needs it | [Roadmap](Roadmap.md) |
| Mastodon's own app interface | WON'T DO (intentional) | | [Intentional differences](Intentional-Differences.md) |
| Relays | WON'T DO (intentional, for now) | | [Roadmap](Roadmap.md) |
| **Running it** | | | |
| Single machine, one data file | WORKS | | [phase 1 exit](../phase-1-exit-criteria.md) |
| Settings file | WORKS | A few settings still only by environment variable or flag | [TODOS](../TODOS.md) |
| Rate limits | WORKS | Same "try again later" answer as Misskey | [endpoint manifest](../endpoint-manifest.md) |
| Error answers | WORKS | Unknown address: not found; unknown field: clear error | [endpoint manifest](../endpoint-manifest.md) |
| Health check | PARTLY | Always answers "ok"; does not check the database | [TODOS](../TODOS.md) |
| Logs and metrics | PARTLY | Basic logs; no metrics | [TODOS](../TODOS.md) |
| Backups | NOT YET | | [TODOS](../TODOS.md) |
| Many machines as one server (FoundationDB) | NOT YET | Phase 4; placeholders only | [Roadmap](Roadmap.md) |

## Read more

- [Current state](Current-State.md): the same facts in sentences, with the open doubts.
- [Roadmap](Roadmap.md): when the NOT YET rows are planned.
- [Full TODO list](../TODOS.md).
