# Roadmap

Status: living page, as of 2026-10-20. Source of the phase plan: the [product proposal](../../sns-server-proposal.md) §9. The proposal wins on any disagreement. Items here are summaries; the full list is in [TODOS](../TODOS.md).

## Where we are

| Phase or batch | What it was for | Status |
|---|---|---|
| 0 Contracts | Agree the rules: API, data shapes, identities, visibility | Done |
| 1 Single-machine server | Sign up, post, timelines, follow, react, on one machine | Done |
| 2 Talking to other servers | Follow and post across servers; retry failed deliveries | Done (tested with real Misskey and Sharkey) |
| 2.5 and 2.6 Cleanup and hardening | Tidy the code; stronger checks on incoming messages | Done |
| 3 App compatibility and live updates | Apps and live channels work for the agreed feature list | Done 2026-10-16 |
| Quote posts | Quote another post, with approval | Done 2026-10-16 |
| Reply threads | Correct replies, including replies whose parent is not known yet | Done 2026-10-16 |
| Batch N | Favorites, custom emoji, antennas, abuse reports, more | Done 2026-10-20 |
| Batches O and P | User lists, pull timelines, email identity, account move, and more | Shipped per the 2026-10-20 inventory; re-check pending |
| Federated clips and pages | Share clips and pages with other servers | Phase 2 shipped; federation switches need a check |
| **4 Many machines as one server** | Run one server across several machines, with the database on FoundationDB | **Planned. Not started in code** |
| **5 Scale and hardening** | Measure, speed up, and make it survive machine failures | **Planned** |

## What is missing, and when

"When" is the planned phase. No dates are set beyond the ones above.

| What | Plain meaning | When |
|---|---|---|
| Many machines as one server | Several machines share one online identity, so a person's account lives on one of them and the rest cope | Phase 4 |
| FoundationDB database | A database built to run across many machines, instead of the single-file database used now | Phase 4 |
| Moving data between the two databases | A safe way to move an existing server's data to the new database | Phase 4 |
| Shared file storage | Pictures and files kept in shared storage, not only on one disk | Phase 4 |
| Safe multi-machine job handling | Background jobs run by several machines without doing the same work twice | Phase 4 (designed, not tested) |
| Cleanup of old remote pictures | Pictures and files from other servers are kept only as long as needed, with a size cap | Planned, not dated. The order is in the [remote media cache plan](../remote-media-cache-plan.md): first safe deletion, then size caps and removal, then fetch-on-demand, then streaming large files to disk, then optional pre-loading |
| Backups | Copy the data safely and restore it | Not dated |
| Health and metrics | A real health check, and counters that show how the server is doing | Not dated |
| Live channels for hashtags, drive, clips, admin | Live updates for these areas | Not dated; see [Feature matrix](Feature-Matrix.md) |
| Folders, per-follow settings, bulk file tools | Features apps expect that are not built yet | Not dated |
| Federation follow-ups | Apply edits that arrive in a short form, read unusual poll shapes, outgoing emoji reactions | Ongoing; listed in TODOS |
| Performance targets | Measured speed limits for one machine and for many machines | Phase 5 |

## Won't do for now

These are left out on purpose. Each could change only by a deliberate decision. See [Intentional differences](Intentional-Differences.md) for the reasons.

- **Chat** (direct messages between people in groups).
- **Channels** (community spaces with their own timelines).
- **Drive folders.**
- **Hashtag trends** (lists of what is trending).
- **Announcements** (server-wide notices). Note that the unread-badge problem they cause is still open in TODOS.
- **Games** (for example, Reversi).
- **Mastodon's own app interface.**
- **Relays** (shared forwarding servers).

## Read more

- [Full TODO list](../TODOS.md): every open item, with evidence.
- [Current state](Current-State.md) and [Feature matrix](Feature-Matrix.md).
- [Phase 3 exit criteria](../phase-3-exit-criteria.md) and [phase 2 exit criteria](../phase-2-exit-criteria.md): the checklists that closed those phases.
