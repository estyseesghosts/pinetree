# Glossary

Status: living page, as of 2026-10-20. Each entry is one plain sentence. Terms are alphabetical within their group.

## Social network words

- **Account:** a person's identity on a server, with a name, profile, and posts.
- **Antenna:** a filter you set up that collects matching posts into its own feed.
- **Boost (renote):** sharing someone else's post to your followers, so it shows in their timelines.
- **Clip:** a saved collection of posts you have chosen, which you can share.
- **Content warning (CW):** a note placed before a post's text that hides it until the reader chooses to see it.
- **Drive:** a person's stored files and pictures.
- **Favorite:** a private bookmark on a post.
- **Local only:** a post that is never sent to other servers.
- **Mention:** naming another account in a post so they are notified.
- **Mute:** hiding another account's posts from your view without them knowing.
- **Note (or post):** one thing written on the network; Misskey calls it a note.
- **Page:** a longer document an account publishes, similar to an article.
- **Poll:** a post that asks a question with options people can vote on.
- **Reaction:** a quick emoji reply to a post.
- **Reply thread:** a chain of posts that answer each other.
- **Timeline:** a list of posts you see, such as home, local, or global.
- **Visibility:** who may see a post: public, home only, followers only, or specific people.
- **Word mute:** hiding posts that contain a word you chose, enforced by the server.

## Federation and the wider network

- **ActivityPub:** the shared rulebook servers use to send posts, follows, and other actions to each other.
- **Actor:** the ActivityPub name for an account, with its address, keys, and inbox.
- **Federation:** servers exchanging posts and follows with each other.
- **Fediverse:** the whole network of independent servers that federate.
- **Inbox:** the address where a server receives messages from others; a shared inbox serves many accounts at once.
- **Outbox:** the list of things an account has sent out.
- **Relay:** a shared forwarding server that passes posts between many servers; not built yet.
- **Remote media:** pictures or files that arrive from another server.
- **WebFinger:** a standard way for one server to look up an account on another by its address.
- **NodeInfo:** a standard page where a server describes what software it runs and what it supports.
- **JRD and XRD:** the two data formats WebFinger answers come in; one is JSON, one is XML.
- **HTTP signature:** a tamper-proof stamp added to each message so the receiver can check who sent it.
- **Digest:** a short fingerprint of a message body, checked together with the signature.
- **Tombstone:** a marker left where a deleted post was, so other servers know it is gone.
- **FEP-044f:** the agreed standard for quote posts across the fediverse.
- **Quote post:** a post that shows and comments on another post.
- **Unsafe internal address attack:** an attack that tricks a server into fetching something from its own private network; pinetree refuses those addresses.
- **Deduplication:** remembering which incoming messages were already handled, so a repeat does nothing.

## Software names

- **Misskey:** a popular social network server, the model pinetree is compatible with.
- **Sharkey:** a fork of Misskey with extra features; pinetree tests against it.
- **Mastodon, Pleroma, Akkoma:** other fediverse server programs.
- **Rust:** the programming language pinetree is written in.
- **MiAuth:** Misskey's login handshake, where an app asks the user for permission and receives a key.
- **MFM:** Misskey's text formatting language, a bit like markdown with extras.
- **Misskey API:** the set of requests Misskey apps send to a server and the answers they expect.

## Apps and interfaces

- **Streaming:** the live connection a server keeps open to push events to an app.
- **WebSocket:** the technology behind a streaming connection, a two-way open line.
- **Channel (streaming):** one live feed an app subscribes to, such as home timeline.
- **Access token:** a secret key an app stores after sign-in, which proves who it is acting for.
- **Permission:** a named right an app asks for, such as "post" or "read my follows".
- **Rate limit:** a cap on how often a caller may ask, with a "try again later" answer.
- **Pagination cursor:** a marker that says where to continue reading a long list.
- **Error envelope:** the standard shape of an error answer: a message and a code.

## How the server works inside

- **Phase:** one stage of the build plan, each with its own goals and checklist.
- **Contract:** a document that fixes what is promised (behaviour, data shapes, rules).
- **Conformance test:** a test that checks a storage backend does what the contract says.
- **Database (redb):** the single-file storage pinetree uses today on one machine.
- **FoundationDB:** a database built to run across many machines; planned for Phase 4.
- **Blob store:** the place where big files such as pictures are kept, outside the database.
- **Node:** one machine running pinetree.
- **Multi-node:** many nodes acting as one server; planned for Phase 4.
- **Shard:** a slice of the data that could live on a different machine; pinetree does not split data this way yet.
- **Cluster mode:** the setting for multi-node operation; refused until Phase 4.
- **Job:** a piece of background work, such as delivering a post, saved so it can be retried.
- **Outbox delivery:** sending a saved message to other servers, retrying if they are unreachable.
- **Lease:** a temporary claim on a job, so only one worker does it at a time and a crashed worker's claim expires.
- **Idempotent:** safe to do twice, so a repeat gives the same result as one attempt.
- **Quarantine:** setting aside a job that failed for good, with a logged reason, instead of retrying forever.
- **Backoff:** waiting longer and longer between retries.
- **Transaction:** a group of changes that either all happen or none do.
- **Atomic:** done as one transaction, so no half-finished state is ever visible.
- **Crash recovery:** restarting after a crash and finishing what was in progress, without doubling anything.
- **Migration:** a planned change of stored data from one format or place to another.
- **Interop test:** a test that runs pinetree against real other software and checks it works together.
- **Live lab:** a local set of real Misskey, Sharkey and pinetree servers used for interop tests.
- **Smoke test:** a quick end-to-end check that the main paths still work.
- **Status (Draft, Frozen, Tracking):** explained on [How to read the docs](How-To-Read-The-Docs.md).

## Read more

- [About pinetree](About.md)
- [Current state](Current-State.md)
- [How to read the docs](How-To-Read-The-Docs.md)
