# Proposal: A Modular, Distributed Misskey-Compatible SNS Server

**Status:** Draft proposal  
**Language:** Rust  
**Primary compatibility target:** Misskey client API and Misskey-style ActivityPub federation

## 1. Goals and non-goals

Build a lightweight social networking server that:

- Is divided into small, well-defined modules that can be extended without rewriting unrelated systems.
- Can run as a simple single-node instance or as one logical instance distributed across multiple servers.
- Supports Misskey clients through a deliberate, tested subset of the Misskey API, including streaming.
- Federates with ActivityPub software in a Misskey-compatible way.
- Avoids requiring distributed infrastructure for single-user and small-community deployments.

Explicit non-goals:

- Reimplementing every legacy Misskey feature or extension.
- Implementing the Mastodon client REST API. ActivityPub federation with Mastodon-compatible servers remains in scope.
- Starting as a collection of microservices.
- Requiring SQL or SQL-like storage, Redis, or a separate message broker.

## 2. Deployment and storage model

Use one storage backend per logical instance, selected by deployment mode:

| Deployment | Storage | Media | Background work |
|---|---|---|---|
| Single-node instance | **redb** | Local filesystem | Durable jobs in redb; worker runs in-process |
| Multi-node instance | **FoundationDB** | Shared S3-compatible object storage or equivalent | Durable jobs in FoundationDB; workers run on one or more nodes |

### redb for single-node deployments

redb is an embedded, pure-Rust key-value database with ACID transactions and concurrent readers. It has one active writer at a time, which is a reasonable trade-off for a compact, single-node installation. It is not the clustering mechanism and must not be shared among application nodes as a substitute for FoundationDB.

### FoundationDB for multi-node deployments

FoundationDB is required when a logical instance spans multiple application nodes. It provides distributed transactional storage, but it does **not** provide the application's account ownership directory, Misskey API routing, node-to-node application protocol, or federation behavior. Those remain explicit parts of this design.

FDB transactions must be short and small: current documented limits include a roughly five-second transaction lifetime and a 10 MB transaction-size limit. Never put media blobs or unbounded timeline fan-out into a transaction. Commit the core change and a small durable job/outbox record together; process fan-out and delivery in separate, retryable jobs.

### Backend boundary and migration

The domain and application layers must not know which backend is active. Define narrow storage interfaces around real use cases, including the ability to atomically commit a state change with its durable follow-up event. Both backends must pass the same persistence contract tests.

Do not pretend that switching from redb to FoundationDB is a transparent live upgrade. Provide an explicit export/import tool and document that changing deployment modes may require downtime. Preserve logical IDs, account keys, relationships, notes, and activity deduplication records during migration.

Do not build a generic database framework. Use the simplest interfaces that cover actual application operations, and allow each backend to use an appropriate internal schema.

## 3. Logical identity and node ownership

The public domain represents one logical SNS instance, for example `xyz`. Physical hosts such as `foo` and `bar` are internal implementation details.

Every local account has:

- A globally unique, immutable internal account ID.
- A globally unique local handle, such as `@a@xyz`.
- A canonical ActivityPub actor URL based on the immutable ID, such as `https://xyz/users/{account_id}`.
- One authoritative write owner when the instance is running in partitioned mode.

Usernames map to account IDs; they are not actor identities. Renaming a user or moving them between nodes must not change their canonical actor URL. Public actor, note, inbox, and media URLs must not expose physical node hostnames. Account moves must preserve the account ID, signing identity, content, relationships, and relevant federation state.

A cluster-wide account directory maps account IDs and normalized local handles to their current ownership information. It must enforce handle uniqueness across the whole logical instance. Ownership changes need an explicit migration and recovery procedure; nodes must not independently guess who owns an account.

### Request routing

Misskey API methods use shared endpoints such as `/api/notes/create`, so routing cannot be based only on URL paths.

1. Authenticate the request and resolve the account ID.
2. Determine the account's authoritative owner, or the owner of the requested resource.
3. Execute the operation at that owner, using an authenticated internal request where necessary.
4. Return the response through the public domain.

The same principle applies to actor inboxes, note lookups, streaming subscriptions, and other owner-specific operations. Cross-node interactions between local accounts are internal application operations or events; they must not be sent through ActivityPub to the instance's own public domain.

FoundationDB can make data changes across distributed key ranges transactionally. It does not replace this ownership and routing layer. Logical account ownership must also remain distinct from FoundationDB's own physical data placement.

## 4. Application architecture

Start as a **modular monolith** in one Rust package. Split modules into workspace crates only when reuse, test isolation, compile times, or dependency control gives a concrete reason.

Suggested structure:

```text
src/
  domain/
    account.rs
    note.rs
    follow.rs
    reaction.rs
    visibility.rs
    policy.rs
  application/
    commands.rs
    queries.rs
    events.rs
    ports.rs
  api/
    misskey/
      endpoints/
      auth.rs
      request.rs
      response.rs
      errors.rs
  streaming/
    connection.rs
    channels.rs
    subscriptions.rs
  activitypub/
    actor.rs
    objects.rs
    webfinger.rs
    signatures.rs
  federation/
    inbox.rs
    outbox.rs
    delivery.rs
    retry.rs
  cluster/
    account_directory.rs
    routing.rs
    internal_protocol.rs
  persistence/
    redb/
    foundationdb/
  jobs/
    queue.rs
    worker.rs
  media/
    ingest.rs
    storage.rs
    remote_cache.rs
  config.rs
  observability.rs
  main.rs
```

Dependency rules:

- **Domain** owns account, note, relationship, visibility, and permission rules. It must not depend on HTTP, Misskey response shapes, ActivityPub JSON, or a specific database.
- **Application** implements use cases and coordinates domain changes. It defines the interfaces it needs for persistence, jobs, and other external operations.
- **Adapters** implement those interfaces: Misskey API, ActivityPub, streaming, redb, FoundationDB, and media storage.
- The Misskey API and ActivityPub adapters call the same application operations. They must not implement separate versions of note creation, deletion, following, or reaction rules.

Avoid oversized service objects, giant endpoint files, and premature abstraction frameworks. Keep each module responsible for a small set of related behaviors.

## 5. Consistency, jobs, and timelines

A successful local operation must not be lost between committing state and scheduling its follow-up work.

For an operation such as creating a note, one storage transaction should:

1. Validate and save the state change.
2. Save a small durable event or outbox/job record.
3. Commit both atomically.

Workers then handle notifications, timeline updates, streaming fan-out, and federation delivery. Jobs must be idempotent, deduplicated where appropriate, retryable, and safe after process crashes. Delivery should normally be at-least-once with deduplication, not assume exactly-once network behavior. A temporary remote failure must not undo an already committed local post.

Keep transactions bounded. In FoundationDB, do not synchronously fan out a post to every follower as part of the note-creation transaction. Start with fan-out-on-read for timelines, then introduce a measured hybrid strategy or capped asynchronous fan-out if performance tests justify it. Avoid building an external broker into the first version; add one only if measured cross-node realtime or queue load requires it.

Media bytes live outside either database. Store media metadata, IDs, hashes, and references in the database. Local filesystem storage is suitable for a single-node instance; multi-node deployment needs shared object storage or an equivalent reliable arrangement.

## 6. Misskey client API compatibility

Treat compatibility as an explicit contract rather than a list of similarly named endpoints. Maintain a manifest for every supported endpoint that records:

- Request method, fields, defaults, and validation.
- Authentication and required permissions.
- Exact response fields, nullability, object nesting, and IDs.
- Error status and response shape.
- Streaming events and subscription behavior.
- Unsupported behavior and its documented response.

Implement a deliberately selected subset in this order:

1. **Core account and note flows:** instance metadata, MiAuth and token handling, account lookup, profile read/update, note create/show/delete, follows, and the timelines required by target clients.
2. **Streaming:** authenticated WebSocket connections, channel connect/disconnect, the required timeline and notification channels, and note capture / `noteUpdated` events when target clients need them.
3. **Common interactions:** replies, reactions, notifications, attachments, and remaining client-visible fields required by compatibility tests.
4. **Optional features:** polls, quotes, and further endpoints only when they are part of the chosen compatibility target.

Misskey's documented API uses JSON requests, typically POST-based endpoints, and accepts credentials through the `i` parameter or an Authorization bearer token. Its streaming API uses `/streaming?i={token}` with JSON channel messages. Match these behaviors instead of designing a new REST API and expecting existing clients to adapt.

Start with MiAuth and standard token handling; add OAuth support if the selected clients require it. Test against the actual Misskey Web/client versions intended to work, not merely internal mocks.

## 7. ActivityPub federation

Implement ActivityPub separately from the Misskey client API, sharing the same application operations and domain rules.

Initial protocol surface:

- WebFinger resolution of `acct:user@xyz` to a canonical actor URL.
- Stable actor documents, public keys, inbox/outbox references, and followers information.
- Actor inboxes and a shared inbox, where appropriate.
- Compatible HTTP signature verification and request-body digest validation.
- Incoming activity validation, activity-ID deduplication, and transactional state changes.
- Durable delivery jobs with retries, per-destination limits, and failure handling.
- NodeInfo for ecosystem discovery and instance metadata.

Initial activity support should cover Follow, Accept, Undo, Create/Note, Update, Delete, Like, and Announce. Include Misskey-style reaction metadata (`_misskey_reaction`) in the supported reaction path. Add Misskey quote metadata as a follow-up compatibility milestone. Defer polls and `_misskey_votes`, along with other extensions, until there is a demonstrated client or federation requirement. Do not implement deprecated `_misskey_content` for new code.

Prioritize the widely deployed legacy HTTP Signatures format for interoperability, then add newer message-signature formats as compatibility tests justify them. Do not require every peer to use a newer signature format.

Treat remote data as untrusted. Enforce request-size limits, timeouts, redirect limits, private-network/IP protections against SSRF, permission and audience checks, and HTML/MFM sanitization. Validate remote media URLs before fetching or caching. Persist delivery work before acknowledging the local operation that created it.

## 8. Multi-node requirements

Multi-node mode is a separate deployment mode, not merely several HTTP processes pointing at one database. It requires:

- FoundationDB as the shared durable store.
- A global account/handle directory and authoritative account ownership.
- Routing by authenticated account or resource ownership.
- A secure internal protocol for cross-node operations.
- Durable work queues and idempotent event processing.
- Shared media storage.
- Recovery, ownership transfer, and account-move procedures.
- Tests for concurrent writes, node loss, retries, duplicate activities, and stale routing information.

Keep one public origin for each logical instance. Internal nodes do not get separate user-facing identities, and local users do not federate with each other through the public ActivityPub network.

## 9. Implementation plan and exit criteria

### Phase 0 — Freeze the contracts

Write the initial Misskey endpoint/streaming manifest, core domain types, identity rules, visibility policy, application ports, and persistence conformance tests. Verify the FoundationDB Rust client and operational requirements with a small vertical-slice prototype before building a large feature surface.

**Exit:** IDs and public URI rules are fixed; a note creation use case can atomically persist both a note and a durable job through the storage interface.

### Phase 1 — Single-node server

Implement the redb adapter, local accounts, token permissions, notes, follows, reactions, local timelines, basic Misskey API, MiAuth, and durable in-process jobs. Use local media storage.

**Exit:** Core flows survive restart and crash-recovery tests; chosen Misskey clients can register/authenticate, post, read timelines, follow, and react.

### Phase 2 — Minimal federation

Implement WebFinger, actors, signatures, inbox processing, core activities, deduplication, and durable outbound delivery. Test with real Misskey and other ActivityPub implementations.

**Exit:** Local and remote follow/post/react flows work end-to-end; transient delivery failures retry without duplicating visible state.

### Phase 3 — Client compatibility and streaming

Add required streaming channels, note capture, notifications, attachment behavior, richer note fields, and endpoint coverage driven by real client tests.

**Exit:** All endpoints in the declared compatibility profile pass contract tests against the target client versions.

### Phase 4 — FoundationDB-backed multi-node mode

Complete the FoundationDB adapter, durable multi-worker jobs, global directory, secure cross-node operations, request routing, and shared media. Deploy at least two application nodes with users owned by different nodes under the same public domain.

**Exit:** `@a@xyz` on `foo` and `@b@xyz` on `bar` retain canonical public identities and can interact locally and federate externally as one instance. Node restarts, duplicate jobs, owner routing, and account moves pass explicit tests.

### Phase 5 — Scale and hardening

Benchmark read/write contention, timeline strategies, queue growth, large followers, federation backlogs, and node failure. Add caching, hybrid timeline fan-out, or separate transient messaging only when measurements demonstrate the need.

**Exit:** Documented performance targets and resource budgets are met for both small redb deployments and representative multi-node deployments.

## 10. Key risks and decisions to preserve

- **API compatibility is the main product risk.** Similar endpoint names do not guarantee working clients. Contract tests against real clients are required throughout development.
- **Distributed identity is foundational.** Node placement must never leak into public IDs or URLs.
- **redb and FoundationDB have different operational and transactional characteristics.** Keep the domain independent, define the required atomicity explicitly, and test both adapters against the same behavior.
- **FoundationDB is not a lightweight single-node dependency.** Its cluster and client installation requirements are accepted only in multi-node mode.
- **The cluster control plane is real work.** FDB transactionality helps preserve data consistency, but does not eliminate routing, ownership, retries, or recovery.
- **Federation is a security boundary.** SSRF prevention, input sanitization, signature verification, and resource limits are MVP requirements for federation, not later polish.

## Decision summary

Adopt a modular Rust monolith, redb for single-node instances, and FoundationDB for all multi-node instances. Separate stable domain identity from node placement; keep account ownership and routing explicit; use durable transactional outbox jobs; implement a tested subset of Misskey's client API and ActivityPub behavior; and defer legacy Misskey features, the Mastodon REST API, and premature infrastructure.

## References

- [redb documentation](https://docs.rs/redb/latest/redb/) — embedded key-value storage and transaction model.
- [FoundationDB known limitations](https://apple.github.io/foundationdb/known-limitations.html) and [transaction size limit](https://github.com/apple/foundationdb/wiki/Transaction-size-limit) — transaction duration and size constraints.
- [FoundationDB Rust client](https://docs.rs/foundationdb/latest/foundationdb/) — Rust binding and client prerequisites.
- [Misskey API documentation](https://misskey-hub.net/en/docs/for-developers/api/) — HTTP API and authentication.
- [Misskey Streaming API](https://misskey-hub.net/en/docs/for-developers/api/streaming/) — WebSocket channels and note capture.
- [Misskey MiAuth documentation](https://misskey-hub.net/en/docs/for-developers/api/token/miauth/) — authentication flow.
