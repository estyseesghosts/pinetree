\### Pinetree — full architecture audit and production-readiness review



I had not delivered the audit yet. Here it is, based on a fresh pass over the latest GitHub source.



\### Release verdict



\### Not ready for production testing against untrusted peers



The codebase has improved materially since the last review. Several of the earlier blockers have been addressed, including the default transcode configuration, misleading smoke-test results, unbounded rate-limit maps, ignored RFC 9421 settings, and the absence of backup/restore tooling.



However, this revision still has data-loss races in media lifecycle operations, a potential remotely triggered signature-verification failure, and serious backup-integrity gaps. Those should be addressed before using important data or exposing the server to arbitrary federated traffic.



\### 1\\. Freshness and audit scope



The newest commit returned by GitHub during this review is:



`dbfe8778dbd7e612e42b2a7ffa20015a7567bf9b`



\[View the commit on GitHub](https://github.com/estyseesghosts/pinetree/commit/dbfe8778dbd7e612e42b2a7ffa20015a7567bf9b?utm\_source=chatgpt.com)



Commit: “prod-ready A-J: smoke gate, config/docker, paging/backfill, rate limits, RFC9421, backup/restore, ingress limits, diagnostics, docs.”



This revision brings significant changes over the earlier commit I reviewed. I checked the latest configuration, media storage and accounting, federation signatures and networking, backup/restore implementation, worker paths, and smoke-gate scripts against the current source.



Scope limitation: this is a static source audit, not a fresh local build, a live four-server federation run, a fuzzing session, or a reproduced corruption test. The commit's own verification record says several Rust suites passed but the overall release qualification remained incomplete because Bun/Docker prerequisites and live federation/backup drills were still outstanding. \[Verification record](https://github.com/estyseesghosts/pinetree/blob/dbfe8778dbd7e612e42b2a7ffa20015a7567bf9b/state/logs/2026-10-21-prod-ready-verify-commit.md?utm\_source=chatgpt.com)



\### 2\\. Architecture map



This is a Rust modular monolith with a single-process runtime and a redb-backed persistence layer. Its boundaries generally follow a sensible design: API adapters call application operations, application code depends on storage ports, and the production redb implementation sits behind those ports.



```text

pinetree/

├── src/

│   ├── main.rs

│   │   ├── Load and validate configuration

│   │   ├── Initialize persistence and application services

│   │   ├── Configure federation signatures and logging

│   │   ├── Start background workers

│   │   └── Run HTTP server and shutdown handling

│   │

│   ├── config.rs

│   │   ├── Public URL and listen configuration

│   │   ├── Federation and SSRF settings

│   │   ├── Media/cache/transcode limits

│   │   ├── Rate-limit-related settings

│   │   └── Diagnostics and backup settings

│   │

│   ├── api/

│   │   └── misskey/

│   │       ├── endpoints/  REST routes and endpoint handlers

│   │       ├── auth.rs     Authentication and permissions

│   │       ├── request.rs  Request extraction and input handling

│   │       ├── response.rs Errors, rate limits and upload slots

│   │       ├── packers.rs  Misskey-compatible response shapes

│   │       └── preview.rs

│   │

│   ├── application/

│   │   ├── commands/       Mutations and use cases

│   │   ├── queries.rs      Read operations

│   │   ├── ports.rs        Storage, job and service contracts

│   │   └── events.rs

│   │

│   ├── domain/

│   │   ├── entities and value objects

│   │   └── visibility and business policies

│   │

│   ├── persistence/

│   │   ├── redb/mod.rs     Active persistence implementation

│   │   └── foundationdb/   Not an operational cluster backend yet

│   │

│   ├── activitypub/

│   │   ├── actor.rs

│   │   ├── objects.rs

│   │   ├── quotes.rs

│   │   ├── signatures.rs

│   │   └── webfinger.rs

│   │

│   ├── federation/

│   │   ├── inbox.rs        Authenticate and process incoming activities

│   │   ├── outbox.rs       Build outbound activities

│   │   ├── delivery.rs     HTTP, DNS, retries and destination handling

│   │   ├── retry.rs

│   │   ├── fetch\_gate.rs   Authenticated fetch and visibility checks

│   │   └── host\_caps.rs

│   │

│   ├── jobs/

│   │   ├── worker.rs       Execute background work

│   │   └── queue.rs        Still incomplete as a standalone abstraction

│   │

│   ├── media/

│   │   ├── storage.rs      Content-addressed filesystem pools

│   │   ├── fetch\_stream.rs Remote fetch staging

│   │   ├── remote\_cache.rs Budgeting, fetch and eviction

│   │   ├── reconciler.rs   Accounting and missing-file repair

│   │   ├── processing.rs

│   │   ├── transcode.rs

│   │   └── ingest.rs

│   │

│   ├── backup/

│   │   ├── bundle.rs       Snapshot export and bundle verification

│   │   ├── restore.rs      Offline restore and integrity checks

│   │   └── mod.rs          Queueing and backup status

│   │

│   ├── streaming/          WebSocket and in-process event hub

│   ├── observability.rs    Structured log events and redaction

│   ├── services/

│   └── cluster/            Not implemented for multi-node operation

│

├── tests/                  Unit, contract, integration and recovery suites

├── scripts/                Smoke and interoperability runners

├── Dockerfile

├── docker/

└── docs/

```



Architecture documentation: \[Architecture.md](https://github.com/estyseesghosts/pinetree/blob/dbfe8778dbd7e612e42b2a7ffa20015a7567bf9b/docs/wiki/Architecture.md?utm\_source=chatgpt.com) and \[database.md](https://github.com/estyseesghosts/pinetree/blob/dbfe8778dbd7e612e42b2a7ffa20015a7567bf9b/docs/database.md?utm\_source=chatgpt.com).



\### How data moves through the system



Client API: Axum extracts a request, the Misskey adapter authenticates it and validates endpoint inputs, and application commands or queries invoke the relevant ports. The redb adapter persists records and indexes.



Inbound federation: HTTP signature verification and the fetch/inbox gates authenticate the sender before activities are processed. Application operations make the domain changes; deduplication records and background jobs support retries and outbound effects.



Media: files live in separate durable and remote-cache pools. Metadata and reference/accounting information live in redb. Because filesystem writes/deletes and redb transactions cannot participate in a single atomic transaction, a crash or race between those operations can produce orphaned files, missing files or inconsistent accounting.



Background work: a persistent redb job queue and worker execute federation delivery, notifications, transcoding, and storage maintenance. Streaming is in-process, so its live state is not shared between independent server processes.



Backup/restore: the new backup code exports redb tables under a consistent read transaction and bundles the media pools. Restore validates the bundle before rebuilding a fresh directory. This is a meaningful improvement, but the media consistency assumptions still need correction.



\### 3\\. Newly identified release blockers



These are the findings I would prioritize over feature parity or code-style cleanup.



\### P1 — Media deletion can remove bytes that a concurrent upload has just made live



This is the most important data-integrity issue I found.



The deletion path first removes a media row and updates its reference count in redb. If that transaction reports no remaining references, it subsequently deletes the physical blob:



\* \[media command deletion path](https://github.com/estyseesghosts/pinetree/blob/dbfe8778dbd7e612e42b2a7ffa20015a7567bf9b/src/application/commands/media.rs?utm\_source=chatgpt.com#L251-L270)



\* \[redb media-record deletion](https://github.com/estyseesghosts/pinetree/blob/dbfe8778dbd7e612e42b2a7ffa20015a7567bf9b/src/persistence/redb/mod.rs?utm\_source=chatgpt.com#L9830-L9885)



\* \[filesystem blob put/delete operations](https://github.com/estyseesghosts/pinetree/blob/dbfe8778dbd7e612e42b2a7ffa20015a7567bf9b/src/media/storage.rs?utm\_source=chatgpt.com#L202-L279)



The problem is that both `FsBlobStore::put` and `FsBlobStore::delete` acquire the shared/read side of the media gate. That gate excludes an active backup, but does not serialize ordinary mutations against one another.



A possible execution order is:



\* Request A deletes the last media record for hash `H`; redb reports zero remaining references.



\* Request B uploads identical bytes, sees or writes blob `H`, then inserts a new media record referencing `H`.



\* Request A deletes blob `H` based on its earlier reference-count result.



\* Request B's newly committed row points to a missing file.



The system can now have valid redb records whose content-addressed file is gone. The reconciler deliberately preserves missing durable metadata for operator repair; it cannot reconstruct bytes that were actually deleted.



Required fix: serialize the complete per-blob lifecycle, not merely the file operation. A per-hash coordination mechanism must cover the relevant blob and metadata transitions for both writes and deletes. A reference-count check immediately before `delete()` alone is insufficient if a concurrent writer can still recreate the blob between the check and deletion.



Add a deterministic concurrency test that races last-reference deletion against an upload of identical bytes, and assert that every committed media record still has readable content.



\### P1 — Remote-cache eviction also deletes shared content-addressed blobs without checking their remaining references



The eviction loop chooses a remote-cache record and deletes its blob key, then adjusts accounting for that record. It does not check whether other media records or remote-cache entries refer to the same blob key. \[Remote-cache eviction](https://github.com/estyseesghosts/pinetree/blob/dbfe8778dbd7e612e42b2a7ffa20015a7567bf9b/src/media/remote\_cache.rs?utm\_source=chatgpt.com#L507-L555)



Since the storage key is content-addressed, identical bytes can be shared by more than one URL or metadata row. Deleting one cache record's bytes can therefore make another still-indexed record temporarily unreadable. Re-fetch logic may eventually repair this, but it can also add duplicate rows or distort accounting during repeated evict/refetch cycles.



Required fix: define eviction around the actual storage object and all records referencing it. Either coordinate all indexes for the key and evict the shared object coherently, or preserve the object until no live cache-pool reference needs its bytes. Keep durable-pool references and cache-pool references logically separate; one global reference count cannot safely determine file deletion across two independent roots.



Add a regression scenario with two remote URLs that return identical bytes, evict one entry, and then verify that the other remains serviceable and accounting converges without duplicating metadata.



\### P1 — Backup/restore does not correctly handle intentionally evicted cache media



The new backup path computes a set of blob references from media metadata, then compares that set against files found in the durable and cache pools. The current collector includes rows where `!meta.evicted`. \[Backup media-reference collection](https://github.com/estyseesghosts/pinetree/blob/dbfe8778dbd7e612e42b2a7ffa20015a7567bf9b/src/backup/bundle.rs?utm\_source=chatgpt.com#L554-L582)



The eviction path shown in the current source deletes the cache blob and updates cache accounting, but leaves the metadata and remote-cache index for future re-fetch. It does not update the persisted media row in that operation. \[Eviction implementation](https://github.com/estyseesghosts/pinetree/blob/dbfe8778dbd7e612e42b2a7ffa20015a7567bf9b/src/media/remote\_cache.rs?utm\_source=chatgpt.com#L537-L555)



This creates a mismatch to resolve: if an evicted row is read directly from the persisted `MEDIA\_META` table with `evicted` still false, the bundle treats the absent blob as a gap. The backup can be published with `media\_gaps` recorded, while restore rejects bundles with non-empty gaps. \[Restore rejects recorded media gaps](https://github.com/estyseesghosts/pinetree/blob/dbfe8778dbd7e612e42b2a7ffa20015a7567bf9b/src/backup/restore.rs?utm\_source=chatgpt.com#L180-L192)



This needs to be tested against the actual `MediaMeta` serialization and read semantics, but the current code paths are inconsistent enough that I would treat it as a release blocker for backup reliability.



Required fix: define a canonical representation of an evicted cache item and use it consistently in the persistence adapter, reference collector, manifest writer, and restore validation. Intentionally absent re-fetchable cache bytes should not automatically make an otherwise restorable account/database snapshot unrecoverable. Truly missing durable media must remain a hard error.



Test backup and restore after deliberately evicting media, not merely when every blob is present.



\### P1 — Backup integrity checking is not based on the same media metadata snapshot as the exported database



The database export uses one redb read transaction, while `collect\_referenced\_blobs` performs a later, separate paged walk. That is not the same snapshot as the tables serialized into the backup. \[Bundle creation](https://github.com/estyseesghosts/pinetree/blob/dbfe8778dbd7e612e42b2a7ffa20015a7567bf9b/src/backup/bundle.rs?utm\_source=chatgpt.com#L390-L490)



There is also a concrete pagination inconsistency: the collector stops when `page.len() < cursor.limit`. But the persistence code explicitly warns that a short page is not necessarily end-of-scan if rows are being deleted while a paged scan proceeds. \[Media pagination contract](https://github.com/estyseesghosts/pinetree/blob/dbfe8778dbd7e612e42b2a7ffa20015a7567bf9b/src/persistence/redb/mod.rs?utm\_source=chatgpt.com#L9746-L9775)



In an adverse case, a media row captured in the database snapshot may no longer be visible to the later live walk. If its blob was already missing, that reference may be omitted from the bundle's missing-media calculation. The backup could then appear valid even though its database snapshot still references unavailable bytes.



Required fix: calculate backup media references from the same consistent snapshot that exports the redb tables, or introduce a snapshot/serialization mechanism that makes the reference inventory exact. Do not use “short page” as proof of completion where concurrent mutation can occur. The backup gate prevents pool mutations while held, but it does not, by itself, make every separate database read the same snapshot.



\### P1 — RFC 9421 verification performs arithmetic on untrusted timestamp values without overflow-safe operations



The current verifier computes the timestamp difference using:



```rust

(now\_secs - created).abs()

```



and later compares the current time with `expires + max\_clock\_skew\_secs`. Both timestamp values are parsed from attacker-supplied signature headers. \[RFC 9421 verifier](https://github.com/estyseesghosts/pinetree/blob/dbfe8778dbd7e612e42b2a7ffa20015a7567bf9b/src/activitypub/signatures.rs?utm\_source=chatgpt.com#L1255-L1274)



Extreme `i64` inputs can overflow these operations. Depending on the build's overflow-checking behavior, this can panic before the request is rejected, or produce wrapped arithmetic that makes the check unreliable.



Required fix: use overflow-safe comparisons, such as checked arithmetic or a safe absolute-difference calculation, and reject timestamp values outside a reasonable supported range before using them. Add fuzz tests for `i64::MIN`, `i64::MAX`, and nearby values for both `created` and `expires`.



This is a security-hardening requirement because verification operates on unauthenticated request metadata. The effect of any panic in a specific build profile still needs runtime confirmation.



\### P1 — DNS lookup timeouts do not bound the number of resolver threads



The outbound networking implementation spawns a dedicated OS thread for DNS resolution and waits only up to a configured timeout. If the timeout expires, the thread is detached rather than cancelled. \[DNS resolution and URL validation](https://github.com/estyseesghosts/pinetree/blob/dbfe8778dbd7e612e42b2a7ffa20015a7567bf9b/src/federation/delivery.rs?utm\_source=chatgpt.com)



The inbound federation paths may resolve an actor's key before cryptographic verification is complete. Consequently, an external peer that supplies many distinct, uncached key URLs can cause outbound lookups before it has authenticated successfully.



With enough slow or hung resolutions, a per-lookup timeout does not prevent accumulation of outstanding threads and associated resources.



Required fix: put a hard bound on concurrent DNS/key-fetch work, use a resolver whose outstanding operations can be bounded and cancelled, and add load tests for many distinct key hosts with slow/failing DNS. Ensure rate limits and worker/concurrency limits apply before expensive work begins.



\### 4\\. Additional correctness and operational findings



| Priority | Finding                                                                               | Why it matters                                                                                                                                                                                                                                                                                                                                                                 |

| -------- | ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |

| P2       | backup.max\_bundle\_bytes is configured but not enforced by backup creation             | The config describes a configurable cap, but BackupContext carries the directory rather than the cap, and create\_bundle checks the global MAX\_BUNDLE\_BYTES. Setting a lower configured value does not currently constrain a backup to that size. Config · Bundle creation                                                                                                      |

| P2       | Filesystem orphan cleanup is not clearly wired into the production reconciliation job | The code provides collect\_live\_blob\_keys and sweep\_orphan\_blobs, but the production worker's ReconcileStorage arm calls reconcile\_media\_paged. I did not find a production call chaining the full live-key collection to the filesystem sweep. Orphan files left by interrupted uploads or rejected post-write inserts can accumulate. Reconciler · Worker reconciliation path |

| P2       | Reconciliation correctness relies on multiple reads, not a database-wide snapshot     | The reconciler correctly avoids repairing global counters from a truncated scan, but a full paged walk is still not a point-in-time snapshot. Any destructive cleanup must account for new rows and deletes racing the scan; a “complete” walk does not imply that the returned set remained stable throughout. Reconciler                                                     |

| P2       | Docker health checks still probe liveness rather than readiness                       | The operations guide describes /healthz as a cheap liveness check and /ready as the dependency-aware readiness probe. The Docker health check still targets /healthz, so it can report healthy even if the service should not be receiving traffic because storage is unavailable. Operations guide                                                                            |

| P2       | Structured logs are not equivalent to operational metrics                             | The logging/redaction surface is a good improvement, but queue backlog, oldest pending delivery, federation failures/quarantine, and storage conditions are still partly unavailable or reported as null in the documented admin status surface. Without metrics and alerts, gradual failure can go unnoticed. Observability implementation · Operations guide                 |

| P2       | Backup retention and total backup-directory growth need an explicit policy            | The code bounds an individual bundle, but I found no clear automated retention/rotation policy. Repeated backups can consume the filesystem until the instance has insufficient room for normal writes or a subsequent backup.                                                                                                                                                 |

| P2       | Production federation and restore acceptance are still unqualified                    | The latest verification record explicitly says a provisioned-host run is still needed, including Docker builds, live federation, a strict full pass and a backup/restore drill. Unit and contract tests alone cannot establish that those paths work against real deployments. Verification record                                                                             |



\### A note on the filesystem sweep



The orphan-sweep functions also deserve careful treatment before being wired into production. The current checked-sweep guard protects against an explicitly incomplete key set, which is good, but that is not the same as making the set stable against concurrent writes. If the sweep is activated, its correctness needs to be considered together with the media lifecycle race described earlier; otherwise, a new blob could be written after the scan but before metadata commits and be mistaken for an orphan.



\### 5\\. Findings from the previous audit that are now fixed



The latest commit is not just documentation work. It directly addresses several of the earlier findings.



Docker transcode startup trap — addressed in configuration



Transcoding is disabled by default, the ordinary runtime and FFmpeg-enabled image targets are separated, and enabling transcoding requires a successful startup toolchain probe. The supplied container config also uses a placeholder public URL that refuses to boot until configured. \[Dockerfile](https://github.com/estyseesghosts/pinetree/blob/dbfe8778dbd7e612e42b2a7ffa20015a7567bf9b/Dockerfile?utm\_source=chatgpt.com) · \[container config](https://github.com/estyseesghosts/pinetree/blob/dbfe8778dbd7e612e42b2a7ffa20015a7567bf9b/docker/pinetree.container.json?utm\_source=chatgpt.com)



Misleading smoke-test success — addressed



The cargo test pipeline now uses an inner `pipefail`, failed steps are recorded rather than silently swallowed, and skipped phases produce a partial result rather than a false release pass. Strict mode exists for CI/release runs. \[smoke-all.sh](https://github.com/estyseesghosts/pinetree/blob/dbfe8778dbd7e612e42b2a7ffa20015a7567bf9b/scripts/smoke-all.sh?utm\_source=chatgpt.com)



Unbounded rate-limit and upload-slot maps — substantially addressed



The new code adds map caps, expiry pruning, cap-pressure eviction and race-safe upload-slot handling. This removes the obvious unbounded-growth behavior identified previously, though the caps and eviction behavior still merit adversarial load testing. \[Rate limiter](https://github.com/estyseesghosts/pinetree/blob/dbfe8778dbd7e612e42b2a7ffa20015a7567bf9b/src/api/misskey/response.rs?utm\_source=chatgpt.com)



RFC 9421 mode previously ignored — addressed in the new wiring



The current source now has an RFC 9421 verifier, a mode-combination function, inbox/fetch-gate integration, and startup configuration wiring. `enforce` is therefore no longer merely a cosmetic config option. The extreme-timestamp finding above is an additional issue in this implementation.



Inbox body limit applied too late — addressed



The latest work moves the 64 KiB inbox cap to route-level extraction rather than relying only on an application check after the body has already been buffered. \[Ingress-limit tests](https://github.com/estyseesghosts/pinetree/blob/dbfe8778dbd7e612e42b2a7ffa20015a7567bf9b/tests/ingress\_limits\_g1.rs?utm\_source=chatgpt.com)



No backup/restore tooling — substantially addressed



There is now a versioned ZIP format, a registry covering 103 redb tables, checksums, archive-path validation, offline restore into a fresh directory, and post-restore database/accounting verification. That is a substantial improvement; the cache-eviction and snapshot consistency findings above are why I would not yet trust it as a proven disaster-recovery system.



\### 6\\. Security and networking controls that look good



The current code has several meaningful safeguards worth retaining:



\* Credentials: Argon2id password hashing and hashed API-token lookups, alongside explicit endpoint authorization checks.



\* Federation: HTTP-signature validation, body-digest checks, timestamp validation, actor/signer checks, object-authority checks, and deduplication.



\* Outbound HTTP: explicit URL validation, literal/resolved address checks, TLS verification, redirect re-validation, timeouts and body/response limits. The major additional concern is that a resolver timeout does not itself bound outstanding resolver threads.



\* Media intake: size caps, MIME sniffing, remote media limits, staged file ingestion and cache accounting. The lifecycle races show that these controls need stronger coordination between metadata and physical blobs.



\* Configuration: unknown config keys are rejected, cluster mode fails instead of pretending to work, and HTTP public URLs are restricted to explicit local/test cases.



\* Recovery: accounting backfills and the smoke gate now have more explicit bounds and failure semantics.



The current security documentation is a useful map of intended behavior, but it is not a substitute for confirming the runtime paths against hostile inputs. \[Security guide](https://github.com/estyseesghosts/pinetree/blob/dbfe8778dbd7e612e42b2a7ffa20015a7567bf9b/docs/wiki/Security.md?utm\_source=chatgpt.com)



\### 7\\. Data integrity: what I would test before trusting the database



I did not find a confirmed direct defect in redb's internal transactional integrity from source inspection alone. The bigger risk is the relationship between its transactions and filesystem state, plus the recent addition of backup/restore.



The following failure-injection cases should be release gates:



| Failure or race                                               | Required invariant                                                                            |

| ------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |

| Crash after file write but before metadata commit             | Reconciliation eventually detects and removes or safely accounts for the orphan               |

| Last media delete racing identical-content upload             | A newly committed metadata row never refers to deleted bytes                                  |

| Two remote URLs sharing a blob key, followed by eviction      | An active entry remains retrievable; counters converge                                        |

| Crash after metadata delete but before physical file deletion | No live reference is lost; orphan can be reclaimed later                                      |

| Backup while media metadata is being deleted                  | The database snapshot and media inventory agree; no missing reference escapes the manifest    |

| Backup of a cache with intentionally evicted media            | Backup is valid under the defined policy and can actually be restored                         |

| Truncated or concurrently modified paged scan                 | No destructive action or false “complete” result follows from a partial set                   |

| Restore from corrupted, truncated or adversarial ZIP          | Restore fails closed and never leaves a directory that could be mistaken for a valid instance |

| Restart during a backfill or reconciliation continuation      | Counters converge without double-counting or dropping rows                                    |

| Disk-full or permission failure during backup                 | The final filename is not published unless the bundle is verified                             |



The new test suite improves coverage of migrations, storage paging, quotas and recovery. The gap now is less about whether each function works in isolation and more about whether interleavings across database transactions, files, background jobs and backup operations remain correct.



\### 8\\. Codebase health and architectural risks



\### The core architectural direction is reasonable



The port/adapter split helps preserve the possibility of alternate persistence backends. Keeping the active deployment model single-process also avoids claiming distributed guarantees that the code does not yet provide. The newly added diagnostics, backup tooling and stricter config defaults are improvements to operator safety.



\### The persistence and API files are too large for safe ongoing changes



`src/persistence/redb/mod.rs` remains a very large file (roughly 13,000 lines in the current tree), and `src/api/misskey/endpoints/mod.rs` is also unusually large. Federation and media logic have further long modules.



This is not, by itself, a runtime defect, but it materially raises review and regression risk—particularly for code that maintains multiple secondary indexes or performs filesystem operations after committing a transaction.



Recommended decomposition for the persistence adapter:



```text

persistence/redb/

├── accounts.rs

├── notes.rs

├── follows.rs

├── timelines.rs

├── notifications.rs

├── media.rs

├── remote\_cache.rs

├── jobs.rs

├── indexes.rs

├── migrations.rs

└── backup.rs

```



Keep shared transaction helpers and schema definitions centralized. Avoid splitting one atomic redb mutation across different modules or transactions merely to reduce file size.



\### Single-node deployment is the only supported operational target



The FoundationDB adapter and cluster routing/directory/internal protocol remain stubs. Streaming state is in-process, and the current worker/job setup should not be assumed to support arbitrary multi-process execution against the same logical instance.



Treat `mode=single` as a hard architectural constraint until the cluster implementation and distributed consistency tests exist. A load balancer with multiple independent Pinetree processes is not a safe shortcut to horizontal scaling.



\### Misskey compatibility remains a separate readiness axis



This review focuses on production safety. It does not mean Pinetree is now a drop-in replacement for every Misskey endpoint. Its own inventory documents missing and intentionally divergent API and streaming surfaces. \[Compatibility inventory](https://github.com/estyseesghosts/pinetree/blob/dbfe8778dbd7e612e42b2a7ffa20015a7567bf9b/docs/inventory-pinetree-vs-misskey-2026-10-20.md?utm\_source=chatgpt.com)



Features such as full admin surfaces, some streaming events, complete 2FA/OAuth workflows, channels/chat and other intentionally out-of-scope functionality need their own compatibility decision. The fact that a feature is intentionally omitted is not a regression, but it must not be advertised as supported.



\### 9\\. Test and release process



The test-gate changes are a genuine improvement: the newest commit's verification record says the overall smoke gate failed honestly when prerequisites were absent rather than incorrectly reporting a full pass. That is the correct behavior.



However, the project still needs a single enforceable release workflow that makes all release-critical checks mandatory. The current strict smoke wrapper covers the named API/client and federation phases, but the separate Docker FFmpeg matrix and backup/restore drill also need to be required by the release pipeline, not merely listed as manual follow-ups.



Before the first serious production test, require:



\* A strict smoke run against the freshly built current binary.



\* The real Docker image matrix, including transcoding-enabled startup and a real worker-path transcode.



\* Live federation against supported Misskey/Sharkey and other intended peer versions.



\* Backup followed by restore into a fresh directory, with media eviction and missing-media cases included.



\* The media lifecycle concurrency tests described above.



\* Security tests for extreme signature timestamps, slow DNS resolution and many uncached actor keys.



\* Readiness checks and operational monitoring adequate to detect growing job queues, failed deliveries, storage exhaustion and repeated media repair.



Do not accept a successful unit-test count as a substitute for these runtime checks. The latest verification record explicitly leaves release qualification outstanding.



\### 10\\. Recommended fix order



I would make these the next concrete work items, in this order:



\### 1



Fix per-blob lifecycle coordination



Prevent last-reference deletion and identical-content uploads from racing. Apply the same object-level coordination to eviction. Add deterministic concurrent mutation tests.



\### 2



Harden untrusted networking paths



Replace unchecked signature-timestamp arithmetic, bound resolver concurrency, and exercise unauthenticated inbound requests that trigger uncached-key fetches.



\### 3



Make backup inventory and restore behavior coherent



Resolve the evicted-cache representation, bind the backup reference inventory to the exported database snapshot, fix the early-termination pagination assumption, and enforce `backup.max\_bundle\_bytes`.



\### 4



Finish media recovery and retention policies



Make it explicit which worker performs filesystem orphan cleanup, give cleanup a safe concurrency model, and establish backup retention/disk-space safeguards.



\### 5



Gate release on live-system evidence



Automate the Docker matrix and backup/restore drill, require the full federation smoke run, and add readiness and operational signals before relying on the service.



\### Final assessment



Pinetree is materially healthier than it was in the previous audit. The latest changes address many obvious configuration, rate-limiting, ingress, signature-mode, backup tooling and test-harness shortcomings. The architecture remains appropriate for a single-node server, and the code includes several thoughtful fail-closed and recovery mechanisms.



The major remaining concern is that the weakest boundary is also the most consequential one: database records, content-addressed files, cache references, background jobs and backup inventories must remain consistent while operations race or fail. The media deletion race can lose live content; the new backup system has unresolved edge cases involving evicted and concurrently changed media; and inbound federation still needs a stronger bound on expensive resolver work.



I would not yet expose this revision to arbitrary federated peers or rely on it to protect irreplaceable instance data. Fix the lifecycle and backup-integrity findings, then qualify the current commit with live federation and restore tests before treating Pinetree as ready for production testing.



