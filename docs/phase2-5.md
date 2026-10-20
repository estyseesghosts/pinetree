> **Status (2026-10-20 freshness pass):** PLAN HISTORY. This file opens with chat residue ('Understood…') and has no H1; the plan body is below it. Slices 1–6 are landed (evidence per slice). Slice 7 is `[OPEN]`. Slices 0, 8, and 9 are historical.
> Tags: `[LANDED — evidence]` verified in code at HEAD; `[OPEN]` not in code; `[SUPERSEDED by …]`; `[DECISION — still binding]`; `[PLAN HISTORY]`. Original text is unchanged; tag blocks are additions. Short paths are under `src/`; `mod.rs` = `src/api/misskey/endpoints/mod.rs`.

Understood. The visibility policy stays as it is, and `commands.rs` gets split now. I haven't touched any code. The `redb` dependency is the one decision still open (slice 1).

## Revised cleanup plan

> **[PLAN HISTORY]** Superseded in detail by `phase2-6.md` for later work.


Two changes from last time. The visibility slice is now docs-only. The `commands.rs` split moves to slice 2, because later slices edit that file, and they're easier to review once it's split.

### Slice 0: Baseline

> **[PLAN HISTORY]** 107-test baseline is historical. Current conformance file is 118 `#[test]` fns.

- **Deliverable:** a recorded `cargo test` result on the current tree. This slice contains no code changes.
- **Expected result:** 107 tests green (78 lib, 26 conformance, 2 crash-recovery, 1 live `misskey-js`). Anything else means stop and investigate.
- **Done when:** the result is pasted into the work log.

### Slice 1: Repo reproducibility

> **[LANDED — .gitignore:1-2 (/foundationdb/, /redb/); redb in Cargo.toml]** Clean-clone build not re-run in this pass.

- **Deliverables:**
  - `.gitignore` anchored to `/foundationdb/`, `/redb/`, `/misskey`, `/sharkey`, `/target/`, `/data/`, plus the existing unrelated entries.
  - The nine files tracked: seven under `src/api/misskey/`, plus `src/persistence/redb/mod.rs` and `src/persistence/foundationdb/mod.rs`.
  - The `redb` dependency resolved so a clean clone builds.
- **Open decision:** pin the crates.io version matching your checkout, or document an extra clone step. I recommend pinning, and I'll assume that unless you say otherwise.
- **Verification:** clone into a fresh directory, run `cargo test`, and get the same 107 green. `git status --ignored` shows no source files.

### Slice 2: Split `commands.rs` (pure move, no behavior change)

> **[LANDED — application/commands/ split: accounts, notes, follows, media, keys, helpers, inbound/]**

- **Deliverables:** `application/commands.rs` becomes a `commands/` module. `AppServices` and its constructor stay in `mod.rs`, with `impl AppServices` blocks spread across these files:

| File | Contents |
|---|---|
| `helpers.rs` | `now_ms`, `notify_job`, `streaming_job`, `federate_job`, `remote_inbox_for_account` |
| `accounts.rs` | `create_account`, `update_profile`, `store_token` |
| `notes.rs` | `create_note`, `delete_note`, `react`, `unreact` |
| `follows.rs` | `follow`, `unfollow` |
| `media.rs` | `upload_media`, `md5_hex`, `sniff_mime` |
| `keys.rs` | `ensure_actor_keypair`, `rotate_actor_keypair`, `ensure_system_actor`, `cache_remote_public_key` |
| `inbound/follow.rs` | `handle_inbound_follow`, `_unfollow`, `_accept_follow`, `_reject_follow` |
| `inbound/notes.rs` | `handle_inbound_create_note`, `_delete_note`, `_like`, `_unlike` |
| `inbound/actors.rs` | `handle_inbound_update_actor` |

- `commands.rs` tests move to a shared test module, with `test_services` reusable.
- `crate::application::commands::AppServices` stays importable at the same path, so no caller changes.
- **Non-goals:** no renames, no signature changes, no logic edits, and no trait or wrapper layers.
- **Verification:** the same 107 tests, a clean build, and a diff check that function bodies are byte-identical.
- **Risk:** private-helper visibility (`pub(super)` or `pub(crate)`).

### Slice 3: Reject-follow atomicity

> **[LANDED — persistence/redb/mod.rs:8560 record_reject_follow_activity; inbound/follow.rs:214]**

- **Deliverables:**
  - A new `ActivityDedup` method for Reject.
  - A redb implementation that dedups and unfollows in one transaction.
  - Updated callers (`inbox.rs:335` and the handler).
  - The `NoopActivityDedup` and two test-mock implementers updated.
  - The `record_*` trait defaults changed to fail loudly.
- **Tests:** an injected-failure test (dedup recorded, unfollow fails, redelivery still removes the follow), plus a normal-path test. Add both to the conformance suite so FDB inherits them.

### Slice 4: Split TLS from SSRF

> **[LANDED — federation/delivery.rs:1602,1621 (tests: no `-k` in curl argv)]**

- **Deliverables:**
  - `-k` removed from `send_https_via_curl`.
  - `allow_private_ips` documented as an SSRF exemption only.
  - A separate, explicitly named insecure-TLS opt-in only if something actually needs it. Nothing found so far does.
  - A test that the curl argument list never contains `-k` by default.
- **Verification:** run `scripts/smoke-phase2.sh`. It uses plain HTTP, so it should be unaffected.
- **Deferred:** replacing the curl shell-out with a library client, the missing overall request deadline, and DNS pinning. These get a note in the log, not code.

### Slice 5: Job failure handling

> **[LANDED — jobs/worker.rs:71,709 (catch_unwind); persistence/redb/mod.rs:7078 (attempts incremented on reclaim)]**

- **Deliverables:**
  - A terminal failed/quarantined state on the `JobQueue` port.
  - redb implementation.
  - Conformance cells.
  - Bad payloads and `PermanentFailure` quarantined with a logged reason.
  - Retry exhaustion recorded.
  - Storage-`Err` retries capped.
- The "legacy payload" no-op branch is removed. Fixtures that need it get well-formed payloads.
- **Contract:** update `docs/application-ports.md`, which is frozen. This needs a logged superseding note.

### Slice 6: Replace the custom RSA

> **[LANDED — activitypub/signatures.rs:138-142 (ring RsaKeyPair, RSA_PKCS1_SHA256); no custom modpow]**

- **Deliverables:**
  - A maintained crate behind the existing `signatures.rs` surface, with bigint, primality, RSA and DER code deleted.
  - The HTTP-signature canonicalization kept.
- **Pre-work:** check the current status of the RustCrypto `rsa` timing advisory versus aws-lc-rs before choosing.
- **Verification:**
  - Existing sign/verify and CRT regression tests pass.
  - Stored PKCS#8 and SPKI keys still parse.
  - `tests/federation.rs` passes.
  - `scripts/smoke-phase2.sh` passes against live Misskey and Sharkey.
- This is the largest slice, and it only touches `keys.rs` and the signature callers after the split.

### Slice 7: Decouple ActivityPub from the Misskey adapter

> **[OPEN]** `activitypub/webfinger.rs:4` still says route wiring lives in `api::misskey`. Not fully re-audited.

- **Deliverables:**
  - `activitypub/{objects,actor,webfinger}.rs` and `federation/inbox.rs` take `Arc<AppServices>` (or a small shared state type) instead of `ApiState`.
  - `format_millis` and `PACKER_VERSION` moved to a neutral module.
  - Zero `api::misskey` imports outside `src/api`.
- **Verification:** grep confirms that, and the tests stay green.

### Slice 8: Visibility (docs only)

> **[PLAN HISTORY]**

- **Deliverable:** one log entry recording the decision to keep Misskey's remote-remote followers fallback (`docs/visibility-policy.md:45`).
  - It should note that the fallback is unreachable in production today, because only tests build remote viewers.
  - It should flag that Phase 3 streaming or any remote-viewer path must revisit it.
- No code or policy change.

### Slice 9: Docs and exit gate

> **[PLAN HISTORY]**

- **Deliverables:**
  - The `users/followers` gap in AGENTS.md corrected: only the Misskey API endpoint is missing.
  - `docs/README.md:20` corrected to Sharkey 2025.4.7.
  - A dated log per slice in `state/logs/`.
  - A cleanup checklist checked off with evidence.
  - The AGENTS.md phase header updated.
- **Phase 3 entry gate:**
  - clean-clone `cargo test` is green;
  - `scripts/smoke-phase2.sh` is green;
  - slices 1 to 8 are logged.

Slices 1 and 2 should land first, and each slice should be its own commit. Slices 3 to 5 are independent of each other once the split is in. Slice 7 should come after slice 6, since both touch the same files.

