This is the decomposition guide. It is written so a later agent can execute it without rediscovering the tree. I did not edit the repo: Plan mode only allows the guide in chat. Switch to a build agent if you want it saved as `pinetree/docs/module-split-guide.md`.



Line counts below are non-blank lines measured on 2026-10-22 with PowerShell `Get-Content | Measure-Object -Line`, excluding `target\\`. Physical lines are higher. Remeasure before each slice; do not trust this table after the first move.



\## Goal



Make every ongoing change touch a file a human can review. Behavior, public paths, JSON shapes, table names, and test names stay the same. This is a move, not a redesign.



Hard cap after the program: no `src/\*\*/\*.rs` leaf over 800 non-blank lines. No integration test file over 1,500 non-blank lines. A parent `mod.rs` only declares child modules and re-exports. It does not contain `impl` blocks, handlers, or helpers.



\## Rules every agent must follow



Read `pinetree/AGENTS.md` and `pinetree/rust\_style\_guide.html` (Google Rust style guide) before editing. The rules that bite on this work:



\- Name each new file after its primary type or trait (`note\_store.rs`, not `helpers2.rs`). Avoid abbreviations.

\- Do not implement APIs in a non-leaf module. After the split, `mod.rs` is a non-leaf.

\- No glob imports. One `use` statement imports from one module. Import types and use them unqualified. Qualify free functions with the module name. Import traits locally, next to the `impl`, not at the crate root of a 2,000-line file.

\- `rustfmt` the files you touch. Do not reformat unrelated files.

\- Prefer `expect` with a context string over `unwrap`. Do not add `unsafe`, new `Arc`/`Rc`, or new threads. Do not "clean up" logic while moving it.

\- Keep existing `pub` paths stable with `pub use` from the parent. Callers of `crate::persistence::redb::RedbStore` and `crate::api::misskey::endpoints::build\_router` must not change.

\- New items that are only used inside the crate are `pub(crate)`, not `pub`.

\- Do not add a workspace crate. Stay inside the `pinetree` crate.

\- Domain must not gain HTTP, Misskey, ActivityPub, or database imports because of a move.



Windows toolchain, never WSL:



\- Shell is PowerShell. Compiler and tests are native `cargo` on `PATH`. Do not call `wsl`, `bash`, or `ubuntu`.

\- Do not use Git-for-Windows `bash` to run `scripts/smoke-all.sh` as the split gate. The split gate is native `cargo test`.

\- If a script is bash-only, say so in the log and stop. Do not "just use WSL this once."

\- Cargo target dir stays on the native filesystem. Do not point `CARGO\_TARGET\_DIR` at a WSL path.



Subagent partition, from `AGENTS.md`:



\- One agent owns one file family. Two agents must never edit the same `mod.rs`.

\- Shared files (`docs/`, `src/lib.rs`, router registration, `application/ports.rs` while it is still one file) are parent-owned. Child agents propose snippets.

\- No `git commit`, `stash`, `reset`, or `checkout --`. Check `git status --short` first and leave other people's edits alone.

\- Tests moved out of a module keep their original `#\[test]` names so `cargo test <name>` still works.



\## Procedure for one slice



1\. Remeasure the file. Record non-blank and physical line counts in the work log.

2\. List every `impl`, free function, and `mod tests` with start line. Split on those boundaries, not on a round number in the middle of a function.

3\. Create the child files. Move the item and the comments directly above it. Move private helpers with the only impl that uses them. Helpers used by two children go in a sibling `codec.rs` / `parse.rs`, not back into `mod.rs`.

4\. Parent `mod.rs` becomes:



```rust

mod note\_store;

pub use note\_store::RedbStore; // only if that type lives there

```



&#x20;  Prefer one home for the type. Trait impls in other files use `impl NoteStore for RedbStore` and are `mod note\_store;` from the parent so they are linked. The type itself is `pub use`d from `store.rs`.

5\. Fix imports. Delete unused imports. Do not add `use super::\*`.

6\. `cargo fmt -- src/persistence/redb` (the paths you touched).

7\. Verify with native cargo. Paste the summary line into `pinetree/state/logs/YYYY-MM-DD-split-<family>.md`. Do not rewrite old logs.



A slice is done only when the old file is gone or is a non-leaf under 150 lines, `cargo test` for that crate target is green, and no caller path changed.



Forbidden in the same slice: bugfixes, renames of public functions, table-name edits, JSON field edits, "while I'm here" refactors, new features.



\## Inventory (all of `src/`, not only the two named files)



Threshold for action: 800 non-blank lines. Files under that stay unless a split of a neighbor forces a move.



\### P0 — persistence



`src/persistence/redb/mod.rs` is 12,591 non-blank lines. One `RedbStore` (line 427) and a long chain of trait impls. `src/persistence/foundationdb/mod.rs` is small today; when it grows, mirror these filenames so the two backends stay comparable. Do not start an FDB port in this program.



Target layout, `src/persistence/redb/`:



| File | Move from current `mod.rs` |

| --- | --- |

| `mod.rs` | `mod` lines and `pub use store::RedbStore` only |

| `store.rs` | `RedbStore`, `impl RedbStore` at 436, 3496, 4330, 11012, 12910. Open/commit/table helpers that every impl needs |

| `codec.rs` | `map\_redb\_err`, `encode\_json`, `decode\_json`, `trim\_to\_none`, and other free functions used by more than one impl |

| `account\_store.rs` | `impl AccountStore` at 4504 |

| `note\_store.rs` | `impl NoteStore` at 4853, `matches\_cursor` |

| `favorite\_store.rs` | 5892 |

| `emoji\_store.rs` | 5972 |

| `antenna\_store.rs` | `AntennaStore` 6328 and `AntennaNoteStore` 6524 |

| `user\_list\_store.rs` | 6613 |

| `clip\_store.rs` | 6988 |

| `page\_store.rs` | 7505 |

| `job\_queue.rs` | `impl JobQueue` 7881, `truncate\_fail\_reason` |

| `follow\_store.rs` | 8198 |

| `block\_store.rs` | `BlockStore` 8449 and `MuteStore` 8545 if they share key helpers; otherwise split |

| `host\_caps\_store.rs` | 8422 |

| `reaction\_store.rs` | 8813 |

| `poll\_store.rs` | 9008 |

| `activity\_dedup.rs` | 9177 |

| `media\_store.rs` | 9671 |

| `notification\_store.rs` | 10224, `notification\_order\_key` |

| `auth\_store.rs` | `AuthStore` 10368, `SigninStore` 10740, `PasswordResetStore` 10867 |

| `registry\_store.rs` | 10919 and `registry\_\*` key helpers |

| `moderation\_store.rs` | `ModerationLogStore` 3592, `AbuseReportStore` 3757, `AbuseReportRecipientStore` 11820, `UserMemoStore` 11928 |

| `instance\_settings\_store.rs` | 3963 |

| `invite\_store.rs` | 4079, `matches\_invite\_filter` |

| `drive\_folder\_store.rs` | 11084 |

| `webhook\_store.rs` | `SystemWebhookStore` 11450, `UserWebhookStore` 11598 |

| `gallery\_store.rs` | 12015 |

| `announcement\_store.rs` | 12406 |

| `note\_thread\_mute\_store.rs` | 11968 |

| `backup.rs` | from `export\_typed\_table` at 12730 through `verify\_backup\_integrity` at 13114, including `BackupTableCount` |



If any child is still over 800 lines after the move (`note\_store.rs` is the likely one), split by method group only after the first compile is green: `note\_store\_write.rs` and `note\_store\_read.rs`, both `impl NoteStore for RedbStore`.



`src/application/ports.rs` (3,243) is the trait mirror of the redb file. Split it in the next slice, after redb compiles, into `src/application/ports/` with one file per trait and the same stems (`note\_store.rs` holds `trait NoteStore`). `ports/mod.rs` re-exports every trait so `crate::application::ports::NoteStore` still resolves. Do not let a redb agent and a ports agent run at the same time.



\### P0 — Misskey API dispatcher



`src/api/misskey/endpoints/mod.rs` is 25,485 non-blank lines. Production handlers run to about line 14339; `mod tests` is the rest. `build\_router` (line 135) already delegates federation to `ap\_webfinger`, `ap\_actor`, `ap\_objects`, `ap\_collections`, and `fed\_inbox`. The Misskey API is one `api\_dispatch` / `api\_dispatch\_blocking` match starting at line 3588, plus a pile of parsers and packers in the same file. That match is also the stack hazard called out in `AGENTS.md`: keep handler bodies out of the match. Each arm must be a call.



Target layout, `src/api/misskey/endpoints/`:



| File | Contents |

| --- | --- |

| `mod.rs` | `ApiState`, `build\_router`, `api\_dispatch`, `api\_dispatch\_blocking`. The match arms only call `notes::create(...)` and friends. No parsing, no JSON construction |

| `middleware.rs` | `nosniff\_middleware`, `cors\_middleware`, `request\_id\_middleware`, `preflight`, `healthz`, `ready`, readiness and ops slots (lines 243–531) |

| `parse.rs` | `parse\_json\_body`, `required\_str`, `opt\_\*`, `get\_limit`, `str\_list`, visibility and notification parsers, draft and poll parsers |

| `auth\_http.rs` | signup, signin, signin-flow, miauth, session cookie, `resolve\_caller`, `require\_caller`, password verify, sign-in recording |

| `files\_http.rs` | `serve\_file`, `serve\_file\_named`, `identicon`, `proxy\_media`, `drive\_files\_create` upload path |

| `admin\_status.rs` | `build\_admin\_status`, backup packers, pool presence |

| `dispatch\_auth.rs` | match arms `meta`, `auth/\*`, `signup`, `signin\*`, `i`, `i/update`, token and password endpoints |

| `dispatch\_users.rs` | `users/show`, `users/search\*`, `users/notes`, `users/followers`, `users/following`, `users/reactions`, `username/available` |

| `dispatch\_lists.rs` | `users/lists/\*` |

| `dispatch\_notes.rs` | `notes/create`, `show`, `delete`, `edit`, timelines, replies, renotes, children, conversation, unrenote, state, reactions, favorites |

| `dispatch\_social.rs` | `following/\*`, clips, pages, antennas, invites |

| `dispatch\_drive.rs` | `drive/files`, `drive/files/show`, and the admin drive arms |

| `dispatch\_admin.rs` | `admin/\*` except emoji and drive |

| `dispatch\_emoji.rs` | `emojis`, `emoji`, `admin/emoji/\*` |

| `dispatch\_i.rs` | `i/notifications`, `i/export-\*`, `i/import-\*`, `i/registry/\*`, `i/apps`, email, move, sign-in history |

| `dispatch\_rest.rs` | every remaining arm (`get-online-users-count`, `pinned-users`, `fetch-rss`, gallery, flash, announcements, roles, ads, channels if still present). After the move, list any arm not covered and give it a file. Do not leave a catch-all file over 800 lines |

| `tests.rs` | only if the existing `mod tests` is under 1,500 lines after handlers move. Otherwise `tests\_<family>.rs` included from `mod.rs` with `#\[cfg(test)] mod tests\_notes;` |



Each `dispatch\_\*.rs` exports one function per endpoint, named after the path with underscores (`notes\_create`). The function owns the current match-arm body. `api\_dispatch\_blocking` becomes a thin match. That is the stack fix; do not raise `RUST\_MIN\_STACK` instead.



Packing helpers that build Misskey JSON and live in this file (gallery, flash, announcement, role, ad literals found around lines 1700–1925) move next to the endpoint that returns them, or into `src/api/misskey/packers/` if `packers.rs` already has that type. Do not create a second packer for the same entity.



Sibling files that are also over the cap, same program, after the dispatcher compiles:



\- `src/api/misskey/packers.rs` (4,326) becomes `packers/` with one file per packed entity: `note.rs`, `user.rs`, `drive\_file.rs`, `notification.rs`, and whatever `struct`/`fn pack\_\*` pairs you find. `packers/mod.rs` re-exports the current public functions.

\- `src/api/misskey/errors.rs` (2,170) becomes `errors/` split by endpoint family, matching the dispatch files. Keep `ApiError` and the HTTP mapping in `errors/mod.rs` only if that file stays under 800; otherwise `error.rs` plus `codes.rs`.

\- `src/api/misskey/response.rs` (905) splits only if still over 800 after packers move. `auth.rs` (509) stays.



\### P1 — federation and ActivityPub



Split on activity type or document type, and move the existing inner `mod tests` with the code it covers.



| Current file | Non-blank | Split into |

| --- | --- | --- |

| `src/federation/inbox.rs` | 4,699 | `federation/inbox/` with `mod.rs` as the receive/verify entry, then `create.rs`, `update.rs`, `delete.rs`, `like.rs`, `announce.rs`, `follow.rs`, `flag.rs`, `vote.rs`. Tests already start at 2756 (`media\_safety\_tests`, `flag\_tests`, `key\_ownership\_tests`, `reply\_resolution\_tests`, `update\_note\_tests`, `like\_reaction\_tests`, `delete\_tombstone\_tests`, `inbound\_vote\_tests`). Each test module moves with its activity file |

| `src/federation/outbox.rs` | 2,004 | `federation/outbox/` by activity kind. Keep `slice10\_tests` with the code it asserts |

| `src/federation/delivery.rs` | 1,756 | `federation/delivery/` — queue, sign-and-post, retry loop. Tests at 1498 and 1721 move with those units |

| `src/activitypub/objects.rs` | 2,539 | `activitypub/objects/` — note document, activity wrappers, tombstone. Tests at 1960 and 2200 move with them |

| `src/activitypub/signatures.rs` | 2,435 | `activitypub/signatures/` — sign, verify, digest, draft-cavage versus RFC 9421 if both are present. Tests at 1388 move with the verifier they cover |

| `src/activitypub/webfinger.rs` | 1,911 | `activitypub/webfinger/` — XRD, JRD, nodeinfo. Tests at 537 move with the parser |

| `src/activitypub/actor.rs` | 1,745 | `activitypub/actor/` — actor document, public key, collections endpoints if they are still in this file. Tests at 1051 follow |

| `src/activitypub/federated\_collections.rs` | 1,236 | `activitypub/federated\_collections/` — clips, pages, featured. Tests at 1058 follow |

| `src/activitypub/quotes.rs` | 770 | Leave unless the objects split pulls quote rendering out and this file then exceeds 800 |



`src/federation/mod.rs` and `src/activitypub/mod.rs` stay the public module roots. Re-export every name that is `pub` today.



\### P1 — media, jobs, streaming, backup, config



| Current file | Non-blank | Action |

| --- | --- | --- |

| `src/media/reconciler.rs` | 1,283 | `media/reconcile/` by pass: scan, heal, quota. Do not merge with `remote\_cache.rs` |

| `src/media/transcode.rs` | 1,269 | `media/transcode/` — probe, ffmpeg argv, output record. Tests stay beside the function they cover |

| `src/media/remote\_cache.rs` | 1,033 | `media/remote\_cache/` — fetch, store, evict. SSRF checks stay in one file and keep their current limits |

| `src/jobs/worker.rs` | 1,231 | `jobs/worker/` — loop, claim, one file per job kind if the match is large |

| `src/streaming/server.rs` | 1,649 | `streaming/server/` — handshake, channel subscribe, write loop. Tests at 1613 move with the protocol code. `hub.rs` (426) stays |

| `src/application/commands/exports.rs` | 1,390 | `commands/exports/` — one file per export kind (`following`, `notes`, `blocking`, …) |

| `src/config.rs` | 1,334 | `config/` — struct and `validate` in `config.rs`, tests in `tests.rs`. Every JSON key stays. Do not drop a `deny\_unknown\_fields` attribute |

| `src/application/queries.rs` | 2,521 | `application/queries/` by read model: timelines, users, notes, notifications, drive. `timeline\_tests` at 1990 moves with timelines |

| `src/main.rs` | 885 | Split only the bootstrap blocks that are pure moves: `cli.rs`, `runtime.rs`. Leave `main` as the wiring function. Do not change the runtime `thread\_stack\_size` while splitting the dispatcher; revisit only after `dispatch\_fits\_bounded\_stack` is green with the thin match |

| `src/backup/bundle.rs` (694) and `restore.rs` (674) | under cap | Leave |



Commands already split (`notes.rs` 900, `drafts.rs` 999, `pages.rs` 889, `gallery.rs` 862, `admin\_ext.rs` 860, `inbound/quotes.rs` 832) are over the cap. Same rule: one file per use-case, parent re-exports. Do these after queries, not in parallel with them, because they call the same ports.



Domain files `account.rs` (1,086) and `note.rs` (837) split only if the non-test body is over 800. Move `mod tests` to `account\_tests.rs` first; if the type file then fits, stop.



\### P2 — tests



Do not split tests until the production file they hit has moved. Then colocate, or split the integration file on the same seams.



| File | Non-blank | Split |

| --- | --- | --- |

| `tests/conformance.rs` | 7,110 | `tests/conformance/` with one file per store trait, matching the redb filenames. A `tests/conformance.rs` shim can `mod` them if Cargo needs a single integration target; prefer a directory crate layout only if the existing `\[\[test]]` entry allows it. Read `Cargo.toml` before choosing. Keep every test name |

| `tests/federation.rs` | 2,589 | `tests/federation/` by activity, matching the inbox split |

| `tests/federation\_sig\_mode\_e5.rs` | 1,677 | by signature mode, only if still one file after signatures split |

| `tests/gaps\_final\_wiring.rs` | 1,401 | under the 1,500 test cap; leave unless you are already editing it |



Other `tests/gaps\_\*.rs` and `tests/storage\_\*.rs` files are under 1,000. Leave them.



\## Order and agent assignment



Sequential families, because they share types:



1\. redb move. One agent. Gate: `cargo test --lib persistence::redb` and `cargo test --test conformance --test crash\_recovery`.

2\. ports move. One agent. Gate: `cargo test --lib application::ports` plus the same conformance tests.

3\. endpoints dispatcher move. One agent, no subagents inside this family. The match and `mod.rs` are shared. Gate: `cargo test --lib api::misskey::endpoints` and `cargo test --test client\_smoke --test gaps\_api\_admin --test gaps\_api\_auth --test gaps\_api\_drive --test gaps\_api\_gallery --test notes\_like\_contract`.

4\. packers, then errors. Separate agents, disjoint files, after step 3.

5\. federation inbox, then outbox, then delivery. One family at a time.

6\. activitypub objects, signatures, webfinger, actor, federated\_collections. One family at a time. Signatures and webfinger are disjoint and may run together only if neither edits `activitypub/mod.rs`; the parent applies the `mod` lines.

7\. media, jobs, streaming, exports, queries, oversized command files.

8\. conformance and federation integration-test splits.

9\. config and main only if still over cap.



Never run two agents whose files both `use` a module that is mid-split.



\## Deliverables per slice



Each slice produces all of the following. A slice without the log is not done.



\- The new files, the slim parent, and deleted or emptied old body.

\- `cargo fmt` on the touched paths.

\- A log at `pinetree/state/logs/YYYY-MM-DD-split-<family>.md` containing: files moved, old and new line counts, the exact `cargo test` command, the native-Windows summary line (`N passed, 0 failed`), and a statement that WSL was not used.

\- A short note in that log if any name could not keep its path, and the `pub use` that preserves it.

\- No `Cargo.toml` dependency changes. No contract doc edits unless a path cited in `docs/` moved; if it did, update the path in the same slice and say so.



Program-level deliverable, after P2:



\- A table in the final log: every former over-cap file, its new children, and each child's non-blank line count. Any child still over 800 is an open item, not a silent exception.

\- `cargo test --offline` (native) green for lib and the integration tests listed above. If the full suite is too long, run it, but investigate a quiet process the way `AGENTS.md` requires: identify the target, compare artifact mtimes, check CPU, do not pipe through `tail`, do not declare "still compiling."



\## Stop conditions



\- A move that requires a logic change to compile is a wrong seam. Put the shared helper in the sibling utility file instead of editing behavior.

\- A test that fails after a pure move means a missed `mod` or a visibility mistake. Fix the module graph. Do not change the assertion.

\- Do not split `src/persistence/foundationdb/` until it crosses 800 lines, and then copy the redb filenames rather than inventing new ones.

