# Roadmap

From `sns-server-proposal.md` §9 (the proposal wins on any disagreement).

| Phase | Scope | Exit criterion | Status |
|---|---|---|---|
| 0 | Freeze contracts: endpoint/streaming manifest, domain types, identity rules, visibility policy, ports, persistence conformance tests; FDB client prototype | IDs/URI rules fixed; a note + durable job commit atomically through the storage interface | ✅ done |
| 1 | Single-node server: redb adapter, accounts, tokens, notes, follows, reactions, local timelines, MiAuth, durable jobs, local media | Core flows survive restart/crash; real Misskey client can register, post, read timelines, follow, react | ✅ done |
| 2 | Minimal federation: WebFinger, actors, signatures, inboxes, core activities, dedup, durable delivery | Local and remote follow/post/react work end-to-end with real Misskey and Sharkey; transient failures retry without duplicating state | ✅ done |
| 2.5 | Cleanup: repo reproducibility, `commands` split, Reject atomicity, TLS/SSRF split, job quarantine, maintained RSA, AP/Misskey decoupling, visibility decision, docs + gate | One commit per slice with executed evidence; clean-clone suite and live interop green | ✅ done 2026-10-15 |
| 3 | Client compatibility and streaming: streaming channels, note capture, notifications, attachments, richer note fields, endpoint coverage driven by real client tests | Every endpoint in the declared compatibility profile passes contract tests against the target client versions | ✅ done 2026-10-16 |
| Quote posts | FEP-044f canonical outbound + legacy inbound compat + Misskey projection + request/accept/reject/revoke workflow | 4-server + elixir interop green, conformance Group Q | ✅ done 2026-10-16 |
| Branched replies | Indexed direct-child lookup + migration, visibility-aware `notes/replies`, pending parents with fail-closed reads, durable resolver + opt-in repair | Full suite + reply-burst stress `STRESS_OK items=28` green | ✅ done 2026-10-16 |
| 4 | FoundationDB multi-node: full FDB adapter, durable multi-worker jobs, global directory, owner routing, secure cross-node ops, shared media | `@a@xyz` on node `foo` and `@b@xyz` on `bar` keep canonical identities, interact locally and federate externally as one instance; restarts, duplicate jobs, routing, account moves tested | planned |
| 5 | Scale and hardening: benchmark contention, timeline strategies, queue growth, big follower lists, federation backlogs, node failure; add caches/hybrid fan-out only when measured | Documented performance targets and budgets met for small redb and representative multi-node deployments | planned |

## Phase 3 in more detail (what "complete" means)

1. **Declare the compatibility profile** — name the target client versions (at minimum the Misskey web client of the pinned checkout, `misskey-js`, plus any mobile/third-party clients the owner chooses) and list every endpoint/channel/field in scope; extend `docs/endpoint-manifest.md` accordingly.
2. **Streaming:** authenticated WebSocket `/streaming`, channel connect/disconnect, `main`, `homeTimeline`, `localTimeline`, `hybridTimeline`, `globalTimeline`, note capture (`subNote`/`unsubNote`, `noteUpdated`), notification events — fed from the existing `StreamingSink` jobs.
3. **Common interactions:** replies/mentions, notifications, attachments (drive listing/folders as clients need), richer note fields, remaining client-visible profile fields.
4. **Optional, only if in the profile:** quotes, polls, custom emoji, mute/block, lists, search.
5. **Contract tests against the real clients** (not mocks) for every endpoint and channel in the profile.

## Later / explicitly deferred

Polls and `_misskey_votes`, newer HTTP message signatures (RFC 9421), OAuth, public-internet federation soak, Mastodon client API (non-goal), antennas/clips/pages/Reversi/chat and other legacy Misskey features (non-goals unless a target client requires them).
