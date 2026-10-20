# Pinetree forward plan — use Catodon as reference, not as template

**Status (2026-10-20 freshness pass):** plan text below is unchanged. Each numbered item is marked against pinetree code. `file:line` is cited for every LANDED or OPEN claim. `n/v` = not re-verified this pass.

| # | Item | Status | Evidence |
|---|---|---|---|
| A1 | SSRF re-validated per hop | **LANDED** | `src/federation/delivery.rs:527` (`resolve_and_validate_url` runs every loop iteration) |
| A1 | DNS pinned via `--resolve` | **LANDED** | `delivery.rs:446-471` |
| A1 | `https→http` redirect refused; ≤3 hops | **LANDED** | `delivery.rs:48`, `:541-549`, `:560-567` |
| A1 | Re-sign GET per hop with system key | n/v | — |
| A1 | Re-run federation-allowed + blocked-host check after each `Location` | **OPEN** | instance-block check is inbound + `deliver_single` only (manifest §25); not per hop |
| A1 | `Link: rel=alternate` before HTML fallback | **OPEN** | no `rel="alternate"` handling in `src/` (grep) |
| A1 | PSL registrable-domain host compare; `sanitizeInlineObject` | **OPEN** | no `haveSameAuthority`/registrable/inline-sanitize in `src/` (grep) |
| A2 | Atomic `FederateDeliver`, transient/permanent split | **LANDED** | `delivery.rs:51-82` (`SendErrorKind::is_transient`); `retry.rs:58` (`300..=499` → permanent) |
| A2 | Planner: direct `to/cc` first, followers coalesced by `sharedInbox` | **partial** | preferred-inbox fan-out, followers paged 1000 (inventory §1.9, `src/federation/outbox.rs:307-1929`) |
| A2 | `410 + shared → goneSuspended`; `not-responding 7d → autoSuspended` | **OPEN** | no `suspension_state`/`auto_suspend`/`not_responding` in `src/` (grep) |
| A2 | Dead-host 1 h cache | **OPEN** | not found |
| A3 | Instance blocks (blockedHosts), subdomain match, inbound + outbound | **LANDED** | manifest §25; `queries.rs` `suspended_or_host_blocked`; `deliver_single` |
| A3 | `federation: all\|specified\|none` mode switch; `silencedHosts`/`mediaSilencedHosts`; per-user domain blocks | **OPEN** | no hits in `src/` (grep) |
| B4 | Authorized fetch (signed GET verify) | **partial** | `src/federation/fetch_gate.rs` `authenticate_fetch_viewer` covers collections/pages only (manifest §36) |
| B4 | `allowUnsignedFetch` per user | **OPEN** | no hits in `src/` (grep) |
| B5 | `actor == signer` | **LANDED** | inventory §1.9 (`inbox.rs:183-355`) |
| B5 | Replay gate before fetch; atomic dedup | **LANDED** | `src/federation/inbox.rs:1592` (replay gate before store lookups); inventory §1.9 |
| B5 | `signerHost == activity.id` host; LD `RsaSignature2017` | n/v / **OPEN** | not found by grep |
| B6 | Quote abuse guard (`QuoteRequest` 30/actor, 5/pair per 300 s) | **OPEN** | no per-actor/pair limiter found in `src/federation/` or quote command paths (grep) |
| C7 | `Like` VS16 strip | **LANDED** | `src/domain/reaction.rs:52`, `:77`, `:157`, `:193` (normalized at write, so outbound `_misskey_reaction` is bare) |
| C7 | Suppress `_misskey_reaction` for mastodon/akkoma/pleroma/iceshrimp | **LANDED** (for plain-`Like` hosts) | `src/federation/host_caps.rs:44-54`; `src/activitypub/objects.rs:991-1003` (plain `Like` carries neither field) |
| C7 | `Flag.object` must be array | n/v | — |
| C7 | `Group Announce` ignore; `quote`/`_misskey_quote` + `RE:` fallback kept | Group: **n/a** (no groups); quote carriers **LANDED** | inventory §1.9 (`quotes.rs:123-486`) |
| C8 | Outbox page size 20 | **divergent (kept at 40)** | inventory §1.9 (`actor.rs:719-1048`, 40/page) |
| C8 | `null` for removed/hidden/orphan outbox items | n/v | — |
| C8 | WebFinger `422/404/max-age=180`; NodeInfo `10m cache` | n/v | inventory §1.9 (JRD+XRD, NodeInfo 2.0/2.1 present) |
| C8 | `ApLogService` inbox/fetch logs with expiry | **OPEN** | `src/observability.rs:21` no-op init (inventory §3.3) |

Summary: the resolver/delivery hardening core (A1 SSRF-per-hop, pinning, downgrade refusal, hop cap; A2 transient/permanent classes) and most of A3's block list are in. Open: per-hop block re-check, `rel=alternate`, PSL host compare, `allowUnsignedFetch`, the `federation` mode switch and silenced hosts, the dead-host suspension state machine, the quote-abuse guard, and the AP log table. The "Where you are" bullets below were spot-checked through the items in this table. Anything not in the table was not re-verified.

Catodon is the right reference because it already survived what Pinetree is about to hit: `Misskey + Sharkey + Mastodon + Lemmy/Pleroma` simultaneously. Don't port it. Steal its federation rules, keep your architecture.

Your constraints stay: `domain <- application <- adapters`, state+job atomically, FDB `5s / 10MB`, no blobs in tx, verify by execution (`cargo test`, `smoke-all.sh`, `smoke-phase2.sh` 4-server, elixir triangle).

## Phase A — resolver + delivery hardening (highest interop value)

**1. Per-hop re-sign + re-gate**
Where you are: `SafeHttpClient::send` validates SSRF per hop, pins DNS via `--resolve`, refuses `https->http`, `3` hops. Good.
Add from `ApRequestService.ts:354-735`, `ApResolverService.ts:57-400`:
- re-sign GET per hop with system key, re-run `isFederationAllowedHost` + `isBlockedHost` after each `Location`.
- support `Link: <...>; rel=alternate type=activity+json` before HTML fallback. Skip WP/Discourse special-cases for now.
- switch host compare to PSL registrable-domain (`haveSameAuthority`), enforce `sanitizeInlineObject` for `inbox/sharedInbox/collections/publicKey`.

Accept: `cargo test` resolver unit + live fetch Misskey/Sharkey/Lemmy object with redirect still resolves, blocked-host redirect is refused.

**2. Delivery terminal states**
Where you are: `FederateDeliver` atomic, `SendErrorKind transient/permanent`, backoff in worker, shared-inbox dedup.
Add from `DeliverProcessorService.ts:55-163`, `ApDeliverManagerService.ts:136-329`:
- planner: direct `to/cc` first, then followers coalesced by `sharedInbox`. Missing row = drop, don't fan out to per-user inboxes.
- worker: `410+shared -> goneSuspended + Unrecoverable`, other `4xx -> Unrecoverable`, `5xx/429/DNS/timeout -> retry 2^n min <=8h +20%`, `not-responding 7d -> autoSuspended`. Add `Instance.suspensionState` + 1h cache so dead hosts aren't hammered.
- keep your `410/404 delete sub` push logic separate — same pattern.

Accept: kill `-refuse` host stops after 1 attempt, dead host suspends, live host still delivers after transient 500.

**3. Federation gates**
Add `Meta{ federation: all|specified|none, federationHosts, blockedHosts, silencedHosts, mediaSilencedHosts, refusedSoftware }` + per-user domain blocks (`InstanceBlockService.ts:120,180 sever()`).
Check inbound after auth, outbound before send. Suffix-match `.host endswith .entry`, self always allowed.

## Phase B — auth-fetch + inbox completeness

**4. Authorized fetch (minimal)**
From `ActivityPubServerService.ts:215-278,439-533`:
- `GET` verify requires `(request-target) host date`, dual verify with/without query, refetch-once.
- `allowUnsignedFetch (always|essential)` per user, staff falls back to instance. Unsigned `always` -> accept, else `401` or redacted `Person` (`renderPersonRedacted`).
- never leak private note/quote target body or stamp through `QuoteRequest` decision or stamp deref. You already gate visibility — add the redacted fallback.

Skip private forums (`signed-Catodon + follower-instance count`, `?is_follower=`) unless you build forums.

**5. Inbox checks you're missing**
From `InboxProcessorService.ts:45-326`:
- `signerHost == activity.id host`, puny `keyId` host, early allowlist drop.
- keep your `actor==owner (not keyId string) + publicKey.id==keyId + replay-gate-before-fetch + atomic dedup`. That's already stricter than Catodon.
- LD `RsaSignature2017`: implement `compact -> forbid @included/@graph/@reverse -> freeze() -> verify` *only* for relay path, or explicitly log-drop. Don't block main path on it.

**6. Quote abuse guard**
From `ApInboxService.ts:283`: `QuoteRequest 30/actor +5/pair per 300s` fixed window. Add it — quotes are free spam today. Keep your existing `Pending/Approved/Rejected/Revoked/Unapproved/Unknown`, stamp `interactingObject/interactionTarget/attributedTo` validation, depth guard, `RE:` never auto-trusted.

## Phase C — interop quirks + collections polish

**7. Quirks (cheap, high compat)**
From `ApRendererService`, `contexts.ts`, `type.ts`:
- `Like` VS16 strip + suppress `_misskey_reaction/content` for `mastodon|akkoma|pleroma|iceshrimp`
- `Flag.object` must be array (Pleroma #641)
- `toot:featuredTags`, `followers?is_follower=` must not 500
- `Group Announce` -> ignore as renote (you have no forums), don't crash
- you already handle `quote/_misskey_quote` slash/no-slash + `quoteUrl/quoteUri` + e232 `Link` + `RE:` fallback — keep verbatim.

**8. Outbox + discovery**
From `ActivityPubServerService.ts:627-947,981-1065`, `WellKnownServerService.ts:45-163`, `NodeinfoServerService.ts:40-160`:
- outbox `20/page`, `null` for removed/hidden/orphan, gate all collections by auth-fetch.
- WebFinger `422 remote / 404 hidden / max-age=180`, JRD+XRD. NodeInfo `10m cache / max-age=600 / protocols=[activitypub]`.
- add `ApLogService` inbox/fetch logs with expiry + `cleanupApLogs */10` — you need this to debug live fed.

## Explicitly skip

- Forums/`Group` writes, C2S (`POST /users/:x/outbox`), relays + LD-sign relays, Mastodon `/api/v1/*` + Mastodon WS, Sharkey `service-server` ML + `spam/embed/score` queues, `assetlinks`, `oauth-authorization-server` (404 is fine without OAuth).

## Order

1. A1-A3 (resolver, delivery, gates) -> re-run `smoke-phase2.sh` + elixir triangle.
2. B4-B6 (auth-fetch, inbox checks, quote limit) -> add `federation.rs` cases for `410 gone`, `private redacted`, `quote flood`.
3. C7-C8 (quirks, outbox/discovery/logs) -> keystone-sweep + interop lab.

Each step: update `docs/` contract in place, add `redb` conformance case (FDB inherits it), log real output in `state/logs/`. No step is done on reasoning alone.
