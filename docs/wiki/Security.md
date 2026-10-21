# Security

Status: living page, 2026-10-21. Sources: `src/api/misskey/auth.rs`, `src/activitypub/signatures.rs`, `src/federation/{delivery,inbox,fetch_gate}.rs`, `src/media/remote_cache.rs`, `src/domain/visibility.rs`.

## 1. Credentials

Passwords Argon2id (`m=19456,t=4,p=1`) + legacy `salt:hash` verify-and-upgrade on blocking pool. Tokens SHA-256 hex, `Bearer → body.i → cookie` order, `AuthenticatedCaller{account,permissions,first_party}`; `require_permission/moderator/admin/secure` per endpoint; scoped MiAuth/legacy tokens enforced, first-party (password/MiAuth-approval) bypass scopes. Suspended ⇒ 403, deleted ⇒ 401, noscope ⇒ 401. MiAuth: server-rendered approve (XSS-escaped, callback blocklist `javascript/file/data/mailto/tel/vbscript`), 30-min session, one-shot check. Passkey stub 400 by decision; 2FA token accepted-ignored (no store).

## 2. Federation trust

HTTP Signatures required on inbox (cached key, fetch-on-miss, one rotation, `(request-target) host date` + `Digest`, 300 s skew, `rsa-sha256|hs2019`); `actor==signer` + object-authority gates; failures ⇒ 401/404, never serve. Fetch gate: same verification but failure ⇒ anonymous viewer + Serve/403/404 mapping (anonymous→404, never confirm existence). RFC 9421 only logged.

SSRF/size/timeouts on every outbound fetch: http/https only, literal+resolved IP vs loopback/RFC1918/link-local/CGNAT/multicast/ULA/doc/mapped, `allow_private_ips` exempts IP only (TLS always verify, curl `--resolve`, no `-k`), 10 s/20 s/5 s DNS, ≤3 redirects no downgrade per-hop re-validate, 64 KiB AP/WebFinger, 32 KiB headers, strict chunked decoder, Content-Length pre-check. Remote media: ignore declared type, sniff only, HTML/SVG→octet-stream, 16/note, 64 MiB/note, `max_file_size` enforced.

Inbound Articles: `script` never stored (`""`) and never serialized (no code path); block renderer escapes text + fixed tag whitelist + server-constructed image URLs. Inbox 64 KiB cap, dedup atomic, tombstones distinguish delete-vs-hidden.

## 3. Abuse controls

Visibility single `is_visible` + `FeedFilter` on every read/streaming/capture, fail-closed pending; muted/blocked/renote-mute/word-mute server-enforced; instance blocklist suffix-match inbound/read/outbound; private follow-lists + mod bypass; notifications gated at Notify choke. Rate limits per-family + `Retry-After`; 1 MiB JSON ⇒ 413; CORS `*` (safe for `credentials:omit`); `nosniff`; destructive admin (`reset-db`, `queue delay/clear`) loud-400; transport failures loud-500 with stable UUIDs.

Open: RFC 9421 enforce, shared limiter for multi-node, metrics/audit logs, backup/restore, `TODO(final)` UUID verification (~20 sites). See `docs/TODOS.md`.
