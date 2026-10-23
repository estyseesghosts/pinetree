# 2026-10-23 Wave 3 — SMTP injection and webhook SSRF (adversarial-3)

Two subagents, disjoint files. Parent commit after both finished.
Left uncommitted by agents; parent formatted one test import line, verified
read-only, staged only Wave 3 files, committed.

## Changed (Wave 3 only)
- `src/domain/webhook.rs` (SMTP agent) — `validate_email_shape` rejects CR,
  LF, `<`, `>`, `,`, whitespace, other controls before unchanged
  length/single-`@`/dot rules. Domain stays pure (`std` + `serde`).
- `src/services/email.rs` (SMTP agent) — `EmailMessage::validate`
  canonical-trims + re-validates; `SmtpEmailSender::send` re-validates
  canonical `smtp_from` before `connect()`, re-checks each trimmed recipient
  before `RCPT TO:<…>` (fail closed `InvalidMessage`); `render()` uses
  trimmed recipients only. Subject path untouched.
- `src/application/commands/webhooks.rs` (webhook agent) —
  `execute_webhook_deliver` builds strict `SafeHttpClient`
  (`allow_private_ips: false`, inherits timeouts/UA) at the single choke
  point; dispatch arm needed no change.
- `src/federation/delivery/client/send.rs` (webhook agent) — new
  `reject_crlf_headers` at top of `send()` and `send_with_resolver()`;
  CR/LF in any header name/value returns permanent `InvalidUrl` before I/O.
- `tests/wave3_smtp_inject.rs` (new) — 5 tests: shape gate, `validate`
  rejection incl. padded input, two socket tests (loopback `accept` sees
  zero connections).
- `tests/wave3_webhook_ssrf.rs` (new) — 3 tests: metadata IP
  `169.254.169.254` → `SsrfBlocked`, loopback blocked on webhook path,
  CRLF header value never hits wire. Parent ran `rustfmt --edition 2024`
  on this file (import order only).

Not touched: `validate_hook_url` accepting `127.0.0.1` kept by design
(shape gate; SSRF enforced at delivery). No `src/config.rs`, `docs/*`,
router, schema, or Wave 1/4 files in this commit.

## Verified (subagent runs, native cargo, shared target dir)
- `cargo test --locked --offline --test wave3_smtp_inject` → 5 passed.
- `cargo test --locked --offline --lib -- domain::webhook services::email`
  → 5 passed.
- `cargo test --locked --offline --test gaps_email --test gaps_webhooks`
  → 10 + 12 passed.
- `cargo test --locked --offline --test wave3_webhook_ssrf` → 3 passed.
- `cargo test --locked --offline --lib federation::delivery::` → 14 passed.
- `cargo test --locked --offline --lib jobs::worker` → 2 passed.
- `rustfmt --edition 2024 --check` on all 6 touched files → clean
  (parent-fixed import order in `wave3_webhook_ssrf.rs`).

## mtime evidence (fresh-binary rule)
- `wave3_smtp_inject-d5cb254e4911c630.exe` 8:55:04 PM >
  `tests/wave3_smtp_inject.rs` 8:52:48 PM, `webhook.rs` 8:48:13 PM,
  `email.rs` 8:48:44 PM.
- `wave3_webhook_ssrf-009c5c650b6f9a7c.exe` 9:08:05 PM > `send.rs`
  8:54:41 PM, `webhooks.rs` 8:54:48 PM, test 9:07:52 PM (parent fmt
  touched only whitespace order after the green run; re-check clean).

## Old-code failure note
- SMTP: old shape checked only length/`@`-count/dot; witnesses the old
  code accepted: `"a@b.example\r\nBcc: x"`, `"a@example.com\rDATA\r"`,
  `"<a@example.com>"`, `"a,b@example.com"`, `"a b@example.com"`.
- Webhook: old code inherited permissive client → dial to
  `169.254.169.254:80`/`127.0.0.1:9` gave transient Connect/Timeout, not
  `PermanentFailure("SSRF blocked…")`; old header path wrote raw
  `"{k}: {v}\r\n"` and connected (socket test `accept()` succeeded).

## Open
- Wave 1 (`store_backfill.rs` H2) shares the file with the in-flight Wave 4
  scans/counters agent (post-verification edit 9:31 PM broke the Wave 1
  mtime ordering + `rustfmt`); Wave 1 commit waits for scans to land, then
  parent reconciles and commits accounting once.
