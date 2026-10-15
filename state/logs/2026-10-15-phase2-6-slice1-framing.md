# 2026-10-15 — Phase 2.6 Slice 1: HTTP response framing (R4 + D7)

## What changed
- `src/federation/delivery.rs`
  - `curl_args`: added `--raw` (curl must NOT de-chunk; our parser does),
    `-q` (ignore `~/.curlrc`), `--noproxy "*"` (ignore proxy env; proxies are
    NOT a supported config — DNS is pinned via `--resolve`, which a proxy
    would bypass). Kept `-sS -i --http1.1 --max-redirs 0 --max-time
    --max-filesize --resolve`. Doc comment updated with the why.
  - `read_http_response`: loops to skip interim header blocks — any `1xx`
    status, plus proxy-CONNECT `200 Connection established` (matched on the
    reason phrase) — discarding each interim block and parsing the next one
    from already-buffered bytes. 32 KiB per-block cap kept; max 8 interim
    blocks (returns Err beyond). Final-block redirect (`Location`) and
    Content-Length / size-limit behavior unchanged.
  - `decode_chunked`: hardened — `checked_add` for `chunk_size + 2`, `get()`
    slicing everywhere, trailing CRLF validated as actual `\r\n`, empty hex /
    invalid hex rejected, chunk-extension after `;` must be visible ASCII
    (SP/TAB allowed; controls, DEL, non-ASCII rejected — `5;ext=foo` still
    decodes), chunks larger than remaining input or that would push decoded
    output past the encoded input budget rejected, missing terminal `0` chunk
    treated as truncation. No panics on `ffffffffffffffff` / truncated /
    missing-trailer inputs.
  - Tests: extended `curl_args_never_disable_tls_and_keep_ssrf_pins` with
    `--raw`/`-q`/`--noproxy "*"` assertions; new
    `decode_chunked_hardening_rejects_garbage_without_panicking`,
    `read_http_response_skips_100_continue_and_decodes_chunked`,
    `read_http_response_skips_proxy_connect_block`.
- `src/persistence/redb/mod.rs` (`claim_next` only): reclaiming an expired
  lease (`lease_expires.is_some()`, i.e. previously claimed) does
  `attempts.saturating_add(1)`; if `attempts >= MAX_JOB_ATTEMPTS` the job is
  quarantined (`failed=true`, `fail_reason="retry-exhausted"`, no new
  due-index entry, still inspectable via `get_job`) and `claim_next` returns
  `None`. Fresh jobs (`lease_expires == None`) never increment. Old due entry
  removed in all cases; new `(lease, id)` entry only for returned jobs.
- `src/application/ports.rs`: `JobQueue::claim_next` doc comment now states
  the lease-expiry-counts-as-attempt + quarantine semantic.
- `src/jobs/worker.rs`: `process_one` wraps handler dispatch in
  `catch_unwind(AssertUnwindSafe)` → panic becomes
  `Quarantine("panic: …")` via `fail_permanent` (new `dispatch_job` helper
  holds the unchanged Disposition logic; new `panic_payload_message` helper).
  `drain` catches unwinds per step and continues (lease left to expire, which
  now burns an attempt) instead of unwinding. `spawn`'s `spawn_blocking`
  closure catches unwinds and returns `false` so the background loop never
  dies silently (`process_one` already settles handler panics via quarantine).
- `tests/conformance.rs`: new cell `b1b_lease_expiry_counts_as_attempt`
  (generic `case_b1b_lease_expiry_counts_as_attempt` over `ConformanceStore`,
  instantiated with `RedbFactory`): claim (attempts 0) → advance past lease →
  reclaim asserts same id, `attempts == 1`, not failed → `fail()` + advance →
  reclaim asserts `attempts == 2`.

## Test evidence
- Caller-verified 2026-10-15 after implementer handoff (working tree as left):
  `cargo test` green — lib 111 passed (108 baseline + 3 new delivery tests),
  conformance 36 passed (35 baseline + b1b), client_smoke 1, crash_recovery 2,
  federation 2, federation_interop 1, minrepro 1. Total 154 green, 0 failed.
  (Implementer deliberately did not execute tests per repo rule; evidence above
  closes that gap.)
- Test-first intent: the new tests fail on the pre-slice code (no
  `--raw`/`-q`/`--noproxy` in `curl_args`; `decode_chunked` panics/accepts
  on `ffffffffffffffff` + truncated + missing-trailer + control-char
  extensions and accepts unterminated bodies; `read_http_response` treats a
  `100`/`Connection established` block as final; `claim_next` never
  increments on reclaim so `attempts == 1` fails). Verified by reasoning
  only — no execution performed (see above).

## Open questions / decisions
- Interim-block cap of 8 is a new bound (not in the slice text); picked the
  safer option (bounded work vs. unbounded `100 Continue` stream). Log here
  for review.
- Proxy-CONNECT detection is reason-phrase matching (`200` +
  case-insensitive `"connection established"`); a real origin returning that
  exact reason with a body would be skipped once — accepted as safer than
  treating proxy framing as a body (no legitimate AP origin uses it).
- Chunk-extension policy allows SP/TAB separators and otherwise requires
  visible ASCII (`0x21–0x7E`); `;` with an empty remainder is rejected.
  Strict terminal-`0`-chunk requirement means a bare `5\r\nhello\r\n`
  (no terminator) is now `Err(truncated)` — intended per "truncated body".
- Other `JobQueue` impls (test doubles in `webfinger.rs`/`packers.rs`) were
  intentionally NOT touched per slice scope; only `RedbStore::claim_next`
  implements the new semantic. FDB inherits via the port doc + conformance
  cell in Phase 4.
- `MAX_JOB_ATTEMPTS` value unchanged (8). No new dependencies. No changes to
  actor/inbox/signatures/keys/media/outbox/passwords/AP collections or
  frozen docs (except the `ports.rs` trait doc comment as allowed).
