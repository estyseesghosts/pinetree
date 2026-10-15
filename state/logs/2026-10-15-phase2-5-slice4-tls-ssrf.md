# 2026-10-15 — Phase 2.5 Slice 4: Split TLS from SSRF

- Removed `-k` from `send_https_via_curl` (`federation/delivery.rs`): the
  `allow_private_ips` SSRF testing exemption no longer disables TLS verification.
  No replacement/insecure-TLS flag added — survey found nothing needing one.
- `allow_private_ips` documented (field + module docs) as an SSRF exemption ONLY,
  never affecting TLS. SSRF check behavior unchanged.
- New `curl_args` pure helper builds the exact prior arg list minus `-k`;
  `send_https_via_curl` uses it. New test
  `curl_args_never_disable_tls_and_keep_ssrf_pins` asserts no `-k`/`--insecure`
  (POST+GET) and that `--resolve` pinning + `--max-redirs 0` survive.
- `scripts/smoke-phase2.sh` unaffected by inspection (plain `http://` loopback
  only; HTTPS curl path unreachable there). Live run deferred to Slice 9 gate.
- Deferred (no code): replacing the curl shell-out, overall request deadline
  (`--max-time` is per-hop; redirect loop has no total budget), DNS pinning
  (only `addrs[0]` pinned via `--resolve`).
- Verified: full `cargo test` green — 114 lib (incl. new test) + 29 conformance,
  0 failed; grep confirms no `cmd.arg("-k")` remains in `src/`.
