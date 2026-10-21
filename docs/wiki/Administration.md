# Administration

Status: living page, 2026-10-21. Sources: `src/config.rs:140-374`, `src/main.rs:257-403`, `src/api/misskey/endpoints/mod.rs` (`admin/*`), `src/application/commands/{moderation,admin_ext}.rs`, `docs/endpoint-manifest.md` §§16,21,43. Contracts win on disagreement.

## 1. What admins can do

Moderation (all real, jobs atomic): `admin/show-user|show-users|update-user-note|delete-account|unset-user-avatar|unset-user-banner|delete-all-files-of-a-user|drive/files|show-file|show-moderation-logs|suspend-user|unsuspend-user|reject-quotes|abuse-user-reports|resolve|forward-abuse-user-report|get-user-ips|find-by-email|reset-password|unset-mfa|update-proxy-account|proxy-account|roles/*(8)|relays/*|federation/*|announcements/*|ad/*|emoji/*(full+6 bulk+copy+list-remote+import-zip)|send-email|queue/stats|deliver-delayed|inbox-delayed|clear(loud-400)|invite/*|meta|update-meta`.

- Reports: `users/report-abuse` → `admin/abuse-user-reports|resolve|update|forward`. Moderator refusals 400 `INVALID_PARAM` (upstream untyped 500).
- Suspend drops live sockets (upstream does not). `deleteAccount` tombstone-no-inline-note-delete. `delete-all-files-of-a-user` + `drive/*` cascade.
- Roles: `admin/roles/*` stores policies verbatim, NOT enforced (`requiredRolePolicy/prohibitMoved` not gated — documented gap §43.5). `is_admin/is_moderator` booleans gate everything; no custom-role checks.
- Announcements/ads/relays/federation overrides: full CRUD; `federateClips/federatePages` switches live here (default OFF).
- Emoji: `add|update|delete|list + add-aliases-bulk|remove-aliases-bulk|set-aliases-bulk|set-category-bulk|set-license-bulk|delete-bulk|copy{host,name}|list-remote|import-zip`, `export-custom-emojis`, `v2/admin/emoji/list`. Remote id `{host}:{shortcode}`.
- Queue: `queue/stats|admin/queue/stats` real; `deliver-delayed|inbox-delayed|clear` are loud 400s (no BullMQ). `reset-db` loud 400 (destructive guard).
- Email: `admin/send-email` → durable `EmailSend` job; without SMTP ⇒ loud `EMAIL_NOT_CONFIGURED`. Fake-SMTP tested.
- Invites: `admin/invite/*` + `invite/*(create|list|delete|limit)`.

Missing vs Misskey: `roles/*` public, `charts/*`, `federation/*` full, `admin/queue/*` BullMQ, `drive/clean*`, `captcha`, `avatar-decorations`, `promo/create`, `get-table-stats`, `database`, `bot-protection`. See [Client API](Client-API.md).

## 2. Configuration (JSON schema)

File `--config path/to/pinetree.json` + CLI flags override + `validate()` once. Every key optional with default; unknown keys are hard errors. Round-trip tests per family in `config.rs:430-873`.

```jsonc
{
  "public_url": "https://example.com",      // required shape, no node hostnames in IDs
  "mode": "Single",                          // "Cluster" rejected (Phase 4)
  "data_dir": "./data",
  "listen": "127.0.0.1:3000",
  "max_file_size": 10485760,
  "preserved_usernames": [], "prohibited_words": [], "sensitive_words": [],
  "trusted_proxies": [],                     // only these may set X-Forwarded-For
  "note_draft_limit": 100, "scheduled_note_limit": 100, "pin_limit": 10,
  "default_like": "❤",
  "drive_capacity_per_user_bytes": 1073741824,
  "remote_cache_max_bytes": 536870912,
  "remote_cache_avatar_bytes": 134217728,
  "remote_cache_preview_bytes": 268435456,
  "remote_cache_full_bytes": 268435456,
  "remote_cache_mode": "OnDemand",            // Disabled|OnDemand|Warm
  "remote_cache_ttl_days": 30, "warming_enabled": false,
  "durable_dir": "./data/blobs_durable", "cache_dir": "./data/blobs_cache",
  "smtp_host": null, "smtp_port": 587, "smtp_username": null,
  "smtp_password": null, "smtp_from": null,
  "email_required_for_signup": false,
  "transcode": { "enabled": true /* ranges validated */ },
  "federation": { "rfc9421_mode": "permissive" } // enforce = future
}
```

Rules enforced: `durable_dir != cache_dir`, class budgets ≤ global and sum ≤ global, SMTP all-or-nothing + `from` shape + split-auth reject, transcode ranges hermetic + toolchain probe at boot. CLI: `--mode/--public-url/--data-dir/--listen/--trusted-proxies/--max-file-size/.../--durable-dir/--cache-dir/--no-transcode`.

Exceptions (env-only, tracked TODOS P3, no exception granted): `PINETREE_ALLOW_PRIVATE_IPS`, `PINETREE_DEBUG_FEDERATION`, `PINETREE_REPLY_REPAIR_LIMIT`; lease/retry numbers hardcoded; timeouts/SSRF/redirects/proxy-8MiB/log/metrics knobs hardcoded. Timeouts: 10 s op / 20 s total / 5 s DNS, ≤3 redirects, 64 KiB AP/WebFinger, 32 KiB headers.

Docker: multi-stage Debian (curl+OpenSSL3, `tini`, unprivileged `pinetree`, `700 /data`, `EXPOSE 3000`, `CMD --config --data-dir --listen`). Known bug: image lacks `ffmpeg` but default `transcode.enabled=true` refuses to boot — pass `--no-transcode` or add layer. `HEALTHCHECK` probes `/healthz` (static `ok`); `/ready` does a real redb read but is unprobed.

## 3. Moderation tools (how to use them)

1. Triage `abuse-user-reports`, `resolve` with action, `forward-abuse-user-report` to origin (federates as `Flag` both directions).
2. `show-user|show-users` (private follow-lists visible to mods), `update-user-note`, `get-user-ips`, `find-by-email`.
3. `suspend-user|unsuspend-user` (suspend hides everywhere incl anon, drops sockets), `unset-avatar|banner`, `delete-all-files-of-a-user`, `delete-account`, `reset-password`, `unset-mfa`.
4. `update-meta {blockedHosts, federateClips, federatePages, ugcVisibilityForVisitor, ...}` — instance blocklist suffix-match inbound/read/outbound.
5. `show-moderation-logs` (moderator-readable; `user:null` for CLI rows).

## 4. Where we differ from Misskey (admin-visible)

Single binary + single `.redb` (no Postgres/Redis); no web UI (API only); timelines built on read (no pre-materialization); streaming in-process (no broker, no multi-node); mutes/blocks server-enforced; suspend drops sockets; `deleteAccount` tombstones; `driveUsage*` real (upstream 0); `retention` readable; `my/apps` per-grant; webhook prefix strips (`system-webhook/*`, `webhooks/*`); emoji `copy{host,name}`; role policies stored-not-enforced; queue/admin-stats simplified; email loud-fail without SMTP. See [Intentional Differences](Intentional-Differences.md).

Read more: [Architecture](Architecture.md), [Operations](Operations.md), [Security](Security.md).
