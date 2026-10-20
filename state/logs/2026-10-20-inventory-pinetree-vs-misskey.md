# 2026-10-20 — pinetree vs Misskey code inventory

7 parallel read-only scouts over disjoint areas (api / streaming / activitypub+federation / domain / application / persistence+jobs+cluster / media+config+observability). No code changed; `git status` clean except pre-existing `scripts/fediverse-bench/__pycache__/`.

What was done:
- Synthesized scout reports (each with `file:line` evidence vs `misskey` 2026.10.1-alpha.0 + Sharkey 2025.4.7 checkouts) into `docs/inventory-pinetree-vs-misskey-2026-10-20.md` (Status: Draft).
- Net: core social graph + timelines + notifications + streaming (main/timeline/list/antenna/capture) + AP federation + remote-media gates + redb (118/118 conformance) all WORK; divergences documented (out-of-scope channels/chat/gallery/folders/roles, Sharkey-parity edits, fan-out-on-read, in-process hub, hardened MiAuth); missing = Phase-4 multi-node (FDB adapter + cluster files empty, `Cluster` rejected), media lifecycle/GC/quotas/S3/backup, client-visible holes (folders/channels/chat/announcements/follow-settings/hashtags/thread-muting/2FA/memos/bulk), streaming holes (hashtag/role/channel/drive/stats/admin/clip/chat/reversi + unread/announcement/drive main events), domain transition fns.

Verification: report cites executed-code locations only; no tests run (read-only inventory, no behavior change). Scouts: ses_ea78aa6b (api), ses_ea78aa69 (streaming), ses_ea78aa69 (federation), ses_ea78aa69 (domain), ses_ea78aa67 (application), ses_ea78aa67 (persistence), ses_ea78aa66 (media/config).

Open questions: keep as living Draft and re-run deltas after Batch M/N/O/P/Q land; consider promoting top-10 gaps into `docs/misskey-parity-plan.md` next rev.
