# 2026-10-21 — container rig + seeded testbench (pinetree side)

## What changed
- Transcribed `Dockerfile`, `.dockerignore`, `docker/pinetree.container.json`
  from `pinetree_container.md` into the repo (new files, uncommitted).
- Built `pinetree:latest` (105 MB): `podman build --format docker
  --network=host -t pinetree:latest .` → `Successfully tagged
  localhost/pinetree:latest`.
- Rig lives in `/root/rig/`: `up.sh` (seed-before-boot + start),
  `down.sh`, `compose.yaml`, `seed.py`, `accounts.json`,
  `account-ids.json`, `post-ids.json`, `bridge.json`, `assets/`.

## Topology (Model C — host network, loopback range)
The guide's Model A (bridge + static IPs) and Model B (bench proxy) both
failed in this guest, verified by execution:
- Container-to-container L2 is broken on every podman network (alpine
  probes: 100% packet loss peer-to-peer on `birchtree-lab` AND the default
  `podman` network; the nft `oifname podman1 drop` rule is dead code —
  nothing jumps to that chain — so the cause is lower).
- Port 80 on all of 127/8 is answered by an unknown nginx/1.31.7 (no guest
  PID; likely WSL-mirrored from Windows), so the proxy model has nowhere
  to bind and bare-handle WebFinger (`:80` default) is unreliable.
- Model C: `--network=host`, each node on its own 127.10.0.0/24 address +
  port as `public_url` (pt-a `127.10.0.11:3000`, pt-b `.12:3103`, pt-c
  `.13:3105`, pt-d `.14:3107`). Distinct IPs (not just ports) because
  birchtree strips ports from local handles while pinetree keeps them.

## Verified (real output)
- `podman ps`: 8/8 up; `/healthz` 200 on all pt nodes.
- `podman logs pt-a | grep -c fed-debug` → 5193 lines, e.g.
  `[fed-debug] ... out send type=Follow inbox="http://127.10.0.21:8080/inbox"
  status=202 ... outcome=Delivered`.
- Cross-type follow: `users/show {u0, 127.10.0.21:8080}` → 200,
  `following/create` → 200.
- Full mesh: every u0 reports 63 followers / 63 following (4032/4032 edges).
- pt-a home timeline (100-note sample) contains posts from all 7 remote
  hosts across both families.
- Seed totals: 256 posts (64 hello/photo/video/poll), 8 pages, 64 replies,
  32 renotes, 64 quotes, 32 poll votes + 32 reacts (pt side).
- Frontends: kissme-fe (`split-project`, `.config/default.yml`
  `url: http://127.10.0.11:3000/`) serves :5173/vite → 200 and signs in
  through its `/api` proxy; keystone (akkoma mode,
  `KEYSTONE_BACKEND_URL=http://127.10.0.21:8080`) serves :5174 → 200 and
  registers apps through its proxy.

## Open questions
- `users/show` with a bare (port-less) host 404s even for known remotes;
  callers must use the authority with port. Worth documenting or fixing.
- Birchtree drops unknown `key` params anywhere? No — but pinetree
  `pages/create` requires `script` (misskey compat: check upstream).
