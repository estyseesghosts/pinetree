#!/usr/bin/env bash
# Phase 3 — profile contract suite against the live binary with the REAL
# misskey-js 2026.10.1-alpha.0 client library (`APIClient` + `Stream` under
# `bun`): every REST endpoint in docs/phase-3-profile.md §3 plus the
# streaming channels/events of profile §2.
#
# Starts the compiled pinetree binary on a temp --data-dir (ephemeral port),
# drives REST + streaming flows, prints SMOKE_OK on success. Cleans up on exit.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pinetree_bin() {
  if [[ -n "${PINETREE_BIN:-}" ]]; then echo "$PINETREE_BIN"; return; fi
  local dir
  dir="$(cd "$ROOT" && cargo metadata --format-version 1 --offline --no-deps 2>/dev/null \
    | python3 -c 'import json,sys;print(json.load(sys.stdin).get("target_directory",""))' 2>/dev/null)"
  if [[ -n "$dir" ]]; then echo "$dir/debug/pinetree"; else echo "$ROOT/target/debug/pinetree"; fi
}
BIN="$(pinetree_bin)"
TMPDIR="$(mktemp -d "${TMPDIR:-/tmp}/pinetree-smoke3-XXXXXX")"
DATA_DIR="$TMPDIR/data"

SERVER_PID=""
cleanup() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -9 "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$TMPDIR"
}
trap cleanup EXIT

if [[ ! -x "$BIN" ]]; then
  echo "[smoke3] building pinetree binary..." >&2
  (cd "$ROOT" && cargo build --offline) || { echo "SMOKE_FAIL: cargo build failed" >&2; exit 1; }
fi
[[ -x "$BIN" ]] || { echo "SMOKE_FAIL: binary missing at $BIN" >&2; exit 1; }
# Warn (never fail) when a source file is newer than the binary: a stale
# binary can pass while the current tree is broken.
if [[ -z "${PINETREE_BIN:-}" ]] && newer="$(find "$ROOT/src" -name '*.rs' -newer "$BIN" -print 2>/dev/null | head -1)" && [[ -n "$newer" ]]; then
  echo "[smoke3] WARNING: binary $BIN predates $newer; rebuild to test the current tree" >&2
fi

if [[ -n "${BUN_BIN:-}" ]]; then
  BUN="$BUN_BIN"
elif [[ -x "$HOME/.bun/bin/bun" ]]; then
  BUN="$HOME/.bun/bin/bun"
else
  BUN="bun"
fi
command -v "$BUN" >/dev/null 2>&1 || { echo "SMOKE_FAIL: bun not found" >&2; exit 1; }
[[ -f "$ROOT/misskey/packages/misskey-js/src/api.ts" ]] || {
  echo "SMOKE_FAIL: misskey checkout missing" >&2; exit 1
}
[[ -f "$ROOT/misskey/packages/misskey-js/src/streaming.ts" ]] || {
  echo "SMOKE_FAIL: misskey-js streaming.ts missing" >&2; exit 1
}

pick_port() {
  for _ in $(seq 1 50); do
    local p=$((20000 + RANDOM % 40000))
    if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
      exec 3>&- || true
    else
      echo "$p"
      return 0
    fi
  done
  return 1
}

PORT="$(pick_port)"
ORIGIN="http://127.0.0.1:$PORT"
"$BIN" --data-dir="$DATA_DIR" --listen="127.0.0.1:$PORT" --public-url="$ORIGIN" --no-transcode --allow-local-http-for-testing &
SERVER_PID=$!
for _ in $(seq 1 100); do
  if curl -fs -o /dev/null -m 2 "$ORIGIN/healthz" 2>/dev/null; then break; fi
  sleep 0.2
done
curl -fs -o /dev/null -m 2 "$ORIGIN/healthz" || {
  echo "SMOKE_FAIL: server did not become healthy at $ORIGIN" >&2; exit 1
}
echo "[smoke3] server up at $ORIGIN (pid $SERVER_PID)" >&2

cat >"$TMPDIR/smoke3.mts" <<'MTS_EOF'
// Phase 3 profile contract suite: real misskey-js APIClient + Stream.
const ROOT = process.env.PINETREE_ROOT!;
const ORIGIN = process.env.PINETREE_ORIGIN!;
const apiMod = await import(`${ROOT}/misskey/packages/misskey-js/src/api.ts`);
const streamMod = await import(`${ROOT}/misskey/packages/misskey-js/src/streaming.ts`);
const APIClient = apiMod.APIClient;
const Stream = streamMod.default;

function assert(cond: unknown, msg: string): void {
  if (!cond) { console.error(`SMOKE_FAIL: ${msg}`); process.exit(1); }
}
function step(name: string): void { console.log(`[smoke3] ${name}`); }
function wait(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
async function waitFor<T>(label: string, collect: () => T | undefined, timeoutMs = 8000): Promise<T> {
  const start = Date.now();
  for (;;) {
    const v = collect();
    if (v !== undefined) return v;
    if (Date.now() - start > timeoutMs) {
      console.error(`SMOKE_FAIL: timed out waiting for ${label}`);
      process.exit(1);
    }
    await wait(50);
  }
}

async function signup(username: string): Promise<{ id: string; token: string }> {
  const res = await fetch(`${ORIGIN}/signup`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ username, password: "password123" }),
  });
  assert(res.status === 200, `signup ${username}: HTTP ${res.status}`);
  const body: any = await res.json();
  return { id: body.id, token: body.token };
}

const alice = await signup("alice");
const bob = await signup("bob");
const aliceCli = new APIClient({ origin: ORIGIN, credential: alice.token });
const bobCli = new APIClient({ origin: ORIGIN, credential: bob.token });

// --- REST: notes graph -------------------------------------------------------
step("notes/create graph (post, reply, renote, quote)");
const n1: any = (await aliceCli.request("notes/create", { text: "root note #hello" } as any)).createdNote;
const reply: any = (await bobCli.request("notes/create", { text: "a reply", replyId: n1.id } as any)).createdNote;
const renote: any = (await bobCli.request("notes/create", { renoteId: n1.id } as any)).createdNote;
const quote: any = (await aliceCli.request("notes/create", { text: "a quote", renoteId: n1.id } as any)).createdNote;

step("users/notes");
const aliceNotes: any[] = await aliceCli.request("users/notes", { userId: alice.id } as any);
assert(aliceNotes.some((n) => n.id === n1.id), "users/notes has root");
assert(aliceNotes.some((n) => n.id === quote.id), "users/notes has quote");
const bobNotes: any[] = await bobCli.request("users/notes", { userId: bob.id, withReplies: false } as any);
assert(!bobNotes.some((n) => n.id === reply.id), "withReplies=false drops replies");
assert(bobNotes.some((n) => n.id === renote.id), "pure renotes kept by default");
const bobNoRenotes: any[] = await bobCli.request("users/notes", { userId: bob.id, withReplies: true, withRenotes: false } as any);
assert(bobNoRenotes.some((n) => n.id === reply.id), "withReplies=true keeps replies");
assert(!bobNoRenotes.some((n) => n.id === renote.id), "withRenotes=false drops pure renotes");
const page2: any[] = await aliceCli.request("users/notes", { userId: alice.id, limit: 1, untilId: quote.id } as any);
assert(page2.length === 1 && page2[0].id === n1.id, `untilId pagination: ${JSON.stringify(page2.map((n: any) => n.id))}`);

step("notes/replies|renotes|children|conversation");
const replies: any[] = await aliceCli.request("notes/replies", { noteId: n1.id } as any);
assert(replies.some((n) => n.id === reply.id), "replies has the reply");
assert(!replies.some((n) => n.id === renote.id), "replies excludes renotes");
const renotes: any[] = await aliceCli.request("notes/renotes", { noteId: n1.id } as any);
assert(renotes.some((n) => n.id === renote.id) && renotes.some((n) => n.id === quote.id), "renotes has both");
const children: any[] = await aliceCli.request("notes/children", { noteId: n1.id } as any);
assert(children.some((n) => n.id === reply.id) && children.some((n) => n.id === quote.id), "children = replies + quotes");
assert(!children.some((n) => n.id === renote.id), "children excludes pure renotes");
const conv: any[] = await aliceCli.request("notes/conversation", { noteId: reply.id } as any);
assert(conv.some((n) => n.id === n1.id), "conversation walks to the root");

step("notes/unrenote");
await bobCli.request("notes/unrenote", { noteId: n1.id } as any);
const renotesAfter: any[] = await aliceCli.request("notes/renotes", { noteId: n1.id } as any);
assert(!renotesAfter.some((n) => n.id === renote.id), "unrenote removed bob's renote");
assert(renotesAfter.some((n) => n.id === quote.id), "unrenote spared alice's quote");

// --- REST: follows -----------------------------------------------------------
step("users/followers|following (Following[] shape)");
await aliceCli.request("following/create", { userId: bob.id } as any);
const followers: any[] = await aliceCli.request("users/followers", { userId: bob.id } as any);
assert(followers.length === 1, `one follower: ${JSON.stringify(followers)}`);
assert(followers[0].followerId === alice.id && followers[0].followeeId === bob.id, "edge ids");
assert(followers[0].follower?.username === "alice", "follower populated");
assert(typeof followers[0].createdAt === "string", "createdAt present");
const following: any[] = await aliceCli.request("users/following", { userId: alice.id } as any);
assert(following.length === 1 && following[0].followee?.username === "bob", "followee populated");

// --- REST: drive ---------------------------------------------------------------
step("drive/files (+show/update/delete)");
const PNG_B64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==";
const uploaded: any = await aliceCli.request("drive/files/create", {
  file: new File([Buffer.from(PNG_B64, "base64")], "pic.png", { type: "image/png" }),
  name: "pic.png",
} as any);
const files: any[] = await aliceCli.request("drive/files", { limit: 10 } as any);
assert(files.some((f) => f.id === uploaded.id), "drive/files lists the upload");
const shown: any = await aliceCli.request("drive/files/show", { fileId: uploaded.id } as any);
assert(shown.id === uploaded.id && shown.name === "pic.png", "drive/files/show");
const updated: any = await aliceCli.request("drive/files/update", { fileId: uploaded.id, isSensitive: true } as any);
assert(updated.isSensitive === true, "drive/files/update flips sensitivity");
await aliceCli.request("drive/files/delete", { fileId: uploaded.id } as any);
const filesAfter: any[] = await aliceCli.request("drive/files", { limit: 10 } as any);
assert(!filesAfter.some((f) => f.id === uploaded.id), "drive/files/delete removes it");

// --- REST: notifications -------------------------------------------------------
step("i/notifications + mark-all-as-read");
// Notifications are written by the background worker: poll like a live client.
let notifs: any[] = [];
for (let i = 0; i < 50; i++) {
  notifs = await bobCli.request("i/notifications", { limit: 10 } as any);
  if (notifs.some((n) => n.type === "follow" && n.userId === alice.id)) break;
  await wait(100);
}
assert(notifs.some((n) => n.type === "follow" && n.userId === alice.id), "bob sees the follow notification");
const noFollows: any[] = await bobCli.request("i/notifications", { limit: 10, excludeTypes: ["follow"] } as any);
assert(!noFollows.some((n) => n.type === "follow"), "excludeTypes filters");
// Fresh mention so there is an unread notification (earlier reads marked all read).
await aliceCli.request("notes/create", { text: "ping @bob, read this" } as any);
let unread: any[] = [];
for (let i = 0; i < 50; i++) {
  unread = await bobCli.request("i/notifications", { limit: 10, markAsRead: false } as any);
  if (unread.some((n) => n.type === "mention" && n.isRead === false)) break;
  await wait(100);
}
assert(unread.some((n) => n.type === "mention" && n.isRead === false), "markAsRead=false keeps unread");await bobCli.request("notifications/mark-all-as-read", {} as any);
const read: any[] = await bobCli.request("i/notifications", { limit: 10 } as any);
assert(read.every((n) => n.isRead === true), "mark-all-as-read flips all");

// --- REST: search ----------------------------------------------------------------
step("notes/search + search-by-tag");
const found: any[] = await aliceCli.request("notes/search", { query: "root note" } as any);
assert(found.some((n) => n.id === n1.id), "search finds by text");
const tagged: any[] = await aliceCli.request("notes/search-by-tag", { tag: "hello" } as any);
assert(tagged.some((n) => n.id === n1.id), "search-by-tag finds #hello");

// --- REST: errors ------------------------------------------------------------------
step("error shapes");
async function expectFail(p: Promise<unknown>, code: string, label: string): Promise<void> {
  try {
    await p;
  } catch (e: any) {
    // misskey-js rejects with the server's error envelope (no HTTP status).
    assert(e?.code === code, `${label} code: ${JSON.stringify(e)}`);
    return;
  }
  console.error(`SMOKE_FAIL: ${label} should have failed with ${code}`);
  process.exit(1);
}
await expectFail(aliceCli.request("nope/missing" as any, {}), "UNKNOWN_API_ENDPOINT", "unknown endpoint");
await expectFail(aliceCli.request("users/followers", { userId: "xxxx" } as any), "NO_SUCH_USER", "followers of missing user");

// --- Streaming: real misskey-js Stream ----------------------------------------------
step("stream: globalTimeline note delivery");
const streamA: any = new Stream(ORIGIN, { token: alice.token });
const streamB: any = new Stream(ORIGIN, { token: bob.token });
const seen: { global: any[]; mainA: any[]; mainB: any[]; updated: any[] } = { global: [], mainA: [], mainB: [], updated: [] };
const gtl = streamA.useChannel("globalTimeline", { withRenotes: true });
gtl.on("note", (n: any) => seen.global.push(n));
const mainA = streamA.useChannel("main");
mainA.on("notification", (n: any) => seen.mainA.push(["notification", n]));
mainA.on("mention", (n: any) => seen.mainA.push(["mention", n]));
mainA.on("meUpdated", (u: any) => seen.mainA.push(["meUpdated", u]));
const mainB = streamB.useChannel("main");
mainB.on("notification", (n: any) => seen.mainB.push(["notification", n]));
mainB.on("followed", (u: any) => seen.mainB.push(["followed", u]));
streamB.on("noteUpdated", (p: any) => seen.updated.push(p));
await wait(500);

const live: any = (await aliceCli.request("notes/create", { text: "live via stream" } as any)).createdNote;
const got = await waitFor("globalTimeline note", () => seen.global.find((n) => n.id === live.id));
assert(got.text === "live via stream" && got.user?.username === "alice", "streamed note shape");

step("stream: main mention + noteUpdated capture");
(await bobCli.request("notes/create", { text: "hey @alice", } as any));
const mention = await waitFor("main mention", () =>
  seen.mainA.find(([t, n]: any[]) => t === "notification" && n.type === "mention"),
);
assert(mention, "mention notification on main");
const mentionNote = await waitFor("main mention note", () =>
  seen.mainA.find(([t]: any[]) => t === "mention"),
);
assert(mentionNote, "mention note event on main");
streamB.send("subNote", { id: live.id });
await wait(300); // let the capture register before the reaction lands
await bobCli.request("notes/reactions/create", { noteId: live.id, reaction: "❤" } as any);
const reacted = await waitFor("noteUpdated reacted", () =>
  seen.updated.find((p) => p.id === live.id && p.type === "reacted"),
);
assert(reacted.body?.reaction === "❤", "reacted body");
await aliceCli.request("notes/delete", { noteId: live.id } as any);
const deleted = await waitFor("noteUpdated deleted", () =>
  seen.updated.find((p) => p.id === live.id && p.type === "deleted"),
);
assert(typeof deleted.body?.deletedAt === "string", "deleted body");
streamA.close();
streamB.close();

// --- REST: revoke (last — it kills the token) -----------------------------------------
step("i/revoke-token");
await bobCli.request("i/revoke-token", {} as any);
await expectFail(bobCli.request("i", {}), "AUTHENTICATION_FAILED", "revoked token rejected");

console.log("SMOKE3_OK");
MTS_EOF

PINETREE_ORIGIN="$ORIGIN" PINETREE_ROOT="$ROOT" "$BUN" "$TMPDIR/smoke3.mts"
