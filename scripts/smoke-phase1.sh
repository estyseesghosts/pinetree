#!/usr/bin/env bash
# Phase 1 Slice E1 — live binary + misskey-js client smoke suite.
#
# Starts the compiled `target/debug/pinetree` binary (building it first when
# absent) on a temporary `--data-dir` and an ephemeral
# `--listen=127.0.0.1:<port>` with `--public-url=http://127.0.0.1:<port>`,
# then drives the full end-to-end flow with the real Misskey
# `2026.10.1-alpha.0` checkout (`./misskey/packages/misskey-js/src/api.ts`,
# `APIClient` + `isAPIError`) executed under `bun`:
#
#   meta -> username/available -> signup (alice, bob) -> MiAuth web flow
#   (both users) -> i -> i/update (+ UNSUPPORTED_FIELD) -> drive/files/create
#   (+ GET /files/:id bytes) -> notes/create -> notes/show ->
#   following/create -> home/local/hybrid timelines (+ sinceId/untilId) ->
#   notes/reactions/create -> SIGKILL + restart on the same --data-dir ->
#   notes/show + timeline + i/notifications + media bytes (durability).
#
# It also checks `--mode=cluster` exits non-zero with
# "not implemented (Phase 4)" and exercises
# `pinetree create-account --username=cli_admin --password=cli_pass`.
#
# Prints SMOKE_OK on success. Cleans up the temp dir and child process on exit.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/target/debug/pinetree"
TMPDIR="$(mktemp -d "${TMPDIR:-/tmp}/pinetree-smoke-XXXXXX")"
DATA_DIR="$TMPDIR/data"
STATE_FILE="$TMPDIR/smoke-state.json"

SERVER_PID=""
SERVER_STARTED=0

cleanup() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -9 "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$TMPDIR"
}
trap cleanup EXIT

# --- Prerequisites -----------------------------------------------------------

if [[ ! -x "$BIN" ]]; then
  echo "[smoke] building pinetree binary..." >&2
  (cd "$ROOT" && cargo build)
fi
[[ -x "$BIN" ]] || { echo "SMOKE_FAIL: binary missing at $BIN" >&2; exit 1; }

if [[ -n "${BUN_BIN:-}" ]]; then
  BUN="$BUN_BIN"
elif [[ -x "$HOME/.bun/bin/bun" ]]; then
  BUN="$HOME/.bun/bin/bun"
else
  BUN="bun"
fi
command -v "$BUN" >/dev/null 2>&1 || { echo "SMOKE_FAIL: bun not found (tried BUN_BIN, ~/.bun/bin/bun, PATH)" >&2; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "SMOKE_FAIL: curl not found" >&2; exit 1; }
[[ -f "$ROOT/misskey/packages/misskey-js/src/api.ts" ]] || {
  echo "SMOKE_FAIL: misskey checkout missing at $ROOT/misskey/packages/misskey-js/src/api.ts" >&2
  exit 1
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

wait_for_health() {
  local origin="$1"
  for _ in $(seq 1 100); do
    if curl -fs -o /dev/null -m 2 "$origin/healthz" 2>/dev/null; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

start_server() {
  local port="$1"
  ORIGIN="http://127.0.0.1:$port"
  "$BIN" --data-dir="$DATA_DIR" --listen="127.0.0.1:$port" --public-url="$ORIGIN" &
  SERVER_PID=$!
  SERVER_STARTED=1
  wait_for_health "$ORIGIN" || {
    echo "SMOKE_FAIL: server did not become healthy at $ORIGIN" >&2
    exit 1
  }
  echo "[smoke] server up at $ORIGIN (pid $SERVER_PID)" >&2
}

stop_server_sigkill() {
  kill -9 "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
  sleep 0.5
}

# --- Cluster mode must refuse (Phase 1 builds only the redb path) ------------

echo "[smoke] checking --mode=cluster refusal..." >&2
CLUSTER_ERR="$TMPDIR/cluster.err"
set +e
"$BIN" --mode=cluster --data-dir="$TMPDIR/cluster-data" --listen="127.0.0.1:1" \
  --public-url="http://127.0.0.1:1" 2>"$CLUSTER_ERR"
CLUSTER_STATUS=$?
set -e
if [[ $CLUSTER_STATUS -eq 0 ]]; then
  echo "SMOKE_FAIL: --mode=cluster exited 0, expected non-zero" >&2
  exit 1
fi
grep -q "not implemented (Phase 4)" "$CLUSTER_ERR" || {
  echo "SMOKE_FAIL: --mode=cluster stderr lacks 'not implemented (Phase 4)':" >&2
  cat "$CLUSTER_ERR" >&2
  exit 1
}
echo "[smoke] cluster refusal OK (exit $CLUSTER_STATUS)" >&2

# --- CLI account creation (before the server opens the store) ----------------

echo "[smoke] checking create-account CLI..." >&2
CREATE_OUT="$("$BIN" create-account --data-dir="$DATA_DIR" --username=cli_admin --password=cli_pass)"
echo "$CREATE_OUT" | grep -q "cli_admin" || {
  echo "SMOKE_FAIL: create-account output missing cli_admin: $CREATE_OUT" >&2
  exit 1
}
echo "[smoke] create-account OK: $CREATE_OUT" >&2

# --- Start the server ---------------------------------------------------------

PORT="$(pick_port)"
start_server "$PORT"

# --- Part 1: live client flow (pre-restart) -----------------------------------

cat >"$TMPDIR/smoke1.mts" <<'MTS_EOF'
// Phase 1 smoke, part 1 (pre-restart): register -> auth -> post -> timeline
// -> follow -> react, using the real misskey-js 2026.10.1-alpha.0 APIClient.
const ROOT = process.env.PINETREE_ROOT!;
const ORIGIN = process.env.PINETREE_ORIGIN!;
const STATE_PATH = process.env.PINETREE_STATE!;
const mod = await import(`${ROOT}/misskey/packages/misskey-js/src/api.ts`);
const APIClient = mod.APIClient;
const isAPIError = mod.isAPIError;
const { writeFile } = await import("node:fs/promises");

function assert(cond: unknown, msg: string): void {
  if (!cond) {
    console.error(`SMOKE_FAIL: ${msg}`);
    process.exit(1);
  }
}
function step(name: string): void {
  console.log(`[smoke1] ${name}`);
}

const PERMS =
  "read:account,write:account,write:notes,write:following,write:reactions,write:drive";
// 1x1 transparent PNG fixture shared with part 2 for byte comparison.
const PNG_B64 =
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==";
const pngBytes = Buffer.from(PNG_B64, "base64");

const cli = new APIClient({ origin: ORIGIN });

// 1. meta: pinned version + miauth feature flag.
step("meta");
const meta: any = await cli.request("meta", { detail: true });
assert(meta.version === "2026.10.1-alpha.0", `meta.version: ${JSON.stringify(meta.version)}`);
assert(meta.features?.miauth === true, `meta.features.miauth: ${JSON.stringify(meta.features)}`);

// 2. username/available before registration.
step("username/available (free)");
const free: any = await cli.request("username/available", { username: "alice" });
assert(free.available === true, `alice should be available: ${JSON.stringify(free)}`);

// 3. account registration via POST /signup.
step("signup alice + bob");
async function signup(username: string, password: string): Promise<any> {
  const res = await fetch(`${ORIGIN}/signup`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password }),
  });
  const body: any = await res.json();
  assert(res.ok, `signup ${username}: HTTP ${res.status} ${JSON.stringify(body)}`);
  assert(typeof body.token === "string" && body.token.length > 0, `signup ${username} token`);
  assert(typeof body.id === "string", `signup ${username} id`);
  return body;
}
const aliceSignup = await signup("alice", "alice-smoke-pw");
const bobSignup = await signup("bob", "bob-smoke-pw");
const taken: any = await cli.request("username/available", { username: "alice" });
assert(taken.available === false, `alice should be taken: ${JSON.stringify(taken)}`);

// 4. web MiAuth flow for both users.
step("miauth alice + bob");
async function miauth(username: string, password: string): Promise<string> {
  const session = crypto.randomUUID().replace(/-/g, "");
  const qs = new URLSearchParams({ name: "SmokeClient", permission: PERMS }).toString();
  const page = await fetch(`${ORIGIN}/miauth/${session}?${qs}`);
  assert(page.ok, `GET /miauth/${session}: HTTP ${page.status}`);
  const form = new URLSearchParams({
    username,
    password,
    permission: PERMS,
    name: "SmokeClient",
  }).toString();
  const approved = await fetch(`${ORIGIN}/miauth/${session}`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: form,
  });
  assert(approved.ok, `POST /miauth/${session}: HTTP ${approved.status}`);
  async function check(): Promise<any> {
    const res = await fetch(`${ORIGIN}/api/miauth/${session}/check`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    assert(res.ok, `check HTTP ${res.status}`);
    return res.json();
  }
  const first: any = await check();
  assert(first.ok === true, `first check ok: ${JSON.stringify(first)}`);
  assert(typeof first.token === "string" && first.token.length > 0, "first check token");
  assert(first.user?.username === username, `check user: ${JSON.stringify(first.user?.username)}`);
  const second: any = await check();
  assert(second.ok === false, `second check must be spent: ${JSON.stringify(second)}`);
  return first.token;
}
const aliceToken = await miauth("alice", "alice-smoke-pw");
const bobToken = await miauth("bob", "bob-smoke-pw");

// 5. authenticated clients.
const aliceClient = new APIClient({ origin: ORIGIN, credential: aliceToken });
const bobClient = new APIClient({ origin: ORIGIN, credential: bobToken });

// 6. i + i/update (+ UNSUPPORTED_FIELD).
step("i + i/update");
const me: any = await aliceClient.request("i", {});
assert(me.username === "alice", `i.username: ${JSON.stringify(me.username)}`);
const aliceId: string = me.id;
const updated: any = await aliceClient.request("i/update", {
  name: "Alice Pinetree",
  description: "Bio text",
  isCat: true,
});
assert(updated.name === "Alice Pinetree", `i/update name: ${JSON.stringify(updated.name)}`);
assert(updated.description === "Bio text", `i/update description`);
assert(updated.isCat === true, `i/update isCat`);
let unsupported: any = null;
try {
  await aliceClient.request("i/update", { unknownField: 123 } as any);
} catch (err: any) {
  unsupported = err;
}
assert(
  unsupported !== null && isAPIError(unsupported) && unsupported.code === "UNSUPPORTED_FIELD",
  `unknown field must be UNSUPPORTED_FIELD: ${JSON.stringify(unsupported)}`,
);

// 7. drive upload + byte-identical download.
step("drive/files/create + GET /files/:id");
const driveFile: any = await aliceClient.request("drive/files/create", {
  file: new File([pngBytes], "test.png", { type: "image/png" }),
  name: "test.png",
} as any);
assert(typeof driveFile.id === "string", `DriveFile id: ${JSON.stringify(driveFile)}`);
assert(driveFile.name === "test.png", `DriveFile name: ${JSON.stringify(driveFile.name)}`);
assert(typeof driveFile.url === "string", "DriveFile url");
const fileRes = await fetch(`${ORIGIN}/files/${driveFile.id}`);
assert(fileRes.status === 200, `GET /files/:id: HTTP ${fileRes.status}`);
const fetched = Buffer.from(await fileRes.arrayBuffer());
assert(fetched.equals(pngBytes), "downloaded bytes must match the upload");

// 8. notes/create with mention, hashtag, and attachment.
step("notes/create");
const created: any = await aliceClient.request("notes/create", {
  text: "Hello @bob #pinetree",
  fileIds: [driveFile.id],
});
const note = created.createdNote;
assert(typeof note?.id === "string", `createdNote: ${JSON.stringify(created)}`);
assert(note.files?.length === 1, `files.length: ${JSON.stringify(note.files?.length)}`);
assert(note.files[0].id === driveFile.id, "attached file id");
const tags = (note.tags ?? []).map((t: string) => t.toLowerCase());
assert(tags.includes("pinetree"), `tags: ${JSON.stringify(note.tags)}`);
const noteId: string = note.id;

// 9. bob reads the note.
step("notes/show");
const shown: any = await bobClient.request("notes/show", { noteId });
assert(shown.id === noteId, "notes/show id");
assert(shown.text === "Hello @bob #pinetree", `notes/show text: ${JSON.stringify(shown.text)}`);

// 10. bob follows alice.
step("following/create");
const followee: any = await bobClient.request("following/create", { userId: aliceId });
assert(followee.id === aliceId, `followee id: ${JSON.stringify(followee.id)}`);
assert(followee.username === "alice", `followee username: ${JSON.stringify(followee.username)}`);

// 11. all three timelines + cursor pagination.
step("timelines");
for (const ep of ["notes/timeline", "notes/local-timeline", "notes/hybrid-timeline"] as const) {
  const tl: any = await bobClient.request(ep, { limit: 10 });
  assert(Array.isArray(tl), `${ep} array`);
  assert(tl.some((n: any) => n.id === noteId), `${ep} contains the note`);
}
const sincePage: any = await bobClient.request("notes/timeline", { limit: 10, sinceId: noteId });
assert(
  Array.isArray(sincePage) && !sincePage.some((n: any) => n.id === noteId),
  "sinceId excludes the note itself",
);
const untilPage: any = await bobClient.request("notes/timeline", { limit: 10, untilId: noteId });
assert(
  Array.isArray(untilPage) && !untilPage.some((n: any) => n.id === noteId),
  "untilId excludes the note itself",
);

// 12. reaction (204 void) then visible counters.
step("notes/reactions/create");
const reacted: any = await bobClient.request("notes/reactions/create", {
  noteId,
  reaction: "👍",
});
assert(reacted === null, `204 void return: ${JSON.stringify(reacted)}`);
const afterReact: any = await bobClient.request("notes/show", { noteId });
assert(afterReact.reactions?.["👍"] === 1, `reactions: ${JSON.stringify(afterReact.reactions)}`);
assert(afterReact.myReaction === "👍", `myReaction: ${JSON.stringify(afterReact.myReaction)}`);

const bobMe: any = await bobClient.request("i", {});
await writeFile(
  STATE_PATH,
  JSON.stringify({
    aliceToken,
    bobToken,
    aliceId,
    bobId: bobMe.id,
    aliceSignupToken: aliceSignup.token,
    bobSignupToken: bobSignup.token,
    noteId,
    fileId: driveFile.id,
    pngB64: PNG_B64,
  }),
);
console.log("[smoke1] OK");
MTS_EOF

PINETREE_ORIGIN="$ORIGIN" PINETREE_ROOT="$ROOT" PINETREE_STATE="$STATE_FILE" \
  "$BUN" "$TMPDIR/smoke1.mts"

# --- Part 2: SIGKILL + restart durability --------------------------------------

echo "[smoke] restarting server with SIGKILL on the same --data-dir..." >&2
stop_server_sigkill
start_server "$PORT"

cat >"$TMPDIR/smoke2.mts" <<'MTS_EOF'
// Phase 1 smoke, part 2 (post-restart): accounts, tokens, notes, follows,
// reactions, notifications, and media bytes all survived the SIGKILL.
const ROOT = process.env.PINETREE_ROOT!;
const ORIGIN = process.env.PINETREE_ORIGIN!;
const STATE_PATH = process.env.PINETREE_STATE!;
const mod = await import(`${ROOT}/misskey/packages/misskey-js/src/api.ts`);
const APIClient = mod.APIClient;
const { readFile } = await import("node:fs/promises");

function assert(cond: unknown, msg: string): void {
  if (!cond) {
    console.error(`SMOKE_FAIL: ${msg}`);
    process.exit(1);
  }
}

const state = JSON.parse(await readFile(STATE_PATH, "utf8"));
const pngBytes = Buffer.from(state.pngB64, "base64");
const aliceClient = new APIClient({ origin: ORIGIN, credential: state.aliceToken });
const bobClient = new APIClient({ origin: ORIGIN, credential: state.bobToken });

// Accounts + tokens survived: authenticated calls still work.
const aliceMe: any = await aliceClient.request("i", {});
assert(aliceMe.username === "alice" && aliceMe.id === state.aliceId, "alice identity stable");
const bobMe: any = await bobClient.request("i", {});
assert(bobMe.username === "bob" && bobMe.id === state.bobId, "bob identity stable");

// Notes + reactions survived.
const shown: any = await bobClient.request("notes/show", { noteId: state.noteId });
assert(shown.id === state.noteId, "note survived");
assert(shown.text === "Hello @bob #pinetree", "note text intact");
assert(shown.reactions?.["👍"] === 1, "reaction count survived");
assert(shown.myReaction === "👍", "myReaction survived");

// Timelines + follows survived.
const tl: any = await bobClient.request("notes/timeline", { limit: 10 });
assert(tl.some((n: any) => n.id === state.noteId), "timeline survived");
const aliceAsBob: any = await bobClient.request("users/show", { userId: state.aliceId });
assert(aliceAsBob.isFollowing === true, `follow survived: ${JSON.stringify(aliceAsBob.isFollowing)}`);

// Notifications survived: bob was mentioned, alice was followed + reacted to.
const bobNotifs: any = await bobClient.request("i/notifications", { limit: 20 });
assert(Array.isArray(bobNotifs) && bobNotifs.length >= 1, "bob notifications survived");
assert(
  bobNotifs.some((n: any) => n.type === "mention"),
  `mention notification: ${JSON.stringify(bobNotifs.map((n: any) => n.type))}`,
);
const aliceNotifs: any = await aliceClient.request("i/notifications", { limit: 20 });
const aliceTypes: string[] = aliceNotifs.map((n: any) => n.type);
assert(aliceTypes.includes("follow"), `follow notification: ${JSON.stringify(aliceTypes)}`);
assert(aliceTypes.includes("reaction"), `reaction notification: ${JSON.stringify(aliceTypes)}`);

// Media bytes survived.
const fileRes = await fetch(`${ORIGIN}/files/${state.fileId}`);
assert(fileRes.status === 200, `GET /files/:id after restart: HTTP ${fileRes.status}`);
const fetched = Buffer.from(await fileRes.arrayBuffer());
assert(fetched.equals(pngBytes), "media bytes intact across restart");

console.log("[smoke2] OK");
MTS_EOF

PINETREE_ORIGIN="$ORIGIN" PINETREE_ROOT="$ROOT" PINETREE_STATE="$STATE_FILE" \
  "$BUN" "$TMPDIR/smoke2.mts"

echo "SMOKE_OK"
