# 2026-10-23 — MiAuth web-flow fix (login dead-end + dropped callback)

Owner symptom: Keystone opens pinetree `/miauth/{session}`, but after
approving, never redirects back — and the page shows no login at all.

## Diagnosis (pinetree bug, both halves; Keystone exonerated)
- Keystone builds `/miauth/{session}?name=..&permission=..&callback=<origin>/auth/callback`
  (`external.ts:buildMiauthUrl`) and completes on `?session=` via
  `AuthCallback` + check-poll. Correct per upstream.
- Pinetree `render_miauth_page` (`src/api/misskey/auth.rs`) rendered
  Approve/Deny buttons with **no username/password inputs**, while
  `miauth_submit` requires them → every Approve re-rendered
  "Username and password are required." Reproduced live.
- The form posted to bare `/miauth/{session}` with no hidden fields, so
  the GET's `callback` never reached the submit handler → the
  `is_safe_callback_url` 302 branch (`auth_http/miauth.rs:421-434`) could
  never fire; only the "close this window" branch was reachable.

## Fix (`src/api/misskey/auth.rs`, `render_miauth_page` only)
- Added Username + Password inputs (`autocomplete`, `required`) to the form.
- Added hidden round-trip inputs for `name`/`icon`/`callback`/`permission`,
  emitted only when the param was present (so re-approve never clobbers
  stored values via the `contains_key` update gates). All values HTML-escaped.
- Deny without credentials still fails closed (unchanged, intentional).
- No contract/doc change: the page is not in `endpoint-manifest.md`; the
  doc comment ("login/signup context plus an Approve form") is now true.

## Verification
- Live against pt-lan-a (fresh release binary, mtime newer than edit):
  page contains the inputs + hidden callback; approve-as-birch with
  callback → `302 Found`,
  `location: http://10.88.111.34:8081/auth/callback?session=...`;
  `miauth/{session}/check` → `{"ok":true,"token":...,"user":...}`.
- `cargo test --release --offline --lib miauth` → 7 passed.
- `cargo test --release --offline --lib callback` → 2 passed.
- `cargo test --release --offline --lib api::misskey::auth` → 12 passed
  (incl. 2 new: `miauth_page_has_login_inputs`,
  `miauth_page_round_trips_params_as_hidden_inputs`).
- `rustfmt --edition 2024 --check` on the touched file → clean.
- Pre-existing `miauth_web_flow_with_callback_redirects` still green
  (submit/check logic untouched).

## Open
- Server restarted with the fix (same config/DB); Metro UI untouched.
- Owner retry: `:8081` → sign in as birch → approve on the pinetree page
  → should land back in Keystone.
