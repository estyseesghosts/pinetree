# Phase 2.6 slice 5 — Reply visibility (D8)

- `create_note`: a reply target must be `is_visible` to the author (author counts as following themselves; follow edge otherwise). Missing, deleted and invisible targets all return `NO_SUCH_NOTE` (previously missing gave `NO_SUCH_REPLY_TARGET`; changed deliberately to avoid an existence oracle). Pending follow requests (slice 4) do not count as following.
- Tests G1 (followers-only non-follower, specified non-recipient and follower-not-recipient rejected; follower/author/recipient/public/home allowed) failed first (`assertion failed: is_no_such_note(reply(&stranger.id, &followers_note))`), passed after the fix. G2 (renote/quote of followers-only rejected) passed unchanged — policy already covered it.
- Inbound remote replies: `inbox.rs` derives audience from the activity's own addressing (`parse_note_audience`), never from the reply target, so storing a reply to an unseen note does not widen audience; no code change.
- `cargo test`: 179 passed, 0 failed.
