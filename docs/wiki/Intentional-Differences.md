# Intentional differences from Misskey

Status: living page, as of 2026-10-20. These are choices, not accidents. Each one is written down in a contract document; the [Read more](#read-more) links show where.

## Smaller on purpose

- **One program, one data file.** Misskey normally needs a separate database server and a separate cache server. pinetree keeps everything in one program with one data file. Why: it is easier to run, back up, and understand.
- **No built-in web pages.** Misskey ships its own website. pinetree does not. Why: people use an existing Misskey-style app.
- **Only the features apps need.** pinetree offers a chosen subset of Misskey's interface. Unknown addresses get a clear "not found" answer, and unknown fields in a request get a clear error. Why: a smaller surface is easier to test and to keep correct.

## Left out for now

- **Chat, channels, games, galleries, flash posts.** These are separate features with their own large surfaces.
- **Drive folders.** Files are kept in one flat list.
- **Hashtag trends and hashtag pages.** Looking up an exact hashtag works.
- **Announcements.** Left out for now. This causes a visible gap in some apps.
- **Roles and most admin screens.** Only admin account creation and basic moderation are there.
- **Two-factor sign-in, webhooks, OAuth.** Not built. OAuth only if an app needs it.
- **Mastodon's own app interface.** pinetree speaks the Mastodon-family federation rules, not Mastodon's app interface.
- **Relays.** Not built.

Full list with reasons: [Roadmap](Roadmap.md) and [Feature matrix](Feature-Matrix.md).

## Built differently

- **Timelines are built when you look, not in advance.** Some servers prepare timelines when a post is written. pinetree builds them on request from the stored posts. Why: simpler, and fine at small sizes. Speed targets come in Phase 5.
- **Live updates stay in one process.** Misskey uses a separate message broker (Redis). pinetree sends live updates inside the same program. A person counts as "online" while they have an open live connection. Why: no extra server. Cost: it does not yet span several machines.
- **Word mutes are enforced by the server.** Misskey's mutes happen in the app. A server-side mute also hides the post from the server's answers. Why: consistent across apps.
- **Quote posts follow a standard.** pinetree uses the FEP-044f quote standard for posts it sends, and still reads the older Misskey quote style. A quote needs approval; public quotes are approved automatically for now.
- **Replies from unknown parents stay hidden.** If a reply arrives before the post it answers is known, pinetree keeps it hidden until the parent is found. Why: nothing shows that might be unsafe or out of place.
- **Reactions to Mastodon-family servers.** Those servers only understand a plain "like", so pinetree sends a like instead of an emoji reaction.
- **Edits.** Sharkey-style edit history is included. The edit history is kept.
- **Strict sign-in.** Sign-in checks the password before it says an account is suspended. Each sign-in from an app must name the permissions it wants, and the login handshake expires after 30 minutes.
- **Certificates are always checked.** Connections to other servers always verify the server's certificate, even in tests.
- **Safe with outside input.** Messages from other servers have size limits, time limits, redirect limits, checks for unsafe internal addresses, and signature checks. Details: [Glossary](Glossary.md).

## Identity rules

- **Account links look like** `https://{domain}/users/{account ID}`. The account ID does not change when the name or the server changes. Public links never reveal the name of an internal machine.
- **Account moves keep the ID.** A move between servers keeps the account's identity and keys.

## Read more

- [Endpoint manifest](../endpoint-manifest.md): the exact list of what is offered and what is refused.
- [Identity rules](../identity-rules.md): account links and IDs.
- [Visibility policy](../visibility-policy.md): who can see what.
- [Phase 3 profile](../phase-3-profile.md): the agreed scope for app compatibility.
- [Quote posts](../quote-posts.md), [replies](../replies.md), [word mutes](../word-mutes.md).
