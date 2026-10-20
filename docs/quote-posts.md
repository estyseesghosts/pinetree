# Pinetree: Quote Posts and FEP-044f

**Audience:** implementation agents working on Pinetree's Rust server  
**Status:** implementation guide; update the relevant contracts and work log as changes are made  
**Reviewed:** 16 October 2026

## Status (2026-10-20 freshness pass)

Verified against `src/domain/{quote,note}.rs`, `src/activitypub/{quotes,objects}.rs`, `src/federation/inbox.rs`, `src/application/commands/{notes,inbound/quotes}.rs`, `src/persistence/redb/mod.rs`, `src/api/misskey/packers.rs`, and `tests/quote_fixtures.rs`.

**LANDED**
- Domain record: `QuoteReference` (`src/domain/quote.rs:107-111`) and `QuoteAuthorizationState` (`quote.rs:27-45`), with the transition table (`allows_transition`) and the API projection `approvedByException → approved` (`quote.rs:228`). The note field is `Note.quote` (`src/domain/note.rs:93`).
- Migration default: `#[serde(default)]` on `Note.quote` (`note.rs:92`), so legacy rows decode as non-quotes. Pure renotes stay `quote: None`. `is_quote()` is at `note.rs:112`. No backfill marker is needed. CLOSED.
- Inbound normalization of all five carriers: `src/activitypub/quotes.rs:10-16` (list) and constants at `:40-50` (both the no-slash and slash `_misskey_quote` IRIs). Unresolved targets keep their URI as unapproved (`src/federation/inbox.rs:2204-2208`).
- Outbound wire: `quote`, `_misskey_quote`, `quoteUrl`, `quoteUri` (`quotes.rs:281-298`; JSON-LD context at `objects.rs:81`). `interactionPolicy.canQuote.automaticApproval` only, under the GoToSocial namespace (`quotes.rs:285-289,318-334`).
- Local quote creation: `Pending` plus a `QuoteRequest` delivery job in one transaction (`src/application/commands/notes.rs:194-245`). Self-quote and mention exceptions yield `ApprovedByException` (`notes.rs:194,218`).
- Inbound `Accept` / `Reject` / stamp `Delete` for quotes we authored: `src/federation/inbox.rs:1029-1110`. Stamp re-verification is a `QuoteStampReverify` job (`src/jobs/worker.rs:164,584-600`). State changes are atomic through the `record_quote_*` methods (`src/persistence/redb/mod.rs:8690-8790`).
- Inbound `QuoteRequest` for a local post: `src/federation/inbox.rs:955-1020`. Policy at `src/application/commands/inbound/quotes.rs:111-126`: self, mention, or Public/Home auto-approve; tighter targets get `Reject` only when the requester is entitled, and a silent `Ignore` otherwise.
- API projection: `quoteAuthorizationStatus`, `quoteTargetUri`, `quoteTargetUnavailable` (`src/api/misskey/packers.rs:1667-1700`; tests `:4238-4298`).
- Hermetic normalization tests in `tests/quote_fixtures.rs` (Misskey, Fedibird, Mastodon FEP-044f, FEP-e232, Pleroma/Akkoma legacy). Conformance Group Q at `tests/conformance.rs:2744`.

**OPEN / CORRECTED**
- Manual approval: NOT advertised. That is correct (`src/activitypub/quotes.rs:323`, test `:750-758`; `objects.rs:652,2216-2222`). There is no review queue for inbound requests; decisions are automatic (`src/application/commands/inbound/quotes.rs:109-115`). The "do not advertise" item is satisfied. The durable review workflow stays OPEN and should not be built without product input.
- Auto-approve scope: the guide recommends automatic approval for `Public`. Code approves `Public` **and** `Home`, and also requires `!local_only` (`inbound/quotes.rs:113-114`). Record this as a deliberate choice, or change it, per the product owner.
- Fixtures from actual target revisions: PARTIAL. `tests/quote_fixtures.rs:3-16` cites Pleroma/Akkoma fixture files. The real-file pin (`:214-260`) runs only when the external checkout `/root/fedifedifedi` exists and skips otherwise. The Mastodon shape is hand-built (`:98-110`). No Mastodon or Glitch-Soc revision is pinned. OPEN.
- Live Misskey/Sharkey quote interop: NOT re-verified in this pass.
- FDB: the quote transitions exist only on redb. The `NoteStore` and `ActivityDedup` quote methods have defaults that either return `StorageUnavailable` or call `unimplemented!()` (see the `application-ports.md` addendum).

## Goal

Implement quote posts as a first-class relationship across ActivityPub federation and Pinetree's Misskey-compatible API. Use **FEP-044f as Pinetree's canonical outbound federation format**, accept legacy quote formats, and project every recognized structured quote through one internal representation and the existing Misskey-style `renote` / `renoteId` API shape so Keystone can display it consistently. FEP-044f authorization is an optional interoperability layer: its absence must never prevent quote detection, storage, or API projection.

Pinetree must record authorization state accurately, but **the API must not suppress a quote solely because its authorization is pending, absent, rejected, or revoked**. Keystone is explicitly required to render such quote cards. This is a product-level display policy, not a claim that those quotes are authorized.

## Before editing

Read `AGENTS.md`, `sns-server-proposal.md`, `docs/README.md`, the latest relevant `state/logs/` entry, and the current domain/API/federation contracts. Follow the repo's existing modular architecture:

- Keep domain types free of ActivityPub, HTTP, and storage-adapter types.
- Route API and ActivityPub paths through the same application operations; do not duplicate business rules in adapters.
- Commit state changes and durable outbox/job records atomically.
- Keep both `redb` and FoundationDB adapters passing the same persistence contract tests.
- Add real Misskey/Sharkey/Pinetree interoperability tests. Do not call a test complete based only on mocks or reasoning.

The existing `src/domain/note.rs` `Note` already includes `renote_id`, `uri`, and `url`. Reuse those semantics where correct, but do not overload a field with an ActivityPub URI if its current contract requires an internal note ID.

## Product and protocol decisions

1. **Canonical outbound federation format:** implement FEP-044f `quote` and its authorization workflow (`quoteAuthorization`, `interactionPolicy.canQuote`, `QuoteRequest`, `QuoteAuthorization`, `Accept`, `Reject`, and stamp revocation by `Delete`). Do not require a remote server to implement any of these authorization features for its quote to be recognized.
2. **Legacy compatibility:** accept and emit Misskey `_misskey_quote`, ActivityStreams-namespace `quoteUrl`, Fedibird `quoteUri`, and optionally FEP-e232 quote `Link` tags. Any recognized field is sufficient to identify the quote relationship; these conventions do not establish authorization and may be the only quote metadata a remote server provides.
3. **Misskey API projection:** a quote is represented as a note with its own content and `renote` / `renoteId` referring to the quoted note. Keep an ordinary renote and a quote semantically distinguishable internally.
4. **Rejected quotes remain representable:** when Pinetree receives an explicit `Reject` for a quote request, persist `rejected`, but do not delete the quote note or erase its quote relationship merely to hide it in Keystone. Keep the quote link available through the API subject to normal visibility rules.
5. **Textual fallback:** outbound ActivityPub Notes should include a textual `RE: <URL>` fallback when a structured quote exists, without duplicating an equivalent author-provided suffix. On inbound posts, preserve content. Do not treat arbitrary `RE:` text as authenticated protocol metadata; Keystone separately supports heuristic fallback rendering.
6. **No false approval:** no legacy property, textual fallback, advertised policy, or `Accept` alone proves authorization. Approval requires a valid stamp or one of FEP-044f's explicit exceptions.

### Conformance boundary

FEP-044f is still a work-in-progress proposal. Implement its current wire format and authorization workflow, but document the display-policy exception: its non-normative security guidance says FEP-aware software should refuse to display unapproved quotes. Keystone is intentionally required to render rejected/unapproved quotes anyway. The FEP also says that after a `Reject`, the quote author MUST treat the quote as rejected, but deletion or removal of the quote part is described as MAY, not MUST. Pinetree will retain the status and relation for the agreed client experience, and must never represent a rejected quote as approved.

## Internal data model

Add a domain-level quote record (names are illustrative; adapt to the established type conventions). All supported wire formats must normalize into this same record; format-specific parsing belongs at the federation boundary, not in API handlers or storage adapters:

```rust
struct QuoteReference {
    target_uri: String,                   // canonical ActivityPub object ID
    target_note_id: Option<String>,        // resolved local DB Note ID, if available
    authorization: QuoteAuthorizationState,
    authorization_uri: Option<String>,    // QuoteAuthorization IRI, if verified
    request_activity_uri: Option<String>, // QuoteRequest IRI, if created locally
    last_verified_at: Option<DateTime>,
}

enum QuoteAuthorizationState {
    Pending,
    Approved,
    ApprovedByException,
    Rejected,
    Revoked,
    Unapproved,
    Unknown,
}
```

Use Pinetree's existing time/string/ID abstractions instead of introducing new dependencies merely to match the example. Persist quote metadata transactionally with its note.

- Keep `target_uri` separate from the target's HTML `url`.
- If resolved, connect `target_note_id` / the Misskey-facing `renote_id` to the actual stored target note.
- If unresolved, retain the target URI; do not discard a valid quote just because lookup is temporarily unavailable.
- Keep `reply_id` independent. A note may be both a reply and a quote.
- Preserve ordinary renote behavior; do not infer that every `renote_id` is automatically a quote.
- Do not cascade-delete the quote post when its target is deleted. Keep a stable reference and allow the target to render as unavailable/tombstoned.
- Keep the authorization state separate from quote visibility and from whether the client renders the card.

Expose a small, documented Pinetree API extension such as `quoteAuthorizationStatus` on quote notes with values `pending`, `approved`, `rejected`, `revoked`, `unapproved`, or `unknown`. This field is informational and non-standard; Keystone may use it for a status label but **must not use it to decide whether to render the quote**. Omit it for ordinary non-quote renotes. Keep the core `renote` / `renoteId` shape compatible with Misskey clients. To preserve a recognized quote when the target has not resolved to a local Note, also expose `quoteTargetUri` when that URI is safe to disclose to the viewer; if the quote relationship is known but its URI/object cannot safely be exposed, expose `quoteTargetUnavailable: true` instead. These optional extensions must not fabricate a `renoteId` or bypass visibility checks.

### Authorization state rules

- `pending`: Pinetree authored a quote and is awaiting the target author's decision.
- `approved`: a valid authorization stamp was verified and is not known revoked.
- `approvedByException`: a valid FEP-044f exception applies (self-quote or quote author is mentioned in the original post); may map to API `approved` if the API contract stays intentionally simple.
- `rejected`: Pinetree authored the quote and received a valid `Reject` referencing its matching `QuoteRequest`. A receiving server cannot infer that a third-party quote was explicitly rejected just because the stamp is absent; the `Reject` is sent to the quote author, not necessarily all recipients.
- `revoked`: a previously valid stamp was validly deleted/revoked.
- `unapproved`: a quote relationship is recognized, but no valid stamp or FEP exception establishes approval. This is the normal state for a recognized legacy quote or a quote from a server that does not implement FEP-044f; it does **not** mean the quote was explicitly rejected.
- `unknown`: validation of authorization is currently inconclusive, such as a transient lookup failure while checking an expected stamp. A timeout is not a rejection. Use `unapproved` when there simply is no authorization evidence to validate, rather than treating every non-FEP quote as a validation failure.

On transient re-verification failure, retain the last known approved state and timestamp rather than treating a network error as revocation. A valid `Delete` for the stamp changes it to `revoked`.

## Inbound quote normalization

Normalize the ActivityPub object into one `QuoteReference`; retain enough diagnostic information to identify the source format. Quote detection is independent of `quoteAuthorization`, `interactionPolicy`, `QuoteRequest`, and other FEP-044f fields: legacy-only payloads and payloads with no authorization metadata must still normalize as quotes. Do not create separate domain models for each wire format. Support:

1. FEP-044f `quote` (`https://w3id.org/fep/044f#quote`), as an IRI or an object with `id`.
2. Misskey `_misskey_quote` (`https://misskey-hub.net/ns#_misskey_quote`).
3. `quoteUrl` (`https://www.w3.org/ns/activitystreams#quoteUrl` convention).
4. Fedibird `quoteUri` (`http://fedibird.com/ns#quoteUri`).
5. A `Link` in `tag` whose `rel` identifies the Misskey quote relation (FEP-e232 style).

For scalar/object/array variants, safely extract an IRI or `id`. Require a valid `http` or `https` URI. Current Misskey documentation and context use `https://misskey-hub.net/ns#_misskey_quote`; FEP-044f's compatibility example also contains the slash variant `https://misskey-hub.net/ns/#_misskey_quote`. They are technically different IRIs. Accept both explicitly for compatibility, but emit Misskey's documented no-slash form.

If multiple fields point to the same target, collapse them to one relationship. If canonical `quote` conflicts with legacy fields, prefer `quote` for the target, record the inconsistency in diagnostics, and do not silently merge two different targets. The quote target must remain separate from `inReplyTo` and from `Announce`/renote activities. Downstream domain logic must consume only the normalized `QuoteReference`, not branch on the original wire format.

### Mastodon and Glitch-Soc interoperability

The current Mastodon and Glitch-Soc source trees parse `quote`, `_misskey_quote`, `quoteUrl`, and `quoteUri`; their serializers include the FEP quote/authorization concepts and legacy compatibility properties, with exact field spellings shaped by their JSON-LD contexts and serializer versions. Their parser also distinguishes a quoted target from the authorization stamp and policy. Do not assume that a recognized quote reference alone will produce a native quote card there: FEP-aware clients/servers can keep an unapproved quote pending or hidden until a valid stamp is known.

Pinetree should therefore publish the canonical FEP-044f fields, compatible Misskey aliases, a correct `QuoteAuthorization` stamp after approval, and the textual fallback. Add payload fixtures from the actual target revisions rather than assuming the legacy field names are identical across all releases. Sources: [Mastodon parser](https://github.com/mastodon/mastodon/blob/main/app/lib/activitypub/parser/status_parser.rb), [Mastodon serializer](https://github.com/mastodon/mastodon/blob/main/app/serializers/activitypub/note_serializer.rb), [Glitch-Soc parser](https://github.com/glitch-soc/mastodon/blob/main/app/lib/activitypub/parser/status_parser.rb), [Glitch-Soc serializer](https://github.com/glitch-soc/mastodon/blob/main/app/serializers/activitypub/note_serializer.rb).

### Resolution and trust

Use Pinetree's existing ActivityPub resolver and SSRF controls—never invent an independent URL fetch path. Do not fetch non-HTTP schemes, private IPs, or redirect chains forbidden by current federation policy. Reuse a stored object when available. If resolution fails transiently, retain the URI and retry safely; do not reject the whole Note solely because the quote target is temporarily offline.

A textual suffix such as `RE: https://example/notes/123` **is not protocol metadata**. Preserve it, but do not silently upgrade arbitrary user content into a trusted quote relationship or authorization state. Keystone performs the requested best-effort UI interpretation of textual `RE:` suffixes. A plain fallback remains visible even when the target cannot be resolved.

## Outbound ActivityPub representation

For a structured quote Note, emit:

- `quote` mapped to `https://w3id.org/fep/044f#quote`;
- `quoteAuthorization` only when a valid authorization stamp is present;
- `interactionPolicy.canQuote` on quotable objects, under the GoToSocial namespace (`https://gotosocial.org/ns#`);
- legacy `_misskey_quote`, `quoteUrl`, and `quoteUri` as compatibility fields;
- optionally an FEP-e232 `Link` in `tag` with `href`, an ActivityPub `mediaType`, and the Misskey quote `rel`;
- a textual fallback `RE: <quoted-object URL>` in serialized `content` when no equivalent terminal fallback is already present.

Use explicit JSON-LD context mappings. FEP-044f uses `quote` and `quoteAuthorization` as ID-valued properties. Map legacy terms exactly as documented in the FEP and Misskey's namespace reference. Do not claim `quoteUrl` is part of the core ActivityStreams standard.

Generate a fallback using the quoted object's stable public URL or object ID only when that URL is safe and appropriate for the post's audience. Do not add a public `RE:` fallback for a private target if it would leak a protected object's existence. Avoid mutating the stored body solely to produce the wire fallback; generate serialized content or preserve the source text separately. Do not append the fallback twice when the author already wrote a terminal `RE:` line for the same URL.

### Quote policies

Each local Note should advertise `interactionPolicy.canQuote` using `automaticApproval` and `manualApproval`. These fields are advisory; they never prove a particular quote is approved.

- Define a deliberate default policy for public/unlisted posts (recommended MVP: automatic approval for `Public`, configurable later).
- For followers-only/specified/local-only content, do not widen the original object's audience by quoting it. Use an appropriate restricted policy and enforce actual visibility separately.
- To disallow other actors, FEP-044f recommends the author's actor IRI as the sole `automaticApproval` entry, rather than an empty array.
- Do not advertise `manualApproval` unless a durable pending-request and accept/reject UI or equivalent operator workflow actually exists.
- Keep an actor's advertised policy and the server's effective policy synchronized.

## FEP-044f authorization workflow

### When a Pinetree user creates a quote

1. Resolve the `renoteId` from the Misskey API to the target note and its canonical ActivityPub `id`.
2. Create a quote Note with its own ID and commentary. Persist the quote relation with `Pending` state and create durable delivery jobs in the same transaction.
3. Send a signed `Create` to the quote's intended audience and send a `QuoteRequest` to the original author's inbox. The request's `object` is the quoted object; `instrument` is the quote Note (prefer an inline Note, as the FEP recommends, or a dereferenceable IRI).
4. If the original author sends `Accept`, validate the response and dereference/verify its `result` as a `QuoteAuthorization`. Only then mark `Approved` and serialize `quoteAuthorization` on subsequent `Update`/representation.
5. If the author sends `Reject`, validate that it corresponds to this request and mark `Rejected`. Keep the quote relation and Misskey API `renote` projection so Keystone can render it. Do not label it approved and do not invent an authorization stamp.
6. If the stamp is later deleted, validate the deletion, mark `Revoked`, retain the relationship for client rendering, and forward the deletion when Pinetree owns the quote post, per the FEP.

Publishing before the authorization reply is permitted by FEP-044f. It makes the quote immediately visible to Keystone. Other FEP-aware servers may hide it until it is approved; do not expect all remote clients to render it like Keystone.

### When Pinetree receives a `QuoteRequest` for a local post

1. Verify the signed activity, actor authority, activity ID/deduplication, and target post ownership.
2. Validate `object` as the local quoted post. Validate `instrument` as the requesting actor's quote Note, and verify its `quote` points to the same target.
3. Enforce the target post's actual visibility before deciding or returning any data. Never let a quote request become a private-content disclosure path.
4. Evaluate the post's own `canQuote` policy. Automatically approve when policy permits; reject when prohibited. Only offer manual review if it is truly implemented.
5. For approval, create a stable, dereferenceable `QuoteAuthorization` object controlled by the original author. Its `attributedTo` is that author, `interactingObject` is the quote Note ID, and `interactionTarget` is the quoted Note ID. Reply with `Accept` referencing the `QuoteRequest` and `result` pointing to the stamp.
6. For rejection, reply with `Reject` referencing the `QuoteRequest`. Do not create a stamp.
7. Notify the original author about automatically approved quotes in line with notification preferences.

The stamp MUST NOT embed the quote Note. Do not inline the quoted object in a stamp response unless the requester is allowed to see it. The stamp must be dereferenceable by parties allowed to view the original post.

### Verify stamps, exceptions, and revocation

For an incoming `quoteAuthorization` IRI, dereference through the safe federation resolver and validate all of the following:

- fetched object type is `QuoteAuthorization`;
- `interactingObject` matches the exact quote Note being evaluated;
- `interactionTarget` matches the target quoted by that Note;
- `attributedTo` matches the author of the quoted object;
- authenticity/authority of the stamp can be established through the target author's trusted ActivityPub identity and safe dereference path.

An arbitrary `quoteAuthorization` URL or an `Accept` without a valid result is not enough. Also account for the FEP's approval exceptions: self-quotes and quote authors mentioned by the original author. Validate structured actor/mention identity; do not use substring matching.

On a valid `Delete` of a previously verified stamp, mark the quote `Revoked` and forward the deletion if Pinetree owns the quote post. Re-verification should be rate-limited and cached, but the cache must support discovering revocations. Keep the operation idempotent.

## Wire-shape examples

These examples show the important fields only. Use Pinetree's real actor IDs, Note IDs, addressing, signatures, JSON-LD context, and serializer helpers; do not paste these examples as hard-coded production payloads.

### Quote Note

```json
{
  "@context": [
    "https://www.w3.org/ns/activitystreams",
    {
      "quote": { "@id": "https://w3id.org/fep/044f#quote", "@type": "@id" },
      "quoteAuthorization": { "@id": "https://w3id.org/fep/044f#quoteAuthorization", "@type": "@id" },
      "quoteUrl": "https://www.w3.org/ns/activitystreams#quoteUrl",
      "quoteUri": "http://fedibird.com/ns#quoteUri",
      "_misskey_quote": "https://misskey-hub.net/ns#_misskey_quote"
    }
  ],
  "type": "Note",
  "id": "https://pinetree.example/notes/quote-id",
  "attributedTo": "https://pinetree.example/users/alice",
  "content": "<p>My commentary.</p><br><br><span class=\"quote-inline\">RE: <a href=\"https://remote.example/notes/original\">https://remote.example/notes/original</a></span>",
  "quote": "https://remote.example/notes/original",
  "_misskey_quote": "https://remote.example/notes/original",
  "quoteUrl": "https://remote.example/notes/original",
  "quoteUri": "https://remote.example/notes/original"
}
```

`quoteAuthorization` is omitted until a valid stamp exists. If the author already supplied an equivalent terminal `RE:` fallback, do not append a second one. Fallback content conveys compatibility, not authorization semantics.

### QuoteRequest

```json
{
  "@context": [
    "https://www.w3.org/ns/activitystreams",
    { "QuoteRequest": "https://w3id.org/fep/044f#QuoteRequest" }
  ],
  "type": "QuoteRequest",
  "id": "https://pinetree.example/activities/request-id",
  "actor": "https://pinetree.example/users/alice",
  "to": ["https://remote.example/users/bob"],
  "object": "https://remote.example/notes/original",
  "instrument": {
    "type": "Note",
    "id": "https://pinetree.example/notes/quote-id",
    "attributedTo": "https://pinetree.example/users/alice",
    "content": "My commentary. RE: https://remote.example/notes/original",
    "quote": "https://remote.example/notes/original"
  }
}
```

The FEP says `instrument` SHOULD inline the quote Note or, if it does not, the Note SHOULD be dereferenceable. The `object` is the quoted Note; `instrument` is the quote Note. Do not swap them.

### QuoteAuthorization stamp

```json
{
  "@context": [
    "https://www.w3.org/ns/activitystreams",
    {
      "QuoteAuthorization": "https://w3id.org/fep/044f#QuoteAuthorization",
      "gts": "https://gotosocial.org/ns#",
      "interactingObject": { "@id": "gts:interactingObject", "@type": "@id" },
      "interactionTarget": { "@id": "gts:interactionTarget", "@type": "@id" }
    }
  ],
  "type": "QuoteAuthorization",
  "id": "https://remote.example/users/bob/stamps/stamp-id",
  "attributedTo": "https://remote.example/users/bob",
  "interactingObject": "https://pinetree.example/notes/quote-id",
  "interactionTarget": "https://remote.example/notes/original"
}
```

The `Accept` response references the matching `QuoteRequest` as `object` and the stamp IRI as `result`. The stamp must be dereferenceable by parties allowed to see the original. It must not embed the quote Note; do not inline the target unless the requester is allowed to view it. A valid `Reject` references the corresponding request and does not create a stamp. Revocation is a `Delete` of the stamp.

## Misskey API contract for Keystone

For any structured quote recognized from any supported wire format, with or without FEP-044f authorization metadata, regardless of `pending`, `unapproved`, `rejected`, or `revoked` state:

- return the quoting Note as its own Note with its own `text`, `cw`, media, timestamps, and reply context;
- return `renoteId` / `renote` pointing to the quoted Note when it is safe and available, using the same response shape regardless of the inbound wire format;
- include `quoteAuthorizationStatus` as the documented Pinetree extension if status is available; missing authorization metadata from a legacy/non-FEP sender is not `rejected`;
- never turn a quote into an ordinary pure renote by discarding the quoting Note's own content;
- do not hide the relation because authorization failed.

Visibility remains mandatory. If the viewer cannot access the quoted Note, do not serialize the protected Note object or sensitive metadata. Return a controlled unavailable/omitted target; use `quoteTargetUnavailable` rather than exposing a protected target URI, and let any existing `RE:` text remain a link fallback. For unresolved targets whose URI is safe to disclose, `quoteTargetUri` lets Keystone preserve the quote relationship and show a retryable or unavailable card. Authorization is not visibility, and a quote card does not grant access.

The textual `RE:` fallback parser is a Keystone responsibility. Pinetree should preserve the text and keep it distinct from the formal ActivityPub quote relation.

## Persistence and reliability

- Persist Note + quote metadata + durable outbound jobs atomically.
- Persist state changes (`Pending` → `Approved`/`Rejected` and `Approved` → `Revoked`) atomically with any resulting `Update`/`Delete` delivery jobs.
- Make `QuoteRequest`, `Accept`, `Reject`, authorization dereference, and `Delete` processing idempotent.
- Apply timeouts, body-size limits, redirect limits, signature checks, SSRF protections, and actor/object authority validation from existing federation code.
- Never perform unbounded network requests in a storage transaction. Do resolution and delivery via existing job/workflow boundaries.
- Add a migration/default for existing notes so ordinary renotes are not misclassified as quotes.

## Required tests

### Parser and serializer

- Parse each of FEP-044f `quote`, `_misskey_quote`, `quoteUrl`, `quoteUri`, and FEP-e232 Link tags, including legacy-only examples with no `quoteAuthorization`, policy, or request activity.
- Accept both documented slash/no-slash Misskey relation IRIs where relevant.
- Handle object-vs-string and array property forms.
- Collapse matching aliases; record conflicting targets and prefer canonical `quote`.
- Verify each supported input format produces the same domain `QuoteReference` and Misskey API projection for the same target.
- A legacy quote with no authorization data is returned as a quote with `unapproved` (or another accurately justified state), never dropped or mislabeled `rejected`.
- Preserve plain user-authored `RE: URL` without asserting that it is authorized or structured metadata.
- Serialize FEP-044f plus Misskey-compatible fields and one non-duplicated textual fallback.

### Authorization

- Local quote: request, valid accept/stamp, reject, and revocation.
- Wrong stamp author, target, quote Note, object type, malformed URI, or invalid authority is rejected.
- Missing stamp and policy alone never create approval.
- Self-quote and verified mention exceptions follow the current FEP draft.
- A timeout is not rejection; a valid stamp `Delete` is revocation.
- Duplicate/replayed QuoteRequest/Accept/Reject/Delete does not duplicate state or notifications.
- Rejection retains `rejected` state and the Misskey quote API projection.

### Visibility, durability, interop

- Quote of private/followers-only content never widens content visibility or leaks an embedded target through a stamp.
- Target missing, deleted, or offline produces a stable unavailable fallback rather than a crash or lost quote URI; API tests verify safe `quoteTargetUri` / `quoteTargetUnavailable` behavior when `renote` cannot be populated.
- The Note, quote state, and outbox jobs survive crash recovery consistently on `redb` and FoundationDB.
- Real Pinetree ↔ Misskey and Pinetree ↔ Sharkey tests cover quote creation, reception, rendering shape, and legacy fallback behavior, including quotes from non-FEP servers that send only legacy quote fields and no authorization artifacts.
- Verify outbound payloads and recipient behavior against current target versions; don't infer it from code ancestry.

## Source references

- [FEP-044f: Consent-respecting quote posts](https://fep.swf.pub/fep/044f/fep-044f.html) — canonical representation, policies, stamp verification, QuoteRequest, acceptance/rejection and revocation.
- [Misskey ActivityPub extensions](https://misskey-hub.net/ns/) — `_misskey_quote` meaning and canonical IRI.
- [Current Misskey inbound parser](https://github.com/misskey-dev/misskey/blob/develop/packages/backend/src/core/activitypub/models/ApNoteService.ts) — current `_misskey_quote` / `quoteUrl` handling.
- [Current Misskey renderer](https://github.com/misskey-dev/misskey/blob/develop/packages/backend/src/core/activitypub/ApRendererService.ts) — outbound legacy fields and textual `RE:` fallback.
- [Mastodon parser and serializer](https://github.com/mastodon/mastodon/blob/main/app/lib/activitypub/parser/status_parser.rb), [serializer](https://github.com/mastodon/mastodon/blob/main/app/serializers/activitypub/note_serializer.rb), plus the corresponding [Glitch-Soc parser](https://github.com/glitch-soc/mastodon/blob/main/app/lib/activitypub/parser/status_parser.rb) and [serializer](https://github.com/glitch-soc/mastodon/blob/main/app/serializers/activitypub/note_serializer.rb) — current quote alias and authorization handling.
- [FEP-e232: Object Links](https://fep.swf.pub/fep/e232/fep-e232.html) — structured Link tags; not a replacement for quote authorization.
- [ActivityPub Recommendation](https://www.w3.org/TR/activitypub/) — baseline federation protocol.
