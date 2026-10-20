# How to read the docs

Status: living page, as of 2026-10-20. This page maps the wiki (plain explanations) to the contract documents (exact rules) in `docs/`.

## Read in this order

1. [About pinetree](About.md): what it is, in a few minutes.
2. [Current state](Current-State.md): what works today.
3. [Feature matrix](Feature-Matrix.md): a table you can scan.
4. [Roadmap](Roadmap.md): what is missing and when.
5. [Intentional differences](Intentional-Differences.md): why it differs from Misskey.
6. [Glossary](Glossary.md): look up any unfamiliar word.

## What the status words mean

Each contract file has a status line at the top. The words mean:

- **Frozen:** agreed at a review and fixed. It changes only through a recorded decision that supersedes it. Most API and data rules are frozen.
- **Draft:** written and still being changed. It may not match the code yet.
- **Tracking (checklist):** a list of boxes that are ticked once the work is proven by a run. It records progress, not rules.
- **Living (wiki):** kept up to date as things change, and the first place to look for plain facts.
- **Implementation guide, how-to, or spec:** describes how a feature is built, how to run something, or a planned shape. Read it for detail, not as a promise.

When a wiki page and a contract disagree, the contract wins. When a contract and the product proposal disagree, the [proposal](../../sns-server-proposal.md) wins.

## Map: topic to contract document

| Topic | Plain page | Contract document | Status |
|---|---|---|---|
| The API that apps use | [Current state](Current-State.md) | [Endpoint manifest](../endpoint-manifest.md) | Frozen |
| Which app features are in scope | [Feature matrix](Feature-Matrix.md) | [Phase 3 profile](../phase-3-profile.md) | Frozen |
| Accounts, IDs and links | [Intentional differences](Intentional-Differences.md) | [Identity rules](../identity-rules.md) | Frozen |
| Data shapes (posts, accounts) | [Glossary](Glossary.md) | [Domain types](../domain-types.md) | Frozen |
| Who can see what | [Intentional differences](Intentional-Differences.md) | [Visibility policy](../visibility-policy.md) | Frozen |
| Internal interfaces | [Glossary](Glossary.md) | [Application ports](../application-ports.md) | Frozen |
| Storage tests | [Current state](Current-State.md) | [Persistence conformance](../persistence-conformance.md) | Frozen (redb green) |
| Federated lists (followers, outbox) | [Current state](Current-State.md) | [ActivityPub collections](../activitypub-collections.md) | Frozen |
| Clips and pages across servers | [Current state](Current-State.md) | [Federated collections](../federated-collections.md) | Spec |
| Quote posts | [Intentional differences](Intentional-Differences.md) | [Quote posts](../quote-posts.md) | Implementation guide |
| Reply threads | [Intentional differences](Intentional-Differences.md) | [Replies](../replies.md) | Implementation guide |
| Word mutes | [Current state](Current-State.md) | [Word mutes](../word-mutes.md) | See file status line |
| Old-media cleanup | [Roadmap](Roadmap.md) | [Remote media cache plan](../remote-media-cache-plan.md) | Draft |
| Matching Misskey | [Feature matrix](Feature-Matrix.md) | [Misskey parity plan](../misskey-parity-plan.md) | Draft |
| Running the tests | [About pinetree](About.md) | [Testing guide](../testing.md) | How-to |
| Phase 1 done checklist | [Roadmap](Roadmap.md) | [Phase 1 exit](../phase-1-exit-criteria.md) | Tracking |
| Phase 2 done checklist | [Roadmap](Roadmap.md) | [Phase 2 exit](../phase-2-exit-criteria.md) | Tracking |
| Phase 3 done checklist | [Roadmap](Roadmap.md) | [Phase 3 exit](../phase-3-exit-criteria.md) | Tracking |
| Running Misskey, Sharkey and two pinetrees together | [Glossary](Glossary.md) | [Phase 2 interop how-to](../phase-2-interop-howto.md) | How-to |
| Full open TODO list | [Roadmap](Roadmap.md) | [TODOS](../TODOS.md) | Draft |
| The code inventory behind this wiki | [Current state](Current-State.md) | [Inventory 2026-10-20](../inventory-pinetree-vs-misskey-2026-10-20.md) | Draft |

## Read more

- [Glossary](Glossary.md) for every technical word.
- [docs index](../README.md) for the full list of contract documents.
