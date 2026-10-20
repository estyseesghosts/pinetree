# About pinetree

Status: living page, as of 2026-10-20.

**pinetree is a small social network server.** People sign up on it, write short posts, follow each other, reply, react, and see what others post. It is the computer program that runs such a service. Think of it as the engine behind a small, self-hosted social site, similar in spirit to Mastodon or Misskey.

It is written in Rust, a programming language known for speed and careful handling of memory. It ships as one program and keeps its data in one file on disk. It does not need a separate database server or a cache server to run.

## It speaks Misskey's language

Many people use apps made for [Misskey](Glossary.md), another popular social network server. Those apps send requests in a specific format and expect specific answers. pinetree answers the same requests the same way, for the features it supports. So an existing Misskey-style app can sign in, post, read timelines, and receive live updates without being rewritten.

"Compatible" has limits. Features pinetree does not support get a clear "not available" answer rather than a silent failure. Some things an app can do on Misskey are simply not offered on pinetree yet. See [Current state](Current-State.md) and [Feature matrix](Feature-Matrix.md).

## It talks to the wider fediverse

The [fediverse](Glossary.md) is the network of independent servers that follow and reply to each other, including Mastodon, Misskey, Sharkey, Pleroma and Akkoma. pinetree speaks the same public exchange rules, called [ActivityPub](Glossary.md). So a person on pinetree can follow someone on another server, and their posts can arrive, and the other way round.

Federation is the word for this server-to-server exchange. It is tested against real Misskey and Sharkey software running in a local lab. It has not yet been tested against the open internet with strangers' servers.

## What it is not

- **Not a website by itself.** pinetree has no built-in web pages. People use an existing Misskey-compatible app to reach it.
- **Not a Mastodon app server.** Mastodon has its own app interface. pinetree does not offer it.
- **Not a cluster yet.** It runs on one machine. Running many machines that act as one server is planned for a later phase. See [Roadmap](Roadmap.md).
- **Not a feature copy of Misskey.** Chat, community channels, games and similar extras are left out on purpose. See [Intentional differences](Intentional-Differences.md).

## Goals, in plain words

- **Small.** One program, one data file, nothing else to install for a basic setup.
- **Predictable.** Each part has one job, so parts can be changed without breaking others.
- **Tested against real software.** Compatibility checks run against real copies of Misskey and Sharkey, not against imitations.
- **Careful with outside input.** Messages from other servers are size-limited, time-limited, and checked for valid signatures before anything is trusted.

## Where it stands

The single-machine server, federation with other servers, and the Misskey-style app interface are built and tested. A round of quote posts, reply threads, and extra features (favorites, custom emoji, antennas, user lists and more) followed on 2026-10-20. Multi-machine operation is the next big step. Details: [Current state](Current-State.md), [Roadmap](Roadmap.md).

## Read more

- [Current state](Current-State.md): what works today, feature by feature.
- [Feature matrix](Feature-Matrix.md): a table comparing pinetree with Misskey and Sharkey.
- [Roadmap](Roadmap.md): what is missing and when it is planned.
- [Glossary](Glossary.md): plain meanings of the technical words used here.
- Engineers: the build and run steps are in the repo's [README](../../README.md). The contract documents are listed in [How to read the docs](How-To-Read-The-Docs.md). Full contract for the API: [endpoint manifest](../endpoint-manifest.md).
