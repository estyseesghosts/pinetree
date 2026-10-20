# Sharkey vs. Misskey: A Feature and Compatibility Comparison

Sharkey is more than a themed version of Misskey. It is a fork that follows upstream changes where possible while adding its own features, user-interface improvements, federation fixes, and administrative controls.

The differences span several areas: ActivityPub interoperability, third-party client support, post management, timelines, accessibility, moderation, and deployment. Some are significant architectural or interoperability improvements; others are relatively small quality-of-life changes.

The list below summarizes documented differences and notable additions. It should not be treated as a guaranteed, exhaustive comparison against every Misskey release.

## 1. Major features and interoperability

- **Federated post editing.** Edited posts can be updated across compatible servers, and previous versions can be inspected. Users mentioned in an edited post can receive edit notifications.
- **Mastodon-compatible API and OAuth2.** Supports third-party clients and integrations designed for the Mastodon API, subject to implementation differences.
- **Post migration and import.** Supports importing exported posts and attachments from Mastodon and many forks, Pleroma/Akkoma, Misskey/Firefish and forks, Twitter/X, Instagram, and Facebook. Thread structure, replies, and other metadata may not survive every import perfectly.
- **Scheduled posts.** Posts can be scheduled through the web interface and supported Mastodon API clients.
- **Following feed.** A dedicated feed inspired by Cohost's following feed, rather than relying exclusively on conventional chronological timelines.
- **Bubble timeline.** Provides a timeline associated with a configured group of remote instances.
- **Independent boost visibility.** The visibility of a boost/renote can be selected separately from the original post, within the available visibility options.
- **Improved quote-post interoperability.** Quote posts can federate more reliably between Sharkey and compatible Mastodon forks.
- **Improved reaction interoperability.** Distinguishes ordinary likes from custom reactions when communicating with Mastodon, Pleroma, and Akkoma, rather than representing every interaction as a Misskey-style reaction.
- **Federated profile backgrounds and music status.** Supports profile backgrounds and ListenBrainz integrations that can be shared with compatible software.
- **Data export.** Users can export account-related data through a data-subject-access-request-style facility.

These features address different problems: API compatibility improves client choice, while correct federation improves how posts and interactions appear on other servers. They should not be treated as interchangeable benefits.

## 2. User interface, accessibility, and customization

### Posts and conversations

- Open a post's detailed view by clicking the post itself, which is particularly useful on mobile.
- Display indicators for images without alternative text; warn users when posting media without alt text.
- Collapse attachments by default and provide download controls for audio and video.
- Expand or collapse content-warning posts across a conversation.
- Configure whether long posts or content-warning posts are expanded automatically.
- Enable or disable animated MFM per post.
- Support longer alt text.
- Display connected reply lines and highlight a particular post when opening a link into a long thread.
- Make polls easier to identify and allow manual refreshes of remote poll results.
- Show ordinary boost and quote-boost counts separately, allowing one of each per user and post.
- Render links to other posts in a quote-like presentation where appropriate.
- Provide controls for default reaction emoji and distinguish the ordinary like action from custom reactions.

### Profiles, timelines, and navigation

- Profile tabs for notes, all activity, and posts including files.
- Profile pop-ups that show whether follow approval is required, custom profile fields, and controls to open remote profiles.
- Follow-request acceptance or rejection from profiles, and follow-back actions from relevant notifications.
- Visibility of pending outgoing follow requests.
- Options to hide bots from timelines and filter searches or timelines for posts with attachments.
- Search Drive files and folders.
- Configurable thread depth and additional navigation controls.
- Improved notification grouping and clearer profile or follow-state information.
- More consistent handling of translated posts wherever they appear, including replies and quoted posts.
- Federated settings that allow users to disable indexing of their posts.

### Composition and small usability improvements

- An MFM cheatsheet in the composer.
- Case-insensitive emoji autocomplete and improved handling of dotted usernames.
- Better handling of non-ASCII custom emoji names.
- Clearer poll indicators, reaction controls, and error messages.
- Themes that can specify fonts, with round or squarer interface styling.
- Improved browser-side CSS customization through more readable class names.
- Optional confirmation before opening external links, with configurable exceptions.
- The ability to disable cat-speak across notes viewed by the user.

These are a representative selection, not a claim that every item is absent from every upstream Misskey version.

## 3. Extras and distinctive features

Sharkey has also added a number of unusual or specialized features:

- **Module and tracker music playback.**
- **Flash `.swf` playback** through the Ruffle emulator.
- **Speak-as-cat and is-a-cat settings** as separate concepts, with federation to compatible software.
- Tossface emoji selection and Oneko, a cat that follows the mouse pointer.
- Avatar decorations that can appear behind the avatar.
- Federated `fediverse:creator` attribution in link previews, with configurable attribution domains.
- Support for the `following` feed and specialized timeline behavior.
- Antennas that can target users from specified instances, rather than requiring every user to be listed individually.

Some MFM operators and extensions have also been attributed to Sharkey, including mathematical formulas, bidirectional text, cropping, fading, mouse-following effects, and inline images. Their exact implementation and exclusivity should be checked against the relevant Sharkey and upstream Misskey revisions before presenting them as Sharkey-only capabilities.

## 4. Administration, moderation, and deployment

### Moderation and account management

- Signup approval, including mandatory reasons for applications.
- Administrative silencing and controls to mark a user's media, or media from a remote instance, as sensitive.
- Tools to delete a user's files or files from a remote instance.
- Per-user or per-instance controls affecting quoting and content warnings.
- Controls for disabling achievements, deleting remote custom emoji, refreshing remote user information, and setting default reaction emoji.
- Improved reporting workflows, including separate report-resolution and forwarding actions.
- Automatic rejection of reports from configured instances.
- Warnings before federation-breaking actions, including information about affected relationships with remote instances.
- Protection against accidental deletion of local system accounts.
- Data-export facilities for users.
- Controls over local and remote post-length limits, including separate limits for main text, content warnings, and alt text.

### Federation and security

- Improved compatibility with relays and selected ActivityPub implementations, including better quote-post handling.
- Controls over authorized-fetch behavior, including administrator policy and user overrides for their own data.
- Better handling of blocked or silenced instances and their subdomains.
- Rate limiting for password checks and WebSocket connections.
- Additional startup checks and safeguards around account deletion.
- Argon2-based hashing in place of bcrypt for the relevant secrets.
- Improved federation diagnostics and ActivityPub object logging.

### Performance and operations

- Bounded memory caches to reduce memory consumption.
- Server-side URL-preview caching.
- Smaller or improved container images in certain releases.
- Support for KeyDB as an alternative to Redis.
- Configuration files that can be stored in different directories.
- Configurable local media storage directories and BunnyCDN support.
- Additional translation backends, including DeepLX-JS and LibreTranslate.
- Configurable welcome-page and sidebar imagery, custom page-head HTML, and configurable `robots.txt` behavior where supported.
- Improvements to VAPID-key management, logging, and administrative configuration.

Some lower-level items sometimes included in feature inventories—such as Redis over a Unix socket, PostgreSQL `tsvector` search details, BSD support, and exact system-account counting behavior—should be verified against the target branch before being presented as current differences. Likewise, “custom HTML per page” is too broad: the documented feature includes injecting custom HTML into each page's `<head>`, not unrestricted arbitrary HTML throughout every page.

## 5. Important distinctions and limitations

### Sharkey does not replace Misskey's entire architecture

Sharkey inherits a substantial amount of Misskey functionality and attempts to incorporate upstream changes. Its own additions are layered on top of that shared base. Consequently, a feature appearing in Sharkey is not necessarily Sharkey-exclusive.

### Federation compatibility is not the same as API compatibility

The Mastodon API enables additional clients to connect. Separately, correct ActivityPub handling determines whether edits, quotes, reactions, replies, and other interactions are represented properly on remote servers. Improvements in one area do not guarantee complete compatibility in the other.

### Compare exact versions

The comparison changes as both projects evolve. Sharkey's upstream-tracking approach does not guarantee that it contains every feature or fix from the newest Misskey release. The official comparison maintained in a separate maintainer repository explicitly describes itself as “probably comprehensive,” not guaranteed current.

For a decision-critical feature, verify the implementation in the exact Sharkey and Misskey versions being evaluated, including whether the feature is enabled by default and whether it interoperates with the remote software you care about.
