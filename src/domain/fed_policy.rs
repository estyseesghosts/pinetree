//! WP5 federation policy core: pure, dependency-free decisions shared by
//! the `activitypub` renderers and the `federation` inbox/outbox workers.
//!
//! Everything here is a total function over its inputs: no I/O, no clocks
//! (callers pass `now_ms`), no storage. Stateful machinery (caches, indexes,
//! coalescers) lives in `crate::federation`, never here.

/// Outbound reaction delivery style for one recipient host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReactionStyle {
    /// Misskey-family: `Like` + `_misskey_reaction` (+ `content`).
    MisskeyLike,
    /// Akkoma/Pleroma-family: `EmojiReact` with `content` (+ `tag` for
    /// custom emoji). These servers model emoji reactions as a distinct
    /// activity type; a bare `Like` renders as a plain favourite and a
    /// Misskey `Like` is not counted as a reaction.
    EmojiReact,
    /// Mastodon-family: bare `Like` with neither `content` nor
    /// `_misskey_reaction`. Unknown fields risk rejection or misrender.
    PlainLike,
}

/// Lowercased software names whose `Like` handling has no emoji-reaction
/// support: recipients on these hosts get a bare Mastodon-favourite `Like`.
pub const PLAIN_LIKE_SOFTWARE: &[&str] = &[
    "mastodon",
    "hometown",
    "glitchsoc",
    "glitch",
    "pixelfed",
    "lemmy",
    "gotosocial",
    "friendica",
    "pleroma-fe",
];

/// Lowercased software names that speak `EmojiReact` (`content`, plus an
/// `Emoji` tag for custom `:shortcode:` reactions).
pub const EMOJI_REACT_SOFTWARE: &[&str] = &["akkoma", "pleroma"];

/// Classify a NodeInfo `software.name` (case-insensitive, surrounding
/// whitespace tolerated). `None` (unknown host or failed probe) defaults to
/// [`ReactionStyle::MisskeyLike`]: the extra field is benign on servers that
/// ignore it, and matches upstream Misskey which always sends it.
pub fn reaction_style_for_software(software_name: Option<&str>) -> ReactionStyle {
    match software_name {
        None => ReactionStyle::MisskeyLike,
        Some(name) => {
            let lower = name.trim().to_ascii_lowercase();
            if EMOJI_REACT_SOFTWARE.contains(&lower.as_str()) {
                ReactionStyle::EmojiReact
            } else if PLAIN_LIKE_SOFTWARE.contains(&lower.as_str()) {
                ReactionStyle::PlainLike
            } else {
                // Unlisted software (misskey, sharkey, foundkey, calckey,
                // catodon, firefish, pinetree, …) defaults to Misskey-like.
                ReactionStyle::MisskeyLike
            }
        }
    }
}

/// Legacy two-way view of [`reaction_style_for_software`]: both
/// [`ReactionStyle::MisskeyLike`] and [`ReactionStyle::EmojiReact`] carry
/// emoji semantics; only [`ReactionStyle::PlainLike`] does not.
pub fn emoji_reaction_supported(software_name: Option<&str>) -> bool {
    !matches!(
        reaction_style_for_software(software_name),
        ReactionStyle::PlainLike
    )
}

/// True when `reaction` is a custom shortcode (`:name:`) rather than a
/// single unicode emoji. Custom reactions travel with an `Emoji` tag entry
/// on Akkoma/Pleroma (`content` alone renders literally there); unicode
/// reactions must NOT carry a tag (a fabricated tag URL would be a lie).
pub fn is_custom_emoji_reaction(reaction: &str) -> bool {
    let r = reaction.trim();
    r.len() >= 3
        && r.starts_with(':')
        && r.ends_with(':')
        && r[1..r.len() - 1]
            .chars()
            .all(|c| c != ':' && !c.is_whitespace())
}

/// Exact-authority comparison for federation URIs: scheme + authority
/// (`host[:port]`) must match exactly after lowercasing and trailing-dot
/// trimming. This is deliberately stricter than "same registrable domain":
/// `evil.example.com` is never the authority for `example.com`, and
/// `example.com:8443` is never `example.com`.
///
/// Used for stamp authority (the `QuoteAuthorization` IRI must live on the
/// quoted author's exact host) and for `Update(Note)` fetch canonicalization
/// (the fetched document id must equal the requested URI's authority).
pub fn exact_authority_allows(uri_a: &str, uri_b: &str) -> bool {
    fn authority(uri: &str) -> Option<(String, String)> {
        let (scheme_raw, rest) = uri.trim().split_once("://")?;
        let scheme = scheme_raw.to_ascii_lowercase();
        if scheme != "http" && scheme != "https" {
            return None;
        }
        let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
        let authority = authority.trim().trim_end_matches('.').to_ascii_lowercase();
        if authority.is_empty() {
            return None;
        }
        Some((scheme, authority))
    }
    match (authority(uri_a), authority(uri_b)) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}

/// Minimum interval between stamp re-fetch attempts for one stamp URI
/// (negative-dedup floor): transient fetch failures are cached as negative
/// entries so a hot `Accept` retry loop cannot turn into a fetch loop
/// against the quoted author's server.
pub const STAMP_NEGATIVE_MIN_INTERVAL_MS: i64 = 60_000;

/// Upper bound for stamp negative-dedup backoff (failures back off
/// linearly to this ceiling, then hold).
pub const STAMP_NEGATIVE_MAX_INTERVAL_MS: i64 = 3_600_000;

/// True when a stamp fetch may be attempted now given `failures`
/// consecutive failures and `last_attempt_ms` (both from the negative
/// cache; `None`/`0` means never attempted → always due). Pure helper for
/// the negative stamp cache in `crate::federation`.
pub fn stamp_retry_due(failures: u32, last_attempt_ms: Option<i64>, now_ms: i64) -> bool {
    let Some(last) = last_attempt_ms else {
        return true;
    };
    let backoff = (failures.max(1) as i64)
        .saturating_mul(STAMP_NEGATIVE_MIN_INTERVAL_MS)
        .min(STAMP_NEGATIVE_MAX_INTERVAL_MS);
    now_ms.saturating_sub(last) >= backoff
}

/// Coalescing window for reply-parent resolver jobs: waiters for one parent
/// URI that arrive within this window of the first enqueue share that job
/// (their pending-index entries are still recorded individually; the job
/// only triggers the worker pass). A waiter arriving after the window
/// enqueues a fresh trigger (covers job loss without unbounded fan-out).
pub const REPLY_COALESCE_TTL_MS: i64 = 5 * 60_000;

/// Deterministic idempotency/coalescing key for a reply-parent resolver
/// job (matches the existing `reply-resolve:{uri}` convention).
pub fn reply_resolve_key(parent_uri: &str) -> String {
    format!("reply-resolve:{}", parent_uri.trim())
}

/// True when linking `child` to `parent` should emit a late-link `Reply`
/// notification to the parent author: the parent author is local (there is
/// someone to notify), the child author is remote (local children already
/// notified at create time), and the parent is not deleted.
pub fn late_link_notify(
    parent_author_is_local: bool,
    child_author_is_local: bool,
    parent_deleted: bool,
) -> bool {
    parent_author_is_local && !child_author_is_local && !parent_deleted
}

/// Maximum entries in the inbound `Accept`-activity idempotency index
/// (Undo-by-ID resolution). Oldest-arbitrary eviction past this bound; the
/// map is a convergence aid, not durable state (a restart simply means a
/// rare bare-IRI `Undo(Accept)` converges without effect until the next
/// embedded `Accept` re-seeds it — same discipline as other in-memory
/// interop caches).
pub const ACTIVITY_INDEX_CAP: usize = 8_192;

/// Relay posture: pinetree runs no relay actor and no relay fan-out
/// infrastructure (no `Relay` actor document, no relay follow rows, no
/// relay forward queue). Inbound relay protocol traffic is therefore
/// refused by decision, not by accident.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayDecision {
    /// Accept + ignore with the logged reason. Transport-accepted (202) so
    /// senders do not retry; no state change, no forward.
    Refuse { reason: &'static str },
}

/// The single relay posture of this build.
pub fn relay_decision() -> RelayDecision {
    RelayDecision::Refuse {
        reason: "no-relay-actor: pinetree runs no relay actor or forward queue; \
                 relay addressed activities are transport-accepted and ignored",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reaction_style_split() {
        // Akkoma/Pleroma get EmojiReact.
        for name in ["akkoma", "pleroma", "Akkoma", " PLEROMA "] {
            assert_eq!(
                reaction_style_for_software(Some(name)),
                ReactionStyle::EmojiReact,
                "{name}"
            );
        }
        // Mastodon-family gets a bare Like.
        for name in [
            "mastodon",
            "hometown",
            "glitchsoc",
            "glitch",
            "pixelfed",
            "lemmy",
            "gotosocial",
            "MASTODON",
        ] {
            assert_eq!(
                reaction_style_for_software(Some(name)),
                ReactionStyle::PlainLike,
                "{name}"
            );
        }
        // Misskey-family + unknown default to Misskey-like.
        for name in [
            None,
            Some("misskey"),
            Some("sharkey"),
            Some("foundkey"),
            Some("calckey"),
            Some("catodon"),
            Some("firefish"),
            Some("pinetree"),
            Some("something-new"),
            Some(""),
            Some("   "),
        ] {
            assert_eq!(
                reaction_style_for_software(name),
                ReactionStyle::MisskeyLike,
                "{name:?}"
            );
        }
        // Legacy bool view: only PlainLike is unsupported.
        assert!(emoji_reaction_supported(Some("akkoma")));
        assert!(emoji_reaction_supported(None));
        assert!(!emoji_reaction_supported(Some("mastodon")));
    }

    #[test]
    fn custom_emoji_detection() {
        assert!(is_custom_emoji_reaction(":blobcat:"));
        assert!(is_custom_emoji_reaction(":ablobcatrainbow:"));
        assert!(!is_custom_emoji_reaction("❤"));
        assert!(!is_custom_emoji_reaction("🍮"));
        assert!(!is_custom_emoji_reaction(":"));
        assert!(!is_custom_emoji_reaction("::"));
        assert!(!is_custom_emoji_reaction(":has space:"));
        assert!(!is_custom_emoji_reaction("plain"));
        assert!(!is_custom_emoji_reaction(""));
    }

    #[test]
    fn exact_authority_is_exact() {
        assert!(exact_authority_allows(
            "https://example.com/users/a",
            "https://example.com/stamps/1"
        ));
        // Port-sensitive.
        assert!(!exact_authority_allows(
            "https://example.com:8443/users/a",
            "https://example.com/stamps/1"
        ));
        // Subdomains are different authorities.
        assert!(!exact_authority_allows(
            "https://evil.example.com/x",
            "https://example.com/y"
        ));
        // Scheme-sensitive.
        assert!(!exact_authority_allows(
            "http://example.com/x",
            "https://example.com/y"
        ));
        // Case + trailing dot tolerant.
        assert!(exact_authority_allows(
            "https://Example.COM./x",
            "https://example.com/y"
        ));
        // Non-http(s) never matches.
        assert!(!exact_authority_allows("gopher://example.com/x", "https://example.com/y"));
        assert!(!exact_authority_allows("not a uri", "https://example.com/y"));
        assert!(!exact_authority_allows("", ""));
    }

    #[test]
    fn stamp_retry_backoff() {
        let now = 1_760_400_000_000;
        assert!(stamp_retry_due(0, None, now));
        // One failure: due after 60s.
        assert!(!stamp_retry_due(1, Some(now - 59_999), now));
        assert!(stamp_retry_due(1, Some(now - 60_000), now));
        // Three failures: due after 180s.
        assert!(!stamp_retry_due(3, Some(now - 179_999), now));
        assert!(stamp_retry_due(3, Some(now - 180_000), now));
        // Ceiling holds at one hour.
        assert!(!stamp_retry_due(1_000, Some(now - 3_599_999), now));
        assert!(stamp_retry_due(1_000, Some(now - 3_600_000), now));
    }

    #[test]
    fn reply_key_and_late_link() {
        assert_eq!(
            reply_resolve_key(" https://x.example/n/1 "),
            "reply-resolve:https://x.example/n/1"
        );
        assert!(late_link_notify(true, false, false));
        assert!(!late_link_notify(false, false, false));
        assert!(!late_link_notify(true, true, false));
        assert!(!late_link_notify(true, false, true));
    }

    #[test]
    fn relay_is_documented_refusal() {
        match relay_decision() {
            RelayDecision::Refuse { reason } => assert!(
                reason.contains("no-relay-actor"),
                "refusal must name its reason: {reason}"
            ),
        }
    }
}
