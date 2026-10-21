//! Note reaction: pure data + validation (no I/O).

use crate::domain::id::DomainError;
use crate::domain::note::ReactionAcceptance;

/// Misskey `ReactionService.ts` `FALLBACK` (`'\u2764'`, bare U+2764, no VS16).
pub const FALLBACK_REACTION: &str = "❤";

/// Misskey `ReactionService.ts` `legacies`: string-type reactions mapped to
/// their emoji.
pub const LEGACY_REACTIONS: &[(&str, &str)] = &[
    ("like", "👍"),
    ("love", "❤"),
    ("laugh", "😆"),
    ("hmm", "🤔"),
    ("surprise", "😮"),
    ("congrats", "🎉"),
    ("angry", "💢"),
    ("confused", "😥"),
    ("rip", "😇"),
    ("pudding", "🍮"),
    ("star", "⭐"),
];

/// Misskey `ReactionService.normalize` parity: map legacy names, keep the
/// single emoji grapheme of a unicode reaction while stripping variation
/// selector `FE0F` (unless the emoji contains a `ZWJ` joiner), and fall back
/// to the bare heart for anything else (including `null`).
///
/// Custom-emoji (`:name:`) shapes pass through unchanged here: registry and
/// permission checks live in the application `react` op
/// (`resolve_custom_emoji_reaction` — unknown names, role-gated rows,
/// `localOnly` on remote notes, and sensitive rows on `nonSensitive*`
/// notes fall back to ❤, Misskey `ReactionService.create` parity).
/// `:name@host:` shapes never originate locally and keep the verbatim
/// divergence.
pub fn normalize_reaction(reaction: Option<&str>) -> String {
    let Some(reaction) = reaction else {
        return FALLBACK_REACTION.to_string();
    };
    if let Some((_, emoji)) = LEGACY_REACTIONS.iter().find(|(name, _)| *name == reaction) {
        return (*emoji).to_string();
    }
    if reaction.starts_with(':') {
        return reaction.to_string();
    }
    let first = first_emoji_grapheme(reaction);
    match first {
        Some(grapheme) => {
            if grapheme.contains('\u{200d}') {
                grapheme
            } else {
                grapheme.replace('\u{fe0f}', "")
            }
        }
        None => FALLBACK_REACTION.to_string(),
    }
}

/// Leading emoji grapheme of `s` (base emoji plus any following `FE0F`,
/// skin-tone modifier, or `ZWJ`-joined sequence), or `None` when `s` does
/// not start with an emoji. Implemented without an emoji-table dependency:
/// consumes `FE0F`/`200D`/modifier codepoints greedily after the first
/// non-ASCII (or symbol) char.
fn first_emoji_grapheme(s: &str) -> Option<String> {
    let mut chars = s.chars().peekable();
    let first = chars.next()?;
    // Emoji graphemes never start with an ASCII char ( Misskey's
    // `emojiRegex` likewise never matches pure-ASCII text); this keeps
    // `#tag`-style input on the FALLBACK path instead of echoing `#`.
    if first.is_ascii() {
        return None;
    }
    let mut out = String::from(first);
    for c in chars {
        // Grapheme continuations: variation selectors, joiners, skin-tone
        // modifiers, and the emoji char following a joiner.
        if c == '\u{fe0f}'
            || c == '\u{200d}'
            || ('\u{1f3fb}'..='\u{1f3ff}').contains(&c)
            || out.ends_with('\u{200d}')
        {
            out.push(c);
        } else {
            break;
        }
    }
    if out.contains('\u{200d}') {
        // A trailing dangling joiner is not a complete sequence.
        while out.ends_with('\u{200d}') {
            out.pop();
        }
    }
    Some(out)
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Reaction {
    pub id: String,
    pub user_id: String,
    pub note_id: String,
    pub reaction: String,
}

impl Reaction {
    pub fn validate(&self) -> Result<(), DomainError> {
        let n = self.reaction.chars().count();
        if n == 0 || n > 260 {
            return Err(DomainError::Validation(
                "reaction must be 1..=260 chars".to_string(),
            ));
        }
        Ok(())
    }
}

/// Coerces a reaction to the fallback heart when the note's acceptance only
/// allows likes (like-only notes, or like-only-for-remote notes reacting from
/// a remote account). Otherwise returns the reaction unchanged.
pub fn coerce_reaction(
    reaction: &str,
    acceptance: Option<ReactionAcceptance>,
    is_remote: bool,
) -> String {
    match acceptance {
        Some(ReactionAcceptance::LikeOnly) => FALLBACK_REACTION.to_string(),
        Some(ReactionAcceptance::LikeOnlyForRemote) if is_remote => {
            FALLBACK_REACTION.to_string()
        }
        _ => reaction.to_string(),
    }
}

/// React outcome (WP6 idempotency rule, shared by local `notes/reactions`
/// and inbound `EmojiReact`/`Like` so adapters never diverge):
/// - `Created`: no prior row — insert.
/// - `Idempotent`: same normalized reaction already stored — no-op success.
/// - `Replaced`: a different reaction is stored — overwrite in place
///   (Misskey `create` updates the existing row, never duplicates).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReactOutcome {
    Created,
    Idempotent,
    Replaced,
}

/// Decide the react path from the existing row's normalized reaction (if
/// any) and the incoming normalized reaction. Both sides must already be
/// `normalize_reaction` outputs so legacy/emoji-shape differences cannot
/// fork the decision per adapter.
pub fn decide_react(existing: Option<&str>, incoming_normalized: &str) -> ReactOutcome {
    match existing {
        None => ReactOutcome::Created,
        Some(current) if current == incoming_normalized => ReactOutcome::Idempotent,
        Some(_) => ReactOutcome::Replaced,
    }
}

/// Unreact permission (WP6): a reaction row must exist.
/// Errors `NotFound(NOT_REACTED)` when `existing` is `None` so missing
/// unreacts converge without a storage write.
pub fn can_unreact(existing: Option<&Reaction>) -> Result<(), DomainError> {
    match existing {
        Some(_) => Ok(()),
        None => Err(DomainError::NotFound("NOT_REACTED".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reaction(s: &str) -> Reaction {
        Reaction {
            id: "rid00000000000001".to_string(),
            user_id: "aid00000000000001".to_string(),
            note_id: "nid00000000000001".to_string(),
            reaction: s.to_string(),
        }
    }

    #[test]
    fn reaction_bounds() {
        assert!(reaction("👍").validate().is_ok());
        assert!(reaction(":custom_emoji@.:").validate().is_ok());
        assert!(reaction("").validate().is_err());
        assert!(reaction(&"e".repeat(260)).validate().is_ok());
        assert!(reaction(&"e".repeat(261)).validate().is_err());
    }

    #[test]
    fn coerce_reaction_cases() {
        // `likeOnly` forces the bare-heart fallback (U+2764, no VS16).
        assert_eq!(
            coerce_reaction("👍", Some(ReactionAcceptance::LikeOnly), false),
            "❤"
        );
        assert_eq!(FALLBACK_REACTION, "❤");
        assert_eq!(
            coerce_reaction("👍", Some(ReactionAcceptance::LikeOnly), true),
            FALLBACK_REACTION
        );
        assert_eq!(
            coerce_reaction(
                "👍",
                Some(ReactionAcceptance::LikeOnlyForRemote),
                true
            ),
            FALLBACK_REACTION
        );
        assert_eq!(
            coerce_reaction(
                "👍",
                Some(ReactionAcceptance::LikeOnlyForRemote),
                false
            ),
            "👍"
        );
        assert_eq!(
            coerce_reaction("👍", Some(ReactionAcceptance::NonSensitiveOnly), true),
            "👍"
        );
        assert_eq!(coerce_reaction("👍", None, true), "👍");
    }

    #[test]
    fn normalize_falls_back_to_bare_heart() {
        assert_eq!(normalize_reaction(None), "❤");
        // VS16 form normalizes to the bare fallback.
        assert_eq!(normalize_reaction(Some("❤️")), "❤");
        assert_eq!(normalize_reaction(Some("❤")), "❤");
    }

    #[test]
    fn normalize_maps_legacies() {
        assert_eq!(normalize_reaction(Some("like")), "👍");
        assert_eq!(normalize_reaction(Some("love")), "❤");
        assert_eq!(normalize_reaction(Some("laugh")), "😆");
        assert_eq!(normalize_reaction(Some("hmm")), "🤔");
        assert_eq!(normalize_reaction(Some("surprise")), "😮");
        assert_eq!(normalize_reaction(Some("congrats")), "🎉");
        assert_eq!(normalize_reaction(Some("angry")), "💢");
        assert_eq!(normalize_reaction(Some("confused")), "😥");
        assert_eq!(normalize_reaction(Some("rip")), "😇");
        assert_eq!(normalize_reaction(Some("pudding")), "🍮");
        assert_eq!(normalize_reaction(Some("star")), "⭐");
    }

    #[test]
    fn normalize_strips_fe0f_unless_zwj() {
        // U+2764 U+FE0F → bare heart; ZWJ sequences keep their selectors.
        assert_eq!(normalize_reaction(Some("❤️")), "❤");
        assert_eq!(
            normalize_reaction(Some("👨‍👩‍👧")),
            "👨‍👩‍👧"
        );
        // Plain emoji pass through; trailing text is cut to one grapheme.
        assert_eq!(normalize_reaction(Some("👍")), "👍");
        // Non-emoji text falls back.
        assert_eq!(normalize_reaction(Some("hello")), FALLBACK_REACTION);
        assert_eq!(normalize_reaction(Some("")), FALLBACK_REACTION);
        // Custom-emoji shapes pass through (no registry to check against).
        assert_eq!(
            normalize_reaction(Some(":custom_emoji@.:")),
            ":custom_emoji@.:"
        );
    }

    #[test]
    fn react_idempotency_and_replace() {
        assert_eq!(decide_react(None, "👍"), ReactOutcome::Created);
        assert_eq!(
            decide_react(Some("👍"), "👍"),
            ReactOutcome::Idempotent
        );
        assert_eq!(decide_react(Some("❤"), "👍"), ReactOutcome::Replaced);
    }

    #[test]
    fn unreact_requires_existing_row() {
        let row = Reaction {
            id: "rid00000000000001".to_string(),
            user_id: "aid00000000000001".to_string(),
            note_id: "nid00000000000001".to_string(),
            reaction: "👍".to_string(),
        };
        assert!(can_unreact(Some(&row)).is_ok());
        assert_eq!(
            can_unreact(None),
            Err(crate::domain::id::DomainError::NotFound(
                "NOT_REACTED".to_string()
            ))
        );
    }
}
