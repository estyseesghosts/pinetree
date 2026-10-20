//! Server-side word mutes (Batch N WS8; owner-approved DIVERGENCE from
//! upstream, which enforces word mutes only in the frontend
//! `check-word-mute.ts` + `use-note.ts`).
//!
//! Matching mirrors the Misskey-exact semantics both clients implement
//! (verified against Misskey `check-word-mute.ts` + Keystone
//! `frontend/src/misskey/softMute.ts`):
//! - the haystack is `(cw ?? '') + '\n' + (text ?? '')`, trimmed (CW first,
//!   so `^` anchors at the CW);
//! - a user's own notes never match; empty text never matches;
//! - array filters are case-sensitive substring AND-groups (empty keywords
//!   dropped; a group left empty never matches);
//! - bare-string filters are `/pattern/flags` regexes (an invalid or
//!   non-matching shape never matches; matching is stateless, so no
//!   `lastIndex` equivalent is needed);
//! - a note matches when itself, its reply, or its renote matches
//!   (`checkNoteWordMute`: first match wins).
//!
//! Enforcement: **hard** mutes (`hardMutedWords`) drop the note in
//! `FeedFilter` on every timeline/note read; **soft** mutes
//! (`mutedWords`) pack a viewer-specific `isSoftMuted` flag when the note
//! matches AND has no CW of its own (the Keystone badge rule).

use regex::Regex;

use crate::domain::account::MutedWord;

/// One compiled read path: AND-groups of literal substrings plus compiled
/// regexes. `regex::Regex` is linear-time, so no RE2 is needed; the filter
/// set is char-capped at write time (200), bounding compile cost.
#[derive(Debug, Default)]
pub struct CompiledWordMute {
    and_groups: Vec<Vec<String>>,
    regexes: Vec<Regex>,
}

impl CompiledWordMute {
    /// Compile one user's filter list (`mutedWords` or `hardMutedWords`).
    /// Empty AND keywords drop; emptied groups drop; non-`/pattern/flags`
    /// or uncompilable bare strings drop (they never match, upstream
    /// parity).
    pub fn compile(filters: &[MutedWord]) -> Self {
        let mut out = Self::default();
        for filter in filters {
            match filter {
                MutedWord::And(words) => {
                    let terms: Vec<String> = words
                        .iter()
                        .filter(|w| !w.is_empty())
                        .cloned()
                        .collect();
                    if !terms.is_empty() {
                        out.and_groups.push(terms);
                    }
                }
                MutedWord::Single(pattern) => {
                    if let Some(re) = compile_js_regex(pattern) {
                        out.regexes.push(re);
                    }
                }
            }
        }
        out
    }

    pub fn is_empty(&self) -> bool {
        self.and_groups.is_empty() && self.regexes.is_empty()
    }

    /// Misskey `checkWordMute` for one note body: self-exempt, empty-text
    /// exempt. `author_id == viewer_id` never matches.
    pub fn matches_note(
        &self,
        cw: Option<&str>,
        text: Option<&str>,
        author_id: &str,
        viewer_id: &str,
    ) -> bool {
        if self.is_empty() || author_id == viewer_id {
            return false;
        }
        let haystack = format!("{}\n{}", cw.unwrap_or(""), text.unwrap_or(""));
        let haystack = haystack.trim();
        if haystack.is_empty() {
            return false;
        }
        if self.and_groups.iter().any(|group| {
            // The group is non-empty by construction; every term hits.
            group.iter().all(|term| haystack.contains(term))
        }) {
            return true;
        }
        self.regexes.iter().any(|re| re.is_match(haystack))
    }
}

/// Parse a `/pattern/flags` bare-string filter into a Rust regex.
/// Flag mapping (JS → Rust inline flags): `i` → `(?i)`, `m` → `(?m)`,
/// `s` → `(?s)`; `g` has no matching effect (stateless `is_match`);
/// `d` (indices) has no matching effect; `y` (sticky) anchors at the
/// start (`\A(?:…)`); `u` is the default. Anything else (notably `v`)
/// fails → the filter never matches (fail-closed documented choice).
fn compile_js_regex(filter: &str) -> Option<Regex> {
    let inner = filter.strip_prefix('/')?;
    let split = inner.rfind('/')?;
    let (pattern, flags) = inner.split_at(split);
    let flags = &flags[1..];
    if pattern.is_empty() {
        return None;
    }
    let mut prefix = String::new();
    let mut sticky = false;
    for flag in flags.chars() {
        match flag {
            'i' => prefix.push_str("(?i)"),
            'm' => prefix.push_str("(?m)"),
            's' => prefix.push_str("(?s)"),
            // No matching effect.
            'g' | 'd' | 'u' => {}
            // Sticky: anchor the whole pattern at the haystack start.
            'y' => sticky = true,
            _ => return None,
        }
    }
    let source = if sticky {
        format!("{prefix}\\A(?:{pattern})")
    } else {
        format!("{prefix}{pattern}")
    };
    Regex::new(&source).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn single(s: &str) -> MutedWord {
        MutedWord::Single(s.to_string())
    }

    fn and(words: &[&str]) -> MutedWord {
        MutedWord::And(words.iter().map(|w| w.to_string()).collect())
    }

    #[test]
    fn haystack_order_and_trim() {
        // CW first: `^` anchors at the CW, and the join trims.
        let c = CompiledWordMute::compile(&[single("/^warn/")]);
        assert!(c.matches_note(Some("warn: x"), Some("body"), "a", "v"));
        assert!(!c.matches_note(Some("x"), Some("warn: y"), "a", "v"));
        // Empty text never matches, even with an empty haystack join.
        let c = CompiledWordMute::compile(&[and(&["x"])]);
        assert!(!c.matches_note(None, None, "a", "v"));
        assert!(!c.matches_note(Some(""), Some("  "), "a", "v"));
    }

    #[test]
    fn and_groups_are_case_sensitive_substrings() {
        let c = CompiledWordMute::compile(&[and(&["Foo", "bar"])]);
        assert!(c.matches_note(None, Some("a Foo then bar"), "a", "v"));
        assert!(!c.matches_note(None, Some("a foo then bar"), "a", "v"));
        assert!(!c.matches_note(None, Some("only Foo"), "a", "v"));
        // Empty keywords drop; a group left with only exact-'' never
        // matches (upstream drops `''` exactly — whitespace stays a term).
        let c = CompiledWordMute::compile(&[and(&[""])]);
        assert!(c.is_empty());
        assert!(!c.matches_note(None, Some("anything"), "a", "v"));
        let c = CompiledWordMute::compile(&[and(&["", "hit"])]);
        assert!(c.matches_note(None, Some("a hit"), "a", "v"));
    }

    #[test]
    fn regex_flags() {
        let c = CompiledWordMute::compile(&[single("/^hello/i")]);
        assert!(c.matches_note(None, Some("HELLO there"), "a", "v"));
        // Multiline: `^` anchors at line starts with `m`.
        let plain = CompiledWordMute::compile(&[single("/^second/")]);
        assert!(!plain.matches_note(Some("first"), Some("second"), "a", "v"));
        let multi = CompiledWordMute::compile(&[single("/^second/m")]);
        assert!(multi.matches_note(Some("first"), Some("second"), "a", "v"));
        // Dot-all.
        let dotall = CompiledWordMute::compile(&[single("/a.+b/s")]);
        assert!(dotall.matches_note(None, Some("a\nb"), "a", "v"));
        // Non-shape and invalid patterns never match.
        let bad = CompiledWordMute::compile(&[single("plain"), single("/(/")]);
        assert!(bad.is_empty());
        assert!(!bad.matches_note(None, Some("plain"), "a", "v"));
        // Unknown flags never match.
        let vflag = CompiledWordMute::compile(&[single("/x/v")]);
        assert!(vflag.is_empty());
    }

    #[test]
    fn self_notes_never_match() {
        let c = CompiledWordMute::compile(&[and(&["x"])]);
        assert!(!c.matches_note(None, Some("x"), "me", "me"));
        assert!(c.matches_note(None, Some("x"), "other", "me"));
    }
}
