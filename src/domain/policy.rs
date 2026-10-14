//! Creation-time visibility narrowing (`docs/visibility-policy.md` §2).
//!
//! The server may only narrow, never widen, the requested visibility at
//! creation. Read-time checks live in [`crate::domain::visibility`].

use crate::domain::id::DomainError;
use crate::domain::note::{Note, Visibility};

pub use crate::domain::visibility::{
    ViewerContext, is_eligible_for_federated_timeline, is_eligible_for_federation_delivery,
    is_eligible_for_local_timeline, is_visible,
};

/// Inputs to creation-time narrowing.
#[derive(Debug)]
pub struct NarrowingInput<'a> {
    pub requested_visibility: Visibility,
    pub requested_local_only: bool,
    pub requested_visible_user_ids: Vec<String>,
    pub author_id: &'a str,
    pub text: Option<&'a str>,
    pub cw: Option<&'a str>,
    pub renote_target: Option<&'a Note>,
    pub reply_target: Option<&'a Note>,
    /// Whether the renote target's author blocks the note author.
    pub renote_target_blocks_author: bool,
    /// Instance-configured sensitive words forcing a public→home downgrade.
    pub sensitive_words: &'a [String],
    /// Whether the author is allowed to post public notes.
    pub can_public_note: bool,
}

/// Narrowed creation attributes.
#[derive(Debug, PartialEq, Eq)]
pub struct NarrowedOutput {
    pub visibility: Visibility,
    pub local_only: bool,
    pub visible_user_ids: Vec<String>,
}

/// Narrow the requested visibility at creation (`docs/visibility-policy.md`
/// §2). Returns the effective visibility, `local_only` flag, and
/// `visible_user_ids`, or a `Validation` error when creation must be
/// rejected.
pub fn narrow_on_create(input: NarrowingInput<'_>) -> Result<NarrowedOutput, DomainError> {
    let NarrowingInput {
        requested_visibility,
        requested_local_only,
        requested_visible_user_ids,
        author_id,
        text,
        cw,
        renote_target,
        reply_target,
        renote_target_blocks_author,
        sensitive_words,
        can_public_note,
    } = input;

    // Blocking: pure renote (non-quote) of a user who blocks the author.
    if renote_target.is_some() && text.is_none() && cw.is_none() && renote_target_blocks_author
    {
        return Err(DomainError::Validation("YOU_HAVE_BEEN_BLOCKED".into()));
    }

    let mut visibility = requested_visibility;
    let mut local_only = requested_local_only;
    let mut visible_user_ids = requested_visible_user_ids;

    // Renote target rules: public allowed; home forces home-or-narrower;
    // followers allowed only for own notes (forcing followers-or-narrower);
    // specified always rejected. A local_only target propagates local_only.
    if let Some(target) = renote_target {
        match &target.visibility {
            Visibility::Public => {}
            Visibility::Home => {
                if visibility < Visibility::Home {
                    visibility = Visibility::Home;
                }
            }
            Visibility::Followers => {
                if target.author_id.as_str() != author_id {
                    return Err(DomainError::Validation("CANNOT_RENOTE".into()));
                }
                if visibility < Visibility::Followers {
                    visibility = Visibility::Followers;
                }
            }
            Visibility::Specified => {
                return Err(DomainError::Validation("CANNOT_RENOTE".into()));
            }
        }
        if target.local_only {
            local_only = true;
        }
    }

    // Reply target rules: same shape, except replying to a specified note
    // forces (not rejects) specified on the reply, inheriting the target's
    // audience when none was requested.
    if let Some(target) = reply_target {
        match &target.visibility {
            Visibility::Public => {}
            Visibility::Home => {
                if visibility < Visibility::Home {
                    visibility = Visibility::Home;
                }
            }
            Visibility::Followers => {
                if visibility < Visibility::Followers {
                    visibility = Visibility::Followers;
                }
            }
            Visibility::Specified => {
                visibility = Visibility::Specified;
                if visible_user_ids.is_empty() {
                    let mut inherited = target.visible_user_ids.clone();
                    if target.author_id.as_str() != author_id
                        && !inherited.iter().any(|id| id == &target.author_id)
                    {
                        inherited.push(target.author_id.clone());
                    }
                    // Exclude self, but keep the list non-empty.
                    let without_self: Vec<String> = inherited
                        .into_iter()
                        .filter(|id| id.as_str() != author_id)
                        .collect();
                    if without_self.is_empty() {
                        let mut fallback = target.visible_user_ids.clone();
                        if fallback.is_empty() {
                            fallback.push(target.author_id.clone());
                        }
                        visible_user_ids = fallback;
                    } else {
                        visible_user_ids = without_self;
                    }
                }
            }
        }
        if target.local_only {
            local_only = true;
        }
    }

    // Instance-policy downgrades: a public note is forced to home when the
    // author lacks public-post permission or the text/CW matches a
    // configured sensitive word (case-insensitive).
    if visibility == Visibility::Public
        && (!can_public_note || matches_sensitive_word(text, cw, sensitive_words))
    {
        visibility = Visibility::Home;
    }

    // Specified requires a non-empty audience.
    if visibility == Visibility::Specified && visible_user_ids.is_empty() {
        return Err(DomainError::Validation(
            "SPECIFIED_REQUIRES_VISIBLE_USERS".into(),
        ));
    }

    Ok(NarrowedOutput {
        visibility,
        local_only,
        visible_user_ids,
    })
}

fn matches_sensitive_word(
    text: Option<&str>,
    cw: Option<&str>,
    sensitive_words: &[String],
) -> bool {
    sensitive_words
        .iter()
        .filter(|word| !word.is_empty())
        .any(|word| {
            let needle = word.to_lowercase();
            let needle = needle.as_str();
            text.is_some_and(|t| t.to_lowercase().contains(needle))
                || cw.is_some_and(|c| c.to_lowercase().contains(needle))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::visibility::ViewerContext;

    #[allow(clippy::too_many_arguments)]
    fn make_note(
        author_id: &str,
        visibility: Visibility,
        local_only: bool,
        visible_user_ids: Vec<&str>,
        mentions: Vec<&str>,
        reply_user_id: Option<&str>,
        user_host: Option<&str>,
        is_deleted: bool,
    ) -> Note {
        Note {
            id: "n1".to_string(),
            author_id: author_id.to_string(),
            text: Some("hello".to_string()),
            cw: None,
            visibility,
            visible_user_ids: visible_user_ids.into_iter().map(|s: &str| s.to_owned()).collect(),
            local_only,
            reply_id: None,
            renote_id: None,
            reply_user_id: reply_user_id.map(|s: &str| s.to_owned()),
            file_ids: Vec::new(),
            mentions: mentions.into_iter().map(|s: &str| s.to_owned()).collect(),
            user_host: user_host.map(|s: &str| s.to_owned()),
            is_deleted,
            ..Default::default()
        }
    }

    fn local_viewers() -> [ViewerContext; 6] {
        [
            ViewerContext::local_user("author", false),
            ViewerContext::local_user("follower", true),
            ViewerContext::local_user("mentioned", false),
            ViewerContext::local_user("replied", false),
            ViewerContext::local_user("stranger", false),
            ViewerContext::anonymous(),
        ]
    }

    /// E1: read matrix over visibility × viewer × local_only.
    /// Column order: author, follower, mentioned, replied-to, stranger,
    /// anonymous. `local_only` does not change local reads.
    #[test]
    fn e1_read_matrix() {
        use Visibility::{Followers, Home, Public, Specified};
        for local_only in [false, true] {
            let cases = [
                (Public, [true, true, true, true, true, true]),
                (Home, [true, true, true, true, true, true]),
                (Followers, [true, true, true, true, false, false]),
                (Specified, [true, false, true, false, false, false]),
            ];
            for (visibility, expected) in cases {
                let label = format!("{visibility:?}");
                let note = make_note(
                    "author",
                    visibility,
                    local_only,
                    vec!["mentioned"],
                    vec!["mentioned"],
                    Some("replied"),
                    None,
                    false,
                );
                for (viewer, want) in local_viewers().iter().zip(expected.iter()) {
                    assert_eq!(
                        is_visible(&note, viewer),
                        *want,
                        "visibility={label} local_only={local_only} viewer={viewer:?}"
                    );
                }
            }
        }
    }

    /// E1 (timelines): only local public notes are local-timeline eligible;
    /// only non-local_only public notes are federated-timeline eligible.
    #[test]
    fn e1_timeline_eligibility_agrees_with_reads() {
        use Visibility::{Followers, Home, Public, Specified};
        for local_only in [false, true] {
            for visibility in [Public, Home, Followers, Specified] {
                let want_local = visibility == Public;
                let want_federated = visibility == Public && !local_only;
                let label = format!("{visibility:?}");
                let mut note = make_note(
                    "author",
                    visibility,
                    local_only,
                    vec!["mentioned"],
                    vec!["mentioned"],
                    Some("replied"),
                    None,
                    false,
                );
                assert_eq!(
                    is_eligible_for_local_timeline(&note),
                    want_local,
                    "local timeline visibility={label}"
                );
                assert_eq!(
                    is_eligible_for_federated_timeline(&note),
                    want_federated,
                    "federated timeline visibility={label} local_only={local_only}"
                );
                assert_eq!(
                    is_eligible_for_federation_delivery(&note),
                    !local_only,
                    "federation delivery local_only={local_only}"
                );
                // Remote notes are never local-timeline eligible.
                note.user_host = Some("remote.example".to_string());
                assert!(
                    !is_eligible_for_local_timeline(&note),
                    "remote note must not be local-timeline eligible"
                );
            }
        }
    }

    /// E2: followers remote-remote fallback — both sides remote, no local
    /// follow row, stranger still sees the note.
    #[test]
    fn e2_followers_remote_remote_fallback() {
        let note = make_note(
            "author",
            Visibility::Followers,
            false,
            vec![],
            vec![],
            None,
            Some("remote.example"),
            false,
        );
        let remote_stranger = ViewerContext::remote_user("stranger", "other.example", false);
        assert!(is_visible(&note, &remote_stranger));
        // Control: a local stranger with no follow edge sees nothing.
        let local_stranger = ViewerContext::local_user("stranger", false);
        assert!(!is_visible(&note, &local_stranger));
        // Control: anonymous sees nothing even remotely.
        assert!(!is_visible(&note, &ViewerContext::anonymous()));
    }

    /// E3: specified with empty visible_user_ids is rejected at creation.
    #[test]
    fn e3_specified_requires_visible_users() {
        let err = narrow_on_create(base_input("author", Visibility::Specified, Some("hi")))
            .expect_err("empty specified audience must be rejected");
        assert_eq!(
            err,
            DomainError::Validation("SPECIFIED_REQUIRES_VISIBLE_USERS".into())
        );
        let ok = narrow_on_create(NarrowingInput {
            requested_visible_user_ids: vec!["bob".to_string()],
            ..base_input("author", Visibility::Specified, Some("hi"))
        })
        .expect("non-empty specified audience must be accepted");
        assert_eq!(ok.visibility, Visibility::Specified);
        assert_eq!(ok.visible_user_ids, vec!["bob".to_string()]);
    }

    /// E4: local_only notes never reach remote viewers or federated reads,
    /// regardless of level.
    #[test]
    fn e4_local_only_never_federated() {
        use Visibility::{Followers, Home, Public, Specified};
        for visibility in [Public, Home, Followers, Specified] {
            let label = format!("{visibility:?}");
            let note = make_note(
                "author",
                visibility,
                true,
                vec!["mentioned"],
                vec!["mentioned"],
                Some("replied"),
                None,
                false,
            );
            let remote = ViewerContext::remote_user("follower", "remote.example", true);
            assert!(
                !is_visible(&note, &remote),
                "local_only {label} must be invisible to remote viewers"
            );
            // Federation rendering context: local id, but remote request.
            let federated_render = ViewerContext {
                viewer_id: Some("follower".to_string()),
                viewer_host: None,
                follows_author: true,
                is_remote_request: true,
            };
            assert!(
                !is_visible(&note, &federated_render),
                "local_only {label} must be invisible on federation renders"
            );
            assert!(!is_eligible_for_federated_timeline(&note));
            assert!(!is_eligible_for_federation_delivery(&note));
            // Local readers still subject only to the §3 predicate.
            let local_listed = ViewerContext::local_user("mentioned", true);
            assert!(is_visible(&note, &local_listed));
        }
    }

    /// Tombstoned notes are invisible to everyone on general reads and
    /// ineligible for every timeline/delivery surface.
    #[test]
    fn deleted_notes_invisible_and_ineligible() {
        let note = make_note("author", Visibility::Public, false, vec![], vec![], None, None, true);
        for viewer in local_viewers() {
            assert!(!is_visible(&note, &viewer));
        }
        assert!(!is_eligible_for_local_timeline(&note));
        assert!(!is_eligible_for_federated_timeline(&note));
        assert!(!is_eligible_for_federation_delivery(&note));
    }

    fn base_input<'a>(
        author_id: &'a str,
        requested_visibility: Visibility,
        text: Option<&'a str>,
    ) -> NarrowingInput<'a> {
        NarrowingInput {
            requested_visibility,
            requested_local_only: false,
            requested_visible_user_ids: Vec::new(),
            author_id,
            text,
            cw: None,
            renote_target: None,
            reply_target: None,
            renote_target_blocks_author: false,
            sensitive_words: &[],
            can_public_note: true,
        }
    }

    fn target_note(author_id: &str, visibility: Visibility, local_only: bool) -> Note {
        make_note(
            author_id,
            visibility,
            local_only,
            vec!["carol"],
            vec![],
            None,
            None,
            false,
        )
    }

    #[test]
    fn renote_narrowing_rules() {
        // Public target: allowed, no narrowing.
        let public = target_note("bob", Visibility::Public, false);
        let out = narrow_on_create(NarrowingInput {
            renote_target: Some(&public),
            ..base_input("alice", Visibility::Public, None)
        })
        .unwrap();
        assert_eq!(out.visibility, Visibility::Public);
        assert!(!out.local_only);

        // Home target forces home-or-narrower.
        let home = target_note("bob", Visibility::Home, false);
        let out = narrow_on_create(NarrowingInput {
            renote_target: Some(&home),
            ..base_input("alice", Visibility::Public, None)
        })
        .unwrap();
        assert_eq!(out.visibility, Visibility::Home);
        // Already-narrower requests are kept.
        let out = narrow_on_create(NarrowingInput {
            renote_target: Some(&home),
            requested_visible_user_ids: vec!["carol".to_string()],
            ..base_input("alice", Visibility::Specified, None)
        })
        .unwrap();
        assert_eq!(out.visibility, Visibility::Specified);

        // Followers target: own notes allowed (forcing followers-or-narrower),
        // others' rejected.
        let own_followers = target_note("alice", Visibility::Followers, false);
        let out = narrow_on_create(NarrowingInput {
            renote_target: Some(&own_followers),
            ..base_input("alice", Visibility::Public, None)
        })
        .unwrap();
        assert_eq!(out.visibility, Visibility::Followers);
        let others_followers = target_note("bob", Visibility::Followers, false);
        let err = narrow_on_create(NarrowingInput {
            renote_target: Some(&others_followers),
            ..base_input("alice", Visibility::Public, None)
        })
        .expect_err("renoting another user's followers note must be rejected");
        assert_eq!(err, DomainError::Validation("CANNOT_RENOTE".into()));

        // Specified target always rejected.
        let specified = target_note("alice", Visibility::Specified, false);
        let err = narrow_on_create(NarrowingInput {
            renote_target: Some(&specified),
            ..base_input("alice", Visibility::Public, None)
        })
        .expect_err("renoting a specified note must be rejected");
        assert_eq!(err, DomainError::Validation("CANNOT_RENOTE".into()));

        // local_only target propagates local_only.
        let local_target = target_note("bob", Visibility::Public, true);
        let out = narrow_on_create(NarrowingInput {
            renote_target: Some(&local_target),
            ..base_input("alice", Visibility::Public, None)
        })
        .unwrap();
        assert!(out.local_only);
    }

    #[test]
    fn reply_narrowing_rules() {
        // Home/followers reply targets force home-/followers-or-narrower.
        let home = target_note("bob", Visibility::Home, false);
        let out = narrow_on_create(NarrowingInput {
            reply_target: Some(&home),
            ..base_input("alice", Visibility::Public, Some("reply"))
        })
        .unwrap();
        assert_eq!(out.visibility, Visibility::Home);

        let followers = target_note("bob", Visibility::Followers, false);
        let out = narrow_on_create(NarrowingInput {
            reply_target: Some(&followers),
            ..base_input("alice", Visibility::Public, Some("reply"))
        })
        .unwrap();
        assert_eq!(out.visibility, Visibility::Followers);

        // Specified reply target forces specified and inherits the
        // target's audience plus its author.
        let specified = target_note("bob", Visibility::Specified, false);
        let out = narrow_on_create(NarrowingInput {
            reply_target: Some(&specified),
            ..base_input("alice", Visibility::Public, Some("reply"))
        })
        .unwrap();
        assert_eq!(out.visibility, Visibility::Specified);
        assert!(out.visible_user_ids.contains(&"bob".to_string()));
        assert!(out.visible_user_ids.contains(&"carol".to_string()));
        assert!(!out.visible_user_ids.contains(&"alice".to_string()));

        // Explicit audience on the reply is kept, not overwritten.
        let out = narrow_on_create(NarrowingInput {
            reply_target: Some(&specified),
            requested_visible_user_ids: vec!["dave".to_string()],
            ..base_input("alice", Visibility::Specified, Some("reply"))
        })
        .unwrap();
        assert_eq!(out.visible_user_ids, vec!["dave".to_string()]);

        // local_only reply target propagates local_only.
        let local_target = target_note("bob", Visibility::Public, true);
        let out = narrow_on_create(NarrowingInput {
            reply_target: Some(&local_target),
            ..base_input("alice", Visibility::Public, Some("reply"))
        })
        .unwrap();
        assert!(out.local_only);
    }

    #[test]
    fn blocking_rejects_pure_renotes_only() {
        let target = target_note("bob", Visibility::Public, false);
        let err = narrow_on_create(NarrowingInput {
            renote_target: Some(&target),
            renote_target_blocks_author: true,
            ..base_input("alice", Visibility::Public, None)
        })
        .expect_err("pure renote by a blocked author must be rejected");
        assert_eq!(err, DomainError::Validation("YOU_HAVE_BEEN_BLOCKED".into()));

        // Quote renote (text present) is not a pure renote: allowed.
        let out = narrow_on_create(NarrowingInput {
            renote_target: Some(&target),
            renote_target_blocks_author: true,
            ..base_input("alice", Visibility::Public, Some("quote"))
        })
        .unwrap();
        assert_eq!(out.visibility, Visibility::Public);
    }

    #[test]
    fn instance_policy_downgrades_public_to_home() {
        // Missing public-post permission downgrades.
        let out = narrow_on_create(NarrowingInput {
            can_public_note: false,
            ..base_input("alice", Visibility::Public, Some("hi"))
        })
        .unwrap();
        assert_eq!(out.visibility, Visibility::Home);

        // Sensitive word in text (case-insensitive) downgrades.
        let words = vec!["spoiler".to_string()];
        let out = narrow_on_create(NarrowingInput {
            sensitive_words: &words,
            ..base_input("alice", Visibility::Public, Some("a SPOILER ahead"))
        })
        .unwrap();
        assert_eq!(out.visibility, Visibility::Home);

        // Sensitive word in CW downgrades.
        let out = narrow_on_create(NarrowingInput {
            sensitive_words: &words,
            cw: Some("spoilerish"),
            ..base_input("alice", Visibility::Public, Some("clean"))
        })
        .unwrap();
        assert_eq!(out.visibility, Visibility::Home);

        // No match: stays public.
        let out = narrow_on_create(NarrowingInput {
            sensitive_words: &words,
            ..base_input("alice", Visibility::Public, Some("clean"))
        })
        .unwrap();
        assert_eq!(out.visibility, Visibility::Public);

        // Downgrade only applies to public requests.
        let out = narrow_on_create(NarrowingInput {
            can_public_note: false,
            sensitive_words: &words,
            ..base_input("alice", Visibility::Followers, Some("spoiler hi"))
        })
        .unwrap();
        assert_eq!(out.visibility, Visibility::Followers);
    }
}
