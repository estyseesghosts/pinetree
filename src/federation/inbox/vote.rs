//! Inbound poll-vote handling: `Update(Question)` tallies
//! (moved from `src/federation/inbox.rs`).

use axum::response::Response;
use serde_json::Value;

use super::accepted;
use super::create::handle_create;
use crate::activitypub::objects::parse_question_poll;
use crate::application::commands::AppServices;
use crate::application::ports::{CreateNoteInput, PortError};
use crate::domain::note::Note;

/// Inbound `Update(Question)` (§4; Misskey `ApQuestionService.updateQuestion`
/// parity): refresh a cached remote poll's tallies from the embedded
/// question document. Authority: the question must be attributed to the
/// cached poll's owner and the signer must be that owner. Unknown notes,
/// locally authored polls, unattributed/misattributed questions, and
/// unparseable options all converge to accept-without-change. Count sync
/// itself is idempotent (overwrite by choice name), so replays converge.
///
/// §8 owns the sibling `Update(Note)`/`Update(Article)`/`Update(Page)`
/// edit branch (`handle_update_note` above, sharing this same inbox
/// dispatcher). This function stays scoped to `Question` objects only.
pub(crate) fn handle_update_question(
    services: &AppServices,
    doc: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    let Some(map) = doc.as_object() else {
        return Ok(accepted());
    };
    let Some(question_id) = map
        .get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return Ok(accepted());
    };
    // Authority gate first (before any fetch): the signer must own the
    // question it claims to update. `attributedTo` may be a string or an
    // embedded actor object; absent attribution inherits the signer.
    let attributed_to = match map.get("attributedTo") {
        Some(Value::String(s)) if !s.trim().is_empty() => Some(s.trim().to_string()),
        Some(Value::Object(m)) => m
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned),
        _ => None,
    };
    if let Some(attribution) = attributed_to.as_deref() {
        if attribution != actor_uri {
            return Ok(accepted());
        }
    }
    let question = match parse_question_poll(map) {
        Some(question) => question,
        None => return Ok(accepted()),
    };
    let counts: Vec<crate::application::ports::PollChoiceCount> = question
        .choices
        .iter()
        .enumerate()
        .map(|(i, name)| crate::application::ports::PollChoiceCount {
            name: name.clone(),
            votes: question.votes.get(i).copied().unwrap_or(0),
        })
        .collect();
    // `handle_inbound_question_update` re-checks authority against the
    // cached poll owner (attribution must equal the owner's actor URI) and
    // ignores locally authored polls, so a forged `Update` can only ever
    // refresh tallies on a remote poll its own owner signed.
    let _ = services.handle_inbound_question_update(
        activity_id,
        question_id,
        attributed_to.as_deref().or(Some(actor_uri)),
        counts,
    )?;
    Ok(accepted())
}

#[cfg(test)]
mod inbound_vote_tests {
    use super::*;
    use serde_json::json;

    use crate::application::ports::CreateAccountInput;
    use crate::domain::poll::PollInput;

    const CAROL_URI: &str = "https://remote.example/users/carol";

    fn setup() -> (AppServices, Note, String) {
        let services = crate::application::commands::tests::test_services("inbound-vote");
        let alice = services.create_account("alice", None).expect("alice");
        services
            .accounts
            .create_account(
                CreateAccountInput {
                    handle: "carol".to_string(),
                    host: Some("remote.example".to_string()),
                    display_name: None,
                    description: None,
                    location: None,
                    birthday: None,
                    uri: Some(CAROL_URI.to_string()),
                    inbox: Some("https://remote.example/inbox".to_string()),
                    shared_inbox: None,
                    is_bot: false,
                    is_cat: false,
                    is_locked: false,
                    preserved_usernames: Vec::new(),
                    also_known_as: None,
                    moved_to_uri: None,
                    moved_at_ms: None,
                },
                None,
            )
            .expect("carol");
        let mut input = CreateNoteInput::simple_public(&alice.id, "poll?");
        input.poll = Some(PollInput {
            choices: vec!["alpha".to_string(), "beta".to_string()],
            multiple: false,
            expires_at_ms: None,
        });
        input.has_poll = true;
        let note = services.create_note(input).expect("poll note");
        let uri = format!(
            "{}/notes/{}",
            services.public_url.trim_end_matches('/'),
            note.id
        );
        (services, note, uri)
    }

    fn votes(services: &AppServices, note: &Note) -> Vec<u64> {
        services.polls.get_poll(&note.id).unwrap().unwrap().votes
    }

    fn create(object: Value) -> Value {
        json!({ "type": "Create", "actor": CAROL_URI, "object": object })
    }

    /// Misskey/Sharkey/Catodon: a `Note` carrying `name` + `inReplyTo`.
    #[test]
    fn note_shaped_vote_is_tallied() {
        let (services, note, uri) = setup();
        let activity = create(json!({
            "id": "https://remote.example/notes/v1", "type": "Note",
            "attributedTo": CAROL_URI, "inReplyTo": uri, "name": "beta",
            "to": [uri.clone()], "content": null,
        }));
        handle_create(
            &services,
            &activity,
            "https://remote.example/a/v1",
            CAROL_URI,
        )
        .unwrap();
        assert_eq!(votes(&services, &note), vec![0, 1]);
    }

    /// Pleroma/Akkoma: `Create(Answer{name, inReplyTo})`. Previously the
    /// parser rejected the type and the vote vanished without a trace.
    #[test]
    fn answer_shaped_vote_is_tallied() {
        let (services, note, uri) = setup();
        let activity = create(json!({
            "id": "https://remote.example/answers/1", "type": "Answer",
            "attributedTo": CAROL_URI, "inReplyTo": uri, "name": "alpha",
            "to": [uri.clone()],
        }));
        handle_create(
            &services,
            &activity,
            "https://remote.example/a/v2",
            CAROL_URI,
        )
        .unwrap();
        assert_eq!(votes(&services, &note), vec![1, 0]);
    }

    /// An `Answer` for an unknown parent is dropped, never stored as a note.
    #[test]
    fn answer_for_unknown_parent_stores_nothing() {
        let (services, _note, _uri) = setup();
        let activity = create(json!({
            "id": "https://remote.example/answers/2", "type": "Answer",
            "attributedTo": CAROL_URI, "inReplyTo": "https://elsewhere.example/notes/x",
            "name": "alpha",
        }));
        handle_create(
            &services,
            &activity,
            "https://remote.example/a/v3",
            CAROL_URI,
        )
        .unwrap();
        assert!(
            services
                .notes
                .find_by_uri("https://remote.example/answers/2")
                .unwrap()
                .is_none()
        );
    }
}
