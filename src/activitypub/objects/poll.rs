//! Polls (§4): the `Question` renderer, the inbound `oneOf`/`anyOf`
//! extractor, and AP time parsing (Misskey `ApRendererService.renderQuestion`
//! + `renderVote` and `ApQuestionService.extractPollFromQuestion` parity).

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::activitypub::format_millis;

use super::audience_context::ap_context;

// ---------------------------------------------------------------------------
// Polls (§4; Misskey `ApRendererService.renderQuestion` + `renderVote`)
// ---------------------------------------------------------------------------

/// Render a standalone `Question` object for a poll note (Misskey
/// `renderQuestion` parity): `{ type: Question, id: {base}/questions/{id},
/// actor, content, oneOf|anyOf: [{ name, _misskey_votes,
/// replies: { type: Collection, totalItems } }] }`. `multiple` selects
/// `anyOf`, otherwise `oneOf`.
pub fn render_question(
    public_url: &str,
    author_actor_uri: &str,
    note_id: &str,
    text: Option<&str>,
    poll: &crate::domain::poll::Poll,
    now_ms: i64,
) -> Value {
    let base = public_url.trim_end_matches('/');
    let options: Vec<Value> = poll
        .choices
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let count = poll.votes.get(i).copied().unwrap_or(0);
            json!({
                "type": "Note",
                "name": name,
                "_misskey_votes": count,
                "replies": { "type": "Collection", "totalItems": count },
            })
        })
        .collect();
    let mut doc = json!({
        "type": "Question",
        "id": format!("{base}/questions/{note_id}"),
        "actor": author_actor_uri,
        "attributedTo": author_actor_uri,
        "content": text.unwrap_or(""),
        if poll.multiple { "anyOf" } else { "oneOf" }: options,
    });
    if let Some(expires_at_ms) = poll.expires_at_ms {
        // Misskey `renderNote` parity: an already-past expiry serializes
        // as `closed`, a future one as `endTime`.
        doc[if expires_at_ms < now_ms {
            "closed"
        } else {
            "endTime"
        }] = Value::String(format_millis(expires_at_ms));
    }
    doc
}

/// A poll carried on an inbound `Note`/`Question` object (Misskey
/// `ApQuestionService.extractPollFromQuestion` parity): `oneOf` selects
/// single-choice, `anyOf` multi-choice; expiry reads `endTime` (else
/// `closed`); per-choice counts read `replies.totalItems` (else
/// `_misskey_votes`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedPoll {
    pub choices: Vec<String>,
    pub votes: Vec<u64>,
    pub multiple: bool,
    pub expires_at_ms: Option<i64>,
}

/// Extract a [`ParsedPoll`] from a Note/Question object map, or `None`
/// when the object carries no usable `oneOf`/`anyOf` (fewer than 2 named
/// choices — the storage invariant minimum).
///
/// Option `type` is deliberately unchecked: Mastodon/Glitch-soc serialize
/// poll options as `{type: "Note", name, replies}` (pinned-revision
/// fixtures in `tests/gaps_fed_emoji.rs`), Misskey omits `type` entirely.
/// Both shapes parse identically; only the choice count gates.
pub fn parse_question_poll(obj: &serde_json::Map<String, Value>) -> Option<ParsedPoll> {
    let one_of = obj.get("oneOf").and_then(Value::as_array);
    let any_of = obj.get("anyOf").and_then(Value::as_array);
    // Misskey parity: `multiple = oneOf === undefined` (an object carrying
    // both reads as `oneOf`).
    let (options, multiple) = match (one_of, any_of) {
        (Some(options), _) => (options, false),
        (None, Some(options)) => (options, true),
        (None, None) => return None,
    };
    let mut choices = Vec::new();
    let mut votes = Vec::new();
    for option in options {
        let name = option.get("name").and_then(Value::as_str)?;
        choices.push(name.to_string());
        let count = option
            .get("replies")
            .and_then(|r| r.get("totalItems"))
            .and_then(Value::as_u64)
            .or_else(|| option.get("_misskey_votes").and_then(Value::as_u64))
            .unwrap_or(0);
        votes.push(count);
    }
    if choices.len() < 2 {
        return None;
    }
    let expires_at_ms = obj
        .get("endTime")
        .or_else(|| obj.get("closed"))
        .and_then(parse_ap_time_ms);
    Some(ParsedPoll {
        choices,
        votes,
        multiple,
        expires_at_ms,
    })
}

/// Parse an ActivityPub time value (unix millis integer or ISO-8601/RFC3339
/// string) to unix millis. `None` on missing/unparseable input.
pub fn parse_ap_time_ms(val: &Value) -> Option<i64> {
    match val {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_u64().and_then(|u| i64::try_from(u).ok())),
        Value::String(s) => {
            let s = s.trim();
            if s.is_empty() {
                return None;
            }
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.timestamp_millis())
        }
        _ => None,
    }
}

/// Render an `Update(Question)` activity carrying the fresh tallies
/// (Misskey `PollService.deliverQuestionUpdate` parity:
/// `renderUpdate(renderNote(note))` to followers + relays).
pub fn render_question_update_activity(
    public_url: &str,
    update_id: &str,
    author_actor_uri: &str,
    question: Value,
    to: Vec<String>,
    now_ms: i64,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{update_id}"),
        "actor": author_actor_uri,
        "type": "Update",
        "to": to,
        "object": question,
        "published": format_millis(now_ms),
    })
}

/// Render a `Create` vote activity for a vote on a remote poll note
/// (Misskey `ApRendererService.renderVote` parity): the object is a `Note`
/// with `inReplyTo` = poll note URI and `name` = choice text.
#[allow(clippy::too_many_arguments)]
pub fn render_vote_activity(
    public_url: &str,
    vote_id: &str,
    voter_actor_uri: &str,
    poll_owner_uri: &str,
    target_note_uri: &str,
    choice_name: &str,
    now_ms: i64,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/users/{vote_id}/votes/activity"),
        "actor": voter_actor_uri,
        "type": "Create",
        "to": [poll_owner_uri],
        "published": format_millis(now_ms),
        "object": {
            "id": format!("{base}/users/{vote_id}/votes"),
            "type": "Note",
            "attributedTo": voter_actor_uri,
            "to": [poll_owner_uri],
            "inReplyTo": target_note_uri,
            "name": choice_name,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activitypub::objects::note_document::render_note_object;
    use crate::activitypub::objects::note_parser::parse_remote_note;
    use crate::application::commands::AppServices;
    use crate::application::ports::{ActivityDedup, CreateNoteInput, NoopStreamingSink};
    use crate::domain::id::{AidxGenerator, Clock, IdGenerator, ManualClock};
    use crate::federation::delivery::{SafeHttpClient, SafeHttpConfig};
    use crate::media::storage::FsBlobStore;
    use crate::persistence::redb::RedbStore;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn test_services(prefix: &str) -> AppServices {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-ap-poll-{}-{prefix}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_760_400_000_000));
        let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
        let store = RedbStore::open(dir.join("test.redb"), clock.clone(), ids.clone()).unwrap();
        let blobs = Arc::new(FsBlobStore::new(dir.join("blobs")).unwrap());
        AppServices {
            clock,
            ids,
            accounts: store.clone(),
            notes: store.clone(),
            follows: store.clone(),
            blocks: store.clone(),
            mutes: store.clone(),
            reactions: store.clone(),
            favorites: store.clone(),
            emojis: store.clone(),
            antennas: store.clone(),
            antenna_notes: store.clone(),
            polls: store.clone(),
            jobs: store.clone(),
            media: store.clone(),
            blobs: blobs.clone(),
            notifications: store.clone(),
            auth: store.clone(),
            streaming: Arc::new(NoopStreamingSink),
            dedup: store.clone() as Arc<dyn ActivityDedup>,
            host_caps: store.clone(),
            moderation: store.clone(),
            instance_settings: store.clone(),
            invites: store.clone(),
            abuse_reports: store.clone(),
            lists: store.clone(),
            signin: store.clone(),
            password_resets: store.clone(),
            registry: store.clone(),
            clips: store.clone(),
            pages: store.clone(),
            folders: store.clone(),
            system_webhooks: store.clone(),
            user_webhooks: store.clone(),
            abuse_recipients: store.clone(),
            memos: store.clone(),
            thread_mutes: store.clone(),
            federation_http: SafeHttpClient::new(SafeHttpConfig {
                allow_private_ips: true,
                ..Default::default()
            }),
            public_url: "https://pinetree.local".to_string(),
            max_file_size: 10 * 1024 * 1024,
            preserved_usernames: Vec::new(),
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
            note_draft_limit: crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
            scheduled_note_limit: crate::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
            pin_limit: crate::domain::pinning::DEFAULT_PIN_LIMIT,
            default_like: None,
            cache_blobs: blobs.clone(),
            drive_capacity_per_user_bytes: 1 << 30,
            remote_cache_max_bytes: 512 << 20,
            remote_cache_avatar_bytes: 128 << 20,
            remote_cache_preview_bytes: 256 << 20,
            remote_cache_full_bytes: 128 << 20,
            remote_cache_mode: crate::config::RemoteCacheMode::OnDemand,
            remote_cache_avatar_ttl_days: 30,
            remote_cache_preview_ttl_days: 7,
            remote_cache_full_ttl_days: 3,
            remote_cache_warming_enabled: false,
        }
    }

    // --- §4 polls: Question render/parse -----------------------------------

    fn poll_fixture() -> crate::domain::poll::Poll {
        crate::domain::poll::Poll {
            note_id: "note1".to_string(),
            choices: vec!["red".to_string(), "blue".to_string()],
            votes: vec![3, 7],
            multiple: false,
            expires_at_ms: Some(1_760_400_060_000),
        }
    }

    #[test]
    fn render_question_matches_misskey_shape() {
        let now = 1_760_400_000_000;
        let poll = poll_fixture();
        let doc = render_question(
            "https://pinetree.local",
            "https://pinetree.local/users/alice",
            "note1",
            Some("pick a color"),
            &poll,
            now,
        );
        assert_eq!(doc["type"], "Question");
        assert_eq!(doc["id"], "https://pinetree.local/questions/note1");
        // Single-choice → `oneOf`, future expiry → `endTime`.
        assert!(doc.get("oneOf").is_some());
        assert!(doc.get("anyOf").is_none());
        assert!(doc.get("endTime").is_some());
        assert!(doc.get("closed").is_none());
        assert_eq!(doc["oneOf"][0]["name"], "red");
        assert_eq!(doc["oneOf"][0]["type"], "Note");
        assert_eq!(doc["oneOf"][0]["replies"]["totalItems"], 3);
        assert_eq!(doc["oneOf"][0]["_misskey_votes"], 3);
        assert_eq!(doc["oneOf"][1]["replies"]["totalItems"], 7);

        // Multi-choice → `anyOf`; past expiry → `closed`.
        let mut multi = poll;
        multi.multiple = true;
        let doc = render_question(
            "https://pinetree.local",
            "https://pinetree.local/users/alice",
            "note1",
            None,
            &multi,
            1_760_400_061_000,
        );
        assert!(doc.get("anyOf").is_some());
        assert!(doc.get("oneOf").is_none());
        assert!(doc.get("closed").is_some());
        assert!(doc.get("endTime").is_none());
    }

    #[test]
    fn parse_question_poll_reads_oneof_anyof_and_times() {
        // `oneOf` with `replies.totalItems`.
        let doc = serde_json::json!({
            "type": "Question",
            "id": "https://remote.example/questions/1",
            "oneOf": [
                {"name": "a", "replies": {"type": "Collection", "totalItems": 2}},
                {"name": "b", "replies": {"type": "Collection", "totalItems": 5}},
            ],
            "endTime": "2026-10-16T00:01:00.000Z",
        });
        let parsed = parse_question_poll(doc.as_object().unwrap()).unwrap();
        assert!(!parsed.multiple);
        assert_eq!(parsed.choices, vec!["a", "b"]);
        assert_eq!(parsed.votes, vec![2, 5]);
        assert_eq!(
            parsed.expires_at_ms,
            Some(
                chrono::DateTime::parse_from_rfc3339("2026-10-16T00:01:00.000Z")
                    .unwrap()
                    .timestamp_millis()
            )
        );

        // `anyOf` with `_misskey_votes` fallback + `closed`.
        let doc = serde_json::json!({
            "type": "Question",
            "id": "https://remote.example/questions/2",
            "anyOf": [
                {"name": "x", "_misskey_votes": 1},
                {"name": "y", "_misskey_votes": 0},
            ],
            "closed": "2026-10-15T00:00:00.000Z",
        });
        let parsed = parse_question_poll(doc.as_object().unwrap()).unwrap();
        assert!(parsed.multiple);
        assert_eq!(parsed.votes, vec![1, 0]);
        assert!(parsed.expires_at_ms.is_some());

        // No options / fewer than two named choices → no poll.
        let doc = serde_json::json!({"type": "Note", "id": "https://x/1"});
        assert!(parse_question_poll(doc.as_object().unwrap()).is_none());
        let doc = serde_json::json!({
            "type": "Question", "id": "https://x/2",
            "oneOf": [{"name": "lonely"}],
        });
        assert!(parse_question_poll(doc.as_object().unwrap()).is_none());
    }

    #[test]
    fn question_render_parse_roundtrip() {
        let now = 1_760_400_000_000;
        let poll = poll_fixture();
        let doc = render_question(
            "https://pinetree.local",
            "https://pinetree.local/users/alice",
            "note1",
            Some("pick a color"),
            &poll,
            now,
        );
        let back = parse_question_poll(doc.as_object().unwrap()).unwrap();
        assert_eq!(back.choices, poll.choices);
        assert_eq!(back.votes, poll.votes);
        assert_eq!(back.multiple, poll.multiple);
        assert_eq!(back.expires_at_ms, poll.expires_at_ms);
    }

    #[test]
    fn render_note_object_embeds_question_for_poll_notes() {
        let services = test_services("question-embed");
        let alice = services.create_account("alice", None).unwrap();
        let mut input = CreateNoteInput::simple_public(&alice.id, "pick one");
        input.poll = Some(crate::domain::poll::PollInput {
            choices: vec!["a".to_string(), "b".to_string()],
            multiple: true,
            expires_at_ms: None,
        });
        input.has_poll = true;
        let note = services.create_note(input).unwrap();

        let doc = render_note_object(&services, &note, true).unwrap();
        // Misskey `renderNote` `...asPoll` parity: the poll overwrites the type.
        assert_eq!(doc["type"], "Question");
        assert!(doc.get("anyOf").is_some());
        assert_eq!(doc["anyOf"][0]["replies"]["totalItems"], 0);

        // Poll-less notes still render as `Note` with no options.
        let plain = services
            .create_note(CreateNoteInput::simple_public(&alice.id, "plain"))
            .unwrap();
        let doc = render_note_object(&services, &plain, true).unwrap();
        assert_eq!(doc["type"], "Note");
        assert!(doc.get("oneOf").is_none());
        assert!(doc.get("anyOf").is_none());

        // The rendered Question re-parses (federation round-trip).
        let doc = render_note_object(&services, &note, true).unwrap();
        let parsed = parse_remote_note(&doc).unwrap();
        let poll = parsed.poll.unwrap();
        assert_eq!(poll.choices, vec!["a", "b"]);
        assert!(poll.multiple);
    }

    #[test]
    fn render_vote_activity_matches_misskey_shape() {
        let doc = render_vote_activity(
            "https://pinetree.local",
            "vote1",
            "https://pinetree.local/users/bob",
            "https://remote.example/users/alice",
            "https://remote.example/notes/1",
            "blue",
            1_760_400_000_000,
        );
        assert_eq!(doc["type"], "Create");
        assert_eq!(doc["actor"], "https://pinetree.local/users/bob");
        assert_eq!(
            doc["to"],
            serde_json::json!(["https://remote.example/users/alice"])
        );
        assert_eq!(doc["object"]["type"], "Note");
        assert_eq!(doc["object"]["inReplyTo"], "https://remote.example/notes/1");
        assert_eq!(doc["object"]["name"], "blue");
    }
}
