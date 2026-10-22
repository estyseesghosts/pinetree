//! Outbound Note rendering (Slice B1): the local [`Note`] becomes an
//! ActivityPub `Note` object with Misskey-compatible audience, tags,
//! attachments, structured quotes, and embedded polls.

use serde_json::{Value, json};

use crate::activitypub::format_millis;
use crate::activitypub::webfinger::extract_host;
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::domain::media::MediaMeta;
use crate::domain::note::{Note, Visibility};

use super::audience_context::{ap_context, render_note_audience};
use super::markup::{html_escape, mfm_to_html};
use super::poll::render_question;

// ---------------------------------------------------------------------------
// Outbound Note & Activity rendering (Slice B1)
// ---------------------------------------------------------------------------

/// Render a local [`Note`] as an ActivityPub `Note` object.
///
/// Includes `@context` when `include_context` is true (top-level `/notes/{id}`
/// GET response). Never emits `_misskey_content`; emits `source` with
/// `mediaType: "text/x.misskeymarkdown"` instead.
pub fn render_note_object(
    services: &AppServices,
    note: &Note,
    include_context: bool,
) -> Result<Value, PortError> {
    let base = services.public_url.trim_end_matches('/');
    let author = services
        .accounts
        .find_by_id(&note.author_id)?
        .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
    let note_uri = note
        .uri
        .clone()
        .unwrap_or_else(|| format!("{base}/notes/{}", note.id));
    let author_uri = author.actor_url(base);
    let published = format_millis(
        services
            .ids
            .parse_time(&note.id)
            .unwrap_or_else(|_| services.clock.now_ms()),
    );
    let (to, cc) = render_note_audience(services, &author, note);

    // Slice B: a pending (unresolved-parent) reply never leaks its
    // `reply_parent_uri` — neither as `inReplyTo` here nor through the
    // Misskey packer. Such notes are withheld from the AP object endpoints
    // by `is_visible` anyway; this keeps the renderer total even if one
    // ever reaches it (e.g. outbound delivery of a note linked between
    // packing and rendering cannot happen, but defense in depth is cheap).
    let in_reply_to = match &note.reply_id {
        Some(rid) if note.reply_parent_uri.is_none() => match services.notes.get_note(rid)? {
            Some(parent) => Some(
                parent
                    .uri
                    .unwrap_or_else(|| format!("{base}/notes/{}", parent.id)),
            ),
            None => None,
        },
        _ => None,
    };

    let content_html = note.text.as_deref().map(|t| mfm_to_html(t, base));
    let source = note.text.as_ref().map(|t| {
        json!({
            "content": t,
            "mediaType": "text/x.misskeymarkdown",
        })
    });
    // Attachments from `file_ids`
    let mut attachments = Vec::new();
    let mut any_sensitive_file = false;
    for fid in &note.file_ids {
        if let Some(meta) = services.media.get(fid)? {
            if meta.is_sensitive {
                any_sensitive_file = true;
            }
            attachments.push(render_attachment(&meta));
        }
    }

    // Tags: Mentions + Hashtags
    let mut tags = Vec::new();
    for uid in &note.mentions {
        if let Some(acc) = services.accounts.find_by_id(uid)? {
            let href = acc.actor_url(base);
            let handle = match &acc.host {
                Some(h) => format!("@{}@{h}", acc.username),
                None => format!("@{}@{}", acc.username, extract_host(base)),
            };
            tags.push(json!({
                "type": "Mention",
                "href": href,
                "name": handle,
            }));
        }
    }
    for tag in &note.tags {
        tags.push(json!({
            "type": "Hashtag",
            "href": format!("{base}/tags/{}", tag.to_lowercase()),
            "name": format!("#{tag}"),
        }));
    }

    // Structured quote (FEP-044f canonical + legacy aliases + policy +
    // single textual fallback). The stored body is never mutated: the
    // fallback is generated on the serialized `content` only, and only
    // when the target is safe to disclose to this post's audience.
    let mut content_html = content_html;
    let mut quote_extra = serde_json::Map::new();
    if let Some(quote) = note.quote.as_ref() {
        use crate::activitypub::quotes::{
            has_terminal_re_fallback, render_quote_fields, render_quote_link_tag,
            render_quote_policy,
        };
        use crate::domain::quote::QuoteAuthorizationState;
        // `quoteAuthorization` only with a valid stamp (Approved carries
        // one; exceptions and all other states serialize no stamp).
        let stamp = match quote.authorization {
            QuoteAuthorizationState::Approved => quote.authorization_uri.as_deref(),
            _ => None,
        };
        for (k, v) in render_quote_fields(&quote.target_uri, stamp)
            .as_object()
            .cloned()
            .unwrap_or_default()
        {
            quote_extra.insert(k, v);
        }
        // Advisory quoting policy for this object (never proof of approval,
        // and never `manualApproval`: no review workflow exists).
        let quoting_public_audience =
            matches!(note.visibility, Visibility::Public | Visibility::Home);
        for (k, v) in render_quote_policy(quoting_public_audience, &author_uri)
            .as_object()
            .cloned()
            .unwrap_or_default()
        {
            quote_extra.insert(k, v);
        }
        // FEP-e232 compatibility Link tag (single, non-duplicated).
        tags.push(render_quote_link_tag(&quote.target_uri));
        // Textual `RE:` fallback: public quoting posts only, known-public
        // targets only, never twice. Unresolved targets get no fallback:
        // their visibility is unknown (possibly private) and a public
        // `RE:` would leak existence.
        let target_public = match quote.target_note_id.as_deref() {
            Some(tid) => match services.notes.get_note(tid)? {
                Some(target) => {
                    matches!(target.visibility, Visibility::Public | Visibility::Home)
                        && !target.local_only
                        && !target.is_deleted
                }
                None => false,
            },
            None => false,
        };
        if quoting_public_audience
            && target_public
            && !has_terminal_re_fallback(note.text.as_deref(), &quote.target_uri)
        {
            let escaped = html_escape(&quote.target_uri);
            let suffix = format!(
                "<br><br><span class=\"quote-inline\">RE: <a href=\"{escaped}\">{escaped}</a></span>"
            );
            match content_html.as_mut() {
                Some(html) => html.push_str(&suffix),
                None => {
                    content_html = Some(format!(
                        "<p><span class=\"quote-inline\">RE: <a href=\"{escaped}\">{escaped}</a></span></p>"
                    ));
                }
            }
        }
    }

    let mut obj = json!({
        "id": note_uri,
        "type": "Note",
        "attributedTo": author_uri,
        "summary": note.cw,
        "content": content_html,
        "source": source,
        "inReplyTo": in_reply_to,
        "published": published,
        "to": to,
        "cc": cc,
        "tag": tags,
        "attachment": attachments,
        "sensitive": note.cw.is_some() || any_sensitive_file,
    });
    // §4 polls: a poll note renders as a `Question` object carrying
    // `oneOf`/`anyOf` tallies (Misskey `renderNote` `...asPoll` parity —
    // the spread overwrites `type` there too).
    if let Some(poll) = services.polls.get_poll(&note.id)? {
        let question = render_question(
            base,
            &author_uri,
            &note.id,
            note.text.as_deref(),
            &poll,
            services.clock.now_ms(),
        );
        if let Some(qmap) = question.as_object() {
            for key in ["oneOf", "anyOf", "endTime", "closed"] {
                if let Some(v) = qmap.get(key) {
                    obj[key] = v.clone();
                }
            }
        }
        obj["type"] = Value::String("Question".to_string());
    }
    for (k, v) in quote_extra {
        obj[k] = v;
    }
    if include_context {
        if let Some(map) = obj.as_object_mut() {
            map.insert("@context".to_string(), ap_context());
        }
    }
    Ok(obj)
}

fn render_attachment(meta: &MediaMeta) -> Value {
    let doc_type = if meta.mime.starts_with("image/") {
        "Image"
    } else if meta.mime.starts_with("video/") {
        "Video"
    } else if meta.mime.starts_with("audio/") {
        "Audio"
    } else {
        "Document"
    };
    let mut att = json!({
        "type": doc_type,
        "mediaType": meta.mime,
        "url": meta.url,
        "name": meta.comment.clone().unwrap_or_else(|| meta.name.clone()),
        "sensitive": meta.is_sensitive,
    });
    if let Some(bh) = &meta.blurhash {
        att["blurhash"] = Value::String(bh.clone());
    }
    if let (Some(w), Some(h)) = (meta.properties.width, meta.properties.height) {
        att["width"] = Value::Number(w.into());
        att["height"] = Value::Number(h.into());
    }
    att
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activitypub::actor::{parse_remote_actor, render_actor_document};
    use crate::activitypub::objects::note_parser::parse_remote_note;
    use crate::application::ports::{ActivityDedup, CreateNoteInput, NoopStreamingSink};
    use crate::domain::id::{AidxGenerator, Clock, IdGenerator, ManualClock};
    use crate::domain::quote::{QuoteAuthorizationState, QuoteReference};
    use crate::federation::delivery::{SafeHttpClient, SafeHttpConfig};
    use crate::media::storage::FsBlobStore;
    use crate::persistence::redb::RedbStore;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn test_services(prefix: &str) -> AppServices {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-ap-doc-{}-{prefix}-{}",
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

    fn quote_of(services: &AppServices, quoter: &str, target: &Note, text: &str) -> Note {
        let mut input = CreateNoteInput::simple_public(quoter, text);
        input.renote_of = Some(target.id.clone());
        services.create_note(input).unwrap()
    }

    #[test]
    fn quote_renders_canonical_plus_aliases_policy_and_single_fallback() {
        let services = test_services("full");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let target = services
            .create_note(CreateNoteInput::simple_public(&alice.id, "original"))
            .unwrap();
        let target_uri = format!("https://pinetree.local/notes/{}", target.id);
        let quote = quote_of(&services, &bob.id, &target, "commentary");

        let doc = render_note_object(&services, &quote, true).unwrap();
        // Canonical FEP-044f + legacy compatibility aliases.
        assert_eq!(doc["quote"], target_uri.as_str());
        assert_eq!(doc["_misskey_quote"], target_uri.as_str());
        assert_eq!(doc["quoteUrl"], target_uri.as_str());
        assert_eq!(doc["quoteUri"], target_uri.as_str());
        // No stamp, no authorization field.
        assert!(doc.get("quoteAuthorization").is_none());
        // Advisory policy, never manualApproval (no review workflow exists).
        assert!(
            doc["interactionPolicy"]["canQuote"]["automaticApproval"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v == "https://www.w3.org/ns/activitystreams#Public")
        );
        assert!(
            doc["interactionPolicy"]["canQuote"]
                .get("manualApproval")
                .is_none()
        );
        // Exactly one FEP-e232 Link tag.
        let links: Vec<&Value> = doc["tag"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|t| t.get("type").and_then(Value::as_str) == Some("Link"))
            .collect();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0]["href"], target_uri.as_str());
        // Single textual fallback in serialized content only (the URI
        // appears twice inside it: link href + link text).
        let content = doc["content"].as_str().unwrap();
        assert_eq!(content.matches("RE:").count(), 1, "exactly one fallback");
        assert!(content.contains("quote-inline"));
        // Stored body untouched.
        let stored = services.notes.get_note(&quote.id).unwrap().unwrap();
        assert_eq!(stored.text.as_deref(), Some("commentary"));
        // Context carries the quote terms, never the deprecated field.
        let ctx = serde_json::to_string(&doc["@context"]).unwrap();
        assert!(ctx.contains("https://w3id.org/fep/044f#quote"));
        assert!(!ctx.contains("_misskey_content"));
    }

    #[test]
    fn quote_render_never_duplicates_author_fallback() {
        let services = test_services("dup");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let target = services
            .create_note(CreateNoteInput::simple_public(&alice.id, "original"))
            .unwrap();
        let target_uri = format!("https://pinetree.local/notes/{}", target.id);
        let quote = quote_of(
            &services,
            &bob.id,
            &target,
            &format!("commentary\nRE: {target_uri}"),
        );
        let doc = render_note_object(&services, &quote, false).unwrap();
        let content = doc["content"].as_str().unwrap();
        assert_eq!(
            content.matches("RE:").count(),
            1,
            "no second fallback appended"
        );
    }

    #[test]
    fn quote_render_restricted_post_adds_no_public_fallback() {
        let services = test_services("restricted");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let target = services
            .create_note(CreateNoteInput::simple_public(&alice.id, "original"))
            .unwrap();
        let target_uri = format!("https://pinetree.local/notes/{}", target.id);
        // Followers-only quoting post: structured fields stay (restricted
        // audience), but no public RE: fallback is generated.
        let mut input = CreateNoteInput::simple_public(&bob.id, "commentary");
        input.visibility = Visibility::Followers;
        input.renote_of = Some(target.id.clone());
        let quote = services.create_note(input).unwrap();
        assert!(quote.is_quote());
        let doc = render_note_object(&services, &quote, false).unwrap();
        assert_eq!(doc["quote"], target_uri.as_str());
        let content = doc["content"].as_str().unwrap_or("");
        assert!(!content.contains("RE:"), "no fallback for restricted posts");
    }

    /// Slice C: outbound `inReplyTo` is always the DIRECT parent's canonical
    /// URI — a reply-to-a-reply targets its immediate parent, never the
    /// thread root. A pending (unresolved-parent) reply renders no
    /// `inReplyTo` at all: the internal URI is never disclosed.
    #[test]
    fn reply_renders_direct_parent_in_reply_to() {
        let services = test_services("reply-parent");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let root = services
            .create_note(CreateNoteInput::simple_public(&alice.id, "root"))
            .unwrap();
        let mut a_input = CreateNoteInput::simple_public(&bob.id, "reply A");
        a_input.reply_to = Some(root.id.clone());
        let a = services.create_note(a_input).unwrap();
        let mut a1_input = CreateNoteInput::simple_public(&alice.id, "reply A1");
        a1_input.reply_to = Some(a.id.clone());
        let a1 = services.create_note(a1_input).unwrap();

        let root_uri = format!("https://pinetree.local/notes/{}", root.id);
        let a_uri = format!("https://pinetree.local/notes/{}", a.id);
        let doc_a = render_note_object(&services, &a, false).unwrap();
        assert_eq!(doc_a["inReplyTo"], root_uri.as_str());
        let doc_a1 = render_note_object(&services, &a1, false).unwrap();
        assert_eq!(
            doc_a1["inReplyTo"],
            a_uri.as_str(),
            "A1 targets its direct parent A, not the root"
        );

        // Pending replies disclose nothing: no inReplyTo, and the stored
        // pending URI appears nowhere in the document.
        let pending_uri = "https://remote.example/notes/parent";
        let mut pending = a1.clone();
        pending.reply_id = None;
        pending.reply_parent_uri = Some(pending_uri.to_string());
        let doc_pending = render_note_object(&services, &pending, true).unwrap();
        assert!(
            doc_pending.get("inReplyTo").is_none() || doc_pending["inReplyTo"].is_null(),
            "pending replies render no inReplyTo"
        );
        let serialized = serde_json::to_string(&doc_pending).unwrap();
        assert!(
            !serialized.contains(pending_uri),
            "pending parent URI never leaks into the document"
        );
    }

    #[test]
    fn quote_render_stamp_only_with_approval() {
        let services = test_services("stamp");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let target = services
            .create_note(CreateNoteInput::simple_public(&alice.id, "original"))
            .unwrap();
        let target_uri = format!("https://pinetree.local/notes/{}", target.id);
        let quote = quote_of(&services, &bob.id, &target, "commentary");

        // Local quotes self-verify as exceptions: no stamp, no field.
        let doc = render_note_object(&services, &quote, false).unwrap();
        assert!(doc.get("quoteAuthorization").is_none());

        // Approved with a valid stamp: the stamp IRI serializes.
        let mut approved = quote.clone();
        approved.quote = Some(QuoteReference {
            target_uri: target_uri.clone(),
            target_note_id: Some(target.id.clone()),
            authorization: QuoteAuthorizationState::Approved,
            authorization_uri: Some("https://remote.example/stamps/1".to_string()),
            request_activity_uri: None,
            last_verified_at: None,
        });
        let doc = render_note_object(&services, &approved, false).unwrap();
        assert_eq!(doc["quoteAuthorization"], "https://remote.example/stamps/1");

        // Rejected quotes serialize no stamp (retained relation, no approval).
        let mut rejected = quote.clone();
        rejected.quote = Some(QuoteReference {
            target_uri,
            target_note_id: Some(target.id.clone()),
            authorization: QuoteAuthorizationState::Rejected,
            authorization_uri: None,
            request_activity_uri: None,
            last_verified_at: None,
        });
        let doc = render_note_object(&services, &rejected, false).unwrap();
        assert!(
            doc.get("quoteAuthorization").is_none(),
            "rejected quotes carry no stamp"
        );
        assert!(
            doc.get("quote").is_some(),
            "rejected quotes keep the relation"
        );
    }

    #[test]
    fn actor_and_note_documents_roundtrip_parse() {
        let services = test_services("roundtrip");
        let alice = services.create_account("alice", None).unwrap();
        let actor_doc = render_actor_document(&services, &alice).unwrap();
        let parsed_actor = parse_remote_actor(&actor_doc).unwrap();
        assert_eq!(parsed_actor.username, "alice");
        assert_eq!(parsed_actor.host, "pinetree.local");
        assert_eq!(
            parsed_actor.uri,
            format!("https://pinetree.local/users/{}", alice.id)
        );
        assert_eq!(
            parsed_actor.public_key_id,
            format!("https://pinetree.local/users/{}#main-key", alice.id)
        );
        assert!(parsed_actor.public_key_pem.contains("BEGIN PUBLIC KEY"));

        let note = services
            .create_note(CreateNoteInput::simple_public(&alice.id, "hello #test"))
            .unwrap();
        let note_doc = render_note_object(&services, &note, true).unwrap();
        let serialized = serde_json::to_string(&note_doc).unwrap();
        assert!(!serialized.contains("_misskey_content"));
        assert_eq!(note_doc["source"]["mediaType"], "text/x.misskeymarkdown");
        let parsed_note = parse_remote_note(&note_doc).unwrap();
        assert_eq!(parsed_note.text.as_deref(), Some("hello #test"));
        assert_eq!(parsed_note.hashtags, vec!["test".to_string()]);
    }
}
