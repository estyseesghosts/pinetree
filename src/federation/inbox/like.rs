//! Inbound `Like` / `EmojiReact` handling (moved from `src/federation/inbox.rs`).

use axum::http::StatusCode;
use axum::response::Response;
use serde_json::Value;

use super::accepted;
use super::common::{resolve_note_target_id, resolve_remote_account};
use super::dispatch;
use super::follow::handle_undo;
use crate::activitypub::objects::{extract_like_reaction, extract_object_uri};
use crate::application::commands::AppServices;
use crate::application::ports::{CreateNoteInput, PortError};
use crate::domain::account::Account;
use crate::domain::note::Note;

/// Record a remote `Like` on a page and notify the page owner (Phase
/// 2c: closes the silent-likes gap). Replays converge without a second
/// notification (the like row is the guard).
pub(crate) fn notify_page_owner_of_like(
    services: &AppServices,
    activity_id: &str,
    liker_id: &str,
    page: &crate::domain::page::Page,
) -> Result<(), PortError> {
    if liker_id == page.owner_id {
        return Ok(());
    }
    if services.pages.is_page_liked(liker_id, &page.id)? {
        return Ok(());
    }
    if !services.handle_inbound_page_like(activity_id, liker_id, &page.id)? {
        return Ok(());
    }
    let job = services.notify_job_full(
        &page.owner_id,
        liker_id,
        crate::domain::notification::NotificationKind::PageLiked,
        None,
        None,
        None,
        Some(page.id.clone()),
        &format!("page-liked:{}:{liker_id}:{activity_id}", page.id),
    )?;
    services.jobs.enqueue_jobs(vec![job])?;
    Ok(())
}

pub(crate) fn handle_like(
    services: &AppServices,
    activity: &Value,
    activity_id: &str,
    actor_uri: &str,
) -> Result<Response, PortError> {
    let Some(target_uri) = extract_object_uri(activity.get("object")) else {
        return Ok(accepted());
    };
    let base = services.public_url.trim_end_matches('/').to_string();
    let liker = resolve_remote_account(services, actor_uri)?;
    // Page-target likes (federated clips + pages handoff, Phase 3): a
    // `Like` whose object is a page URI records a remote-attributed like
    // (counter bump commits in-txn; replays converge). Local pages match
    // by `/pages/{id}` URL; materialized remote pages match by canonical
    // URI. Unknown pages are ignored.
    if let Some(page_id) = target_uri
        .strip_prefix(&format!("{base}/pages/"))
        .map(|rest| rest.split(['/', '?', '#']).next().unwrap_or("").trim())
        .filter(|id| !id.is_empty())
    {
        if let Some(page) = services.pages.get_page(page_id)? {
            notify_page_owner_of_like(services, activity_id, &liker.id, &page)?;
        }
        return Ok(accepted());
    }
    if let Some(page) = services.pages.get_page_by_uri(&target_uri)? {
        notify_page_owner_of_like(services, activity_id, &liker.id, &page)?;
        return Ok(accepted());
    }
    let Some(target_id) = resolve_note_target_id(services, &target_uri)? else {
        return Ok(accepted());
    };
    // Server-side coercion to the note's acceptance policy happens inside.
    let reaction = extract_like_reaction(activity);
    services.handle_inbound_like(activity_id, &liker.id, &target_id, &reaction)?;
    Ok(accepted())
}

#[cfg(test)]
mod like_reaction_tests {
    use super::*;
    use serde_json::json;

    use crate::application::ports::{CreateAccountInput, ReactionStore};
    use crate::domain::reaction::FALLBACK_REACTION;

    const CAROL_URI: &str = "https://remote.example/users/carol";

    fn services() -> AppServices {
        crate::application::commands::tests::test_services("like-reaction")
    }

    fn remote_carol(services: &AppServices) -> Account {
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
                },
                None,
            )
            .expect("remote liker")
    }

    fn local_note(services: &AppServices, author_id: &str) -> (Note, String) {
        let note = services
            .create_note(CreateNoteInput::simple_public(author_id, "likeable"))
            .expect("local note");
        let base = services.public_url.trim_end_matches('/');
        let uri = format!("{base}/notes/{}", note.id);
        (note, uri)
    }

    fn stored_reaction(services: &AppServices, user_id: &str, note_id: &str) -> String {
        services
            .reactions
            .get_reaction(user_id, note_id)
            .expect("read reaction")
            .expect("reaction stored")
            .reaction
    }

    #[test]
    fn like_without_misskey_reaction_maps_to_fallback() {
        let services = services();
        let alice = services
            .create_account("alice", None)
            .expect("local author");
        let carol = remote_carol(&services);
        let (note, note_uri) = local_note(&services, &alice.id);

        // Plain Mastodon-favourite `Like`: no `_misskey_reaction`, no
        // `content` → bare-heart FALLBACK.
        let activity = json!({
            "id": "https://remote.example/likes/1",
            "type": "Like",
            "actor": CAROL_URI,
            "object": note_uri,
        });
        let resp = handle_like(&services, &activity, "like-act-1", CAROL_URI).expect("handle");
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(
            stored_reaction(&services, &carol.id, &note.id),
            FALLBACK_REACTION
        );
    }

    #[test]
    fn like_with_misskey_reaction_stores_custom_reaction() {
        let services = services();
        let alice = services
            .create_account("alice", None)
            .expect("local author");
        let carol = remote_carol(&services);
        let (note, note_uri) = local_note(&services, &alice.id);

        let activity = json!({
            "id": "https://remote.example/likes/2",
            "type": "Like",
            "actor": CAROL_URI,
            "object": note_uri,
            "content": "🎉",
            "_misskey_reaction": "🎉",
        });
        let resp = handle_like(&services, &activity, "like-act-2", CAROL_URI).expect("handle");
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(stored_reaction(&services, &carol.id, &note.id), "🎉");
    }

    #[test]
    fn emoji_react_with_unicode_content_stores_emoji() {
        let services = services();
        let alice = services
            .create_account("alice", None)
            .expect("local author");
        let carol = remote_carol(&services);
        let (note, note_uri) = local_note(&services, &alice.id);

        // Akkoma unicode-emoji shape: `EmojiReact{content}` with no tag.
        let activity = json!({
            "id": "https://akkoma.example/reactions/1",
            "type": "EmojiReact",
            "actor": CAROL_URI,
            "object": note_uri,
            "content": "🎉",
        });
        let resp = dispatch(&services, &activity, "emoji-act-1", "EmojiReact", CAROL_URI);
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(stored_reaction(&services, &carol.id, &note.id), "🎉");
    }

    #[test]
    fn emoji_react_with_tag_falls_back_to_emoji_name() {
        let services = services();
        let alice = services
            .create_account("alice", None)
            .expect("local author");
        let carol = remote_carol(&services);
        let (note, note_uri) = local_note(&services, &alice.id);

        // Custom-emoji shape with no `content`: fall back to the `Emoji`
        // tag `name`.
        let activity = json!({
            "id": "https://akkoma.example/reactions/2",
            "type": "EmojiReact",
            "actor": CAROL_URI,
            "object": note_uri,
            "tag": [{"type": "Emoji", "name": ":party:", "icon": {"type": "Image", "url": "https://akkoma.example/emoji/party.png"}}],
        });
        let resp = handle_like(&services, &activity, "emoji-act-2", CAROL_URI).expect("handle");
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(stored_reaction(&services, &carol.id, &note.id), ":party:");
    }

    #[test]
    fn undo_emoji_react_removes_the_reaction() {
        let services = services();
        let alice = services
            .create_account("alice", None)
            .expect("local author");
        let carol = remote_carol(&services);
        let (note, note_uri) = local_note(&services, &alice.id);

        let react = json!({
            "id": "https://akkoma.example/reactions/3",
            "type": "EmojiReact",
            "actor": CAROL_URI,
            "object": note_uri,
            "content": "🎉",
        });
        handle_like(&services, &react, "emoji-act-3", CAROL_URI).expect("react");
        assert_eq!(stored_reaction(&services, &carol.id, &note.id), "🎉");

        let undo = json!({
            "id": "https://akkoma.example/reactions/3/undo",
            "type": "Undo",
            "actor": CAROL_URI,
            "object": {
                "id": "https://akkoma.example/reactions/3",
                "type": "EmojiReact",
                "actor": CAROL_URI,
                "object": note_uri,
            },
        });
        let resp = handle_undo(&services, &undo, "emoji-undo-3", CAROL_URI).expect("undo");
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert!(
            services
                .reactions
                .get_reaction(&carol.id, &note.id)
                .expect("read reaction")
                .is_none()
        );
    }
}
