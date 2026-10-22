//! Outbound activity wrappers (Slice B1): `Create`/`Announce`, social
//! graph activities, likes/reactions, and note/actor `Update`s.

use serde_json::{Value, json};

use crate::activitypub::format_millis;
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::domain::account::Account;
use crate::domain::note::Note;

use super::audience_context::{PUBLIC_COLLECTION_URI, ap_context, render_note_audience};
use super::note_document::render_note_object;

/// Render a `Create(Note)` or `Announce` activity for `note` (`GET /notes/{id}/activity`
/// and outbox items).
pub fn render_create_or_announce_for_note(
    services: &AppServices,
    note: &Note,
) -> Result<Value, PortError> {
    let base = services.public_url.trim_end_matches('/');
    let author = services
        .accounts
        .find_by_id(&note.author_id)?
        .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
    let author_uri = author.actor_url(base);
    let published = format_millis(
        services
            .ids
            .parse_time(&note.id)
            .unwrap_or_else(|_| services.clock.now_ms()),
    );
    let (to, cc) = render_note_audience(services, &author, note);

    // Pure renote (no text, no cw, no files) renders as `Announce`.
    if let Some(renote_id) = &note.renote_id {
        if note.text.is_none() && note.cw.is_none() && note.file_ids.is_empty() {
            let target = services
                .notes
                .get_note(renote_id)?
                .ok_or_else(|| PortError::NotFound("NO_SUCH_RENOTE_TARGET".to_string()))?;
            let target_uri = target
                .uri
                .unwrap_or_else(|| format!("{base}/notes/{}", target.id));
            return Ok(json!({
                "@context": ap_context(),
                "id": format!("{base}/notes/{}/activity", note.id),
                "type": "Announce",
                "actor": author_uri,
                "published": published,
                "to": to,
                "cc": cc,
                "object": target_uri,
            }));
        }
    }

    let note_obj = render_note_object(services, note, false)?;
    Ok(json!({
        "@context": ap_context(),
        "id": format!("{base}/notes/{}/activity", note.id),
        "type": "Create",
        "actor": note_obj["attributedTo"].clone(),
        "published": note_obj["published"].clone(),
        "to": note_obj["to"].clone(),
        "cc": note_obj["cc"].clone(),
        "object": note_obj,
    }))
}

/// Render a `Block` activity (§0A; Misskey `ApRendererService.renderBlock`
/// parity: `{url}/blocks/{id}`, actor = blocker, object = blockee URI).
pub fn render_block_activity(
    public_url: &str,
    block_id: &str,
    blocker_uri: &str,
    blockee_uri: &str,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/blocks/{block_id}"),
        "type": "Block",
        "actor": blocker_uri,
        "object": blockee_uri,
    })
}

/// Render a `Flag` activity (Batch M WP4; upstream
/// `ApRendererService.renderFlag`: type Flag, actor = reporting (system)
/// account, content = report comment, object = reported account URI).
pub fn render_flag_activity(
    public_url: &str,
    flag_id: &str,
    actor_uri: &str,
    object_uri: &str,
    content: &str,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/flags/{flag_id}"),
        "type": "Flag",
        "actor": actor_uri,
        "content": content,
        "object": object_uri,
    })
}

/// Render a `Follow` activity.
pub fn render_follow_activity(
    public_url: &str,
    follow_id: &str,
    follower_uri: &str,
    followee_uri: &str,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/follows/{follow_id}"),
        "type": "Follow",
        "actor": follower_uri,
        "object": followee_uri,
    })
}

/// Render an `Accept(Follow)` activity.
pub fn render_accept_follow_activity(
    public_url: &str,
    accept_id: &str,
    actor_uri: &str,
    follow_activity: &Value,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{accept_id}"),
        "type": "Accept",
        "actor": actor_uri,
        "object": follow_activity,
    })
}

/// Render a `Reject(Follow)` activity.
pub fn render_reject_follow_activity(
    public_url: &str,
    reject_id: &str,
    actor_uri: &str,
    follow_activity: &Value,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{reject_id}"),
        "type": "Reject",
        "actor": actor_uri,
        "object": follow_activity,
    })
}

/// Render an `Undo` activity wrapping an inner activity (`Follow`, `Like`, `Announce`).
pub fn render_undo_activity(
    public_url: &str,
    undo_id: &str,
    actor_uri: &str,
    inner_activity: &Value,
    to: Option<Vec<String>>,
    cc: Option<Vec<String>>,
) -> Value {
    let base = public_url.trim_end_matches('/');
    let mut doc = json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{undo_id}"),
        "type": "Undo",
        "actor": actor_uri,
        "object": inner_activity,
    });
    if let Some(to) = to {
        doc["to"] = json!(to);
    }
    if let Some(cc) = cc {
        doc["cc"] = json!(cc);
    }
    doc
}

/// Render a `Move` activity (Batch P `i/move`; Misskey
/// `ApRendererService.renderMove` parity: `id =
/// {url}/moves/{srcId}/{dstId}`, `actor` = old account URI, `object` =
/// old account URI, `target` = new account URI).
pub fn render_move_activity(
    public_url: &str,
    src_account_id: &str,
    src_actor_uri: &str,
    dst_actor_uri: &str,
    dst_account_id: &str,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/moves/{src_account_id}/{dst_account_id}"),
        "type": "Move",
        "actor": src_actor_uri,
        "object": src_actor_uri,
        "target": dst_actor_uri,
    })
}

/// Render an `Add`/`Remove` of a note to/from the actor's featured
/// collection (pinning §7; Misskey `NotePiningService.deliverPinnedChange`
/// `renderAdd`/`renderRemove` parity: `target =
/// {actor}/collections/featured`, `object` = the note URI).
pub fn render_featured_change_activity(
    public_url: &str,
    change_id: &str,
    actor_uri: &str,
    note_uri: &str,
    added: bool,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{change_id}"),
        "type": if added { "Add" } else { "Remove" },
        "actor": actor_uri,
        "target": format!("{actor_uri}/collections/featured"),
        "object": note_uri,
    })
}

/// Render an `EmojiReact` activity for Akkoma/Pleroma-family recipients
/// (WP5 §5): `type: EmojiReact`, `content` always set to the reaction, plus
/// a `tag: [{type: Emoji, name}]` entry for custom `:shortcode:` reactions
/// (Akkoma renders custom emoji from the tag; unicode reactions carry no
/// tag — a fabricated tag URL would be unverifiable, so only the portable
/// `name` is emitted). Inbound compatibility is symmetric: see
/// [`super::note_parser::extract_like_reaction`] (prefers `content`, then the
/// `Emoji` tag).
pub fn render_emoji_react_activity(
    public_url: &str,
    reaction_id: &str,
    actor_uri: &str,
    target_note_uri: &str,
    reaction: &str,
) -> Value {
    let base = public_url.trim_end_matches('/');
    let mut activity = json!({
        "@context": ap_context(),
        "id": format!("{base}/likes/{reaction_id}"),
        "type": "EmojiReact",
        "actor": actor_uri,
        "object": target_note_uri,
        "content": reaction,
    });
    if crate::domain::fed_policy::is_custom_emoji_reaction(reaction) {
        activity["tag"] = json!([{
            "type": "Emoji",
            "name": reaction,
        }]);
    }
    activity
}

/// Render a `Like` activity, choosing the emoji-reaction form or the plain
/// Mastodon-favourite form per recipient capability (§6
/// `docs/misskey-parity-plan.md`): emoji-aware servers get `Like` +
/// `_misskey_reaction`, others get a plain `Like` with neither `content` nor
/// `_misskey_reaction`. Both variants share [`ap_context`]: its
/// `_misskey_reaction` entry is an inert namespace declaration (and upstream
/// Misskey always sends the full context too), so the distinguishing feature
/// is the absence of the `_misskey_reaction` field.
pub fn render_like_activity_for(
    public_url: &str,
    reaction_id: &str,
    actor_uri: &str,
    target_note_uri: &str,
    reaction: &str,
    emoji_capable: bool,
) -> Value {
    let base = public_url.trim_end_matches('/');
    if emoji_capable {
        json!({
            "@context": ap_context(),
            "id": format!("{base}/likes/{reaction_id}"),
            "type": "Like",
            "actor": actor_uri,
            "object": target_note_uri,
            "content": reaction,
            "_misskey_reaction": reaction,
        })
    } else {
        json!({
            "@context": ap_context(),
            "id": format!("{base}/likes/{reaction_id}"),
            "type": "Like",
            "actor": actor_uri,
            "object": target_note_uri,
        })
    }
}

/// Render a `Like` activity with Misskey `_misskey_reaction` (the
/// emoji-aware form; see [`render_like_activity_for`]).
pub fn render_like_activity(
    public_url: &str,
    reaction_id: &str,
    actor_uri: &str,
    target_note_uri: &str,
    reaction: &str,
) -> Value {
    render_like_activity_for(
        public_url,
        reaction_id,
        actor_uri,
        target_note_uri,
        reaction,
        true,
    )
}

/// Render an `Update(Note)` activity for an edited local note (§8;
/// Sharkey `postNoteEdited` AP-deliver parity: the full re-rendered note
/// object wrapped in `Update`, addressed like the create path).
pub fn render_note_update_activity(
    public_url: &str,
    update_id: &str,
    author_actor_uri: &str,
    note: Value,
    to: Vec<String>,
    cc: Vec<String>,
    now_ms: i64,
) -> Value {
    let base = public_url.trim_end_matches('/');
    json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{update_id}"),
        "actor": author_actor_uri,
        "type": "Update",
        "to": to,
        "cc": cc,
        "object": note,
        "published": format_millis(now_ms),
    })
}

/// Render an `Update(Person)` activity when a local account updates its profile.
pub fn render_update_actor_activity(
    services: &AppServices,
    update_id: &str,
    account: &Account,
) -> Result<Value, PortError> {
    let base = services.public_url.trim_end_matches('/');
    let actor_doc = crate::activitypub::actor::render_actor_document(services, account)?;
    let actor_uri = account.actor_url(base);
    Ok(json!({
        "@context": ap_context(),
        "id": format!("{base}/activities/{update_id}"),
        "type": "Update",
        "actor": actor_uri,
        "to": [PUBLIC_COLLECTION_URI],
        "object": actor_doc,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
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
            "pinetree-ap-act-{}-{prefix}-{}",
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

    #[test]
    fn like_render_splits_emoji_aware_and_plain() {
        let emoji = render_like_activity_for(
            "https://pinetree.local",
            "r1",
            "https://pinetree.local/users/alice",
            "https://remote.example/notes/1",
            "🎉",
            true,
        );
        assert_eq!(emoji["type"], "Like");
        assert_eq!(emoji["_misskey_reaction"], "🎉");
        assert_eq!(emoji["content"], "🎉");

        let plain = render_like_activity_for(
            "https://pinetree.local",
            "r1",
            "https://pinetree.local/users/alice",
            "https://remote.example/notes/1",
            "🎉",
            false,
        );
        assert_eq!(plain["type"], "Like");
        assert_eq!(plain["actor"], "https://pinetree.local/users/alice");
        assert_eq!(plain["object"], "https://remote.example/notes/1");
        assert!(
            plain.get("_misskey_reaction").is_none(),
            "plain Like carries no _misskey_reaction"
        );
        assert!(
            plain.get("content").is_none(),
            "plain Like carries no content"
        );

        // The legacy entry point keeps rendering the emoji-aware form.
        let legacy = render_like_activity(
            "https://pinetree.local",
            "r1",
            "https://pinetree.local/users/alice",
            "https://remote.example/notes/1",
            "🎉",
        );
        assert_eq!(legacy, emoji);
    }

    #[test]
    fn featured_change_renders_add_remove_to_featured_collection() {
        let actor = "https://pinetree.local/users/alice";
        let note = "https://pinetree.local/notes/n1";
        let add =
            render_featured_change_activity("https://pinetree.local/", "c1", actor, note, true);
        assert_eq!(add["type"], "Add");
        assert_eq!(add["actor"], actor);
        assert_eq!(add["target"], format!("{actor}/collections/featured"));
        assert_eq!(add["object"], note);
        assert_eq!(add["id"], "https://pinetree.local/activities/c1");
        let remove =
            render_featured_change_activity("https://pinetree.local", "c2", actor, note, false);
        assert_eq!(remove["type"], "Remove");
        assert_eq!(remove["target"], format!("{actor}/collections/featured"));
        assert_eq!(remove["object"], note);
    }

    #[test]
    fn pure_renote_still_renders_announce_without_quote_fields() {
        let services = test_services("announce");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let target = services
            .create_note(CreateNoteInput::simple_public(&alice.id, "original"))
            .unwrap();
        let mut input = CreateNoteInput::simple_public(&bob.id, "x");
        input.text = None;
        input.renote_of = Some(target.id.clone());
        let renote = services.create_note(input).unwrap();
        assert!(!renote.is_quote());
        let activity = render_create_or_announce_for_note(&services, &renote).unwrap();
        assert_eq!(activity["type"], "Announce");
        assert!(activity.get("quote").is_none());
    }
}
