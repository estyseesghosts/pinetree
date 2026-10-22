//! ActivityPub `@context`, audience (`to`/`cc` ↔ `Visibility`) mapping,
//! and content-type helpers (Phase 2 Slices B1 & B2).
//!
//! Never emits deprecated `_misskey_content` (uses `source: { content,
//! mediaType: "text/x.misskeymarkdown" }` instead). Reactions use
//! `_misskey_reaction` alongside `content` per proposal §7.

use axum::http::{HeaderMap, header};
use serde_json::{Value, json};

use crate::application::commands::AppServices;
use crate::domain::account::Account;
use crate::domain::note::{Note, Visibility};

/// Canonical ActivityPub content-type header value.
pub const AP_CONTENT_TYPE: &str = "application/activity+json; charset=utf-8";

/// Public audience URI constants recognized by ActivityPub peers.
pub const PUBLIC_COLLECTION_URI: &str = "https://www.w3.org/ns/activitystreams#Public";
pub const PUBLIC_COLLECTION_COMPACT_1: &str = "as:Public";
pub const PUBLIC_COLLECTION_COMPACT_2: &str = "Public";

/// Check whether an incoming `Accept` header requests ActivityPub JSON-LD
/// (or is absent / `*/*`). Returns `false` only when the client explicitly
/// asks for `text/html` without accepting ActivityPub or JSON.
pub fn is_ap_accept(headers: &HeaderMap) -> bool {
    let Some(raw) = headers.get(header::ACCEPT).and_then(|v| v.to_str().ok()) else {
        return true;
    };
    let lower = raw.to_ascii_lowercase();
    if lower.contains("application/activity+json")
        || lower.contains("application/ld+json")
        || lower.contains("application/json")
    {
        return true;
    }
    if lower.contains("text/html") || lower.contains("application/xhtml+xml") {
        return false;
    }
    true
}

/// Canonical `@context` array matching Misskey/Sharkey (`context.ts`),
/// omitting deprecated `_misskey_content`, plus FEP-044f quote terms
/// (canonical `quote`/`quoteAuthorization`, Fedibird `quoteUri`, GoToSocial
/// interaction-policy terms) and the `quoteUrl` convention.
pub fn ap_context() -> Value {
    let mut ctx = json!([
        "https://www.w3.org/ns/activitystreams",
        "https://w3id.org/security/v1",
        {
            "manuallyApprovesFollowers": "as:manuallyApprovesFollowers",
            "sensitive": "as:sensitive",
            "Hashtag": "as:Hashtag",
            "quoteUrl": "as:quoteUrl",
            "toot": "http://joinmastodon.org/ns#",
            "Emoji": "toot:Emoji",
            "featured": "toot:featured",
            "discoverable": "toot:discoverable",
            "indexable": "toot:indexable",
            "schema": "http://schema.org#",
            "PropertyValue": "schema:PropertyValue",
            "value": "schema:value",
            "misskey": "https://misskey-hub.net/ns#",
            "_misskey_summary": "misskey:_misskey_summary",
            "_misskey_quote": "misskey:_misskey_quote",
            "_misskey_Reaction": "misskey:_misskey_Reaction",
            "_misskey_reaction": "misskey:_misskey_reaction",
            "_misskey_talk": "misskey:_misskey_talk",
            "_misskey_requireSigninToViewContents": "misskey:_misskey_requireSigninToViewContents",
            "_misskey_makeNotesHiddenBefore": "misskey:_misskey_makeNotesHiddenBefore",
            "_misskey_license": "misskey:_misskey_license",
            "isCat": "misskey:isCat",
            "vcard": "http://www.w3.org/2006/vcard/ns#"
        }
    ]);
    // Merge the FEP-044f / compatibility quote terms into the trailing
    // context object (never the deprecated `_misskey_content`).
    if let Some(entries) = ctx
        .as_array_mut()
        .and_then(|arr| arr.last_mut())
        .and_then(|v| v.as_object_mut())
    {
        for (k, v) in super::super::quotes::quote_context_entries()
            .as_object()
            .cloned()
            .unwrap_or_default()
        {
            entries.insert(k, v);
        }
    }
    ctx
}

/// Return true if `uri` is one of the ActivityStreams Public collection URIs.
pub fn is_public_collection_uri(uri: &str) -> bool {
    matches!(
        uri.trim(),
        PUBLIC_COLLECTION_URI | PUBLIC_COLLECTION_COMPACT_1 | PUBLIC_COLLECTION_COMPACT_2
    )
}

/// Extract a list of URI strings from an ActivityPub `to` or `cc` JSON value
/// (accepting a single string, an object with `"id"`, or an array).
pub fn extract_uri_list(val: Option<&Value>) -> Vec<String> {
    let mut out = Vec::new();
    let Some(val) = val else {
        return out;
    };
    let mut push_one = |item: &Value| match item {
        Value::String(s) if !s.trim().is_empty() => out.push(s.trim().to_string()),
        Value::Object(m) => {
            if let Some(id) = m.get("id").and_then(Value::as_str) {
                if !id.trim().is_empty() {
                    out.push(id.trim().to_string());
                }
            }
        }
        _ => {}
    };
    match val {
        Value::Array(arr) => {
            for item in arr {
                push_one(item);
            }
        }
        other => push_one(other),
    }
    out
}

/// Map a local note's `(visibility, visible_user_ids, mentions)` onto
/// ActivityPub `(to, cc)` arrays matching `ApAudienceService.renderNoteAudience`.
pub fn render_note_audience(
    services: &AppServices,
    author: &Account,
    note: &Note,
) -> (Vec<String>, Vec<String>) {
    let base = services.public_url.trim_end_matches('/');
    let followers_collection = format!("{}/followers", author.actor_url(base));

    let mut mention_uris = Vec::new();
    for uid in &note.mentions {
        if let Ok(Some(acc)) = services.accounts.find_by_id(uid) {
            let uri = acc.actor_url(base);
            if !uri.is_empty() && !mention_uris.contains(&uri) {
                mention_uris.push(uri);
            }
        }
    }

    match note.visibility {
        Visibility::Public => {
            let to = vec![PUBLIC_COLLECTION_URI.to_string()];
            let mut cc = vec![followers_collection];
            for u in mention_uris {
                if !cc.contains(&u) {
                    cc.push(u);
                }
            }
            (to, cc)
        }
        Visibility::Home => {
            let to = vec![followers_collection];
            let mut cc = vec![PUBLIC_COLLECTION_URI.to_string()];
            for u in mention_uris {
                if !cc.contains(&u) {
                    cc.push(u);
                }
            }
            (to, cc)
        }
        Visibility::Followers => {
            let to = vec![followers_collection];
            let cc = mention_uris;
            (to, cc)
        }
        Visibility::Specified => {
            let mut to = Vec::new();
            for uid in &note.visible_user_ids {
                if let Ok(Some(acc)) = services.accounts.find_by_id(uid) {
                    let uri = acc.actor_url(base);
                    if !uri.is_empty() && !to.contains(&uri) {
                        to.push(uri);
                    }
                }
            }
            for u in mention_uris {
                if !to.contains(&u) {
                    to.push(u);
                }
            }
            (to, Vec::new())
        }
    }
}

/// Map incoming ActivityPub `(to, cc)` lists and the author's `followers`
/// collection URI onto `(Visibility, specified_actor_uris)` per
/// `ApAudienceService.resolveNoteAudience`.
pub fn parse_note_audience(
    to: &[String],
    cc: &[String],
    author_followers_uri: Option<&str>,
) -> (Visibility, Vec<String>) {
    let to_public = to.iter().any(|u| is_public_collection_uri(u));
    let cc_public = cc.iter().any(|u| is_public_collection_uri(u));

    let is_followers_uri = |u: &str| -> bool {
        if let Some(expected) = author_followers_uri {
            if u == expected {
                return true;
            }
        }
        u.ends_with("/followers")
    };

    let has_followers = to.iter().chain(cc.iter()).any(|u| is_followers_uri(u));

    let visibility = if to_public {
        Visibility::Public
    } else if cc_public {
        Visibility::Home
    } else if has_followers {
        Visibility::Followers
    } else {
        Visibility::Specified
    };

    let mut recipient_uris = Vec::new();
    for u in to.iter().chain(cc.iter()) {
        if !is_public_collection_uri(u) && !is_followers_uri(u) && !recipient_uris.contains(u) {
            recipient_uris.push(u.clone());
        }
    }

    (visibility, recipient_uris)
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
            "pinetree-ap-ctx-{}-{prefix}-{}",
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
    fn context_never_advertises_deprecated_misskey_content() {
        let ctx = serde_json::to_string(&ap_context()).unwrap();
        assert!(!ctx.contains("_misskey_content"));
        assert!(ctx.contains("_misskey_reaction"));
    }

    #[test]
    fn audience_mapping_roundtrips_all_visibilities() {
        let services = test_services("aud");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let followers_uri = format!("https://pinetree.local/users/{}/followers", alice.id);
        let bob_uri = format!("https://pinetree.local/users/{}", bob.id);

        for (vis, visible_users) in [
            (Visibility::Public, vec![]),
            (Visibility::Home, vec![]),
            (Visibility::Followers, vec![]),
            (Visibility::Specified, vec![bob.id.clone()]),
        ] {
            let mut input = CreateNoteInput::simple_public(&alice.id, "hello @bob #rust");
            input.visibility = vis;
            input.visible_users = visible_users;
            let note = services.create_note(input).unwrap();
            let (to, cc) = render_note_audience(&services, &alice, &note);
            let (parsed_vis, recipients) = parse_note_audience(&to, &cc, Some(&followers_uri));
            assert_eq!(parsed_vis, vis, "visibility roundtrip for {vis:?}");
            assert!(recipients.contains(&bob_uri));
        }
    }
}
