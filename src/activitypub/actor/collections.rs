//! Actor-scoped ActivityPub collections: `outbox`, `followers`,
//! `following`, and `featured` (`OrderedCollection` indexes + pages,
//! Slice 11 paging contract in `docs/activitypub-collections.md`).

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::{Value, json};

use crate::activitypub::objects::ap_context;
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::domain::account::FollowVisibility;

use super::actor_document::{ap_json_response, find_visible_local_user};

/// Items per page for the outbox, followers and following (Slice 11;
/// `docs/activitypub-collections.md` §3).
pub(crate) const COLLECTION_PAGE_SIZE: usize = 40;

/// Encodes an internal position as an opaque cursor: URL-safe base64, no padding.
fn encode_collection_cursor(position: &str) -> String {
    URL_SAFE_NO_PAD.encode(position.as_bytes())
}

/// Decodes a `cursor` query value to its internal position. Anything that is
/// not URL-safe unpadded base64 of non-empty UTF-8 is a 400.
fn decode_collection_cursor(raw: &str) -> Result<String, StatusCode> {
    let bytes = URL_SAFE_NO_PAD
        .decode(raw)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let position = String::from_utf8(bytes).map_err(|_| StatusCode::BAD_REQUEST)?;
    if position.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(position)
}

/// Reads the optional `cursor` query value. `Ok(None)` when absent.
pub(crate) fn collection_cursor(
    params: &HashMap<String, String>,
) -> Result<Option<String>, StatusCode> {
    params
        .get("cursor")
        .map(|raw| decode_collection_cursor(raw))
        .transpose()
}

/// `?page=true` selects a page; anything else is the index.
pub(crate) fn wants_collection_page(params: &HashMap<String, String>) -> bool {
    params.get("page").map(|v| v == "true").unwrap_or(false)
}

/// URL of one page. `cursor` is the internal position the page starts after.
fn collection_page_url(coll_url: &str, cursor: Option<&str>) -> String {
    match cursor {
        None => format!("{coll_url}?page=true"),
        Some(position) => format!(
            "{coll_url}?page=true&cursor={}",
            encode_collection_cursor(position)
        ),
    }
}

/// `OrderedCollection` index. `total_items` is `None` when the count is not
/// cheaply accurate (outbox); the field is then omitted.
pub(crate) fn ordered_collection_index(coll_url: &str, total_items: Option<u64>) -> Response {
    let mut collection = json!({
        "@context": ap_context(),
        "id": coll_url,
        "type": "OrderedCollection",
        "first": collection_page_url(coll_url, None),
    });
    if let Some(total) = total_items {
        collection["totalItems"] = json!(total);
    }
    ap_json_response(StatusCode::OK, &collection, None)
}

/// `OrderedCollectionPage`. `next_position` is `Some` only when more items exist.
pub(crate) fn ordered_collection_page(
    coll_url: &str,
    cursor: Option<&str>,
    next_position: Option<&str>,
    total_items: Option<u64>,
    items: Vec<Value>,
) -> Response {
    let mut page = json!({
        "@context": ap_context(),
        "id": collection_page_url(coll_url, cursor),
        "type": "OrderedCollectionPage",
        "partOf": coll_url,
        "orderedItems": items,
    });
    if let Some(total) = total_items {
        page["totalItems"] = json!(total);
    }
    if let Some(position) = next_position {
        page["next"] = Value::String(collection_page_url(coll_url, Some(position)));
    }
    ap_json_response(StatusCode::OK, &page, None)
}

/// `403` for a followers/following collection whose owner restricted its
/// visibility (`ActivityPubServerService` ff-visibility parity: any
/// non-`public` visibility denies every requester, with no per-requester
/// exception). Carries upstream's `Cache-Control: public, max-age=30`.
fn forbidden_collection() -> Response {
    let mut resp = StatusCode::FORBIDDEN.into_response();
    resp.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=30"),
    );
    resp
}
/// `GET /users/{id}/followers` — ActivityPub `OrderedCollection` of follower actor URIs.
/// Paging contract: `docs/activitypub-collections.md`.
pub async fn get_actor_followers(
    State(services): State<Arc<AppServices>>,
    Path(id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let account = match find_visible_local_user(&services, &id) {
        Ok(a) => a,
        Err(code) => return code.into_response(),
    };
    if account.followers_visibility != FollowVisibility::Public {
        return forbidden_collection();
    }
    let base = services.public_url.trim_end_matches('/');
    let coll_url = format!("{base}/users/{}/followers", account.id);
    if !wants_collection_page(&params) {
        return ordered_collection_index(&coll_url, Some(account.followers_count));
    }
    let cursor = match collection_cursor(&params) {
        Ok(c) => c,
        Err(code) => return code.into_response(),
    };
    // One extra row tells us whether a `next` page exists.
    let fetched = match services.follows.followers_of(
        &account.id,
        cursor.as_deref(),
        COLLECTION_PAGE_SIZE + 1,
    ) {
        Ok(rows) => rows,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let has_more = fetched.len() > COLLECTION_PAGE_SIZE;
    let rows = &fetched[..fetched.len().min(COLLECTION_PAGE_SIZE)];
    let mut uris = Vec::with_capacity(rows.len());
    for row in rows {
        match services.accounts.find_by_id(&row.follower_id) {
            Ok(Some(acc)) => uris.push(Value::String(acc.actor_url(base))),
            Ok(None) => eprintln!(
                "followers: account {} follows {} but has no account row; skipped",
                row.follower_id, account.id
            ),
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }
    let next = if has_more {
        rows.last().map(|r| r.follower_id.clone())
    } else {
        None
    };
    ordered_collection_page(
        &coll_url,
        cursor.as_deref(),
        next.as_deref(),
        Some(account.followers_count),
        uris,
    )
}

/// `GET /users/{id}/following` — ActivityPub `OrderedCollection` of followee actor URIs.
/// Paging contract: `docs/activitypub-collections.md`.
pub async fn get_actor_following(
    State(services): State<Arc<AppServices>>,
    Path(id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let account = match find_visible_local_user(&services, &id) {
        Ok(a) => a,
        Err(code) => return code.into_response(),
    };
    if account.following_visibility != FollowVisibility::Public {
        return forbidden_collection();
    }
    let base = services.public_url.trim_end_matches('/');
    let coll_url = format!("{base}/users/{}/following", account.id);
    if !wants_collection_page(&params) {
        return ordered_collection_index(&coll_url, Some(account.following_count));
    }
    let cursor = match collection_cursor(&params) {
        Ok(c) => c,
        Err(code) => return code.into_response(),
    };
    // One extra row tells us whether a `next` page exists.
    let fetched = match services.follows.following_of(
        &account.id,
        cursor.as_deref(),
        COLLECTION_PAGE_SIZE + 1,
    ) {
        Ok(rows) => rows,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let has_more = fetched.len() > COLLECTION_PAGE_SIZE;
    let rows = &fetched[..fetched.len().min(COLLECTION_PAGE_SIZE)];
    let mut uris = Vec::with_capacity(rows.len());
    for row in rows {
        match services.accounts.find_by_id(&row.followee_id) {
            Ok(Some(acc)) => uris.push(Value::String(acc.actor_url(base))),
            Ok(None) => eprintln!(
                "following: account {} follows {} but has no account row; skipped",
                account.id, row.followee_id
            ),
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }
    let next = if has_more {
        rows.last().map(|r| r.followee_id.clone())
    } else {
        None
    };
    ordered_collection_page(
        &coll_url,
        cursor.as_deref(),
        next.as_deref(),
        Some(account.following_count),
        uris,
    )
}

/// `GET /users/{id}/collections/featured` — ActivityPub featured notes `OrderedCollection`.
pub async fn get_actor_featured(
    State(services): State<Arc<AppServices>>,
    Path(id): Path<String>,
) -> Response {
    let account = match find_visible_local_user(&services, &id) {
        Ok(a) => a,
        Err(code) => return code.into_response(),
    };
    let base = services.public_url.trim_end_matches('/');
    let coll_url = format!("{base}/users/{}/collections/featured", account.id);
    let collection = json!({
        "@context": ap_context(),
        "id": coll_url,
        "type": "OrderedCollection",
        "totalItems": 0,
        "orderedItems": [],
    });
    ap_json_response(StatusCode::OK, &collection, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activitypub::actor::outbox::get_actor_outbox;
    use std::sync::atomic::{AtomicU64, Ordering};

    use serde_json::json;

    use crate::application::commands::AppServices;
    use crate::application::ports::{
        AccountStore, ActivityDedup, BlobStore, Clock, CreateAccountInput, CreateNoteInput,
        FollowStore, IdGenerator, NewJob, NoopStreamingSink, NoteStore, UpdateAccountPatch,
    };
    use crate::domain::account::{Account, FollowVisibility};
    use crate::domain::follow::{Follow, FollowNotify};
    use crate::domain::id::{AidxGenerator, ManualClock};
    use crate::domain::note::Visibility;
    use crate::federation::delivery::{SafeHttpClient, SafeHttpConfig};
    use crate::media::storage::FsBlobStore;
    use crate::persistence::redb::RedbStore;
    use std::collections::HashSet;

    fn test_services(prefix: &str) -> AppServices {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-inbox-url-{}-{prefix}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_760_400_000_000));
        let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
        let store = RedbStore::open(dir.join("test.redb"), clock.clone(), ids.clone()).unwrap();
        let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.join("blobs")).unwrap());
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

    // ---- Slice 11 (M7): AP collection paging. Tests call the real handlers
    // and read the JSON body back. Contract: docs/activitypub-collections.md.

    const SLICE11_BASE: &str = "https://pinetree.local";

    /// Calls one of the three paged collection handlers and returns the
    /// status plus the parsed JSON body (`Value::Null` when there is none).
    async fn slice11_get(
        services: &Arc<AppServices>,
        collection: &str,
        account_id: &str,
        params: &[(&str, &str)],
    ) -> (StatusCode, Value) {
        let query: HashMap<String, String> = params
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let state = State(services.clone());
        let path = Path(account_id.to_string());
        let query = Query(query);
        let response = match collection {
            "outbox" => get_actor_outbox(state, path, query).await,
            "followers" => get_actor_followers(state, path, query).await,
            "following" => get_actor_following(state, path, query).await,
            other => panic!("unknown collection {other}"),
        };
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(Value::Null)
        };
        (status, body)
    }

    /// Starts at `?page=true` and follows `next` links until none is returned.
    /// Asserts every `next` has the documented `{coll}?page=true&cursor=` form.
    async fn slice11_walk(
        services: &Arc<AppServices>,
        collection: &str,
        account_id: &str,
    ) -> Vec<Value> {
        let coll_url = format!("{SLICE11_BASE}/users/{account_id}/{collection}");
        let prefix = format!("{coll_url}?page=true&cursor=");
        let (status, first) =
            slice11_get(services, collection, account_id, &[("page", "true")]).await;
        assert_eq!(status, StatusCode::OK, "first page of {collection}");
        let mut pages = Vec::new();
        let mut current = first;
        loop {
            assert!(
                pages.len() < 200,
                "paging of {collection} did not terminate"
            );
            let next = current
                .get("next")
                .and_then(Value::as_str)
                .map(str::to_string);
            pages.push(current);
            let Some(next) = next else { break };
            assert!(
                next.starts_with(&prefix),
                "next has unexpected form: {next}"
            );
            let cursor = next[prefix.len()..].to_string();
            assert!(!cursor.is_empty(), "next cursor must not be empty");
            let (status, page) = slice11_get(
                services,
                collection,
                account_id,
                &[("page", "true"), ("cursor", cursor.as_str())],
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{collection} page with cursor");
            current = page;
        }
        pages
    }

    fn slice11_local(services: &AppServices, handle: &str) -> Account {
        services
            .accounts
            .create_account(CreateAccountInput::local(handle), None)
            .expect("create local account")
    }

    /// Storage double: delegates everything to the real store, but the two
    /// collection reads fail with a storage error (Slice 11 500 test).
    struct Slice11FailingFollows {
        inner: Arc<dyn FollowStore>,
    }

    impl FollowStore for Slice11FailingFollows {
        fn follow(
            &self,
            follower_id: &str,
            followee_id: &str,
            with_replies: bool,
            notify: Option<FollowNotify>,
            jobs: Vec<NewJob>,
        ) -> Result<Follow, PortError> {
            self.inner
                .follow(follower_id, followee_id, with_replies, notify, jobs)
        }
        fn unfollow(
            &self,
            follower_id: &str,
            followee_id: &str,
            jobs: Vec<NewJob>,
        ) -> Result<(), PortError> {
            self.inner.unfollow(follower_id, followee_id, jobs)
        }
        fn is_following(&self, follower_id: &str, followee_id: &str) -> Result<bool, PortError> {
            self.inner.is_following(follower_id, followee_id)
        }
        fn get_follow(
            &self,
            follower_id: &str,
            followee_id: &str,
        ) -> Result<Option<Follow>, PortError> {
            self.inner.get_follow(follower_id, followee_id)
        }
        fn followers_of(
            &self,
            _account_id: &str,
            _cursor: Option<&str>,
            _limit: usize,
        ) -> Result<Vec<Follow>, PortError> {
            Err(PortError::StorageUnavailable(
                "injected follow-store failure".to_string(),
            ))
        }
        fn following_of(
            &self,
            _account_id: &str,
            _cursor: Option<&str>,
            _limit: usize,
        ) -> Result<Vec<Follow>, PortError> {
            Err(PortError::StorageUnavailable(
                "injected follow-store failure".to_string(),
            ))
        }
    }

    // (a) 250 followers page through completely: no duplicates, none missing,
    // last page has no `next`, and the index totalItems equals the counter.
    #[tokio::test]
    async fn slice11_followers_250_page_through_completely() {
        let services = Arc::new(test_services("s11-followers"));
        let target = slice11_local(&services, "target");
        let mut expected: Vec<String> = Vec::new();
        for i in 0..250 {
            let fan = slice11_local(&services, &format!("fan{i:03}"));
            services
                .follows
                .follow(&fan.id, &target.id, false, None, Vec::new())
                .expect("follow");
            expected.push(fan.actor_url(SLICE11_BASE));
        }
        let counter = services
            .accounts
            .find_by_id(&target.id)
            .expect("lookup")
            .expect("target exists")
            .followers_count;
        assert_eq!(counter, 250);

        let coll_url = format!("{SLICE11_BASE}/users/{}/followers", target.id);
        let (status, index) = slice11_get(&services, "followers", &target.id, &[]).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(index["type"].as_str(), Some("OrderedCollection"));
        assert_eq!(index["id"].as_str(), Some(coll_url.as_str()));
        assert_eq!(index["totalItems"].as_u64(), Some(counter));
        assert_eq!(
            index["first"].as_str(),
            Some(format!("{coll_url}?page=true").as_str())
        );
        assert!(index.get("orderedItems").is_none());
        assert!(index.get("next").is_none());

        let pages = slice11_walk(&services, "followers", &target.id).await;
        assert_eq!(pages.len(), 7, "250 items at 40 per page is 7 pages");
        let mut seen: Vec<String> = Vec::new();
        for (i, page) in pages.iter().enumerate() {
            assert_eq!(page["type"].as_str(), Some("OrderedCollectionPage"));
            assert_eq!(page["partOf"].as_str(), Some(coll_url.as_str()));
            assert_eq!(page["totalItems"].as_u64(), Some(counter));
            let items = page["orderedItems"].as_array().expect("orderedItems array");
            if i + 1 < pages.len() {
                assert_eq!(items.len(), 40, "full page {i}");
                assert!(page.get("next").is_some(), "page {i} must carry next");
            } else {
                assert_eq!(items.len(), 10, "last page holds the remainder");
                assert!(page.get("next").is_none(), "last page must not carry next");
            }
            assert!(page.get("prev").is_none(), "prev is never emitted");
            for item in items {
                seen.push(item.as_str().expect("actor uri string").to_string());
            }
        }
        let unique: HashSet<&String> = seen.iter().collect();
        assert_eq!(seen.len(), 250, "no duplicates across pages");
        assert_eq!(unique.len(), 250, "no duplicates across pages");
        let mut expected_sorted = expected.clone();
        expected_sorted.sort();
        let mut seen_sorted = seen.clone();
        seen_sorted.sort();
        assert_eq!(seen_sorted, expected_sorted, "none missing across pages");
    }

    // (b) A storage error in the follow store returns 500, not an empty
    // collection.
    #[tokio::test]
    async fn slice11_follow_store_failure_returns_500() {
        let mut services = test_services("s11-500");
        let target = slice11_local(&services, "victim");
        let fan = slice11_local(&services, "fan");
        services
            .follows
            .follow(&fan.id, &target.id, false, None, Vec::new())
            .expect("follow");
        let failing: Arc<dyn FollowStore> = Arc::new(Slice11FailingFollows {
            inner: services.follows.clone(),
        });
        services.follows = failing;
        let services = Arc::new(services);

        for collection in ["followers", "following"] {
            let (status, _) =
                slice11_get(&services, collection, &target.id, &[("page", "true")]).await;
            assert_eq!(
                status,
                StatusCode::INTERNAL_SERVER_ERROR,
                "{collection} page must surface the storage error"
            );
        }
    }

    // (b2) §§1–2 parity: any non-`public` followers/following visibility
    // denies the ActivityPub collections (index and page alike) with 403 —
    // no per-requester exception.
    #[tokio::test]
    async fn ff_visibility_non_public_collections_are_403() {
        let services = Arc::new(test_services("s11-ffvis"));
        let target = slice11_local(&services, "shy");
        let fan = slice11_local(&services, "fan");
        services
            .follows
            .follow(&fan.id, &target.id, false, None, Vec::new())
            .expect("follow");

        for visibility in [FollowVisibility::Followers, FollowVisibility::Private] {
            for list in ["followers", "following"] {
                services
                    .accounts
                    .update_account(
                        &target.id,
                        UpdateAccountPatch {
                            followers_visibility: Some(visibility),
                            following_visibility: Some(visibility),
                            ..Default::default()
                        },
                        Vec::new(),
                    )
                    .expect("set visibility");
                for params in [Vec::new(), vec![("page", "true")]] {
                    let (status, _) = slice11_get(&services, list, &target.id, &params).await;
                    assert_eq!(
                        status,
                        StatusCode::FORBIDDEN,
                        "{list} {visibility:?} {params:?} must be 403"
                    );
                }
            }
        }

        // Back to public: both collections read again.
        services
            .accounts
            .update_account(
                &target.id,
                UpdateAccountPatch {
                    followers_visibility: Some(FollowVisibility::Public),
                    following_visibility: Some(FollowVisibility::Public),
                    ..Default::default()
                },
                Vec::new(),
            )
            .expect("set public");
        for (list, params) in [
            ("followers", Vec::new()),
            ("followers", vec![("page", "true")]),
            ("following", Vec::new()),
            ("following", vec![("page", "true")]),
        ] {
            let (status, _) = slice11_get(&services, list, &target.id, &params).await;
            assert_eq!(status, StatusCode::OK, "{list} {params:?} must read again");
        }
    }

    // (c) Outbox: more than one page of public notes, with Followers-only
    // notes interleaved. Public notes page exactly once; hidden ones never leak.
    #[tokio::test]
    async fn slice11_outbox_pages_public_notes_skipping_non_public() {
        let services = Arc::new(test_services("s11-outbox"));
        let author = slice11_local(&services, "poster");
        let mut public_ids: Vec<String> = Vec::new();
        let mut hidden_ids: Vec<String> = Vec::new();
        for i in 0..100 {
            let mut input = CreateNoteInput::simple_public(author.id.clone(), format!("note {i}"));
            if i % 3 == 0 {
                input.visibility = Visibility::Followers;
                hidden_ids.push(
                    services
                        .notes
                        .create_note(input, Vec::new())
                        .expect("create hidden note")
                        .id,
                );
            } else {
                public_ids.push(
                    services
                        .notes
                        .create_note(input, Vec::new())
                        .expect("create public note")
                        .id,
                );
            }
        }
        assert_eq!(public_ids.len(), 66);
        assert_eq!(hidden_ids.len(), 34);

        let coll_url = format!("{SLICE11_BASE}/users/{}/outbox", author.id);
        let (status, index) = slice11_get(&services, "outbox", &author.id, &[]).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(index["type"].as_str(), Some("OrderedCollection"));
        assert!(
            index.get("totalItems").is_none(),
            "outbox totalItems is omitted"
        );

        let pages = slice11_walk(&services, "outbox", &author.id).await;
        assert_eq!(pages.len(), 2, "66 public notes at 40 per page is 2 pages");
        assert!(pages[1].get("next").is_none());
        assert!(pages[0].get("totalItems").is_none());
        assert_eq!(pages[0]["partOf"].as_str(), Some(coll_url.as_str()));

        let rendered: Vec<String> = pages
            .iter()
            .flat_map(|p| {
                p["orderedItems"]
                    .as_array()
                    .expect("orderedItems array")
                    .iter()
                    .map(|v| v.to_string())
            })
            .collect();
        assert_eq!(rendered.len(), 66, "every public note exactly once");
        for id in &public_ids {
            let hits = rendered.iter().filter(|r| r.contains(id.as_str())).count();
            assert_eq!(hits, 1, "public note {id} must appear exactly once");
        }
        for id in &hidden_ids {
            assert!(
                !rendered.iter().any(|r| r.contains(id.as_str())),
                "followers-only note {id} leaked into the outbox"
            );
        }
    }

    // (d) An invalid cursor is a 400 on every paged collection.
    #[tokio::test]
    async fn slice11_invalid_cursor_returns_400() {
        let services = Arc::new(test_services("s11-cursor"));
        let account = slice11_local(&services, "cursorvictim");
        // "!!!" and "a+b/c=" are not URL-safe unpadded base64; "" decodes to
        // nothing; "_w" decodes to 0xFF, which is not UTF-8.
        for collection in ["followers", "following", "outbox"] {
            for bad in ["!!!", "a+b/c=", "", "_w"] {
                let (status, _) = slice11_get(
                    &services,
                    collection,
                    &account.id,
                    &[("page", "true"), ("cursor", bad)],
                )
                .await;
                assert_eq!(
                    status,
                    StatusCode::BAD_REQUEST,
                    "{collection} with cursor {bad:?} must be 400"
                );
            }
        }
    }

    // (e) Following pages completely across three pages (40 + 40 + 5).
    #[tokio::test]
    async fn slice11_following_pages_completely() {
        let services = Arc::new(test_services("s11-following"));
        let reader = slice11_local(&services, "reader");
        let mut expected: Vec<String> = Vec::new();
        for i in 0..85 {
            let star = slice11_local(&services, &format!("star{i:03}"));
            services
                .follows
                .follow(&reader.id, &star.id, false, None, Vec::new())
                .expect("follow");
            expected.push(star.actor_url(SLICE11_BASE));
        }
        let (status, index) = slice11_get(&services, "following", &reader.id, &[]).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(index["totalItems"].as_u64(), Some(85));

        let pages = slice11_walk(&services, "following", &reader.id).await;
        assert_eq!(pages.len(), 3);
        let mut seen: Vec<String> = Vec::new();
        for (i, page) in pages.iter().enumerate() {
            let items = page["orderedItems"].as_array().expect("orderedItems array");
            let expected_len = if i < 2 { 40 } else { 5 };
            assert_eq!(items.len(), expected_len, "page {i} length");
            assert_eq!(
                page.get("next").is_some(),
                i < 2,
                "next only before the last page"
            );
            for item in items {
                seen.push(item.as_str().expect("actor uri string").to_string());
            }
        }
        let mut expected_sorted = expected.clone();
        expected_sorted.sort();
        let mut seen_sorted = seen.clone();
        seen_sorted.sort();
        assert_eq!(seen_sorted, expected_sorted, "no duplicates, none missing");
    }
}
