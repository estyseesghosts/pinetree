use crate::application::commands::AppServices;

use crate::application::ports::PortError;
use crate::domain::collection::CollectionTier;
use crate::domain::id::DomainError;
use crate::domain::note::Visibility;
use crate::domain::page::{Page, PageFont, PageLike, page_visible_to};

impl AppServices {
    pub fn check_page_visible(
        &self,
        page: &Page,
        viewer_id: Option<&str>,
    ) -> Result<(), PortError> {
        if viewer_id.is_none()
            && page.visibility != Visibility::Public
            && page.visibility != Visibility::Home
        {
            return Err(PortError::NotFound("NO_SUCH_PAGE".to_string()));
        }
        let (is_mod, follows) = match viewer_id {
            Some(v) => {
                let is_mod = self
                    .accounts
                    .find_by_id(v)?
                    .is_some_and(|a| a.is_admin || a.is_moderator);
                let follows = if v == page.owner_id {
                    true
                } else {
                    self.follows.is_following(v, &page.owner_id)?
                };
                (is_mod, follows)
            }
            None => (false, false),
        };
        if page_visible_to(page, viewer_id, is_mod, follows) {
            Ok(())
        } else {
            Err(PortError::NotFound("NO_SUCH_PAGE".to_string()))
        }
    }

    /// `pages/create`: full body required (upstream param set).
    pub fn show_page_by_id(
        &self,
        page_id: &str,
        viewer_id: Option<&str>,
    ) -> Result<Page, PortError> {
        match self.pages.get_page(page_id)? {
            Some(page) => {
                self.check_page_visible(&page, viewer_id)?;
                Ok(page)
            }
            None => Err(PortError::NotFound("NO_SUCH_PAGE".to_string())),
        }
    }

    pub fn show_page_by_name(
        &self,
        name: &str,
        username: &str,
        viewer_id: Option<&str>,
    ) -> Result<Page, PortError> {
        let author = self
            .accounts
            .find_by_handle(&username.to_lowercase())?
            .filter(|a| a.is_local())
            .ok_or_else(|| PortError::NotFound("NO_SUCH_PAGE".to_string()))?;
        match self.pages.get_page_by_name(&author.id, name)? {
            Some(page) => {
                self.check_page_visible(&page, viewer_id)?;
                Ok(page)
            }
            None => Err(PortError::NotFound("NO_SUCH_PAGE".to_string())),
        }
    }

    /// `pages/like {pageId!}`: own pages → `YOUR_PAGE`, duplicate →
    /// `ALREADY_LIKED` (count bump commits with the row insert).
    pub fn like_page(&self, user_id: &str, page_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        let page = match self.pages.get_page(page_id)? {
            Some(page) => page,
            None => return Err(PortError::NotFound("NO_SUCH_PAGE".to_string())),
        };
        self.check_page_visible(&page, Some(user_id))?;
        if page.owner_id == user_id {
            return Err(PortError::Validation("YOUR_PAGE".to_string()));
        }
        let like = PageLike {
            id: self.ids.generate(),
            user_id: user_id.to_string(),
            page_id: page_id.to_string(),
            created_at_ms: self.now_ms(),
        };
        // Remote pages (materialized `Article`s): the like federates back
        // to the origin as `Like` with the page-URI object. Co-located
        // likes need no delivery.
        let mut jobs = Vec::new();
        if page.uri.is_some() {
            if let Some(job) = self.page_like_job(&page, user_id)? {
                jobs.push(job);
            }
        }
        if !self.pages.like_page(like, jobs)? {
            return Err(PortError::Conflict("ALREADY_LIKED".to_string()));
        }
        Ok(())
    }

    /// `pages/unlike {pageId!}`: missing row → `NOT_LIKED`.
    pub fn unlike_page(&self, user_id: &str, page_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        if self.pages.get_page(page_id)?.is_none() {
            return Err(PortError::NotFound("NO_SUCH_PAGE".to_string()));
        }
        if !self.pages.unlike_page(user_id, page_id)? {
            return Err(PortError::Conflict("NOT_LIKED".to_string()));
        }
        Ok(())
    }

    /// Record an inbound `Like` targeting a local page URI. The liker is
    /// a known account row (local or cached-remote); the pair insert is
    /// idempotent and the counter bump commits in the same transaction,
    /// so replays converge. No notification is emitted (documented
    /// DIVERGENCE: page likes surface in `likedCount`/`featured`, not the
    /// notification timeline). Returns `true` when the like now holds.
    pub fn handle_inbound_page_like(
        &self,
        activity_id: &str,
        liker_id: &str,
        page_id: &str,
    ) -> Result<bool, PortError> {
        if self.pages.get_page(page_id)?.is_none() {
            return Ok(false);
        }
        if self.inbound_host_blocked(liker_id) {
            return Ok(false);
        }
        // Exactly-once: replays converge without double-counting.
        if self.dedup.check_and_record(activity_id)? {
            return Ok(true);
        }
        let like = PageLike {
            id: self.ids.generate(),
            user_id: liker_id.to_string(),
            page_id: page_id.to_string(),
            created_at_ms: self.now_ms(),
        };
        // Idempotent: a duplicate pair still holds the like.
        match self.pages.like_page(like, Vec::new()) {
            Ok(_) => Ok(true),
            Err(PortError::Conflict(_)) => Ok(true),
            Err(e) => Err(e),
        }
    }

    /// Parse a `pageVisibility` param (`public|home|followers|specified`).
    pub fn parse_page_visibility_param(s: &str) -> Result<Visibility, PortError> {
        parse_page_visibility(s).map_err(|e| PortError::Validation(e.to_string()))
    }

    /// Slugify an `Article` name/title into a page slug (lowercase
    /// alnum/hyphen/underscore; anything else becomes `-`; empty falls
    /// back to `page`). The result always passes `validate_page_name`.
    pub fn slugify_page_name(raw: &str) -> String {
        use crate::domain::page::PAGE_NAME_MAX;
        let mut slug: String = raw
            .to_lowercase()
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '-'
                }
            })
            .collect();
        while slug.contains("--") {
            slug = slug.replace("--", "-");
        }
        slug = slug.trim_matches('-').to_string();
        let slug: String = slug.chars().take(PAGE_NAME_MAX).collect();
        if slug.is_empty() {
            "page".to_string()
        } else {
            slug
        }
    }

    /// Materialize (or refresh) a remote page from an inbound `Article`
    /// object. Any well-formed `Article` (object, `type == "Article"`,
    /// non-empty `id`) materializes as a page — the read gate is the
    /// row's `visibility`, mapped from `to`/`cc` (documented table in
    /// `docs/federated-collections.md` §4). Only malformed objects keep
    /// the old note path. Existing Article-derived *notes* stay notes —
    /// no retro-conversion (recorded DIVERGENCE).
    ///
    /// * Identity: the Article `id` becomes the row's canonical `uri`
    ///   (updates correlate by URI, never by slug).
    /// * Content: `source.content` blocks when they validate, else a
    ///   single synthesized text block from the sanitized `content` HTML.
    ///   `script` is NEVER stored from remote input (remote rows keep
    ///   `script: ""`).
    /// * Slugs: derived from the title with numeric disambiguation per
    ///   owner.
    /// * Attachments: `http(s)` attachment URLs are kept verbatim for
    ///   re-serve; nothing is fetched inline.
    /// * Audience → visibility: Public-in-`to` ⇒ Public; Public-in-`cc` ⇒
    ///   Public (pages have no timelines to exclude from — explicit,
    ///   documented collapse); followers collection in `to`/`cc` ⇒
    ///   Followers; otherwise Specified with best-effort-resolved user ids
    ///   (unresolvable recipients dropped, fail-closed, same as note
    ///   ingest).
    pub fn ingest_remote_article(
        &self,
        author_id: &str,
        obj: &serde_json::Value,
    ) -> Result<Option<Page>, PortError> {
        use crate::activitypub::objects::{
            extract_uri_list, is_public_collection_uri, sanitize_html_to_mfm,
        };
        use crate::domain::page::{PAGE_SUMMARY_MAX, PAGE_TITLE_MAX};
        let Some(map) = obj.as_object() else {
            return Ok(None);
        };
        if map.get("type").and_then(|v| v.as_str()).unwrap_or("") != "Article" {
            return Ok(None);
        }
        let Some(uri) = map
            .get("id")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
        else {
            return Ok(None);
        };
        self.accounts.get_owner(author_id)?;
        let now = self.now_ms();
        // Audience → visibility (documented table).
        let to = extract_uri_list(map.get("to"));
        let cc = extract_uri_list(map.get("cc"));
        let public_in_to = to.iter().any(|u| is_public_collection_uri(u));
        let public_in_cc = cc.iter().any(|u| is_public_collection_uri(u));
        let base = self.public_url.trim_end_matches('/').to_string();
        let followers_uri = format!("{base}/users/{author_id}/followers");
        // Note: remote authors address OUR followers collection by their
        // own actor URI, not ours — accept any */followers suffix.
        let followers_in = to
            .iter()
            .chain(cc.iter())
            .any(|u| u.ends_with("/followers"));
        let _ = followers_uri;
        let (visibility, visible_user_ids) = if public_in_to || public_in_cc {
            (Visibility::Public, Vec::new())
        } else if followers_in {
            (Visibility::Followers, Vec::new())
        } else {
            let mut ids = Vec::new();
            for u in to.iter().chain(cc.iter()) {
                if u == &uri {
                    continue;
                }
                if let Some(a) = self.accounts.find_by_uri(u)? {
                    if !ids.contains(&a.id) {
                        ids.push(a.id);
                    }
                }
            }
            (Visibility::Specified, ids)
        };
        // Origin timestamps (Phase 3: served in place of row times).
        let remote_published_ms = map
            .get("published")
            .and_then(|v| v.as_str())
            .and_then(crate::activitypub::parse_millis)
            .or_else(|| map.get("published").and_then(|v| v.as_i64()));
        let remote_updated_ms = map
            .get("updated")
            .and_then(|v| v.as_str())
            .and_then(crate::activitypub::parse_millis)
            .or_else(|| map.get("updated").and_then(|v| v.as_i64()));
        // Title: `name`, else a text snippet, else a fallback.
        let text_fallback = map
            .get("content")
            .and_then(|v| v.as_str())
            .map(sanitize_html_to_mfm)
            .filter(|t| !t.trim().is_empty());
        let mut title = map
            .get("name")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or_else(|| {
                text_fallback
                    .clone()
                    .map(|t| t.chars().take(40).collect::<String>())
            })
            .unwrap_or_else(|| "Untitled".to_string());
        if title.chars().count() > PAGE_TITLE_MAX {
            title = title.chars().take(PAGE_TITLE_MAX).collect();
        }
        let summary = map
            .get("summary")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.chars().take(PAGE_SUMMARY_MAX).collect::<String>());
        // Content: source blocks when valid, else one synthesized block.
        let content = match map.get("source").and_then(|v| v.get("content")) {
            Some(blocks @ serde_json::Value::Array(_)) => {
                if Page::validate_content(blocks).is_ok() {
                    blocks.clone()
                } else {
                    serde_json::Value::Array(Vec::new())
                }
            }
            _ => serde_json::Value::Array(Vec::new()),
        };
        let content = if content.as_array().is_some_and(|a| !a.is_empty()) {
            content
        } else {
            match text_fallback.filter(|t| !t.trim().is_empty()) {
                Some(text) => serde_json::json!([{ "type": "text", "text": text }]),
                None => serde_json::Value::Array(Vec::new()),
            }
        };
        let variables = match map.get("source").and_then(|v| v.get("variables")) {
            Some(vars @ serde_json::Value::Array(_)) => vars.clone(),
            _ => serde_json::json!([]),
        };
        // Presentation fidelity (Phase 3: `source.presentation` + `slug`
        // round-trip font/align/hide + the author's slug; unknown values
        // fall back to local defaults, never fail ingest).
        let source = map.get("source");
        let presentation = source.and_then(|v| v.get("presentation"));
        let ing_font = presentation
            .and_then(|v| v.get("font"))
            .and_then(|v| v.as_str())
            .and_then(PageFont::parse)
            .unwrap_or(PageFont::SansSerif);
        let ing_align = presentation
            .and_then(|v| v.get("alignCenter"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let ing_hide = presentation
            .and_then(|v| v.get("hideTitleWhenPinned"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let ing_slug = source
            .and_then(|v| v.get("slug"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|v| crate::domain::page::validate_page_name(v).is_ok())
            .map(str::to_string);
        // Attachments: keep http(s) URLs verbatim (re-served, never fetched).
        let mut remote_attachments = Vec::new();
        if let Some(items) = map.get("attachment").and_then(|v| v.as_array()) {
            for item in items {
                let url_ok = item
                    .get("url")
                    .and_then(|v| v.as_str())
                    .is_some_and(|u| u.starts_with("https://") || u.starts_with("http://"));
                if url_ok {
                    remote_attachments.push(item.clone());
                }
            }
        }
        // Refresh path: the canonical URI already materialized (the
        // update path carries visibility forward; serving re-checks per
        // request, so stale replicas self-heal on refetch).
        if let Some(mut page) = self.pages.get_page_by_uri(&uri)? {
            page.title = title;
            page.summary = summary;
            page.content = content;
            page.variables = variables;
            page.remote_attachments = remote_attachments;
            page.visibility = visibility;
            page.visible_user_ids = visible_user_ids;
            page.font = ing_font;
            page.align_center = ing_align;
            page.hide_title_when_pinned = ing_hide;
            if remote_published_ms.is_some() {
                page.remote_published_ms = remote_published_ms;
            }
            if remote_updated_ms.is_some() {
                page.remote_updated_ms = remote_updated_ms;
            }
            page.updated_at_ms = now;
            match self.pages.update_page(page, Vec::new())? {
                Some(page) => Ok(Some(page)),
                None => Ok(None),
            }
        } else {
            // Fresh slug (remote `source.slug` when valid, else
            // derived), disambiguated per owner.
            let slug = ing_slug.unwrap_or_else(|| Self::slugify_page_name(&title));
            let mut candidate = slug.clone();
            let mut n = 2u32;
            while self
                .pages
                .get_page_by_name(author_id, &candidate)?
                .is_some()
            {
                candidate = format!("{slug}-{n}");
                n += 1;
                if n > 10_000 {
                    candidate = format!("{slug}-{}", self.ids.generate());
                    break;
                }
            }
            let page = Page {
                id: self.ids.generate(),
                owner_id: author_id.to_string(),
                title,
                name: candidate,
                summary,
                content,
                variables,
                script: String::new(),
                eye_catching_image_id: None,
                font: ing_font,
                align_center: ing_align,
                hide_title_when_pinned: ing_hide,
                tier: CollectionTier::Listed,
                visibility,
                visible_user_ids,
                federate: false,
                approved_actor_uris: Vec::new(),
                uri: Some(uri),
                remote_attachments,
                remote_published_ms,
                remote_updated_ms,
                liked_count: 0,
                created_at_ms: now,
                updated_at_ms: now,
            };
            match self.pages.create_page(page, Vec::new()) {
                Ok(page) => Ok(Some(page)),
                Err(PortError::Conflict(_)) => Ok(None),
                Err(e) => Err(e),
            }
        }
    }

    /// Federation authorization callback for one page Article: same tier
    /// discipline as clips (global kill or per-page flag off ⇒ `Hidden`;
    /// `Private` ⇒ `Hidden`; `LocalApproved` ⇒ `Forbidden`; `Unlisted` /
    /// `Listed` ⇒ `Serve`; `RemoteApproved` ⇒ approver actors only).
    pub fn authorize_page_fetch(
        page: &Page,
        federate_switch_on: bool,
        fetcher_actor_uri: Option<&str>,
    ) -> crate::federation::fetch_gate::FetchDecision {
        use crate::federation::fetch_gate::FetchDecision;
        if !federate_switch_on || !page.federate {
            return FetchDecision::Hidden;
        }
        match page.tier {
            CollectionTier::Private => FetchDecision::Hidden,
            CollectionTier::LocalApproved => FetchDecision::Forbidden,
            CollectionTier::Unlisted | CollectionTier::Listed => FetchDecision::Serve,
            CollectionTier::RemoteApproved => match fetcher_actor_uri {
                Some(uri) if page.approved_actor_uris.iter().any(|a| a == uri) => {
                    FetchDecision::Serve
                }
                _ => FetchDecision::Forbidden,
            },
        }
    }
}

/// Parse a page read-gate visibility name.
pub(crate) fn parse_page_visibility(s: &str) -> Result<Visibility, DomainError> {
    match s {
        "public" => Ok(Visibility::Public),
        "home" => Ok(Visibility::Home),
        "followers" => Ok(Visibility::Followers),
        "specified" => Ok(Visibility::Specified),
        _ => Err(DomainError::Validation(
            "pageVisibility must be public|home|followers|specified".to_string(),
        )),
    }
}
