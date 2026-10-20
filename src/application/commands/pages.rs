//! Pages (federated clips + pages handoff, Phase 3 local half).
//!
//! One application operation per Misskey `pages/*` endpoint, shared by the
//! Misskey adapter. Slug-uniqueness checks + inserts commit atomically in
//! ONE store call; like/unlike bump `liked_count` in the same transaction.
//! No federation fan-out happens here — outbound `Create`/`Update(Article)`
//! delivery and Article serving live in the federation layer and stay
//! dormant unless the admin `federatePages` switch AND the per-page
//! `federate` flag are both on.
//!
//! DIVERGENCE notes: `script` is stored (local parity for scripted pages)
//! but never serialized into any federated field and never executed
//! server-side (pinetree has no AiScript engine); `featured` is implemented
//! per the upstream query (`likedCount > 0` desc, max 10) — the handoff
//! assumed an admin-curation surface, but upstream `featured.ts` has none.

use super::AppServices;

use crate::application::ports::PortError;
use crate::domain::collection::CollectionTier;
use crate::domain::id::DomainError;
use crate::domain::moderation::{ModerationAction, ModerationLogEntry};
use crate::domain::page::{Page, PageFont, PageLike};

fn domain_err(e: DomainError) -> PortError {
    match e {
        DomainError::Validation(msg) => PortError::Validation(msg),
        DomainError::Conflict(msg) => PortError::Conflict(msg),
        DomainError::NotFound(msg) => PortError::NotFound(msg),
        DomainError::StorageUnavailable(msg) => PortError::StorageUnavailable(msg),
    }
}

/// Partial page body for create/update (mirrors the upstream param sets).
#[derive(Debug, Clone, Default)]
pub struct PageBody {
    pub title: Option<String>,
    pub name: Option<String>,
    pub summary: Option<Option<String>>,
    pub content: Option<serde_json::Value>,
    pub variables: Option<serde_json::Value>,
    pub script: Option<String>,
    pub eye_catching_image_id: Option<Option<String>>,
    pub font: Option<String>,
    pub align_center: Option<bool>,
    pub hide_title_when_pinned: Option<bool>,
    pub visibility: Option<String>,
    /// Per-page federate flag. `None` (param absent) means **true**:
    /// pages are public by default and federate once the instance
    /// enables `federatePages` (owner decision 2026-10-20; clips stay
    /// opt-in). Explicit `false` opts a page out.
    pub federate: Option<bool>,
    pub approved_actor_uris: Option<Vec<String>>,
}

impl AppServices {
    /// Eye-catch images must be the caller's own files (upstream
    /// `NO_SUCH_FILE` parity).
    fn check_own_file(&self, owner_id: &str, file_id: &str) -> Result<(), PortError> {
        match self.media.get(file_id)? {
            Some(meta) if meta.owner_id.as_deref() == Some(owner_id) => Ok(()),
            _ => Err(PortError::NotFound("NO_SUCH_FILE".to_string())),
        }
    }

    /// Validate one page body (shared by create and update).
    fn validate_page_body(&self, owner_id: &str, body: &PageBody) -> Result<(), PortError> {
        if let Some(title) = &body.title {
            Page::validate_title(title).map_err(domain_err)?;
        }
        if let Some(name) = &body.name {
            crate::domain::page::validate_page_name(name).map_err(domain_err)?;
        }
        if let Some(summary) = &body.summary {
            Page::validate_summary(summary.as_deref()).map_err(domain_err)?;
        }
        if let Some(content) = &body.content {
            Page::validate_content(content).map_err(domain_err)?;
        }
        if let Some(variables) = &body.variables {
            if !variables.is_array() {
                return Err(PortError::Validation(
                    "page variables must be an array".to_string(),
                ));
            }
        }
        if let Some(script) = &body.script {
            Page::validate_script(script).map_err(domain_err)?;
        }
        if let Some(eye) = &body.eye_catching_image_id {
            if let Some(id) = eye {
                self.check_own_file(owner_id, id)?;
            }
        }
        if let Some(font) = &body.font {
            if PageFont::parse(font).is_none() {
                return Err(PortError::Validation(
                    "font must be serif or sans-serif".to_string(),
                ));
            }
        }
        if let Some(v) = &body.visibility {
            crate::domain::collection::validate_tier_name(v).map_err(domain_err)?;
        }
        Ok(())
    }

    /// `pages/create`: full body required (upstream param set).
    pub fn create_page(&self, owner_id: &str, body: PageBody) -> Result<Page, PortError> {
        self.accounts.get_owner(owner_id)?;
        let title = body.title.clone().ok_or_else(|| {
            PortError::Validation("title: this field is required".to_string())
        })?;
        let name = body.name.clone().ok_or_else(|| {
            PortError::Validation("name: this field is required".to_string())
        })?;
        let content = body.content.clone().ok_or_else(|| {
            PortError::Validation("content: this field is required".to_string())
        })?;
        let variables = body.variables.clone().ok_or_else(|| {
            PortError::Validation("variables: this field is required".to_string())
        })?;
        let script = body.script.clone().ok_or_else(|| {
            PortError::Validation("script: this field is required".to_string())
        })?;
        self.validate_page_body(owner_id, &body)?;
        let now = self.now_ms();
        let page = Page {
            id: self.ids.generate(),
            owner_id: owner_id.to_string(),
            title,
            name,
            summary: body.summary.clone().unwrap_or(None).filter(|s| !s.is_empty()),
            content,
            variables,
            script,
            eye_catching_image_id: body.eye_catching_image_id.clone().unwrap_or(None),
            font: body
                .font
                .as_deref()
                .and_then(PageFont::parse)
                .unwrap_or_default(),
            align_center: body.align_center.unwrap_or(false),
            hide_title_when_pinned: body.hide_title_when_pinned.unwrap_or(false),
            tier: match &body.visibility {
                Some(v) => crate::domain::collection::validate_tier_name(v)
                    .map_err(domain_err)?,
                None => CollectionTier::Listed,
            },
            federate: body.federate.unwrap_or(true),
            approved_actor_uris: body.approved_actor_uris.clone().unwrap_or_default(),
            uri: None,
            remote_attachments: Vec::new(),
            liked_count: 0,
            created_at_ms: now,
            updated_at_ms: now,
        };
        let mut jobs = Vec::new();
        if self.page_federates(&page)? {
            jobs.push(self.page_activity_job(&page, false)?);
        }
        self.pages.create_page(page, jobs)
    }

    /// Whether page create/update federates right now (both switches
    /// on, per-page flag on, `Listed` tier — capability-tier pages never
    /// fan out; they serve live by bearer/direct fetch). Pure read; the
    /// serving gate enforces the same switches on fetch.
    fn page_federates(&self, page: &Page) -> Result<bool, PortError> {
        if !page.federate {
            return Ok(false);
        }
        if page.tier != CollectionTier::Listed {
            return Ok(false);
        }
        Ok(self
            .instance_settings
            .get()
            .map(|s| s.federate_pages)
            .unwrap_or(false))
    }

    /// Build the `Create(Article)` / `Update(Article)` fan-out job (the
    /// worker expands recipients; per-inbox jobs derive from this one).
    fn page_activity_job(
        &self,
        page: &Page,
        updated: bool,
    ) -> Result<crate::application::ports::NewJob, PortError> {
        use crate::federation::outbox::FederateDeliverPayload;
        let payload = if updated {
            FederateDeliverPayload::PageUpdated {
                page_id: page.id.clone(),
                author_id: page.owner_id.clone(),
                inbox_override: None,
            }
        } else {
            FederateDeliverPayload::PageCreated {
                page_id: page.id.clone(),
                author_id: page.owner_id.clone(),
                inbox_override: None,
            }
        };
        payload.into_job(
            format!(
                "federate:page:{}:{}",
                page.id,
                if updated { "update" } else { "create" }
            ),
            self.now_ms(),
        )
    }

    /// Build the `Like` fan-out job for a like on a remote page, or
    /// `None` when the origin has no known inbox (local pages never
    /// federate likes — liker and page are co-located).
    fn page_like_job(
        &self,
        page: &Page,
        liker_id: &str,
    ) -> Result<Option<crate::application::ports::NewJob>, PortError> {
        use crate::federation::outbox::FederateDeliverPayload;
        let Some(page_uri) = page.uri.as_deref() else {
            return Ok(None);
        };
        let owner = match self.accounts.find_by_id(&page.owner_id)? {
            Some(a) if !a.is_local() => a,
            _ => return Ok(None),
        };
        let inbox_url = match crate::federation::outbox::preferred_inbox(&owner) {
            Some(inbox) => inbox,
            None => return Ok(None),
        };
        let liker = self
            .accounts
            .find_by_id(liker_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
        let base = self.public_url.trim_end_matches('/');
        let actor_uri = liker.actor_url(base);
        let activity_id = format!("page-like-{}-{liker_id}-{}", page.id, self.ids.generate());
        FederateDeliverPayload::PageLiked {
            activity_id,
            actor_id: liker.id,
            actor_uri,
            page_uri: page_uri.to_string(),
            inbox_url,
        }
        .into_job(
            format!("federate:page-like:{}:{liker_id}", page.id),
            self.now_ms(),
        )
        .map(Some)
    }

    /// Owner-scoped page lookup (`NO_SUCH_PAGE` on miss or foreign page).
    fn owner_page(&self, owner_id: &str, page_id: &str) -> Result<Page, PortError> {
        self.accounts.get_owner(owner_id)?;
        match self.pages.get_page(page_id)? {
            Some(page) if page.owner_id == owner_id => Ok(page),
            _ => Err(PortError::NotFound("NO_SUCH_PAGE".to_string())),
        }
    }

    /// `pages/update {pageId!, ...}`: partial merge, `summary: null`
    /// clears, `updatedAt = now` (upstream parity).
    pub fn update_page(
        &self,
        owner_id: &str,
        page_id: &str,
        body: PageBody,
    ) -> Result<(), PortError> {
        let mut page = self.owner_page(owner_id, page_id)?;
        if body.title.is_none()
            && body.name.is_none()
            && body.summary.is_none()
            && body.content.is_none()
            && body.variables.is_none()
            && body.script.is_none()
            && body.eye_catching_image_id.is_none()
            && body.font.is_none()
            && body.align_center.is_none()
            && body.hide_title_when_pinned.is_none()
            && body.visibility.is_none()
            && body.federate.is_none()
            && body.approved_actor_uris.is_none()
        {
            self.validate_page_body(owner_id, &body)?;
        } else {
            self.validate_page_body(owner_id, &body)?;
        }
        if let Some(title) = body.title {
            page.title = title;
        }
        if let Some(name) = body.name {
            page.name = name;
        }
        if let Some(summary) = body.summary {
            page.summary = summary.filter(|s| !s.is_empty());
        }
        if let Some(content) = body.content {
            page.content = content;
        }
        if let Some(variables) = body.variables {
            page.variables = variables;
        }
        if let Some(script) = body.script {
            page.script = script;
        }
        if let Some(eye) = body.eye_catching_image_id {
            page.eye_catching_image_id = eye;
        }
        if let Some(font) = body.font {
            page.font = PageFont::parse(&font).unwrap_or_default();
        }
        if let Some(align) = body.align_center {
            page.align_center = align;
        }
        if let Some(hide) = body.hide_title_when_pinned {
            page.hide_title_when_pinned = hide;
        }
        if let Some(v) = body.visibility {
            page.tier =
                crate::domain::collection::validate_tier_name(&v).map_err(domain_err)?;
        }
        if let Some(federate) = body.federate {
            page.federate = federate;
        }
        if let Some(uris) = body.approved_actor_uris {
            page.approved_actor_uris = uris;
        }
        page.updated_at_ms = self.now_ms();
        let mut jobs = Vec::new();
        if self.page_federates(&page)? {
            jobs.push(self.page_activity_job(&page, true)?);
        }
        match self.pages.update_page(page, jobs)? {
            Some(_) => Ok(()),
            None => Err(PortError::NotFound("NO_SUCH_PAGE".to_string())),
        }
    }

    /// `pages/delete {pageId!}`: owner or moderator (upstream
    /// `PageService.delete()` parity). Moderator deletes write a
    /// `deletePage` moderation-log row. Access by anyone else is
    /// `ACCESS_DENIED`.
    pub fn delete_page(&self, caller_id: &str, page_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(caller_id)?;
        let caller = self
            .accounts
            .find_by_id(caller_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
        let page = match self.pages.get_page(page_id)? {
            Some(page) => page,
            None => return Err(PortError::NotFound("NO_SUCH_PAGE".to_string())),
        };
        let is_owner = page.owner_id == caller_id;
        let is_staff = caller.is_admin || caller.is_moderator;
        if !is_owner && !is_staff {
            return Err(PortError::Validation("ACCESS_DENIED".to_string()));
        }
        if !self.pages.delete_page(page_id)? {
            return Err(PortError::NotFound("NO_SUCH_PAGE".to_string()));
        }
        if !is_owner {
            let log = ModerationLogEntry::new(
                self.ids.generate(),
                Some(caller_id.to_string()),
                ModerationAction::DeletePage,
                Some(page.owner_id.clone()),
                serde_json::json!({ "pageId": page_id }),
                self.clock.now_ms(),
            )?;
            self.moderation.append(log)?;
        }
        Ok(())
    }

    /// `pages/show`: by id, or by `(name, username)` for a LOCAL user only
    /// (upstream `host: IsNull` parity). Any page is readable (upstream has
    /// no page visibility gate).
    pub fn show_page_by_id(&self, page_id: &str) -> Result<Page, PortError> {
        match self.pages.get_page(page_id)? {
            Some(page) => Ok(page),
            None => Err(PortError::NotFound("NO_SUCH_PAGE".to_string())),
        }
    }

    pub fn show_page_by_name(&self, name: &str, username: &str) -> Result<Page, PortError> {
        let author = self
            .accounts
            .find_by_handle(&username.to_lowercase())?
            .filter(|a| a.is_local())
            .ok_or_else(|| PortError::NotFound("NO_SUCH_PAGE".to_string()))?;
        match self.pages.get_page_by_name(&author.id, name)? {
            Some(page) => Ok(page),
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
    /// object. Only **public** Articles materialize — anything else keeps
    /// the note path, which enforces visibility (serving a cached
    /// restricted Article as a world-readable page would leak it).
    /// Returns the page, or `None` when the object is not a public
    /// `Article` (caller falls back to the note pipeline).
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
        // Public only (see doc comment).
        let to = extract_uri_list(map.get("to"));
        let cc = extract_uri_list(map.get("cc"));
        if !to.iter().any(|u| is_public_collection_uri(u))
            && !cc.iter().any(|u| is_public_collection_uri(u))
        {
            return Ok(None);
        }
        self.accounts.get_owner(author_id)?;
        let now = self.now_ms();
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
        // Refresh path: the canonical URI already materialized.
        if let Some(mut page) = self.pages.get_page_by_uri(&uri)? {
            page.title = title;
            page.summary = summary;
            page.content = content;
            page.variables = variables;
            page.remote_attachments = remote_attachments;
            page.updated_at_ms = now;
            match self.pages.update_page(page, Vec::new())? {
                Some(page) => Ok(Some(page)),
                None => Ok(None),
            }
        } else {
            // Fresh slug, disambiguated per owner.
            let slug = Self::slugify_page_name(&title);
            let mut candidate = slug.clone();
            let mut n = 2u32;
            while self.pages.get_page_by_name(author_id, &candidate)?.is_some() {
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
                font: PageFont::SansSerif,
                align_center: false,
                hide_title_when_pinned: false,
                tier: CollectionTier::Listed,
                federate: false,
                approved_actor_uris: Vec::new(),
                uri: Some(uri),
                remote_attachments,
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
                Some(uri)
                    if page.approved_actor_uris.iter().any(|a| a == uri) =>
                {
                    FetchDecision::Serve
                }
                _ => FetchDecision::Forbidden,
            },
        }
    }
}
