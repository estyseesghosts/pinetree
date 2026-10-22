use crate::application::commands::AppServices;

use super::read::parse_page_visibility;
use crate::application::ports::PortError;
use crate::domain::collection::CollectionTier;
use crate::domain::id::DomainError;
use crate::domain::moderation::{ModerationAction, ModerationLogEntry};
use crate::domain::note::Visibility;
use crate::domain::page::{Page, PageFont};

pub(crate) fn domain_err(e: DomainError) -> PortError {
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
    /// Read-gate visibility (`public|home|followers|specified`; pinetree
    /// extension `pageVisibility`, DIVERGENCE — upstream has no page
    /// visibility). Local creates default `Public`. `None` = untouched on
    /// update.
    pub page_visibility: Option<String>,
    /// Explicit reader ids when `pageVisibility == specified`
    /// (pinetree extension `visibleUserIds`, DIVERGENCE).
    pub visible_user_ids: Option<Vec<String>>,
    /// Per-page federate flag. `None` (param absent) means **true**:
    /// pages are public by default and federate once the instance
    /// enables `federatePages` (owner decision 2026-10-20; clips stay
    /// opt-in). Explicit `false` opts a page out.
    pub federate: Option<bool>,
    pub approved_actor_uris: Option<Vec<String>>,
}

impl AppServices {
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
        if let Some(v) = &body.page_visibility {
            parse_page_visibility(v).map_err(domain_err)?;
        }
        if let Some(ids) = &body.visible_user_ids {
            if ids.len() > 100 {
                return Err(PortError::Validation(
                    "visibleUserIds must have at most 100 entries".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Viewer gate for one page row: `NO_SUCH_PAGE`-hiding for unauthorized
    /// readers (never a distinguishing error). Owner always passes;
    /// moderators bypass (consistent with `pages/delete`).
    pub fn create_page(&self, owner_id: &str, body: PageBody) -> Result<Page, PortError> {
        self.accounts.get_owner(owner_id)?;
        let title = body
            .title
            .clone()
            .ok_or_else(|| PortError::Validation("title: this field is required".to_string()))?;
        let name = body
            .name
            .clone()
            .ok_or_else(|| PortError::Validation("name: this field is required".to_string()))?;
        let content = body
            .content
            .clone()
            .ok_or_else(|| PortError::Validation("content: this field is required".to_string()))?;
        let variables = body.variables.clone().ok_or_else(|| {
            PortError::Validation("variables: this field is required".to_string())
        })?;
        let script = body
            .script
            .clone()
            .ok_or_else(|| PortError::Validation("script: this field is required".to_string()))?;
        self.validate_page_body(owner_id, &body)?;
        let now = self.now_ms();
        let page = Page {
            id: self.ids.generate(),
            owner_id: owner_id.to_string(),
            title,
            name,
            summary: body
                .summary
                .clone()
                .unwrap_or(None)
                .filter(|s| !s.is_empty()),
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
                Some(v) => crate::domain::collection::validate_tier_name(v).map_err(domain_err)?,
                None => CollectionTier::Listed,
            },
            visibility: match &body.page_visibility {
                Some(v) => parse_page_visibility(v)?,
                None => Visibility::Public,
            },
            visible_user_ids: body.visible_user_ids.clone().unwrap_or_default(),
            federate: body.federate.unwrap_or(true),
            approved_actor_uris: body.approved_actor_uris.clone().unwrap_or_default(),
            uri: None,
            remote_attachments: Vec::new(),
            remote_published_ms: None,
            remote_updated_ms: None,
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
    /// on, per-page flag on, `Listed` tier, visibility Public/Home/Followers
    /// — Specified pages never fan out; they serve live by bearer/direct
    /// fetch. Pure read; the serving gate enforces the same switches on
    /// fetch).
    fn page_federates(&self, page: &Page) -> Result<bool, PortError> {
        if !page.federate {
            return Ok(false);
        }
        if page.tier != CollectionTier::Listed {
            return Ok(false);
        }
        if page.visibility == Visibility::Specified {
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
    pub(crate) fn page_like_job(
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
            && body.page_visibility.is_none()
            && body.visible_user_ids.is_none()
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
            page.tier = crate::domain::collection::validate_tier_name(&v).map_err(domain_err)?;
        }
        if let Some(v) = body.page_visibility {
            page.visibility = parse_page_visibility(&v)?;
        }
        if let Some(ids) = body.visible_user_ids {
            page.visible_user_ids = ids;
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
        let mut jobs = Vec::new();
        if self.page_federates(&page)? {
            let base = self.public_url.trim_end_matches('/').to_string();
            let page_uri = format!("{base}/pages/{page_id}");
            jobs.push(
                crate::federation::outbox::FederateDeliverPayload::PageDeleted {
                    page_id: page_id.to_string(),
                    author_id: page.owner_id.clone(),
                    page_uri,
                    inbox_override: None,
                }
                .into_job(format!("federate:page:{page_id}:delete"), self.now_ms())?,
            );
        }
        if !self.pages.delete_page(page_id, jobs)? {
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
}
