//! `gallery/*` + `flash/*` application ops (WP2-part-B).
//!
//! Ships gallery posts (`posts/create|delete|like|show|unlike|update`,
//! `posts`, `featured`, `popular` — 9 endpoints) and Play pages
//! (`flash/create|show|update|delete|my|like|unlike|my-likes|featured` — 9
//! endpoints) so clients stop 404ing.
//!
//! Domain-grade entities + validators live in the `domain` section below
//! (pure, no HTTP/DB — parent may relocate them to `src/domain/gallery.rs`
//! verbatim). Error codes mirror upstream phrasing; UUIDs pinetree-fixed:
//! - unknown gallery post → `NotFound("NO_SUCH_POST")` ("No such post.");
//! - unknown flash → `NotFound("NO_SUCH_FLASH")` ("No such flash.");
//! - non-owner update/delete → `Validation("ACCESS_DENIED")`;
//! - double like → `Conflict("ALREADY_LIKED")`, unlike of never-liked →
//!   `Conflict("NOT_LIKED")` (both codes already in the repo registry).
//!
//! `featured` = most-liked posts; `popular` = most-liked among recent
//! (30-day) posts — derived rankings, documented DIVERGENCE (upstream
//! curation is admin-driven; there is no admin gallery surface here).

use std::collections::HashMap;
use std::sync::RwLock;

use super::AppServices;
use crate::application::commands::channels::IdCursor;
use crate::application::ports::{NewJob, PortError};

// ---------------------------------------------------------------------------
// Domain (pure; parent may move to src/domain/gallery.rs verbatim).
// ---------------------------------------------------------------------------

/// Gallery post title bound (pinetree-fixed).
pub const GALLERY_TITLE_MAX: usize = 128;
/// Gallery post description bound (pinetree-fixed).
pub const GALLERY_DESCRIPTION_MAX: usize = 2048;
/// Gallery files per post (mirrors the note 16-file bound).
pub const GALLERY_FILES_MAX: usize = 16;
/// Flash title bound (pinetree-fixed).
pub const FLASH_TITLE_MAX: usize = 128;
/// Flash summary bound (pinetree-fixed).
pub const FLASH_SUMMARY_MAX: usize = 2048;
/// Flash script bound (Play scripts are code; generous cap, pinetree-fixed).
pub const FLASH_SCRIPT_MAX: usize = 100_000;
/// `popular` recency window.
pub const GALLERY_POPULAR_WINDOW_MS: i64 = 30 * 24 * 60 * 60 * 1000;

/// One gallery post (upstream `MiGalleryPost` local columns).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GalleryPost {
    pub id: String,
    pub owner_id: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub file_ids: Vec<String>,
    pub is_sensitive: bool,
    pub liked_count: u64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// One gallery like (unique on `(post_id, user_id)`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GalleryLike {
    pub id: String,
    pub post_id: String,
    pub user_id: String,
    pub created_at_ms: i64,
}

/// One Play page (upstream `MiFlash` local columns).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Flash {
    pub id: String,
    pub owner_id: String,
    pub title: String,
    pub summary: Option<String>,
    pub script: String,
    pub visibility: FlashVisibility,
    pub liked_count: u64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Play visibility (`public` / `home`, upstream `FlashVisibility`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FlashVisibility {
    Public,
    Home,
}

impl FlashVisibility {
    pub fn parse(value: &str) -> Result<Self, PortError> {
        match value {
            "public" => Ok(Self::Public),
            "home" => Ok(Self::Home),
            _ => Err(PortError::Validation(
                "INVALID_PARAM visibility must be public or home".to_string(),
            )),
        }
    }
}

/// One flash like (unique on `(flash_id, user_id)`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FlashLike {
    pub id: String,
    pub flash_id: String,
    pub user_id: String,
    pub created_at_ms: i64,
}

pub fn validate_gallery_post(
    title: Option<&str>,
    description: Option<&str>,
    file_ids: &[String],
) -> Result<(), PortError> {
    if let Some(t) = title {
        if t.chars().count() > GALLERY_TITLE_MAX {
            return Err(PortError::Validation(format!(
                "gallery title must be <={GALLERY_TITLE_MAX} chars"
            )));
        }
    }
    if let Some(d) = description {
        if d.chars().count() > GALLERY_DESCRIPTION_MAX {
            return Err(PortError::Validation(format!(
                "gallery description must be <={GALLERY_DESCRIPTION_MAX} chars"
            )));
        }
    }
    if file_ids.is_empty() {
        return Err(PortError::Validation(
            "FILE_REQUIRED gallery posts need at least one file".to_string(),
        ));
    }
    if file_ids.len() > GALLERY_FILES_MAX {
        return Err(PortError::Validation(format!(
            "gallery posts hold at most {GALLERY_FILES_MAX} files"
        )));
    }
    let mut seen = std::collections::HashSet::new();
    for file_id in file_ids {
        if !seen.insert(file_id) {
            return Err(PortError::Validation(
                "INVALID_PARAM duplicate fileIds".to_string(),
            ));
        }
    }
    Ok(())
}

pub fn validate_flash(
    title: &str,
    summary: Option<&str>,
    script: &str,
) -> Result<(), PortError> {
    let title = title.trim();
    if title.is_empty() || title.chars().count() > FLASH_TITLE_MAX {
        return Err(PortError::Validation(format!(
            "flash title must be 1..={FLASH_TITLE_MAX} chars"
        )));
    }
    if let Some(s) = summary {
        if s.chars().count() > FLASH_SUMMARY_MAX {
            return Err(PortError::Validation(format!(
                "flash summary must be <={FLASH_SUMMARY_MAX} chars"
            )));
        }
    }
    if script.is_empty() || script.chars().count() > FLASH_SCRIPT_MAX {
        return Err(PortError::Validation(format!(
            "flash script must be 1..={FLASH_SCRIPT_MAX} chars"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Storage seam (parent promotes to ports.rs / redb).
// ---------------------------------------------------------------------------

/// Gallery + flash storage seam. Mutating calls commit state + `jobs`
/// atomically.
pub trait GalleryStore: Send + Sync {
    fn insert_post(&self, post: GalleryPost, jobs: Vec<NewJob>) -> Result<GalleryPost, PortError>;
    fn update_post(&self, post: GalleryPost, jobs: Vec<NewJob>) -> Result<GalleryPost, PortError>;
    /// Delete + like cascade. Returns true when a row existed.
    fn delete_post(&self, post_id: &str, jobs: Vec<NewJob>) -> Result<bool, PortError>;
    fn get_post(&self, post_id: &str) -> Result<Option<GalleryPost>, PortError>;
    /// All posts, newest-first.
    fn all_posts(&self) -> Result<Vec<GalleryPost>, PortError>;
    /// One owner's posts, newest-first.
    fn posts_by_owner(&self, owner_id: &str) -> Result<Vec<GalleryPost>, PortError>;
    /// Like (insert). `Conflict(ALREADY_LIKED)` on a duplicate pair.
    fn like_post(&self, like: GalleryLike, jobs: Vec<NewJob>) -> Result<GalleryLike, PortError>;
    /// Unlike. `Conflict(NOT_LIKED)` when no row exists.
    fn unlike_post(&self, post_id: &str, user_id: &str, jobs: Vec<NewJob>) -> Result<(), PortError>;

    fn insert_flash(&self, flash: Flash, jobs: Vec<NewJob>) -> Result<Flash, PortError>;
    fn update_flash(&self, flash: Flash, jobs: Vec<NewJob>) -> Result<Flash, PortError>;
    /// Delete + like cascade. Returns true when a row existed.
    fn delete_flash(&self, flash_id: &str, jobs: Vec<NewJob>) -> Result<bool, PortError>;
    fn get_flash(&self, flash_id: &str) -> Result<Option<Flash>, PortError>;
    /// All flashes, newest-first.
    fn all_flashes(&self) -> Result<Vec<Flash>, PortError>;
    /// One owner's flashes, newest-first.
    fn flashes_by_owner(&self, owner_id: &str) -> Result<Vec<Flash>, PortError>;
    /// Like (insert). `Conflict(ALREADY_LIKED)` on a duplicate pair.
    fn like_flash(&self, like: FlashLike, jobs: Vec<NewJob>) -> Result<FlashLike, PortError>;
    /// Unlike. `Conflict(NOT_LIKED)` when no row exists.
    fn unlike_flash(
        &self,
        flash_id: &str,
        user_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError>;
    /// Newest-first likes of one user (for `flash/my-likes`).
    fn flash_likes_by_user(&self, user_id: &str) -> Result<Vec<FlashLike>, PortError>;
}

#[derive(Debug, Default)]
struct MemGalleryState {
    posts: HashMap<String, GalleryPost>,
    post_likes: HashMap<(String, String), GalleryLike>,
    flashes: HashMap<String, Flash>,
    flash_likes: HashMap<(String, String), FlashLike>,
    outbox: Vec<NewJob>,
}

/// Atomic in-memory [`GalleryStore`] (single lock = one transaction).
#[derive(Debug, Default)]
pub struct MemGalleryStore {
    state: RwLock<MemGalleryState>,
}

impl MemGalleryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Jobs committed alongside state (test drain / parent worker seam).
    pub fn drain_jobs(&self) -> Vec<NewJob> {
        self.state
            .write()
            .expect("mem gallery store poisoned")
            .outbox
            .drain(..)
            .collect()
    }

    fn lock(&self) -> std::sync::RwLockWriteGuard<'_, MemGalleryState> {
        self.state.write().expect("mem gallery store poisoned")
    }
}

fn newest_first_posts(mut posts: Vec<GalleryPost>) -> Vec<GalleryPost> {
    posts.sort_by(|a, b| {
        b.created_at_ms
            .cmp(&a.created_at_ms)
            .then(b.id.cmp(&a.id))
    });
    posts
}

fn newest_first_flashes(mut flashes: Vec<Flash>) -> Vec<Flash> {
    flashes.sort_by(|a, b| {
        b.created_at_ms
            .cmp(&a.created_at_ms)
            .then(b.id.cmp(&a.id))
    });
    flashes
}

impl GalleryStore for MemGalleryStore {
    fn insert_post(&self, post: GalleryPost, jobs: Vec<NewJob>) -> Result<GalleryPost, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        st.posts.insert(post.id.clone(), post.clone());
        st.outbox.extend(jobs);
        Ok(post)
    }

    fn update_post(&self, post: GalleryPost, jobs: Vec<NewJob>) -> Result<GalleryPost, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        if !st.posts.contains_key(&post.id) {
            return Err(PortError::NotFound("NO_SUCH_POST".to_string()));
        }
        st.posts.insert(post.id.clone(), post.clone());
        st.outbox.extend(jobs);
        Ok(post)
    }

    fn delete_post(&self, post_id: &str, jobs: Vec<NewJob>) -> Result<bool, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        if st.posts.remove(post_id).is_none() {
            return Ok(false);
        }
        st.post_likes.retain(|(pid, _), _| pid != post_id);
        st.outbox.extend(jobs);
        Ok(true)
    }

    fn get_post(&self, post_id: &str) -> Result<Option<GalleryPost>, PortError> {
        Ok(self
            .state
            .read()
            .expect("mem gallery store poisoned")
            .posts
            .get(post_id)
            .cloned())
    }

    fn all_posts(&self) -> Result<Vec<GalleryPost>, PortError> {
        Ok(newest_first_posts(
            self.state
                .read()
                .expect("mem gallery store poisoned")
                .posts
                .values()
                .cloned()
                .collect(),
        ))
    }

    fn posts_by_owner(&self, owner_id: &str) -> Result<Vec<GalleryPost>, PortError> {
        Ok(newest_first_posts(
            self.state
                .read()
                .expect("mem gallery store poisoned")
                .posts
                .values()
                .filter(|p| p.owner_id == owner_id)
                .cloned()
                .collect(),
        ))
    }

    fn like_post(&self, like: GalleryLike, jobs: Vec<NewJob>) -> Result<GalleryLike, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        if !st.posts.contains_key(&like.post_id) {
            return Err(PortError::NotFound("NO_SUCH_POST".to_string()));
        }
        let key = (like.post_id.clone(), like.user_id.clone());
        if st.post_likes.contains_key(&key) {
            return Err(PortError::Conflict("ALREADY_LIKED".to_string()));
        }
        st.post_likes.insert(key, like.clone());
        if let Some(post) = st.posts.get_mut(&like.post_id) {
            post.liked_count = post.liked_count.saturating_add(1);
        }
        st.outbox.extend(jobs);
        Ok(like)
    }

    fn unlike_post(
        &self,
        post_id: &str,
        user_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        if st
            .post_likes
            .remove(&(post_id.to_string(), user_id.to_string()))
            .is_none()
        {
            return Err(PortError::Conflict("NOT_LIKED".to_string()));
        }
        if let Some(post) = st.posts.get_mut(post_id) {
            post.liked_count = post.liked_count.saturating_sub(1);
        }
        st.outbox.extend(jobs);
        Ok(())
    }

    fn insert_flash(&self, flash: Flash, jobs: Vec<NewJob>) -> Result<Flash, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        st.flashes.insert(flash.id.clone(), flash.clone());
        st.outbox.extend(jobs);
        Ok(flash)
    }

    fn update_flash(&self, flash: Flash, jobs: Vec<NewJob>) -> Result<Flash, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        if !st.flashes.contains_key(&flash.id) {
            return Err(PortError::NotFound("NO_SUCH_FLASH".to_string()));
        }
        st.flashes.insert(flash.id.clone(), flash.clone());
        st.outbox.extend(jobs);
        Ok(flash)
    }

    fn delete_flash(&self, flash_id: &str, jobs: Vec<NewJob>) -> Result<bool, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        if st.flashes.remove(flash_id).is_none() {
            return Ok(false);
        }
        st.flash_likes.retain(|(fid, _), _| fid != flash_id);
        st.outbox.extend(jobs);
        Ok(true)
    }

    fn get_flash(&self, flash_id: &str) -> Result<Option<Flash>, PortError> {
        Ok(self
            .state
            .read()
            .expect("mem gallery store poisoned")
            .flashes
            .get(flash_id)
            .cloned())
    }

    fn all_flashes(&self) -> Result<Vec<Flash>, PortError> {
        Ok(newest_first_flashes(
            self.state
                .read()
                .expect("mem gallery store poisoned")
                .flashes
                .values()
                .cloned()
                .collect(),
        ))
    }

    fn flashes_by_owner(&self, owner_id: &str) -> Result<Vec<Flash>, PortError> {
        Ok(newest_first_flashes(
            self.state
                .read()
                .expect("mem gallery store poisoned")
                .flashes
                .values()
                .filter(|f| f.owner_id == owner_id)
                .cloned()
                .collect(),
        ))
    }

    fn like_flash(&self, like: FlashLike, jobs: Vec<NewJob>) -> Result<FlashLike, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        if !st.flashes.contains_key(&like.flash_id) {
            return Err(PortError::NotFound("NO_SUCH_FLASH".to_string()));
        }
        let key = (like.flash_id.clone(), like.user_id.clone());
        if st.flash_likes.contains_key(&key) {
            return Err(PortError::Conflict("ALREADY_LIKED".to_string()));
        }
        st.flash_likes.insert(key, like.clone());
        if let Some(flash) = st.flashes.get_mut(&like.flash_id) {
            flash.liked_count = flash.liked_count.saturating_add(1);
        }
        st.outbox.extend(jobs);
        Ok(like)
    }

    fn unlike_flash(
        &self,
        flash_id: &str,
        user_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        if st
            .flash_likes
            .remove(&(flash_id.to_string(), user_id.to_string()))
            .is_none()
        {
            return Err(PortError::Conflict("NOT_LIKED".to_string()));
        }
        if let Some(flash) = st.flashes.get_mut(flash_id) {
            flash.liked_count = flash.liked_count.saturating_sub(1);
        }
        st.outbox.extend(jobs);
        Ok(())
    }

    fn flash_likes_by_user(&self, user_id: &str) -> Result<Vec<FlashLike>, PortError> {
        let st = self.state.read().expect("mem gallery store poisoned");
        let mut out: Vec<FlashLike> = st
            .flash_likes
            .values()
            .filter(|l| l.user_id == user_id)
            .cloned()
            .collect();
        out.sort_by(|a, b| {
            b.created_at_ms
                .cmp(&a.created_at_ms)
                .then(b.id.cmp(&a.id))
        });
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Application ops.
// ---------------------------------------------------------------------------

impl AppServices {
    /// `gallery/posts/create` (drive-file ids are stored opaque; the
    /// dispatcher validates ownership when wiring).
    pub fn gallery_post_create(
        &self,
        store: &dyn GalleryStore,
        owner_id: &str,
        title: Option<&str>,
        description: Option<&str>,
        file_ids: Vec<String>,
        is_sensitive: bool,
    ) -> Result<GalleryPost, PortError> {
        self.accounts.get_owner(owner_id)?;
        validate_gallery_post(title, description, &file_ids)?;
        let now = self.clock.now_ms();
        store.insert_post(
            GalleryPost {
                id: self.ids.generate(),
                owner_id: owner_id.to_string(),
                title: title.map(str::to_string),
                description: description.map(str::to_string),
                file_ids,
                is_sensitive,
                liked_count: 0,
                created_at_ms: now,
                updated_at_ms: now,
            },
            Vec::new(),
        )
    }

    fn require_post(
        &self,
        store: &dyn GalleryStore,
        post_id: &str,
    ) -> Result<GalleryPost, PortError> {
        store
            .get_post(post_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_POST".to_string()))
    }

    /// `gallery/posts/show`.
    pub fn gallery_post_show(
        &self,
        store: &dyn GalleryStore,
        post_id: &str,
    ) -> Result<GalleryPost, PortError> {
        self.require_post(store, post_id)
    }

    /// `gallery/posts/update` (owner-only).
    pub fn gallery_post_update(
        &self,
        store: &dyn GalleryStore,
        owner_id: &str,
        post_id: &str,
        title: Option<Option<&str>>,
        description: Option<Option<&str>>,
        file_ids: Option<Vec<String>>,
        is_sensitive: Option<bool>,
    ) -> Result<GalleryPost, PortError> {
        self.accounts.get_owner(owner_id)?;
        let mut post = self.require_post(store, post_id)?;
        if post.owner_id != owner_id {
            return Err(PortError::Validation("ACCESS_DENIED".to_string()));
        }
        if let Some(title) = title {
            post.title = title.map(str::to_string);
        }
        if let Some(description) = description {
            post.description = description.map(str::to_string);
        }
        if let Some(file_ids) = file_ids {
            post.file_ids = file_ids;
        }
        validate_gallery_post(post.title.as_deref(), post.description.as_deref(), &post.file_ids)?;
        if let Some(is_sensitive) = is_sensitive {
            post.is_sensitive = is_sensitive;
        }
        post.updated_at_ms = self.clock.now_ms();
        store.update_post(post, Vec::new())
    }

    /// `gallery/posts/delete` (owner-only).
    pub fn gallery_post_delete(
        &self,
        store: &dyn GalleryStore,
        owner_id: &str,
        post_id: &str,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(owner_id)?;
        let post = self.require_post(store, post_id)?;
        if post.owner_id != owner_id {
            return Err(PortError::Validation("ACCESS_DENIED".to_string()));
        }
        store.delete_post(post_id, Vec::new())?;
        Ok(())
    }

    /// `gallery/posts/like`.
    pub fn gallery_post_like(
        &self,
        store: &dyn GalleryStore,
        user_id: &str,
        post_id: &str,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        self.require_post(store, post_id)?;
        store.like_post(
            GalleryLike {
                id: self.ids.generate(),
                post_id: post_id.to_string(),
                user_id: user_id.to_string(),
                created_at_ms: self.clock.now_ms(),
            },
            Vec::new(),
        )?;
        Ok(())
    }

    /// `gallery/posts/unlike`.
    pub fn gallery_post_unlike(
        &self,
        store: &dyn GalleryStore,
        user_id: &str,
        post_id: &str,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        self.require_post(store, post_id)?;
        store.unlike_post(post_id, user_id, Vec::new())
    }

    /// `gallery/posts` (all posts, newest-first).
    pub fn gallery_posts_list(
        &self,
        store: &dyn GalleryStore,
        cursor: &IdCursor,
    ) -> Result<Vec<GalleryPost>, PortError> {
        let all = store.all_posts()?;
        let limit = cursor.limit.clamp(1, 100);
        let mut out = all;
        if let Some(since) = cursor.since_id.as_deref() {
            out.retain(|p| p.id.as_str() > since);
        }
        if let Some(until) = cursor.until_id.as_deref() {
            out.retain(|p| p.id.as_str() < until);
        }
        out.truncate(limit);
        Ok(out)
    }

    /// `gallery/featured` (most-liked first).
    pub fn gallery_featured(
        &self,
        store: &dyn GalleryStore,
        cursor: &IdCursor,
    ) -> Result<Vec<GalleryPost>, PortError> {
        let mut all = store.all_posts()?;
        all.sort_by(|a, b| {
            b.liked_count
                .cmp(&a.liked_count)
                .then(b.created_at_ms.cmp(&a.created_at_ms))
                .then(b.id.cmp(&a.id))
        });
        let limit = cursor.limit.clamp(1, 100);
        all.truncate(limit);
        Ok(all)
    }

    /// `gallery/popular` (most-liked among the last-30-day posts).
    pub fn gallery_popular(
        &self,
        store: &dyn GalleryStore,
        cursor: &IdCursor,
    ) -> Result<Vec<GalleryPost>, PortError> {
        let cutoff = self.clock.now_ms() - GALLERY_POPULAR_WINDOW_MS;
        let mut recent: Vec<GalleryPost> = store
            .all_posts()?
            .into_iter()
            .filter(|p| p.created_at_ms >= cutoff)
            .collect();
        recent.sort_by(|a, b| {
            b.liked_count
                .cmp(&a.liked_count)
                .then(b.created_at_ms.cmp(&a.created_at_ms))
                .then(b.id.cmp(&a.id))
        });
        let limit = cursor.limit.clamp(1, 100);
        recent.truncate(limit);
        Ok(recent)
    }

    /// `flash/create`.
    pub fn flash_create(
        &self,
        store: &dyn GalleryStore,
        owner_id: &str,
        title: &str,
        summary: Option<&str>,
        script: &str,
        visibility: &str,
    ) -> Result<Flash, PortError> {
        self.accounts.get_owner(owner_id)?;
        validate_flash(title, summary, script)?;
        let visibility = FlashVisibility::parse(visibility)?;
        let now = self.clock.now_ms();
        store.insert_flash(
            Flash {
                id: self.ids.generate(),
                owner_id: owner_id.to_string(),
                title: title.trim().to_string(),
                summary: summary.map(str::to_string),
                script: script.to_string(),
                visibility,
                liked_count: 0,
                created_at_ms: now,
                updated_at_ms: now,
            },
            Vec::new(),
        )
    }

    fn require_flash(&self, store: &dyn GalleryStore, flash_id: &str) -> Result<Flash, PortError> {
        store
            .get_flash(flash_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_FLASH".to_string()))
    }

    /// `flash/show`.
    pub fn flash_show(
        &self,
        store: &dyn GalleryStore,
        flash_id: &str,
    ) -> Result<Flash, PortError> {
        self.require_flash(store, flash_id)
    }

    /// `flash/update` (owner-only).
    pub fn flash_update(
        &self,
        store: &dyn GalleryStore,
        owner_id: &str,
        flash_id: &str,
        title: Option<&str>,
        summary: Option<Option<&str>>,
        script: Option<&str>,
        visibility: Option<&str>,
    ) -> Result<Flash, PortError> {
        self.accounts.get_owner(owner_id)?;
        let mut flash = self.require_flash(store, flash_id)?;
        if flash.owner_id != owner_id {
            return Err(PortError::Validation("ACCESS_DENIED".to_string()));
        }
        if let Some(title) = title {
            flash.title = title.trim().to_string();
        }
        if let Some(summary) = summary {
            flash.summary = summary.map(str::to_string);
        }
        if let Some(script) = script {
            flash.script = script.to_string();
        }
        validate_flash(&flash.title, flash.summary.as_deref(), &flash.script)?;
        if let Some(visibility) = visibility {
            flash.visibility = FlashVisibility::parse(visibility)?;
        }
        flash.updated_at_ms = self.clock.now_ms();
        store.update_flash(flash, Vec::new())
    }

    /// `flash/delete` (owner-only).
    pub fn flash_delete(
        &self,
        store: &dyn GalleryStore,
        owner_id: &str,
        flash_id: &str,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(owner_id)?;
        let flash = self.require_flash(store, flash_id)?;
        if flash.owner_id != owner_id {
            return Err(PortError::Validation("ACCESS_DENIED".to_string()));
        }
        store.delete_flash(flash_id, Vec::new())?;
        Ok(())
    }

    /// `flash/my` (one owner's flashes, newest-first).
    pub fn flash_my(
        &self,
        store: &dyn GalleryStore,
        owner_id: &str,
        cursor: &IdCursor,
    ) -> Result<Vec<Flash>, PortError> {
        let all = store.flashes_by_owner(owner_id)?;
        let limit = cursor.limit.clamp(1, 100);
        let mut out = all;
        if let Some(since) = cursor.since_id.as_deref() {
            out.retain(|f| f.id.as_str() > since);
        }
        if let Some(until) = cursor.until_id.as_deref() {
            out.retain(|f| f.id.as_str() < until);
        }
        out.truncate(limit);
        Ok(out)
    }

    /// `flash/like`.
    pub fn flash_like(
        &self,
        store: &dyn GalleryStore,
        user_id: &str,
        flash_id: &str,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        self.require_flash(store, flash_id)?;
        store.like_flash(
            FlashLike {
                id: self.ids.generate(),
                flash_id: flash_id.to_string(),
                user_id: user_id.to_string(),
                created_at_ms: self.clock.now_ms(),
            },
            Vec::new(),
        )?;
        Ok(())
    }

    /// `flash/unlike`.
    pub fn flash_unlike(
        &self,
        store: &dyn GalleryStore,
        user_id: &str,
        flash_id: &str,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        self.require_flash(store, flash_id)?;
        store.unlike_flash(flash_id, user_id, Vec::new())
    }

    /// `flash/my-likes` — flashes one user liked, newest-like-first.
    /// Likes on deleted flashes are skipped (caller-side, same convention
    /// as clipped favorites).
    pub fn flash_my_likes(
        &self,
        store: &dyn GalleryStore,
        user_id: &str,
        cursor: &IdCursor,
    ) -> Result<Vec<Flash>, PortError> {
        let likes = store.flash_likes_by_user(user_id)?;
        let mut out = Vec::new();
        for like in likes {
            if let Some(flash) = store.get_flash(&like.flash_id)? {
                out.push(flash);
            }
        }
        let limit = cursor.limit.clamp(1, 100);
        out.truncate(limit);
        Ok(out)
    }

    /// `flash/featured` (most-liked first).
    pub fn flash_featured(
        &self,
        store: &dyn GalleryStore,
        cursor: &IdCursor,
    ) -> Result<Vec<Flash>, PortError> {
        let mut all = store.all_flashes()?;
        all.sort_by(|a, b| {
            b.liked_count
                .cmp(&a.liked_count)
                .then(b.created_at_ms.cmp(&a.created_at_ms))
                .then(b.id.cmp(&a.id))
        });
        let limit = cursor.limit.clamp(1, 100);
        all.truncate(limit);
        Ok(all)
    }
}
