use crate::application::commands::AppServices;

use super::store::GalleryStore;
use super::types::{
    GALLERY_POPULAR_WINDOW_MS, GalleryLike, GalleryPost, IdCursor, validate_gallery_post,
};
use crate::application::ports::PortError;

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
        validate_gallery_post(
            post.title.as_deref(),
            post.description.as_deref(),
            &post.file_ids,
        )?;
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
}
