use crate::application::commands::AppServices;

use super::store::GalleryStore;
use super::types::{Flash, FlashLike, FlashVisibility, IdCursor, validate_flash};
use crate::application::ports::PortError;

impl AppServices {
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
    pub fn flash_show(&self, store: &dyn GalleryStore, flash_id: &str) -> Result<Flash, PortError> {
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
