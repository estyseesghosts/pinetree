use std::collections::HashMap;
use std::sync::RwLock;

use super::types::{Flash, FlashLike, GalleryLike, GalleryPost};
use crate::application::ports::{NewJob, PortError};

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
    fn unlike_post(&self, post_id: &str, user_id: &str, jobs: Vec<NewJob>)
    -> Result<(), PortError>;

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
    posts.sort_by(|a, b| b.created_at_ms.cmp(&a.created_at_ms).then(b.id.cmp(&a.id)));
    posts
}

fn newest_first_flashes(mut flashes: Vec<Flash>) -> Vec<Flash> {
    flashes.sort_by(|a, b| b.created_at_ms.cmp(&a.created_at_ms).then(b.id.cmp(&a.id)));
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
        out.sort_by(|a, b| b.created_at_ms.cmp(&a.created_at_ms).then(b.id.cmp(&a.id)));
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Application ops.
// ---------------------------------------------------------------------------
