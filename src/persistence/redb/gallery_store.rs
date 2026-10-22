use super::codec::{decode_json, encode_json, map_redb_err};
use super::store::RedbStore;
use crate::application::commands::gallery::Flash;
use crate::application::commands::gallery::FlashLike;
use crate::application::commands::gallery::GalleryLike;
use crate::application::commands::gallery::GalleryPost;
use crate::application::commands::gallery::GalleryStore;
use crate::application::ports::{NewJob, PortError};
use redb::{ReadableDatabase, ReadableTable, TableDefinition};

/// Gallery posts by id (gaps WP2b). Values are JSON
/// [`GalleryPost`](crate::application::commands::gallery::GalleryPost) rows.
pub(crate) const GALLERY_POSTS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("gallery_posts");
/// Gallery post likes `(post_id, user_id)` → JSON
/// [`GalleryLike`](crate::application::commands::gallery::GalleryLike) row.
pub(crate) const GALLERY_POST_LIKES: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("gallery_post_likes");
/// Play pages (flash) by id (gaps WP2b). Values are JSON
/// [`Flash`](crate::application::commands::gallery::Flash) rows.
pub(crate) const FLASHES: TableDefinition<&str, &[u8]> = TableDefinition::new("flashes");
/// Flash likes `(flash_id, user_id)` → JSON
/// [`FlashLike`](crate::application::commands::gallery::FlashLike) row.
pub(crate) const FLASH_LIKES: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("flash_likes");
impl GalleryStore for RedbStore {
    fn insert_post(&self, post: GalleryPost, jobs: Vec<NewJob>) -> Result<GalleryPost, PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(GALLERY_POSTS).map_err(map_redb_err)?;
            let encoded = encode_json(&post)?;
            table
                .insert(post.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(post)
    }

    fn update_post(&self, post: GalleryPost, jobs: Vec<NewJob>) -> Result<GalleryPost, PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(GALLERY_POSTS).map_err(map_redb_err)?;
            if table.get(post.id.as_str()).map_err(map_redb_err)?.is_none() {
                drop(table);
                let _ = write_txn.abort();
                return Err(PortError::NotFound("NO_SUCH_POST".to_string()));
            }
            let encoded = encode_json(&post)?;
            table
                .insert(post.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(post)
    }

    fn delete_post(&self, post_id: &str, jobs: Vec<NewJob>) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        let existed = {
            let mut posts = write_txn.open_table(GALLERY_POSTS).map_err(map_redb_err)?;
            let existed = posts.remove(post_id).map_err(map_redb_err)?.is_some();
            if existed {
                let mut likes = write_txn
                    .open_table(GALLERY_POST_LIKES)
                    .map_err(map_redb_err)?;
                let keys: Vec<(String, String)> = likes
                    .range((post_id, "")..=(post_id, "\u{10ffff}"))
                    .map_err(map_redb_err)?
                    .map(|entry| {
                        let (k, _) = entry.map_err(map_redb_err)?;
                        Ok::<(String, String), PortError>((
                            k.value().0.to_string(),
                            k.value().1.to_string(),
                        ))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                for key in keys {
                    likes
                        .remove((key.0.as_str(), key.1.as_str()))
                        .map_err(map_redb_err)?;
                }
            }
            existed
        };
        if existed {
            self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        }
        self.commit_write(write_txn)?;
        Ok(existed)
    }

    fn get_post(&self, post_id: &str) -> Result<Option<GalleryPost>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(GALLERY_POSTS).map_err(map_redb_err)?;
        match table.get(post_id).map_err(map_redb_err)? {
            Some(value) => Ok(Some(decode_json(value.value())?)),
            None => Ok(None),
        }
    }

    fn all_posts(&self) -> Result<Vec<GalleryPost>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(GALLERY_POSTS).map_err(map_redb_err)?;
        let mut out: Vec<GalleryPost> = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (_, value) = entry.map_err(map_redb_err)?;
            out.push(decode_json(value.value())?);
        }
        out.sort_by(|a: &GalleryPost, b: &GalleryPost| {
            b.created_at_ms.cmp(&a.created_at_ms).then(b.id.cmp(&a.id))
        });
        Ok(out)
    }

    fn posts_by_owner(&self, owner_id: &str) -> Result<Vec<GalleryPost>, PortError> {
        let mut out: Vec<GalleryPost> = self
            .all_posts()?
            .into_iter()
            .filter(|post| post.owner_id == owner_id)
            .collect();
        out.sort_by(|a: &GalleryPost, b: &GalleryPost| {
            b.created_at_ms.cmp(&a.created_at_ms).then(b.id.cmp(&a.id))
        });
        Ok(out)
    }

    fn like_post(&self, like: GalleryLike, jobs: Vec<NewJob>) -> Result<GalleryLike, PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut posts = write_txn.open_table(GALLERY_POSTS).map_err(map_redb_err)?;
            let raw: Option<Vec<u8>> = posts
                .get(like.post_id.as_str())
                .map_err(map_redb_err)?
                .map(|guard| guard.value().to_vec());
            let mut post: GalleryPost = match raw {
                Some(bytes) => decode_json(&bytes)?,
                None => {
                    drop(posts);
                    let _ = write_txn.abort();
                    return Err(PortError::NotFound("NO_SUCH_POST".to_string()));
                }
            };
            let mut likes = write_txn
                .open_table(GALLERY_POST_LIKES)
                .map_err(map_redb_err)?;
            if likes
                .get((like.post_id.as_str(), like.user_id.as_str()))
                .map_err(map_redb_err)?
                .is_some()
            {
                drop(likes);
                drop(posts);
                let _ = write_txn.abort();
                return Err(PortError::Conflict("ALREADY_LIKED".to_string()));
            }
            let encoded = encode_json(&like)?;
            likes
                .insert(
                    (like.post_id.as_str(), like.user_id.as_str()),
                    encoded.as_slice(),
                )
                .map_err(map_redb_err)?;
            post.liked_count = post.liked_count.saturating_add(1);
            let encoded_post = encode_json(&post)?;
            posts
                .insert(post.id.as_str(), encoded_post.as_slice())
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(like)
    }

    fn unlike_post(
        &self,
        post_id: &str,
        user_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut likes = write_txn
                .open_table(GALLERY_POST_LIKES)
                .map_err(map_redb_err)?;
            if likes
                .remove((post_id, user_id))
                .map_err(map_redb_err)?
                .is_none()
            {
                drop(likes);
                let _ = write_txn.abort();
                return Err(PortError::Conflict("NOT_LIKED".to_string()));
            }
            let mut posts = write_txn.open_table(GALLERY_POSTS).map_err(map_redb_err)?;
            let raw: Option<Vec<u8>> = posts
                .get(post_id)
                .map_err(map_redb_err)?
                .map(|guard| guard.value().to_vec());
            if let Some(bytes) = raw {
                let mut post: GalleryPost = decode_json(&bytes)?;
                post.liked_count = post.liked_count.saturating_sub(1);
                let encoded = encode_json(&post)?;
                posts
                    .insert(post.id.as_str(), encoded.as_slice())
                    .map_err(map_redb_err)?;
            }
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(())
    }

    fn insert_flash(&self, flash: Flash, jobs: Vec<NewJob>) -> Result<Flash, PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(FLASHES).map_err(map_redb_err)?;
            let encoded = encode_json(&flash)?;
            table
                .insert(flash.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(flash)
    }

    fn update_flash(&self, flash: Flash, jobs: Vec<NewJob>) -> Result<Flash, PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(FLASHES).map_err(map_redb_err)?;
            if table
                .get(flash.id.as_str())
                .map_err(map_redb_err)?
                .is_none()
            {
                drop(table);
                let _ = write_txn.abort();
                return Err(PortError::NotFound("NO_SUCH_FLASH".to_string()));
            }
            let encoded = encode_json(&flash)?;
            table
                .insert(flash.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(flash)
    }

    fn delete_flash(&self, flash_id: &str, jobs: Vec<NewJob>) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        let existed = {
            let mut flashes = write_txn.open_table(FLASHES).map_err(map_redb_err)?;
            let existed = flashes.remove(flash_id).map_err(map_redb_err)?.is_some();
            if existed {
                let mut likes = write_txn.open_table(FLASH_LIKES).map_err(map_redb_err)?;
                let keys: Vec<(String, String)> = likes
                    .range((flash_id, "")..=(flash_id, "\u{10ffff}"))
                    .map_err(map_redb_err)?
                    .map(|entry| {
                        let (k, _) = entry.map_err(map_redb_err)?;
                        Ok::<(String, String), PortError>((
                            k.value().0.to_string(),
                            k.value().1.to_string(),
                        ))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                for key in keys {
                    likes
                        .remove((key.0.as_str(), key.1.as_str()))
                        .map_err(map_redb_err)?;
                }
            }
            existed
        };
        if existed {
            self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        }
        self.commit_write(write_txn)?;
        Ok(existed)
    }

    fn get_flash(&self, flash_id: &str) -> Result<Option<Flash>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(FLASHES).map_err(map_redb_err)?;
        match table.get(flash_id).map_err(map_redb_err)? {
            Some(value) => Ok(Some(decode_json(value.value())?)),
            None => Ok(None),
        }
    }

    fn all_flashes(&self) -> Result<Vec<Flash>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(FLASHES).map_err(map_redb_err)?;
        let mut out: Vec<Flash> = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (_, value) = entry.map_err(map_redb_err)?;
            out.push(decode_json(value.value())?);
        }
        out.sort_by(|a: &Flash, b: &Flash| {
            b.created_at_ms.cmp(&a.created_at_ms).then(b.id.cmp(&a.id))
        });
        Ok(out)
    }

    fn flashes_by_owner(&self, owner_id: &str) -> Result<Vec<Flash>, PortError> {
        let mut out: Vec<Flash> = self
            .all_flashes()?
            .into_iter()
            .filter(|flash| flash.owner_id == owner_id)
            .collect();
        out.sort_by(|a: &Flash, b: &Flash| {
            b.created_at_ms.cmp(&a.created_at_ms).then(b.id.cmp(&a.id))
        });
        Ok(out)
    }

    fn like_flash(&self, like: FlashLike, jobs: Vec<NewJob>) -> Result<FlashLike, PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut flashes = write_txn.open_table(FLASHES).map_err(map_redb_err)?;
            let raw: Option<Vec<u8>> = flashes
                .get(like.flash_id.as_str())
                .map_err(map_redb_err)?
                .map(|guard| guard.value().to_vec());
            let mut flash: Flash = match raw {
                Some(bytes) => decode_json(&bytes)?,
                None => {
                    drop(flashes);
                    let _ = write_txn.abort();
                    return Err(PortError::NotFound("NO_SUCH_FLASH".to_string()));
                }
            };
            let mut likes = write_txn.open_table(FLASH_LIKES).map_err(map_redb_err)?;
            if likes
                .get((like.flash_id.as_str(), like.user_id.as_str()))
                .map_err(map_redb_err)?
                .is_some()
            {
                drop(likes);
                drop(flashes);
                let _ = write_txn.abort();
                return Err(PortError::Conflict("ALREADY_LIKED".to_string()));
            }
            let encoded = encode_json(&like)?;
            likes
                .insert(
                    (like.flash_id.as_str(), like.user_id.as_str()),
                    encoded.as_slice(),
                )
                .map_err(map_redb_err)?;
            flash.liked_count = flash.liked_count.saturating_add(1);
            let encoded_flash = encode_json(&flash)?;
            flashes
                .insert(flash.id.as_str(), encoded_flash.as_slice())
                .map_err(map_redb_err)?;
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(like)
    }

    fn unlike_flash(
        &self,
        flash_id: &str,
        user_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<(), PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut likes = write_txn.open_table(FLASH_LIKES).map_err(map_redb_err)?;
            if likes
                .remove((flash_id, user_id))
                .map_err(map_redb_err)?
                .is_none()
            {
                drop(likes);
                let _ = write_txn.abort();
                return Err(PortError::Conflict("NOT_LIKED".to_string()));
            }
            let mut flashes = write_txn.open_table(FLASHES).map_err(map_redb_err)?;
            let raw: Option<Vec<u8>> = flashes
                .get(flash_id)
                .map_err(map_redb_err)?
                .map(|guard| guard.value().to_vec());
            if let Some(bytes) = raw {
                let mut flash: Flash = decode_json(&bytes)?;
                flash.liked_count = flash.liked_count.saturating_sub(1);
                let encoded = encode_json(&flash)?;
                flashes
                    .insert(flash.id.as_str(), encoded.as_slice())
                    .map_err(map_redb_err)?;
            }
        }
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        self.commit_write(write_txn)?;
        Ok(())
    }

    fn flash_likes_by_user(&self, user_id: &str) -> Result<Vec<FlashLike>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(FLASH_LIKES).map_err(map_redb_err)?;
        let mut out: Vec<FlashLike> = Vec::new();
        for entry in table.iter().map_err(map_redb_err)? {
            let (key, value) = entry.map_err(map_redb_err)?;
            if key.value().1 == user_id {
                out.push(decode_json(value.value())?);
            }
        }
        out.sort_by(|a: &FlashLike, b: &FlashLike| {
            b.created_at_ms.cmp(&a.created_at_ms).then(b.id.cmp(&a.id))
        });
        Ok(out)
    }
}
