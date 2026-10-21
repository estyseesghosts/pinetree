//! `channels/*` application ops (WP2-part-B).
//!
//! Ships the REST surfaces so clients stop 404ing: create / update / delete /
//! show / follow / unfollow / followed / owned / featured / search /
//! timeline / favorite / unfavorite / my-favorites (14 endpoints; "x16" in
//! the plan counts the same family — no other `channels/*` endpoints exist
//! upstream).
//!
//! Error-code contract (Misskey verbatim codes/messages; UUIDs are
//! pinetree-fixed until the parent copies them from the misskey checkout):
//! - unknown channel → `NotFound("NO_SUCH_CHANNEL")` ("No such channel.");
//! - non-owner update/delete → `Validation("ACCESS_DENIED")`.
//! Follow/unfollow/favorite/unfavorite are idempotent (no `ALREADY_*`
//! errors).
//!
//! `channels/timeline` returns `[]` for existing channels: notes carry no
//! channel column (`notes/create` forces `channelId` null /
//! `NO_SUCH_CHANNEL` for non-null), so no note can belong to a channel yet.
//! Wiring channel posting is parent work (ports + redb + dispatcher); the
//! read seam (`channel_timeline`) already exists.
//!
//! Storage: [`ChannelStore`] is the seam the parent promotes to
//! `ports.rs`/`redb`; [`MemChannelStore`] is the atomic in-memory backend
//! (one lock = state + jobs commit). No streaming events are emitted:
//! the `channel` streaming channel stays `UNSUPPORTED_CHANNEL` (WP4).

use std::collections::HashMap;
use std::sync::RwLock;

use super::AppServices;
use crate::application::ports::{NewJob, PortError};
use crate::domain::channel::{Channel, ChannelFavorite, ChannelFollow};

// ---------------------------------------------------------------------------
// Storage seam (parent promotes to ports.rs / redb).
// ---------------------------------------------------------------------------

/// Channel storage seam. Every mutating call commits state + `jobs`
/// atomically (one transaction on durable backends).
pub trait ChannelStore: Send + Sync {
    fn insert_channel(&self, channel: Channel, jobs: Vec<NewJob>) -> Result<Channel, PortError>;
    fn update_channel(&self, channel: Channel, jobs: Vec<NewJob>) -> Result<Channel, PortError>;
    /// Delete + cascade follows/favorites. Returns true when a row existed.
    fn delete_channel(&self, channel_id: &str, jobs: Vec<NewJob>) -> Result<bool, PortError>;
    fn get_channel(&self, channel_id: &str) -> Result<Option<Channel>, PortError>;
    /// All channels, newest-first.
    fn all_channels(&self) -> Result<Vec<Channel>, PortError>;
    /// Channels owned by `owner_id`, newest-first.
    fn channels_by_owner(&self, owner_id: &str) -> Result<Vec<Channel>, PortError>;

    /// Follow (idempotent insert). Returns true when newly added.
    fn follow_channel(&self, follow: ChannelFollow, jobs: Vec<NewJob>) -> Result<bool, PortError>;
    /// Unfollow (idempotent). Returns true when a row existed.
    fn unfollow_channel(
        &self,
        channel_id: &str,
        follower_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError>;
    fn is_following_channel(&self, channel_id: &str, follower_id: &str) -> Result<bool, PortError>;
    /// Newest-first follows of one user.
    fn followed_channels(&self, follower_id: &str) -> Result<Vec<ChannelFollow>, PortError>;
    /// Follower count for one channel.
    fn channel_follower_count(&self, channel_id: &str) -> Result<u64, PortError>;

    /// Favorite (idempotent insert). Returns true when newly added.
    fn favorite_channel(
        &self,
        favorite: ChannelFavorite,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError>;
    /// Unfavorite (idempotent). Returns true when a row existed.
    fn unfavorite_channel(
        &self,
        channel_id: &str,
        user_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError>;
    /// Newest-first favorites of one user.
    fn favorite_channels(&self, user_id: &str) -> Result<Vec<ChannelFavorite>, PortError>;
}

#[derive(Debug, Default)]
struct MemChannelState {
    channels: HashMap<String, Channel>,
    follows: HashMap<(String, String), ChannelFollow>,
    favorites: HashMap<(String, String), ChannelFavorite>,
    outbox: Vec<NewJob>,
}

/// Atomic in-memory [`ChannelStore`] (single lock = one transaction).
#[derive(Debug, Default)]
pub struct MemChannelStore {
    state: RwLock<MemChannelState>,
}

impl MemChannelStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Jobs committed alongside state (test drain / parent worker seam).
    pub fn drain_jobs(&self) -> Vec<NewJob> {
        self.state
            .write()
            .expect("mem channel store poisoned")
            .outbox
            .drain(..)
            .collect()
    }

    fn lock(
        &self,
    ) -> std::sync::RwLockWriteGuard<'_, MemChannelState> {
        self.state.write().expect("mem channel store poisoned")
    }
}

impl ChannelStore for MemChannelStore {
    fn insert_channel(&self, channel: Channel, jobs: Vec<NewJob>) -> Result<Channel, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        st.channels.insert(channel.id.clone(), channel.clone());
        st.outbox.extend(jobs);
        Ok(channel)
    }

    fn update_channel(&self, channel: Channel, jobs: Vec<NewJob>) -> Result<Channel, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        if !st.channels.contains_key(&channel.id) {
            return Err(PortError::NotFound("NO_SUCH_CHANNEL".to_string()));
        }
        st.channels.insert(channel.id.clone(), channel.clone());
        st.outbox.extend(jobs);
        Ok(channel)
    }

    fn delete_channel(&self, channel_id: &str, jobs: Vec<NewJob>) -> Result<bool, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        if st.channels.remove(channel_id).is_none() {
            return Ok(false);
        }
        st.follows.retain(|(cid, _), _| cid != channel_id);
        st.favorites.retain(|(cid, _), _| cid != channel_id);
        st.outbox.extend(jobs);
        Ok(true)
    }

    fn get_channel(&self, channel_id: &str) -> Result<Option<Channel>, PortError> {
        Ok(self
            .state
            .read()
            .expect("mem channel store poisoned")
            .channels
            .get(channel_id)
            .cloned())
    }

    fn all_channels(&self) -> Result<Vec<Channel>, PortError> {
        let st = self.state.read().expect("mem channel store poisoned");
        let mut out: Vec<Channel> = st.channels.values().cloned().collect();
        out.sort_by(|a, b| b.created_at_ms.cmp(&a.created_at_ms).then(b.id.cmp(&a.id)));
        Ok(out)
    }

    fn channels_by_owner(&self, owner_id: &str) -> Result<Vec<Channel>, PortError> {
        let st = self.state.read().expect("mem channel store poisoned");
        let mut out: Vec<Channel> = st
            .channels
            .values()
            .filter(|c| c.owner_id.as_deref() == Some(owner_id))
            .cloned()
            .collect();
        out.sort_by(|a, b| b.created_at_ms.cmp(&a.created_at_ms).then(b.id.cmp(&a.id)));
        Ok(out)
    }

    fn follow_channel(&self, follow: ChannelFollow, jobs: Vec<NewJob>) -> Result<bool, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        let key = (follow.channel_id.clone(), follow.follower_id.clone());
        if st.follows.contains_key(&key) {
            return Ok(false);
        }
        st.follows.insert(key, follow);
        st.outbox.extend(jobs);
        Ok(true)
    }

    fn unfollow_channel(
        &self,
        channel_id: &str,
        follower_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        let existed = st
            .follows
            .remove(&(channel_id.to_string(), follower_id.to_string()))
            .is_some();
        if existed {
            st.outbox.extend(jobs);
        }
        Ok(existed)
    }

    fn is_following_channel(&self, channel_id: &str, follower_id: &str) -> Result<bool, PortError> {
        Ok(self
            .state
            .read()
            .expect("mem channel store poisoned")
            .follows
            .contains_key(&(channel_id.to_string(), follower_id.to_string())))
    }

    fn followed_channels(&self, follower_id: &str) -> Result<Vec<ChannelFollow>, PortError> {
        let st = self.state.read().expect("mem channel store poisoned");
        let mut out: Vec<ChannelFollow> = st
            .follows
            .values()
            .filter(|f| f.follower_id == follower_id)
            .cloned()
            .collect();
        out.sort_by(|a, b| b.created_at_ms.cmp(&a.created_at_ms).then(b.id.cmp(&a.id)));
        Ok(out)
    }

    fn channel_follower_count(&self, channel_id: &str) -> Result<u64, PortError> {
        Ok(self
            .state
            .read()
            .expect("mem channel store poisoned")
            .follows
            .keys()
            .filter(|(cid, _)| cid == channel_id)
            .count() as u64)
    }

    fn favorite_channel(
        &self,
        favorite: ChannelFavorite,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        let key = (favorite.channel_id.clone(), favorite.user_id.clone());
        if st.favorites.contains_key(&key) {
            return Ok(false);
        }
        st.favorites.insert(key, favorite);
        st.outbox.extend(jobs);
        Ok(true)
    }

    fn unfavorite_channel(
        &self,
        channel_id: &str,
        user_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        let existed = st
            .favorites
            .remove(&(channel_id.to_string(), user_id.to_string()))
            .is_some();
        if existed {
            st.outbox.extend(jobs);
        }
        Ok(existed)
    }

    fn favorite_channels(&self, user_id: &str) -> Result<Vec<ChannelFavorite>, PortError> {
        let st = self.state.read().expect("mem channel store poisoned");
        let mut out: Vec<ChannelFavorite> = st
            .favorites
            .values()
            .filter(|f| f.user_id == user_id)
            .cloned()
            .collect();
        out.sort_by(|a, b| b.created_at_ms.cmp(&a.created_at_ms).then(b.id.cmp(&a.id)));
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Application ops.
// ---------------------------------------------------------------------------

/// Simple id cursor (`since_id` exclusive lower bound, `until_id` exclusive
/// upper bound, newest-first, `limit` clamped to 1..=100).
#[derive(Debug, Clone, Default)]
pub struct IdCursor {
    pub since_id: Option<String>,
    pub until_id: Option<String>,
    pub limit: usize,
}

impl IdCursor {
    pub fn with_limit(limit: usize) -> Self {
        Self {
            since_id: None,
            until_id: None,
            limit: limit.clamp(1, 100),
        }
    }
}

fn apply_id_cursor<T>(mut items: Vec<T>, cursor: &IdCursor, id_of: impl Fn(&T) -> &str) -> Vec<T> {
    if let Some(since) = cursor.since_id.as_deref() {
        items.retain(|item| id_of(item) > since);
    }
    if let Some(until) = cursor.until_id.as_deref() {
        items.retain(|item| id_of(item) < until);
    }
    let limit = cursor.limit.clamp(1, 100);
    items.truncate(limit);
    items
}

impl AppServices {
    fn require_channel(
        &self,
        store: &dyn ChannelStore,
        channel_id: &str,
    ) -> Result<Channel, PortError> {
        store
            .get_channel(channel_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_CHANNEL".to_string()))
    }

    /// `channels/create`. The creator owns the channel.
    pub fn channel_create(
        &self,
        store: &dyn ChannelStore,
        owner_id: &str,
        name: &str,
        description: Option<&str>,
        banner_id: Option<&str>,
        color: Option<&str>,
    ) -> Result<Channel, PortError> {
        self.accounts.get_owner(owner_id)?;
        let name = name.trim();
        Channel::validate_name(name)?;
        Channel::validate_description(description)?;
        Channel::validate_color(color)?;
        let now = self.clock.now_ms();
        let channel = Channel {
            id: self.ids.generate(),
            owner_id: Some(owner_id.to_string()),
            name: name.to_string(),
            description: description.map(str::to_string),
            banner_id: banner_id.map(str::to_string),
            color: color.map(str::to_string),
            is_sensitive: false,
            allow_renote_to_external: true,
            created_at_ms: now,
            last_noted_at_ms: Some(now),
        };
        store.insert_channel(channel, Vec::new())
    }

    /// `channels/update` (owner-only).
    pub fn channel_update(
        &self,
        store: &dyn ChannelStore,
        owner_id: &str,
        channel_id: &str,
        name: Option<&str>,
        description: Option<Option<&str>>,
        banner_id: Option<Option<&str>>,
        color: Option<Option<&str>>,
        is_sensitive: Option<bool>,
        allow_renote_to_external: Option<bool>,
    ) -> Result<Channel, PortError> {
        self.accounts.get_owner(owner_id)?;
        let mut channel = self.require_channel(store, channel_id)?;
        if channel.owner_id.as_deref() != Some(owner_id) {
            return Err(PortError::Validation("ACCESS_DENIED".to_string()));
        }
        if let Some(name) = name {
            let name = name.trim();
            Channel::validate_name(name)?;
            channel.name = name.to_string();
        }
        if let Some(description) = description {
            Channel::validate_description(description)?;
            channel.description = description.map(str::to_string);
        }
        if let Some(banner_id) = banner_id {
            channel.banner_id = banner_id.map(str::to_string);
        }
        if let Some(color) = color {
            Channel::validate_color(color.as_deref())?;
            channel.color = color.map(str::to_string);
        }
        if let Some(is_sensitive) = is_sensitive {
            channel.is_sensitive = is_sensitive;
        }
        if let Some(allow) = allow_renote_to_external {
            channel.allow_renote_to_external = allow;
        }
        channel.last_noted_at_ms = Some(self.clock.now_ms());
        store.update_channel(channel, Vec::new())
    }

    /// `channels/delete` (owner-only; cascades follows/favorites).
    pub fn channel_delete(
        &self,
        store: &dyn ChannelStore,
        owner_id: &str,
        channel_id: &str,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(owner_id)?;
        let channel = self.require_channel(store, channel_id)?;
        if channel.owner_id.as_deref() != Some(owner_id) {
            return Err(PortError::Validation("ACCESS_DENIED".to_string()));
        }
        store.delete_channel(channel_id, Vec::new())?;
        Ok(())
    }

    /// `channels/show`.
    pub fn channel_show(
        &self,
        store: &dyn ChannelStore,
        channel_id: &str,
    ) -> Result<Channel, PortError> {
        self.require_channel(store, channel_id)
    }

    /// `channels/follow` (idempotent).
    pub fn channel_follow(
        &self,
        store: &dyn ChannelStore,
        follower_id: &str,
        channel_id: &str,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(follower_id)?;
        self.require_channel(store, channel_id)?;
        let follow = ChannelFollow {
            id: self.ids.generate(),
            channel_id: channel_id.to_string(),
            follower_id: follower_id.to_string(),
            created_at_ms: self.clock.now_ms(),
        };
        store.follow_channel(follow, Vec::new())?;
        Ok(())
    }

    /// `channels/unfollow` (idempotent).
    pub fn channel_unfollow(
        &self,
        store: &dyn ChannelStore,
        follower_id: &str,
        channel_id: &str,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(follower_id)?;
        self.require_channel(store, channel_id)?;
        store.unfollow_channel(channel_id, follower_id, Vec::new())?;
        Ok(())
    }

    /// `channels/followed` (channels one user follows, newest-first).
    pub fn channels_followed(
        &self,
        store: &dyn ChannelStore,
        follower_id: &str,
        cursor: &IdCursor,
    ) -> Result<Vec<Channel>, PortError> {
        let follows = store.followed_channels(follower_id)?;
        let mut out = Vec::new();
        for follow in follows {
            if let Some(channel) = store.get_channel(&follow.channel_id)? {
                out.push(channel);
            }
        }
        Ok(apply_id_cursor(out, cursor, |c| &c.id))
    }

    /// `channels/owned` (channels one user owns, newest-first).
    pub fn channels_owned(
        &self,
        store: &dyn ChannelStore,
        owner_id: &str,
        cursor: &IdCursor,
    ) -> Result<Vec<Channel>, PortError> {
        Ok(apply_id_cursor(
            store.channels_by_owner(owner_id)?,
            cursor,
            |c| &c.id,
        ))
    }

    /// `channels/featured` (most-followed first, bounded by cursor limit).
    pub fn channels_featured(
        &self,
        store: &dyn ChannelStore,
        cursor: &IdCursor,
    ) -> Result<Vec<Channel>, PortError> {
        let mut all = store.all_channels()?;
        let mut counts = Vec::new();
        for channel in all.drain(..) {
            let count = store.channel_follower_count(&channel.id)?;
            counts.push((count, channel));
        }
        counts.sort_by(|a, b| {
            b.0.cmp(&a.0)
                .then(b.1.created_at_ms.cmp(&a.1.created_at_ms))
                .then(b.1.id.cmp(&a.1.id))
        });
        let ordered: Vec<Channel> = counts.into_iter().map(|(_, c)| c).collect();
        Ok(apply_id_cursor(ordered, cursor, |c| &c.id))
    }

    /// `channels/search` (case-insensitive substring on name/description).
    pub fn channels_search(
        &self,
        store: &dyn ChannelStore,
        query: &str,
        cursor: &IdCursor,
    ) -> Result<Vec<Channel>, PortError> {
        let needle = query.to_lowercase();
        let all = store.all_channels()?;
        let matched: Vec<Channel> = all
            .into_iter()
            .filter(|c| {
                c.name.to_lowercase().contains(&needle)
                    || c.description
                        .as_deref()
                        .unwrap_or_default()
                        .to_lowercase()
                        .contains(&needle)
            })
            .collect();
        Ok(apply_id_cursor(matched, cursor, |c| &c.id))
    }

    /// `channels/timeline`. No channel posting surface exists (notes carry
    /// no channel column), so existing channels read as empty. Unknown
    /// channels are `NO_SUCH_CHANNEL`.
    pub fn channel_timeline(
        &self,
        store: &dyn ChannelStore,
        channel_id: &str,
        _cursor: &IdCursor,
    ) -> Result<Vec<crate::domain::note::Note>, PortError> {
        self.require_channel(store, channel_id)?;
        Ok(Vec::new())
    }

    /// `channels/favorite` (idempotent).
    pub fn channel_favorite(
        &self,
        store: &dyn ChannelStore,
        user_id: &str,
        channel_id: &str,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        self.require_channel(store, channel_id)?;
        let favorite = ChannelFavorite {
            id: self.ids.generate(),
            channel_id: channel_id.to_string(),
            user_id: user_id.to_string(),
            created_at_ms: self.clock.now_ms(),
        };
        store.favorite_channel(favorite, Vec::new())?;
        Ok(())
    }

    /// `channels/unfavorite` (idempotent).
    pub fn channel_unfavorite(
        &self,
        store: &dyn ChannelStore,
        user_id: &str,
        channel_id: &str,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        self.require_channel(store, channel_id)?;
        store.unfavorite_channel(channel_id, user_id, Vec::new())?;
        Ok(())
    }

    /// `channels/my-favorites` (newest-first).
    pub fn channels_my_favorites(
        &self,
        store: &dyn ChannelStore,
        user_id: &str,
        cursor: &IdCursor,
    ) -> Result<Vec<Channel>, PortError> {
        let favorites = store.favorite_channels(user_id)?;
        let mut out = Vec::new();
        for favorite in favorites {
            if let Some(channel) = store.get_channel(&favorite.channel_id)? {
                out.push(channel);
            }
        }
        Ok(apply_id_cursor(out, cursor, |c| &c.id))
    }
}
