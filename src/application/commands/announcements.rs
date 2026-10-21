//! `announcements*` + `i/read-announcement` + `promo/read` ops (WP2-part-B).
//!
//! Ships `announcements` (list with per-viewer `isRead`), `announcements/show`,
//! `i/read-announcement` (marks one announcement read; the badge clears when
//! no unread remains), and `promo/read` (marks the promo read).
//!
//! Domain-grade entities live in the `domain` section below (pure, no
//! HTTP/DB — parent may relocate them to `src/domain/announcement.rs`
//! verbatim). Error contract: unknown announcement →
//! `NotFound("NO_SUCH_ANNOUNCEMENT")` ("No such announcement.", upstream
//! verbatim code/message, pinetree-fixed UUID).
//!
//! Announcements are created by the admin family (`admin/announcements/*`,
//! another WP2 workstream); [`AppServices::announcement_create`] is the
//! admin-path seam exposed here so the surface is testable end to end.
//! Reads are idempotent. No notifications or streaming events are emitted
//! (`announcementCreated` is a WP4 streaming item).

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use super::AppServices;
use crate::application::ports::{NewJob, PortError};

// ---------------------------------------------------------------------------
// Domain (pure; parent may move to src/domain/announcement.rs verbatim).
// ---------------------------------------------------------------------------

/// Announcement title bound (pinetree-fixed).
pub const ANNOUNCEMENT_TITLE_MAX: usize = 128;
/// Announcement text bound (pinetree-fixed).
pub const ANNOUNCEMENT_TEXT_MAX: usize = 8192;

/// One announcement (upstream `MiAnnouncement` local columns).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Announcement {
    pub id: String,
    pub title: String,
    pub text: String,
    pub image_url: Option<String>,
    pub show_popup: bool,
    pub is_good_news: bool,
    pub created_at_ms: i64,
    pub updated_at_ms: Option<i64>,
}

/// Announcement + per-viewer read flag (upstream list/show shape).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnouncementView {
    pub announcement: Announcement,
    pub is_read: bool,
}

pub fn validate_announcement(title: &str, text: &str) -> Result<(), PortError> {
    let title = title.trim();
    if title.is_empty() || title.chars().count() > ANNOUNCEMENT_TITLE_MAX {
        return Err(PortError::Validation(format!(
            "announcement title must be 1..={ANNOUNCEMENT_TITLE_MAX} chars"
        )));
    }
    if text.is_empty() || text.chars().count() > ANNOUNCEMENT_TEXT_MAX {
        return Err(PortError::Validation(format!(
            "announcement text must be 1..={ANNOUNCEMENT_TEXT_MAX} chars"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Storage seam (parent promotes to ports.rs / redb).
// ---------------------------------------------------------------------------

/// Announcement storage seam. Mutating calls commit state + `jobs`
/// atomically. Read marks are per `(announcement, user)` rows; the promo
/// flag is one row per user.
pub trait AnnouncementStore: Send + Sync {
    fn insert_announcement(
        &self,
        announcement: Announcement,
        jobs: Vec<NewJob>,
    ) -> Result<Announcement, PortError>;
    fn get_announcement(&self, announcement_id: &str) -> Result<Option<Announcement>, PortError>;
    /// All announcements, newest-first.
    fn all_announcements(&self) -> Result<Vec<Announcement>, PortError>;
    /// Mark one announcement read (idempotent). Returns the announcement, or
    /// `None` when unknown.
    fn mark_announcement_read(
        &self,
        announcement_id: &str,
        user_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Announcement>, PortError>;
    /// Announcement ids read by `user_id`.
    fn read_announcement_ids(&self, user_id: &str) -> Result<HashSet<String>, PortError>;
    /// Mark the promo read for `user_id` (idempotent). Returns true when
    /// this call newly marked it.
    fn mark_promo_read(&self, user_id: &str, jobs: Vec<NewJob>) -> Result<bool, PortError>;
    /// True when `user_id` already read the promo.
    fn promo_is_read(&self, user_id: &str) -> Result<bool, PortError>;
}

#[derive(Debug, Default)]
struct MemAnnouncementState {
    announcements: HashMap<String, Announcement>,
    reads: HashSet<(String, String)>,
    promo_reads: HashSet<String>,
    outbox: Vec<NewJob>,
}

/// Atomic in-memory [`AnnouncementStore`] (single lock = one transaction).
#[derive(Debug, Default)]
pub struct MemAnnouncementStore {
    state: RwLock<MemAnnouncementState>,
}

impl MemAnnouncementStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Jobs committed alongside state (test drain / parent worker seam).
    pub fn drain_jobs(&self) -> Vec<NewJob> {
        self.state
            .write()
            .expect("mem announcement store poisoned")
            .outbox
            .drain(..)
            .collect()
    }

    fn lock(&self) -> std::sync::RwLockWriteGuard<'_, MemAnnouncementState> {
        self.state.write().expect("mem announcement store poisoned")
    }
}

impl AnnouncementStore for MemAnnouncementStore {
    fn insert_announcement(
        &self,
        announcement: Announcement,
        jobs: Vec<NewJob>,
    ) -> Result<Announcement, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        st.announcements
            .insert(announcement.id.clone(), announcement.clone());
        st.outbox.extend(jobs);
        Ok(announcement)
    }

    fn get_announcement(&self, announcement_id: &str) -> Result<Option<Announcement>, PortError> {
        Ok(self
            .state
            .read()
            .expect("mem announcement store poisoned")
            .announcements
            .get(announcement_id)
            .cloned())
    }

    fn all_announcements(&self) -> Result<Vec<Announcement>, PortError> {
        let st = self.state.read().expect("mem announcement store poisoned");
        let mut out: Vec<Announcement> = st.announcements.values().cloned().collect();
        out.sort_by(|a, b| {
            b.created_at_ms
                .cmp(&a.created_at_ms)
                .then(b.id.cmp(&a.id))
        });
        Ok(out)
    }

    fn mark_announcement_read(
        &self,
        announcement_id: &str,
        user_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<Option<Announcement>, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        let announcement = match st.announcements.get(announcement_id).cloned() {
            Some(a) => a,
            None => return Ok(None),
        };
        if st
            .reads
            .insert((announcement_id.to_string(), user_id.to_string()))
        {
            st.outbox.extend(jobs);
        }
        Ok(Some(announcement))
    }

    fn read_announcement_ids(&self, user_id: &str) -> Result<HashSet<String>, PortError> {
        Ok(self
            .state
            .read()
            .expect("mem announcement store poisoned")
            .reads
            .iter()
            .filter(|(_, uid)| uid == user_id)
            .map(|(aid, _)| aid.clone())
            .collect())
    }

    fn mark_promo_read(&self, user_id: &str, jobs: Vec<NewJob>) -> Result<bool, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        if !st.promo_reads.insert(user_id.to_string()) {
            return Ok(false);
        }
        st.outbox.extend(jobs);
        Ok(true)
    }

    fn promo_is_read(&self, user_id: &str) -> Result<bool, PortError> {
        Ok(self
            .state
            .read()
            .expect("mem announcement store poisoned")
            .promo_reads
            .contains(user_id))
    }
}

// ---------------------------------------------------------------------------
// Application ops.
// ---------------------------------------------------------------------------

impl AppServices {
    /// Admin-path seam (`admin/announcements/create`, dispatcher owned by
    /// the admin WP2 workstream).
    pub fn announcement_create(
        &self,
        store: &dyn AnnouncementStore,
        title: &str,
        text: &str,
        image_url: Option<&str>,
        show_popup: bool,
        is_good_news: bool,
    ) -> Result<Announcement, PortError> {
        validate_announcement(title, text)?;
        let now = self.clock.now_ms();
        store.insert_announcement(
            Announcement {
                id: self.ids.generate(),
                title: title.trim().to_string(),
                text: text.to_string(),
                image_url: image_url.map(str::to_string),
                show_popup,
                is_good_news,
                created_at_ms: now,
                updated_at_ms: None,
            },
            Vec::new(),
        )
    }

    /// `announcements` (newest-first with per-viewer `isRead`).
    pub fn announcements_list(
        &self,
        store: &dyn AnnouncementStore,
        viewer_id: Option<&str>,
    ) -> Result<Vec<AnnouncementView>, PortError> {
        let read: HashSet<String> = match viewer_id {
            Some(viewer) => store.read_announcement_ids(viewer)?,
            None => HashSet::new(),
        };
        Ok(store
            .all_announcements()?
            .into_iter()
            .map(|announcement| {
                let is_read = read.contains(&announcement.id);
                AnnouncementView {
                    announcement,
                    is_read,
                }
            })
            .collect())
    }

    /// `announcements/show`.
    pub fn announcement_show(
        &self,
        store: &dyn AnnouncementStore,
        announcement_id: &str,
        viewer_id: Option<&str>,
    ) -> Result<AnnouncementView, PortError> {
        let announcement = store
            .get_announcement(announcement_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_ANNOUNCEMENT".to_string()))?;
        let is_read = match viewer_id {
            Some(viewer) => store.read_announcement_ids(viewer)?.contains(announcement_id),
            None => false,
        };
        Ok(AnnouncementView {
            announcement,
            is_read,
        })
    }

    /// `i/read-announcement` (idempotent).
    pub fn announcement_read(
        &self,
        store: &dyn AnnouncementStore,
        user_id: &str,
        announcement_id: &str,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        let found = store.mark_announcement_read(announcement_id, user_id, Vec::new())?;
        if found.is_none() {
            return Err(PortError::NotFound("NO_SUCH_ANNOUNCEMENT".to_string()));
        }
        Ok(())
    }

    /// Unread-announcement count (backs the badge; parent wires it into the
    /// MeDetailed pack alongside `promo` state).
    pub fn announcements_unread_count(
        &self,
        store: &dyn AnnouncementStore,
        user_id: &str,
    ) -> Result<u64, PortError> {
        let read = store.read_announcement_ids(user_id)?;
        Ok(store
            .all_announcements()?
            .iter()
            .filter(|a| !read.contains(&a.id))
            .count() as u64)
    }

    /// `promo/read` (idempotent; no params upstream).
    pub fn promo_read(
        &self,
        store: &dyn AnnouncementStore,
        user_id: &str,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        store.mark_promo_read(user_id, Vec::new())?;
        Ok(())
    }
}
