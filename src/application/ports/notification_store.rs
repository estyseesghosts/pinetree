use super::note_store::TimelineCursor;
use crate::domain::id::PortError;
use crate::domain::notification::{Notification, NotificationKind};

pub trait NotificationStore: Send + Sync {
    fn insert_notification(&self, notification: Notification) -> Result<Notification, PortError>;
    fn list_notifications(
        &self,
        notifiee_id: &str,
        limit: usize,
    ) -> Result<Vec<Notification>, PortError>;
    /// Mark every notification of `notifiee_id` read. Returns the count
    /// flipped. Used by `notifications/mark-all-as-read` and the streaming
    /// `readNotification` message (Phase 3).
    fn mark_all_read(&self, notifiee_id: &str) -> Result<u64, PortError>;
    /// Newest-first page of `notifiee_id`'s notifications filtered by
    /// `cursor` (ids + dates, limit) and kind allow/deny lists (Phase 3
    /// `i/notifications`). An empty `include` matches every kind; kinds in
    /// `exclude` never match.
    fn list_notifications_filtered(
        &self,
        notifiee_id: &str,
        cursor: &TimelineCursor,
        include: &[NotificationKind],
        exclude: &[NotificationKind],
    ) -> Result<Vec<Notification>, PortError>;
}
