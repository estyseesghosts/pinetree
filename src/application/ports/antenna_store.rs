use super::job_queue::NewJob;
use super::note_store::TimelineCursor;
use crate::domain::id::PortError;

/// Antennas (Batch N WS4; guide WP15): rules + per-antenna feeds.
/// `create`/`update` take the moderation-style log? No — upstream publishes
/// `antennaCreated/Updated/Deleted` broadcast events, not moderation-log
/// rows. Pinetree publishes the `antenna` stream event post-commit through
/// `StreamingSink` (no moderation rows; admins need no log here either).
pub trait AntennaStore: Send + Sync {
    fn create_antenna(
        &self,
        antenna: crate::domain::antenna::Antenna,
        jobs: Vec<NewJob>,
    ) -> Result<crate::domain::antenna::Antenna, PortError>;
    fn update_antenna(
        &self,
        antenna: crate::domain::antenna::Antenna,
        jobs: Vec<NewJob>,
    ) -> Result<crate::domain::antenna::Antenna, PortError>;
    fn delete_antenna(&self, owner_id: &str, id: &str) -> Result<bool, PortError>;
    fn get_antenna(
        &self,
        owner_id: &str,
        id: &str,
    ) -> Result<Option<crate::domain::antenna::Antenna>, PortError>;
    /// Owner's antennas, newest-first.
    fn antennas_by_owner(
        &self,
        owner_id: &str,
    ) -> Result<Vec<crate::domain::antenna::Antenna>, PortError>;
    /// Every active antenna (match sweep source; worker pages in memory —
    /// bounded by the per-instance antenna count).
    fn all_active_antennas(&self) -> Result<Vec<crate::domain::antenna::Antenna>, PortError>;
    /// Touch `is_active` + `last_used_at_ms` (read-path reactivation,
    /// upstream `notes.ts` parity).
    fn touch_antenna(&self, owner_id: &str, id: &str, now_ms: i64) -> Result<(), PortError>;
}

/// Per-antenna feeds: `(antenna_id, note_id)` rows, newest-first reads,
/// trimmed to [`ANTENNA_NOTES_CAP`] on append (same transaction).
pub trait AntennaNoteStore: Send + Sync {
    /// Append `(antenna, note)` (deduplicated by key) and trim the feed to
    /// the newest [`ANTENNA_NOTES_CAP`] entries — one transaction.
    fn append_antenna_note(&self, antenna_id: &str, note_id: &str) -> Result<(), PortError>;
    /// Remove one entry (idempotent).
    fn remove_antenna_note(&self, antenna_id: &str, note_id: &str) -> Result<(), PortError>;
    /// Remove every entry of one antenna (delete cascade).
    fn delete_antenna_notes(&self, antenna_id: &str) -> Result<(), PortError>;
    /// Newest-first note ids of one antenna, cursor-paged.
    fn antenna_notes(
        &self,
        antenna_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<String>, PortError>;
}
