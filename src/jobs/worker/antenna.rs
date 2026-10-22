use super::JobWorker;
use crate::api::misskey::packers::{pack_antenna, pack_note};
use crate::application::events::{MainEventKind, MainMessage};
use crate::application::ports::{PortError, TimelineCursor};
use crate::domain::antenna::ANTENNA_NOTES_CAP;

impl JobWorker {
    /// Run one `AntennaMatch` job (Batch N WS4): fan one committed note
    /// into matching antenna feeds (+ post-commit stream events), then
    /// push one `unreadAntenna` main event per matched antenna owner.
    /// Gone notes converge quietly; only storage faults retry.
    pub(crate) fn handle_antenna_match(&self, payload: &[u8]) -> Result<(), PortError> {
        use crate::application::ports::AntennaMatchPayload;
        let parsed: AntennaMatchPayload = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("antenna-match payload invalid: {e}")))?;
        self.services.match_antenna_note(&parsed.note_id)?;
        self.push_unread_antennas(&parsed.note_id)
    }

    /// Push one `unreadAntenna` main event per antenna whose feed gained
    /// `note_id` (Misskey `publishMainStream(userId, 'unreadAntenna',
    /// { antenna, note })` parity). Membership is re-read from the feed
    /// (newest-first, one page covers the trimmed feed), so this stays
    /// inside the worker without duplicating the matcher. Bounded: one
    /// feed read per active antenna. A replayed job re-pushes hints for
    /// feeds the note already joined (the row append dedups, the hint
    /// does not) — the same at-most-once-per-run behavior as the
    /// `antenna` stream events the matcher publishes.
    fn push_unread_antennas(&self, note_id: &str) -> Result<(), PortError> {
        let Some(note) = self.services.notes.get_note(note_id)? else {
            return Ok(());
        };
        if note.is_deleted {
            return Ok(());
        }
        let antennas = self.services.antennas.all_active_antennas()?;
        if antennas.is_empty() {
            return Ok(());
        }
        let cursor = TimelineCursor::with_limit(ANTENNA_NOTES_CAP);
        for antenna in &antennas {
            let feed = self
                .services
                .antenna_notes
                .antenna_notes(&antenna.id, &cursor)?;
            if !feed.iter().any(|id| id == note_id) {
                continue;
            }
            let packed_note = pack_note(&note, Some(&antenna.owner_id), &self.services, 0)?;
            let body = serde_json::json!({
                "antenna": pack_antenna(antenna, &self.services),
                "note": packed_note,
            });
            self.services.streaming.push_main(MainMessage {
                user_id: antenna.owner_id.clone(),
                kind: MainEventKind::UnreadAntenna,
                body,
            })?;
        }
        Ok(())
    }
}
