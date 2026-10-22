use crate::application::commands::AppServices;

use crate::application::ports::PortError;

impl AppServices {
    /// Read-time visibility of one note for the exporter/importer
    /// (export-clips visibility-query parity): local-user viewer with the
    /// real follow edge.
    pub(crate) fn note_visible_to(
        &self,
        note: &crate::domain::note::Note,
        viewer_id: &str,
    ) -> Result<bool, PortError> {
        use crate::domain::visibility::{ViewerContext, is_visible};
        let follows =
            note.author_id == viewer_id || self.follows.is_following(viewer_id, &note.author_id)?;
        Ok(is_visible(
            note,
            &ViewerContext::local_user(viewer_id, follows),
        ))
    }

    /// pinetree-only clip import (no upstream endpoint exists): consume our
    /// own `export-clips` JSON (`[{name, description?, clipNotes?}]`).
    /// Each entry find-or-creates the caller's clip by name (first match
    /// wins; invalid entries skip); each member note attaches when the
    /// note exists, is not deleted, and is visible to the importer
    /// (per-row try/catch parity — only storage faults abort). `applied`
    /// counts clips created + notes attached; `skipped` counts invalid
    /// entries + unattachable notes.
    pub(crate) fn apply_clip_import(
        &self,
        user_id: &str,
        text: &str,
    ) -> Result<(usize, usize), PortError> {
        let parsed: serde_json::Value = match serde_json::from_str(text) {
            Ok(value) => value,
            Err(_) => return Ok((0, 1)),
        };
        let entries = match parsed.as_array() {
            Some(entries) => entries,
            None => return Ok((0, 1)),
        };
        let mut applied = 0usize;
        let mut skipped = 0usize;
        for entry in entries {
            let Some(map) = entry.as_object() else {
                skipped += 1;
                continue;
            };
            let Some(name) = map.get("name").and_then(|v| v.as_str()) else {
                skipped += 1;
                continue;
            };
            if crate::domain::clip::Clip::validate_name(name).is_err() {
                skipped += 1;
                continue;
            }
            let description = map
                .get("description")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            // Find-or-create by name (upstream user-list import parity:
            // names are not unique, first match wins).
            let clip = match self
                .clips
                .clips_by_owner(user_id)?
                .into_iter()
                .find(|c| c.name == name)
            {
                Some(clip) => clip,
                None => match self.create_clip(
                    user_id,
                    name.to_string(),
                    false,
                    description,
                    None,
                    false,
                    Vec::new(),
                ) {
                    Ok(clip) => {
                        applied += 1;
                        clip
                    }
                    Err(e @ PortError::StorageUnavailable(_)) => return Err(e),
                    Err(_) => {
                        skipped += 1;
                        continue;
                    }
                },
            };
            let members = map
                .get("clipNotes")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            for member in &members {
                let note_id = member
                    .get("note")
                    .and_then(|n| n.get("id"))
                    .and_then(|v| v.as_str())
                    .or_else(|| member.get("noteId").and_then(|v| v.as_str()));
                let Some(note_id) = note_id else {
                    skipped += 1;
                    continue;
                };
                let visible = match self.notes.get_note(note_id)? {
                    Some(note) if !note.is_deleted => self.note_visible_to(&note, user_id)?,
                    _ => false,
                };
                if !visible {
                    skipped += 1;
                    continue;
                }
                match self.add_clip_note(user_id, &clip.id, note_id) {
                    Ok(()) => applied += 1,
                    Err(e @ PortError::StorageUnavailable(_)) => return Err(e),
                    Err(_) => skipped += 1,
                }
            }
        }
        Ok((applied, skipped))
    }
}
