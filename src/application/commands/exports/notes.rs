use crate::application::commands::AppServices;

use crate::application::ports::PortError;

impl AppServices {
    /// One `ExportNotesProcessorService` note DTO.
    pub(crate) fn export_note_dto(
        &self,
        note: &crate::domain::note::Note,
    ) -> Result<serde_json::Value, PortError> {
        let created_at = self
            .ids
            .parse_time(&note.id)
            .map(crate::activitypub::format_millis)
            .unwrap_or_else(|_| crate::activitypub::format_millis(self.now_ms()));
        let mut files = Vec::new();
        for file_id in &note.file_ids {
            if let Some(meta) = self.media.get(file_id)? {
                files.push(crate::api::misskey::packers::pack_drive_file(&meta, self));
            }
        }
        let poll = match self.polls.get_poll(&note.id)? {
            None => serde_json::Value::Null,
            Some(poll) => serde_json::json!({
                "choices": poll.choices,
                "votes": poll.votes,
                "multiple": poll.multiple,
                "expiresAt": poll.expires_at_ms.map(crate::activitypub::format_millis),
            }),
        };
        Ok(serde_json::json!({
            "id": note.id,
            "text": note.text,
            "createdAt": created_at,
            "fileIds": note.file_ids,
            "files": files,
            "replyId": note.reply_id,
            "renoteId": note.renote_id,
            "poll": poll,
            "cw": note.cw,
            "visibility": note.visibility,
            "visibleUserIds": note.visible_user_ids,
            "localOnly": note.local_only,
            "reactionAcceptance": note.reaction_acceptance,
        }))
    }
}
