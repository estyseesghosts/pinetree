use crate::application::commands::AppServices;
use crate::application::ports::{PortError, TimelineCursor};

impl AppServices {
    pub fn instance_stats(&self) -> Result<(u64, u64, u64, u64, u64, u64, u64, u64), PortError> {
        let notes = self.notes.count_notes()?;
        let local_notes = self.notes.count_local_notes()?;
        let reactions = self.reactions.count_reactions()?;
        let mut users = 0u64;
        let mut local_users = 0u64;
        let mut hosts = std::collections::HashSet::new();
        let mut local_ids = std::collections::HashSet::new();
        let mut after: Option<String> = None;
        loop {
            let (page, next) = self.accounts.all_accounts(after.as_deref(), 500)?;
            if page.is_empty() {
                break;
            }
            for acc in page {
                after = Some(acc.id.clone());
                users += 1;
                if acc.is_local() {
                    local_users += 1;
                    local_ids.insert(acc.id.clone());
                } else if let Some(host) = acc.host.clone() {
                    hosts.insert(host);
                }
            }
            if next.is_none() {
                break;
            }
        }
        let mut drive_local = 0u64;
        let mut drive_remote = 0u64;
        let mut media_cursor = TimelineCursor {
            limit: 500,
            ..Default::default()
        };
        loop {
            let page = self.media.list_all_media(&media_cursor)?;
            if page.is_empty() {
                break;
            }
            for meta in &page {
                let is_local = meta
                    .owner_id
                    .as_deref()
                    .map(|owner| local_ids.contains(owner))
                    .unwrap_or(false);
                if is_local {
                    drive_local += meta.size_bytes;
                } else {
                    drive_remote += meta.size_bytes;
                }
            }
            // Empty-page termination only (Batch C/H): a short page means
            // concurrent deletes, not end-of-scan, and an exact-multiple
            // dataset needs the trailing empty read.
            media_cursor.until_id = page.last().map(|m| m.id.clone());
        }
        Ok((
            notes,
            local_notes,
            users,
            local_users,
            reactions,
            hosts.len() as u64,
            drive_local,
            drive_remote,
        ))
    }
}
