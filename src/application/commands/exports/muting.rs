use crate::application::commands::AppServices;

use super::common::PAGE_SIZE;
use crate::application::ports::{PortError, TimelineCursor};

impl AppServices {
    /// Every muting row by `user_id` (including expired — the caller keeps
    /// only non-expiring ones), mute-row-id order.
    pub(crate) fn all_mutings(
        &self,
        user_id: &str,
    ) -> Result<Vec<crate::domain::muting::Muting>, PortError> {
        let mut rows = Vec::new();
        let mut until_id: Option<String> = None;
        loop {
            let page = self.mutes.list_mutings(
                user_id,
                &TimelineCursor {
                    since_id: None,
                    until_id: until_id.clone(),
                    since_date: None,
                    until_date: None,
                    limit: PAGE_SIZE,
                },
            )?;
            if page.is_empty() {
                break;
            }
            until_id = page.last().map(|r| r.id.clone());
            let len = page.len();
            rows.extend(page);
            if len < PAGE_SIZE {
                break;
            }
        }
        rows.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(rows)
    }
}
