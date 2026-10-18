use super::super::AppServices;

use crate::application::ports::{PortError, UpdateAccountPatch};
use crate::domain::account::Account;

impl AppServices {
    /// Record an inbound `Update(Person)` for a remote actor.
    pub fn handle_inbound_update_actor(
        &self,
        activity_id: &str,
        account_id: &str,
        patch: UpdateAccountPatch,
    ) -> Result<Option<Account>, PortError> {
        if self.inbound_host_blocked(account_id) {
            return Ok(None);
        }
        self.dedup
            .record_update_account_activity(activity_id, account_id, patch, vec![])
    }
}
