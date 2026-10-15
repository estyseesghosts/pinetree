use super::AppServices;

use crate::application::ports::{ApiTokenRecord, CreateAccountInput, PortError, UpdateAccountPatch};
use crate::domain::account::Account;

impl AppServices {
    pub fn create_account(
        &self,
        username: &str,
        password_hash: Option<&str>,
    ) -> Result<Account, PortError> {
        let input = CreateAccountInput {
            handle: username.to_string(),
            host: None,
            display_name: None,
            description: None,
            location: None,
            birthday: None,
            uri: None,
            inbox: None,
            shared_inbox: None,
            is_bot: false,
            is_cat: false,
            is_locked: false,
            preserved_usernames: self.preserved_usernames.clone(),
        };
        let account = self.accounts.create_account(input)?;
        if let Some(hash) = password_hash {
            self.auth.set_password_hash(&account.id, hash)?;
        }
        Ok(account)
    }

    pub fn update_profile(
        &self,
        account_id: &str,
        patch: UpdateAccountPatch,
    ) -> Result<Account, PortError> {
        self.accounts.get_owner(account_id)?;
        for field in [&patch.avatar_id, &patch.banner_id] {
            if let Some(Some(fid)) = field {
                match self.media.get(fid)? {
                    None => return Err(PortError::NotFound("NO_SUCH_FILE".to_string())),
                    Some(meta) => {
                        if meta.owner_id.as_deref() != Some(account_id) {
                            return Err(PortError::Validation("ACCESS_DENIED".to_string()));
                        }
                    }
                }
            }
        }
        // Fan out Update(Person) when remote followers exist. The
        // AccountStore trait cannot carry jobs, so the federated path commits
        // state+jobs atomically through the dedup record path with a fresh
        // activity id (replay is impossible; the dedup row itself is harmless).
        let mut federate_job = None;
        for f in self.follows.followers_of(account_id, None, 10_000)? {
            if let Some(follower) = self.accounts.find_by_id(&f.follower_id)? {
                if !follower.is_local() {
                    federate_job = Some(
                        crate::federation::outbox::FederateDeliverPayload::ActorUpdated {
                            update_id: self.ids.generate(),
                            account_id: account_id.to_string(),
                            inbox_override: None,
                        }
                        .into_job(
                            format!(
                                "federate:actor:{account_id}:{}",
                                self.ids.generate()
                            ),
                            self.now_ms(),
                        )?,
                    );
                    break;
                }
            }
        }
        if let Some(job) = federate_job {
            let activity_id = format!("local-update-{account_id}-{}", self.ids.generate());
            match self
                .dedup
                .record_update_account_activity(&activity_id, account_id, patch, vec![job])?
            {
                Some(account) => return Ok(account),
                // Dedup replay is impossible with a fresh activity id; refetch
                // defensively instead of failing the update.
                None => {
                    return self.accounts.find_by_id(account_id)?.ok_or_else(|| {
                        PortError::NotFound("NO_SUCH_USER".to_string())
                    });
                }
            }
        }
        self.accounts.update_account(account_id, patch)
    }

    /// Re-export for callers that only need token bookkeeping through commands.
    pub fn store_token(&self, record: ApiTokenRecord) -> Result<(), PortError> {
        self.auth.store_token(record)
    }
}
