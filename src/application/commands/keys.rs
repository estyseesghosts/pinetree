use super::AppServices;

use crate::activitypub::signatures::{
    generate_rsa_keypair_2048, private_key_to_pkcs8_pem, public_key_to_spki_pem,
};
use crate::application::ports::{
    ActorKeypairRecord, CreateAccountInput, PortError, RemotePublicKeyRecord,
};
use crate::domain::account::Account;

impl AppServices {
    /// Return the RSA-2048 keypair for a local account, lazily generating and
    /// persisting one if none exists yet.
    pub fn ensure_actor_keypair(
        &self,
        account_id: &str,
    ) -> Result<ActorKeypairRecord, PortError> {
        if let Some(existing) = self.auth.get_actor_keypair(account_id)? {
            return Ok(existing);
        }
        self.rotate_actor_keypair(account_id)
    }

    /// Generate and store a fresh RSA-2048 keypair for `account_id` (key
    /// rotation or initial provisioning).
    pub fn rotate_actor_keypair(
        &self,
        account_id: &str,
    ) -> Result<ActorKeypairRecord, PortError> {
        let account = self
            .accounts
            .find_by_id(account_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
        if !account.is_local() {
            return Err(PortError::Validation(
                "cannot generate keypair for remote account".to_string(),
            ));
        }
        let (priv_key, pub_key) = generate_rsa_keypair_2048().map_err(PortError::Validation)?;
        let record = ActorKeypairRecord {
            account_id: account_id.to_string(),
            public_key_pem: public_key_to_spki_pem(&pub_key),
            private_key_pem: private_key_to_pkcs8_pem(&priv_key),
            created_at_ms: self.now_ms(),
        };
        self.auth.save_actor_keypair(record.clone())?;
        Ok(record)
    }

    /// Ensure the local `system_actor` service account and its RSA-2048 keypair
    /// exist (used for instance-signed ActivityPub GETs).
    pub fn ensure_system_actor(&self) -> Result<(Account, ActorKeypairRecord), PortError> {
        let account = match self.accounts.find_by_username_and_host("system_actor", None)? {
            Some(acc) => acc,
            None => {
                let mut input = CreateAccountInput::local("system_actor");
                input.is_bot = true;
                self.accounts.create_account(input)?
            }
        };
        let keypair = self.ensure_actor_keypair(&account.id)?;
        Ok((account, keypair))
    }

    /// Cache a remote actor's public key in storage.
    pub fn cache_remote_public_key(
        &self,
        key_id: &str,
        actor_uri: &str,
        account_id: Option<String>,
        public_key_pem: &str,
    ) -> Result<RemotePublicKeyRecord, PortError> {
        let record = RemotePublicKeyRecord {
            key_id: key_id.to_string(),
            actor_uri: actor_uri.to_string(),
            account_id,
            public_key_pem: public_key_pem.to_string(),
            fetched_at_ms: self.now_ms(),
        };
        self.auth.save_remote_public_key(record.clone())?;
        Ok(record)
    }
}
