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
        // Several requests can reach here at once for a new actor (every remote server fetching it
        // for the first time). Generate, then keep whichever keypair was stored first, so every
        // caller, and every remote cache, ends up with the same key.
        let record = self.new_actor_keypair_record(account_id)?;
        self.auth.save_actor_keypair_if_absent(record)
    }

    /// Generate and store a fresh RSA-2048 keypair for `account_id` (key
    /// rotation or initial provisioning).
    pub fn rotate_actor_keypair(
        &self,
        account_id: &str,
    ) -> Result<ActorKeypairRecord, PortError> {
        let record = self.new_actor_keypair_record(account_id)?;
        self.auth.save_actor_keypair(record.clone())?;
        Ok(record)
    }

    /// A freshly generated RSA-2048 keypair record for a local account (not stored).
    fn new_actor_keypair_record(&self, account_id: &str) -> Result<ActorKeypairRecord, PortError> {
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
        Ok(ActorKeypairRecord {
            account_id: account_id.to_string(),
            public_key_pem: public_key_to_spki_pem(&pub_key),
            private_key_pem: private_key_to_pkcs8_pem(&priv_key),
            created_at_ms: self.now_ms(),
        })
    }

    /// Ensure the local `system_actor` service account and its RSA-2048 keypair
    /// exist (used for instance-signed ActivityPub GETs).
    pub fn ensure_system_actor(&self) -> Result<(Account, ActorKeypairRecord), PortError> {
        let account = match self.accounts.find_by_username_and_host("system_actor", None)? {
            Some(acc) => acc,
            None => {
                let mut input = CreateAccountInput::local("system_actor");
                input.is_bot = true;
                match self.accounts.create_account(input, None) {
                    Ok(acc) => acc,
                    // A concurrent first call created it between our lookup and our insert (every
                    // remote server fetching a new actor at once): use the account that won.
                    Err(PortError::Conflict(_)) => self
                        .accounts
                        .find_by_username_and_host("system_actor", None)?
                        .ok_or_else(|| PortError::NotFound("system_actor".to_string()))?,
                    Err(e) => return Err(e),
                }
            }
        };
        let keypair = self.ensure_actor_keypair(&account.id)?;
        Ok((account, keypair))
    }

    /// Cache a remote actor's public key in storage.
    ///
    /// Slice 2 (C1): a key id is bound to the first actor URI it was cached
    /// for. Re-caching the same `(key_id, actor_uri)` pair (e.g. after key
    /// rotation, where the PEM changes but the owner does not) overwrites
    /// normally; caching the same `key_id` under a *different* `actor_uri`
    /// is refused with [`PortError::Conflict`] so a malicious actor document
    /// cannot rebind (poison) another actor's key id.
    pub fn cache_remote_public_key(
        &self,
        key_id: &str,
        actor_uri: &str,
        account_id: Option<String>,
        public_key_pem: &str,
    ) -> Result<RemotePublicKeyRecord, PortError> {
        if let Some(existing) = self.auth.get_remote_public_key(key_id)? {
            if existing.actor_uri != actor_uri {
                return Err(PortError::Conflict(format!(
                    "remote public key {key_id} is already cached for {}",
                    existing.actor_uri
                )));
            }
        }
        let record = RemotePublicKeyRecord {
            key_id: key_id.to_string(),
            actor_uri: actor_uri.to_string(),
            account_id,
            // Canonicalize: `parse_remote_actor` trims the PEM, so trim here
            // too or the same key compares differently by ingest path.
            public_key_pem: public_key_pem.trim().to_string(),
            fetched_at_ms: self.now_ms(),
        };
        self.auth.save_remote_public_key(record.clone())?;
        Ok(record)
    }
}
