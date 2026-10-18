//! Batch M WP2: registration policy and invite codes (upstream
//! `SignupApiService` invite gate, `invite/*`, `admin/invite/*`).
//!
//! §9.1 default policy (recorded choice): invite creation is a moderator
//! action (upstream default role policy `canInvite: false`), unlimited in
//! count (upstream `inviteLimit: 0`), with no default expiry (upstream
//! `inviteExpirationTime: 0`) unless an explicit `expiresAt` is given.

use super::AppServices;

use crate::application::ports::{
    ConsumeInviteInput, CreateAccountInput, CreateInviteInput, InviteListFilter, PortError,
};
use crate::domain::account::Account;
use crate::domain::instance::InviteCode;
use crate::domain::moderation::{ModerationAction, ModerationLogEntry};

/// Outcome of a public signup attempt ([`AppServices::signup`]).
#[derive(Debug)]
pub enum SignupRejection {
    /// Registration is invite-only and no consumable code was supplied
    /// (or the code is missing, used, or expired). Upstream answers plain
    /// HTTP 400 with an empty body for every registration-gate failure
    /// (`SignupApiService.ts` around line 136: `reply.code(400); return;`).
    InviteOnly,
    /// Any other failure (username rules, duplicate handle, storage).
    Port(PortError),
}

impl From<PortError> for SignupRejection {
    fn from(err: PortError) -> Self {
        Self::Port(err)
    }
}

/// Upstream `misc/generate-invite-code.ts`: 8 chars from
/// `23456789ABCDEFGHJKLMNPQRSTUVWXYZ` ([0-9A-Z] without [01IO]) plus the
/// current minute encoded in base 32 over the same alphabet.
pub(crate) fn generate_invite_code(now_ms: i64) -> String {
    const CHARS: &[u8] = b"23456789ABCDEFGHJKLMNPQRSTUVWXYZ";
    let mut code = String::with_capacity(12);
    for _ in 0..8 {
        code.push(CHARS[(rand::random::<u8>() as usize) % CHARS.len()] as char);
    }
    let mut n = now_ms / 1000 / 60;
    let mut suffix = Vec::new();
    loop {
        suffix.push(CHARS[(n % CHARS.len() as i64) as usize] as char);
        let t = n / CHARS.len() as i64;
        if t == 0 {
            break;
        }
        n = t;
    }
    code.extend(suffix.into_iter().rev());
    code
}

impl AppServices {
    /// Public signup (upstream `SignupApiService.signup`): enforce the
    /// registration policy and, when it is closed, claim an invite code
    /// and create the account in ONE transaction (conformance I1/I2).
    ///
    /// When registration is open a supplied code is ignored entirely —
    /// upstream skips the ticket logic unless `disableRegistration` is on,
    /// so the code is not consumed either.
    pub fn signup(
        &self,
        username: &str,
        password_hash: Option<&str>,
        invitation_code: Option<&str>,
        now_ms: i64,
    ) -> Result<Account, SignupRejection> {
        let settings = self.instance_settings.get()?;
        if !settings.registration.disable_registration() {
            return Ok(self.create_account(username, password_hash)?);
        }
        let code = invitation_code
            .map(str::trim)
            .filter(|code| !code.is_empty())
            .ok_or(SignupRejection::InviteOnly)?;
        // Pre-check mirrors upstream's early 400; the real claim is atomic
        // inside consume_and_create_account, so a lost race (another
        // signup consumed the code first) fails the same way.
        let ticket = self
            .invites
            .find_invite_by_code(code)?
            .ok_or(SignupRejection::InviteOnly)?;
        if ticket.is_used() || ticket.is_expired(now_ms) {
            return Err(SignupRejection::InviteOnly);
        }
        let (account, _ticket) = self
            .invites
            .consume_and_create_account(ConsumeInviteInput {
                code: code.to_string(),
                account: self.account_input(username),
                password_hash: password_hash.map(str::to_string),
            })
            .map_err(|err| match err {
                PortError::NotFound(_) => SignupRejection::InviteOnly,
                other => SignupRejection::Port(other),
            })?;
        Ok(account)
    }

    /// `invite/create` / `admin/invite/create`: create `count` codes for
    /// `created_by` in one transaction. With `log`, the batch commits one
    /// `createInvitation` row with it (upstream logs only the admin
    /// endpoint; `invite/create` is silent there).
    pub fn create_invites(
        &self,
        created_by: &str,
        count: usize,
        expires_at_ms: Option<i64>,
        now_ms: i64,
        log: bool,
    ) -> Result<Vec<InviteCode>, PortError> {
        if count == 0 || count > 100 {
            return Err(PortError::Validation(
                "INVALID_PARAM count must be 1..=100".to_string(),
            ));
        }
        let mut inputs = Vec::with_capacity(count);
        for _ in 0..count {
            inputs.push(CreateInviteInput {
                code: generate_invite_code(now_ms),
                created_by: Some(created_by.to_string()),
                expires_at_ms,
            });
        }
        let entry = if log {
            Some(ModerationLogEntry::new(
                self.ids.generate(),
                Some(created_by.to_string()),
                ModerationAction::CreateInvitation,
                None,
                serde_json::json!({
                    "invitations": inputs
                        .iter()
                        .map(|input| serde_json::json!({
                            "code": input.code,
                            "createdBy": input.created_by,
                            "expiresAt": input.expires_at_ms,
                        }))
                        .collect::<Vec<_>>(),
                    "invitationsCount": inputs.len(),
                }),
                now_ms,
            )?)
        } else {
            None
        };
        self.invites.create_invites(&inputs, entry.as_ref())
    }

    /// `invite/list`: the caller's own codes. Upstream scopes to
    /// `createdById = me` even for moderators (the admin view is
    /// `admin/invite/list`).
    pub fn list_my_invites(
        &self,
        viewer: &str,
        mut filter: InviteListFilter,
    ) -> Result<Vec<InviteCode>, PortError> {
        filter.created_by = Some(viewer.to_string());
        self.invites.list_invites(&filter)
    }

    /// `admin/invite/list`: every creator's codes (moderator view).
    pub fn list_all_invites(&self, filter: InviteListFilter) -> Result<Vec<InviteCode>, PortError> {
        self.invites.list_invites(&filter)
    }

    /// `invite/delete` (upstream `invite/delete.ts`): a moderator may
    /// delete any code, including used ones; anyone else only their own
    /// unused code.
    ///
    /// DIVERGENCE: upstream writes no moderation-log row for deletions;
    /// guide §3.2 names `deleteInvite` (no upstream action type), so
    /// pinetree records one in the delete transaction.
    pub fn delete_invite(
        &self,
        invite_id: &str,
        actor: &str,
        actor_is_moderator: bool,
    ) -> Result<(), PortError> {
        let ticket = self
            .invites
            .find_invite_by_id(invite_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_INVITE_CODE".to_string()))?;
        if ticket.created_by.as_deref() != Some(actor) && !actor_is_moderator {
            return Err(PortError::Validation(
                "ACCESS_DENIED you can't delete this invite code".to_string(),
            ));
        }
        if ticket.is_used() && !actor_is_moderator {
            return Err(PortError::Validation(
                "CAN_NOT_DELETE_INVITE_CODE this invite code was already used".to_string(),
            ));
        }
        let creator = match &ticket.created_by {
            Some(id) => self.accounts.find_by_id(id)?,
            None => None,
        };
        let log = ModerationLogEntry::new(
            self.ids.generate(),
            Some(actor.to_string()),
            ModerationAction::DeleteInvite,
            None,
            serde_json::json!({
                "inviteCode": ticket.code,
                "userId": ticket.created_by,
                "userUsername": creator.as_ref().map(|account| account.username.clone()),
                "userHost": creator.as_ref().and_then(|account| account.host.clone()),
            }),
            self.clock.now_ms(),
        )?;
        self.invites.delete_invite(invite_id, Some(&log))
    }

    /// `invite/limit` (upstream `invite/limit.ts`): `remaining` is `null`
    /// when the policy has no per-user cap — the §9.1 default (upstream
    /// `inviteLimit: 0` means unlimited, so `remaining: null`). With a
    /// cap this is `cap − codes created in the cycle`.
    pub fn invite_limit(&self, _actor: &str) -> Option<usize> {
        None
    }

    /// Packed `InviteCode` (upstream `InviteCodeEntityService.pack`:
    /// `{id, code, expiresAt, createdAt, createdBy, usedBy, usedAt,
    /// used}`). `created_by`/`used_by` are packed by the caller (the API
    /// packer) to keep this layer free of response shapes.
    pub fn invite_fields(&self, invite: &InviteCode) -> (Option<Account>, Option<Account>) {
        let created_by = invite
            .created_by
            .as_ref()
            .and_then(|id| self.accounts.find_by_id(id).ok().flatten());
        let used_by = invite
            .used_by
            .as_ref()
            .and_then(|id| self.accounts.find_by_id(id).ok().flatten());
        (created_by, used_by)
    }
}

/// Shared construction for account-creation inputs (signup, admin create,
/// CLI, invite claim all pass the same preserved-username list).
impl AppServices {
    pub(crate) fn account_input(&self, username: &str) -> CreateAccountInput {
        CreateAccountInput {
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
        }
    }
}
