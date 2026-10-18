use super::AppServices;

use crate::application::ports::{
    ApiTokenRecord, CreateAccountInput, PortError, UpdateAccountPatch,
};
use crate::domain::account::{
    AVATAR_DECORATION_LIMIT_DEFAULT, MutedWord, WORD_MUTE_LIMIT_DEFAULT,
};
use crate::domain::account::Account;
use crate::domain::moderation::{ModerationAction, ModerationLogEntry};

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

    /// Set the Sharkey-style quote-rejection flag (§3; Sharkey
    /// `admin/reject-quotes.ts` parity). Works for local and cached remote
    /// accounts. A no-change call is a no-op success (upstream skips both
    /// the update and the log when the flag already matches).
    ///
    /// Batch M WP1: `moderator_id` is the acting moderator. The action is
    /// refused on moderator/admin targets (WP3 refusal rule) and logs as
    /// Sharkey's `acceptQuotesUser`/`rejectQuotesUser` with Sharkey's
    /// `{userId, userUsername, userHost}` payload. The moderation-log row,
    /// the flag write, and the `MeUpdated` streaming job commit in one
    /// transaction (§2.3). No federation: unlike `update_profile`, a
    /// moderation toggle never emits `Update(Person)`.
    pub fn set_reject_quotes(
        &self,
        moderator_id: &str,
        account_id: &str,
        reject: bool,
    ) -> Result<Account, PortError> {
        self.accounts.get_owner(account_id)?;
        let current = self
            .accounts
            .find_by_id(account_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
        if current.has_moderator_powers() {
            return Err(PortError::Validation(
                "cannot moderate moderator account".to_string(),
            ));
        }
        if current.reject_quotes == reject {
            return Ok(current);
        }
        let log = ModerationLogEntry::new(
            self.ids.generate(),
            Some(moderator_id.to_string()),
            if reject {
                ModerationAction::RejectQuotesUser
            } else {
                ModerationAction::AcceptQuotesUser
            },
            Some(account_id.to_string()),
            serde_json::json!({
                "userId": account_id,
                "userUsername": current.username,
                "userHost": current.host,
            }),
            self.clock.now_ms(),
        )?;
        let jobs = vec![self.streaming_job(
            &crate::application::events::StreamingEvent::MeUpdated {
                account_id: account_id.to_string(),
            },
            format!("reject-quotes:{account_id}:{}", self.ids.generate()),
        )?];
        let patch = UpdateAccountPatch {
            reject_quotes: Some(reject),
            ..Default::default()
        };
        self.moderation
            .update_account_logged(account_id, patch, log, jobs)
    }

    /// Full-profile update (`i/update.ts` handler parity; all 39 params plus
    /// the DIVERGENCE `rejectQuotes`/`defaultReaction` params arrive here as
    /// an [`UpdateAccountPatch`] built by the endpoint).
    ///
    /// Behaviors implemented here (not just column writes):
    /// - `name` trim / `""` → null (`update.ts` name block).
    /// - Prohibited words on the resulting name →
    ///   `YOUR_NAME_CONTAINS_PROHIBITED_WORDS` (422). TODO(§0B): upstream
    ///   checks the name-specific `prohibitedWordsForNameOfUser` list and
    ///   skips moderators; pinetree has one instance word list and no roles
    ///   infra, so the shared list applies to everyone.
    /// - `avatarId`/`bannerId` existence + ownership + `image/*` checks →
    ///   `NO_SUCH_AVATAR`/`NO_SUCH_BANNER`/`AVATAR_NOT_AN_IMAGE`/`BANNER_NOT_AN_IMAGE`;
    ///   explicit `null` clears. TODO(§0B): upstream also gates on the
    ///   `canUpdateBioMedia` role policy (default allow).
    /// - `avatarDecorations` count gate (`avatarDecorationLimit`, default 1)
    ///   → `RESTRICTED_BY_ROLE`. TODO(§0B): upstream also drops decorations
    ///   whose `roleIdsThatCanBeUsedThisDecoration` the user may not wear;
    ///   no decoration registry exists yet, so all submitted ids are kept.
    /// - `mutedWords`/`hardMutedWords` char-count gate (`wordMuteLimit`,
    ///   default 200) → `TOO_MANY_MUTED_WORDS`, plus `/pattern/flags`
    ///   compile check → `INVALID_REGEXP` (`update.ts`
    ///   `checkMuteWordCount`/`validateMuteWordRegex` parity).
    /// - `alwaysMarkNsfw`: upstream throws `RESTRICTED_BY_ROLE` when the
    ///   caller's role policy forces it on; the default policy is false so
    ///   the flag stays user-settable (no roles infra yet).
    /// - `pinnedPageId`: non-null ids resolve against the pages store →
    ///   `NO_SUCH_PAGE`. TODO(§0B): no pages store exists yet, so any
    ///   non-null value reports `NO_SUCH_PAGE`; `null` clears.
    /// - `alsoKnownAs` remote resolution: blank lines → `NO_SUCH_USER`,
    ///   self → `FORBIDDEN_TO_SET_YOURSELF`, resolved accounts without a URI
    ///   → `URI_NULL`; unresolvable entries are skipped (the sync use-case
    ///   layer performs local-store resolution only, no webfinger fetch).
    ///   Stored as actor URIs (`update.ts` `toUrl` parity).
    /// - `verifiedLinks` reset to `[]` on every update (`update.ts`
    ///   parity). TODO(§0B): the `verifyLink` re-check loop for `https://`
    ///   field values needs an off-request HTTP fetch; not run here.
    /// - Unlock (`isLocked: false` on a locked account) accepts all pending
    ///   follow requests (`update.ts:553-568` parity, simplified: upstream
    ///   also honors time-based approval windows, pinetree accepts all).
    pub fn update_profile(
        &self,
        account_id: &str,
        mut patch: UpdateAccountPatch,
    ) -> Result<Account, PortError> {
        self.accounts.get_owner(account_id)?;
        let current = self
            .accounts
            .find_by_id(account_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;

        // Name trim / `""` → null.
        if let Some(name_opt) = patch.display_name.take() {
            patch.display_name = Some(match name_opt {
                Some(name) => {
                    let trimmed = name.trim().to_string();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed)
                    }
                }
                None => None,
            });
        }
        let new_name = match &patch.display_name {
            Some(name) => name.clone(),
            None => current.display_name.clone(),
        };
        if let Some(name) = new_name {
            if contains_prohibited_word(&name, &self.prohibited_words) {
                return Err(PortError::Validation(
                    "YOUR_NAME_CONTAINS_PROHIBITED_WORDS".to_string(),
                ));
            }
        }

        // Avatar / banner media checks (`update.ts:377-411` parity).
        for (field, no_such, not_image) in [
            (&patch.avatar_id, "NO_SUCH_AVATAR", "AVATAR_NOT_AN_IMAGE"),
            (&patch.banner_id, "NO_SUCH_BANNER", "BANNER_NOT_AN_IMAGE"),
        ] {
            if let Some(Some(fid)) = field {
                match self.media.get(fid)? {
                    None => return Err(PortError::NotFound(no_such.to_string())),
                    Some(meta) => {
                        if meta.owner_id.as_deref() != Some(account_id) {
                            return Err(PortError::NotFound(no_such.to_string()));
                        }
                        if !meta.mime.starts_with("image/") {
                            return Err(PortError::Validation(not_image.to_string()));
                        }
                    }
                }
            }
        }

        // Avatar-decoration count gate.
        if let Some(decorations) = &patch.avatar_decorations {
            if decorations.len() > AVATAR_DECORATION_LIMIT_DEFAULT {
                return Err(PortError::Validation("RESTRICTED_BY_ROLE".to_string()));
            }
            for decoration in decorations {
                decoration
                    .validate()
                    .map_err(|e| PortError::Validation(format!("INVALID_PARAM {e}")))?;
            }
        }

        // Word-mute count + regex validation.
        for words in [&patch.muted_words, &patch.hard_muted_words]
            .into_iter()
            .flatten()
        {
            let total: usize = words.iter().map(MutedWord::char_len).sum();
            if total > WORD_MUTE_LIMIT_DEFAULT {
                return Err(PortError::Validation(
                    "TOO_MANY_MUTED_WORDS".to_string(),
                ));
            }
            for word in words {
                if let MutedWord::Single(single) = word {
                    validate_mute_regex(single)?;
                }
            }
        }

        // Pinned page: no pages store yet.
        if let Some(Some(_)) = &patch.pinned_page_id {
            return Err(PortError::NotFound("NO_SUCH_PAGE".to_string()));
        }

        // Also-known-as resolution.
        if let Some(lines_opt) = patch.also_known_as.take() {
            // TODO(§0B): upstream rejects updates from moved accounts here
            // (`YOUR_ACCOUNT_MOVED`); pinetree has no account-move feature
            // yet, so the guard is vacuous.
            let resolved = match lines_opt {
                None => None,
                Some(lines) => {
                    let mut uris = std::collections::BTreeSet::new();
                    for line in &lines {
                        let line = line.trim();
                        if line.is_empty() {
                            return Err(PortError::NotFound("NO_SUCH_USER".to_string()));
                        }
                        let Some(target) = self.resolve_alias_target(line)? else {
                            continue;
                        };
                        if target.id == current.id {
                            return Err(PortError::Validation(
                                "FORBIDDEN_TO_SET_YOURSELF".to_string(),
                            ));
                        }
                        if !target.is_local() && target.uri.is_none() {
                            return Err(PortError::Validation("URI_NULL".to_string()));
                        }
                        let uri = target
                            .uri
                            .clone()
                            .unwrap_or_else(|| target.actor_url(&self.public_url));
                        uris.insert(uri);
                    }
                    if uris.is_empty() {
                        None
                    } else {
                        Some(uris.into_iter().collect())
                    }
                }
            };
            patch.also_known_as = Some(resolved);
        }

        // Verified links reset on every profile update.
        patch.verified_links = Some(Vec::new());

        let unlocking = current.is_locked && patch.is_locked == Some(false);

        // Fan out Update(Person) when remote followers exist. The
        // AccountStore trait cannot carry jobs, so the federated path commits
        // state+jobs atomically through the dedup record path with a fresh
        // activity id (replay is impossible; the dedup row itself is harmless).
        let mut jobs = vec![self.streaming_job(
            &crate::application::events::StreamingEvent::MeUpdated {
                account_id: account_id.to_string(),
            },
            format!("profile:{account_id}:{}", self.ids.generate()),
        )?];
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
        let updated = if let Some(job) = federate_job {
            jobs.push(job);
            let activity_id = format!("local-update-{account_id}-{}", self.ids.generate());
            match self
                .dedup
                .record_update_account_activity(&activity_id, account_id, patch, jobs)?
            {
                Some(account) => account,
                // Dedup replay is impossible with a fresh activity id; refetch
                // defensively instead of failing the update.
                None => self.accounts.find_by_id(account_id)?.ok_or_else(|| {
                    PortError::NotFound("NO_SUCH_USER".to_string())
                })?,
            }
        } else {
            self.accounts.update_account(account_id, patch, jobs)?
        };

        // Unlock accepts pending follows (separate bounded transactions;
        // each accept is itself atomic state+jobs).
        if unlocking {
            loop {
                let pending = self
                    .follows
                    .follow_requests_to(account_id, None, 100)?;
                if pending.is_empty() {
                    break;
                }
                for req in pending {
                    match self.accept_follow_request(account_id, &req.follower_id) {
                        Ok(_) | Err(PortError::NotFound(_)) => {}
                        Err(e) => return Err(e),
                    }
                }
            }
        }

        Ok(updated)
    }

    /// Resolve one `alsoKnownAs` line to a local-store account: `user`,
    /// `@user`, `user@host`, `@user@host`, or a full actor URL. Returns
    /// `Ok(None)` when unresolvable (skipped by the caller).
    fn resolve_alias_target(&self, line: &str) -> Result<Option<Account>, PortError> {
        let line = line.strip_prefix('@').unwrap_or(line);
        if line.starts_with("http://") || line.starts_with("https://") {
            return self.accounts.find_by_uri(line);
        }
        let (username, host) = match line.split_once('@') {
            Some((user, host)) if !user.is_empty() && !host.is_empty() => {
                (user, Some(host.to_ascii_lowercase()))
            }
            _ if !line.is_empty() && !line.contains('@') && !line.contains('/') => {
                (line, None)
            }
            _ => return Ok(None),
        };
        // A `user@host` line pointing at this instance resolves locally.
        let local_host = host_of_url(&self.public_url);
        let account = match host {
            None => self
                .accounts
                .find_by_username_and_host(username, None)?,
            Some(h) if Some(h.as_str()) == local_host.as_deref() => {
                self.accounts
                    .find_by_username_and_host(username, None)?
            }
            Some(h) => self
                .accounts
                .find_by_username_and_host(username, Some(&h))?,
        };
        Ok(account)
    }

    /// Re-export for callers that only need token bookkeeping through commands.
    pub fn store_token(&self, record: ApiTokenRecord) -> Result<(), PortError> {
        self.auth.store_token(record)
    }
}

/// Host part of a base URL (`https://example.local/base` → `example.local`),
/// lowercased. `None` when the URL has no usable host.
fn host_of_url(url: &str) -> Option<String> {
    let after_scheme = url.split_once("://").map(|(_, rest)| rest)?;
    let host = after_scheme
        .split('/')
        .next()
        .unwrap_or("")
        .split('@')
        .next_back()
        .unwrap_or("");
    let host = host.split(':').next().unwrap_or("").trim();
    if host.is_empty() {
        None
    } else {
        Some(host.to_ascii_lowercase())
    }
}

/// `UtilityService.isKeyWordIncluded` parity: `/pattern/flags` entries test
/// as regex, other entries as space-separated AND substring keywords.
/// Matching is case-insensitive (Misskey normalizes before comparing).
fn contains_prohibited_word(text: &str, keywords: &[String]) -> bool {
    if keywords.is_empty() || text.is_empty() {
        return false;
    }
    let lowered = text.to_lowercase();
    keywords.iter().any(|filter| {
        let filter = filter.trim();
        if filter.is_empty() {
            return false;
        }
        if let Some((pattern, flags)) = split_slash_regex(filter) {
            return compile_mute_pattern(pattern, flags)
                .map(|re| re.is_match(&lowered))
                .unwrap_or(false);
        }
        filter
            .split(' ')
            .filter(|w| !w.is_empty())
            .all(|w| lowered.contains(&w.to_lowercase()))
    })
}

/// Split a `/pattern/flags` word-mute entry (`update.ts`
/// `validateMuteWordRegex` parity). Returns `None` for plain words.
fn split_slash_regex(word: &str) -> Option<(&str, &str)> {
    if !word.starts_with('/') {
        return None;
    }
    let inner = &word[1..];
    let end = inner.rfind('/')?;
    if end == 0 {
        return None;
    }
    Some((&inner[..end], &inner[end + 1..]))
}

/// Validate one single-word mute entry: plain words must already be in
/// `/pattern/flags` form (`update.ts` throws `INVALID_REGEXP` otherwise),
/// and the pattern must compile.
fn validate_mute_regex(word: &str) -> Result<(), PortError> {
    let Some((pattern, flags)) = split_slash_regex(word) else {
        return Err(PortError::Validation("INVALID_REGEXP".to_string()));
    };
    if compile_mute_pattern(pattern, flags).is_none() {
        return Err(PortError::Validation("INVALID_REGEXP".to_string()));
    }
    Ok(())
}

/// Compile a word-mute pattern with its flags. Supports the flag subset
/// Rust's `regex` can express inline (`i`, `m`, `s`); anything else is
/// rejected like a failed RE2 construction upstream.
fn compile_mute_pattern(pattern: &str, flags: &str) -> Option<regex::Regex> {
    let mut scoped = String::new();
    for flag in flags.chars() {
        match flag {
            'i' => scoped.push_str("(?i)"),
            'm' => scoped.push_str("(?m)"),
            's' => scoped.push_str("(?s)"),
            _ => return None,
        }
    }
    scoped.push_str(pattern);
    regex::Regex::new(&scoped).ok()
}
