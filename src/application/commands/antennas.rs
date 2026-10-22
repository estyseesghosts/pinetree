//! Antennas (Batch N WS4; guide WP15; Misskey `AntennaService` parity).
//!
//! Rules collect matching notes into a personal feed. Note creation
//! enqueues one bounded `AntennaMatch` job (note id) in the same
//! transaction as the note; the worker loads the active antennas, matches
//! with [`antenna_matches`], and appends `antenna_notes` rows (trimmed to
//! the newest 200 per antenna in the same transaction). The feed read is a
//! range scan with read-time visibility/mute/block filtering (`FeedFilter`).
//! DIVERGENCEs (manifest §29): `home` = own + followed notes (upstream
//! `home` is a TODO matching everything); no auto-deactivation (instance
//! cap instead); `src=list` resolves members through the user-list index (WP16).

use super::AppServices;

use crate::application::ports::{NewJob, PortError, TimelineCursor};
use crate::domain::antenna::{
    Antenna, AntennaNoteView, AntennaSrc, DEFAULT_ANTENNA_LIMIT, antenna_matches, has_keyword,
};
use crate::domain::note::Note;

impl AppServices {
    /// `antennas/create`: validate (name/src/keywords, `NO_SUCH_USER_LIST`
    /// for `src=list` naming a missing/foreign list, `EMPTY_KEYWORD`,
    /// `TOO_MANY_ANTENNAS` over the enforced per-owner cap), then insert.
    /// New antennas start active (upstream `isActive: true` parity).
    #[allow(clippy::too_many_arguments)]
    pub fn create_antenna(
        &self,
        owner_id: &str,
        name: String,
        src: AntennaSrc,
        user_list_id: Option<String>,
        keywords: Vec<Vec<String>>,
        exclude_keywords: Vec<Vec<String>>,
        users: Vec<String>,
        case_sensitive: bool,
        with_replies: bool,
        with_file: bool,
        local_only: bool,
        exclude_bots: bool,
    ) -> Result<Antenna, PortError> {
        self.accounts.get_owner(owner_id)?;
        // Upstream `create.ts`: verify the list only when `src=list` names
        // one (`NO_SUCH_USER_LIST` on miss/foreign); `src=list` without an
        // id stores null and matches nothing (upstream parity).
        let verified_list_id = if src == AntennaSrc::List {
            match user_list_id.as_deref().map(str::trim) {
                Some(id) if !id.is_empty() => match self.lists.get_user_list(id)? {
                    Some(list) if list.owner_id == owner_id => Some(list.id),
                    _ => return Err(PortError::NotFound("NO_SUCH_USER_LIST".to_string())),
                },
                _ => None,
            }
        } else {
            None
        };
        if self.antennas.antennas_by_owner(owner_id)?.len() >= DEFAULT_ANTENNA_LIMIT {
            return Err(PortError::Validation("TOO_MANY_ANTENNAS".to_string()));
        }
        let antenna = Antenna {
            id: self.ids.generate(),
            owner_id: owner_id.to_string(),
            name,
            src,
            user_list_id: verified_list_id,
            keywords,
            exclude_keywords,
            users: users
                .into_iter()
                .map(|u| {
                    let u = u.trim().trim_start_matches('@').to_ascii_lowercase();
                    u
                })
                .filter(|u| !u.is_empty())
                .collect(),
            case_sensitive,
            with_replies,
            with_file,
            local_only,
            exclude_bots,
            exclude_notes_in_sensitive_channel: false,
            is_active: true,
            last_used_at_ms: self.now_ms(),
            created_at_ms: self.now_ms(),
        };
        // Upstream `create.ts` EMPTY_KEYWORD rule (verbatim code): at least
        // one non-blank entry across both lists. Checked here — not via the
        // `validate()` Debug wrapper — so the exact code survives mapping.
        if !has_keyword(&antenna.keywords) && !has_keyword(&antenna.exclude_keywords) {
            return Err(PortError::Validation("EMPTY_KEYWORD".to_string()));
        }
        antenna.validate().map_err(|e| match e {
            crate::domain::id::DomainError::Validation(msg) => PortError::Validation(msg),
            crate::domain::id::DomainError::Conflict(msg) => PortError::Conflict(msg),
            crate::domain::id::DomainError::NotFound(msg) => PortError::NotFound(msg),
            crate::domain::id::DomainError::StorageUnavailable(msg) => {
                PortError::StorageUnavailable(msg)
            }
        })?;
        self.antennas.create_antenna(antenna, Vec::new())
    }

    /// `antennas/update`: owner-scoped fetch (`NO_SUCH_ANTENNA` on miss),
    /// same validation as create (including the `src=list` ownership check).
    /// Sets `is_active = true` and bumps `last_used_at_ms` (upstream parity).
    #[allow(clippy::too_many_arguments)]
    pub fn update_antenna(
        &self,
        owner_id: &str,
        antenna_id: &str,
        name: Option<String>,
        src: Option<AntennaSrc>,
        user_list_id: Option<Option<String>>,
        keywords: Option<Vec<Vec<String>>>,
        exclude_keywords: Option<Vec<Vec<String>>>,
        users: Option<Vec<String>>,
        case_sensitive: Option<bool>,
        with_replies: Option<bool>,
        with_file: Option<bool>,
        local_only: Option<bool>,
        exclude_bots: Option<bool>,
    ) -> Result<Antenna, PortError> {
        self.accounts.get_owner(owner_id)?;
        let Some(mut antenna) = self.antennas.get_antenna(owner_id, antenna_id)? else {
            return Err(PortError::NotFound("NO_SUCH_ANTENNA".to_string()));
        };
        if let Some(name) = name {
            antenna.name = name;
        }
        // Upstream `update.ts`: when the new or old source is `list` and a
        // list id rides the call, it must name an owned list
        // (`NO_SUCH_USER_LIST`); a present id otherwise stores null, an
        // absent id leaves the stored value untouched.
        let old_src = antenna.src;
        if let Some(s) = src {
            antenna.src = s;
        }
        if let Some(id_opt) = user_list_id {
            match id_opt {
                Some(id) if antenna.src == AntennaSrc::List || old_src == AntennaSrc::List => {
                    match self.lists.get_user_list(id.trim())? {
                        Some(list) if list.owner_id == owner_id => {
                            antenna.user_list_id = Some(list.id);
                        }
                        _ => return Err(PortError::NotFound("NO_SUCH_USER_LIST".to_string())),
                    }
                }
                _ => antenna.user_list_id = None,
            }
        }
        // Upstream `update.ts` checks EMPTY_KEYWORD only when both lists
        // ride the same call (verbatim code on violation).
        let check_empty = keywords.is_some() && exclude_keywords.is_some();
        if let Some(keywords) = keywords {
            antenna.keywords = keywords;
        }
        if let Some(exclude_keywords) = exclude_keywords {
            antenna.exclude_keywords = exclude_keywords;
        }
        if let Some(users) = users {
            antenna.users = users
                .into_iter()
                .map(|u| u.trim().trim_start_matches('@').to_ascii_lowercase())
                .filter(|u| !u.is_empty())
                .collect();
        }
        if let Some(v) = case_sensitive {
            antenna.case_sensitive = v;
        }
        if let Some(v) = with_replies {
            antenna.with_replies = v;
        }
        if let Some(v) = with_file {
            antenna.with_file = v;
        }
        if let Some(v) = local_only {
            antenna.local_only = v;
        }
        if let Some(v) = exclude_bots {
            antenna.exclude_bots = v;
        }
        antenna.is_active = true;
        antenna.last_used_at_ms = self.now_ms();
        // Upstream `update.ts` checks EMPTY_KEYWORD only when both lists
        // ride the same call (verbatim code on violation).
        if check_empty && !has_keyword(&antenna.keywords) && !has_keyword(&antenna.exclude_keywords)
        {
            return Err(PortError::Validation("EMPTY_KEYWORD".to_string()));
        }
        antenna.validate().map_err(|e| match e {
            crate::domain::id::DomainError::Validation(msg) => PortError::Validation(msg),
            crate::domain::id::DomainError::Conflict(msg) => PortError::Conflict(msg),
            crate::domain::id::DomainError::NotFound(msg) => PortError::NotFound(msg),
            crate::domain::id::DomainError::StorageUnavailable(msg) => {
                PortError::StorageUnavailable(msg)
            }
        })?;
        self.antennas.update_antenna(antenna, Vec::new())
    }

    /// `antennas/delete`: owner-scoped; missing rows are `NO_SUCH_ANTENNA`
    /// (upstream parity). Feed rows cascade in the same transaction.
    pub fn delete_antenna(&self, owner_id: &str, antenna_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(owner_id)?;
        if !self.antennas.delete_antenna(owner_id, antenna_id)? {
            return Err(PortError::NotFound("NO_SUCH_ANTENNA".to_string()));
        }
        Ok(())
    }

    /// `antennas/notes` feed: owner-scoped antenna, reactivation touch
    /// (upstream `isActive = true, lastUsedAt = now` parity), newest-first
    /// note ids, visibility + mute/block read-time filtering, gone notes
    /// skipped.
    pub fn antenna_notes_for(
        &self,
        owner_id: &str,
        antenna_id: &str,
        cursor: &TimelineCursor,
    ) -> Result<Vec<Note>, PortError> {
        self.accounts.get_owner(owner_id)?;
        let Some(_) = self.antennas.get_antenna(owner_id, antenna_id)? else {
            return Err(PortError::NotFound("NO_SUCH_ANTENNA".to_string()));
        };
        self.antennas
            .touch_antenna(owner_id, antenna_id, self.now_ms())?;
        let ids = self.antenna_notes.antenna_notes(antenna_id, cursor)?;
        let filter = self.feed_filter(Some(owner_id))?;
        let mut account_cache = std::collections::HashMap::new();
        let mut note_cache = std::collections::HashMap::new();
        let follows_cache = self.follows_cache(Some(owner_id))?;
        let mut out = Vec::new();
        for id in ids {
            let Some(note) = self.notes.get_note(&id)? else {
                continue;
            };
            if note.is_deleted {
                continue;
            }
            let viewer = self.viewer_ctx(Some(owner_id), &note.author_id, &follows_cache);
            if !crate::domain::policy::is_visible(&note, &viewer) {
                continue;
            }
            if self.feed_filter_hides(&filter, &note, &mut account_cache, &mut note_cache)? {
                continue;
            }
            out.push(note);
        }
        Ok(out)
    }

    /// Worker: fan one committed note into every matching active antenna
    /// (guide WP15 architecture). Gone notes converge quietly; matching is
    /// bounded (active antennas only); each append trims to
    /// [`ANTENNA_NOTES_CAP`] in the same transaction. The `antenna` stream
    /// event publishes post-commit through `StreamingSink`.
    pub fn match_antenna_note(&self, note_id: &str) -> Result<(), PortError> {
        let Some(note) = self.notes.get_note(note_id)? else {
            return Ok(());
        };
        if note.is_deleted {
            return Ok(());
        }
        let antennas = self.antennas.all_active_antennas()?;
        if antennas.is_empty() {
            return Ok(());
        }
        let author = self.accounts.find_by_id(&note.author_id)?;
        let author_acct = match &author {
            Some(a) => {
                let lower = a.username.to_ascii_lowercase();
                match a.host.as_deref() {
                    Some(host) if !host.is_empty() => {
                        format!("{lower}@{}", host.to_ascii_lowercase())
                    }
                    _ => lower,
                }
            }
            None => return Ok(()),
        };
        let author_is_bot = author.as_ref().map(|a| a.is_bot).unwrap_or(false);
        let author_is_local = author.as_ref().map(|a| a.host.is_none()).unwrap_or(false);
        for antenna in &antennas {
            let owner_is_author = antenna.owner_id == note.author_id;
            let owner_follows_author = !owner_is_author
                && self
                    .follows
                    .is_following(&antenna.owner_id, &note.author_id)
                    .unwrap_or(false);
            let owner_in_visible_ids = owner_is_author
                || note
                    .visible_user_ids
                    .iter()
                    .any(|id| id == &antenna.owner_id);
            let author_in_list = match (antenna.src, antenna.user_list_id.as_deref()) {
                (AntennaSrc::List, Some(list_id)) => self
                    .lists
                    .get_list_membership(list_id, &note.author_id)
                    .map(|m| m.is_some())
                    .unwrap_or(false),
                _ => false,
            };
            let view = AntennaNoteView {
                note: &note,
                author_acct: author_acct.clone(),
                author_is_bot,
                author_is_local,
                owner_is_author,
                owner_follows_author,
                owner_in_visible_ids,
                author_in_list,
            };
            if antenna_matches(antenna, &view) {
                self.antenna_notes
                    .append_antenna_note(&antenna.id, &note.id)?;
                // Post-commit fan-out (the append above is its own
                // transaction): publish the `antenna` stream event through
                // the sink, like the worker's `StreamingPublish` expansion.
                let event = crate::application::events::StreamingEvent::AntennaNote {
                    antenna_id: antenna.id.clone(),
                    note_id: note.id.clone(),
                };
                let payload = serde_json::to_vec(&event).map_err(|e| {
                    PortError::StorageUnavailable(format!("antenna event encode failed: {e}"))
                })?;
                self.streaming.publish("streaming", &payload)?;
            }
        }
        Ok(())
    }

    /// Build the `antenna_match` job for a note id (enqueued in the
    /// note-creation transaction by the callers below).
    pub fn antenna_match_job(&self, note_id: &str) -> Result<NewJob, PortError> {
        use crate::application::ports::{AntennaMatchPayload, JobKind};
        NewJob::new_json(
            JobKind::AntennaMatch,
            &AntennaMatchPayload {
                note_id: note_id.to_string(),
            },
            format!("antenna-match:{note_id}:{}", self.ids.generate()),
            self.now_ms(),
        )
    }
}
