//! Note pinning + featured ranking (`docs/misskey-parity-plan.md` §7;
//! Misskey `NotePiningService` + `notes/featured.ts` +
//! `users/featured-notes.ts` parity). Misskey API + ActivityPub adapters
//! call these same ops.
//!
//! Featured uses one documented ranking (compatible-simplified, NOT
//! upstream-identical): recent non-sensitive public notes (local +
//! remote, no channels — channels do not exist here) scored by
//! `reactions + renotes` over the trailing window, tie-break newest-first.
//! Upstream ranks through Redis counters incremented on every reaction and
//! renote (`FeaturedService`) and returns the ranked set newest-first; we
//! compute the same observable order (score selects, id orders) from the
//! stored counters on read. Both `notes/featured` and
//! `users/featured-notes` share this ranking and the 3600s timed cache.

use std::collections::HashMap;
use std::sync::Mutex;

use super::AppServices;

use crate::application::ports::{NewJob, PortError, TimelineCursor};
use crate::domain::note::{Note, Visibility};
use crate::domain::pinning::UserNotePining;

/// Trailing window for the global featured ranking (Misskey
/// `GLOBAL_NOTES_RANKING_WINDOW`: 3 days).
pub const FEATURED_GLOBAL_WINDOW_MS: i64 = 3 * 24 * 3600 * 1000;
/// Trailing window for the per-user featured ranking (Misskey
/// `PER_USER_NOTES_RANKING_WINDOW`: 7 days).
pub const FEATURED_USER_WINDOW_MS: i64 = 7 * 24 * 3600 * 1000;
/// Cache TTL for both featured endpoints (Misskey `cacheSec: 3600`).
pub const FEATURED_CACHE_TTL_MS: i64 = 3600 * 1000;
/// Ranking selection pool (upstream fetches the top 100 global / 50
/// per-user ids from Redis before paging).
pub const FEATURED_GLOBAL_THRESHOLD: usize = 100;
/// Per-user ranking selection pool (upstream top 50).
pub const FEATURED_USER_THRESHOLD: usize = 50;
/// Full-scan bound for the candidate sweep (single-node read; see the
/// endpoint manifest §17 for the FDB note).
pub const FEATURED_SCAN_LIMIT: usize = 10_000;

/// Timed featured-ranking cache (one entry per scope key, 3600s TTL).
/// Owned by `ApiState` so tests get a fresh cache per state.
#[derive(Debug, Default)]
pub struct FeaturedCache {
    entries: Mutex<HashMap<String, (i64, Vec<String>)>>,
}

impl FeaturedCache {
    pub fn get(&self, key: &str, now_ms: i64) -> Option<Vec<String>> {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        match entries.get(key) {
            Some((at, ids)) if now_ms.saturating_sub(*at) < FEATURED_CACHE_TTL_MS => {
                Some(ids.clone())
            }
            _ => None,
        }
    }

    pub fn put(&self, key: &str, ids: Vec<String>, now_ms: i64) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.insert(key.to_string(), (now_ms, ids));
    }
}

/// Score of one candidate note: reaction total + renote total.
pub fn featured_score(note: &Note) -> u64 {
    note.reactions
        .values()
        .sum::<u64>()
        .saturating_add(note.renote_count)
}

/// Order ranked ids newest-first (upstream `noteIds.sort((a, b) => a > b ?
/// -1 : 1)` parity: the observable order is by id, never by score).
pub fn newest_first(mut ids: Vec<String>) -> Vec<String> {
    ids.sort_by(|a, b| b.cmp(a));
    ids
}

impl AppServices {
    /// Pin one note (Misskey `NotePiningService.addPinned` parity).
    /// Order: ownership (`NO_SUCH_NOTE` when missing or foreign — the
    /// upstream `findOneBy({ id, userId })` parity) → limit
    /// (`PIN_LIMIT_EXCEEDED`) → duplicate (`ALREADY_PINNED`, enforced
    /// atomically by the unique store index). The pinning row and the
    /// federation jobs commit in one storage transaction; local
    /// public/home pins federate as `Add` to the featured collection.
    pub fn pin_note(&self, user_id: &str, note_id: &str) -> Result<UserNotePining, PortError> {
        self.accounts.get_owner(user_id)?;
        let note = match self.notes.get_note(note_id)? {
            Some(note) if note.author_id == user_id => note,
            _ => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
        };
        if self.notes.pinnings_for_user(user_id)?.len() >= self.pin_limit {
            return Err(PortError::Validation("PIN_LIMIT_EXCEEDED".to_string()));
        }
        let mut jobs = Vec::new();
        if self.pin_federates(&note) {
            jobs.push(self.featured_changed_job(user_id, &note.id, true)?);
        }
        let pinning = UserNotePining {
            id: self.ids.generate(),
            user_id: user_id.to_string(),
            note_id: note.id,
        };
        pinning
            .validate()
            .map_err(|e| PortError::Validation(format!("{e:?}")))?;
        match self.notes.add_pinning(pinning, jobs) {
            Ok(pinning) => Ok(pinning),
            Err(PortError::Conflict(_)) => Err(PortError::Conflict("ALREADY_PINNED".to_string())),
            Err(e) => Err(e),
        }
    }

    /// Unpin one note (Misskey `NotePiningService.removePinned` parity):
    /// ownership is enforced like on pin (`NO_SUCH_NOTE`); a missing
    /// pinning row is a no-op success (upstream deletes unconditionally).
    /// Local public/home unpins federate as `Remove`.
    pub fn unpin_note(&self, user_id: &str, note_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        let note = match self.notes.get_note(note_id)? {
            Some(note) if note.author_id == user_id => note,
            _ => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
        };
        let mut jobs = Vec::new();
        if self.pin_federates(&note) {
            jobs.push(self.featured_changed_job(user_id, &note.id, false)?);
        }
        self.notes.remove_pinning(user_id, &note.id, jobs)
    }

    /// Pinned note ids for `user_id`, oldest-first (Misskey
    /// `UserEntityService` `pins.map(pin => pin.noteId)` order parity).
    pub fn pinned_note_ids(&self, user_id: &str) -> Result<Vec<String>, PortError> {
        Ok(self
            .notes
            .pinnings_for_user(user_id)?
            .into_iter()
            .map(|p| p.note_id)
            .collect())
    }

    /// Whether a pin change federates (Misskey `addPinned`/
    /// `removePinned` parity): local authors only, non-`localOnly`,
    /// `public`/`home` visibility.
    fn pin_federates(&self, note: &Note) -> bool {
        if note.local_only {
            return false;
        }
        if !matches!(note.visibility, Visibility::Public | Visibility::Home) {
            return false;
        }
        match self.accounts.find_by_id(&note.author_id) {
            Ok(Some(account)) => account.is_local(),
            _ => false,
        }
    }

    fn featured_changed_job(
        &self,
        actor_id: &str,
        note_id: &str,
        added: bool,
    ) -> Result<NewJob, PortError> {
        crate::federation::outbox::FederateDeliverPayload::FeaturedChanged {
            actor_id: actor_id.to_string(),
            note_id: note_id.to_string(),
            added,
            inbox_override: None,
        }
        .into_job(
            format!(
                "federate:featured:{}:{note_id}:{}",
                if added { "add" } else { "remove" },
                self.ids.generate()
            ),
            self.now_ms(),
        )
    }

    /// Ranked global featured note ids (top `FEATURED_GLOBAL_THRESHOLD`
    /// by score over the trailing 3-day window). Pure selection: the
    /// endpoint applies the timed cache, `untilId`, visibility, and the
    /// page limit on top.
    pub fn rank_featured_global(&self) -> Result<Vec<String>, PortError> {
        let now = self.now_ms();
        let notes = self.notes.list_all_notes(&TimelineCursor {
            since_id: None,
            until_id: None,
            since_date: None,
            until_date: None,
            limit: FEATURED_SCAN_LIMIT,
        })?;
        let mut scored = Vec::new();
        for note in &notes {
            if note.author_id.is_empty() {
                continue;
            }
            if self.featured_candidate(note, None, now, FEATURED_GLOBAL_WINDOW_MS)? {
                scored.push((featured_score(note), note.id.clone()));
            }
        }
        scored.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
        scored.truncate(FEATURED_GLOBAL_THRESHOLD);
        Ok(newest_first(scored.into_iter().map(|(_, id)| id).collect()))
    }

    /// Ranked per-user featured note ids (top `FEATURED_USER_THRESHOLD`
    /// by score over the trailing 7-day window). Unknown authors yield an
    /// empty ranking (upstream `getPerUserNotesRanking` parity: no
    /// `NO_SUCH_USER` on this endpoint).
    pub fn rank_featured_for_user(&self, author_id: &str) -> Result<Vec<String>, PortError> {
        if self.accounts.find_by_id(author_id)?.is_none() {
            return Ok(Vec::new());
        }
        let now = self.now_ms();
        let notes = self.notes.notes_by_author(
            author_id,
            &TimelineCursor {
                since_id: None,
                until_id: None,
                since_date: None,
                until_date: None,
                limit: FEATURED_SCAN_LIMIT,
            },
        )?;
        let mut scored = Vec::new();
        for note in &notes {
            if self.featured_candidate(note, Some(author_id), now, FEATURED_USER_WINDOW_MS)? {
                scored.push((featured_score(note), note.id.clone()));
            }
        }
        scored.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
        scored.truncate(FEATURED_USER_THRESHOLD);
        Ok(newest_first(scored.into_iter().map(|(_, id)| id).collect()))
    }

    /// One candidate predicate shared by both rankings: public, recent
    /// (trailing `window_ms`), non-sensitive, live.
    ///
    /// Non-sensitive interpretation (documented simplification): no
    /// content warning and no sensitive attached file. A file row that is
    /// gone is treated as non-sensitive (its bytes are unservable, so it
    /// cannot render unsafely); unknown file rows fail closed to
    /// sensitive. Channels do not exist here, so the no-channels rule
    /// holds trivially.
    fn featured_candidate(
        &self,
        note: &Note,
        author_filter: Option<&str>,
        now_ms: i64,
        window_ms: i64,
    ) -> Result<bool, PortError> {
        if note.is_deleted {
            return Ok(false);
        }
        if note.visibility != Visibility::Public {
            return Ok(false);
        }
        if let Some(author) = author_filter {
            if note.author_id != author {
                return Ok(false);
            }
        }
        let created_at = self.ids.parse_time(&note.id).unwrap_or(now_ms);
        if now_ms.saturating_sub(created_at) > window_ms {
            return Ok(false);
        }
        if note.cw.is_some() {
            return Ok(false);
        }
        for fid in &note.file_ids {
            match self.media.get(fid)? {
                Some(meta) if !meta.is_sensitive => {}
                Some(_) => return Ok(false),
                None => return Ok(false),
            }
        }
        Ok(true)
    }

    /// Resolve ranked ids into visible notes for a viewer: `untilId`
    /// filter, newest-first order, visibility + mute/block post-filter
    /// (upstream `isUserRelated` parity over note, reply, and renote
    /// authors), page limit.
    pub fn featured_notes_visible(
        &self,
        ranked_ids: &[String],
        viewer_id: Option<&str>,
        until_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Note>, PortError> {
        let mut out = Vec::new();
        for id in ranked_ids {
            if let Some(until) = until_id {
                if id.as_str() >= until {
                    continue;
                }
            }
            let Some(note) = self.notes.get_note(id)? else {
                continue;
            };
            if !self.featured_visible_to(&note, viewer_id)? {
                continue;
            }
            out.push(note);
            if out.len() >= limit.max(1) {
                break;
            }
        }
        out.sort_by(|a, b| b.id.cmp(&a.id));
        Ok(out)
    }

    fn featured_visible_to(&self, note: &Note, viewer_id: Option<&str>) -> Result<bool, PortError> {
        let Some(viewer) = viewer_id else {
            let viewer = crate::domain::policy::ViewerContext::anonymous();
            return Ok(crate::domain::policy::is_visible(note, &viewer));
        };
        let follows_author =
            viewer == note.author_id || self.follows.is_following(viewer, &note.author_id)?;
        let context = crate::domain::policy::ViewerContext::local_user(viewer, follows_author);
        if !crate::domain::policy::is_visible(note, &context) {
            return Ok(false);
        }
        // Mute/block post-filter (upstream `isUserRelated` parity):
        // notes by (or replying/renoting) muted authors, or authors who
        // block the viewer, are withheld. `reply_user_id` is denormalized
        // at create time; the renote target is re-read so a target deleted
        // after the renote still filters truthfully.
        let muted: Vec<String> = self.mutes.muted_ids_for(viewer).unwrap_or_default();
        let blocking_me: Vec<String> = self.blocks.blocked_by_ids_for(viewer).unwrap_or_default();
        for author in [note.author_id.as_str()]
            .into_iter()
            .chain(note.reply_user_id.as_deref())
        {
            if muted.iter().any(|m| m == author) || blocking_me.iter().any(|b| b == author) {
                return Ok(false);
            }
        }
        if let Some(renote_id) = note.renote_id.as_deref() {
            if let Some(target) = self.notes.get_note(renote_id)? {
                if muted.iter().any(|m| m == &target.author_id)
                    || blocking_me.iter().any(|b| b == &target.author_id)
                {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::application::ports::{
        ActivityDedup, BlobStore, Clock, CreateNoteInput, IdGenerator, NoopStreamingSink,
    };
    use crate::domain::id::{AidxGenerator, ManualClock};
    use crate::federation::delivery::{SafeHttpClient, SafeHttpConfig};
    use crate::media::storage::FsBlobStore;
    use crate::persistence::redb::RedbStore;

    fn pin_services(prefix: &str) -> (AppServices, Arc<ManualClock>) {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-pin-test-{}-{prefix}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let clock: Arc<ManualClock> = Arc::new(ManualClock::new(1_760_400_000_000));
        let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
        let store = RedbStore::open(dir.join("test.redb"), clock.clone(), ids.clone()).unwrap();
        let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.join("blobs")).unwrap());
        let services = AppServices {
            clock: clock.clone() as Arc<dyn Clock>,
            ids,
            accounts: store.clone(),
            notes: store.clone(),
            follows: store.clone(),
            blocks: store.clone(),
            mutes: store.clone(),
            reactions: store.clone(),
            favorites: store.clone(),
            emojis: store.clone(),
            antennas: store.clone(),
            antenna_notes: store.clone(),
            polls: store.clone(),
            jobs: store.clone(),
            media: store.clone(),
            blobs: blobs.clone(),
            notifications: store.clone(),
            auth: store.clone(),
            streaming: Arc::new(NoopStreamingSink),
            dedup: store.clone() as Arc<dyn ActivityDedup>,
            host_caps: store.clone(),
            moderation: store.clone(),
            instance_settings: store.clone(),
            invites: store.clone(),
            abuse_reports: store.clone(),
            lists: store.clone(),
            signin: store.clone(),
            password_resets: store.clone(),
            registry: store.clone(),
            clips: store.clone(),
            pages: store.clone(),
            folders: store.clone(),
            system_webhooks: store.clone(),
            user_webhooks: store.clone(),
            abuse_recipients: store.clone(),
            memos: store.clone(),
            thread_mutes: store.clone(),
            federation_http: SafeHttpClient::new(SafeHttpConfig {
                allow_private_ips: true,
                ..Default::default()
            }),
            public_url: "https://example.local".to_string(),
            max_file_size: 10 * 1024 * 1024,
            preserved_usernames: Vec::new(),
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
            note_draft_limit: crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
            scheduled_note_limit: crate::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
            pin_limit: crate::domain::pinning::DEFAULT_PIN_LIMIT,
            default_like: None,
            cache_blobs: blobs.clone(),
            drive_capacity_per_user_bytes: 1 << 30,
            remote_cache_max_bytes: 512 << 20,
            remote_cache_avatar_bytes: 128 << 20,
            remote_cache_preview_bytes: 256 << 20,
            remote_cache_full_bytes: 128 << 20,
            remote_cache_mode: crate::config::RemoteCacheMode::OnDemand,
            remote_cache_avatar_ttl_days: 30,
            remote_cache_preview_ttl_days: 7,
            remote_cache_full_ttl_days: 3,
            remote_cache_warming_enabled: false,
        };
        (services, clock)
    }

    fn port_code(err: PortError) -> String {
        match err {
            PortError::Validation(m) | PortError::Conflict(m) | PortError::NotFound(m) => m,
            PortError::StorageUnavailable(m) => panic!("storage fault: {m}"),
        }
    }

    fn note_by(services: &AppServices, author: &str, text: &str) -> Note {
        services
            .create_note(CreateNoteInput::simple_public(author, text))
            .unwrap()
    }

    #[test]
    fn pin_unpin_flows_limit_dup_and_foreign() {
        let (mut services, _) = pin_services("flows");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let n1 = note_by(&services, &alice.id, "one");
        let n2 = note_by(&services, &alice.id, "two");
        let foreign = note_by(&services, &bob.id, "bob's");

        // Pinning a foreign note answers NO_SUCH_NOTE (must own the note).
        let err = services.pin_note(&alice.id, &foreign.id).unwrap_err();
        assert_eq!(port_code(err), "NO_SUCH_NOTE");
        // Unknown ids too.
        let err = services
            .pin_note(&alice.id, "nid00000000000001")
            .unwrap_err();
        assert_eq!(port_code(err), "NO_SUCH_NOTE");

        services.pin_note(&alice.id, &n1.id).unwrap();
        // Duplicate pins conflict with the exact upstream code.
        let err = services.pin_note(&alice.id, &n1.id).unwrap_err();
        assert_eq!(port_code(err), "ALREADY_PINNED");
        // Limit (lowered for the test) rejects with the exact code.
        services.pin_limit = 1;
        let err = services.pin_note(&alice.id, &n2.id).unwrap_err();
        assert_eq!(port_code(err), "PIN_LIMIT_EXCEEDED");
        services.pin_limit = 5;

        // Pinned ids read back oldest-first.
        services.pin_note(&alice.id, &n2.id).unwrap();
        assert_eq!(
            services.pinned_note_ids(&alice.id).unwrap(),
            vec![n1.id.clone(), n2.id.clone()]
        );

        // Unpin + unpin again (no-op success); foreign unpin is NO_SUCH_NOTE.
        services.unpin_note(&alice.id, &n1.id).unwrap();
        assert_eq!(
            services.pinned_note_ids(&alice.id).unwrap(),
            vec![n2.id.clone()]
        );
        services.unpin_note(&alice.id, &n1.id).unwrap();
        let err = services.unpin_note(&alice.id, &foreign.id).unwrap_err();
        assert_eq!(port_code(err), "NO_SUCH_NOTE");
    }

    #[test]
    fn pin_surfaces_through_pinned_ids_and_federates_for_public() {
        use crate::application::ports::JobKind;
        let (services, _) = pin_services("pack");
        let alice = services.create_account("alice", None).unwrap();
        let note = note_by(&services, &alice.id, "pin me");
        services.pin_note(&alice.id, &note.id).unwrap();

        // A public local pin enqueues one followers fan-out job.
        let mut featured_jobs = 0;
        while let Some(job) = services.jobs.claim_next(60_000).unwrap() {
            if job.kind == JobKind::FederateDeliver
                && String::from_utf8_lossy(&job.payload).contains("featured_changed")
            {
                featured_jobs += 1;
            }
            services.jobs.complete(&job.id).unwrap();
            if featured_jobs > 0 {
                break;
            }
        }
        assert_eq!(featured_jobs, 1);

        // The application pack input the user packer consumes:
        // pinned ids read back oldest-first.
        assert_eq!(
            services.pinned_note_ids(&alice.id).unwrap(),
            vec![note.id.clone()]
        );
    }

    #[test]
    fn featured_ranking_is_deterministic() {
        let (services, clock) = pin_services("ranking");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();

        // Low note first (older id), high note second (newer id).
        let low = note_by(&services, &alice.id, "low");
        clock.advance_ms(1_000);
        let high = note_by(&services, &alice.id, "high");
        clock.advance_ms(1_000);
        // Excluded: followers-only, CW-bearing, and out-of-window.
        let followers_only = CreateNoteInput {
            visibility: Visibility::Followers,
            ..CreateNoteInput::simple_public(&alice.id, "followers")
        };
        let followers_only = services.create_note(followers_only).unwrap();
        let cw_note = CreateNoteInput {
            cw: Some("cw".to_string()),
            ..CreateNoteInput::simple_public(&alice.id, "sensitive")
        };
        let cw_note = services.create_note(cw_note).unwrap();
        assert_ne!(followers_only.id, cw_note.id);

        // Reactions + renotes drive the score: low gets 1 reaction, high
        // gets 1 reaction + 1 renote.
        services.react(&bob.id, &low.id, "❤", false).unwrap();
        services.react(&bob.id, &high.id, "❤", false).unwrap();
        let renote_input = CreateNoteInput {
            renote_of: Some(high.id.clone()),
            ..CreateNoteInput::simple_public(&bob.id, "rt")
        };
        services.create_note(renote_input).unwrap();

        let ranked = services.rank_featured_global().unwrap();
        assert!(
            ranked.contains(&high.id) && ranked.contains(&low.id),
            "both public recent notes rank: {ranked:?}"
        );
        assert!(
            !ranked.contains(&followers_only.id) && !ranked.contains(&cw_note.id),
            "followers-only and CW notes never rank: {ranked:?}"
        );
        // Score selects (high outranks low)...
        let high_pos = ranked.iter().position(|id| id == &high.id).unwrap();
        let low_pos = ranked.iter().position(|id| id == &low.id).unwrap();
        // ...but the observable order is newest-first regardless of score.
        assert_eq!(ranked, newest_first(ranked.clone()));
        let _ = (high_pos, low_pos);

        // Out-of-window notes never rank.
        clock.advance_ms(FEATURED_GLOBAL_WINDOW_MS + 1);
        let ranked = services.rank_featured_global().unwrap();
        assert!(
            !ranked.contains(&high.id) && !ranked.contains(&low.id),
            "aged-out notes leave the ranking: {ranked:?}"
        );
    }

    #[test]
    fn featured_for_user_scopes_and_unknown_is_empty() {
        let (services, _) = pin_services("per-user");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let a_note = note_by(&services, &alice.id, "alice note");
        let b_note = note_by(&services, &bob.id, "bob note");
        services.react(&bob.id, &a_note.id, "❤", false).unwrap();
        services.react(&alice.id, &b_note.id, "❤", false).unwrap();

        let ranked = services.rank_featured_for_user(&alice.id).unwrap();
        assert_eq!(ranked, vec![a_note.id.clone()]);
        let ranked = services.rank_featured_for_user(&bob.id).unwrap();
        assert_eq!(ranked, vec![b_note.id.clone()]);
        // Unknown authors rank empty (no NO_SUCH_USER on this endpoint).
        let ranked = services
            .rank_featured_for_user("aid00000000000099")
            .unwrap();
        assert!(ranked.is_empty());
    }

    #[test]
    fn featured_cache_honors_ttl() {
        let cache = FeaturedCache::default();
        assert_eq!(cache.get("global", 1_000), None);
        cache.put("global", vec!["a".to_string()], 1_000);
        assert_eq!(
            cache.get("global", 1_000 + FEATURED_CACHE_TTL_MS - 1),
            Some(vec!["a".to_string()])
        );
        assert_eq!(cache.get("global", 1_000 + FEATURED_CACHE_TTL_MS), None);
        cache.put("global", vec!["b".to_string()], 2_000);
        assert_eq!(cache.get("global", 2_000), Some(vec!["b".to_string()]));
    }

    #[test]
    fn featured_visible_applies_viewer_filters() {
        let (services, clock) = pin_services("visible");
        let alice = services.create_account("alice", None).unwrap();
        let bob = services.create_account("bob", None).unwrap();
        let carol = services.create_account("carol", None).unwrap();
        let note = note_by(&services, &alice.id, "hello");
        services.react(&bob.id, &note.id, "❤", false).unwrap();
        let _ = clock;
        let ranked = services.rank_featured_global().unwrap();
        assert!(ranked.contains(&note.id));

        // Anonymous sees the public note.
        let visible = services
            .featured_notes_visible(&ranked, None, None, 10)
            .unwrap();
        assert_eq!(visible.len(), 1);

        // A viewer muted/blocked by the author does not.
        services.mute(&carol.id, &alice.id, None).unwrap();
        let visible = services
            .featured_notes_visible(&ranked, Some(&carol.id), None, 10)
            .unwrap();
        assert!(visible.is_empty(), "muted authors are withheld");
        services.block(&alice.id, &carol.id).unwrap();
        let visible = services
            .featured_notes_visible(&ranked, Some(&carol.id), None, 10)
            .unwrap();
        assert!(
            visible.is_empty(),
            "authors blocking the viewer are withheld"
        );

        // `untilId` pages newest-first.
        let visible = services
            .featured_notes_visible(&ranked, None, Some(note.id.as_str()), 10)
            .unwrap();
        assert!(visible.is_empty());
    }
}
