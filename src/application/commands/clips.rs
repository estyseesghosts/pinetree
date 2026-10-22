//! Clips (federated clips + pages handoff, Phases 1–2 local half).
//!
//! One application operation per Misskey `clips/*` endpoint, shared by the
//! Misskey adapter. Cap checks + inserts commit atomically in ONE store
//! call (same discipline as user lists); no federation fan-out happens
//! here — outbound Add/Remove delivery and collection serving live in the
//! federation layer and stay dormant unless the admin `federateClips`
//! switch AND the per-clip `federate` flag are both on.
//!
//! DIVERGENCE notes: pinetree publishes no per-clip stream channel
//! (upstream `ClipService` emits throttled `updated`/`deleted` on a clip
//! stream; no client in the compatibility profile subscribes to it);
//! pinetree notes carry no `clippedCount` counter (counts derive from the
//! `clip_notes` index at read).

use super::AppServices;

use crate::application::ports::PortError;
use crate::domain::clip::{Clip, ClipFavorite, ClipFollow, ClipNote};
use crate::domain::collection::CollectionTier;
use crate::domain::id::DomainError;
use crate::domain::notification::NotificationKind;

fn domain_err(e: DomainError) -> PortError {
    match e {
        DomainError::Validation(msg) => PortError::Validation(msg),
        DomainError::Conflict(msg) => PortError::Conflict(msg),
        DomainError::NotFound(msg) => PortError::NotFound(msg),
        DomainError::StorageUnavailable(msg) => PortError::StorageUnavailable(msg),
    }
}

impl AppServices {
    /// Owner-scoped clip lookup (`NO_SUCH_CLIP` on miss or foreign clip —
    /// upstream `ClipService` only ever looks clips up by `{id, userId}`).
    fn owner_clip(&self, owner_id: &str, clip_id: &str) -> Result<Clip, PortError> {
        self.accounts.get_owner(owner_id)?;
        match self.clips.get_clip(clip_id)? {
            Some(clip) if clip.owner_id == owner_id => Ok(clip),
            _ => Err(PortError::NotFound("NO_SUCH_CLIP".to_string())),
        }
    }

    /// `clips/create {name!, isPublic=false, description?}` (+ pinetree
    /// extension `visibility?`, `federate?`, `approvedActorUris?` for the
    /// federation tiers; all default closed).
    pub fn create_clip(
        &self,
        owner_id: &str,
        name: String,
        is_public: bool,
        description: Option<String>,
        visibility: Option<String>,
        federate: bool,
        approved_actor_uris: Vec<String>,
    ) -> Result<Clip, PortError> {
        self.accounts.get_owner(owner_id)?;
        Clip::validate_name(&name).map_err(domain_err)?;
        let description = description.filter(|d| !d.is_empty());
        Clip::validate_description(description.as_deref()).map_err(domain_err)?;
        let tier = match visibility {
            Some(v) => crate::domain::collection::validate_tier_name(&v).map_err(domain_err)?,
            None => crate::domain::collection::tier_from_is_public(is_public),
        };
        let mut clip = Clip::new(
            self.ids.generate(),
            owner_id.to_string(),
            name,
            description,
            is_public,
            self.now_ms(),
        );
        clip.tier = tier;
        clip.federate = federate;
        clip.approved_actor_uris = approved_actor_uris;
        self.clips.create_clip(clip)
    }

    /// `clips/update {clipId!, name?, isPublic?, description?}` (+ the same
    /// federation extension fields; absent fields untouched; `isPublic`
    /// re-derives the tier unless `visibility` is passed explicitly).
    pub fn update_clip(
        &self,
        owner_id: &str,
        clip_id: &str,
        name: Option<String>,
        is_public: Option<bool>,
        description: Option<Option<String>>,
        visibility: Option<String>,
        federate: Option<bool>,
        approved_actor_uris: Option<Vec<String>>,
    ) -> Result<Clip, PortError> {
        let mut clip = self.owner_clip(owner_id, clip_id)?;
        if let Some(name) = name {
            Clip::validate_name(&name).map_err(domain_err)?;
            clip.name = name;
        }
        if let Some(is_public) = is_public {
            clip.is_public = is_public;
            if visibility.is_none() {
                clip.tier = crate::domain::collection::tier_from_is_public(is_public);
            }
        }
        if let Some(description) = description {
            let description = description.filter(|d| !d.is_empty());
            Clip::validate_description(description.as_deref()).map_err(domain_err)?;
            clip.description = description;
        }
        if let Some(v) = visibility {
            clip.tier = crate::domain::collection::validate_tier_name(&v).map_err(domain_err)?;
        }
        if let Some(federate) = federate {
            clip.federate = federate;
        }
        if let Some(uris) = approved_actor_uris {
            clip.approved_actor_uris = uris;
        }
        let old = self.owner_clip(owner_id, clip_id)?;
        let was_open = old.tier.advertised() && old.federate;
        clip.updated_at_ms = self.now_ms();
        let mut jobs = Vec::new();
        if self.clip_sync_federates(&clip)? {
            jobs.push(
                crate::federation::outbox::FederateDeliverPayload::ClipUpdated {
                    clip_id: clip.id.clone(),
                    owner_id: owner_id.to_string(),
                    inbox_override: None,
                }
                .into_job(format!("federate:clip:{}:update", clip.id), self.now_ms())?,
            );
            // User-facing signal: metadata edits notify clip followers.
            for f in self.clips.followers_of_clip(&clip.id, 1000)? {
                if f.user_id == owner_id {
                    continue;
                }
                jobs.push(self.notify_job_full(
                    &f.user_id,
                    owner_id,
                    NotificationKind::ClipUpdated,
                    None,
                    None,
                    Some(clip.id.clone()),
                    None,
                    &format!("clip-updated:{}:{}", clip.id, self.ids.generate()),
                )?);
            }
        } else if was_open {
            // Tier flip to a more restrictive tier (or unfederated):
            // receivers evict + prune. Locally, remote follow rows are
            // pruned too (they stop receiving fan-out; re-follow when
            // re-listed). Local-user follows stay (possession + read
            // access is unchanged for co-located readers).
            for f in self.clips.followers_of_clip(&clip.id, 10_000)? {
                if f.user_id == owner_id {
                    continue;
                }
                if let Some(acc) = self.accounts.find_by_id(&f.user_id)? {
                    if !acc.is_local() {
                        self.clips.unfollow_clip(&f.user_id, &clip.id)?;
                    }
                }
            }
            jobs.push(
                crate::federation::outbox::FederateDeliverPayload::ClipDeleted {
                    clip_id: clip.id.clone(),
                    owner_id: owner_id.to_string(),
                    clip_uri: format!(
                        "{}/clips/{}",
                        self.public_url.trim_end_matches('/'),
                        clip.id
                    ),
                    inbox_override: None,
                }
                .into_job(format!("federate:clip:{}:delete", clip.id), self.now_ms())?,
            );
        }
        match self.clips.update_clip(clip, jobs)? {
            Some(clip) => Ok(clip),
            None => Err(PortError::NotFound("NO_SUCH_CLIP".to_string())),
        }
    }

    /// `clips/delete {clipId!}` → unit (memberships + favorites + follows
    /// cascade in the store transaction; federated clips fan out
    /// `Delete(Collection)` + write a tombstone so receivers distinguish
    /// "deleted, drop your copy" from 404).
    pub fn delete_clip(&self, owner_id: &str, clip_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(owner_id)?;
        let clip = self.owner_clip(owner_id, clip_id)?;
        let base = self.public_url.trim_end_matches('/').to_string();
        let clip_uri = format!("{base}/clips/{clip_id}");
        let mut jobs: Vec<crate::application::ports::NewJob> = Vec::new();
        if self.clip_sync_federates(&clip)? {
            jobs.push(
                crate::federation::outbox::FederateDeliverPayload::ClipDeleted {
                    clip_id: clip_id.to_string(),
                    owner_id: owner_id.to_string(),
                    clip_uri: clip_uri.clone(),
                    inbox_override: None,
                }
                .into_job(format!("federate:clip:{clip_id}:delete"), self.now_ms())?,
            );
            // Notify followers + all member-note authors (quieter rule:
            // membership edits notify authors only; delete/restrict
            // notifies followers too).
            let mut notifiees: Vec<String> = self
                .clips
                .followers_of_clip(clip_id, 1000)?
                .into_iter()
                .map(|f| f.user_id)
                .collect();
            for note_id in self.clips.clip_note_ids(clip_id)? {
                if let Some(note) = self.notes.get_note(&note_id)? {
                    if !notifiees.contains(&note.author_id) {
                        notifiees.push(note.author_id);
                    }
                }
            }
            for n in notifiees {
                if n == owner_id {
                    continue;
                }
                jobs.push(self.notify_job_full(
                    &n,
                    owner_id,
                    NotificationKind::ClipUpdated,
                    None,
                    None,
                    Some(clip_id.to_string()),
                    None,
                    &format!("clip-deleted:{clip_id}:{n}:{}", self.ids.generate()),
                )?);
            }
        }
        if !self.clips.delete_clip(owner_id, clip_id, jobs)? {
            return Err(PortError::NotFound("NO_SUCH_CLIP".to_string()));
        }
        self.clips.record_clip_tombstone(&clip_uri, self.now_ms())?;
        Ok(())
    }

    /// Whether clip sync (`Update`/`Delete(Collection)`) fans out right now:
    /// `Listed` + per-clip flag + admin switch (tightened vs membership
    /// promptness — unlisted-with-flag loses broadcast; capability/direct
    /// fetch still works. Documented behavior change).
    fn clip_sync_federates(&self, clip: &Clip) -> Result<bool, PortError> {
        if !clip.federate {
            return Ok(false);
        }
        if clip.tier != crate::domain::collection::CollectionTier::Listed {
            return Ok(false);
        }
        Ok(self
            .instance_settings
            .get()
            .map(|s| s.federate_clips)
            .unwrap_or(false))
    }

    /// `clips/follow {clipId!}` → unit. Anyone who can read the clip may
    /// follow (possession of the URI + read access is the gate — no
    /// separate capability). Idempotent: already-following succeeds.
    /// Permission `write:clip-favorite` (symmetry with clip favorites;
    /// recorded choice).
    pub fn follow_clip(&self, user_id: &str, clip_id: &str) -> Result<bool, PortError> {
        self.accounts.get_owner(user_id)?;
        self.visible_clip(clip_id, Some(user_id))?;
        let follow = ClipFollow {
            id: self.ids.generate(),
            user_id: user_id.to_string(),
            clip_id: clip_id.to_string(),
            created_at_ms: self.now_ms(),
        };
        Ok(self.clips.follow_clip(follow)?)
    }

    /// `clips/unfollow {clipId!}` → unit. Idempotent.
    pub fn unfollow_clip(&self, user_id: &str, clip_id: &str) -> Result<bool, PortError> {
        self.accounts.get_owner(user_id)?;
        if self.clips.get_clip(clip_id)?.is_none() {
            return Err(PortError::NotFound("NO_SUCH_CLIP".to_string()));
        }
        Ok(self.clips.unfollow_clip(user_id, clip_id)?)
    }

    /// Record an inbound `Follow` targeting a local clip collection URI.
    /// Authority = signer == follower; deduped; pruned when the clip goes
    /// non-listed/deleted (pruning happens at the restrict/delete paths
    /// via cascade — remote rows simply stop receiving fan-out).
    pub fn handle_inbound_clip_follow(
        &self,
        activity_id: &str,
        follower_id: &str,
        clip_id: &str,
    ) -> Result<bool, PortError> {
        if self.clips.get_clip(clip_id)?.is_none() {
            return Ok(false);
        }
        // Anyone who can read the clip may follow (possession of the URI
        // + read access is the gate — no separate capability).
        if self.visible_clip(clip_id, Some(follower_id)).is_err() {
            return Ok(false);
        }
        if self.inbound_host_blocked(follower_id) {
            return Ok(false);
        }
        if self.dedup.check_and_record(activity_id)? {
            return Ok(true);
        }
        let follow = ClipFollow {
            id: self.ids.generate(),
            user_id: follower_id.to_string(),
            clip_id: clip_id.to_string(),
            created_at_ms: self.now_ms(),
        };
        match self.clips.follow_clip(follow) {
            Ok(_) => Ok(true),
            Err(PortError::Conflict(_)) => Ok(true),
            Err(e) => Err(e),
        }
    }

    /// Record an inbound `Undo(Follow)` targeting a local clip URI.
    pub fn handle_inbound_clip_unfollow(
        &self,
        activity_id: &str,
        follower_id: &str,
        clip_id: &str,
    ) -> Result<bool, PortError> {
        if self.dedup.check_and_record(activity_id)? {
            return Ok(true);
        }
        match self.clips.unfollow_clip(follower_id, clip_id) {
            Ok(_) => Ok(true),
            Err(e) => Err(e),
        }
    }

    /// `clips/list` (own clips, newest-first). The store read is bounded
    /// by the per-owner cap; cursor/limit slicing happens here.
    pub fn list_clips(
        &self,
        owner_id: &str,
        limit: usize,
        since_id: Option<&str>,
        until_id: Option<&str>,
        since_date: Option<i64>,
        until_date: Option<i64>,
    ) -> Result<Vec<Clip>, PortError> {
        self.accounts.get_owner(owner_id)?;
        let mut clips = self.clips.clips_by_owner(owner_id)?;
        clips.retain(|c| {
            if let Some(s) = since_id {
                if c.id.as_str() <= s {
                    return false;
                }
            }
            if let Some(u) = until_id {
                if c.id.as_str() >= u {
                    return false;
                }
            }
            if let Some(sd) = since_date {
                if c.created_at_ms <= sd {
                    return false;
                }
            }
            if let Some(ud) = until_date {
                if c.created_at_ms >= ud {
                    return false;
                }
            }
            true
        });
        clips.truncate(limit);
        Ok(clips)
    }

    /// Shared private-clip gate for `show`/`notes`/`favorite`: a private
    /// clip is `NO_SUCH_CLIP` to anonymous and non-owner readers (upstream
    /// parity; `unfavorite` deliberately skips this check).
    pub fn visible_clip(&self, clip_id: &str, viewer_id: Option<&str>) -> Result<Clip, PortError> {
        match self.clips.get_clip(clip_id)? {
            Some(clip) if clip.is_public => Ok(clip),
            Some(clip) if viewer_id == Some(clip.owner_id.as_str()) => Ok(clip),
            _ => Err(PortError::NotFound("NO_SUCH_CLIP".to_string())),
        }
    }

    /// `clips/notes {clipId!, ...}` → member note ids, newest-first
    /// (visibility/mute/block filtering + search happen at the endpoint
    /// pack layer, which owns the viewer).
    pub fn clip_member_note_ids(
        &self,
        clip_id: &str,
        viewer_id: Option<&str>,
        limit: usize,
        since_id: Option<&str>,
        until_id: Option<&str>,
        since_date: Option<i64>,
        until_date: Option<i64>,
    ) -> Result<Vec<String>, PortError> {
        let clip = self.visible_clip(clip_id, viewer_id)?;
        let mut ids = self.clips.clip_note_ids(&clip.id)?;
        if since_id.is_some() || until_id.is_some() || since_date.is_some() || until_date.is_some()
        {
            let mut kept = Vec::with_capacity(ids.len());
            for id in &ids {
                if let Some(note) = self.notes.get_note(id)? {
                    let created = self.ids.parse_time(&note.id).unwrap_or(0);
                    if let Some(s) = since_id {
                        if note.id.as_str() <= s {
                            continue;
                        }
                    }
                    if let Some(u) = until_id {
                        if note.id.as_str() >= u {
                            continue;
                        }
                    }
                    if let Some(sd) = since_date {
                        if created <= sd {
                            continue;
                        }
                    }
                    if let Some(ud) = until_date {
                        if created >= ud {
                            continue;
                        }
                    }
                    kept.push(id.clone());
                }
            }
            ids = kept;
        }
        ids.truncate(limit);
        Ok(ids)
    }

    /// `clips/add-note {clipId!, noteId!}`. No visibility check on the
    /// note (upstream parity — any existing note id clips). Duplicate →
    /// `ALREADY_CLIPPED`, full clip → `TOO_MANY_CLIP_NOTES`, unknown note
    /// → `NO_SUCH_NOTE`. Federated clips enqueue an `Add` to the note
    /// author's inbox (remote authors only) in the same transaction.
    pub fn add_clip_note(
        &self,
        owner_id: &str,
        clip_id: &str,
        note_id: &str,
    ) -> Result<(), PortError> {
        let clip = self.owner_clip(owner_id, clip_id)?;
        let note = match self.notes.get_note(note_id)? {
            Some(note) => note,
            None => return Err(PortError::NotFound("NO_SUCH_NOTE".to_string())),
        };
        let mut jobs = self.clip_membership_jobs(&clip, &note, true)?;
        // Membership edits notify the note's author (quieter rule:
        // followers are NOT spammed on every membership edit).
        if note.author_id != owner_id {
            jobs.push(self.notify_job(
                &note.author_id,
                owner_id,
                NotificationKind::ClipUpdated,
                Some(note.id.clone()),
                None,
                &format!(
                    "clip-member:{}:{}:{}",
                    clip.id,
                    note.id,
                    self.ids.generate()
                ),
            )?);
        }
        let row = ClipNote {
            id: self.ids.generate(),
            clip_id: clip.id.clone(),
            note_id: note.id.clone(),
            created_at_ms: self.now_ms(),
        };
        match self.clips.add_clip_note(row, self.now_ms(), jobs) {
            Ok(_) => Ok(()),
            Err(PortError::Conflict(code)) => Err(PortError::Conflict(code)),
            Err(e) => Err(e),
        }
    }

    /// `clips/remove-note {clipId!, noteId!}`. A missing pair is
    /// `NO_SUCH_NOTE` (upstream parity — covers never-added and never
    /// existed alike). Federated clips enqueue a `Remove` to the note
    /// author's inbox (remote authors only) in the same transaction.
    pub fn remove_clip_note(
        &self,
        owner_id: &str,
        clip_id: &str,
        note_id: &str,
    ) -> Result<(), PortError> {
        let clip = self.owner_clip(owner_id, clip_id)?;
        let note_uri: Option<String> = match self.notes.get_note(note_id)? {
            Some(note) => Some(note.uri.clone().unwrap_or_else(|| {
                format!(
                    "{}/notes/{}",
                    self.public_url.trim_end_matches('/'),
                    note.id
                )
            })),
            None => None,
        };
        // The `Remove` ships the note URI even when the row is already
        // gone: receivers apply idempotently, and the URI is stable.
        let mut jobs = Vec::new();
        if let Some(uri) = note_uri {
            jobs = self.clip_membership_jobs_for_uri(&clip, note_id, &uri, false)?;
            if let Some(note) = self.notes.get_note(note_id)? {
                if note.author_id != owner_id {
                    jobs.push(self.notify_job(
                        &note.author_id,
                        owner_id,
                        NotificationKind::ClipUpdated,
                        Some(note.id.clone()),
                        None,
                        &format!(
                            "clip-member:{}:{}:{}",
                            clip.id,
                            note.id,
                            self.ids.generate()
                        ),
                    )?);
                }
            }
        }
        if !self
            .clips
            .remove_clip_note(&clip.id, note_id, self.now_ms(), jobs)?
        {
            return Err(PortError::NotFound("NO_SUCH_NOTE".to_string()));
        }
        Ok(())
    }

    /// `clips/favorite {clipId!}`. Unknown clip, or a private clip to a
    /// non-owner, is `NO_SUCH_CLIP`; duplicate → `ALREADY_FAVORITED`.
    pub fn favorite_clip(&self, user_id: &str, clip_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        self.visible_clip(clip_id, Some(user_id))?;
        let fav = ClipFavorite {
            id: self.ids.generate(),
            user_id: user_id.to_string(),
            clip_id: clip_id.to_string(),
            created_at_ms: self.now_ms(),
        };
        if !self.clips.favorite_clip(fav)? {
            return Err(PortError::Conflict("ALREADY_FAVORITED".to_string()));
        }
        Ok(())
    }

    /// `clips/unfavorite {clipId!}`. Unknown clip → `NO_SUCH_CLIP` (no
    /// visibility check, upstream parity); not favorited →
    /// `NOT_FAVORITED`.
    pub fn unfavorite_clip(&self, user_id: &str, clip_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        if self.clips.get_clip(clip_id)?.is_none() {
            return Err(PortError::NotFound("NO_SUCH_CLIP".to_string()));
        }
        if !self.clips.unfavorite_clip(user_id, clip_id)? {
            return Err(PortError::Conflict("NOT_FAVORITED".to_string()));
        }
        Ok(())
    }

    /// Build the `Add`/`Remove` fan-out jobs for one membership change:
    /// note author (direct, existing) + owner followers (new, mirrors
    /// pinned-note delivery) + clip followers (new). One job per remote
    /// inbox sharing a single `activity_id` so receivers dedupe; the
    /// existing idempotency-key scheme carries over per inbox.
    fn clip_membership_jobs(
        &self,
        clip: &Clip,
        note: &crate::domain::note::Note,
        added: bool,
    ) -> Result<Vec<crate::application::ports::NewJob>, PortError> {
        let base = self.public_url.trim_end_matches('/');
        let note_uri = note
            .uri
            .clone()
            .unwrap_or_else(|| format!("{base}/notes/{}", note.id));
        self.clip_membership_jobs_for_uri(clip, &note.id, &note_uri, added)
    }

    fn clip_membership_jobs_for_uri(
        &self,
        clip: &Clip,
        note_id: &str,
        note_uri: &str,
        added: bool,
    ) -> Result<Vec<crate::application::ports::NewJob>, PortError> {
        use crate::federation::outbox::FederateDeliverPayload;
        use std::collections::HashSet;
        if !self.clip_sync_federates(clip)? {
            return Ok(Vec::new());
        }
        // Deleted notes never federate membership changes — the
        // collection gate withholds them too.
        if let Some(n) = self.notes.get_note(note_id)? {
            if n.is_deleted {
                return Ok(Vec::new());
            }
        }
        let mut inbox_urls: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut push_inbox = |url: Option<String>| {
            if let Some(u) = url {
                if seen.insert(u.clone()) {
                    inbox_urls.push(u);
                }
            }
        };
        // Note author (direct, existing behavior).
        if let Some(n) = self.notes.get_note(note_id)? {
            if let Some(author) = self.accounts.find_by_id(&n.author_id)? {
                if !author.is_local() {
                    push_inbox(crate::federation::outbox::preferred_inbox(&author));
                }
            }
        }
        // Owner followers (mirrors pinned-note delivery).
        for f in self
            .follows
            .followers_of(&clip.owner_id, None, 1000)
            .unwrap_or_default()
        {
            if let Ok(Some(a)) = self.accounts.find_by_id(&f.follower_id) {
                if !a.is_local() {
                    push_inbox(crate::federation::outbox::preferred_inbox(&a));
                }
            }
        }
        // Clip followers (remote rows join the fan-out set).
        for f in self.clips.followers_of_clip(&clip.id, 1000)? {
            if let Ok(Some(a)) = self.accounts.find_by_id(&f.user_id) {
                if !a.is_local() {
                    push_inbox(crate::federation::outbox::preferred_inbox(&a));
                }
            }
        }
        if inbox_urls.is_empty() {
            return Ok(Vec::new());
        }
        let activity_id = format!("clip-{}-{}-{note_id}", clip.id, self.ids.generate());
        let mut out = Vec::with_capacity(inbox_urls.len());
        for inbox_url in inbox_urls {
            out.push(
                FederateDeliverPayload::ClipMembershipChanged {
                    activity_id: activity_id.clone(),
                    actor_id: clip.owner_id.clone(),
                    clip_id: clip.id.clone(),
                    note_uri: note_uri.to_string(),
                    added,
                    inbox_url: inbox_url.clone(),
                }
                .into_job(
                    format!(
                        "federate:clip-membership:{}:{note_id}:{}:{}",
                        clip.id,
                        if added { "add" } else { "remove" },
                        inbox_url
                    ),
                    self.now_ms(),
                )?,
            );
        }
        Ok(out)
    }

    /// `clips/my-favorites` → favorited clips, newest-first (favorites of
    /// deleted clips are skipped, like upstream's packed-null filtering).
    pub fn clip_favorites(&self, user_id: &str) -> Result<Vec<Clip>, PortError> {
        self.accounts.get_owner(user_id)?;
        let mut out = Vec::new();
        for fav in self.clips.clip_favorites_by_user(user_id)? {
            if let Some(clip) = self.clips.get_clip(&fav.clip_id)? {
                out.push(clip);
            }
        }
        Ok(out)
    }

    /// Federation authorization callback for one clip collection: given the    /// verified fetcher (actor URI or anonymous), decide Serve / Forbidden /
    /// Hidden. Pure function over the clip row — no I/O, no object-type
    /// logic below the fetch gate.
    ///
    /// * global kill (`federate_switch_on == false`) or per-clip flag off
    ///   ⇒ `Hidden` (local-only, zero protocol footprint);
    /// * `Private` ⇒ `Hidden`; `LocalApproved` ⇒ `Forbidden`;
    /// * `Unlisted`/`Listed` ⇒ `Serve` (bearer / advertised);
    /// * `RemoteApproved` ⇒ `Serve` only to a listed approver actor,
    ///   else `Forbidden` (the gate collapses anonymous/stranger fetches
    ///   to 404 so existence stays unconfirmed).
    pub fn authorize_clip_fetch(
        clip: &Clip,
        federate_switch_on: bool,
        fetcher_actor_uri: Option<&str>,
    ) -> crate::federation::fetch_gate::FetchDecision {
        use crate::federation::fetch_gate::FetchDecision;
        if !federate_switch_on || !clip.federate {
            return FetchDecision::Hidden;
        }
        match clip.tier {
            CollectionTier::Private => FetchDecision::Hidden,
            CollectionTier::LocalApproved => FetchDecision::Forbidden,
            CollectionTier::Unlisted | CollectionTier::Listed => FetchDecision::Serve,
            CollectionTier::RemoteApproved => match fetcher_actor_uri {
                Some(uri) if clip.approved_actor_uris.iter().any(|a| a == uri) => {
                    FetchDecision::Serve
                }
                _ => FetchDecision::Forbidden,
            },
        }
    }
}
