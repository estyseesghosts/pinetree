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
use crate::domain::clip::{Clip, ClipFavorite, ClipNote};
use crate::domain::collection::CollectionTier;
use crate::domain::id::DomainError;

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
        match self.clips.update_clip(clip)? {
            Some(clip) => Ok(clip),
            None => Err(PortError::NotFound("NO_SUCH_CLIP".to_string())),
        }
    }

    /// `clips/delete {clipId!}` → unit (memberships + favorites cascade in
    /// the store transaction).
    pub fn delete_clip(&self, owner_id: &str, clip_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(owner_id)?;
        if !self.clips.delete_clip(owner_id, clip_id)? {
            return Err(PortError::NotFound("NO_SUCH_CLIP".to_string()));
        }
        Ok(())
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
    pub fn visible_clip(
        &self,
        clip_id: &str,
        viewer_id: Option<&str>,
    ) -> Result<Clip, PortError> {
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
        let mut jobs = Vec::new();
        if let Some(job) = self.clip_membership_job(&clip, &note, true)? {
            jobs.push(job);
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
            if let Some(job) = self.clip_membership_job_for(&clip, note_id, &uri, false)? {
                jobs.push(job);
            }
        }
        if !self.clips.remove_clip_note(&clip.id, note_id, jobs)? {
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

    /// Whether clip membership changes federate right now (both
    /// switches on, per-clip flag on). Pure read; the serving gate
    /// enforces the same switches on fetch.
    fn clip_membership_federates(&self, clip: &Clip) -> Result<bool, PortError> {
        if !clip.federate {
            return Ok(false);
        }
        Ok(self
            .instance_settings
            .get()
            .map(|s| s.federate_clips)
            .unwrap_or(false))
    }

    /// Build the `Add`/`Remove` fan-out job for one membership change, or
    /// `None` when nothing federates (local note, local-only clip, or a
    /// remote author with no known inbox).
    fn clip_membership_job(
        &self,
        clip: &Clip,
        note: &crate::domain::note::Note,
        added: bool,
    ) -> Result<Option<crate::application::ports::NewJob>, PortError> {
        let base = self.public_url.trim_end_matches('/');
        let note_uri = note
            .uri
            .clone()
            .unwrap_or_else(|| format!("{base}/notes/{}", note.id));
        self.clip_membership_job_for(clip, &note.id, &note_uri, added)
    }

    fn clip_membership_job_for(
        &self,
        clip: &Clip,
        note_id: &str,
        note_uri: &str,
        added: bool,
    ) -> Result<Option<crate::application::ports::NewJob>, PortError> {
        use crate::federation::outbox::FederateDeliverPayload;
        if !self.clip_membership_federates(clip)? {
            return Ok(None);
        }
        // Only remote-authored notes have a remote inbox to notify; the
        // note must still exist locally (deleted notes never federate
        // membership changes — the collection gate withholds them too).
        let note = match self.notes.get_note(note_id)? {
            Some(n) if !n.is_deleted => n,
            _ => return Ok(None),
        };
        let author = match self.accounts.find_by_id(&note.author_id)? {
            Some(a) if !a.is_local() => a,
            _ => return Ok(None),
        };
        let inbox_url = match crate::federation::outbox::preferred_inbox(&author) {
            Some(inbox) => inbox,
            None => return Ok(None),
        };
        let activity_id = format!("clip-{}-{}-{note_id}", clip.id, self.ids.generate());
        FederateDeliverPayload::ClipMembershipChanged {
            activity_id,
            actor_id: clip.owner_id.clone(),
            clip_id: clip.id.clone(),
            note_uri: note_uri.to_string(),
            added,
            inbox_url,
        }
        .into_job(
            format!(
                "federate:clip-membership:{}:{note_id}:{}",
                clip.id,
                if added { "add" } else { "remove" }
            ),
            self.now_ms(),
        )
        .map(Some)
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
                Some(uri)
                    if clip.approved_actor_uris.iter().any(|a| a == uri) =>
                {
                    FetchDecision::Serve
                }
                _ => FetchDecision::Forbidden,
            },
        }
    }
}
