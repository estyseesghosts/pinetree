//! `chat/*` application ops (WP2-part-B).
//!
//! Ships rooms / invitations / messages / history REST surfaces so clients
//! stop 404ing (19 ops; "chat/* ~25" upstream counts_history-adjacent arms
//! and per-room variants — every functional surface below is covered). No
//! streaming events are emitted: `chatUser`/`chatRoom` channels stay
//! `UNSUPPORTED_CHANNEL` (WP4 decision).
//!
//! Error-code contract (Misskey-style codes/messages; UUIDs pinetree-fixed
//! until the parent copies them from the misskey checkout):
//! - unknown room (or invisible to a non-member) → `NotFound("NO_SUCH_ROOM")`
//!   ("No such room.");
//! - unknown message → `NotFound("NO_SUCH_MESSAGE")` ("No such message.");
//! - missing invitation → `NotFound("NO_SUCH_INVITATION")`
//!   ("No such invitation.");
//! - bad invitee / 1:1 target → `NotFound("NO_SUCH_USER")` (existing code);
//! - non-owner room update/delete, editing another member's message →
//!   `Validation("ACCESS_DENIED")` (existing code);
//! - empty message (no text, no file) → `Validation("INVALID_PARAM …")`.
//! Joins, leaves-of-absence, follows of invitations, mutes, and reads are
//! idempotent (no `ALREADY_*` errors).
//!
//! Storage: [`ChatStore`] is the seam the parent promotes to `ports.rs` /
//! `redb`; [`MemChatStore`] is the atomic in-memory backend (one lock =
//! state + jobs commit). Invitations emit no `Notify` jobs: there is no
//! `chatRoomInvitation` notification variant in this tree (it sits on the
//! unemitted list); invitees discover invitations via
//! `rooms/invitations/inbox`.

use std::collections::HashMap;
use std::sync::RwLock;

use super::AppServices;
use crate::application::commands::channels::IdCursor;
use crate::application::ports::{NewJob, PortError};
use crate::domain::chat_room::{
    validate_room_description, validate_room_name, ChatInvitation, ChatMessage, ChatRoom,
    ChatRoomMember, ChatRoomRead,
};

// ---------------------------------------------------------------------------
// Storage seam (parent promotes to ports.rs / redb).
// ---------------------------------------------------------------------------

/// Chat storage seam. Every mutating call commits state + `jobs`
/// atomically (one transaction on durable backends).
pub trait ChatStore: Send + Sync {
    fn insert_room(&self, room: ChatRoom, jobs: Vec<NewJob>) -> Result<ChatRoom, PortError>;
    fn update_room(&self, room: ChatRoom, jobs: Vec<NewJob>) -> Result<ChatRoom, PortError>;
    /// Delete a room + cascade (members, invitations, messages, reads).
    /// Returns true when a row existed.
    fn delete_room(&self, room_id: &str, jobs: Vec<NewJob>) -> Result<bool, PortError>;
    fn get_room(&self, room_id: &str) -> Result<Option<ChatRoom>, PortError>;
    /// All rooms, newest-first (callers filter by membership).
    fn all_rooms(&self) -> Result<Vec<ChatRoom>, PortError>;

    /// Upsert membership (join / mute flag flip). Returns true when the
    /// membership row was newly inserted.
    fn upsert_member(&self, member: ChatRoomMember, jobs: Vec<NewJob>) -> Result<bool, PortError>;
    /// Remove membership (leave). Returns true when a row existed.
    fn remove_member(
        &self,
        room_id: &str,
        user_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError>;
    fn get_member(&self, room_id: &str, user_id: &str) -> Result<Option<ChatRoomMember>, PortError>;
    /// Members of one room, oldest-first (join order).
    fn members_of(&self, room_id: &str) -> Result<Vec<ChatRoomMember>, PortError>;
    /// Rooms one user belongs to, newest-first.
    fn rooms_of(&self, user_id: &str) -> Result<Vec<ChatRoom>, PortError>;
    /// Transfer ownership (owner-leaves-with-members case).
    fn set_room_owner(
        &self,
        room_id: &str,
        owner_id: &str,
        now_ms: i64,
        jobs: Vec<NewJob>,
    ) -> Result<ChatRoom, PortError>;

    /// Insert (or refresh) an invitation; unique on `(room_id, invitee_id)`.
    fn upsert_invitation(
        &self,
        invitation: ChatInvitation,
        jobs: Vec<NewJob>,
    ) -> Result<ChatInvitation, PortError>;
    /// Delete one invitation. Returns true when a row existed.
    fn delete_invitation(
        &self,
        room_id: &str,
        invitee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError>;
    fn get_invitation(
        &self,
        room_id: &str,
        invitee_id: &str,
    ) -> Result<Option<ChatInvitation>, PortError>;
    /// Invitations addressed to `invitee_id`, newest-first (inbox).
    fn invitations_to(&self, invitee_id: &str) -> Result<Vec<ChatInvitation>, PortError>;
    /// Invitations sent by `inviter_id`, newest-first (outbox).
    fn invitations_from(&self, inviter_id: &str) -> Result<Vec<ChatInvitation>, PortError>;
    /// Most recent pending invitation involving `user_a` + `user_b`
    /// (either direction), for the `accept { userId }` / `decline { userId }`
    /// upstream param shape.
    fn latest_invitation_between(
        &self,
        user_a: &str,
        user_b: &str,
    ) -> Result<Option<ChatInvitation>, PortError>;

    fn insert_message(&self, message: ChatMessage, jobs: Vec<NewJob>)
        -> Result<ChatMessage, PortError>;
    fn update_message(
        &self,
        message: ChatMessage,
        jobs: Vec<NewJob>,
    ) -> Result<ChatMessage, PortError>;
    /// Soft-delete (row kept, `is_deleted` set). Returns true on change.
    fn delete_message(&self, message_id: &str, jobs: Vec<NewJob>) -> Result<bool, PortError>;
    fn get_message(&self, message_id: &str) -> Result<Option<ChatMessage>, PortError>;
    /// Live messages of one room, newest-first (unbounded; callers page).
    fn messages_in(&self, room_id: &str) -> Result<Vec<ChatMessage>, PortError>;

    /// Record a read watermark (monotonic per `(room, user)` by message id;
    /// regressions are ignored). Returns the stored row.
    fn mark_read(
        &self,
        room_id: &str,
        user_id: &str,
        message_id: &str,
        now_ms: i64,
        jobs: Vec<NewJob>,
    ) -> Result<ChatRoomRead, PortError>;
    fn get_read(&self, room_id: &str, user_id: &str) -> Result<Option<ChatRoomRead>, PortError>;
}

#[derive(Debug, Default)]
struct MemChatState {
    rooms: HashMap<String, ChatRoom>,
    members: HashMap<(String, String), ChatRoomMember>,
    invitations: HashMap<(String, String), ChatInvitation>,
    messages: HashMap<String, ChatMessage>,
    reads: HashMap<(String, String), ChatRoomRead>,
    outbox: Vec<NewJob>,
}

/// Atomic in-memory [`ChatStore`] (single lock = one transaction).
#[derive(Debug, Default)]
pub struct MemChatStore {
    state: RwLock<MemChatState>,
}

impl MemChatStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Jobs committed alongside state (test drain / parent worker seam).
    pub fn drain_jobs(&self) -> Vec<NewJob> {
        self.state
            .write()
            .expect("mem chat store poisoned")
            .outbox
            .drain(..)
            .collect()
    }

    fn lock(&self) -> std::sync::RwLockWriteGuard<'_, MemChatState> {
        self.state.write().expect("mem chat store poisoned")
    }
}

fn newest_first_rooms(mut rooms: Vec<ChatRoom>) -> Vec<ChatRoom> {
    rooms.sort_by(|a, b| {
        b.created_at_ms
            .cmp(&a.created_at_ms)
            .then(b.id.cmp(&a.id))
    });
    rooms
}

fn newest_first_messages(mut messages: Vec<ChatMessage>) -> Vec<ChatMessage> {
    messages.sort_by(|a, b| {
        b.created_at_ms
            .cmp(&a.created_at_ms)
            .then(b.id.cmp(&a.id))
    });
    messages
}

impl ChatStore for MemChatStore {
    fn insert_room(&self, room: ChatRoom, jobs: Vec<NewJob>) -> Result<ChatRoom, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        st.rooms.insert(room.id.clone(), room.clone());
        st.outbox.extend(jobs);
        Ok(room)
    }

    fn update_room(&self, room: ChatRoom, jobs: Vec<NewJob>) -> Result<ChatRoom, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        if !st.rooms.contains_key(&room.id) {
            return Err(PortError::NotFound("NO_SUCH_ROOM".to_string()));
        }
        st.rooms.insert(room.id.clone(), room.clone());
        st.outbox.extend(jobs);
        Ok(room)
    }

    fn delete_room(&self, room_id: &str, jobs: Vec<NewJob>) -> Result<bool, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        if st.rooms.remove(room_id).is_none() {
            return Ok(false);
        }
        st.members.retain(|(rid, _), _| rid != room_id);
        st.invitations.retain(|(rid, _), _| rid != room_id);
        st.messages.retain(|_, m| m.room_id != room_id);
        st.reads.retain(|(rid, _), _| rid != room_id);
        st.outbox.extend(jobs);
        Ok(true)
    }

    fn get_room(&self, room_id: &str) -> Result<Option<ChatRoom>, PortError> {
        Ok(self
            .state
            .read()
            .expect("mem chat store poisoned")
            .rooms
            .get(room_id)
            .cloned())
    }

    fn all_rooms(&self) -> Result<Vec<ChatRoom>, PortError> {
        Ok(newest_first_rooms(
            self.state
                .read()
                .expect("mem chat store poisoned")
                .rooms
                .values()
                .cloned()
                .collect(),
        ))
    }

    fn upsert_member(&self, member: ChatRoomMember, jobs: Vec<NewJob>) -> Result<bool, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        let key = (member.room_id.clone(), member.user_id.clone());
        let is_new = !st.members.contains_key(&key);
        st.members.insert(key, member);
        st.outbox.extend(jobs);
        Ok(is_new)
    }

    fn remove_member(
        &self,
        room_id: &str,
        user_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        let existed = st
            .members
            .remove(&(room_id.to_string(), user_id.to_string()))
            .is_some();
        if existed {
            st.outbox.extend(jobs);
        }
        Ok(existed)
    }

    fn get_member(
        &self,
        room_id: &str,
        user_id: &str,
    ) -> Result<Option<ChatRoomMember>, PortError> {
        Ok(self
            .state
            .read()
            .expect("mem chat store poisoned")
            .members
            .get(&(room_id.to_string(), user_id.to_string()))
            .cloned())
    }

    fn members_of(&self, room_id: &str) -> Result<Vec<ChatRoomMember>, PortError> {
        let st = self.state.read().expect("mem chat store poisoned");
        let mut out: Vec<ChatRoomMember> = st
            .members
            .values()
            .filter(|m| m.room_id == room_id)
            .cloned()
            .collect();
        out.sort_by(|a, b| {
            a.joined_at_ms
                .cmp(&b.joined_at_ms)
                .then(a.user_id.cmp(&b.user_id))
        });
        Ok(out)
    }

    fn rooms_of(&self, user_id: &str) -> Result<Vec<ChatRoom>, PortError> {
        let st = self.state.read().expect("mem chat store poisoned");
        let rooms: Vec<ChatRoom> = st
            .members
            .values()
            .filter(|m| m.user_id == user_id)
            .filter_map(|m| st.rooms.get(&m.room_id).cloned())
            .collect();
        Ok(newest_first_rooms(rooms))
    }

    fn set_room_owner(
        &self,
        room_id: &str,
        owner_id: &str,
        now_ms: i64,
        jobs: Vec<NewJob>,
    ) -> Result<ChatRoom, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        let mut room = st
            .rooms
            .get(room_id)
            .cloned()
            .ok_or_else(|| PortError::NotFound("NO_SUCH_ROOM".to_string()))?;
        room.owner_id = owner_id.to_string();
        room.updated_at_ms = now_ms;
        st.rooms.insert(room_id.to_string(), room.clone());
        st.outbox.extend(jobs);
        Ok(room)
    }

    fn upsert_invitation(
        &self,
        invitation: ChatInvitation,
        jobs: Vec<NewJob>,
    ) -> Result<ChatInvitation, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        let key = (invitation.room_id.clone(), invitation.invitee_id.clone());
        st.invitations.insert(key, invitation.clone());
        st.outbox.extend(jobs);
        Ok(invitation)
    }

    fn delete_invitation(
        &self,
        room_id: &str,
        invitee_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        let existed = st
            .invitations
            .remove(&(room_id.to_string(), invitee_id.to_string()))
            .is_some();
        if existed {
            st.outbox.extend(jobs);
        }
        Ok(existed)
    }

    fn get_invitation(
        &self,
        room_id: &str,
        invitee_id: &str,
    ) -> Result<Option<ChatInvitation>, PortError> {
        Ok(self
            .state
            .read()
            .expect("mem chat store poisoned")
            .invitations
            .get(&(room_id.to_string(), invitee_id.to_string()))
            .cloned())
    }

    fn invitations_to(&self, invitee_id: &str) -> Result<Vec<ChatInvitation>, PortError> {
        let st = self.state.read().expect("mem chat store poisoned");
        let mut out: Vec<ChatInvitation> = st
            .invitations
            .values()
            .filter(|i| i.invitee_id == invitee_id)
            .cloned()
            .collect();
        out.sort_by(|a, b| {
            b.created_at_ms
                .cmp(&a.created_at_ms)
                .then(b.id.cmp(&a.id))
        });
        Ok(out)
    }

    fn invitations_from(&self, inviter_id: &str) -> Result<Vec<ChatInvitation>, PortError> {
        let st = self.state.read().expect("mem chat store poisoned");
        let mut out: Vec<ChatInvitation> = st
            .invitations
            .values()
            .filter(|i| i.inviter_id == inviter_id)
            .cloned()
            .collect();
        out.sort_by(|a, b| {
            b.created_at_ms
                .cmp(&a.created_at_ms)
                .then(b.id.cmp(&a.id))
        });
        Ok(out)
    }

    fn latest_invitation_between(
        &self,
        user_a: &str,
        user_b: &str,
    ) -> Result<Option<ChatInvitation>, PortError> {
        let st = self.state.read().expect("mem chat store poisoned");
        Ok(st
            .invitations
            .values()
            .filter(|i| {
                (i.inviter_id == user_a && i.invitee_id == user_b)
                    || (i.inviter_id == user_b && i.invitee_id == user_a)
            })
            .max_by(|a, b| {
                a.created_at_ms
                    .cmp(&b.created_at_ms)
                    .then(a.id.cmp(&b.id))
            })
            .cloned())
    }

    fn insert_message(
        &self,
        message: ChatMessage,
        jobs: Vec<NewJob>,
    ) -> Result<ChatMessage, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        st.messages.insert(message.id.clone(), message.clone());
        st.outbox.extend(jobs);
        Ok(message)
    }

    fn update_message(
        &self,
        message: ChatMessage,
        jobs: Vec<NewJob>,
    ) -> Result<ChatMessage, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        let existing = st
            .messages
            .get(&message.id)
            .cloned()
            .filter(|m| !m.is_deleted)
            .ok_or_else(|| PortError::NotFound("NO_SUCH_MESSAGE".to_string()))?;
        if existing.room_id != message.room_id || existing.author_id != message.author_id {
            return Err(PortError::Validation("ACCESS_DENIED".to_string()));
        }
        st.messages.insert(message.id.clone(), message.clone());
        st.outbox.extend(jobs);
        Ok(message)
    }

    fn delete_message(&self, message_id: &str, jobs: Vec<NewJob>) -> Result<bool, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        let mut message = match st.messages.get(message_id).cloned() {
            Some(m) if !m.is_deleted => m,
            _ => return Ok(false),
        };
        message.is_deleted = true;
        st.messages.insert(message_id.to_string(), message);
        st.outbox.extend(jobs);
        Ok(true)
    }

    fn get_message(&self, message_id: &str) -> Result<Option<ChatMessage>, PortError> {
        Ok(self
            .state
            .read()
            .expect("mem chat store poisoned")
            .messages
            .get(message_id)
            .filter(|m| !m.is_deleted)
            .cloned())
    }

    fn messages_in(&self, room_id: &str) -> Result<Vec<ChatMessage>, PortError> {
        Ok(newest_first_messages(
            self.state
                .read()
                .expect("mem chat store poisoned")
                .messages
                .values()
                .filter(|m| m.room_id == room_id && !m.is_deleted)
                .cloned()
                .collect(),
        ))
    }

    fn mark_read(
        &self,
        room_id: &str,
        user_id: &str,
        message_id: &str,
        now_ms: i64,
        jobs: Vec<NewJob>,
    ) -> Result<ChatRoomRead, PortError> {
        let mut st = self.lock();
        for job in &jobs {
            job.validate()?;
        }
        let key = (room_id.to_string(), user_id.to_string());
        if let Some(existing) = st.reads.get(&key) {
            if let Some(last) = existing.last_read_message_id.as_deref() {
                if last >= message_id {
                    return Ok(existing.clone());
                }
            }
        }
        let row = ChatRoomRead {
            room_id: room_id.to_string(),
            user_id: user_id.to_string(),
            last_read_message_id: Some(message_id.to_string()),
            updated_at_ms: now_ms,
        };
        st.reads.insert(key, row.clone());
        st.outbox.extend(jobs);
        Ok(row)
    }

    fn get_read(&self, room_id: &str, user_id: &str) -> Result<Option<ChatRoomRead>, PortError> {
        Ok(self
            .state
            .read()
            .expect("mem chat store poisoned")
            .reads
            .get(&(room_id.to_string(), user_id.to_string()))
            .cloned())
    }
}

// ---------------------------------------------------------------------------
// Application ops.
// ---------------------------------------------------------------------------

impl AppServices {
    fn require_room(&self, store: &dyn ChatStore, room_id: &str) -> Result<ChatRoom, PortError> {
        store
            .get_room(room_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_ROOM".to_string()))
    }

    /// Membership gate: non-members read the room as missing (existence
    /// hiding, same convention as invisible notes → `NO_SUCH_NOTE`).
    fn require_member(
        &self,
        store: &dyn ChatStore,
        room_id: &str,
        user_id: &str,
    ) -> Result<(ChatRoom, ChatRoomMember), PortError> {
        let room = self.require_room(store, room_id)?;
        let member = store
            .get_member(room_id, user_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_ROOM".to_string()))?;
        Ok((room, member))
    }

    fn require_user(&self, user_id: &str) -> Result<(), PortError> {
        self.accounts
            .find_by_id(user_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_USER".to_string()))?;
        Ok(())
    }

    /// `chat/rooms/create`. With `user_id`, opens a 1:1 room and invites the
    /// target (the target joins via `accept`/`join`); without, a group room
    /// owned by the creator.
    pub fn chat_room_create(
        &self,
        store: &dyn ChatStore,
        owner_id: &str,
        name: Option<&str>,
        description: Option<&str>,
        user_id: Option<&str>,
    ) -> Result<ChatRoom, PortError> {
        self.accounts.get_owner(owner_id)?;
        validate_room_name(name)?;
        validate_room_description(description)?;
        if let Some(target) = user_id {
            self.require_user(target)?;
            if target == owner_id {
                return Err(PortError::Validation(
                    "INVALID_PARAM cannot open a 1:1 room with yourself".to_string(),
                ));
            }
        }
        let now = self.clock.now_ms();
        let room = ChatRoom {
            id: self.ids.generate(),
            owner_id: owner_id.to_string(),
            name: name.map(str::trim).map(str::to_string),
            description: description.map(str::to_string),
            created_at_ms: now,
            updated_at_ms: now,
        };
        store.insert_room(room.clone(), Vec::new())?;
        store.upsert_member(
            ChatRoomMember {
                room_id: room.id.clone(),
                user_id: owner_id.to_string(),
                muted: false,
                joined_at_ms: now,
            },
            Vec::new(),
        )?;
        if let Some(target) = user_id {
            store.upsert_invitation(
                ChatInvitation {
                    id: self.ids.generate(),
                    room_id: room.id.clone(),
                    inviter_id: owner_id.to_string(),
                    invitee_id: target.to_string(),
                    created_at_ms: now,
                },
                Vec::new(),
            )?;
        }
        Ok(room)
    }

    /// `chat/rooms/show` (member-only).
    pub fn chat_room_show(
        &self,
        store: &dyn ChatStore,
        viewer_id: &str,
        room_id: &str,
    ) -> Result<ChatRoom, PortError> {
        Ok(self.require_member(store, room_id, viewer_id)?.0)
    }

    /// `chat/rooms/update` (member-only visibility; owner-only mutation).
    pub fn chat_room_update(
        &self,
        store: &dyn ChatStore,
        owner_id: &str,
        room_id: &str,
        name: Option<Option<&str>>,
        description: Option<Option<&str>>,
    ) -> Result<ChatRoom, PortError> {
        self.accounts.get_owner(owner_id)?;
        let mut room = self.require_member(store, room_id, owner_id)?.0;
        if room.owner_id != owner_id {
            return Err(PortError::Validation("ACCESS_DENIED".to_string()));
        }
        if let Some(name) = name {
            validate_room_name(name)?;
            room.name = name.map(str::trim).map(str::to_string);
        }
        if let Some(description) = description {
            validate_room_description(description)?;
            room.description = description.map(str::to_string);
        }
        room.updated_at_ms = self.clock.now_ms();
        store.update_room(room, Vec::new())
    }

    /// `chat/rooms/delete` (member-only visibility; owner-only mutation;
    /// cascades).
    pub fn chat_room_delete(
        &self,
        store: &dyn ChatStore,
        owner_id: &str,
        room_id: &str,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(owner_id)?;
        let room = self.require_member(store, room_id, owner_id)?.0;
        if room.owner_id != owner_id {
            return Err(PortError::Validation("ACCESS_DENIED".to_string()));
        }
        store.delete_room(room_id, Vec::new())?;
        Ok(())
    }

    /// `chat/rooms/join` (open; idempotent; consumes a pending invitation).
    pub fn chat_room_join(
        &self,
        store: &dyn ChatStore,
        user_id: &str,
        room_id: &str,
    ) -> Result<ChatRoom, PortError> {
        self.accounts.get_owner(user_id)?;
        let room = self.require_room(store, room_id)?;
        let now = self.clock.now_ms();
        store.upsert_member(
            ChatRoomMember {
                room_id: room_id.to_string(),
                user_id: user_id.to_string(),
                muted: false,
                joined_at_ms: now,
            },
            Vec::new(),
        )?;
        store.delete_invitation(room_id, user_id, Vec::new())?;
        Ok(room)
    }

    /// `chat/rooms/leave`. Owner leaving passes ownership to the oldest
    /// remaining member; the last member out deletes the room (cascade).
    pub fn chat_room_leave(
        &self,
        store: &dyn ChatStore,
        user_id: &str,
        room_id: &str,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        let room = self.require_member(store, room_id, user_id)?.0;
        store.remove_member(room_id, user_id, Vec::new())?;
        if room.owner_id == user_id {
            let remaining = store.members_of(room_id)?;
            match remaining.first() {
                Some(next) => {
                    store.set_room_owner(
                        room_id,
                        &next.user_id.clone(),
                        self.clock.now_ms(),
                        Vec::new(),
                    )?;
                }
                None => {
                    store.delete_room(room_id, Vec::new())?;
                }
            }
        }
        Ok(())
    }

    /// `chat/rooms/members` (member-only).
    pub fn chat_room_members(
        &self,
        store: &dyn ChatStore,
        viewer_id: &str,
        room_id: &str,
    ) -> Result<Vec<ChatRoomMember>, PortError> {
        self.require_member(store, room_id, viewer_id)?;
        store.members_of(room_id)
    }

    /// `chat/rooms/mute` / `unmute` (member-only, idempotent flag).
    pub fn chat_room_set_muted(
        &self,
        store: &dyn ChatStore,
        user_id: &str,
        room_id: &str,
        muted: bool,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        let (_, member) = self.require_member(store, room_id, user_id)?;
        store.upsert_member(
            ChatRoomMember {
                muted,
                ..member
            },
            Vec::new(),
        )?;
        Ok(())
    }

    /// Member invites `invitee_id` to `room_id` (idempotent re-invite).
    /// App-level seam for whichever upstream endpoint mints invitations.
    pub fn chat_room_invite(
        &self,
        store: &dyn ChatStore,
        inviter_id: &str,
        room_id: &str,
        invitee_id: &str,
    ) -> Result<ChatInvitation, PortError> {
        self.accounts.get_owner(inviter_id)?;
        self.require_member(store, room_id, inviter_id)?;
        self.require_user(invitee_id)?;
        if store.get_member(room_id, invitee_id)?.is_some() {
            if let Some(existing) = store.get_invitation(room_id, invitee_id)? {
                return Ok(existing);
            }
            return Err(PortError::Validation(
                "INVALID_PARAM user is already a member".to_string(),
            ));
        }
        let now = self.clock.now_ms();
        store.upsert_invitation(
            ChatInvitation {
                id: self.ids.generate(),
                room_id: room_id.to_string(),
                inviter_id: inviter_id.to_string(),
                invitee_id: invitee_id.to_string(),
                created_at_ms: now,
            },
            Vec::new(),
        )
    }

    /// `chat/rooms/invitations/inbox` (newest-first).
    pub fn chat_invitations_inbox(
        &self,
        store: &dyn ChatStore,
        user_id: &str,
    ) -> Result<Vec<ChatInvitation>, PortError> {
        store.invitations_to(user_id)
    }

    /// `chat/rooms/invitations/outbox` (newest-first).
    pub fn chat_invitations_outbox(
        &self,
        store: &dyn ChatStore,
        user_id: &str,
    ) -> Result<Vec<ChatInvitation>, PortError> {
        store.invitations_from(user_id)
    }

    /// `chat/rooms/invitations/accept { userId }`: join the room of the most
    /// recent pending invitation involving me + `user_id`.
    pub fn chat_invitation_accept(
        &self,
        store: &dyn ChatStore,
        user_id: &str,
        other_id: &str,
    ) -> Result<ChatRoom, PortError> {
        self.accounts.get_owner(user_id)?;
        let invitation = store
            .latest_invitation_between(user_id, other_id)?
            .filter(|i| i.invitee_id == user_id)
            .ok_or_else(|| PortError::NotFound("NO_SUCH_INVITATION".to_string()))?;
        let room_id = invitation.room_id.clone();
        self.require_room(store, &room_id)?;
        let now = self.clock.now_ms();
        store.upsert_member(
            ChatRoomMember {
                room_id: room_id.clone(),
                user_id: user_id.to_string(),
                muted: false,
                joined_at_ms: now,
            },
            Vec::new(),
        )?;
        store.delete_invitation(&room_id, user_id, Vec::new())?;
        self.require_room(store, &room_id)
    }

    /// `chat/rooms/invitations/decline { userId }`.
    pub fn chat_invitation_decline(
        &self,
        store: &dyn ChatStore,
        user_id: &str,
        other_id: &str,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        let invitation = store
            .latest_invitation_between(user_id, other_id)?
            .filter(|i| i.invitee_id == user_id)
            .ok_or_else(|| PortError::NotFound("NO_SUCH_INVITATION".to_string()))?;
        store.delete_invitation(&invitation.room_id, user_id, Vec::new())?;
        Ok(())
    }

    /// `chat/messages/create` (member-only; text or file required).
    pub fn chat_message_create(
        &self,
        store: &dyn ChatStore,
        author_id: &str,
        room_id: &str,
        text: Option<&str>,
        file_id: Option<&str>,
    ) -> Result<ChatMessage, PortError> {
        self.accounts.get_owner(author_id)?;
        self.require_member(store, room_id, author_id)?;
        ChatMessage::validate_body(text, file_id)?;
        let now = self.clock.now_ms();
        store.insert_message(
            ChatMessage {
                id: self.ids.generate(),
                room_id: room_id.to_string(),
                author_id: author_id.to_string(),
                text: text.map(str::trim).filter(|t| !t.is_empty()).map(str::to_string),
                file_id: file_id.map(str::to_string),
                created_at_ms: now,
                updated_at_ms: None,
                is_deleted: false,
            },
            Vec::new(),
        )
    }

    /// `chat/messages/show` (member-only).
    pub fn chat_message_show(
        &self,
        store: &dyn ChatStore,
        viewer_id: &str,
        message_id: &str,
    ) -> Result<ChatMessage, PortError> {
        let message = store
            .get_message(message_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_MESSAGE".to_string()))?;
        self.require_member(store, &message.room_id, viewer_id)?;
        Ok(message)
    }

    /// `chat/messages/update` (author-only).
    pub fn chat_message_update(
        &self,
        store: &dyn ChatStore,
        author_id: &str,
        message_id: &str,
        text: Option<&str>,
        file_id: Option<Option<&str>>,
    ) -> Result<ChatMessage, PortError> {
        self.accounts.get_owner(author_id)?;
        let mut message = store
            .get_message(message_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_MESSAGE".to_string()))?;
        if message.author_id != author_id {
            return Err(PortError::Validation("ACCESS_DENIED".to_string()));
        }
        self.require_member(store, &message.room_id, author_id)?;
        let next_text = text.map(|t| t.trim().to_string());
        let next_file = file_id.map(|f| f.map(str::to_string));
        ChatMessage::validate_body(
            next_text.as_deref().or(message.text.as_deref()),
            next_file
                .as_ref()
                .and_then(|f| f.as_deref())
                .or(message.file_id.as_deref()),
        )?;
        if let Some(t) = text {
            let t = t.trim();
            message.text = if t.is_empty() { None } else { Some(t.to_string()) };
        }
        if let Some(f) = file_id {
            message.file_id = f.map(str::to_string);
        }
        message.updated_at_ms = Some(self.clock.now_ms());
        store.update_message(message, Vec::new())
    }

    /// `chat/messages/delete` (author or room owner).
    pub fn chat_message_delete(
        &self,
        store: &dyn ChatStore,
        user_id: &str,
        message_id: &str,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        let message = store
            .get_message(message_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_MESSAGE".to_string()))?;
        let (room, _) = self.require_member(store, &message.room_id, user_id)?;
        if message.author_id != user_id && room.owner_id != user_id {
            return Err(PortError::Validation("ACCESS_DENIED".to_string()));
        }
        store.delete_message(message_id, Vec::new())?;
        Ok(())
    }

    /// `chat/messages/read { messageId }` (member-only; monotonic watermark).
    pub fn chat_message_read(
        &self,
        store: &dyn ChatStore,
        user_id: &str,
        message_id: &str,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        let message = store
            .get_message(message_id)?
            .ok_or_else(|| PortError::NotFound("NO_SUCH_MESSAGE".to_string()))?;
        self.require_member(store, &message.room_id, user_id)?;
        store.mark_read(
            &message.room_id,
            user_id,
            message_id,
            self.clock.now_ms(),
            Vec::new(),
        )?;
        Ok(())
    }

    /// `chat/history` (member rooms, newest-first, optional room filter).
    pub fn chat_history(
        &self,
        store: &dyn ChatStore,
        user_id: &str,
        room_id: Option<&str>,
        cursor: &IdCursor,
    ) -> Result<Vec<ChatMessage>, PortError> {
        let rooms = store.rooms_of(user_id)?;
        let mut all = Vec::new();
        for room in rooms {
            if let Some(filter) = room_id {
                if room.id != filter {
                    continue;
                }
            }
            all.extend(store.messages_in(&room.id)?);
        }
        all.sort_by(|a, b| {
            b.created_at_ms
                .cmp(&a.created_at_ms)
                .then(b.id.cmp(&a.id))
        });
        let limit = cursor.limit.clamp(1, 100);
        if let Some(since) = cursor.since_id.as_deref() {
            all.retain(|m| m.id.as_str() > since);
        }
        if let Some(until) = cursor.until_id.as_deref() {
            all.retain(|m| m.id.as_str() < until);
        }
        all.truncate(limit);
        Ok(all)
    }

    /// Unread-message count across member rooms (backs the
    /// `hasUnreadChatMessages` badge; the packer currently hardcodes false —
    /// parent wires this in).
    pub fn chat_unread_count(
        &self,
        store: &dyn ChatStore,
        user_id: &str,
    ) -> Result<u64, PortError> {
        let rooms = store.rooms_of(user_id)?;
        let mut unread = 0u64;
        for room in rooms {
            let watermark = store
                .get_read(&room.id, user_id)?
                .and_then(|r| r.last_read_message_id);
            for message in store.messages_in(&room.id)? {
                if message.author_id == user_id {
                    continue;
                }
                let is_new = match watermark.as_deref() {
                    Some(last) => message.id.as_str() > last,
                    None => true,
                };
                if is_new {
                    unread += 1;
                }
            }
        }
        Ok(unread)
    }
}
