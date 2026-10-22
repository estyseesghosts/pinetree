use super::codec::{decode_json, encode_json, map_redb_err};
use super::store::RedbStore;
use crate::application::ports::{NewJob, PortError, UserListStore};
use redb::{ReadableDatabase, ReadableTable, TableDefinition};

/// User lists (Batch O WP16): id → JSON [`UserList`] row.
pub(crate) const USER_LISTS: TableDefinition<&str, &[u8]> = TableDefinition::new("user_lists");
/// Owner index `(owner_id, list_id)` (listing + per-owner cap).
pub(crate) const USER_LISTS_BY_OWNER: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("user_lists_by_owner");
/// Memberships `(list_id, member_id)` → JSON [`UserListMembership`] row.
pub(crate) const USER_LIST_MEMBERSHIPS: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("user_list_memberships");
/// List favorites `(owner_id, list_id)` (upstream `MiUserListFavorite`).
pub(crate) const USER_LIST_FAVORITES: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("user_list_favorites");
impl UserListStore for RedbStore {
    fn create_user_list(
        &self,
        list: crate::domain::user_list::UserList,
    ) -> Result<crate::domain::user_list::UserList, PortError> {
        use crate::domain::user_list::USER_LIST_LIMIT;
        let write_txn = self.begin_write()?;
        {
            let by_owner = write_txn
                .open_table(USER_LISTS_BY_OWNER)
                .map_err(map_redb_err)?;
            let owned = by_owner
                .range((list.owner_id.as_str(), "")..=(list.owner_id.as_str(), "\u{10ffff}"))
                .map_err(map_redb_err)?
                .count();
            if owned >= USER_LIST_LIMIT {
                return Err(PortError::Conflict("TOO_MANY_USERLISTS".to_string()));
            }
            drop(by_owner);
            let mut lists = write_txn.open_table(USER_LISTS).map_err(map_redb_err)?;
            let mut by_owner = write_txn
                .open_table(USER_LISTS_BY_OWNER)
                .map_err(map_redb_err)?;
            let encoded = encode_json(&list)?;
            lists
                .insert(list.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
            by_owner
                .insert((list.owner_id.as_str(), list.id.as_str()), &1u8)
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)?;
        Ok(list)
    }

    fn update_user_list(
        &self,
        list: crate::domain::user_list::UserList,
    ) -> Result<Option<crate::domain::user_list::UserList>, PortError> {
        let write_txn = self.begin_write()?;
        let updated = {
            let mut lists = write_txn.open_table(USER_LISTS).map_err(map_redb_err)?;
            let existing: Option<crate::domain::user_list::UserList> =
                match lists.get(list.id.as_str()).map_err(map_redb_err)? {
                    Some(g) => Some(decode_json(g.value())?),
                    None => None,
                };
            match existing {
                Some(prev) if prev.owner_id == list.owner_id => {
                    let encoded = encode_json(&list)?;
                    lists
                        .insert(list.id.as_str(), encoded.as_slice())
                        .map_err(map_redb_err)?;
                    Some(list)
                }
                _ => None,
            }
        };
        self.commit_write(write_txn)?;
        Ok(updated)
    }

    fn delete_user_list(&self, owner_id: &str, list_id: &str) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        let removed = {
            let mut lists = write_txn.open_table(USER_LISTS).map_err(map_redb_err)?;
            let existing: Option<crate::domain::user_list::UserList> =
                match lists.get(list_id).map_err(map_redb_err)? {
                    Some(g) => Some(decode_json(g.value())?),
                    None => None,
                };
            let Some(prev) = existing else {
                return Ok(false);
            };
            if prev.owner_id != owner_id {
                return Ok(false);
            }
            lists.remove(list_id).map_err(map_redb_err)?;
            drop(lists);
            let mut by_owner = write_txn
                .open_table(USER_LISTS_BY_OWNER)
                .map_err(map_redb_err)?;
            by_owner.remove((owner_id, list_id)).map_err(map_redb_err)?;
            drop(by_owner);
            let mut memberships = write_txn
                .open_table(USER_LIST_MEMBERSHIPS)
                .map_err(map_redb_err)?;
            let stale: Vec<(String, String)> = memberships
                .range((list_id, "")..=(list_id, "\u{10ffff}"))
                .map_err(map_redb_err)?
                .filter_map(|entry| {
                    entry.ok().map(|(k, _)| {
                        let (l, m) = k.value();
                        (l.to_string(), m.to_string())
                    })
                })
                .collect();
            for (l, m) in stale {
                memberships
                    .remove((l.as_str(), m.as_str()))
                    .map_err(map_redb_err)?;
            }
            drop(memberships);
            let mut favorites = write_txn
                .open_table(USER_LIST_FAVORITES)
                .map_err(map_redb_err)?;
            let stale_favs: Vec<(String, String)> = favorites
                .iter()
                .map_err(map_redb_err)?
                .filter_map(|entry| {
                    entry.ok().and_then(|(k, _)| {
                        let (o, l) = k.value();
                        (l == list_id).then(|| (o.to_string(), l.to_string()))
                    })
                })
                .collect();
            for (o, l) in stale_favs {
                favorites
                    .remove((o.as_str(), l.as_str()))
                    .map_err(map_redb_err)?;
            }
            true
        };
        self.commit_write(write_txn)?;
        Ok(removed)
    }

    fn get_user_list(
        &self,
        list_id: &str,
    ) -> Result<Option<crate::domain::user_list::UserList>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(USER_LISTS).map_err(map_redb_err)?;
        match table.get(list_id).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn lists_by_owner(
        &self,
        owner_id: &str,
    ) -> Result<Vec<crate::domain::user_list::UserList>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_owner = read_txn
            .open_table(USER_LISTS_BY_OWNER)
            .map_err(map_redb_err)?;
        let table = read_txn.open_table(USER_LISTS).map_err(map_redb_err)?;
        let mut ids: Vec<String> = by_owner
            .range((owner_id, "")..=(owner_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
            .filter_map(|entry| {
                entry.ok().map(|(k, _)| {
                    let (_, l) = k.value();
                    l.to_string()
                })
            })
            .collect();
        ids.sort_by(|a, b| b.cmp(a));
        let mut out = Vec::new();
        for id in ids {
            if let Some(g) = table.get(id.as_str()).map_err(map_redb_err)? {
                out.push(decode_json(g.value())?);
            }
        }
        Ok(out)
    }

    fn public_lists_by_owner(
        &self,
        owner_id: &str,
    ) -> Result<Vec<crate::domain::user_list::UserList>, PortError> {
        Ok(self
            .lists_by_owner(owner_id)?
            .into_iter()
            .filter(|l| l.is_public)
            .collect())
    }

    fn add_list_member(
        &self,
        member: crate::domain::user_list::UserListMembership,
        jobs: Vec<NewJob>,
    ) -> Result<crate::domain::user_list::UserListMembership, PortError> {
        use crate::domain::user_list::USER_LIST_MEMBER_LIMIT;
        let write_txn = self.begin_write()?;
        {
            let mut memberships = write_txn
                .open_table(USER_LIST_MEMBERSHIPS)
                .map_err(map_redb_err)?;
            if memberships
                .get((member.list_id.as_str(), member.member_id.as_str()))
                .map_err(map_redb_err)?
                .is_some()
            {
                return Err(PortError::Conflict("ALREADY_ADDED".to_string()));
            }
            let count = memberships
                .range((member.list_id.as_str(), "")..=(member.list_id.as_str(), "\u{10ffff}"))
                .map_err(map_redb_err)?
                .count();
            if count >= USER_LIST_MEMBER_LIMIT {
                return Err(PortError::Conflict("TOO_MANY_USERS".to_string()));
            }
            let encoded = encode_json(&member)?;
            memberships
                .insert(
                    (member.list_id.as_str(), member.member_id.as_str()),
                    encoded.as_slice(),
                )
                .map_err(map_redb_err)?;
            drop(memberships);
            self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        }
        self.commit_write(write_txn)?;
        Ok(member)
    }

    fn remove_list_member(
        &self,
        list_id: &str,
        member_id: &str,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        let removed = {
            let mut memberships = write_txn
                .open_table(USER_LIST_MEMBERSHIPS)
                .map_err(map_redb_err)?;
            let removed = memberships
                .remove((list_id, member_id))
                .map_err(map_redb_err)?
                .is_some();
            drop(memberships);
            self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
            removed
        };
        self.commit_write(write_txn)?;
        Ok(removed)
    }

    fn update_list_membership(
        &self,
        list_id: &str,
        member_id: &str,
        with_replies: bool,
    ) -> Result<Option<crate::domain::user_list::UserListMembership>, PortError> {
        let write_txn = self.begin_write()?;
        let updated = {
            let mut memberships = write_txn
                .open_table(USER_LIST_MEMBERSHIPS)
                .map_err(map_redb_err)?;
            let mut member: crate::domain::user_list::UserListMembership = match memberships
                .get((list_id, member_id))
                .map_err(map_redb_err)?
            {
                Some(g) => decode_json(g.value())?,
                None => return Ok(None),
            };
            member.with_replies = with_replies;
            let encoded = encode_json(&member)?;
            memberships
                .insert((list_id, member_id), encoded.as_slice())
                .map_err(map_redb_err)?;
            member
        };
        self.commit_write(write_txn)?;
        Ok(Some(updated))
    }

    fn get_list_membership(
        &self,
        list_id: &str,
        member_id: &str,
    ) -> Result<Option<crate::domain::user_list::UserListMembership>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn
            .open_table(USER_LIST_MEMBERSHIPS)
            .map_err(map_redb_err)?;
        match table.get((list_id, member_id)).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn members_of_list(
        &self,
        list_id: &str,
    ) -> Result<Vec<crate::domain::user_list::UserListMembership>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn
            .open_table(USER_LIST_MEMBERSHIPS)
            .map_err(map_redb_err)?;
        let mut out = Vec::new();
        for entry in table
            .range((list_id, "")..=(list_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
        {
            let (_, v) = entry.map_err(map_redb_err)?;
            out.push(decode_json(v.value())?);
        }
        Ok(out)
    }

    fn favorite_list(&self, owner_id: &str, list_id: &str) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        let added = {
            let mut favorites = write_txn
                .open_table(USER_LIST_FAVORITES)
                .map_err(map_redb_err)?;
            if favorites
                .get((owner_id, list_id))
                .map_err(map_redb_err)?
                .is_some()
            {
                false
            } else {
                favorites
                    .insert((owner_id, list_id), &1u8)
                    .map_err(map_redb_err)?;
                true
            }
        };
        self.commit_write(write_txn)?;
        Ok(added)
    }

    fn unfavorite_list(&self, owner_id: &str, list_id: &str) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        let removed = {
            let mut favorites = write_txn
                .open_table(USER_LIST_FAVORITES)
                .map_err(map_redb_err)?;
            favorites
                .remove((owner_id, list_id))
                .map_err(map_redb_err)?
                .is_some()
        };
        self.commit_write(write_txn)?;
        Ok(removed)
    }

    fn is_list_favorited(&self, owner_id: &str, list_id: &str) -> Result<bool, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn
            .open_table(USER_LIST_FAVORITES)
            .map_err(map_redb_err)?;
        Ok(table
            .get((owner_id, list_id))
            .map_err(map_redb_err)?
            .is_some())
    }

    fn list_favorite_count(&self, list_id: &str) -> Result<u64, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn
            .open_table(USER_LIST_FAVORITES)
            .map_err(map_redb_err)?;
        let mut count = 0u64;
        for entry in table.iter().map_err(map_redb_err)? {
            let (k, _) = entry.map_err(map_redb_err)?;
            if k.value().1 == list_id {
                count += 1;
            }
        }
        Ok(count)
    }
}
