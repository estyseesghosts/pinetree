use super::codec::{decode_json, encode_json, map_redb_err};
use super::store::RedbStore;
use crate::application::ports::{NewJob, PageStore, PortError};
use redb::{ReadableDatabase, ReadableTable, TableDefinition};

/// Pages by id (federated clips + pages handoff, Phase 3 local). Values are
/// JSON [`Page`](crate::domain::page::Page) rows.
pub(crate) const PAGES: TableDefinition<&str, &[u8]> = TableDefinition::new("pages");
/// Owner index `(owner_id, page_id)` (listing).
pub(crate) const PAGES_BY_OWNER: TableDefinition<(&str, &str), u8> =
    TableDefinition::new("pages_by_owner");
/// Page slug index `(owner_id, name)` → page id (per-user slug uniqueness).
pub(crate) const PAGES_BY_OWNER_NAME: TableDefinition<(&str, &str), &str> =
    TableDefinition::new("pages_by_owner_name");
/// Canonical remote-URI index `uri` → page id (remote-page correlation
/// for inbound `Article`/`Update`/`Like`/`Delete`).
pub(crate) const PAGES_BY_URI: TableDefinition<&str, &str> = TableDefinition::new("pages_by_uri");
/// Page likes `(page_id, user_id)` → JSON
/// [`PageLike`](crate::domain::page::PageLike) row.
pub(crate) const PAGE_LIKES: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("page_likes");
impl PageStore for RedbStore {
    fn create_page(
        &self,
        page: crate::domain::page::Page,
        jobs: Vec<NewJob>,
    ) -> Result<crate::domain::page::Page, PortError> {
        let write_txn = self.begin_write()?;
        {
            self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
            let by_name = write_txn
                .open_table(PAGES_BY_OWNER_NAME)
                .map_err(map_redb_err)?;
            if by_name
                .get((page.owner_id.as_str(), page.name.as_str()))
                .map_err(map_redb_err)?
                .is_some()
            {
                return Err(PortError::Conflict("NAME_ALREADY_EXISTS".to_string()));
            }
            drop(by_name);
            let mut pages = write_txn.open_table(PAGES).map_err(map_redb_err)?;
            let mut by_owner = write_txn.open_table(PAGES_BY_OWNER).map_err(map_redb_err)?;
            let mut by_name = write_txn
                .open_table(PAGES_BY_OWNER_NAME)
                .map_err(map_redb_err)?;
            let encoded = encode_json(&page)?;
            pages
                .insert(page.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
            by_owner
                .insert((page.owner_id.as_str(), page.id.as_str()), &1u8)
                .map_err(map_redb_err)?;
            by_name
                .insert(
                    (page.owner_id.as_str(), page.name.as_str()),
                    page.id.as_str(),
                )
                .map_err(map_redb_err)?;
            if let Some(uri) = page.uri.as_deref() {
                let mut by_uri = write_txn.open_table(PAGES_BY_URI).map_err(map_redb_err)?;
                by_uri.insert(uri, page.id.as_str()).map_err(map_redb_err)?;
            }
        }
        self.commit_write(write_txn)?;
        Ok(page)
    }

    fn update_page(
        &self,
        page: crate::domain::page::Page,
        jobs: Vec<NewJob>,
    ) -> Result<Option<crate::domain::page::Page>, PortError> {
        let write_txn = self.begin_write()?;
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        let updated = {
            let mut pages = write_txn.open_table(PAGES).map_err(map_redb_err)?;
            let existing: Option<crate::domain::page::Page> =
                match pages.get(page.id.as_str()).map_err(map_redb_err)? {
                    Some(g) => Some(decode_json(g.value())?),
                    None => None,
                };
            let Some(prev) = existing else {
                return Ok(None);
            };
            if prev.owner_id != page.owner_id {
                return Ok(None);
            }
            // Slug move: the target slug must not belong to another page of
            // the same owner (pre-check + race both surface as the conflict;
            // the redb single writer makes the check atomic here).
            if prev.name != page.name {
                let by_name = write_txn
                    .open_table(PAGES_BY_OWNER_NAME)
                    .map_err(map_redb_err)?;
                if let Some(g) = by_name
                    .get((page.owner_id.as_str(), page.name.as_str()))
                    .map_err(map_redb_err)?
                {
                    if g.value() != page.id.as_str() {
                        return Err(PortError::Conflict("NAME_ALREADY_EXISTS".to_string()));
                    }
                }
                drop(by_name);
                let mut by_name = write_txn
                    .open_table(PAGES_BY_OWNER_NAME)
                    .map_err(map_redb_err)?;
                by_name
                    .remove((page.owner_id.as_str(), prev.name.as_str()))
                    .map_err(map_redb_err)?;
                by_name
                    .insert(
                        (page.owner_id.as_str(), page.name.as_str()),
                        page.id.as_str(),
                    )
                    .map_err(map_redb_err)?;
            }
            let encoded = encode_json(&page)?;
            pages
                .insert(page.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
            // Canonical-URI index follows the row (remote pages are
            // immutable-addressed; a changed URI rebinds here).
            if prev.uri.as_deref() != page.uri.as_deref() {
                let mut by_uri = write_txn.open_table(PAGES_BY_URI).map_err(map_redb_err)?;
                if let Some(old) = prev.uri.as_deref() {
                    by_uri.remove(old).map_err(map_redb_err)?;
                }
                if let Some(uri) = page.uri.as_deref() {
                    by_uri.insert(uri, page.id.as_str()).map_err(map_redb_err)?;
                }
            }
            Some(page)
        };
        self.commit_write(write_txn)?;
        Ok(updated)
    }

    fn delete_page(&self, page_id: &str, jobs: Vec<NewJob>) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        let removed = {
            let mut pages = write_txn.open_table(PAGES).map_err(map_redb_err)?;
            let existing: Option<crate::domain::page::Page> =
                match pages.get(page_id).map_err(map_redb_err)? {
                    Some(g) => Some(decode_json(g.value())?),
                    None => None,
                };
            let Some(prev) = existing else {
                return Ok(false);
            };
            pages.remove(page_id).map_err(map_redb_err)?;
            drop(pages);
            let mut by_owner = write_txn.open_table(PAGES_BY_OWNER).map_err(map_redb_err)?;
            by_owner
                .remove((prev.owner_id.as_str(), page_id))
                .map_err(map_redb_err)?;
            drop(by_owner);
            let mut by_name = write_txn
                .open_table(PAGES_BY_OWNER_NAME)
                .map_err(map_redb_err)?;
            by_name
                .remove((prev.owner_id.as_str(), prev.name.as_str()))
                .map_err(map_redb_err)?;
            drop(by_name);
            if let Some(uri) = prev.uri.as_deref() {
                let mut by_uri = write_txn.open_table(PAGES_BY_URI).map_err(map_redb_err)?;
                by_uri.remove(uri).map_err(map_redb_err)?;
                drop(by_uri);
            }
            let mut likes = write_txn.open_table(PAGE_LIKES).map_err(map_redb_err)?;
            let stale: Vec<(String, String)> = likes
                .range((page_id, "")..=(page_id, "\u{10ffff}"))
                .map_err(map_redb_err)?
                .filter_map(|entry| {
                    entry.ok().map(|(k, _)| {
                        let (p, u) = k.value();
                        (p.to_string(), u.to_string())
                    })
                })
                .collect();
            for (p, u) in stale {
                likes
                    .remove((p.as_str(), u.as_str()))
                    .map_err(map_redb_err)?;
            }
            true
        };
        self.commit_write(write_txn)?;
        Ok(removed)
    }

    fn get_page(&self, page_id: &str) -> Result<Option<crate::domain::page::Page>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(PAGES).map_err(map_redb_err)?;
        match table.get(page_id).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn get_page_by_name(
        &self,
        owner_id: &str,
        name: &str,
    ) -> Result<Option<crate::domain::page::Page>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_name = read_txn
            .open_table(PAGES_BY_OWNER_NAME)
            .map_err(map_redb_err)?;
        let Some(g) = by_name.get((owner_id, name)).map_err(map_redb_err)? else {
            return Ok(None);
        };
        let id = g.value().to_string();
        drop(by_name);
        let table = read_txn.open_table(PAGES).map_err(map_redb_err)?;
        match table.get(id.as_str()).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn get_page_by_uri(&self, uri: &str) -> Result<Option<crate::domain::page::Page>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_uri = read_txn.open_table(PAGES_BY_URI).map_err(map_redb_err)?;
        let Some(g) = by_uri.get(uri).map_err(map_redb_err)? else {
            return Ok(None);
        };
        let id = g.value().to_string();
        drop(by_uri);
        let table = read_txn.open_table(PAGES).map_err(map_redb_err)?;
        match table.get(id.as_str()).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn pages_by_owner(&self, owner_id: &str) -> Result<Vec<crate::domain::page::Page>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let by_owner = read_txn.open_table(PAGES_BY_OWNER).map_err(map_redb_err)?;
        let table = read_txn.open_table(PAGES).map_err(map_redb_err)?;
        let mut ids: Vec<String> = by_owner
            .range((owner_id, "")..=(owner_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
            .filter_map(|entry| {
                entry.ok().map(|(k, _)| {
                    let (_, p) = k.value();
                    p.to_string()
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

    fn like_page(
        &self,
        like: crate::domain::page::PageLike,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        self.enqueue_jobs_in_txn(&write_txn, jobs, None)?;
        let added = {
            let mut likes = write_txn.open_table(PAGE_LIKES).map_err(map_redb_err)?;
            if likes
                .get((like.page_id.as_str(), like.user_id.as_str()))
                .map_err(map_redb_err)?
                .is_some()
            {
                false
            } else {
                let encoded = encode_json(&like)?;
                likes
                    .insert(
                        (like.page_id.as_str(), like.user_id.as_str()),
                        encoded.as_slice(),
                    )
                    .map_err(map_redb_err)?;
                drop(likes);
                let prev: Option<crate::domain::page::Page> = {
                    let pages = write_txn.open_table(PAGES).map_err(map_redb_err)?;
                    match pages.get(like.page_id.as_str()).map_err(map_redb_err)? {
                        Some(g) => Some(decode_json(g.value())?),
                        None => None,
                    }
                };
                if let Some(mut page) = prev {
                    page.liked_count = page.liked_count.saturating_add(1);
                    let encoded = encode_json(&page)?;
                    let mut pages = write_txn.open_table(PAGES).map_err(map_redb_err)?;
                    pages
                        .insert(page.id.as_str(), encoded.as_slice())
                        .map_err(map_redb_err)?;
                }
                true
            }
        };
        self.commit_write(write_txn)?;
        Ok(added)
    }

    fn unlike_page(&self, user_id: &str, page_id: &str) -> Result<bool, PortError> {
        let write_txn = self.begin_write()?;
        let removed = {
            let mut likes = write_txn.open_table(PAGE_LIKES).map_err(map_redb_err)?;
            if likes
                .remove((page_id, user_id))
                .map_err(map_redb_err)?
                .is_none()
            {
                false
            } else {
                drop(likes);
                let prev: Option<crate::domain::page::Page> = {
                    let pages = write_txn.open_table(PAGES).map_err(map_redb_err)?;
                    match pages.get(page_id).map_err(map_redb_err)? {
                        Some(g) => Some(decode_json(g.value())?),
                        None => None,
                    }
                };
                if let Some(mut page) = prev {
                    page.liked_count = page.liked_count.saturating_sub(1);
                    let encoded = encode_json(&page)?;
                    let mut pages = write_txn.open_table(PAGES).map_err(map_redb_err)?;
                    pages
                        .insert(page.id.as_str(), encoded.as_slice())
                        .map_err(map_redb_err)?;
                }
                true
            }
        };
        self.commit_write(write_txn)?;
        Ok(removed)
    }

    fn is_page_liked(&self, user_id: &str, page_id: &str) -> Result<bool, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(PAGE_LIKES).map_err(map_redb_err)?;
        Ok(table
            .get((page_id, user_id))
            .map_err(map_redb_err)?
            .is_some())
    }

    fn featured_pages(&self) -> Result<Vec<crate::domain::page::Page>, PortError> {
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(PAGES).map_err(map_redb_err)?;
        let mut rows: Vec<crate::domain::page::Page> = table
            .iter()
            .map_err(map_redb_err)?
            .filter_map(|entry| {
                entry.ok().and_then(|(_, v)| {
                    let page: crate::domain::page::Page = decode_json(v.value()).ok()?;
                    (page.liked_count > 0).then_some(page)
                })
            })
            .collect();
        rows.sort_by(|a, b| b.liked_count.cmp(&a.liked_count).then(b.id.cmp(&a.id)));
        rows.truncate(10);
        Ok(rows)
    }
}
