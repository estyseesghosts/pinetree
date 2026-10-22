use super::job_queue::NewJob;
use crate::domain::id::PortError;

/// Pages (Misskey `MiPage`/`MiPageLike` parity + pinetree federation
/// columns). Name-uniqueness checks + inserts commit atomically in ONE
/// port call; like/unlike bump `liked_count` in the same transaction.
pub trait PageStore: Send + Sync {
    /// Insert one page with `jobs` (federation fan-out); one
    /// transaction. `Conflict(NAME_ALREADY_EXISTS)` when the owner
    /// already holds `name`.
    fn create_page(
        &self,
        page: crate::domain::page::Page,
        jobs: Vec<NewJob>,
    ) -> Result<crate::domain::page::Page, PortError>;
    /// Rewrite by id (owner checked by the caller via `get_page`) with
    /// `jobs`, one transaction. `None` on miss;
    /// `Conflict(NAME_ALREADY_EXISTS)` when `name` moves onto another
    /// page of the same owner. Callers only pass `jobs` when the row is
    /// known to exist (owner-scoped pre-read), so a `None` miss never
    /// enqueues phantom fan-out.
    fn update_page(
        &self,
        page: crate::domain::page::Page,
        jobs: Vec<NewJob>,
    ) -> Result<Option<crate::domain::page::Page>, PortError>;
    /// Delete by id with like cascade, one transaction. Returns true when
    /// a row existed. `jobs` (outbound `Delete(Article)` fan-out) commit
    /// atomically.
    fn delete_page(&self, page_id: &str, jobs: Vec<NewJob>) -> Result<bool, PortError>;
    fn get_page(&self, page_id: &str) -> Result<Option<crate::domain::page::Page>, PortError>;
    fn get_page_by_name(
        &self,
        owner_id: &str,
        name: &str,
    ) -> Result<Option<crate::domain::page::Page>, PortError>;
    /// Look up a page by its canonical remote URI (inbound `Article` /
    /// `Update(Article)` / `Like` / `Delete` correlation for materialized
    /// remote pages; local pages carry `None`). `None` on miss.
    fn get_page_by_uri(&self, uri: &str) -> Result<Option<crate::domain::page::Page>, PortError>;
    /// Owner's pages, newest-first.
    fn pages_by_owner(&self, owner_id: &str) -> Result<Vec<crate::domain::page::Page>, PortError>;
    /// Like one page (idempotent insert + `liked_count` bump +
    /// `jobs`, one transaction). Returns true when the row was newly
    /// added.
    fn like_page(
        &self,
        like: crate::domain::page::PageLike,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError>;
    /// Unlike (idempotent delete + `liked_count` decrement floor 0, one
    /// transaction). Returns true when a row existed.
    fn unlike_page(&self, user_id: &str, page_id: &str) -> Result<bool, PortError>;
    fn is_page_liked(&self, user_id: &str, page_id: &str) -> Result<bool, PortError>;
    /// Upstream `pages/featured` parity: at most 10 pages with
    /// `liked_count > 0`, highest count first.
    fn featured_pages(&self) -> Result<Vec<crate::domain::page::Page>, PortError>;
}
