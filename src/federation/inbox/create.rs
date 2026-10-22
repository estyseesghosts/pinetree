//! Inbound `Create(Note)` handling (moved from `src/federation/inbox.rs`).

mod notes;
#[cfg(test)]
mod replies;

pub(crate) use self::notes::handle_create;
