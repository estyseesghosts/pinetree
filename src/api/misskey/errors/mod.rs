//! Misskey API error envelope (split from `errors.rs`).
//!
//! Pure move, no behavior change.

mod auth;
mod codes;
mod drive;
mod emoji;
mod error;
mod i;
mod lists;
mod lookup;
mod notes;
mod rest;
mod social;
mod users;

pub use codes::*;
pub use error::ApiError;
