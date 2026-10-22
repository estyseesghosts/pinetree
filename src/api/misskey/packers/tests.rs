//! Packer tests (split from `packers.rs`). Pure move.
//!
//! Open item: 2005 lines, over cap; split further after green.
//!
//! Split: `tests.rs` is the parent; `tests_support.rs` (Mem stub),
//! `tests_helpers.rs` (factories + schema asserts), and
//! `tests_user.rs` / `tests_note.rs` / `tests_drive.rs` / `tests_meta.rs`.

#[path = "tests_drive.rs"]
mod drive;
#[path = "tests_helpers.rs"]
mod helpers;
#[path = "tests_meta.rs"]
mod meta;
#[path = "tests_note.rs"]
mod note;
#[path = "tests_support.rs"]
mod support;
#[path = "tests_user.rs"]
mod user;

// Re-export helpers for `cargo test <name>` stability and cross-file use.
pub(crate) use helpers::assert_shape;
pub(crate) use helpers::load_props;
pub(crate) use helpers::test_account;
pub(crate) use helpers::test_media;
pub(crate) use helpers::test_note;
pub(crate) use helpers::test_services;
pub(crate) use support::Mem;
