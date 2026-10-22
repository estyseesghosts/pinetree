//! Persistence conformance suite (Phase 1 Slice B2).
//!
//! Covers every case in `docs/persistence-conformance.md` (Groups A–F plus
//! the exit-box-2 acceptance test).
//!
//! Backend genericity: in-process cases are written as generic functions over
//! [`ConformanceStore`] (the port traits under test) and instantiated here
//! with [`RedbFactory`]. Phase 4 runs this exact suite against FoundationDB
//! by providing a second [`StoreFactory`] implementation; only the crash
//! harness (subprocess `SIGKILL` + reopen by path) is redb-specific and is
//! mirrored per backend.
//!
//! Crash cases spawn `std::env::current_exe()` with
//! `PINETREE_CONFORMANCE_CHILD=<kind>` + `PINETREE_DB_PATH=<path>` and args
//! `--exact <test> --nocapture` (`--nocapture` is load-bearing: libtest
//! captures per-test stdout by default, which would swallow the child's
//! `READY` line and deadlock the parent). The child commits/claims, drops
//! its `Database` handle, prints `READY`, then parks until the parent
//! `child.kill()`s it (`SIGKILL` on Linux). `commit()` fsyncs and the OS
//! releases the child's file locks on death, so the parent can reopen.

#[path = "conformance/account_store.rs"]
mod account_store;
#[path = "conformance/activity_dedup.rs"]
mod activity_dedup;
#[path = "conformance/antenna_store.rs"]
mod antenna_store;
#[path = "conformance/atomicity.rs"]
mod atomicity;
#[path = "conformance/auth_store.rs"]
mod auth_store;
#[path = "conformance/block_store.rs"]
mod block_store;
#[path = "conformance/clip_store.rs"]
mod clip_store;
#[path = "conformance/common.rs"]
mod common;
#[path = "conformance/emoji_store.rs"]
mod emoji_store;
#[path = "conformance/favorite_store.rs"]
mod favorite_store;
#[path = "conformance/follow_store.rs"]
mod follow_store;
#[path = "conformance/instance_settings_store.rs"]
mod instance_settings_store;
#[path = "conformance/invite_store.rs"]
mod invite_store;
#[path = "conformance/job_queue.rs"]
mod job_queue;
#[path = "conformance/media_store.rs"]
mod media_store;
#[path = "conformance/moderation_store.rs"]
mod moderation_store;
#[path = "conformance/note_edits.rs"]
mod note_edits;
#[path = "conformance/note_quotes.rs"]
mod note_quotes;
#[path = "conformance/note_replies.rs"]
mod note_replies;
#[path = "conformance/note_store.rs"]
mod note_store;
#[path = "conformance/notification_store.rs"]
mod notification_store;
#[path = "conformance/page_store.rs"]
mod page_store;
#[path = "conformance/registry_store.rs"]
mod registry_store;
#[path = "conformance/user_list_store.rs"]
mod user_list_store;
