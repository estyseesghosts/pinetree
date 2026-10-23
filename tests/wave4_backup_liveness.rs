//! Wave 4 (agent backup): liveness under load for the backup drain.
//!
//! Hermetic where possible (temp dirs + crafted bytes); structural where the
//! subject is the server wiring itself (`src/main.rs`, `runner_lock`).
//!
//! * The drain (`drain_pending_backups`: redb scan, zip, sha256) must run on
//!   `spawn_blocking`, never on a Tokio worker, and `runner_lock` must never
//!   be held across `.await`.
//! * `MAX_TABLE_ROW_BYTES` is a small-metadata cap (1–4 MiB); oversized
//!   framed values stream to temp files instead of `vec![0u8; len]`.
//! * Both status-list paths skip corrupt `*.status.json` observably (a
//!   `backup.status_skip` warn line), the bounded path keeps O(cap) retained
//!   memory with the newest-first tie-break (`requested_ms`, then `job_id`)
//!   intact, and never delegates to the full listing.
//!
//! This file is uniquely named so it cannot collide with the shared
//! `tests/backup_status_bound.rs` (which must keep passing untouched).

use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::backup::bundle::{MAX_TABLE_ROW_BYTES, decode_table_file};
use pinetree::backup::{BackupState, BackupStatus, list_backup_statuses, newest_backup_statuses};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn work_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinetree-wave4-liveness-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_status_file(
    backup_dir: &std::path::Path,
    job_id: &str,
    state: BackupState,
    requested_ms: u64,
) {
    let status = BackupStatus {
        job_id: job_id.to_string(),
        state,
        requested_ms,
        started_ms: Some(requested_ms),
        finished_ms: Some(requested_ms),
        attempts: 1,
        output_file: None,
        bytes_written: None,
        tables_exported: 0,
        media_files: 0,
        gate_wait_ms: 0,
        failure_redacted: None,
    };
    std::fs::write(
        backup_dir.join(format!("{job_id}.status.json")),
        serde_json::to_vec_pretty(&status).unwrap(),
    )
    .unwrap();
}

fn fn_body<'a>(source: &'a str, marker: &str) -> &'a str {
    let start = source
        .find(marker)
        .unwrap_or_else(|| panic!("{marker} missing from source"));
    let rest = &source[start..];
    let end = rest[1..]
        .find("\npub ")
        .map(|pos| pos + 1)
        .unwrap_or(rest.len());
    &rest[..end]
}

#[test]
fn row_cap_is_a_small_metadata_cap() {
    const MIB: u64 = 1024 * 1024;
    assert!(
        (1 * MIB..=4 * MIB).contains(&MAX_TABLE_ROW_BYTES),
        "MAX_TABLE_ROW_BYTES must be a 1–4 MiB small-metadata cap, got {MAX_TABLE_ROW_BYTES}"
    );
}

#[test]
fn oversized_row_declaration_fails_closed_on_the_cap() {
    // 8 MiB declared value: over the new 1–4 MiB cap, far under the old
    // 64 MiB cap. Old code passed the cap check and failed later with
    // "truncated table payload"; new code must fail on the cap itself —
    // without ever allocating the declared length.
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"PBT1");
    bytes.extend_from_slice(&4u32.to_le_bytes());
    bytes.extend_from_slice(&(8u32 * 1024 * 1024).to_le_bytes());
    bytes.extend_from_slice(b"key!");
    let err = decode_table_file(&bytes, 1).expect_err("oversized row must fail");
    assert!(
        err.to_string().contains("exceeds cap"),
        "oversized row must fail on the cap, got: {err}"
    );
}

#[test]
fn corrupt_status_files_skip_observably_with_tie_break_intact() {
    let dir = work_dir("status");
    let requested: u64 = 1_760_500_000_000;
    for job_id in [
        "bk-live-a",
        "bk-live-b",
        "bk-live-c",
        "bk-live-d",
        "bk-live-e",
    ] {
        write_status_file(&dir, job_id, BackupState::Completed, requested);
    }
    std::fs::write(dir.join("bk-live-corrupt.status.json"), b"{ not json").unwrap();

    // Full listing: corrupt record skipped, valid records oldest-first.
    let listed = list_backup_statuses(&dir);
    assert_eq!(
        listed.len(),
        5,
        "corrupt record must be skipped: {listed:?}"
    );
    assert_eq!(listed[0].job_id, "bk-live-a");
    assert_eq!(listed[4].job_id, "bk-live-e");

    // Bounded tail with a cap smaller than the tie group: only the largest
    // job ids survive, newest-first — the corrupt file changes nothing
    // about the (requested_ms, job_id) tie-break.
    let newest = newest_backup_statuses(&dir, 3);
    assert_eq!(newest.len(), 3);
    let ids: Vec<&str> = newest.iter().map(|s| s.job_id.as_str()).collect();
    assert_eq!(ids, vec!["bk-live-e", "bk-live-d", "bk-live-c"]);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn corrupt_skips_are_logged_in_both_list_paths() {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/backup/mod.rs"))
        .expect("read backup/mod.rs");
    for marker in [
        "pub fn list_backup_statuses",
        "pub fn newest_backup_statuses",
    ] {
        let body = fn_body(&source, marker);
        assert!(
            body.contains("backup.status_skip"),
            "{marker} must log corrupt skips as backup.status_skip"
        );
        assert!(
            body.contains("log_event"),
            "{marker} must emit a log line for corrupt skips, not skip silently"
        );
    }
    let bounded = fn_body(&source, "pub fn newest_backup_statuses");
    assert!(
        !bounded.contains("list_backup_statuses("),
        "bounded path must scan the directory itself, never delegate to the full listing"
    );
}

#[test]
fn restore_streams_framed_values_without_attacker_sized_alloc() {
    let source = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/backup/restore.rs"
    ))
    .expect("read backup/restore.rs");
    assert!(
        !source.contains("vec![0u8; klen]") && !source.contains("vec![0u8; vlen]"),
        "restore must never allocate vec![0u8; len] from an untrusted header"
    );
    assert!(
        source.contains("drain_framed_to_temp"),
        "oversized framed values must drain to a temp file and fail closed"
    );
}

#[test]
fn backup_drain_runs_off_the_tokio_worker() {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/main.rs"))
        .expect("read src/main.rs");
    let call = source
        .find("drain_pending_backups(&backup_ctx, 1)")
        .expect("backup drain call must exist");
    let window = &source[call.saturating_sub(3000)..call];
    assert!(
        window.contains("spawn_blocking"),
        "drain_pending_backups (redb scan + zip + sha256) must run on \
         spawn_blocking, never directly on a Tokio worker"
    );
}

#[test]
fn runner_lock_is_never_held_across_await() {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/backup/mod.rs"))
        .expect("read backup/mod.rs");
    for marker in ["pub fn run_next_backup", "pub fn run_backup_blocking"] {
        let body = fn_body(&source, marker);
        assert!(
            body.contains("runner_lock()"),
            "{marker} must keep the exclusive backup gate"
        );
        assert!(
            !body.contains(".await"),
            "{marker} must never hold runner_lock across .await"
        );
    }
}
