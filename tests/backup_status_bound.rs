//! Slice 8: `newest_backup_statuses` stays bounded in retained memory.
//!
//! Hermetic (temp dirs only, status files only — no store, no gate).

use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::backup::{BackupState, BackupStatus, list_backup_statuses, newest_backup_statuses};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn work_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinetree-status-bound-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn status_file_path(backup_dir: &std::path::Path, job_id: &str) -> std::path::PathBuf {
    backup_dir.join(format!("{job_id}.status.json"))
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
    let path = status_file_path(backup_dir, job_id);
    std::fs::write(&path, serde_json::to_vec_pretty(&status).unwrap()).unwrap();
}

fn status_file_count(backup_dir: &std::path::Path) -> usize {
    std::fs::read_dir(backup_dir)
        .map(|r| {
            r.filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().ends_with(".status.json"))
                .count()
        })
        .unwrap_or(0)
}

#[test]
fn newest_cap_returns_newest_tail_only() {
    let dir = work_dir("tail");
    let base: u64 = 1_760_400_000_000;
    for i in 0..50u64 {
        write_status_file(
            &dir,
            &format!("bk-bound-{i:05}"),
            BackupState::Completed,
            base + i,
        );
    }
    assert_eq!(status_file_count(&dir), 50);
    let newest = newest_backup_statuses(&dir, 20);
    assert_eq!(newest.len(), 20, "must return exactly cap records");
    for (pos, status) in newest.iter().enumerate() {
        let expected = base + 49 - pos as u64;
        assert_eq!(
            status.requested_ms, expected,
            "position {pos} must hold the newest remaining record"
        );
    }
    assert_eq!(newest[0].job_id, "bk-bound-00049");
    assert_eq!(newest[19].job_id, "bk-bound-00030");
    assert!(
        !newest.iter().any(|s| s.job_id == "bk-bound-00000"),
        "oldest records must fall outside the bounded tail"
    );
    assert_eq!(
        status_file_count(&dir),
        50,
        "bounded read must not delete status files"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn newest_cap_breaks_requested_ms_ties_by_descending_job_id() {
    let dir = work_dir("ties");
    let requested: u64 = 1_760_400_001_000;
    for job_id in ["bk-tie-a", "bk-tie-b", "bk-tie-c", "bk-tie-d", "bk-tie-e"] {
        write_status_file(&dir, job_id, BackupState::Completed, requested);
    }
    // Cap smaller than the tie group: only the largest job ids survive,
    // newest-first.
    let newest = newest_backup_statuses(&dir, 3);
    assert_eq!(newest.len(), 3);
    let ids: Vec<&str> = newest.iter().map(|s| s.job_id.as_str()).collect();
    assert_eq!(ids, vec!["bk-tie-e", "bk-tie-d", "bk-tie-c"]);
    // Full tie group with room: every record, descending job id.
    let newest = newest_backup_statuses(&dir, 10);
    assert_eq!(newest.len(), 5);
    let ids: Vec<&str> = newest.iter().map(|s| s.job_id.as_str()).collect();
    assert_eq!(
        ids,
        vec!["bk-tie-e", "bk-tie-d", "bk-tie-c", "bk-tie-b", "bk-tie-a"]
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn newest_cap_edge_cases_keep_list_order_untouched() {
    let dir = work_dir("edges");
    let base: u64 = 1_760_400_002_000;
    for i in 0..5u64 {
        write_status_file(
            &dir,
            &format!("bk-edge-{i:02}"),
            BackupState::Completed,
            base + i,
        );
    }
    assert!(newest_backup_statuses(&dir, 0).is_empty());
    let all = newest_backup_statuses(&dir, 100);
    assert_eq!(all.len(), 5);
    assert_eq!(all[0].job_id, "bk-edge-04");
    assert_eq!(all[4].job_id, "bk-edge-00");
    // `list_backup_statuses` keeps its oldest-first order for the worker
    // drain; this slice must not silently change it.
    let listed = list_backup_statuses(&dir);
    assert_eq!(listed.len(), 5);
    assert_eq!(listed[0].job_id, "bk-edge-00");
    assert_eq!(listed[4].job_id, "bk-edge-04");
    assert!(
        listed
            .windows(2)
            .all(|w| w[0].requested_ms <= w[1].requested_ms),
        "full listing stays oldest-first"
    );
    assert!(newest_backup_statuses(std::path::Path::new("/nonexistent-dir-xyz"), 20).is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn newest_scan_does_not_require_the_full_vec() {
    // Structural check: the bounded function must not delegate to the full
    // listing (load-everything-then-truncate). It scans the directory
    // itself and retains at most `cap` records.
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/backup/mod.rs"))
        .expect("read backup/mod.rs");
    let start = source
        .find("pub fn newest_backup_statuses")
        .expect("newest_backup_statuses exists");
    let end = source[start..]
        .find("pub struct RetentionReport")
        .map(|pos| start + pos)
        .expect("function ends before RetentionReport");
    let body = &source[start..end];
    assert!(
        !body.contains("list_backup_statuses"),
        "newest_backup_statuses must scan the directory itself instead of \
         loading the full list and truncating"
    );
}
