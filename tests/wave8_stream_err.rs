//! Wave 8 streaming-errors regression tests (adversarial-3 §Wave 8).
//!
//! - `StorageUnavailable` DB strings never reach the HTTP body: the
//!   response is the generic `INTERNAL_ERROR` with no `info`.
//! - `Hub::send` at-most-once drops are counted (`streaming.hub_dropped`)
//!   without changing the non-blocking drop policy.
//!
//! Both tests fail on the old behavior: the old `error.rs:188` put the DB
//! string in `info`, and the old `Hub::send` discarded the `Err` silently.

use pinetree::api::misskey::errors::ApiError;
use pinetree::application::events::StreamingEvent;
use pinetree::application::ports::StreamingSink;
use pinetree::domain::id::PortError;
use pinetree::observability;
use pinetree::streaming::hub::{HubMessage, hub_and_sink};

/// Sentinel with path-, key-, and row-like substrings: if any of it ever
/// reaches the HTTP body, the storage layer is leaking internals.
const DB_SENTINEL: &str =
    "WAVE8_DB_SENTINEL /var/lib/redb/main.redb key=ab12 token=tok_secret row={\"x\":1}";

fn counted(category: &str) -> u64 {
    observability::error_counts()
        .get(category)
        .copied()
        .unwrap_or(0)
}

/// The hub drop ledger is process-global: serialize these tests so a
/// concurrent drop-count increment cannot slip between a `before` and an
/// `after` read.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn storage_unavailable_never_leaks_db_string() {
    let _guard = SERIAL.lock().unwrap();
    let err = ApiError::from_port_error(PortError::StorageUnavailable(DB_SENTINEL.to_string()));
    assert_eq!(err.code, "INTERNAL_ERROR");
    assert_eq!(err.kind, "server");
    assert_eq!(err.status(), axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    assert!(
        err.info.is_none(),
        "storage faults carry no info block at all"
    );
    let rendered = serde_json::to_string(&err.body()).expect("body serializes");
    assert!(
        !rendered.contains("WAVE8_DB_SENTINEL"),
        "DB sentinel leaked into HTTP body: {rendered}"
    );
    assert!(
        !rendered.contains("var/lib/redb"),
        "DB path leaked into HTTP body: {rendered}"
    );
    // The envelope still identifies the generic failure for clients.
    assert!(rendered.contains("INTERNAL_ERROR"));
}
#[test]
fn hub_drop_without_receivers_is_counted() {
    let _guard = SERIAL.lock().unwrap();
    let (hub, _) = hub_and_sink(16);
    assert_eq!(hub.receiver_count(), 0);
    let before = counted("streaming.hub_dropped");
    hub.send(HubMessage::Note {
        note_id: "n1".to_string(),
    });
    assert_eq!(
        counted("streaming.hub_dropped"),
        before + 1,
        "a no-receiver broadcast drop must be counted"
    );
}

#[test]
fn hub_delivered_broadcast_is_not_counted_as_drop() {
    let _guard = SERIAL.lock().unwrap();
    let (hub, sink) = hub_and_sink(16);
    let mut rx = hub.subscribe();
    let before = counted("streaming.hub_dropped");
    sink.publish(
        "streaming",
        &serde_json::to_vec(&StreamingEvent::NoteCreated {
            note_id: "n2".to_string(),
        })
        .expect("event serializes"),
    )
    .expect("publish routes");
    match rx.try_recv().expect("live receiver gets the event") {
        HubMessage::Note { note_id } => assert_eq!(note_id, "n2"),
        other => panic!("unexpected hub message: {other:?}"),
    }
    assert_eq!(
        counted("streaming.hub_dropped"),
        before,
        "delivered broadcasts must not count as drops"
    );
}
