//! Wave 3 (Agent webhook-SSRF) negative tests: webhook delivery never
//! dials private addresses, and header injection never reaches the wire.
//!
//! - `execute_webhook_deliver` forces `allow_private_ips = false` even when
//!   handed a permissive client (the federation testing exemption): delivery
//!   to `http://169.254.169.254/` and to loopback quarantines as
//!   `PermanentFailure` ("SSRF blocked"), regardless of `validate_hook_url`
//!   accepting loopback as a shape.
//! - `SafeHttpClient::send` rejects header names/values containing CR or LF
//!   at entry with a permanent error, before any socket or subprocess I/O
//!   (proven by asserting the loopback listener observes zero connections).

use std::net::TcpListener;
use std::time::Duration;

use pinetree::application::commands::webhooks::{WebhookDeliverPayload, execute_webhook_deliver};
use pinetree::domain::webhook::validate_hook_url;
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig, SendErrorKind};
use pinetree::federation::retry::{DeliveryOutcome, DestinationTracker};

/// A client with the federation testing exemption on: on the old code the
/// webhook path inherited this flag and dialed private addresses.
fn permissive_client() -> SafeHttpClient {
    SafeHttpClient::new(SafeHttpConfig {
        allow_private_ips: true,
        timeout: Duration::from_secs(2),
        total_timeout: Duration::from_secs(3),
        ..SafeHttpConfig::default()
    })
}

fn webhook_payload(url: &str) -> WebhookDeliverPayload {
    WebhookDeliverPayload {
        webhook_id: "hook-1".to_string(),
        url: url.to_string(),
        secret: "s3cret".to_string(),
        event_type: "abuseReport".to_string(),
        body: serde_json::json!({"hookId": "hook-1"}),
        is_system: true,
        owner_id: None,
    }
}

fn deliver(url: &str) -> DeliveryOutcome {
    execute_webhook_deliver(
        &permissive_client(),
        &DestinationTracker::new(),
        1_000,
        0,
        &webhook_payload(url),
    )
}

#[test]
fn webhook_delivery_blocks_link_local_metadata_ip() {
    match deliver("http://169.254.169.254/hook") {
        DeliveryOutcome::PermanentFailure(reason) => assert!(
            reason.contains("SSRF blocked"),
            "metadata IP must be SSRF-blocked, got: {reason}"
        ),
        other => panic!("metadata IP must quarantine as permanent, got: {other:?}"),
    }
}

#[test]
fn webhook_delivery_blocks_loopback_despite_shape_gate() {
    // The shape gate accepts loopback by design (SSRF is enforced at
    // delivery, not at registration).
    assert!(validate_hook_url("http://127.0.0.1:9/hook").is_ok());
    match deliver("http://127.0.0.1:9/hook") {
        DeliveryOutcome::PermanentFailure(reason) => assert!(
            reason.contains("SSRF blocked"),
            "loopback must be SSRF-blocked on the webhook path, got: {reason}"
        ),
        other => panic!("loopback must quarantine as permanent, got: {other:?}"),
    }
}

const HEADER_INJECTIONS: &[(&str, &str)] = &[
    ("X-Test", "ok\r\nBcc: x@y"),
    ("X-Test", "ok\r\nDATA\r\n"),
    ("X-Test", "ok\nCc: other@example.com"),
    ("X-Test", "ok\rsingle"),
    ("X-Evil\r\nInjected", "1"),
    ("X-Evil\nInjected", "1"),
];

/// A poisoned header must fail at `send()` entry with a permanent error and
/// never open a connection: the loopback listener must observe zero connects.
#[test]
fn header_crlf_rejected_before_wire() {
    for (name, value) in HEADER_INJECTIONS {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        let port = listener.local_addr().expect("addr").port();
        let err = permissive_client()
            .send(
                "POST",
                &format!("http://127.0.0.1:{port}/hook"),
                &[(name, value)],
                Some(b"{}"),
                1024,
            )
            .expect_err("CRLF header must fail");
        assert_eq!(
            err.kind,
            SendErrorKind::InvalidUrl,
            "header injection must be a permanent error for {name:?}: {value:?}"
        );
        assert!(
            !err.kind.is_transient(),
            "header injection must never retry for {name:?}: {value:?}"
        );
        assert!(
            err.to_string().contains("CR/LF"),
            "error must name the cause, got: {err}"
        );
        // No TCP connection may have been opened: a nonblocking accept must
        // report `WouldBlock` (no pending peer).
        match listener.accept() {
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Ok(_) => panic!("injected header reached the socket: {name:?}: {value:?}"),
            Err(e) => panic!("unexpected accept error: {e:?}"),
        }
        // Give a late/slow connect every chance to arrive, then re-check.
        std::thread::sleep(Duration::from_millis(100));
        match listener.accept() {
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Ok(_) => panic!("late injected-header connection reached socket: {name:?}"),
            Err(e) => panic!("unexpected accept error: {e:?}"),
        }
    }
}
