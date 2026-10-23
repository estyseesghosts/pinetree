//! Wave 3 (Agent SMTP) negative tests: SMTP header/envelope injection
//! addresses must never reach the socket.
//!
//! Covers the two audit payloads (`"a@b\r\nBcc: x@y"`,
//! `"a@b\r\nDATA\r\n"`) plus the surrounding shape gate: the shape check
//! rejects them, `EmailMessage::validate` rejects them (canonical-trim +
//! re-validate), and `SmtpEmailSender::send` fails with `InvalidMessage`
//! *before* opening a TCP connection (proven by asserting the loopback
//! listener observes zero connections).

use std::net::TcpListener;
use std::time::Duration;

use pinetree::domain::webhook::validate_email_shape;
use pinetree::services::email::{
    EmailError, EmailMessage, EmailSender, SmtpConfig, SmtpEmailSender,
};

const INJECTIONS: &[&str] = &[
    "a@b\r\nBcc: x@y",
    "a@b\r\nDATA\r\n",
    "victim@example.com\r\nBcc: spam@evil.example",
    "a@example.com\nCc: other@example.com",
    "a@example.com\rDATA\r",
    "<a@example.com>",
    "a,b@example.com",
    "a b@example.com",
];

#[test]
fn injection_shapes_rejected() {
    for addr in INJECTIONS {
        assert!(
            validate_email_shape(addr).is_err(),
            "shape must reject {addr:?}"
        );
    }
    // Length and single-`@` rules are not weakened: valid mail still passes,
    // malformed mail without injection still fails.
    assert!(validate_email_shape("mod@example.com").is_ok());
    assert!(validate_email_shape("nope").is_err());
    assert!(validate_email_shape("a@b").is_err(), "dot rule intact");
    assert!(
        validate_email_shape("a@b@c.example").is_err(),
        "single-@ rule intact"
    );
    assert!(validate_email_shape("ab").is_err(), "length rule intact");
}

#[test]
fn message_validate_rejects_injection_recipients() {
    for addr in INJECTIONS {
        let msg = EmailMessage {
            to: vec![(*addr).to_string()],
            subject: "s".to_string(),
            text_body: "b".to_string(),
        };
        let err = msg.validate().expect_err("injection recipient must fail");
        assert!(
            matches!(err, EmailError::InvalidMessage(_)),
            "terminal, not retryable: {err:?} for {addr:?}"
        );
    }
}

#[test]
fn message_validate_rejects_padded_recipients() {
    // Canonical-trim: surrounding whitespace is not canonical.
    for addr in [" a@example.com", "a@example.com ", "\ta@example.com\t"] {
        assert!(
            EmailMessage {
                to: vec![addr.to_string()],
                subject: "s".to_string(),
                text_body: "b".to_string(),
            }
            .validate()
            .is_err(),
            "padded recipient must fail: {addr:?}"
        );
    }
}

/// Malicious recipients never reach the socket: `send` validates before
/// `connect`, so the loopback listener must observe zero connections and
/// the error must be terminal (`InvalidMessage`, quarantine) rather than a
/// transport failure (retry).
///
/// The first two payloads are the audit literals. The third is the
/// old-code witness: a single-`@` dotted-domain injection that the previous
/// shape check (length + `@`-count + dot only) *accepted*, so on the old
/// code `send` would `connect()` and write `RCPT TO:<…>` raw.
#[test]
fn injection_never_reaches_socket() {
    for payload in [
        "a@b\r\nBcc: x@y",
        "a@b\r\nDATA\r\n",
        "a@b.example\r\nBcc: x",
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        let port = listener.local_addr().expect("addr").port();
        let sender = SmtpEmailSender::new(SmtpConfig {
            smtp_host: "127.0.0.1".to_string(),
            smtp_port: port,
            smtp_from: "noreply@example.com".to_string(),
            ..Default::default()
        });
        let err = sender
            .send(&EmailMessage {
                to: vec![payload.to_string()],
                subject: "s".to_string(),
                text_body: "b".to_string(),
            })
            .expect_err("injection send must fail");
        assert!(
            matches!(err, EmailError::InvalidMessage(_)),
            "terminal InvalidMessage, got {err:?} for {payload:?}"
        );
        // No TCP connection may have been opened: a nonblocking accept must
        // report `WouldBlock` (no pending peer).
        match listener.accept() {
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Ok(_) => panic!("injection payload reached the socket: {payload:?}"),
            Err(e) => panic!("unexpected accept error: {e:?}"),
        }
        // Give a late/slow connect every chance to arrive, then re-check.
        std::thread::sleep(Duration::from_millis(100));
        match listener.accept() {
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Ok(_) => panic!("late injection connection reached socket: {payload:?}"),
            Err(e) => panic!("unexpected accept error: {e:?}"),
        }
    }
}

#[test]
fn injection_sender_from_never_reaches_socket() {
    // A poisoned envelope sender must also fail before `connect`.
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");
    let port = listener.local_addr().expect("addr").port();
    let sender = SmtpEmailSender::new(SmtpConfig {
        smtp_host: "127.0.0.1".to_string(),
        smtp_port: port,
        smtp_from: "a@b\r\nBcc: x@y".to_string(),
        ..Default::default()
    });
    let err = sender
        .send(&EmailMessage {
            to: vec!["victim@example.com".to_string()],
            subject: "s".to_string(),
            text_body: "b".to_string(),
        })
        .expect_err("poisoned sender must fail");
    assert!(
        matches!(err, EmailError::InvalidMessage(_)),
        "terminal InvalidMessage, got {err:?}"
    );
    match listener.accept() {
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
        Ok(_) => panic!("poisoned sender reached the socket"),
        Err(e) => panic!("unexpected accept error: {e:?}"),
    }
}
