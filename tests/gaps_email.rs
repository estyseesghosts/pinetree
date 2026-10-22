//! WP3 hermetic email tests: config, builders, gates, and the real
//! `SmtpEmailSender` against a loopback fake SMTP server (never live send).
//!
//! The fake speaks just enough SMTP (greeting / EHLO / AUTH LOGIN /
//! MAIL / RCPT / DATA / QUIT) to prove the client dialogue byte-for-byte.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use pinetree::services::email::{
    EmailMessage, EmailSendPayload, EmailSender, MAX_EMAIL_BODY_BYTES, SmtpConfig, SmtpEmailSender,
    abuse_report_email, encode_subject, execute_email_send, password_reset_email,
    signup_email_gate, verification_email,
};

// --- config ---------------------------------------------------------------------

#[test]
fn smtp_config_serde_round_trip_and_defaults() {
    let cfg: SmtpConfig = serde_json::from_str("{}").expect("empty object parses");
    assert_eq!(cfg, SmtpConfig::default());
    assert!(!cfg.is_configured());
    assert!(cfg.validate().is_ok(), "unconfigured is valid (dormant)");

    let cfg: SmtpConfig = serde_json::from_str(
        r#"{
            "smtp_host": "relay.example",
            "smtp_port": 587,
            "smtp_username": "user",
            "smtp_password": "pass",
            "smtp_from": "noreply@example.com",
            "email_required_for_signup": true
        }"#,
    )
    .unwrap();
    assert!(cfg.validate().is_ok());
    assert!(cfg.is_configured());
    assert!(cfg.email_required_for_signup);
    let back: SmtpConfig = serde_json::from_value(serde_json::to_value(&cfg).unwrap()).unwrap();
    assert_eq!(back, cfg);
}

#[test]
fn smtp_config_rejects_half_filled_blocks() {
    // Host without from.
    let cfg = SmtpConfig {
        smtp_host: "relay.example".into(),
        ..Default::default()
    };
    assert!(cfg.validate().is_err());
    // From without host.
    let cfg = SmtpConfig {
        smtp_from: "noreply@example.com".into(),
        ..Default::default()
    };
    assert!(cfg.validate().is_err());
    // Zero port.
    let cfg = SmtpConfig {
        smtp_host: "relay.example".into(),
        smtp_from: "noreply@example.com".into(),
        smtp_port: 0,
        ..Default::default()
    };
    assert!(cfg.validate().is_err());
    // Malformed from.
    let cfg = SmtpConfig {
        smtp_host: "relay.example".into(),
        smtp_from: "not-an-email".into(),
        ..Default::default()
    };
    assert!(cfg.validate().is_err());
    // Split auth (username without password).
    let cfg = SmtpConfig {
        smtp_host: "relay.example".into(),
        smtp_from: "noreply@example.com".into(),
        smtp_username: "user".into(),
        ..Default::default()
    };
    assert!(cfg.validate().is_err());
    // Unknown keys stay hard errors (typo safety, config rule).
    assert!(serde_json::from_str::<SmtpConfig>(r#"{"smtp_hosts": "x"}"#).is_err());
}

// --- messages --------------------------------------------------------------------

#[test]
fn message_validation_gates() {
    let good = EmailMessage {
        to: vec!["a@example.com".to_string()],
        subject: "hi".to_string(),
        text_body: "body".to_string(),
    };
    assert!(good.validate().is_ok());
    assert!(
        EmailMessage {
            to: vec![],
            ..good.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        EmailMessage {
            to: vec!["bad".to_string()],
            ..good.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        EmailMessage {
            subject: "  ".to_string(),
            ..good.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        EmailMessage {
            text_body: "x".repeat(MAX_EMAIL_BODY_BYTES + 1),
            ..good.clone()
        }
        .validate()
        .is_err()
    );
}

#[test]
fn subject_encoding_and_signup_gate() {
    assert_eq!(encode_subject("plain ascii"), "plain ascii");
    let enc = encode_subject("héllo ✅");
    assert!(enc.starts_with("=?UTF-8?B?"), "{enc}");
    assert!(!enc.contains('\n'));

    assert!(signup_email_gate(false, None).is_ok());
    assert!(
        signup_email_gate(true, None).is_err(),
        "required email missing"
    );
    assert!(signup_email_gate(true, Some("bad")).is_err());
    assert!(signup_email_gate(true, Some("new@example.com")).is_ok());
}

#[test]
fn builders_carry_codes_and_links() {
    let v = verification_email("u@example.com", "https://srv.example/", "CODE123");
    assert_eq!(v.to, vec!["u@example.com".to_string()]);
    assert!(v.text_body.contains("CODE123"));
    assert!(
        v.text_body
            .contains("https://srv.example/verify-email/CODE123")
    );

    let r = password_reset_email("u@example.com", "https://srv.example", "TOKEN9");
    assert!(r.subject.contains("password"));
    assert!(
        r.text_body
            .contains("https://srv.example/reset-password/TOKEN9")
    );
    assert!(r.text_body.contains("30 minutes"));

    let a = abuse_report_email(&["m@example.com".to_string()], "rep7", "spam wave");
    assert!(a.subject.contains("rep7"));
    assert!(a.text_body.contains("spam wave"));
    assert_eq!(a.to, vec!["m@example.com".to_string()]);
}

// --- durable send via recorder -----------------------------------------------------

#[derive(Default)]
struct Recorder {
    sent: Mutex<Vec<EmailMessage>>,
    fail: Mutex<Option<String>>,
}

impl EmailSender for Recorder {
    fn send(&self, msg: &EmailMessage) -> Result<(), pinetree::services::email::EmailError> {
        if let Some(reason) = self.fail.lock().unwrap().clone() {
            return Err(pinetree::services::email::EmailError::Rejected(reason));
        }
        self.sent.lock().unwrap().push(msg.clone());
        Ok(())
    }
}

#[test]
fn execute_email_send_validates_before_sending() {
    let recorder = Recorder::default();
    execute_email_send(
        &recorder,
        &EmailSendPayload {
            to: vec!["a@example.com".to_string()],
            subject: "s".to_string(),
            body: "b".to_string(),
            dedup_key: "k1".to_string(),
        },
    )
    .unwrap();
    assert_eq!(recorder.sent.lock().unwrap().len(), 1);
    // Invalid payload never reaches the sender.
    let err = execute_email_send(
        &recorder,
        &EmailSendPayload {
            to: vec![],
            subject: "s".to_string(),
            body: "b".to_string(),
            dedup_key: "k2".to_string(),
        },
    )
    .expect_err("empty recipients");
    assert!(matches!(
        err,
        pinetree::services::email::EmailError::InvalidMessage(_)
    ));
    assert_eq!(recorder.sent.lock().unwrap().len(), 1);
}

// --- hermetic fake SMTP ---------------------------------------------------------------

#[derive(Debug, Default)]
struct SmtpCapture {
    mail_from: Option<String>,
    rcpts: Vec<String>,
    data: Vec<u8>,
    auth_user: Option<String>,
    auth_pass: Option<String>,
    saw_auth: bool,
}

struct FakeSmtp {
    addr: std::net::SocketAddr,
    capture: Arc<Mutex<SmtpCapture>>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl FakeSmtp {
    fn spawn(rcpt_code: u16, require_auth: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let addr = listener.local_addr().unwrap();
        let capture: Arc<Mutex<SmtpCapture>> = Arc::new(Mutex::new(SmtpCapture::default()));
        let out = Arc::clone(&capture);
        let handle = std::thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept");
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                .unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut writer = stream;
            let mut cap = out.lock().unwrap();
            let mut say = |line: &str| {
                writer.write_all(line.as_bytes()).unwrap();
                writer.flush().unwrap();
            };
            type TcpStreamClone = std::net::TcpStream;
            let read_line = |reader: &mut BufReader<TcpStreamClone>| -> String {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                line.trim_end_matches(['\r', '\n']).to_string()
            };
            say("220 fake ESMTP\r\n");
            assert_eq!(read_line(&mut reader).to_uppercase(), "EHLO PINETREE");
            say("250-fake\r\n250 AUTH LOGIN\r\n");
            let mut line = read_line(&mut reader);
            if line.to_uppercase() == "AUTH LOGIN" {
                cap.saw_auth = true;
                say("334 VXNlcm5hbWU6\r\n");
                let user_b64 = read_line(&mut reader);
                say("334 UGFzc3dvcmQ6\r\n");
                let pass_b64 = read_line(&mut reader);
                cap.auth_user = Some(user_b64);
                cap.auth_pass = Some(pass_b64);
                if require_auth {
                    say("235 ok\r\n");
                } else {
                    say("535 no\r\n");
                    return;
                }
                line = read_line(&mut reader);
            }
            assert!(line.to_uppercase().starts_with("MAIL FROM:"), "{line}");
            cap.mail_from = Some(line);
            say("250 ok\r\n");
            loop {
                let rcpt = read_line(&mut reader);
                if rcpt.to_uppercase() == "DATA" {
                    break;
                }
                assert!(rcpt.to_uppercase().starts_with("RCPT TO:"), "{rcpt}");
                cap.rcpts.push(rcpt);
                say(&format!(
                    "{rcpt_code} {}\r\n",
                    if rcpt_code == 250 { "ok" } else { "nope" }
                ));
                if rcpt_code != 250 {
                    return;
                }
            }
            say("354 end with .\r\n");
            let mut data = Vec::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let t = line.trim_end_matches(['\r', '\n']).to_string();
                if t == "." {
                    break;
                }
                data.extend_from_slice(line.as_bytes());
            }
            cap.data = data;
            say("250 queued\r\n");
            let quit = read_line(&mut reader);
            assert_eq!(quit.to_uppercase(), "QUIT");
            say("221 bye\r\n");
        });
        Self {
            addr,
            capture,
            handle: Some(handle),
        }
    }

    fn config(&self, from: &str, user: &str, pass: &str) -> SmtpConfig {
        SmtpConfig {
            smtp_host: "127.0.0.1".to_string(),
            smtp_port: self.addr.port(),
            smtp_username: user.to_string(),
            smtp_password: pass.to_string(),
            smtp_from: from.to_string(),
            email_required_for_signup: false,
        }
    }

    fn finish(mut self) -> SmtpCapture {
        self.handle.take().unwrap().join().unwrap();
        let cap = self.capture.lock().unwrap();
        SmtpCapture {
            mail_from: cap.mail_from.clone(),
            rcpts: cap.rcpts.clone(),
            data: cap.data.clone(),
            auth_user: cap.auth_user.clone(),
            auth_pass: cap.auth_pass.clone(),
            saw_auth: cap.saw_auth,
        }
    }
}

use base64::Engine as _;

#[test]
fn real_client_completes_auth_dialogue_against_fake_smtp() {
    let server = FakeSmtp::spawn(250, true);
    let sender = SmtpEmailSender::new(server.config("noreply@example.com", "user", "pass"));
    sender
        .send(&EmailMessage {
            to: vec!["a@example.com".to_string(), "b@example.com".to_string()],
            subject: "hello".to_string(),
            // Leading-dot line exercises dot-stuffing.
            text_body: "first\n.line\nlast".to_string(),
        })
        .expect("send");
    let cap = server.finish();
    assert!(cap.saw_auth);
    assert_eq!(
        cap.auth_user
            .as_deref()
            .map(|s| base64::engine::general_purpose::STANDARD.decode(s).unwrap()),
        Some(b"user".to_vec())
    );
    assert_eq!(
        cap.auth_pass
            .as_deref()
            .map(|s| base64::engine::general_purpose::STANDARD.decode(s).unwrap()),
        Some(b"pass".to_vec())
    );
    assert!(
        cap.mail_from
            .as_deref()
            .unwrap()
            .contains("noreply@example.com")
    );
    assert_eq!(cap.rcpts.len(), 2);
    let data = String::from_utf8(cap.data).expect("data is text");
    assert!(data.contains("Subject: hello"));
    assert!(data.contains("To: a@example.com, b@example.com"));
    assert!(
        data.contains("first\r\n..line\r\nlast"),
        "dot-stuffed: {data:?}"
    );
}

#[test]
fn real_client_skips_auth_without_credentials() {
    let server = FakeSmtp::spawn(250, true);
    let sender = SmtpEmailSender::new(server.config("noreply@example.com", "", ""));
    sender
        .send(&EmailMessage {
            to: vec!["a@example.com".to_string()],
            subject: "s".to_string(),
            text_body: "b".to_string(),
        })
        .expect("send");
    let cap = server.finish();
    assert!(!cap.saw_auth, "no AUTH attempted without credentials");
}

#[test]
fn real_client_surfaces_rcpt_rejection() {
    let server = FakeSmtp::spawn(550, true);
    let sender = SmtpEmailSender::new(server.config("noreply@example.com", "u", "p"));
    let err = sender
        .send(&EmailMessage {
            to: vec!["gone@example.com".to_string()],
            subject: "s".to_string(),
            text_body: "b".to_string(),
        })
        .expect_err("550 must fail");
    assert!(
        matches!(err, pinetree::services::email::EmailError::Rejected(_)),
        "{err:?}"
    );
    server.finish();
}

#[test]
fn unconfigured_sender_fails_without_connecting() {
    let sender = SmtpEmailSender::new(SmtpConfig::default());
    let err = sender
        .send(&EmailMessage {
            to: vec!["a@example.com".to_string()],
            subject: "s".to_string(),
            text_body: "b".to_string(),
        })
        .expect_err("dormant transport");
    assert!(
        matches!(err, pinetree::services::email::EmailError::NotConfigured),
        "{err:?}"
    );
}
