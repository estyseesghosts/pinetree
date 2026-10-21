//! Real email delivery over SMTP plus the message builders for the
//! identity loop (`verify-email`, `request-reset-password`,
//! `i/update-email`) and the abuse-report recipient fan-out.
//!
//! The transport is a minimal, dependency-free SMTP submission client
//! (`SmtpEmailSender`): plain-text submission with optional `AUTH LOGIN`,
//! envelope + header `From` taken from config. TLS (SMTPS/STARTTLS) is NOT
//! attempted here — point `smtp_host` at a local relay or a cleartext
//! submission port, or front with a TLS-terminating relay; the proposed
//! config snippet documents the trade-off. Tests never touch the network
//! beyond loopback: `tests/gaps_email.rs` runs a hermetic fake SMTP server
//! on 127.0.0.1.

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use base64::Engine as _;

/// SMTP connection + I/O budget per send (one small message; the DATA
/// phase of a local relay is milliseconds).
pub const SMTP_TIMEOUT: Duration = Duration::from_secs(10);

/// Largest accepted message body (1 MiB — abuse-report fan-out carries
/// short summaries, never attachments).
pub const MAX_EMAIL_BODY_BYTES: usize = 1 << 20;

/// SMTP configuration. Lives in the JSON config file next to every other
/// knob (no `env:` indirection per the config rule); the parent adds these
/// fields to `src/config.rs` with the proposed snippet — this struct is the
/// serde shape the snippet mirrors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SmtpConfig {
    #[serde(default)]
    pub smtp_host: String,
    #[serde(default = "default_smtp_port")]
    pub smtp_port: u16,
    #[serde(default)]
    pub smtp_username: String,
    #[serde(default)]
    pub smtp_password: String,
    #[serde(default)]
    pub smtp_from: String,
    #[serde(default)]
    pub email_required_for_signup: bool,
}

fn default_smtp_port() -> u16 {
    25
}

impl Default for SmtpConfig {
    fn default() -> Self {
        Self {
            smtp_host: String::new(),
            smtp_port: default_smtp_port(),
            smtp_username: String::new(),
            smtp_password: String::new(),
            smtp_from: String::new(),
            email_required_for_signup: false,
        }
    }
}

impl SmtpConfig {
    /// `true` when enough of the transport is configured to attempt a send
    /// (host + from; auth is optional — open relays / local submission).
    /// The endpoints fail loudly with `EMAIL_NOT_CONFIGURED` otherwise.
    pub fn is_configured(&self) -> bool {
        !self.smtp_host.trim().is_empty() && !self.smtp_from.trim().is_empty()
    }

    /// Config-time validation for the proposed `Config::validate()`
    /// addition: ports non-zero, `from` well-shaped, auth all-or-nothing.
    pub fn validate(&self) -> Result<(), String> {
        if !self.is_configured() {
            // Unconfigured is valid (dormant transport); only validate a
            // half-filled block.
            if self.smtp_host.trim().is_empty()
                && self.smtp_from.trim().is_empty()
                && self.smtp_username.is_empty()
                && self.smtp_password.is_empty()
            {
                return Ok(());
            }
            return Err(
                "smtp_host and smtp_from are both required to enable email".to_string(),
            );
        }
        if self.smtp_port == 0 {
            return Err("smtp_port must be non-zero".to_string());
        }
        crate::domain::webhook::validate_email_shape(&self.smtp_from)
            .map_err(|e| format!("smtp_from: {e}"))?;
        if self.smtp_username.is_empty() != self.smtp_password.is_empty() {
            return Err(
                "smtp_username and smtp_password must be set together".to_string(),
            );
        }
        Ok(())
    }
}

/// One outbound message (envelope recipients + headers + text body).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmailMessage {
    pub to: Vec<String>,
    pub subject: String,
    pub text_body: String,
}

impl EmailMessage {
    pub fn validate(&self) -> Result<(), EmailError> {
        if self.to.is_empty() {
            return Err(EmailError::InvalidMessage(
                "email needs at least one recipient".to_string(),
            ));
        }
        for addr in &self.to {
            crate::domain::webhook::validate_email_shape(addr).map_err(|e| {
                EmailError::InvalidMessage(format!("bad recipient {addr:?}: {e}"))
            })?;
        }
        if self.subject.trim().is_empty() || self.subject.chars().count() > 200 {
            return Err(EmailError::InvalidMessage(
                "subject must be 1..=200 chars".to_string(),
            ));
        }
        if self.text_body.len() > MAX_EMAIL_BODY_BYTES {
            return Err(EmailError::InvalidMessage(
                "body exceeds size cap".to_string(),
            ));
        }
        Ok(())
    }
}

/// Durable email job payload (ids + rendered text only — never secrets
/// beyond the envelope; at most one recipient set per job so a replay
/// re-sends the same bytes under the same idempotency key).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmailSendPayload {
    pub to: Vec<String>,
    pub subject: String,
    pub body: String,
    /// Caller-supplied dedup scope (e.g. `verify:{account_id}:{code}`).
    pub dedup_key: String,
}

/// Send failure modes. Transient vs permanent is decided by the worker
/// disposition for the proposed `JobKind::EmailSend` arm (5xx at DATA end
/// retries; auth/config failures quarantine) — this error only reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmailError {
    NotConfigured,
    InvalidMessage(String),
    Connection(String),
    Auth(String),
    Rejected(String),
}

impl std::fmt::Display for EmailError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EmailError::NotConfigured => write!(f, "email not configured"),
            EmailError::InvalidMessage(m) => write!(f, "invalid email: {m}"),
            EmailError::Connection(m) => write!(f, "smtp connection failed: {m}"),
            EmailError::Auth(m) => write!(f, "smtp auth failed: {m}"),
            EmailError::Rejected(m) => write!(f, "smtp rejected message: {m}"),
        }
    }
}

impl std::error::Error for EmailError {}

/// Blocking mail sender (the worker runs jobs on blocking threads, like
/// every other job handler).
pub trait EmailSender: Send + Sync {
    fn send(&self, msg: &EmailMessage) -> Result<(), EmailError>;
}

/// Real SMTP submission client. Plain-text only (see module docs).
pub struct SmtpEmailSender {
    pub config: SmtpConfig,
    pub timeout: Duration,
}

impl SmtpEmailSender {
    pub fn new(config: SmtpConfig) -> Self {
        Self {
            config,
            timeout: SMTP_TIMEOUT,
        }
    }

    fn connect(&self) -> Result<TcpStream, EmailError> {
        let addrs: Vec<SocketAddr> = format!("{}:{}", self.config.smtp_host, self.config.smtp_port)
            .to_socket_addrs()
            .map_err(|e| EmailError::Connection(format!("resolve {}: {e}", self.config.smtp_host)))?
            .collect();
        let addr = addrs
            .into_iter()
            .next()
            .ok_or_else(|| EmailError::Connection("no addresses".to_string()))?;
        let stream = TcpStream::connect_timeout(&addr, self.timeout)
            .map_err(|e| EmailError::Connection(format!("connect {addr}: {e}")))?;
        stream
            .set_read_timeout(Some(self.timeout))
            .and(stream.set_write_timeout(Some(self.timeout)))
            .map_err(|e| EmailError::Connection(format!("set timeouts: {e}")))?;
        Ok(stream)
    }
}

impl EmailSender for SmtpEmailSender {
    fn send(&self, msg: &EmailMessage) -> Result<(), EmailError> {
        if !self.config.is_configured() {
            return Err(EmailError::NotConfigured);
        }
        msg.validate()?;
        let stream = self.connect()?;
        let mut reader = BufReader::new(stream.try_clone().map_err(|e| {
            EmailError::Connection(format!("clone stream: {e}"))
        })?);
        let mut writer = stream;
        let mut session = SmtpSession { reader: &mut reader, writer: &mut writer };

        session.expect_code(220, "greeting")?;
        session.command("EHLO pinetree", 250, "EHLO")?;
        if !self.config.smtp_username.is_empty() {
            session.command("AUTH LOGIN", 334, "AUTH LOGIN")?;
            session.command(
                &base64::engine::general_purpose::STANDARD
                    .encode(self.config.smtp_username.as_bytes()),
                334,
                "AUTH username",
            )
            .map_err(|e| match e {
                EmailError::Rejected(m) => EmailError::Auth(m),
                other => other,
            })?;
            session
                .command(
                    &base64::engine::general_purpose::STANDARD
                        .encode(self.config.smtp_password.as_bytes()),
                    235,
                    "AUTH password",
                )
                .map_err(|e| match e {
                    EmailError::Rejected(m) => EmailError::Auth(m),
                    other => other,
                })?;
        }
        session.command(
            &format!("MAIL FROM:<{}>", self.config.smtp_from.trim()),
            250,
            "MAIL FROM",
        )?;
        for rcpt in &msg.to {
            // `command` already accepts 250 and 251 for RCPT TO.
            session.command(&format!("RCPT TO:<{rcpt}>"), 250, "RCPT TO")?;
        }
        session.command("DATA", 354, "DATA")?;
        session.write_data(&self.render(msg))?;
        session.expect_code(250, "message")?;
        let _ = session.command("QUIT", 221, "QUIT");
        Ok(())
    }
}

struct SmtpSession<'a> {
    reader: &'a mut BufReader<TcpStream>,
    writer: &'a mut TcpStream,
}

impl SmtpSession<'_> {
    fn read_reply(&mut self) -> Result<(u16, String), EmailError> {
        let mut line = String::new();
        loop {
            line.clear();
            self.reader
                .read_line(&mut line)
                .map_err(|e| EmailError::Connection(format!("read reply: {e}")))?;
            if line.len() > 4096 {
                return Err(EmailError::Connection("reply too long".to_string()));
            }
            // Multi-line replies use `250-...` continuations; the last line
            // uses `250 ...`.
            if line.len() >= 4 && line.as_bytes()[3] == b' ' {
                break;
            }
            if line.is_empty() {
                return Err(EmailError::Connection("empty reply".to_string()));
            }
        }
        let code: u16 = line
            .get(..3)
            .unwrap_or("")
            .parse()
            .map_err(|_| EmailError::Connection(format!("bad reply: {line:?}")))?;
        Ok((code, line))
    }

    fn expect_code(&mut self, want: u16, ctx: &str) -> Result<(), EmailError> {
        let (code, line) = self.read_reply()?;
        if code == want {
            Ok(())
        } else {
            Err(EmailError::Rejected(format!("{ctx}: got {code} ({line:?})")))
        }
    }

    fn command(&mut self, cmd: &str, want: u16, ctx: &str) -> Result<(), EmailError> {
        self.writer
            .write_all(format!("{cmd}\r\n").as_bytes())
            .map_err(|e| EmailError::Connection(format!("write {ctx}: {e}")))?;
        self.writer
            .flush()
            .map_err(|e| EmailError::Connection(format!("flush {ctx}: {e}")))?;
        // RCPT TO may answer 251 (forwarded) as well as 250.
        if ctx == "RCPT TO" && want == 250 {
            let (code, line) = self.read_reply()?;
            if code == 250 || code == 251 {
                return Ok(());
            }
            return Err(EmailError::Rejected(format!("{ctx}: got {code} ({line:?})")));
        }
        self.expect_code(want, ctx)
    }

    fn write_data(&mut self, bytes: &[u8]) -> Result<(), EmailError> {
        // Dot-stuffing: any line starting with '.' gains a second dot.
        let mut out = Vec::with_capacity(bytes.len() + 64);
        for line in bytes.split(|b| *b == b'\n') {
            let line = strip_cr(line);
            if line.first() == Some(&b'.') {
                out.push(b'.');
            }
            out.extend_from_slice(line);
            out.extend_from_slice(b"\r\n");
        }
        out.extend_from_slice(b".\r\n");
        self.writer
            .write_all(&out)
            .map_err(|e| EmailError::Connection(format!("write DATA: {e}")))?;
        self.writer
            .flush()
            .map_err(|e| EmailError::Connection(format!("flush DATA: {e}")))?;
        Ok(())
    }
}

fn strip_cr(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\r").unwrap_or(line)
}

impl SmtpEmailSender {
    fn render(&self, msg: &EmailMessage) -> Vec<u8> {
        let mut out = String::new();
        out.push_str(&format!("From: {}\r\n", self.config.smtp_from.trim()));
        out.push_str(&format!("To: {}\r\n", msg.to.join(", ")));
        out.push_str(&format!("Subject: {}\r\n", encode_subject(&msg.subject)));
        out.push_str("MIME-Version: 1.0\r\n");
        out.push_str("Content-Type: text/plain; charset=utf-8\r\n");
        out.push_str("Content-Transfer-Encoding: 8bit\r\n");
        out.push_str("\r\n");
        out.push_str(&msg.text_body.replace('\n', "\r\n"));
        if !msg.text_body.ends_with('\n') {
            out.push_str("\r\n");
        }
        out.into_bytes()
    }
}

/// RFC 2047 `=?UTF-8?B?...?=` encoding for non-ASCII subjects; ASCII
/// subjects pass through (folding omitted — subjects are capped at 200
/// chars and single-line by validation).
pub fn encode_subject(subject: &str) -> String {
    if subject.is_ascii() {
        return subject.trim().replace(['\r', '\n'], " ");
    }
    let clean: String = subject.chars().filter(|c| *c != '\r' && *c != '\n').collect();
    format!(
        "=?UTF-8?B?{}?=",
        base64::engine::general_purpose::STANDARD.encode(clean)
    )
}

/// Signup gate for the proposed `email_required_for_signup` flag: when the
/// instance requires email, signup without a well-shaped address fails.
pub fn signup_email_gate(
    email_required_for_signup: bool,
    email: Option<&str>,
) -> Result<(), EmailError> {
    if !email_required_for_signup {
        return Ok(());
    }
    let Some(addr) = email else {
        return Err(EmailError::InvalidMessage(
            "email is required for signup".to_string(),
        ));
    };
    crate::domain::webhook::validate_email_shape(addr)
        .map_err(|e| EmailError::InvalidMessage(format!("signup email: {e}")))
}

/// Verify-email message (`verify-email` code link).
pub fn verification_email(to: &str, base_url: &str, code: &str) -> EmailMessage {
    EmailMessage {
        to: vec![to.to_string()],
        subject: "Verify your email address".to_string(),
        text_body: format!(
            "Please verify your email address by visiting:\n{}/verify-email/{}\n\nIf you did not request this, ignore this message.\n",
            base_url.trim_end_matches('/'),
            code
        ),
    }
}

/// Password-reset message (`request-reset-password` token link).
pub fn password_reset_email(to: &str, base_url: &str, token: &str) -> EmailMessage {
    EmailMessage {
        to: vec![to.to_string()],
        subject: "Reset your password".to_string(),
        text_body: format!(
            "Reset your password (link valid 30 minutes):\n{}/reset-password/{}\n\nIf you did not request this, ignore this message.\n",
            base_url.trim_end_matches('/'),
            token
        ),
    }
}

/// Abuse-report notification (one per recipient batch; the fan-out planner
/// in `application::commands::webhooks` bounds the recipient count).
pub fn abuse_report_email(to: &[String], report_id: &str, summary: &str) -> EmailMessage {
    EmailMessage {
        to: to.to_vec(),
        subject: format!("Abuse report {report_id} needs review"),
        text_body: format!("A new abuse report needs review.\n\nReport: {report_id}\n{summary}\n"),
    }
}

/// Execute one durable `EmailSend` job against any sender (the proposed
/// worker arm passes the configured `SmtpEmailSender`; tests pass a
/// recorder). Validation failures are terminal (quarantine); transport
/// failures propagate for worker backoff.
pub fn execute_email_send(
    sender: &dyn EmailSender,
    payload: &EmailSendPayload,
) -> Result<(), EmailError> {
    let msg = EmailMessage {
        to: payload.to.clone(),
        subject: payload.subject.clone(),
        text_body: payload.body.clone(),
    };
    msg.validate()?;
    sender.send(&msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smtp_config_validation() {
        assert!(SmtpConfig::default().validate().is_ok());
        let half = SmtpConfig { smtp_host: "relay.local".into(), ..Default::default() };
        assert!(half.validate().is_err());
        let bad_from = SmtpConfig {
            smtp_host: "relay.local".into(),
            smtp_from: "not-an-email".into(),
            ..Default::default()
        };
        assert!(bad_from.validate().is_err());
        let split_auth = SmtpConfig {
            smtp_host: "relay.local".into(),
            smtp_from: "noreply@example.com".into(),
            smtp_username: "u".into(),
            ..Default::default()
        };
        assert!(split_auth.validate().is_err());
        let good = SmtpConfig {
            smtp_host: "relay.local".into(),
            smtp_from: "noreply@example.com".into(),
            ..Default::default()
        };
        assert!(good.validate().is_ok());
        assert!(good.is_configured());
        assert!(!SmtpConfig::default().is_configured());
    }

    #[test]
    fn subject_encoding() {
        assert_eq!(encode_subject("hello"), "hello");
        let enc = encode_subject("héllo ✅");
        assert!(enc.starts_with("=?UTF-8?B?"), "{enc}");
    }
}
