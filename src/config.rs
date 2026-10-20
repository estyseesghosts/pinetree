use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;

/// Deployment mode (Phase 4 owns `Cluster`; selecting it is a startup
/// error until then).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeploymentMode {
    Single,
    Cluster,
}

impl Default for DeploymentMode {
    fn default() -> Self {
        DeploymentMode::Single
    }
}

/// Server configuration. Validated before any services are built.
///
/// JSON keys match the field names. Every key is optional in the file:
/// absent keys take the [`Default`] values below. Unknown keys are
/// rejected (typo safety). `--config=PATH` sets the base; CLI flags
/// override file values; `validate()` runs once over the merged result.
///
/// No `env:` indirection exists here (unlike birchtree): pinetree's
/// config carries no secrets, so there is nothing to keep out of the file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Public base URL used for actor/object URIs (must be http/https).
    pub public_url: String,
    pub mode: DeploymentMode,
    pub data_dir: PathBuf,
    pub listen: SocketAddr,
    /// Maximum media upload size in bytes.
    pub max_file_size: u64,
    /// Local usernames that can never register (upstream `preservedUsernames`).
    pub preserved_usernames: Vec<String>,
    /// Word patterns rejected in display names / profile text.
    pub prohibited_words: Vec<String>,
    /// Words that force the sensitive flag on notes carrying them.
    pub sensitive_words: Vec<String>,
    /// Reverse proxies whose `X-Forwarded-For` is honored for client IPs
    /// (slice 9, H4). Empty by default: the TCP peer is always the client.
    pub trusted_proxies: Vec<IpAddr>,
    /// Maximum note drafts per account (Misskey `RoleService`
    /// `noteDraftLimit` default 10; §5).
    pub note_draft_limit: usize,
    /// Maximum actually-scheduled drafts per account (Misskey `RoleService`
    /// `scheduledNoteLimit` default 1; §5).
    pub scheduled_note_limit: usize,
    /// Maximum pinned notes per account (Misskey `RoleService` `pinLimit`
    /// default 5; §7).
    pub pin_limit: usize,
    /// Instance-wide default like emoji (Sharkey/Catodon `defaultLike`
    /// parity): the emoji `notes/like` and reaction-less
    /// `notes/reactions/create` resolve to when the caller passes no
    /// `override`/`reaction` and the reactor has no `defaultReaction`.
    /// `None` (default) means the bare heart ❤. Any unicode emoji or
    /// local custom `:name:` works; unknown/custom-gated names still fall
    /// back to ❤ through the reaction gate at use time.
    pub default_like: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            public_url: "https://example.local".to_string(),
            mode: DeploymentMode::Single,
            data_dir: PathBuf::from("./data"),
            listen: "127.0.0.1:3000".parse().expect("valid default socket addr"),
            max_file_size: 262_144_000, // 250 MiB
            preserved_usernames: vec![
                "admin".to_string(),
                "root".to_string(),
                "system".to_string(),
                "i".to_string(),
            ],
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
            trusted_proxies: Vec::new(),
            note_draft_limit: crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
            scheduled_note_limit: crate::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
            pin_limit: crate::domain::pinning::DEFAULT_PIN_LIMIT,
            default_like: None,
        }
    }
}

impl Config {
    /// Validate configuration. Returns a human-readable error string.
    pub fn validate(&self) -> Result<(), String> {
        if self.mode == DeploymentMode::Cluster {
            return Err("cluster mode is not implemented (Phase 4)".to_string());
        }
        let url = self.public_url.trim_end_matches('/');
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return Err("public_url must start with http:// or https://".to_string());
        }
        let host = self.public_host();
        if host.is_empty() {
            return Err("public_url must include a host".to_string());
        }
        if self.data_dir.as_os_str().is_empty() {
            return Err("data_dir must not be empty".to_string());
        }
        if self.max_file_size == 0 {
            return Err("max_file_size must be > 0".to_string());
        }
        if self.note_draft_limit == 0 {
            return Err("note_draft_limit must be > 0".to_string());
        }
        if self.scheduled_note_limit == 0 {
            return Err("scheduled_note_limit must be > 0".to_string());
        }
        if self.pin_limit == 0 {
            return Err("pin_limit must be > 0".to_string());
        }
        if let Some(like) = self.default_like.as_deref() {
            let n = like.chars().count();
            if n == 0 || n > 260 {
                return Err("default_like must be 1..=260 chars".to_string());
            }
        }
        Ok(())
    }

    /// Load a JSON config file over the defaults. Unknown keys, unreadable
    /// files and malformed JSON are hard errors naming the path. CLI flags
    /// override afterwards; `validate()` runs once over the merged result.
    pub fn load_from_file(path: &std::path::Path) -> Result<Self, String> {
        let bytes = std::fs::read(path)
            .map_err(|err| format!("--config {}: {err}", path.display()))?;
        serde_json::from_slice(&bytes)
            .map_err(|err| format!("--config {}: {err}", path.display()))
    }

    pub fn normalized_public_url(&self) -> String {
        self.public_url.trim_end_matches('/').to_string()
    }

    pub fn public_host(&self) -> String {
        let stripped = self
            .public_url
            .trim_end_matches('/')
            .strip_prefix("https://")
            .or_else(|| self.public_url.trim_end_matches('/').strip_prefix("http://"))
            .unwrap_or("");
        stripped.split('/').next().unwrap_or("").to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        assert!(Config::default().validate().is_ok());
    }

    #[test]
    fn rejects_unknown_keys() {
        // Typo safety: `deny_unknown_fields` fails the whole file.
        let raw = r#"{"public_url": "https://example.local", "pubic_url": "x"}"#;
        assert!(serde_json::from_str::<Config>(raw).is_err());
    }

    #[test]
    fn missing_keys_take_defaults() {
        let cfg: Config = serde_json::from_str("{}").expect("empty object parses");
        assert_eq!(cfg.public_url, Config::default().public_url);
        assert_eq!(cfg.listen, Config::default().listen);
        assert_eq!(cfg.note_draft_limit, crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT);
        assert!(cfg.trusted_proxies.is_empty());
    }

    #[test]
    fn file_round_trip() {
        let dir = std::env::temp_dir().join(format!(
            "pinetree-config-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("pinetree.json");
        let body = r#"{
            "public_url": "https://social.example.com/",
            "listen": "0.0.0.0:3000",
            "max_file_size": 1048576,
            "trusted_proxies": ["10.0.0.1"],
            "note_draft_limit": 20,
            "scheduled_note_limit": 2,
            "pin_limit": 8,
            "preserved_usernames": ["admin"],
            "prohibited_words": ["spam"],
            "sensitive_words": ["nsfw"],
            "default_like": "👍"
        }"#;
        std::fs::write(&path, body).unwrap();
        let cfg = Config::load_from_file(&path).expect("load");
        assert_eq!(cfg.public_url, "https://social.example.com/");
        assert_eq!(cfg.normalized_public_url(), "https://social.example.com");
        assert_eq!(cfg.listen.to_string(), "0.0.0.0:3000");
        assert_eq!(cfg.max_file_size, 1048576);
        assert_eq!(cfg.trusted_proxies, vec!["10.0.0.1".parse::<IpAddr>().unwrap()]);
        assert_eq!(cfg.note_draft_limit, 20);
        assert_eq!(cfg.scheduled_note_limit, 2);
        assert_eq!(cfg.pin_limit, 8);
        assert_eq!(cfg.preserved_usernames, vec!["admin".to_string()]);
        assert_eq!(cfg.default_like.as_deref(), Some("👍"));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn default_like_bounds() {
        assert!(Config { default_like: None, ..Config::default() }.validate().is_ok());
        assert!(Config { default_like: Some("👍".to_string()), ..Config::default() }.validate().is_ok());
        assert!(Config { default_like: Some(":blobcat:".to_string()), ..Config::default() }.validate().is_ok());
        assert!(Config { default_like: Some(String::new()), ..Config::default() }.validate().is_err());
        assert!(Config { default_like: Some("r".repeat(261)), ..Config::default() }.validate().is_err());
    }

    #[test]
    fn load_missing_file_names_path() {
        let err = Config::load_from_file(std::path::Path::new("/nonexistent/pinetree.json"))
            .expect_err("missing file errors");
        assert!(err.contains("/nonexistent/pinetree.json"), "{err}");
    }

    #[test]
    fn rejects_bad_values() {
        for cfg in [
            Config { public_url: "ftp://example.local".into(), ..Config::default() },
            Config { public_url: "https://".into(), ..Config::default() },
            Config { max_file_size: 0, ..Config::default() },
            Config { note_draft_limit: 0, ..Config::default() },
            Config { scheduled_note_limit: 0, ..Config::default() },
            Config { pin_limit: 0, ..Config::default() },
            Config { mode: DeploymentMode::Cluster, ..Config::default() },
        ] {
            assert!(cfg.validate().is_err(), "{cfg:?}");
        }
        let empty_dir = Config { data_dir: PathBuf::from(""), ..Config::default() };
        assert!(empty_dir.validate().is_err());
    }
}
