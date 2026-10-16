use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeploymentMode {
    Single,
    Cluster,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub public_url: String,
    pub mode: DeploymentMode,
    pub data_dir: PathBuf,
    pub listen: SocketAddr,
    pub max_file_size: u64,
    pub preserved_usernames: Vec<String>,
    pub prohibited_words: Vec<String>,
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
        }
    }
}

impl Config {
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
        Ok(())
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
