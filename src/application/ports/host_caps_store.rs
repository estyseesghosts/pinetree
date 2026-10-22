use crate::domain::id::PortError;
use serde::{Deserialize, Serialize};

/// Cached per-host ActivityPub software name (`docs/misskey-parity-plan.md`
/// §6; backs the outbound `Like` emoji/plain split). One row per lowercased
/// host: the `software.name` from the host's NodeInfo document plus the fetch
/// time. Rows are read off-transaction by the delivery worker; staleness is
/// decided by the caller against
/// [`crate::federation::host_caps::HOST_SOFTWARE_TTL_MS`]. Probe failures are
/// never cached (the caller treats a miss as emoji-aware and retries the
/// probe on the next delivery).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostSoftware {
    pub host: String,
    pub software: Option<String>,
    pub fetched_at_ms: i64,
}

/// Per-host capability cache backing [`HostSoftware`]: point lookups and
/// whole-row upserts only (no fan-out, no jobs — the delivery worker reads
/// and writes these rows outside any state transaction).
pub trait HostCapsStore: Send + Sync {
    fn get_host_software(&self, host: &str) -> Result<Option<HostSoftware>, PortError>;
    fn put_host_software(&self, entry: HostSoftware) -> Result<(), PortError>;
}
