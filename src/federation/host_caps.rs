//! Per-host ActivityPub software detection for the §6 outbound reaction
//! split (`docs/misskey-parity-plan.md` §6).
//!
//! Outbound reactions deliver per recipient host, classified by
//! [`crate::domain::fed_policy::ReactionStyle`]:
//! * Misskey-family (misskey, sharkey, foundkey, calckey, catodon,
//!   firefish, …) and unknown hosts get `Like` + `_misskey_reaction`;
//! * Akkoma/Pleroma get `EmojiReact` (`content`, plus an `Emoji` tag for
//!   custom `:shortcode:` reactions);
//! * Mastodon-family hosts get a bare Mastodon-favourite `Like`.
//! Detection is a lazy NodeInfo probe per host, fetched OFF-TRANSACTION in
//! the delivery worker (network can stall — never inside a state
//! transaction) via the existing SSRF-protected [`SafeHttpClient`], cached
//! in the `host_software` redb table (the `HostCapsStore` port trait) with
//! a ~7-day TTL. Probe failures are never cached; unknown/failed hosts are
//! treated as emoji-aware (an extra field is benign and matches upstream
//! Misskey, which always sends it).
//!
//! Placement: this lives in the federation layer, not `domain`, because it is
//! remote-software interop policy (software-name lists, NodeInfo fetching,
//! cache orchestration) over [`AppServices`]. The pure string core —
//! [`crate::domain::fed_policy::reaction_style_for_software`] — lives in
//! `domain`; only the probe/cache orchestration stays here.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

use crate::application::commands::AppServices;
use crate::application::ports::HostSoftware;
use crate::domain::fed_policy::{ReactionStyle, reaction_style_for_software};
use crate::federation::delivery::{MAX_AP_JSON_BYTES, SafeHttpClient};

/// Cache TTL for a probed host software name: ~7 days in millis.
pub const HOST_SOFTWARE_TTL_MS: i64 = 7 * 24 * 60 * 60 * 1000;

/// NodeInfo probe paths, newest schema first.
const NODEINFO_PATHS: [&str; 2] = ["/nodeinfo/2.1", "/nodeinfo/2.0"];

/// Maximum concurrent NodeInfo probes process-wide (Wave 4 liveness): a
/// mention flood across many distinct hosts must not open an unbounded
/// burst of outbound TLS connections from the delivery workers.
pub const NODEINFO_PROBE_MAX_IN_FLIGHT: usize = 8;

/// Maximum NodeInfo probe starts per minute process-wide (Wave 4
/// liveness): sustained distinct-host traffic degrades to the
/// emoji-aware default instead of probing forever.
pub const NODEINFO_PROBE_BUDGET_PER_MIN: usize = 60;

/// Budget window for [`NODEINFO_PROBE_BUDGET_PER_MIN`].
const NODEINFO_PROBE_WINDOW_MS: i64 = 60_000;

/// Singleflight slot for one in-progress host probe: followers block on
/// the condvar until the leader publishes the fetch result.
struct ProbeSlot {
    done: bool,
    result: Option<String>,
}

struct ProbeGateState {
    in_flight: HashMap<String, Arc<(Mutex<ProbeSlot>, Condvar)>>,
    window_start_ms: i64,
    window_used: usize,
}

/// Process-local singleflight + probe budget for NodeInfo fetches (Wave 4
/// liveness). Concurrent deliveries to one uncached host share a single
/// fetch; when the in-flight cap or the per-minute budget is exhausted the
/// probe is skipped (`None` — an uncached failure the caller treats as
/// emoji-aware). Failures are never cached (see `resolve_*` below), so a
/// skipped probe is retried on the next delivery, not stuck.
pub struct NodeinfoProbeGate {
    max_in_flight: usize,
    budget_per_min: usize,
    state: Mutex<ProbeGateState>,
}

impl NodeinfoProbeGate {
    /// Gate with explicit bounds (tests pass small budgets; production
    /// uses [`NODEINFO_PROBE_MAX_IN_FLIGHT`] /
    /// [`NODEINFO_PROBE_BUDGET_PER_MIN`] via the shared gate inside
    /// [`fetch_nodeinfo_software`]).
    pub fn new(max_in_flight: usize, budget_per_min: usize) -> Self {
        Self {
            max_in_flight: max_in_flight.max(1),
            budget_per_min,
            state: Mutex::new(ProbeGateState {
                in_flight: HashMap::new(),
                window_start_ms: 0,
                window_used: 0,
            }),
        }
    }

    /// Run `fetch` for `host` under the gate. `now_ms` is the caller's
    /// clock (tests pass synthetic times); the shared gate passes wall
    /// time. At most one `fetch` runs per host at a time and all waiters
    /// share its result.
    pub fn probe(
        &self,
        host: &str,
        now_ms: i64,
        fetch: impl FnOnce() -> Option<String>,
    ) -> Option<String> {
        let slot = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(existing) = state.in_flight.get(host) {
                let existing = existing.clone();
                drop(state);
                return Self::wait(&existing);
            }
            if now_ms.saturating_sub(state.window_start_ms) >= NODEINFO_PROBE_WINDOW_MS {
                state.window_start_ms = now_ms;
                state.window_used = 0;
            }
            if state.in_flight.len() >= self.max_in_flight
                || state.window_used >= self.budget_per_min
            {
                return None;
            }
            state.window_used = state.window_used.saturating_add(1);
            let slot = Arc::new((
                Mutex::new(ProbeSlot {
                    done: false,
                    result: None,
                }),
                Condvar::new(),
            ));
            state.in_flight.insert(host.to_string(), slot.clone());
            slot
        };
        let result = fetch();
        {
            let (lock, cvar) = &*slot;
            let mut guard = lock.lock().unwrap_or_else(|e| e.into_inner());
            guard.result = result.clone();
            guard.done = true;
            cvar.notify_all();
        }
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.in_flight.remove(host);
        }
        result
    }

    /// Block until the leader publishes the shared fetch result.
    fn wait(slot: &Arc<(Mutex<ProbeSlot>, Condvar)>) -> Option<String> {
        let (lock, cvar) = &**slot;
        let mut guard = lock.lock().unwrap_or_else(|e| e.into_inner());
        while !guard.done {
            guard = cvar.wait(guard).unwrap_or_else(|e| e.into_inner());
        }
        guard.result.clone()
    }
}

fn global_probe_gate() -> &'static NodeinfoProbeGate {
    static GATE: LazyLock<NodeinfoProbeGate> = LazyLock::new(|| {
        NodeinfoProbeGate::new(NODEINFO_PROBE_MAX_IN_FLIGHT, NODEINFO_PROBE_BUDGET_PER_MIN)
    });
    &GATE
}

fn wall_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}
/// Software names known to render Misskey-style emoji reactions
/// (`Like` + `_misskey_reaction`). Documented as known-good; classification
/// itself is deny-list based (only [`PLAIN_LIKE_SOFTWARE`] and the
/// Akkoma/Pleroma `EmojiReact` set divert) so unlisted software defaults to
/// emoji-aware.
pub const EMOJI_CAPABLE_SOFTWARE: &[&str] = &[
    "misskey", "sharkey", "foundkey", "calckey", "pinetree", "akkoma", "pleroma",
];

/// Lowercased software names whose `Like` handling has no emoji-reaction
/// support: recipients on these hosts get a plain Mastodon-favourite `Like`.
/// Anything else (including unknown/probe-failed hosts) is treated as
/// emoji-aware; Akkoma/Pleroma are NOT in this list — they get `EmojiReact`
/// (see [`crate::domain::fed_policy::EMOJI_REACT_SOFTWARE`]).
pub const PLAIN_LIKE_SOFTWARE: &[&str] = &[
    "mastodon",
    "hometown",
    "glitchsoc",
    "glitch",
    "pixelfed",
    "lemmy",
    "gotosocial",
];

/// Classify a NodeInfo `software.name` (case-insensitive): `None` (unknown
/// host or failed probe) → emoji-aware. Delegates to the domain pure core
/// [`reaction_style_for_software`]; both `MisskeyLike` and `EmojiReact`
/// carry emoji semantics, only `PlainLike` does not.
pub fn emoji_reaction_supported(software_name: Option<&str>) -> bool {
    !matches!(
        reaction_style_for_software(software_name),
        ReactionStyle::PlainLike
    )
}

/// Extract the lowercased `host[:port]`-style authority from an inbox (or any
/// HTTP(S)) URL for cache-keying and probing. Returns `None` when no usable
/// host remains.
pub fn host_of_url(url: &str) -> Option<String> {
    let without_scheme = match url.split_once("://") {
        Some((_, rest)) => rest,
        None => url,
    };
    let authority = without_scheme.split('/').next().unwrap_or("");
    // Strip a leading `user@`; keep `[:port]` (pinetree host semantics are
    // `host[:port]`, matching `webfinger.rs extract_host` — stripping the
    // port would collide `host:8443` with the default-origin `host` and
    // probe the wrong origin). Bracketed IPv6 keeps its colons.
    let host = authority.rsplit('@').next().unwrap_or("");
    let host = host.trim().trim_end_matches('.').to_ascii_lowercase();
    if host.is_empty() { None } else { Some(host) }
}

/// Parse a NodeInfo document's `software.name` (both 2.0 and 2.1 carry it at
/// `software.name`). `None` on missing/empty/non-string input.
pub fn parse_nodeinfo_software(doc: &Value) -> Option<String> {
    let name = doc.get("software")?.get("name")?.as_str()?.trim();
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

/// Fetch `software.name` for `host` via NodeInfo (2.1, then 2.0 fallback)
/// over the SSRF-protected client. Unsigned GET (public document; the actor
/// fetcher's signed GET is for authenticated actor documents). `None` on any
/// transport, status, or parse failure — the caller treats that as
/// emoji-aware and does NOT cache it.
///
/// Wave 4 liveness: concurrent probes for one host share a single fetch
/// (singleflight), and a process-wide in-flight cap plus per-minute budget
/// ([`NODEINFO_PROBE_MAX_IN_FLIGHT`], [`NODEINFO_PROBE_BUDGET_PER_MIN`])
/// shed excess probes as `None` instead of opening unbounded connections.
/// Probe failures stay uncached by design.
///
/// Trust note: the NodeInfo `software.name` string is self-asserted,
/// unsigned, and advisory only. It selects a rendering flavour for
/// outbound reactions below — it is never pinned, never trusted for
/// authentication or authorization, and an unknown/lying value only
/// changes which benign emoji field a remote receives.
pub fn fetch_nodeinfo_software(client: &SafeHttpClient, host: &str) -> Option<String> {
    let host = host.trim().trim_end_matches('.');
    if host.is_empty() {
        return None;
    }
    let host_owned = host.to_string();
    global_probe_gate().probe(&host_owned, wall_now_ms(), || {
        fetch_nodeinfo_software_uncached(client, &host_owned)
    })
}

/// The uncached NodeInfo probe: both schema versions over the
/// SSRF-protected client. Reached only through the singleflight + budget
/// gate in [`fetch_nodeinfo_software`].
fn fetch_nodeinfo_software_uncached(client: &SafeHttpClient, host: &str) -> Option<String> {
    for path in NODEINFO_PATHS {
        let url = format!("https://{host}{path}");
        let resp = client
            .send(
                "GET",
                &url,
                &[("Accept", "application/json")],
                None,
                MAX_AP_JSON_BYTES,
            )
            .ok()?;
        if !(200..300).contains(&resp.status) {
            continue;
        }
        let doc: Value = serde_json::from_slice(&resp.body).ok()?;
        if let Some(name) = parse_nodeinfo_software(&doc) {
            return Some(name);
        }
    }
    None
}

/// True when `entry` is still within [`HOST_SOFTWARE_TTL_MS`] of `now_ms`
/// (a future-dated row, from clock skew, counts as fresh).
pub fn is_host_software_fresh(entry: &HostSoftware, now_ms: i64) -> bool {
    now_ms.saturating_sub(entry.fetched_at_ms) <= HOST_SOFTWARE_TTL_MS
}

/// Resolve which reaction wire style `host` gets: fresh cache hit wins;
/// otherwise `fetch` (the NodeInfo probe, or a mock in tests) runs and its
/// success is cached. Store errors are best-effort (a failed read is a miss,
/// a failed write just skips caching). Unknown/failed → `MisskeyLike`.
/// The resolved name is advisory rendering input only (see
/// [`fetch_nodeinfo_software`); it never gates auth.
pub fn resolve_reaction_style(
    services: &AppServices,
    host: &str,
    now_ms: i64,
    fetch: &dyn Fn(&str) -> Option<String>,
) -> ReactionStyle {
    let key = host.trim().to_ascii_lowercase();
    if key.is_empty() {
        return ReactionStyle::MisskeyLike;
    }
    if let Ok(Some(cached)) = services.host_caps.get_host_software(&key) {
        if is_host_software_fresh(&cached, now_ms) {
            return reaction_style_for_software(cached.software.as_deref());
        }
    }
    match fetch(&key) {
        Some(name) => {
            let _ = services.host_caps.put_host_software(HostSoftware {
                host: key,
                software: Some(name.clone()),
                fetched_at_ms: now_ms,
            });
            reaction_style_for_software(Some(&name))
        }
        None => ReactionStyle::MisskeyLike,
    }
}

/// Resolve whether `host` gets emoji-reaction `Like`s: fresh cache hit wins;
/// otherwise `fetch` (the NodeInfo probe, or a mock in tests) runs and its
/// success is cached. Store errors are best-effort (a failed read is a miss,
/// a failed write just skips caching). Unknown/failed → emoji-aware. The
/// resolved name is advisory rendering input only (see
/// [`fetch_nodeinfo_software`); it never gates auth.
pub fn resolve_emoji_capable(
    services: &AppServices,
    host: &str,
    now_ms: i64,
    fetch: &dyn Fn(&str) -> Option<String>,
) -> bool {
    let key = host.trim().to_ascii_lowercase();
    if key.is_empty() {
        return true;
    }
    if let Ok(Some(cached)) = services.host_caps.get_host_software(&key) {
        if is_host_software_fresh(&cached, now_ms) {
            return emoji_reaction_supported(cached.software.as_deref());
        }
    }
    match fetch(&key) {
        Some(name) => {
            let _ = services.host_caps.put_host_software(HostSoftware {
                host: key,
                software: Some(name.clone()),
                fetched_at_ms: now_ms,
            });
            emoji_reaction_supported(Some(&name))
        }
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::ports::HostCapsStore;

    #[test]
    fn emoji_support_classification_matches_decided_split() {
        use crate::domain::fed_policy::ReactionStyle;
        // Unknown (unprobed/failed) defaults to emoji-aware.
        assert!(emoji_reaction_supported(None));
        assert_eq!(
            reaction_style_for_software(None),
            ReactionStyle::MisskeyLike
        );
        // Known Misskey-family names stay emoji-aware `Like`s.
        for name in ["misskey", "sharkey", "foundkey", "calckey", "pinetree"] {
            assert!(
                emoji_reaction_supported(Some(name)),
                "{name} must be emoji-aware"
            );
            assert!(
                EMOJI_CAPABLE_SOFTWARE.contains(&name),
                "{name} must be in the documented list"
            );
            assert_eq!(
                reaction_style_for_software(Some(name)),
                ReactionStyle::MisskeyLike
            );
        }
        // Case-insensitive.
        assert!(emoji_reaction_supported(Some("Misskey")));
        assert!(!emoji_reaction_supported(Some("MASTODON")));
        // Akkoma/Pleroma are emoji-aware via `EmojiReact`, never a bare Like.
        for name in ["pleroma", "akkoma"] {
            assert!(
                emoji_reaction_supported(Some(name)),
                "{name} must be emoji-aware"
            );
            assert!(
                EMOJI_CAPABLE_SOFTWARE.contains(&name),
                "{name} must be in the documented list"
            );
            assert_eq!(
                reaction_style_for_software(Some(name)),
                ReactionStyle::EmojiReact,
                "{name} must get EmojiReact"
            );
        }
        // Decided plain-Like set.
        for name in [
            "mastodon",
            "hometown",
            "glitchsoc",
            "glitch",
            "pixelfed",
            "lemmy",
            "gotosocial",
        ] {
            assert!(
                !emoji_reaction_supported(Some(name)),
                "{name} must get a plain Like"
            );
            assert_eq!(
                reaction_style_for_software(Some(name)),
                ReactionStyle::PlainLike
            );
        }
        // Unlisted software defaults to emoji-aware (benign extra field,
        // upstream Misskey always sends it).
        assert!(emoji_reaction_supported(Some("catodon")));
        assert!(emoji_reaction_supported(Some("firefish")));
    }

    #[test]
    fn nodeinfo_parse_reads_software_name() {
        let doc = serde_json::json!({
            "version": "2.1",
            "software": { "name": "mastodon", "version": "4.3.0" },
        });
        assert_eq!(parse_nodeinfo_software(&doc).as_deref(), Some("mastodon"));
        let missing = serde_json::json!({"version": "2.1"});
        assert_eq!(parse_nodeinfo_software(&missing), None);
        let empty = serde_json::json!({"software": {"name": "  "}});
        assert_eq!(parse_nodeinfo_software(&empty), None);
    }

    #[test]
    fn host_of_url_extracts_cache_key() {
        assert_eq!(
            host_of_url("https://mastodon.example/inbox").as_deref(),
            Some("mastodon.example")
        );
        assert_eq!(
            host_of_url("https://Mastodon.Example:8443/users/x/inbox").as_deref(),
            Some("mastodon.example:8443")
        );
        assert_eq!(
            host_of_url("https://example./inbox").as_deref(),
            Some("example")
        );
        assert_eq!(host_of_url(""), None);
        assert_eq!(host_of_url("https:///"), None);
    }

    #[test]
    fn freshness_boundary_is_seven_days() {
        let now = 1_760_400_000_000;
        let fresh = HostSoftware {
            host: "a.example".to_string(),
            software: Some("mastodon".to_string()),
            fetched_at_ms: now - HOST_SOFTWARE_TTL_MS,
        };
        assert!(is_host_software_fresh(&fresh, now));
        let stale = HostSoftware {
            host: "a.example".to_string(),
            software: Some("mastodon".to_string()),
            fetched_at_ms: now - HOST_SOFTWARE_TTL_MS - 1,
        };
        assert!(!is_host_software_fresh(&stale, now));
    }

    fn services() -> AppServices {
        crate::application::commands::tests::test_services("host-caps")
    }

    #[test]
    fn resolve_uses_fresh_cache_without_probing() {
        let services = services();
        let now = 1_760_400_000_000;
        services
            .host_caps
            .put_host_software(HostSoftware {
                host: "mastodon.example".to_string(),
                software: Some("mastodon".to_string()),
                fetched_at_ms: now,
            })
            .unwrap();
        // A probing fetch must never run on a fresh hit.
        let fetch = |_: &str| -> Option<String> { panic!("probe on fresh cache hit") };
        assert!(!resolve_emoji_capable(
            &services,
            "mastodon.example",
            now,
            &fetch
        ));
        assert!(!resolve_emoji_capable(
            &services,
            "MASTODON.EXAMPLE",
            now,
            &fetch
        ));
    }

    #[test]
    fn resolve_reprobes_stale_entries_and_recaches() {
        let services = services();
        let now = 1_760_400_000_000;
        services
            .host_caps
            .put_host_software(HostSoftware {
                host: "peer.example".to_string(),
                software: Some("misskey".to_string()),
                fetched_at_ms: now - HOST_SOFTWARE_TTL_MS - 1,
            })
            .unwrap();
        let probed = std::cell::RefCell::new(Vec::new());
        let fetch = |h: &str| -> Option<String> {
            probed.borrow_mut().push(h.to_string());
            Some("akkoma".to_string())
        };
        // WP5: Akkoma is emoji-aware (via `EmojiReact`), so the re-probed
        // value reads as emoji-capable. The test's subject — stale entries
        // are re-probed and recached — is unchanged.
        assert!(resolve_emoji_capable(
            &services,
            "peer.example",
            now,
            &fetch
        ));
        assert_eq!(*probed.borrow(), vec!["peer.example".to_string()]);
        // The fresh re-probed value now serves without probing.
        let stored = services
            .host_caps
            .get_host_software("peer.example")
            .unwrap()
            .unwrap();
        assert_eq!(stored.software.as_deref(), Some("akkoma"));
        assert_eq!(stored.fetched_at_ms, now);
    }

    #[test]
    fn resolve_probe_failure_is_emoji_aware_and_uncached() {
        let services = services();
        let now = 1_760_400_000_000;
        let fetch = |_: &str| -> Option<String> { None };
        assert!(resolve_emoji_capable(
            &services,
            "down.example",
            now,
            &fetch
        ));
        assert_eq!(
            services
                .host_caps
                .get_host_software("down.example")
                .unwrap(),
            None,
            "probe failures are never cached"
        );
    }

    #[test]
    fn resolve_caches_successful_probe() {
        let services = services();
        let now = 1_760_400_000_000;
        let fetch = |_: &str| -> Option<String> { Some("Sharkey".to_string()) };
        assert!(resolve_emoji_capable(&services, "new.example", now, &fetch));
        let stored = services
            .host_caps
            .get_host_software("new.example")
            .unwrap()
            .unwrap();
        assert_eq!(stored.software.as_deref(), Some("Sharkey"));
    }

    #[test]
    fn resolve_reaction_style_three_way() {
        use crate::domain::fed_policy::ReactionStyle;
        let services = services();
        let now = 1_760_400_000_000;
        // Akkoma probe → EmojiReact, cached for the second call (no probe).
        let fetch = |_: &str| -> Option<String> { Some("akkoma".to_string()) };
        assert_eq!(
            resolve_reaction_style(&services, "akk.example", now, &fetch),
            ReactionStyle::EmojiReact
        );
        let probe = |_: &str| -> Option<String> { panic!("probe on fresh cache hit") };
        assert_eq!(
            resolve_reaction_style(&services, "akk.example", now, &probe),
            ReactionStyle::EmojiReact
        );
        // Mastodon probe → PlainLike; unknown/failed → MisskeyLike.
        let fetch_masto = |_: &str| -> Option<String> { Some("Mastodon".to_string()) };
        assert_eq!(
            resolve_reaction_style(&services, "masto.example", now, &fetch_masto),
            ReactionStyle::PlainLike
        );
        let fetch_none = |_: &str| -> Option<String> { None };
        assert_eq!(
            resolve_reaction_style(&services, "down.example", now, &fetch_none),
            ReactionStyle::MisskeyLike
        );
    }
}
