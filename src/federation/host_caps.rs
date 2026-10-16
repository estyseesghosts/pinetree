//! Per-host ActivityPub software detection for the §6 outbound `Like`
//! emoji/plain split (`docs/misskey-parity-plan.md` §6).
//!
//! Outbound `Like` delivery decides per recipient host: emoji-aware servers
//! get the Misskey form (`Like` + `_misskey_reaction`), others get a plain
//! Mastodon-favourite `Like` (no `_misskey_reaction` field). Detection is a
//! lazy NodeInfo probe per host, fetched OFF-TRANSACTION in the delivery
//! worker (network can stall — never inside a state transaction) via the
//! existing SSRF-protected [`SafeHttpClient`], cached in the `host_software`
//! redb table (the `HostCapsStore` port trait) with a ~7-day TTL. Probe failures are never
//! cached; unknown/failed hosts are treated as emoji-aware (an extra field is
//! benign and matches upstream Misskey, which always sends it).
//!
//! Placement: this lives in the federation layer, not `domain`, because it is
//! remote-software interop policy (software-name lists, NodeInfo fetching,
//! cache orchestration) over [`AppServices`]. The only pure-string core —
//! [`emoji_reaction_supported`] — could live in `domain`, but splitting it
//! from the probe/cache logic it exclusively serves would scatter the slice;
//! `domain` stays free of remote-software knowledge.

use serde_json::Value;

use crate::application::commands::AppServices;
use crate::application::ports::HostSoftware;
use crate::federation::delivery::{SafeHttpClient, MAX_AP_JSON_BYTES};

/// Cache TTL for a probed host software name: ~7 days in millis.
pub const HOST_SOFTWARE_TTL_MS: i64 = 7 * 24 * 60 * 60 * 1000;

/// NodeInfo probe paths, newest schema first.
const NODEINFO_PATHS: [&str; 2] = ["/nodeinfo/2.1", "/nodeinfo/2.0"];

/// Software names known to render Misskey-style emoji reactions
/// (`Like` + `_misskey_reaction`). Documented as known-good; classification
/// itself is deny-list based (see [`PLAIN_LIKE_SOFTWARE`]) so unlisted
/// software defaults to emoji-aware.
pub const EMOJI_CAPABLE_SOFTWARE: &[&str] =
    &["misskey", "sharkey", "foundkey", "calckey", "pinetree"];

/// Lowercased software names whose `Like` handling has no emoji-reaction
/// support: recipients on these hosts get a plain Mastodon-favourite `Like`.
/// Anything else (including unknown/probe-failed hosts) is treated as
/// emoji-aware.
pub const PLAIN_LIKE_SOFTWARE: &[&str] = &[
    "mastodon",
    "hometown",
    "glitchsoc",
    "glitch",
    "pixelfed",
    "lemmy",
    "pleroma",
    "akkoma",
    "gotosocial",
];

/// Classify a NodeInfo `software.name` (case-insensitive): `None` (unknown
/// host or failed probe) → emoji-aware.
pub fn emoji_reaction_supported(software_name: Option<&str>) -> bool {
    match software_name {
        None => true,
        Some(name) => {
            let lower = name.trim().to_ascii_lowercase();
            !PLAIN_LIKE_SOFTWARE.contains(&lower.as_str())
        }
    }
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
    if host.is_empty() {
        None
    } else {
        Some(host)
    }
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
pub fn fetch_nodeinfo_software(client: &SafeHttpClient, host: &str) -> Option<String> {
    let host = host.trim().trim_end_matches('.');
    if host.is_empty() {
        return None;
    }
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

/// Resolve whether `host` gets emoji-reaction `Like`s: fresh cache hit wins;
/// otherwise `fetch` (the NodeInfo probe, or a mock in tests) runs and its
/// success is cached. Store errors are best-effort (a failed read is a miss,
/// a failed write just skips caching). Unknown/failed → emoji-aware.
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
        // Unknown (unprobed/failed) defaults to emoji-aware.
        assert!(emoji_reaction_supported(None));
        // Known emoji-aware names.
        for name in ["misskey", "sharkey", "foundkey", "calckey", "pinetree"] {
            assert!(
                emoji_reaction_supported(Some(name)),
                "{name} must be emoji-aware"
            );
            assert!(
                EMOJI_CAPABLE_SOFTWARE.contains(&name),
                "{name} must be in the documented list"
            );
        }
        // Case-insensitive.
        assert!(emoji_reaction_supported(Some("Misskey")));
        assert!(!emoji_reaction_supported(Some("MASTODON")));
        // Decided plain-Like set.
        for name in [
            "mastodon",
            "hometown",
            "glitchsoc",
            "glitch",
            "pixelfed",
            "lemmy",
            "pleroma",
            "akkoma",
            "gotosocial",
        ] {
            assert!(
                !emoji_reaction_supported(Some(name)),
                "{name} must get a plain Like"
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
        assert_eq!(host_of_url("https://example./inbox").as_deref(), Some("example"));
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
        assert!(!resolve_emoji_capable(&services, "mastodon.example", now, &fetch));
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
        assert!(!resolve_emoji_capable(&services, "peer.example", now, &fetch));
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
        assert!(resolve_emoji_capable(&services, "down.example", now, &fetch));
        assert_eq!(
            services.host_caps.get_host_software("down.example").unwrap(),
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
}
