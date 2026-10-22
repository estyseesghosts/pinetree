//! Remote clip resolution: full-state pull with TTL, the `updated`
//! revalidation fast path, the embedded-document ingest path, and the
//! bounded process-memory membership cache.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::application::commands::AppServices;
use crate::application::ports::PortError;

use super::clips::CLIP_COLLECTION_PAGE_SIZE;

/// TTL for cached remote-clip membership (`resolve_remote_clip`): refetch
/// at most this often; `Add`/`Remove` deliveries converge faster in
/// between.
pub const REMOTE_CLIP_TTL: Duration = Duration::from_secs(10 * 60);

/// One cached remote-clip membership pull.
#[derive(Debug, Clone)]
struct RemoteClipEntry {
    fetched_at: Instant,
    note_uris: Vec<String>,
    /// Last-seen ActivityStreams `updated` stamp of the collection doc
    /// (Phase 2a: revalidation compares it before full pulls).
    updated: Option<String>,
}

fn remote_clip_cache() -> &'static Mutex<HashMap<String, RemoteClipEntry>> {
    static CACHE: OnceLock<Mutex<HashMap<String, RemoteClipEntry>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn remote_clip_cache_insert(uri: &str, note_uris: Vec<String>, updated: Option<String>) {
    let Ok(mut cache) = remote_clip_cache().lock() else {
        return;
    };
    if cache.len() >= 100 {
        // Evict the oldest entry (bounded process-memory cache, same
        // discipline as the `url-preview` cache).
        if let Some(oldest) = cache
            .iter()
            .min_by_key(|(_, e)| e.fetched_at)
            .map(|(k, _)| k.clone())
        {
            cache.remove(&oldest);
        }
    }
    cache.insert(
        uri.to_string(),
        RemoteClipEntry {
            fetched_at: Instant::now(),
            note_uris,
            updated,
        },
    );
}

/// Resolve a remote clip collection to its member note URIs (full-state
/// pull). Fresh cache entries (younger than [`REMOTE_CLIP_TTL`]) serve
/// without network I/O; `Add`/`Remove` deliveries converge faster in
/// between. Unsigned fetch (listed collections are public); unlisted
/// collections resolve by capability URI; `RemoteApproved` collections
/// need a signed fetch the caller performs itself.
pub fn resolve_remote_clip(
    services: &AppServices,
    clip_uri: &str,
) -> Result<Vec<String>, PortError> {
    resolve_remote_clip_with_ttl(services, clip_uri, REMOTE_CLIP_TTL)
}

/// [`resolve_remote_clip`] with an explicit TTL (the integration seam:
/// unit tests pin revalidation behavior deterministically).
pub fn resolve_remote_clip_with_ttl(
    services: &AppServices,
    clip_uri: &str,
    ttl: Duration,
) -> Result<Vec<String>, PortError> {
    if let Ok(cache) = remote_clip_cache().lock() {
        if let Some(entry) = cache.get(clip_uri) {
            if entry.fetched_at.elapsed() < ttl {
                return Ok(entry.note_uris.clone());
            }
        }
    }
    let client = &services.federation_http;
    let fetch_doc = |url: &str| -> Result<Value, PortError> {
        let resp = client
            .send(
                "GET",
                url,
                &[("Accept", "application/activity+json")],
                None,
                65_536,
            )
            .map_err(|e| PortError::StorageUnavailable(format!("clip fetch failed: {e}")))?;
        if resp.status != 200 {
            return Err(PortError::StorageUnavailable(format!(
                "clip fetch status {}",
                resp.status
            )));
        }
        serde_json::from_slice(&resp.body)
            .map_err(|e| PortError::StorageUnavailable(format!("clip doc invalid: {e}")))
    };
    let doc = fetch_doc(clip_uri)?;
    // `updated` fast-path (Phase 2a): when the served collection carries
    // an `updated` stamp equal to the cached stamp, reuse the cached
    // membership without walking `first` — the doc fetch itself is the
    // revalidation, the item pull is what we skip.
    if let Some(remote_updated) = doc.get("updated").and_then(Value::as_str) {
        let hit = remote_clip_cache()
            .lock()
            .ok()
            .and_then(|c| c.get(clip_uri).cloned());
        if let Some(entry) = hit {
            if entry.updated.as_deref() == Some(remote_updated) {
                remote_clip_cache_insert(clip_uri, entry.note_uris.clone(), entry.updated);
                if let Ok(cache) = remote_clip_cache().lock() {
                    if let Some(e) = cache.get(clip_uri) {
                        return Ok(e.note_uris.clone());
                    }
                }
            }
        }
    }
    let served_updated = doc
        .get("updated")
        .and_then(Value::as_str)
        .map(str::to_string);
    let mut uris = Vec::new();
    // Full-state: inline `orderedItems` or one `first` page hop (the
    // serving side always fits a full collection on one page).
    let mut pages = vec![doc];
    for _ in 0..2 {
        let Some(current) = pages.pop() else {
            break;
        };
        if let Some(items) = current.get("orderedItems").and_then(Value::as_array) {
            for item in items {
                if let Some(uri) = item.as_str() {
                    if uris.len() < CLIP_COLLECTION_PAGE_SIZE && !uris.contains(&uri.to_string()) {
                        uris.push(uri.to_string());
                    }
                }
            }
            break;
        }
        if let Some(first) = current.get("first").and_then(Value::as_str) {
            pages.push(fetch_doc(first)?);
        } else {
            break;
        }
    }
    remote_clip_cache_insert(clip_uri, uris.clone(), served_updated);
    Ok(uris)
}

/// Ingest an embedded `OrderedCollection` document into the membership
/// cache (Phase 2a `Update(Collection)` fast path: one round trip, no
/// refetch). Extracts string `orderedItems` (capped) + the `updated`
/// stamp. Idempotent — replays converge.
pub fn ingest_remote_clip_doc(clip_uri: &str, doc: &Value) {
    let mut uris = Vec::new();
    if let Some(items) = doc.get("orderedItems").and_then(Value::as_array) {
        for item in items {
            if let Some(uri) = item.as_str() {
                if uris.len() < CLIP_COLLECTION_PAGE_SIZE && !uris.contains(&uri.to_string()) {
                    uris.push(uri.to_string());
                }
            }
        }
    }
    let updated = doc
        .get("updated")
        .and_then(Value::as_str)
        .map(str::to_string);
    remote_clip_cache_insert(clip_uri, uris, updated);
}

/// Test hook: forget one cached remote-clip entry.
#[cfg(test)]
pub fn forget_remote_clip_cache(uri: &str) {
    if let Ok(mut cache) = remote_clip_cache().lock() {
        cache.remove(uri);
    }
}

/// Introspection for tests: URIs currently held in the remote-clip
/// cache (process-memory, bounded — same discipline as the `url-preview`
/// cache).
pub fn remote_clip_cached_uris() -> Vec<String> {
    remote_clip_cache()
        .lock()
        .map(|cache| cache.keys().cloned().collect())
        .unwrap_or_default()
}

/// Introspection for tests: cached membership URIs for one collection
/// (`None` = not cached — never fetched or evicted).
pub fn remote_clip_cached_for(uri: &str) -> Option<Vec<String>> {
    remote_clip_cache()
        .lock()
        .ok()?
        .get(uri)
        .map(|e| e.note_uris.clone())
}

/// Evict one cached remote-clip entry (inbound `Add`/`Remove` promptness;
/// idempotent — evicting an absent entry is a no-op).
pub fn evict_remote_clip_cache(uri: &str) {
    if let Ok(mut cache) = remote_clip_cache().lock() {
        cache.remove(uri);
    }
}

/// Fetch a remote collection document (signed GET as the system actor)
/// and report its `attributedTo` owner. `None` when the collection is
/// unreachable, gated for us, or malformed — callers treat that as
/// "cannot verify ownership" and ignore the activity.
pub fn fetch_collection_owner(services: &AppServices, clip_uri: &str) -> Option<String> {
    let client = &services.federation_http;
    let doc = crate::federation::delivery::signed_get_json(
        services,
        client,
        clip_uri,
        "application/activity+json",
    )
    .ok()?;
    doc.get("attributedTo")
        .and_then(Value::as_str)
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_services() -> AppServices {
        crate::application::commands::tests::test_services("fed-collections")
    }

    /// Full-state pull + TTL revalidation against a loopback collection
    /// server (hit-counted): fresh entries serve without network I/O, a
    /// zero TTL always revalidates, and eviction forces a re-pull.
    /// (Phase 2 gate, deterministic half — the live half is the
    /// two-pinetree test in `tests/federation.rs`.)
    #[test]
    fn remote_clip_pull_caches_and_revalidates() {
        use std::io::{Read, Write};
        use std::sync::atomic::{AtomicUsize, Ordering};

        static HITS: AtomicUsize = AtomicUsize::new(0);
        HITS.store(0, Ordering::SeqCst);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let addr = listener.local_addr().expect("loopback addr");
        let body = serde_json::json!({
            "@context": ["https://www.w3.org/ns/activitystreams"],
            "id": format!("http://{addr}/clips/1"),
            "type": "OrderedCollection",
            "totalItems": 2,
            "orderedItems": [
                format!("http://{addr}/notes/1"),
                format!("http://{addr}/notes/2"),
            ],
        });
        let payload = serde_json::to_vec(&body).unwrap();
        std::thread::spawn(move || {
            for _ in 0..16 {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(5)));
                let mut buf = [0u8; 4096];
                let n = stream.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                if req.starts_with("GET /clips/1") {
                    HITS.fetch_add(1, Ordering::SeqCst);
                    let hdr = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/activity+json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        payload.len()
                    );
                    let _ = stream.write_all(hdr.as_bytes());
                    let _ = stream.write_all(&payload);
                } else {
                    let _ = stream.write_all(
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 4\r\nConnection: close\r\n\r\nnope",
                    );
                }
            }
        });

        let services = test_services();
        let uri = format!("http://{addr}/clips/1");
        forget_remote_clip_cache(&uri);
        // Pull populates the cache (one hit).
        let first =
            resolve_remote_clip_with_ttl(&services, &uri, Duration::from_secs(3600)).unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(HITS.load(Ordering::SeqCst), 1);
        // Fresh entry: no second hit.
        let second =
            resolve_remote_clip_with_ttl(&services, &uri, Duration::from_secs(3600)).unwrap();
        assert_eq!(second, first);
        assert_eq!(HITS.load(Ordering::SeqCst), 1);
        // Zero TTL: always revalidates (second hit).
        let third = resolve_remote_clip_with_ttl(&services, &uri, Duration::ZERO).unwrap();
        assert_eq!(third, first);
        assert_eq!(HITS.load(Ordering::SeqCst), 2);
        // Eviction forces a re-pull (third hit).
        evict_remote_clip_cache(&uri);
        let fourth =
            resolve_remote_clip_with_ttl(&services, &uri, Duration::from_secs(3600)).unwrap();
        assert_eq!(fourth, first);
        assert_eq!(HITS.load(Ordering::SeqCst), 3);
        // Errors never populate the cache.
        let missing = format!("http://{addr}/clips/missing");
        assert!(resolve_remote_clip_with_ttl(&services, &missing, Duration::ZERO).is_err());
        assert!(!remote_clip_cached_uris().contains(&missing));
        forget_remote_clip_cache(&uri);
    }
}
