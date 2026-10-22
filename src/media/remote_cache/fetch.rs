use super::evict::ensure_cache_budget;
use crate::activitypub::objects::ParsedAttachment;
use crate::application::commands::AppServices;
use crate::application::ports::{InsertMediaInput, MediaClass, PortError, RemoteCacheRecord};
use crate::config::RemoteCacheMode;
use crate::domain::media::{MediaMeta, MediaProperties, sniff_mime};
use crate::federation::delivery::{SafeHttpClient, parse_http_url, resolve_and_validate_url};
use crate::media::fetch_stream::stream_response_to_store;
use crate::media::ingest::{md5_hex, sanitize_filename, sha256_hex};
use crate::media::storage::{acquire_blob_key, acquire_media_op};

/// Variant key for the original bytes (what the `GET /files/:id` handler
/// asks for and what the fetch job caches).
pub const DEFAULT_VARIANT: &str = "original";

/// Variant key for avatar/emoji proxies (callers pass this with
/// `force_class = Some(MediaClass::Avatar)`).
pub const AVATAR_VARIANT: &str = "avatar";

/// Validate a remote media URL before any network fetch occurs.
pub fn validate_remote_media_url(url: &str, client: &SafeHttpClient) -> Result<(), PortError> {
    let parsed = parse_http_url(url).map_err(PortError::Validation)?;
    if parsed.scheme != "http" && parsed.scheme != "https" {
        return Err(PortError::Validation(format!(
            "unsupported remote media scheme: {}",
            parsed.scheme
        )));
    }
    if url.chars().count() > 1024 {
        return Err(PortError::Validation(
            "remote media URL exceeds 1024 chars".to_string(),
        ));
    }
    resolve_and_validate_url(url, &client.config)
        .map_err(|e| PortError::Validation(e.to_string()))?;
    Ok(())
}

/// Fetch-on-first-request (plan §4.4): the `GET /files/:id` handler calls
/// this when a remote row's bytes are absent from the cache pool. Fresh
/// index hit + bytes present → touch + serve, no network. Hit with evicted
/// (or otherwise missing) bytes, or a miss → re-fetch under the global
/// reservation, stream to disk, import into the cache pool, upsert metadata
/// + index, and account. Fail-closed: `Disabled` mode and an unreservable
/// budget both surface as `Validation` (the caller serves its fallback).
pub fn fetch_on_first_request(
    services: &AppServices,
    source_url: &str,
    variant: &str,
) -> Result<MediaMeta, PortError> {
    fetch_and_index_remote_media(
        services,
        &services.federation_http,
        source_url,
        variant,
        None,
        None,
        None,
        false,
        None,
        None,
        None,
    )
}

/// Fetch-on-first-request with an explicit class override (avatar/emoji
/// proxies: `variant = AVATAR_VARIANT`, `force_class =
/// Some(MediaClass::Avatar)`). Same semantics as
/// [`fetch_on_first_request`]; the override only decides which class cap
/// and TTL govern the row — the bytes still sniff their own MIME.
pub fn fetch_on_first_request_as(
    services: &AppServices,
    source_url: &str,
    variant: &str,
    force_class: Option<MediaClass>,
) -> Result<MediaMeta, PortError> {
    fetch_and_index_remote_media(
        services,
        &services.federation_http,
        source_url,
        variant,
        force_class,
        None,
        None,
        false,
        None,
        None,
        None,
    )
}

/// Shared fetch → cache-pool → index core behind [`fetch_on_first_request`],
/// [`fetch_on_first_request_as`], the fetch job, and
/// [`fetch_and_cache_remote_media`]. Never touches the durable pool.
#[allow(clippy::too_many_arguments)]
pub(crate) fn fetch_and_index_remote_media(
    services: &AppServices,
    client: &SafeHttpClient,
    source_url: &str,
    variant: &str,
    force_class: Option<MediaClass>,
    name_hint: Option<&str>,
    comment: Option<String>,
    is_sensitive: bool,
    blurhash: Option<String>,
    width: Option<u32>,
    height: Option<u32>,
) -> Result<MediaMeta, PortError> {
    // Fail closed when the cache is disabled.
    if services.remote_cache_mode == RemoteCacheMode::Disabled {
        return Err(PortError::Validation("REMOTE_CACHE_DISABLED".to_string()));
    }
    // Freshness lookup: hit + bytes present → touch + hit++ + serve.
    // Hit with evicted (or otherwise missing) bytes, or metadata lost
    // meanwhile → fall through and re-fetch (miss path).
    if let Some(rec) = services.media.remote_cache_lookup(source_url, variant)? {
        if services.cache_blobs.byte_len(&rec.blob_key)?.is_some() {
            services
                .media
                .remote_cache_touch(source_url, variant, services.clock.now_ms())?;
            services
                .media
                .cache_accounting_apply(0, rec.class, 0, 1, 0, 0, 0, 0)?;
            if let Some(meta) = services.media.get(&rec.media_id)? {
                return Ok(meta);
            }
        }
    }
    // Miss path: validate before holding any reservation (cheap skip).
    validate_remote_media_url(source_url, client)?;

    // In-flight reservation under the global cap. The true size is unknown
    // before the fetch, so reserve `max_file_size` first and true-up after
    // the real size is known (release `max - actual`, account `actual`).
    // Fail closed when the budget cannot be reserved — even after
    // `ensure_cache_budget` evicted everything it could.
    let max_bytes = services.max_file_size;
    let provisional = force_class.unwrap_or(MediaClass::Preview);
    ensure_cache_budget(services, max_bytes, provisional)?;
    if !services.media.reserve_cache_bytes(max_bytes)? {
        services
            .media
            .cache_accounting_apply(0, provisional, 0, 0, 1, 0, 0, 0)?;
        return Err(PortError::Validation("CACHE_CAPACITY_EXCEEDED".to_string()));
    }

    let attachment = ParsedAttachment {
        url: source_url.to_string(),
        media_type: None,
        name: name_hint.map(ToOwned::to_owned),
        is_sensitive,
        blurhash,
        width,
        height,
    };
    let prepared = match download_and_sniff(services, client, &attachment) {
        Ok(prepared) => prepared,
        Err(e) => {
            // Post-reservation fetch failure = failed origin fetch.
            let _ = services.media.release_cache_reservation(max_bytes);
            let _ = services
                .media
                .cache_accounting_apply(0, provisional, 0, 0, 0, 0, 0, 1);
            return Err(e);
        }
    };
    let actual = prepared.bytes.len() as u64;

    // Stage to disk under the cap (no locks held: pure fs + hashing of the
    // already-fetched bytes). The coordinated commit below is the only
    // section that touches shared pool/accounting state, and no redb
    // transaction ever spans the network fetch above.
    // (`SafeHttpClient` still buffers the body; the disk stage + cap
    // re-enforcement + `import_file` are real, the end-to-end streaming
    // transport is a follow-up.) The staging directory is a process-temp
    // fallback: a pool-owned staging dir needs a `BlobStore` tmp-dir
    // accessor the trait does not expose yet.
    let (tmp_path, staged) = match (|| -> Result<
        (
            std::path::PathBuf,
            crate::media::fetch_stream::StreamFetchResult,
        ),
        PortError,
    > {
        let tmp_dir = std::env::temp_dir().join("pinetree-remote-cache");
        let (tmp_path, staged) =
            stream_response_to_store(&prepared.bytes[..], max_bytes, &tmp_dir)?;
        if staged.sha256 != prepared.blob_key || staged.bytes_written != actual {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(PortError::StorageUnavailable(
                "staged remote-media bytes failed hash check".to_string(),
            ));
        }
        Ok((tmp_path, staged))
    })() {
        Ok(staged) => staged,
        Err(e) => {
            let _ = services.media.release_cache_reservation(max_bytes);
            return Err(e);
        }
    };
    let class = force_class.unwrap_or_else(|| MediaClass::classify_mime(&staged.mime));

    // Coordinated commit under the shared lifecycle gate + per-blob-key
    // lock: the stage above ran unlocked, so a concurrent fetch of the same
    // URL may have completed meanwhile — recheck the index and reuse the
    // winner instead of double-installing and double-accounting. The inner
    // `import_file` re-acquire nests safely (counted no-op). Bytes always
    // land in the cache pool (`cache_blobs`), never the durable pool.
    let _lifecycle = acquire_media_op("fetch_commit")?;
    let _key_lock = acquire_blob_key(&prepared.blob_key)?;
    if let Some(rec) = services.media.remote_cache_lookup(source_url, variant)?
        && services.cache_blobs.byte_len(&rec.blob_key)?.is_some()
    {
        let _ = std::fs::remove_file(&tmp_path);
        services
            .media
            .remote_cache_touch(source_url, variant, services.clock.now_ms())?;
        services
            .media
            .cache_accounting_apply(0, rec.class, 0, 1, 0, 0, 0, 0)?;
        if let Some(meta) = services.media.get(&rec.media_id)? {
            let _ = services.media.release_cache_reservation(max_bytes);
            return Ok(meta);
        }
    }

    // Metadata + index + accounting. These are separate store calls (no
    // cross-table transaction on the trait): a crash between them heals
    // via the reconciler (dangling meta without index re-fetches on next
    // lookup miss; production backends should co-locate them).
    let stored = (|| -> Result<MediaMeta, PortError> {
        // `BlobStore::import_file` dispatches to the filesystem rename
        // path on `FsBlobStore`; the staged file is removed either way
        // (post-rename the remove is a harmless miss).
        let out = services
            .cache_blobs
            .import_file(&prepared.blob_key, &tmp_path);
        let _ = std::fs::remove_file(&tmp_path);
        out?;
        let media_id = services.ids.generate();
        let local_url = format!(
            "{}/files/{media_id}",
            services.public_url.trim_end_matches('/')
        );
        let meta = services.media.insert_metadata(InsertMediaInput {
            id: Some(media_id.clone()),
            owner_id: None,
            name: prepared.name.clone(),
            mime: staged.mime.clone(),
            size_bytes: actual,
            hash: staged.md5.clone(),
            blob_key: prepared.blob_key.clone(),
            blurhash: prepared.blurhash.clone(),
            properties: prepared.properties.clone(),
            is_sensitive: prepared.is_sensitive,
            is_link: true,
            url: local_url,
            thumbnail_url: None,
            comment: comment.or(prepared.comment.clone()),
            source_url: Some(source_url.to_string()),
            max_file_size: services.max_file_size,
        })?;
        let now_ms = services.clock.now_ms();
        services.media.remote_cache_insert(&RemoteCacheRecord {
            source_url: source_url.to_string(),
            variant: variant.to_string(),
            media_id: media_id.clone(),
            blob_key: prepared.blob_key.clone(),
            size_bytes: actual,
            class,
            fetched_at_ms: now_ms,
            last_access_ms: now_ms,
            access_count: 1,
        })?;
        services.media.cache_accounting_apply(
            actual as i64,
            class,
            actual as i64,
            0,
            1,
            actual,
            0,
            0,
        )?;
        Ok(meta)
    })();
    // True-up the reservation on success; release it whole on failure
    // (storage fault — the caller retries and the lookup converges).
    match stored {
        Ok(meta) => {
            let _ = services
                .media
                .release_cache_reservation(max_bytes.saturating_sub(actual));
            Ok(meta)
        }
        Err(e) => {
            let _ = services.media.release_cache_reservation(max_bytes);
            Err(e)
        }
    }
}

/// Fetched bytes plus the sniffed metadata, with no store writes yet.
struct PreparedRemoteMedia {
    bytes: Vec<u8>,
    mime: String,
    name: String,
    comment: Option<String>,
    /// MD5: stays the client-visible `MediaMeta.hash` (packer `md5`).
    hash: String,
    /// SHA-256 hex: the `BlobStore` content address (slice 8).
    blob_key: String,
    blurhash: Option<String>,
    properties: MediaProperties,
    is_sensitive: bool,
}

/// Validate, fetch, and sniff a remote attachment. The stored MIME comes
/// from the shared sniffer ONLY: the declared `mediaType` and the response
/// `Content-Type` are deliberately ignored (slice 7, H2), so neither can
/// introduce `text/html`, `image/svg+xml` or any other active type.
fn download_and_sniff(
    services: &AppServices,
    client: &SafeHttpClient,
    attachment: &ParsedAttachment,
) -> Result<PreparedRemoteMedia, PortError> {
    validate_remote_media_url(&attachment.url, client)?;

    let max_bytes = usize::try_from(services.max_file_size).unwrap_or(16 * 1024 * 1024);
    let resp = client
        .send("GET", &attachment.url, &[], None, max_bytes)
        .map_err(|e| PortError::Validation(e.to_string()))?;
    if !(200..300).contains(&resp.status) {
        return Err(PortError::Validation(format!(
            "remote media GET {} returned HTTP {}",
            attachment.url, resp.status
        )));
    }
    if resp.body.is_empty() {
        return Err(PortError::Validation(
            "remote media body is empty".to_string(),
        ));
    }
    if resp.body.len() as u64 > services.max_file_size {
        return Err(PortError::Validation("MAX_FILE_SIZE_EXCEEDED".to_string()));
    }

    let mime = sniff_mime(&resp.body);
    let ext = match mime.as_str() {
        "image/png" => "png",
        "image/jpeg" => "jpg",
        "image/gif" => "gif",
        "image/webp" => "webp",
        "image/avif" => "avif",
        "video/mp4" => "mp4",
        "video/webm" => "webm",
        "video/mpeg" => "mpg",
        "audio/mpeg" => "mp3",
        "audio/ogg" => "ogg",
        "audio/wav" => "wav",
        "text/plain" => "txt",
        _ => "bin",
    };

    let url_leaf = parse_http_url(&attachment.url).ok().and_then(|u| {
        u.path_and_query
            .split('?')
            .next()
            .and_then(|p| p.rsplit('/').next())
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned)
    });
    let name = sanitize_filename(attachment.name.as_deref().or(url_leaf.as_deref()), ext);
    let comment = attachment
        .name
        .as_deref()
        .map(|c| c.chars().take(512).collect());

    let hash = md5_hex(&resp.body);
    let blob_key = sha256_hex(&resp.body);
    Ok(PreparedRemoteMedia {
        bytes: resp.body,
        mime,
        name,
        comment,
        hash,
        blob_key,
        blurhash: attachment.blurhash.clone(),
        properties: MediaProperties {
            width: attachment.width,
            height: attachment.height,
            orientation: None,
            avg_color: None,
        },
        is_sensitive: attachment.is_sensitive,
    })
}

/// Validate, fetch, and cache a remote attachment into the cache pool +
/// `MediaStore` (standalone, without attaching to a note).
///
/// The raw bytes are staged to disk and imported into `cache_blobs` (never
/// the durable pool); `MediaStore` only stores `MediaMeta` plus the
/// `(source_url, "original")` index row. Prefer [`fetch_on_first_request`]
/// for serving; this stays for callers that need the row eagerly (and for
/// the tests below).
pub fn fetch_and_cache_remote_media(
    services: &AppServices,
    client: &SafeHttpClient,
    attachment: &ParsedAttachment,
) -> Result<MediaMeta, PortError> {
    fetch_and_index_remote_media(
        services,
        client,
        &attachment.url,
        DEFAULT_VARIANT,
        None,
        attachment.name.as_deref(),
        attachment
            .name
            .as_deref()
            .map(|c| c.chars().take(512).collect()),
        attachment.is_sensitive,
        attachment.blurhash.clone(),
        attachment.width,
        attachment.height,
    )
}

#[cfg(test)]
mod tests {
    use super::super::support::{attachment_for, serve_bodies, test_services};
    use super::*;
    use crate::activitypub::objects::ParsedAttachment;
    use crate::application::ports::PortError;
    use crate::config::RemoteCacheMode;
    use crate::federation::delivery::{SafeHttpClient, SafeHttpConfig};
    use crate::media::ingest::{md5_hex, sha256_hex};
    use std::io::{Read, Write};
    use std::net::TcpListener;

    #[test]
    fn fetch_on_first_request_disabled_fails_closed() {
        // Mode gate fires before any store or network I/O, so this holds
        // regardless of backend quota/index implementations.
        let mut services = test_services("disabled-mode", 4096);
        services.remote_cache_mode = RemoteCacheMode::Disabled;
        let err = fetch_on_first_request(&services, "http://remote.invalid/a.png", "original")
            .expect_err("disabled cache must fail closed");
        assert!(
            matches!(err, PortError::Validation(ref m) if m == "REMOTE_CACHE_DISABLED"),
            "unexpected: {err:?}"
        );
    }

    #[test]
    fn remote_media_ssrf_and_fetch_cache_roundtrip() {
        let services = test_services("cache", 4096);
        let strict_client = SafeHttpClient::new(SafeHttpConfig {
            allow_private_ips: false,
            ..Default::default()
        });
        // Private IP rejected before any network fetch.
        let bad_att = ParsedAttachment {
            url: "http://127.0.0.1:9999/secret.png".to_string(),
            media_type: Some("image/png".to_string()),
            name: Some("secret.png".to_string()),
            is_sensitive: false,
            blurhash: None,
            width: None,
            height: None,
        };
        assert!(fetch_and_cache_remote_media(&services, &strict_client, &bad_att).is_err());

        // Start a local test HTTP server serving a valid PNG and an oversized payload.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let png_bytes = vec![
            0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 1, 2, 3, 4, 5, 6, 7, 8,
        ];
        let png_clone = png_bytes.clone();
        let handle = std::thread::spawn(move || {
            for _ in 0..3 {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                let mut buf = [0u8; 1024];
                let n = stream.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                if req.starts_with("GET /pic.png") {
                    let hdr = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\n\r\n",
                        png_clone.len()
                    );
                    let _ = stream.write_all(hdr.as_bytes());
                    let _ = stream.write_all(&png_clone);
                } else {
                    let big = vec![0x89; 8192];
                    let hdr = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\n\r\n",
                        big.len()
                    );
                    let _ = stream.write_all(hdr.as_bytes());
                    let _ = stream.write_all(&big);
                }
            }
        });

        let local_client = SafeHttpClient::new(SafeHttpConfig {
            allow_private_ips: true,
            ..Default::default()
        });
        let good_att = ParsedAttachment {
            url: format!("http://{addr}/pic.png"),
            media_type: Some("image/png".to_string()),
            name: Some("Alt text".to_string()),
            is_sensitive: true,
            blurhash: None,
            width: Some(64),
            height: Some(64),
        };
        let meta = fetch_and_cache_remote_media(&services, &local_client, &good_att).unwrap();
        assert_eq!(meta.owner_id, None);
        assert!(meta.is_link);
        assert!(meta.is_sensitive);
        assert_eq!(meta.mime, "image/png");
        assert_eq!(meta.size_bytes, png_bytes.len() as u64);
        // Slice 8 (D6, written before the fix): the blob lives under the
        // SHA-256 key while the meta keeps the md5; both are the real
        // digests of the fetched bytes. Remote bytes live in the cache
        // pool, never the durable pool.
        assert_eq!(meta.hash, md5_hex(&png_bytes));
        assert_eq!(meta.blob_key, sha256_hex(&png_bytes));
        assert_ne!(meta.blob_key, meta.hash);
        assert_eq!(
            services.cache_blobs.get(&meta.blob_key).unwrap().unwrap(),
            png_bytes
        );
        assert_eq!(services.blobs.get(&meta.blob_key).unwrap(), None);

        // On-demand second request for the same URL hits the index: no
        // second fetch (the server only served 3 connections; oversize
        // fetch below would be the 3rd) and the same row comes back.
        let again =
            fetch_on_first_request(&services, &format!("http://{addr}/pic.png"), "original")
                .unwrap();
        assert_eq!(again.id, meta.id);

        let big_att = ParsedAttachment {
            url: format!("http://{addr}/big.bin"),
            media_type: None,
            name: None,
            is_sensitive: false,
            blurhash: None,
            width: None,
            height: None,
        };
        assert!(fetch_and_cache_remote_media(&services, &local_client, &big_att).is_err());
        drop(handle);
    }

    #[test]
    fn remote_html_and_svg_attachments_stored_as_octet_stream() {
        // Declared `mediaType` (and the response header) must never win:
        // HTML served as `text/html` and declared `image/png` still stores
        // as octet-stream; an SVG served/declared as svg still does.
        let services = test_services("slice7-html-svg", 4096);
        let html = b"<!DOCTYPE html><html><body><script>alert(1)</script></body></html>".to_vec();
        let svg =
            b"<svg xmlns=\"http://www.w3.org/2000/svg\"><script>alert(1)</script></svg>".to_vec();
        let addr = serve_bodies(vec![
            ("/evil.html".to_string(), "text/html".to_string(), html),
            ("/pic.svg".to_string(), "image/svg+xml".to_string(), svg),
        ]);
        let local_client = SafeHttpClient::new(SafeHttpConfig {
            allow_private_ips: true,
            ..Default::default()
        });

        let html_att = attachment_for(&format!("http://{addr}/evil.html"), Some("image/png"));
        let meta = fetch_and_cache_remote_media(&services, &local_client, &html_att).unwrap();
        assert_eq!(meta.mime, "application/octet-stream");
        assert_ne!(meta.mime, "text/html");

        let svg_att = attachment_for(&format!("http://{addr}/pic.svg"), Some("image/svg+xml"));
        let meta = fetch_and_cache_remote_media(&services, &local_client, &svg_att).unwrap();
        assert_eq!(meta.mime, "application/octet-stream");
        assert_ne!(meta.mime, "image/svg+xml");
    }
}
