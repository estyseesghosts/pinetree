//! Remote media URL validation, downloading, and caching (Phase 2 Slice C2).
//!
//! Validates remote attachment URLs (scheme `http`/`https`, SSRF check,
//! size limit, MIME sniffing) before fetching/caching; stores metadata in
//! `MediaStore` (`owner_id = None`, `is_link = true`) and raw bytes in
//! `BlobStore` outside the database.

use crate::activitypub::objects::ParsedAttachment;
use crate::application::commands::AppServices;
use crate::application::ports::{InsertMediaInput, PortError};
use crate::domain::media::{MediaMeta, MediaProperties};
use crate::federation::delivery::{SafeHttpClient, parse_http_url, resolve_and_validate_url};
use crate::media::ingest::{md5_hex, sanitize_filename, sniff_mime};

/// Validate a remote media URL before any network fetch occurs.
pub fn validate_remote_media_url(
    url: &str,
    client: &SafeHttpClient,
) -> Result<(), PortError> {
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
    resolve_and_validate_url(url, &client.config).map_err(PortError::Validation)?;
    Ok(())
}

/// Validate, fetch, and cache a remote attachment into `BlobStore` + `MediaStore`.
///
/// The raw bytes are written to `BlobStore` (filesystem) and never stored inside
/// the database transaction (`MediaStore` only stores `MediaMeta`).
pub fn fetch_and_cache_remote_media(
    services: &AppServices,
    client: &SafeHttpClient,
    attachment: &ParsedAttachment,
) -> Result<MediaMeta, PortError> {
    validate_remote_media_url(&attachment.url, client)?;

    let max_bytes = usize::try_from(services.max_file_size).unwrap_or(16 * 1024 * 1024);
    let resp = client
        .send("GET", &attachment.url, &[], None, max_bytes)
        .map_err(PortError::Validation)?;
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
        return Err(PortError::Validation(
            "MAX_FILE_SIZE_EXCEEDED".to_string(),
        ));
    }

    let header_mime = resp.header("content-type").map(ToOwned::to_owned);
    let fallback_mime = attachment
        .media_type
        .as_deref()
        .or(header_mime.as_deref());
    let mime = sniff_mime(&resp.body, fallback_mime);
    let ext = match mime.as_str() {
        "image/png" => "png",
        "image/jpeg" => "jpg",
        "image/gif" => "gif",
        "image/webp" => "webp",
        "video/mp4" => "mp4",
        "application/pdf" => "pdf",
        _ => "bin",
    };

    let url_leaf = parse_http_url(&attachment.url)
        .ok()
        .and_then(|u| {
            u.path_and_query
                .split('?')
                .next()
                .and_then(|p| p.rsplit('/').next())
                .filter(|s| !s.is_empty())
                .map(ToOwned::to_owned)
        });
    let name = sanitize_filename(
        attachment.name.as_deref().or(url_leaf.as_deref()),
        ext,
    );
    let comment = attachment
        .name
        .as_deref()
        .map(|c| c.chars().take(512).collect());

    let hash = md5_hex(&resp.body);
    services.blobs.put(&hash, &resp.body)?;

    let media_id = services.ids.generate();
    let local_url = format!(
        "{}/files/{media_id}",
        services.public_url.trim_end_matches('/')
    );
    services.media.insert_metadata(InsertMediaInput {
        id: Some(media_id),
        owner_id: None,
        name,
        mime,
        size_bytes: resp.body.len() as u64,
        hash,
        blurhash: attachment.blurhash.clone(),
        properties: MediaProperties {
            width: attachment.width,
            height: attachment.height,
            orientation: None,
            avg_color: None,
        },
        is_sensitive: attachment.is_sensitive,
        is_link: true,
        url: local_url,
        thumbnail_url: None,
        comment,
        max_file_size: services.max_file_size,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::ports::{ActivityDedup, NoopStreamingSink};
    use crate::domain::id::{AidxGenerator, Clock, IdGenerator, ManualClock};
    use crate::federation::delivery::SafeHttpConfig;
    use crate::media::storage::FsBlobStore;
    use crate::persistence::redb::RedbStore;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn test_services(prefix: &str, max_file_size: u64) -> AppServices {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-media-cache-{}-{prefix}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_760_400_000_000));
        let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
        let store = RedbStore::open(dir.join("test.redb"), clock.clone(), ids.clone()).unwrap();
        let blobs = Arc::new(FsBlobStore::new(dir.join("blobs")).unwrap());
        AppServices {
            clock,
            ids,
            accounts: store.clone(),
            notes: store.clone(),
            follows: store.clone(),
            reactions: store.clone(),
            jobs: store.clone(),
            media: store.clone(),
            blobs,
            notifications: store.clone(),
            auth: store.clone(),
            streaming: Arc::new(NoopStreamingSink),
            dedup: store.clone() as Arc<dyn ActivityDedup>,
            federation_http: SafeHttpClient::new(SafeHttpConfig {
                allow_private_ips: true,
                ..Default::default()
            }),
            public_url: "https://pinetree.local".to_string(),
            max_file_size,
            preserved_usernames: Vec::new(),
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
        }
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
            for _ in 0..2 {
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
        assert_eq!(services.blobs.get(&meta.hash).unwrap().unwrap(), png_bytes);

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
}
