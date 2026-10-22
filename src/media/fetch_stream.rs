//! Stream-to-disk fetch staging (`docs/remote-media-cache-plan.md` §3.5/§5).
//!
//! The transport lives in [`crate::media::remote_cache`] (which owns
//! validation, SSRF checks and redirect policy); this module owns the byte
//! path: stream a response reader straight to a temp file under a byte cap,
//! hashing incrementally (SHA-256 + MD5) so the body is never held fully in
//! memory. The staged file is then handed to
//! [`crate::media::storage::FsBlobStore::import_file`] via
//! [`finalize_import`], which moves it into the pool without re-reading the
//! content.
//!
//! Cap semantics: the cap applies to bytes actually read (absent or
//! dishonest `Content-Length` cannot smuggle extra bytes; chunked bodies
//! just read until EOF under the same cap). The moment the cap would be
//! exceeded the partial file is deleted and the caller gets
//! `Validation("MAX_FILE_SIZE_EXCEEDED")`. An empty body is
//! `Validation("remote media body is empty")`.

use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use md5::{Digest, Md5};
use sha2::Sha256;

use crate::application::ports::PortError;
use crate::domain::media::sniff_mime;
use crate::media::storage::FsBlobStore;

/// MIME sniffing reads at most this prefix before falling back to the full
/// staged file. 64 KiB covers every magic table entry (`ftyp` brands at
/// offset 8..12, EBML, RIFF) with headroom.
const SNIFF_PREFIX_LEN: usize = 64 * 1024;

/// Copy buffer: small enough to enforce the cap promptly, large enough to
/// keep syscall overhead down.
const COPY_BUF_LEN: usize = 8192;

static FETCH_TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// Outcome of [`stream_response_to_store`]: digests plus the sniffed MIME.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamFetchResult {
    /// Exact staged length (what the caller charges against quotas).
    pub bytes_written: u64,
    /// Lowercase SHA-256 hex: the `BlobStore` content address.
    pub sha256: String,
    /// Lowercase MD5 hex: the client-visible `MediaMeta.hash`.
    pub md5: String,
    /// Sniffed content type (sniffer only, never a declared type).
    pub mime: String,
}

/// Stream `reader` to a temp file inside `store_tmp_dir`, enforcing
/// `max_bytes` on bytes actually read. Returns the staged path plus
/// digests/MIME. The caller owns the staged file (move it into the pool
/// with [`finalize_import`], or delete it on error paths of its own).
pub fn stream_response_to_store<R: Read>(
    mut reader: R,
    max_bytes: u64,
    store_tmp_dir: &Path,
) -> Result<(PathBuf, StreamFetchResult), PortError> {
    fs::create_dir_all(store_tmp_dir)
        .map_err(|e| PortError::StorageUnavailable(format!("create fetch staging dir: {e}")))?;
    let seq = FETCH_TMP_SEQ.fetch_add(1, Ordering::SeqCst);
    let tmp_path = store_tmp_dir.join(format!("fetch.{}.{seq}.tmp", std::process::id()));
    let mut file = fs::File::create(&tmp_path)
        .map_err(|e| PortError::StorageUnavailable(format!("create staged fetch file: {e}")))?;
    let mut sha = Sha256::new();
    let mut md5 = Md5::new();
    let mut bytes_written: u64 = 0;
    let mut buf = [0u8; COPY_BUF_LEN];
    loop {
        let n = reader.read(&mut buf).map_err(|e| {
            let _ = fs::remove_file(&tmp_path);
            PortError::StorageUnavailable(format!("read fetch body: {e}"))
        })?;
        if n == 0 {
            break;
        }
        if bytes_written + n as u64 > max_bytes {
            drop(file);
            let _ = fs::remove_file(&tmp_path);
            return Err(PortError::Validation("MAX_FILE_SIZE_EXCEEDED".to_string()));
        }
        file.write_all(&buf[..n]).map_err(|e| {
            let _ = fs::remove_file(&tmp_path);
            PortError::StorageUnavailable(format!("write staged fetch file: {e}"))
        })?;
        sha.update(&buf[..n]);
        md5.update(&buf[..n]);
        bytes_written += n as u64;
    }
    file.sync_all().map_err(|e| {
        let _ = fs::remove_file(&tmp_path);
        PortError::StorageUnavailable(format!("sync staged fetch file: {e}"))
    })?;
    drop(file);
    if bytes_written == 0 {
        let _ = fs::remove_file(&tmp_path);
        return Err(PortError::Validation(
            "remote media body is empty".to_string(),
        ));
    }
    let mime = sniff_staged_file(&tmp_path)?;
    Ok((
        tmp_path,
        StreamFetchResult {
            bytes_written,
            sha256: hex::encode(sha.finalize()),
            md5: hex::encode(md5.finalize()),
            mime,
        },
    ))
}

/// Sniff the MIME of a staged file: head prefix first, full-file fallback
/// when the head is inconclusive (`text/plain` over a truncated UTF-8
/// prefix, or `application/octet-stream` that longer magic could refine).
/// The full read is bounded: staged files never exceed their fetch cap.
fn sniff_staged_file(tmp: &Path) -> Result<String, PortError> {
    let file_len = fs::metadata(tmp)
        .map(|m| m.len())
        .map_err(|e| PortError::StorageUnavailable(format!("stat staged fetch file: {e}")))?;
    let head_len = (file_len.min(SNIFF_PREFIX_LEN as u64)) as usize;
    let mut head = vec![0u8; head_len];
    {
        let mut f = fs::File::open(tmp)
            .map_err(|e| PortError::StorageUnavailable(format!("open staged fetch file: {e}")))?;
        f.read_exact(&mut head)
            .map_err(|e| PortError::StorageUnavailable(format!("read staged fetch file: {e}")))?;
    }
    let head_mime = sniff_mime(&head);
    if head_len as u64 == file_len
        || (head_mime != "text/plain" && head_mime != "application/octet-stream")
    {
        return Ok(head_mime);
    }
    let full = fs::read(tmp)
        .map_err(|e| PortError::StorageUnavailable(format!("read staged fetch file: {e}")))?;
    Ok(sniff_mime(&full))
}

/// Move a staged file from [`stream_response_to_store`] into `store` under
/// `blob_key` without re-reading the content. Returns the stored length.
/// The staged file is consumed (renamed or removed) on success.
pub fn finalize_import(store: &FsBlobStore, blob_key: &str, tmp: &Path) -> Result<u64, PortError> {
    let len = fs::metadata(tmp)
        .map(|m| m.len())
        .map_err(|e| PortError::StorageUnavailable(format!("stat staged blob file: {e}")))?;
    store.import_file(blob_key, tmp)?;
    Ok(len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::ports::BlobStore;
    use crate::media::ingest::{md5_hex, sha256_hex};

    /// A reader that dribbles `data` out in `chunk`-sized pieces with no
    /// known length up front (models chunked HTTP bodies).
    struct ChunkReader {
        data: Vec<u8>,
        pos: usize,
        chunk: usize,
    }

    impl ChunkReader {
        fn new(data: Vec<u8>, chunk: usize) -> Self {
            Self {
                data,
                pos: 0,
                chunk,
            }
        }
    }

    impl Read for ChunkReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.pos >= self.data.len() {
                return Ok(0);
            }
            let n = self.chunk.min(self.data.len() - self.pos).min(buf.len());
            buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
            self.pos += n;
            Ok(n)
        }
    }

    fn staging_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pinetree-fetch-test-{tag}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn stream_small_body_hashes_and_sniffs() {
        let dir = staging_dir("small");
        let payload = vec![
            0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 1, 2, 3, 4, 5, 6, 7, 8,
        ];
        let (tmp, res) =
            stream_response_to_store(ChunkReader::new(payload.clone(), 3), 4096, &dir).unwrap();
        assert_eq!(res.bytes_written, payload.len() as u64);
        assert_eq!(res.sha256, sha256_hex(&payload));
        assert_eq!(res.md5, md5_hex(&payload));
        assert_eq!(res.mime, "image/png");
        assert_eq!(std::fs::read(&tmp).unwrap(), payload);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn stream_cap_exceeded_deletes_partial() {
        let dir = staging_dir("cap");
        let payload = vec![0x89u8; 8192];
        let before: Vec<PathBuf> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        let err = stream_response_to_store(ChunkReader::new(payload, 7), 4096, &dir).unwrap_err();
        assert!(matches!(err, PortError::Validation(msg) if msg == "MAX_FILE_SIZE_EXCEEDED"));
        // No partial staged file may linger.
        let after: Vec<PathBuf> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert_eq!(before, after);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn stream_cap_is_exact_boundary() {
        // Exactly `max_bytes` succeeds; one byte more fails.
        let dir = staging_dir("boundary");
        let payload = vec![b'a'; 100];
        let (tmp, res) =
            stream_response_to_store(ChunkReader::new(payload.clone(), 30), 100, &dir).unwrap();
        assert_eq!(res.bytes_written, 100);
        assert_eq!(res.mime, "text/plain");
        let _ = std::fs::remove_file(&tmp);
        let err =
            stream_response_to_store(ChunkReader::new(vec![b'a'; 101], 30), 100, &dir).unwrap_err();
        assert!(matches!(err, PortError::Validation(msg) if msg == "MAX_FILE_SIZE_EXCEEDED"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn stream_empty_body_errors_and_cleans_up() {
        let dir = staging_dir("empty");
        let err = stream_response_to_store(ChunkReader::new(vec![], 8), 4096, &dir).unwrap_err();
        assert!(matches!(err, PortError::Validation(msg) if msg == "remote media body is empty"));
        let entries: Vec<_> = std::fs::read_dir(&dir).unwrap().collect();
        assert!(entries.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn full_file_fallback_refines_octet_prefix() {
        // A file whose first 64 KiB is valid UTF-8 text but whose tail is
        // binary sniffs as text on the head and octet on the whole.
        let dir = staging_dir("fallback");
        let mut payload = vec![b'x'; SNIFF_PREFIX_LEN + 16];
        payload.extend_from_slice(&[0xFF, 0xFE, 0x00, 0x01]);
        assert_eq!(sniff_mime(&payload[..SNIFF_PREFIX_LEN]), "text/plain");
        let (tmp, res) =
            stream_response_to_store(ChunkReader::new(payload.clone(), 70000), 1 << 20, &dir)
                .unwrap();
        assert_eq!(res.mime, "application/octet-stream");
        assert_eq!(std::fs::read(&tmp).unwrap(), payload);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn finalize_import_moves_staged_into_pool() {
        let dir = staging_dir("finalize");
        let pool = FsBlobStore::new(dir.join("pool")).unwrap();
        let payload = b"finalized staged bytes".to_vec();
        let (tmp, res) =
            stream_response_to_store(ChunkReader::new(payload.clone(), 5), 4096, &dir).unwrap();
        let len = finalize_import(&pool, &res.sha256, &tmp).unwrap();
        assert_eq!(len, payload.len() as u64);
        assert!(!tmp.exists(), "staged file must be consumed");
        assert_eq!(
            pool.get(&res.sha256).unwrap().as_deref(),
            Some(&payload[..])
        );
        assert_eq!(pool.byte_len(&res.sha256).unwrap(), Some(len));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
