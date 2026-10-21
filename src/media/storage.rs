use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::application::ports::{BlobStore, PortError};

#[derive(Debug)]
pub struct FsBlobStore {
    root: PathBuf,
    temp_seq: AtomicU64,
}

impl FsBlobStore {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, PortError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("objects"))
            .map_err(|e| PortError::StorageUnavailable(format!("create blob objects dir: {e}")))?;
        fs::create_dir_all(root.join("tmp"))
            .map_err(|e| PortError::StorageUnavailable(format!("create blob tmp dir: {e}")))?;
        Ok(Self {
            root,
            temp_seq: AtomicU64::new(0),
        })
    }

    fn validate_hash(hash: &str) -> Result<(), PortError> {
        if hash.len() < 4
            || hash.len() > 128
            || !hash.bytes().all(|b| b.is_ascii_alphanumeric())
        {
            return Err(PortError::Validation(format!("invalid blob hash: {hash}")));
        }
        Ok(())
    }

    pub fn object_path(&self, hash: &str) -> Result<PathBuf, PortError> {
        Self::validate_hash(hash)?;
        let prefix = &hash[0..2];
        Ok(self.root.join("objects").join(prefix).join(hash))
    }

    pub fn temp_path(&self, hash: &str) -> Result<PathBuf, PortError> {
        Self::validate_hash(hash)?;
        let seq = self.temp_seq.fetch_add(1, Ordering::SeqCst);
        let pid = std::process::id();
        Ok(self.root.join("tmp").join(format!("{hash}.{pid}.{seq}.tmp")))
    }

    /// Length sidecar for an object: `{object_path}.len`, holding the
    /// decimal byte length (Phase 2.6 slice 8). Written via tmp+rename
    /// alongside the bytes; `get` treats a missing/mismatched sidecar as
    /// corrupt (`Ok(None)`) so a torn write can never serve short bytes.
    /// The `BlobStore` trait is unchanged — the sidecar is internal.
    fn sidecar_path(&self, hash: &str) -> Result<PathBuf, PortError> {
        let path = self.object_path(hash)?;
        let mut name = path
            .file_name()
            .map(|s| s.to_os_string())
            .ok_or_else(|| PortError::Validation(format!("invalid blob hash: {hash}")))?;
        name.push(".len");
        let mut sidecar = path;
        sidecar.set_file_name(name);
        Ok(sidecar)
    }

    fn write_atomic(&self, hash: &str, final_path: &Path, bytes: &[u8]) -> Result<(), PortError> {
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                PortError::StorageUnavailable(format!("create blob shard dir: {e}"))
            })?;
        }
        let tmp_path = self.temp_path(hash)?;
        {
            let mut file = fs::File::create(&tmp_path).map_err(|e| {
                PortError::StorageUnavailable(format!("create temp blob file: {e}"))
            })?;
            file.write_all(bytes).map_err(|e| {
                let _ = fs::remove_file(&tmp_path);
                PortError::StorageUnavailable(format!("write temp blob file: {e}"))
            })?;
            file.sync_all().map_err(|e| {
                let _ = fs::remove_file(&tmp_path);
                PortError::StorageUnavailable(format!("sync temp blob file: {e}"))
            })?;
        }
        fs::rename(&tmp_path, final_path).map_err(|e| {
            let _ = fs::remove_file(&tmp_path);
            PortError::StorageUnavailable(format!("rename temp blob to final path: {e}"))
        })?;
        Ok(())
    }

    /// Object-file presence without reading any content. `true` means a
    /// file exists at the content-addressed path; use [`Self::byte_len`]
    /// when the length sidecar must also agree (torn-write detection).
    pub fn exists(&self, hash: &str) -> Result<bool, PortError> {
        let final_path = self.object_path(hash)?;
        match fs::metadata(&final_path) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(PortError::StorageUnavailable(format!(
                "stat blob file: {e}"
            ))),
        }
    }

    /// Byte length without reading content: the object-file size must agree
    /// with the `.len` sidecar, otherwise the row is treated as corrupt
    /// (`Ok(None)`, same rule as `get`). `Ok(None)` also covers absent
    /// objects and pre-sidecar blobs.
    pub fn byte_len(&self, hash: &str) -> Result<Option<u64>, PortError> {
        let final_path = self.object_path(hash)?;
        let sidecar = self.sidecar_path(hash)?;
        let expected: u64 = match fs::read_to_string(&sidecar) {
            Ok(s) => match s.trim().parse() {
                Ok(n) => n,
                Err(_) => return Ok(None),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(PortError::StorageUnavailable(format!(
                    "read blob sidecar: {e}"
                )))
            }
        };
        match fs::metadata(&final_path) {
            Ok(m) => {
                let len = m.len();
                if len == expected {
                    Ok(Some(len))
                } else {
                    Ok(None)
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(PortError::StorageUnavailable(format!(
                "stat blob file: {e}"
            ))),
        }
    }

    /// Efficient server-side import of a staged temp file (stream-to-disk
    /// target): rename-or-copy into the content-addressed path, verify the
    /// length, then write the `.len` sidecar. Mirrors the `put` skip: when
    /// identical bytes are already stored the staged file is just dropped.
    /// The staged file is consumed (renamed or removed) on success.
    pub fn import_file(&self, hash: &str, src: &Path) -> Result<(), PortError> {
        let src_len = fs::metadata(src).map(|m| m.len()).map_err(|e| {
            PortError::StorageUnavailable(format!("stat staged blob file: {e}"))
        })?;
        let final_path = self.object_path(hash)?;
        let sidecar = self.sidecar_path(hash)?;
        // SHA-256 keys make the skip safe (same key ⇒ same bytes): skip
        // only when the object length and the sidecar both record exactly
        // this length, otherwise (re)write so a torn object heals.
        let object_len_ok = fs::metadata(&final_path).map(|m| m.len()).ok() == Some(src_len);
        let sidecar_ok = fs::read_to_string(&sidecar)
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            == Some(src_len);
        if object_len_ok && sidecar_ok {
            let _ = fs::remove_file(src);
            return Ok(());
        }
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                PortError::StorageUnavailable(format!("create blob shard dir: {e}"))
            })?;
        }
        if fs::rename(src, &final_path).is_err() {
            // Cross-device move (or Windows rename-onto-existing): fall
            // back to copy, removing a stale target first.
            if final_path.exists() {
                fs::remove_file(&final_path).map_err(|e| {
                    PortError::StorageUnavailable(format!("replace stale blob file: {e}"))
                })?;
            }
            if fs::rename(src, &final_path).is_err() {
                fs::copy(src, &final_path).map_err(|e| {
                    PortError::StorageUnavailable(format!("copy staged blob file: {e}"))
                })?;
                let _ = fs::remove_file(src);
            }
        }
        let actual = fs::metadata(&final_path).map(|m| m.len()).map_err(|e| {
            PortError::StorageUnavailable(format!("stat imported blob file: {e}"))
        })?;
        if actual != src_len {
            return Err(PortError::StorageUnavailable(format!(
                "blob import length mismatch: staged {src_len}, stored {actual}"
            )));
        }
        self.write_atomic(hash, &sidecar, src_len.to_string().as_bytes())?;
        Ok(())
    }
}

impl BlobStore for FsBlobStore {
    fn put(&self, hash: &str, bytes: &[u8]) -> Result<(), PortError> {
        let final_path = self.object_path(hash)?;
        let sidecar = self.sidecar_path(hash)?;
        // SHA-256 keys make the skip safe (same key ⇒ same bytes): skip only
        // when the object length (via stat, no content read) and the sidecar
        // both record exactly this length, otherwise (re)write both files so
        // a torn object or a missing/stale sidecar heals instead of lingering.
        let object_len_ok = fs::metadata(&final_path).map(|m| m.len()).ok()
            == Some(bytes.len() as u64);
        let sidecar_ok = fs::read_to_string(&sidecar)
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            == Some(bytes.len() as u64);
        if object_len_ok && sidecar_ok {
            return Ok(());
        }
        self.write_atomic(hash, &final_path, bytes)?;
        self.write_atomic(hash, &sidecar, bytes.len().to_string().as_bytes())?;
        Ok(())
    }

    fn get(&self, hash: &str) -> Result<Option<Vec<u8>>, PortError> {
        let final_path = self.object_path(hash)?;
        let sidecar = self.sidecar_path(hash)?;
        let expected: u64 = match fs::read_to_string(&sidecar) {
            // Missing or unparsable sidecar (torn `put`, or a pre-slice-8
            // MD5-keyed blob) is corruption: `None`, never raw bytes.
            // Callers already map `None` to 500/404.
            Ok(s) => match s.trim().parse() {
                Ok(n) => n,
                Err(_) => return Ok(None),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(PortError::StorageUnavailable(format!(
                    "read blob sidecar: {e}"
                )))
            }
        };
        match fs::read(&final_path) {
            Ok(bytes) => {
                if bytes.len() as u64 != expected {
                    return Ok(None);
                }
                Ok(Some(bytes))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(PortError::StorageUnavailable(format!(
                "read blob file: {e}"
            ))),
        }
    }

    fn delete(&self, hash: &str) -> Result<(), PortError> {
        let final_path = self.object_path(hash)?;
        match fs::remove_file(&final_path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => {
                return Err(PortError::StorageUnavailable(format!(
                    "delete blob file: {e}"
                )))
            }
        }?;
        let sidecar = self.sidecar_path(hash)?;
        match fs::remove_file(&sidecar) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(PortError::StorageUnavailable(format!(
                "delete blob sidecar: {e}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_store_put_get_delete_and_partial_write_invisible() {
        let dir = std::env::temp_dir().join(format!("pinetree-blob-test-{}", uuid::Uuid::new_v4()));
        let store = FsBlobStore::new(&dir).unwrap();
        let hash = "d41d8cd98f00b204e9800998ecf8427e";
        let payload = b"hello content-addressed world";

        assert_eq!(store.get(hash).unwrap(), None);

        // Simulate an un-renamed partial temp write in tmp/
        let orphan_tmp = store.temp_path(hash).unwrap();
        std::fs::write(&orphan_tmp, b"partial-corrupt").unwrap();
        assert_eq!(
            store.get(hash).unwrap(),
            None,
            "partial temp write must never be visible via get()"
        );

        store.put(hash, payload).unwrap();
        assert_eq!(store.get(hash).unwrap().as_deref(), Some(&payload[..]));

        // Idempotent second put
        store.put(hash, payload).unwrap();
        assert_eq!(store.get(hash).unwrap().as_deref(), Some(&payload[..]));

        store.delete(hash).unwrap();
        assert_eq!(store.get(hash).unwrap(), None);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn blob_store_get_verifies_length_sidecar() {
        // Slice 8 (D6, written before the fix): `get` must never serve
        // short bytes from a torn write. A length mismatch — or a
        // missing/corrupt sidecar — reads as `None`, never as truncated
        // content (callers map `None` to 500/404; the bytes are re-fetched
        // or re-uploaded).
        let dir = std::env::temp_dir().join(format!("pinetree-blob-test-{}", uuid::Uuid::new_v4()));
        let store = FsBlobStore::new(&dir).unwrap();
        let hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let payload = b"length-checked blob bytes";

        store.put(hash, payload).unwrap();
        assert_eq!(store.get(hash).unwrap().as_deref(), Some(&payload[..]));

        // Truncate the object file in place: must not serve short bytes.
        let object = store.object_path(hash).unwrap();
        std::fs::write(&object, &payload[..4]).unwrap();
        assert_eq!(
            store.get(hash).unwrap(),
            None,
            "truncated blob must read as corrupt, not short"
        );

        // A fresh put heals the torn write.
        store.put(hash, payload).unwrap();
        assert_eq!(store.get(hash).unwrap().as_deref(), Some(&payload[..]));

        // A corrupt sidecar is corruption too, not a fallback to raw bytes.
        let sidecar = store.sidecar_path(hash).unwrap();
        std::fs::write(&sidecar, b"not-a-number").unwrap();
        assert_eq!(store.get(hash).unwrap(), None);

        // A missing sidecar (e.g. a pre-slice-8 blob) is also `None`.
        std::fs::remove_file(&sidecar).unwrap();
        assert_eq!(store.get(hash).unwrap(), None);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn blob_store_rejects_traversal_hashes() {
        let dir = std::env::temp_dir().join(format!("pinetree-blob-test-{}", uuid::Uuid::new_v4()));
        let store = FsBlobStore::new(&dir).unwrap();
        assert!(store.put("../etc/passwd", b"x").is_err());
        assert!(store.put("ab", b"x").is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn test_store(tag: &str) -> (FsBlobStore, PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "pinetree-blob-test-{tag}-{}",
            uuid::Uuid::new_v4()
        ));
        let store = FsBlobStore::new(&dir).unwrap();
        (store, dir)
    }

    #[test]
    fn blob_store_exists_and_byte_len_without_reading() {
        let (store, dir) = test_store("exists-len");
        let hash = "d41d8cd98f00b204e9800998ecf8427e";
        assert_eq!(store.exists(hash).unwrap(), false);
        assert_eq!(store.byte_len(hash).unwrap(), None);

        let payload = b"length-checked blob bytes";
        store.put(hash, payload).unwrap();
        assert_eq!(store.exists(hash).unwrap(), true);
        assert_eq!(store.byte_len(hash).unwrap(), Some(payload.len() as u64));

        // Torn object: length mismatch reads as corrupt, never short.
        let object = store.object_path(hash).unwrap();
        std::fs::write(&object, &payload[..4]).unwrap();
        assert_eq!(store.byte_len(hash).unwrap(), None);

        // Missing sidecar (pre-sidecar blob) is also `None`, but the file
        // still exists.
        store.put(hash, payload).unwrap();
        std::fs::remove_file(store.sidecar_path(hash).unwrap()).unwrap();
        assert_eq!(store.byte_len(hash).unwrap(), None);
        assert_eq!(store.exists(hash).unwrap(), true);

        // Invalid hashes are rejected, not probed on disk.
        assert!(store.exists("../etc/passwd").is_err());
        assert!(store.byte_len("ab").is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn blob_store_import_file_moves_staged_bytes() {
        let (store, dir) = test_store("import");
        let hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let payload = b"staged stream-to-disk bytes";

        let staged = dir.join("staged.tmp");
        std::fs::write(&staged, payload).unwrap();
        store.import_file(hash, &staged).unwrap();
        // Staged file consumed (same-device rename), bytes addressable.
        assert!(!staged.exists());
        assert_eq!(store.get(hash).unwrap().as_deref(), Some(&payload[..]));
        assert_eq!(store.byte_len(hash).unwrap(), Some(payload.len() as u64));

        // Idempotent second import of identical bytes: staged file dropped,
        // stored bytes untouched.
        std::fs::write(&staged, payload).unwrap();
        store.import_file(hash, &staged).unwrap();
        assert!(!staged.exists());
        assert_eq!(store.get(hash).unwrap().as_deref(), Some(&payload[..]));

        // Import heals a torn object instead of leaving short bytes.
        let object = store.object_path(hash).unwrap();
        std::fs::write(&object, &payload[..4]).unwrap();
        assert_eq!(store.get(hash).unwrap(), None);
        std::fs::write(&staged, payload).unwrap();
        store.import_file(hash, &staged).unwrap();
        assert_eq!(store.get(hash).unwrap().as_deref(), Some(&payload[..]));

        // Missing staged file and bad hashes surface errors.
        assert!(store.import_file(hash, &dir.join("nope.tmp")).is_err());
        std::fs::write(&staged, payload).unwrap();
        assert!(store.import_file("../etc/passwd", &staged).is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
