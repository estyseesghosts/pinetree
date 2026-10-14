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
}

impl BlobStore for FsBlobStore {
    fn put(&self, hash: &str, bytes: &[u8]) -> Result<(), PortError> {
        let final_path = self.object_path(hash)?;
        if final_path.exists() {
            return Ok(());
        }
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
        fs::rename(&tmp_path, &final_path).map_err(|e| {
            let _ = fs::remove_file(&tmp_path);
            PortError::StorageUnavailable(format!("rename temp blob to final path: {e}"))
        })?;
        Ok(())
    }

    fn get(&self, hash: &str) -> Result<Option<Vec<u8>>, PortError> {
        let final_path = self.object_path(hash)?;
        match fs::read(&final_path) {
            Ok(bytes) => Ok(Some(bytes)),
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
            Err(e) => Err(PortError::StorageUnavailable(format!(
                "delete blob file: {e}"
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
    fn blob_store_rejects_traversal_hashes() {
        let dir = std::env::temp_dir().join(format!("pinetree-blob-test-{}", uuid::Uuid::new_v4()));
        let store = FsBlobStore::new(&dir).unwrap();
        assert!(store.put("../etc/passwd", b"x").is_err());
        assert!(store.put("ab", b"x").is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
