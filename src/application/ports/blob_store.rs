use crate::domain::id::PortError;

pub trait BlobStore: Send + Sync {
    fn put(&self, hash: &str, bytes: &[u8]) -> Result<(), PortError>;
    fn get(&self, hash: &str) -> Result<Option<Vec<u8>>, PortError>;
    fn delete(&self, hash: &str) -> Result<(), PortError>;
    /// Byte length without reading content (`None` when absent/corrupt).
    fn byte_len(&self, hash: &str) -> Result<Option<u64>, PortError> {
        Ok(self.get(hash)?.map(|b| b.len() as u64))
    }
    /// Efficient server-side import of a staged temp file (stream-to-disk
    /// target). Default reads + `put`; filesystem backends rename.
    fn import_file(&self, hash: &str, src: &std::path::Path) -> Result<(), PortError> {
        let bytes = std::fs::read(src)
            .map_err(|e| PortError::StorageUnavailable(format!("read staged blob file: {e}")))?;
        self.put(hash, &bytes)
    }
}
