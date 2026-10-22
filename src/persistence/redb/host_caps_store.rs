use super::codec::{decode_json, encode_json, map_redb_err};
use super::store::RedbStore;
use crate::application::ports::{HostCapsStore, HostSoftware, PortError};
use redb::{ReadableDatabase, ReadableTable, TableDefinition};

/// Cached per-host ActivityPub software names (§6 `docs/misskey-parity-plan.md`;
/// `HostCapsStore` parity): lowercased host → JSON [`HostSoftware`] row.
pub(crate) const HOST_SOFTWARE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("host_software");
impl HostCapsStore for RedbStore {
    fn get_host_software(&self, host: &str) -> Result<Option<HostSoftware>, PortError> {
        let key = host.to_ascii_lowercase();
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let table = read_txn.open_table(HOST_SOFTWARE).map_err(map_redb_err)?;
        match table.get(key.as_str()).map_err(map_redb_err)? {
            Some(g) => Ok(Some(decode_json(g.value())?)),
            None => Ok(None),
        }
    }

    fn put_host_software(&self, entry: HostSoftware) -> Result<(), PortError> {
        let key = entry.host.to_ascii_lowercase();
        let bytes = encode_json(&HostSoftware {
            host: key.clone(),
            ..entry
        })?;
        let write_txn = self.begin_write()?;
        {
            let mut table = write_txn.open_table(HOST_SOFTWARE).map_err(map_redb_err)?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)?;
        Ok(())
    }
}
