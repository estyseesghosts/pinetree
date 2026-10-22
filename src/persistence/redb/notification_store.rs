use super::codec::{decode_json, encode_json, map_redb_err};
use super::note_store::matches_cursor;
use super::store::RedbStore;
use crate::application::ports::{NotificationStore, PortError, TimelineCursor};
use crate::domain::notification::{Notification, NotificationKind};
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};

pub(crate) const NOTIFICATIONS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("notifications");
/// `(notifiee, order key) -> notification id`. The order key is the time-ordered
/// suffix of the id (see [`notification_order_key`]). The full id cannot be the key:
/// it starts with the kind (`notify:inbound-reply:…`), so ordering by it groups rows
/// by kind. Renamed from `notifications_by_user`; old rows are backfilled at open.
pub(crate) const NOTIFICATIONS_BY_USER: TableDefinition<(&str, &str), &str> =
    TableDefinition::new("notifications_by_user_ordered");
/// Time-ordered part of a notification id: `notify:{kind}:{suffix}` → `{suffix}`.
/// Ids without a `:` are returned unchanged.
pub(crate) fn notification_order_key(id: &str) -> &str {
    id.rsplit(':').next().unwrap_or(id)
}

// --- Batch M: moderation log + instance settings (§3.2, §3.3) -------------

impl NotificationStore for RedbStore {
    fn insert_notification(&self, notification: Notification) -> Result<Notification, PortError> {
        let write_txn = self.begin_write()?;
        {
            let mut notif_table = write_txn.open_table(NOTIFICATIONS).map_err(map_redb_err)?;
            let mut by_user = write_txn
                .open_table(NOTIFICATIONS_BY_USER)
                .map_err(map_redb_err)?;
            let encoded = encode_json(&notification)?;
            notif_table
                .insert(notification.id.as_str(), encoded.as_slice())
                .map_err(map_redb_err)?;
            by_user
                .insert(
                    (
                        notification.notifiee_id.as_str(),
                        notification_order_key(&notification.id),
                    ),
                    notification.id.as_str(),
                )
                .map_err(map_redb_err)?;
        }
        self.commit_write(write_txn)?;
        Ok(notification)
    }

    fn list_notifications(
        &self,
        notifiee_id: &str,
        limit: usize,
    ) -> Result<Vec<Notification>, PortError> {
        let limit = if limit == 0 { 20 } else { limit };
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let notif_table = read_txn.open_table(NOTIFICATIONS).map_err(map_redb_err)?;
        let by_user = read_txn
            .open_table(NOTIFICATIONS_BY_USER)
            .map_err(map_redb_err)?;

        let mut out = Vec::new();
        for entry in by_user
            .range((notifiee_id, "")..=(notifiee_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
            .rev()
        {
            let (_, v) = entry.map_err(map_redb_err)?;
            let nid = v.value();
            if let Some(g) = notif_table.get(nid).map_err(map_redb_err)? {
                out.push(decode_json(g.value())?);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    fn list_notifications_filtered(
        &self,
        notifiee_id: &str,
        cursor: &TimelineCursor,
        include: &[NotificationKind],
        exclude: &[NotificationKind],
    ) -> Result<Vec<Notification>, PortError> {
        let limit = if cursor.limit == 0 { 10 } else { cursor.limit };
        // Cursor ids are full notification ids; compare them on the order key.
        let aid_cursor = TimelineCursor {
            since_id: cursor
                .since_id
                .as_deref()
                .map(|s| notification_order_key(s).to_string()),
            until_id: cursor
                .until_id
                .as_deref()
                .map(|s| notification_order_key(s).to_string()),
            ..cursor.clone()
        };
        let read_txn = self.db.begin_read().map_err(map_redb_err)?;
        let notif_table = read_txn.open_table(NOTIFICATIONS).map_err(map_redb_err)?;
        let by_user = read_txn
            .open_table(NOTIFICATIONS_BY_USER)
            .map_err(map_redb_err)?;

        let mut out = Vec::new();
        for entry in by_user
            .range((notifiee_id, "")..=(notifiee_id, "\u{10ffff}"))
            .map_err(map_redb_err)?
            .rev()
        {
            let (_, v) = entry.map_err(map_redb_err)?;
            let nid = v.value();
            if !matches_cursor(notification_order_key(nid), &aid_cursor) {
                continue;
            }
            let Some(g) = notif_table.get(nid).map_err(map_redb_err)? else {
                continue;
            };
            let stored: Notification = decode_json(g.value())?;
            if !include.is_empty() && !include.contains(&stored.kind) {
                continue;
            }
            if exclude.contains(&stored.kind) {
                continue;
            }
            out.push(stored);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    fn mark_all_read(&self, notifiee_id: &str) -> Result<u64, PortError> {
        let write_txn = self.begin_write()?;
        let flipped = {
            let mut notif_table = write_txn.open_table(NOTIFICATIONS).map_err(map_redb_err)?;
            let by_user = write_txn
                .open_table(NOTIFICATIONS_BY_USER)
                .map_err(map_redb_err)?;
            let mut flipped: u64 = 0;
            let ids: Vec<String> = by_user
                .range((notifiee_id, "")..=(notifiee_id, "\u{10ffff}"))
                .map_err(map_redb_err)?
                .filter_map(|entry| entry.ok().map(|(_, v)| v.value().to_string()))
                .collect();
            for nid in ids {
                let raw: Option<Vec<u8>> = notif_table
                    .get(nid.as_str())
                    .map_err(map_redb_err)?
                    .map(|g| g.value().to_vec());
                let Some(raw) = raw else { continue };
                let mut stored: Notification = decode_json(&raw)?;
                if stored.is_read {
                    continue;
                }
                stored.is_read = true;
                let encoded = encode_json(&stored)?;
                notif_table
                    .insert(nid.as_str(), encoded.as_slice())
                    .map_err(map_redb_err)?;
                flipped += 1;
            }
            flipped
        };
        self.commit_write(write_txn)?;
        Ok(flipped)
    }
}
