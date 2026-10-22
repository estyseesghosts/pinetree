use super::JobWorker;
use crate::api::misskey::packers::{pack_emoji_detailed, pack_user_detailed_not_me};
use crate::application::events::{EmojiBroadcastOp, MainEventKind, MainMessage, StreamingEvent};
use crate::application::ports::PortError;

impl JobWorker {
    /// Expand one `StreamingPublish` job. `Unfollowed` needs packed bodies
    /// (worker-side, like notifications); every other payload flows
    /// straight into the sink untouched — including payloads this worker
    /// cannot parse (the sink validates on its own terms, so a legacy or
    /// foreign payload keeps the sink's retry/quarantine behavior instead
    /// of being reclassified here).
    pub(crate) fn handle_streaming_publish(&self, payload: &[u8]) -> Result<(), PortError> {
        if let Ok(StreamingEvent::Unfollowed {
            follower_id,
            followee_id,
        }) = serde_json::from_slice::<StreamingEvent>(payload)
        {
            // Only local followers hold sockets; the followee pack is for
            // the follower's `main` channel (Misskey parity).
            let local_follower = self
                .services
                .accounts
                .find_by_id(&follower_id)?
                .is_some_and(|a| a.is_local());
            if local_follower {
                if let Some(followee) = self.services.accounts.find_by_id(&followee_id)? {
                    let packed =
                        pack_user_detailed_not_me(&followee, Some(&follower_id), &self.services)?;
                    self.services.streaming.push_main(MainMessage {
                        user_id: follower_id,
                        kind: MainEventKind::Unfollow,
                        body: packed,
                    })?;
                }
            }
            return Ok(());
        }
        // Custom-emoji broadcasts carry domain snapshots (a delete snapshots
        // before its row is gone): pack them here — the sink has no
        // `AppServices` — and push the wire-shaped bodies to every
        // connection (Misskey `onBroadcastMessage` parity).
        if let Ok(StreamingEvent::EmojiBroadcast { op, emojis }) =
            serde_json::from_slice::<StreamingEvent>(payload)
        {
            return self.handle_emoji_broadcast(&op, &emojis);
        }
        self.streaming.publish("streaming", payload)
    }

    /// Expand one `EmojiBroadcast` streaming event (custom-emoji
    /// `emojiAdded`/`emojiUpdated`/`emojiDeleted` parity): pack each
    /// snapshot to `EmojiDetailed` and push the wire-shaped body to every
    /// connection. `Added`/`Updated` re-read the row and skip snapshots
    /// deleted between commit and run; `Deleted` snapshots were taken
    /// before the row was removed, so they always pack.
    fn handle_emoji_broadcast(
        &self,
        op: &EmojiBroadcastOp,
        emojis: &[crate::domain::emoji::CustomEmoji],
    ) -> Result<(), PortError> {
        let mut packed = Vec::with_capacity(emojis.len());
        for snap in emojis {
            match op {
                EmojiBroadcastOp::Added | EmojiBroadcastOp::Updated => {
                    if self.services.emojis.get_emoji(&snap.id)?.is_none() {
                        continue;
                    }
                }
                EmojiBroadcastOp::Deleted => {}
            }
            packed.push(pack_emoji_detailed(snap, &self.services));
        }
        let body = match op {
            EmojiBroadcastOp::Added => serde_json::json!({
                "emoji": packed.into_iter().next().unwrap_or(serde_json::Value::Null),
            }),
            EmojiBroadcastOp::Updated | EmojiBroadcastOp::Deleted => {
                serde_json::json!({ "emojis": packed })
            }
        };
        self.services.streaming.push_broadcast(op.wire_name(), body)
    }
}
