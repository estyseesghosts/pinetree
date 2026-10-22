use axum::extract::ws::{Message, WebSocket};
use futures_util::SinkExt as _;
use serde_json::Value;

/// `connected {id}` ack for `connect` with `pong: true` (Misskey parity).
pub(crate) async fn send_pong(
    body: Option<&Value>,
    id: &str,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) {
    let pong = body
        .and_then(|b| b.get("pong"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if pong {
        let frame = Message::Text(
            serde_json::json!({ "type": "connected", "body": { "id": id } })
                .to_string()
                .into(),
        );
        let _ = sender.send(frame).await;
    }
}

pub(crate) fn channel_frame(channel_id: &str, event: &str, body: Value) -> Message {
    Message::Text(
        serde_json::json!({
            "type": "channel",
            "body": { "id": channel_id, "type": event, "body": body },
        })
        .to_string()
        .into(),
    )
}

pub(crate) fn note_updated_frame(note_id: &str, event: &str, body: Value) -> Message {
    Message::Text(
        serde_json::json!({
            "type": "noteUpdated",
            "body": { "id": note_id, "type": event, "body": body },
        })
        .to_string()
        .into(),
    )
}

#[cfg(test)]
mod tests {
    use super::channel_frame;
    use super::note_updated_frame;
    use axum::extract::ws::Message;
    use serde_json::Value;

    #[test]
    fn channel_frame_shapes_match_misskey_js_demux() {
        let body = serde_json::json!({ "id": "n1" });
        let Message::Text(text) = channel_frame("chan-1", "note", body) else {
            panic!("expected text frame");
        };
        let parsed: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(parsed["type"], "channel");
        assert_eq!(parsed["body"]["id"], "chan-1");
        assert_eq!(parsed["body"]["type"], "note");
        assert_eq!(parsed["body"]["body"]["id"], "n1");
        let Message::Text(text) = note_updated_frame(
            "n1",
            "reacted",
            serde_json::json!({ "reaction": "❤", "userId": "u1" }),
        ) else {
            panic!("expected text frame");
        };
        let parsed: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(parsed["type"], "noteUpdated");
        assert_eq!(parsed["body"]["id"], "n1");
        assert_eq!(parsed["body"]["type"], "reacted");
    }
}
