//! Channels/chat stay 404 by owner SKIP order (docs/gaps.md:5).

use serde_json::json;

use super::common::{Live, code_of};

pub async fn run(srv: &Live, alice_token: &str) {
    // --- channels/chat stay 404 -------------------------------------------
    // Owner SKIP order (`docs/gaps.md:5`): the code was removed from the
    // tree, so these stay `UNKNOWN_API_ENDPOINT` (not `UNSUPPORTED`).
    for endpoint in [
        "channels/create",
        "channels/show",
        "chat/rooms/create",
        "chat/messages/create",
    ] {
        let (status, value) = srv.api(endpoint, json!({}), Some(&alice_token)).await;
        assert_eq!(status, 404, "{endpoint}");
        assert_eq!(code_of(&value), "UNKNOWN_API_ENDPOINT", "{endpoint}");
    }
}
