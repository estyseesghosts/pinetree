//! Drive file extras: drive/files/check-existence, drive/files/attached-notes, drive/stream.

use serde_json::json;

use super::common::{Live, code_of};

pub async fn run(srv: &Live, alice_id: &str, alice_token: &str, bob_token: &str) -> String {
    // --- drive extras ----------------------------------------------------------
    let (status, value) = srv
        .api(
            "drive/files/check-existence",
            json!({"md5": "abc"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_FILE");
    let (status, value) = srv
        .api(
            "drive/files/attached-notes",
            json!({"fileId": "nope"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_FILE");
    let (status, value) = srv.api("drive/stream", json!({}), Some(&alice_token)).await;
    assert_eq!((status, value), (200, json!([])));
    let file_id = srv.make_file(&alice_id, "pic.png");
    let (status, _) = srv
        .api(
            "drive/files/check-existence",
            json!({"md5": "nope"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    let (status, value) = srv.api("drive/stream", json!({}), Some(&alice_token)).await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 1);
    let (status, value) = srv
        .api(
            "drive/files/attached-notes",
            json!({"fileId": file_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!((status, value), (200, json!([])));
    let _ = bob_token;
    file_id
}
