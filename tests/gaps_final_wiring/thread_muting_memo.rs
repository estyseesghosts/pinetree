//! Thread muting (notes/thread-muting/*) and users/update-memo.

use serde_json::{Value, json};

use super::common::{Live, code_of};

pub async fn run(srv: &Live, alice_token: &str, bob_id: &str) {
    // --- notes/thread-muting + users/update-memo -------------------------------
    let (status, value) = srv
        .api(
            "notes/create",
            json!({"text": "thread me"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let note_id = value["createdNote"]["id"].as_str().unwrap().to_string();
    let (status, _) = srv
        .api(
            "notes/thread-muting/create",
            json!({"noteId": note_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, _) = srv
        .api(
            "notes/thread-muting/create",
            json!({"noteId": note_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, _) = srv
        .api(
            "notes/thread-muting/delete",
            json!({"noteId": note_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, value) = srv
        .api(
            "notes/thread-muting/delete",
            json!({"noteId": "missing"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_NOTE");
    assert_eq!(
        value["error"]["id"],
        json!("f92bec2b-2f38-460e-8f68-b3e47fb0986d")
    );
    let (status, value) = srv
        .api(
            "users/update-memo",
            json!({"userId": bob_id, "memo": "owes me lunch"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!((status, value), (200, json!("owes me lunch")));
    let (status, value) = srv
        .api(
            "users/update-memo",
            json!({"userId": bob_id, "memo": ""}),
            Some(&alice_token),
        )
        .await;
    assert_eq!((status, value), (200, Value::Null));
    let (status, value) = srv
        .api(
            "users/update-memo",
            json!({"userId": "missing"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_USER");
}
