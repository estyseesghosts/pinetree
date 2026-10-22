//! Drive folder CRUD: drive/folders/create, show, find, update, delete.

use serde_json::{Value, json};

use super::common::{Live, code_of};

pub async fn run(srv: &Live, alice_id: &str, alice_token: &str, bob_token: &str) -> String {
    // --- drive/folders CRUD -------------------------------------------------
    let (status, value) = srv
        .api(
            "drive/folders/create",
            json!({"name": "docs"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value["name"], json!("docs"));
    assert_eq!(value["parentId"], Value::Null);
    assert_eq!(value["userId"], json!(alice_id));
    let folder_id = value["id"].as_str().unwrap().to_string();
    let (status, value) = srv
        .api(
            "drive/folders/create",
            json!({"parentId": folder_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value["name"], json!("Untitled"));
    assert_eq!(value["parentId"], json!(folder_id));
    let child_id = value["id"].as_str().unwrap().to_string();
    let (status, value) = srv
        .api(
            "drive/folders/create",
            json!({"name": "orphan", "parentId": "missing"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_FOLDER");
    assert_eq!(
        value["error"]["id"],
        json!("17ebfd4a-6d3b-4318-bc8f-b605f418ee3f")
    );
    let (status, value) = srv
        .api(
            "drive/folders/show",
            json!({"folderId": folder_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!((status, value["name"].clone()), (200, json!("docs")));
    let (status, value) = srv
        .api(
            "drive/folders/show",
            json!({"folderId": "missing"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_FOLDER");
    // Bob's folder reads as missing to alice (ownership precedence).
    let (status, value) = srv
        .api(
            "drive/folders/create",
            json!({"name": "bobs"}),
            Some(&bob_token),
        )
        .await;
    assert_eq!(status, 200);
    let bob_folder = value["id"].as_str().unwrap().to_string();
    let (status, value) = srv
        .api(
            "drive/folders/show",
            json!({"folderId": bob_folder}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_FOLDER");
    let (status, value) = srv
        .api(
            "drive/folders/find",
            json!({"name": "docs"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 1);
    let (status, value) = srv
        .api(
            "drive/folders/find",
            json!({"name": "docs", "parentId": "missing"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!((status, value), (200, json!([])));
    let (status, value) = srv
        .api(
            "drive/folders/update",
            json!({"folderId": folder_id, "name": "papers"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!((status, value["name"].clone()), (200, json!("papers")));
    let (status, value) = srv
        .api(
            "drive/folders/update",
            json!({"folderId": child_id, "parentId": child_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "RECURSIVE_NESTING");
    let (status, value) = srv
        .api(
            "drive/folders/update",
            json!({"folderId": child_id, "parentId": "missing"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_PARENT_FOLDER");
    // Non-empty folders refuse deletion (the child is still inside).
    let (status, value) = srv
        .api(
            "drive/folders/delete",
            json!({"folderId": folder_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "HAS_CHILD_FILES_OR_FOLDERS");
    let (status, value) = srv
        .api(
            "drive/folders/update",
            json!({"folderId": child_id, "parentId": Value::Null}),
            Some(&alice_token),
        )
        .await;
    assert_eq!((status, value["parentId"].clone()), (200, Value::Null));
    // Deleting the child (empty, at root) succeeds.
    let (status, _) = srv
        .api(
            "drive/folders/delete",
            json!({"folderId": child_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    folder_id
}
