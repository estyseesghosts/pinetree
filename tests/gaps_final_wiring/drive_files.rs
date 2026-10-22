//! Drive file folder legs: drive/files/find, drive/files/move-bulk, folder-scoped drive/files + update.

use serde_json::{Value, json};

use super::common::{Live, code_of};

pub async fn run(srv: &Live, alice_id: &str, alice_token: &str, folder_id: &str) {
    // --- drive/files/find + move-bulk + folder legs --------------------------
    let file_a = srv.make_file(&alice_id, "report-a.png");
    let file_b = srv.make_file(&alice_id, "report-a.png");
    let (status, value) = srv
        .api(
            "drive/files/find",
            json!({"name": "report-a.png"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 2);
    let (status, value) = srv
        .api(
            "drive/files/move-bulk",
            json!({"fileIds": [], "folderId": folder_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "INVALID_PARAM");
    let (status, value) = srv
        .api(
            "drive/files/move-bulk",
            json!({"fileIds": [file_a, file_b], "folderId": "missing"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_FOLDER");
    let (status, _) = srv
        .api(
            "drive/files/move-bulk",
            json!({"fileIds": [file_a, file_b], "folderId": folder_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
    // Root-scoped find no longer sees the moved files.
    let (status, value) = srv
        .api(
            "drive/files/find",
            json!({"name": "report-a.png"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 0);
    // Folder-scoped find sees both.
    let (status, value) = srv
        .api(
            "drive/files/find",
            json!({"name": "report-a.png", "folderId": folder_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 2);
    let (status, value) = srv
        .api(
            "drive/files",
            json!({"folderId": folder_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value.as_array().unwrap().len(), 2);
    // `drive/files/update` moves one file; the pack carries `folderId`.
    let (status, value) = srv
        .api(
            "drive/files/update",
            json!({"fileId": file_a, "folderId": Value::Null}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(value["folderId"], Value::Null);
    let (status, value) = srv
        .api(
            "drive/files/update",
            json!({"fileId": file_a, "folderId": "missing"}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "NO_SUCH_FOLDER");
    // A folder holding files still refuses deletion; emptying it clears
    // the guard.
    let (status, value) = srv
        .api(
            "drive/folders/delete",
            json!({"folderId": folder_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&value), "HAS_CHILD_FILES_OR_FOLDERS");
    let (status, _) = srv
        .api(
            "drive/files/update",
            json!({"fileId": file_b, "folderId": Value::Null}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = srv
        .api(
            "drive/folders/delete",
            json!({"folderId": folder_id}),
            Some(&alice_token),
        )
        .await;
    assert_eq!(status, 204);
}
