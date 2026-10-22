//! Endpoint tests_drive tests (split from `endpoints/mod.rs`).
//!
//! Pure move, no behavior change.

#![allow(unused_imports)]

use super::files_http::{
    drive_files_create, identicon, is_inline_safe_mime, serve_file, serve_file_named,
    upload_cap_bytes,
};
use super::parse::MAX_JSON_BODY_BYTES;
use super::tests_common::{CountingChunks, call, read_json, signup_user, test_peer, test_state};
use super::{ApiState, api_dispatch, api_dispatch_blocking};
use crate::application::ports::PortError;
use axum::body::{Body, Bytes};
use axum::extract::{Path, Request, State};
use axum::http::header::{AUTHORIZATION, CONTENT_DISPOSITION, CONTENT_TYPE, LOCATION};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use serde_json::Value;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
#[tokio::test]
async fn drive_upload_json_and_file_serving() {
    let (state, _) = test_state();
    let (_, token) = signup_user(&state, "heidi").await;

    // Multipart upload.
    let boundary = "BOUND123";
    let multipart_body = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"i\"\r\n\r\n{token}\r\n\
         --{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"hi.txt\"\r\n\
         Content-Type: text/plain\r\n\r\nhello multipart\r\n--{boundary}--\r\n"
    );
    let request = Request::builder()
        .header(
            CONTENT_TYPE,
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(Body::from(multipart_body))
        .unwrap();
    let response = drive_files_create(State(state.clone()), test_peer(), request)
        .await
        .unwrap();
    let (status, mp_file, _) = read_json(response).await;
    assert_eq!(status, StatusCode::OK, "{mp_file}");
    assert_eq!(mp_file["name"], "hi.txt");

    // JSON upload (base64 content).
    let (status, file) = call(
        &state,
        "drive/files/create",
        serde_json::json!({
            "i": token,
            "name": "hello.txt",
            "content": "aGVsbG8gd29ybGQ=",
        }),
        None,
    )
    .await;
    // Endpoint is behind the wildcard dispatcher; drive uploads have a
    // dedicated route, so this must 404 as an unknown endpoint.
    assert_eq!(status, StatusCode::NOT_FOUND, "{file}");

    let request = Request::builder()
        .header(CONTENT_TYPE, "application/json")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({
                "name": "hello.txt",
                "content": "aGVsbG8gd29ybGQ=",
            }))
            .unwrap(),
        ))
        .unwrap();
    let response = drive_files_create(State(state.clone()), test_peer(), request)
        .await
        .unwrap();
    let (status, file, _) = read_json(response).await;
    assert_eq!(status, StatusCode::OK, "{file}");
    let file_id = file["id"].as_str().unwrap().to_string();
    assert_eq!(file["name"], "hello.txt");

    // Served back with the stored content type and bytes.
    let response = serve_file(State(state.clone()), Path(file_id.clone()))
        .await
        .unwrap();
    assert_eq!(response.headers()[CONTENT_TYPE], "text/plain");
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(body.as_ref(), b"hello world");
    let _ = file_id;
}

#[tokio::test]
async fn oversized_upload_rejected_without_buffering() {
    // Slice 6 (H1): a streaming body larger than max_file_size + 1 MiB
    // must fail 413 while pulling only ~cap bytes from the source.
    let (state, _) = test_state();
    let (_, token) = signup_user(&state, "uploader_cap").await;
    let cap = upload_cap_bytes(&state.services);
    let chunk = 64 * 1024;
    let total = cap + 5 * 1024 * 1024;
    let yielded = Arc::new(AtomicUsize::new(0));
    let request = Request::builder()
        .header(CONTENT_TYPE, "multipart/form-data; boundary=BOUND123")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::from_stream(CountingChunks {
            remaining: total,
            chunk,
            yielded: yielded.clone(),
        }))
        .unwrap();
    let err = drive_files_create(State(state.clone()), test_peer(), request)
        .await
        .unwrap_err();
    assert_eq!(err.code, "MAX_FILE_SIZE_EXCEEDED");
    assert_eq!(err.http_status, 413);
    let pulled = yielded.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        pulled < total,
        "must abort before reading the whole body: pulled {pulled} of {total}"
    );
    assert!(
        pulled <= cap + chunk,
        "must stay bounded by the cap: pulled {pulled}, cap {cap}"
    );
}

#[tokio::test]
async fn drive_upload_header_auth_checked_before_body_read() {
    // Slice 6 (H1): a bad `Authorization: Bearer` token fails 401
    // without pulling a single body byte.
    let (state, _) = test_state();
    let yielded = Arc::new(AtomicUsize::new(0));
    let request = Request::builder()
        .header(CONTENT_TYPE, "multipart/form-data; boundary=BOUND123")
        .header(AUTHORIZATION, "Bearer wrong-token")
        .body(Body::from_stream(CountingChunks {
            remaining: 4 * 1024 * 1024,
            chunk: 64 * 1024,
            yielded: yielded.clone(),
        }))
        .unwrap();
    let err = drive_files_create(State(state.clone()), test_peer(), request)
        .await
        .unwrap_err();
    assert_eq!(err.http_status, 401);
    assert_eq!(
        yielded.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "no body byte may be read before header auth fails"
    );
}

#[tokio::test]
async fn oversized_json_body_returns_413() {
    // Slice 6 (H1): a JSON body over 1 MiB is rejected 413 in the
    // Misskey dispatch path even when called directly.
    let (state, _) = test_state();
    let (_, token) = signup_user(&state, "json_cap").await;
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        format!("Bearer {token}").parse::<HeaderValue>().unwrap(),
    );
    let big = "x".repeat(MAX_JSON_BODY_BYTES + 1);
    let raw = format!("{{\"text\":{}}}", serde_json::to_string(&big).unwrap());
    let body = Bytes::from(raw.into_bytes());
    assert!(body.len() > MAX_JSON_BODY_BYTES);
    let err = api_dispatch_blocking(
        state.clone(),
        "notes/create".to_string(),
        headers,
        body,
        "127.0.0.1".to_string(),
    )
    .unwrap_err();
    assert_eq!(err.code, "MAX_FILE_SIZE_EXCEEDED");
    assert_eq!(err.http_status, 413);
}

#[tokio::test]
async fn drive_upload_concurrency_limit_returns_429() {
    // Slice 6 (H1, decision D-c): with both per-IP upload slots held,
    // another upload from the same IP fails Misskey-shaped 429.
    let (state, _) = test_state();
    let (_, token) = signup_user(&state, "uploader_busy").await;
    let _first = state.upload_slots.acquire("127.0.0.1").expect("slot 1");
    let _second = state.upload_slots.acquire("127.0.0.1").expect("slot 2");
    let boundary = "BUSY123";
    let multipart_body = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"i\"\r\n\r\n{token}\r\n\
         --{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"tiny.txt\"\r\n\
         Content-Type: text/plain\r\n\r\ntiny\r\n--{boundary}--\r\n"
    );
    let request = Request::builder()
        .header(
            CONTENT_TYPE,
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(Body::from(multipart_body))
        .unwrap();
    let err = drive_files_create(State(state.clone()), test_peer(), request)
        .await
        .unwrap_err();
    assert_eq!(err.code, "RATE_LIMIT_EXCEEDED");
    assert_eq!(err.http_status, 429);
}

#[tokio::test]
async fn unknown_endpoint_is_404() {
    let (state, _) = test_state();
    let (status, value) = call(&state, "nope/not-real", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(value["code"], "UNKNOWN_API_ENDPOINT");
}

#[tokio::test]
async fn serve_file_sets_security_headers() {
    // Slice 7 (H2, written before the fix): every served file carries
    // nosniff + CSP; octet-stream/text/plain (and anything outside the
    // inline-safe set) serve as attachments; sniffed images stay inline.
    let (state, _) = test_state();
    let (account_id, _) = signup_user(&state, "serena").await;
    let mut png = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
    png.extend_from_slice(&[1, 2, 3, 4]);
    let png_id = state
        .services
        .upload_media(&account_id, "pic.png".to_string(), &png, None, false, true)
        .unwrap()
        .id;
    let txt_id = state
        .services
        .upload_media(
            &account_id,
            "hi.txt".to_string(),
            b"hello world",
            None,
            false,
            true,
        )
        .unwrap()
        .id;
    // Non-UTF-8 bytes named `.html`: stored (and served) as
    // octet-stream, never as HTML.
    let bin_id = state
        .services
        .upload_media(
            &account_id,
            "evil.html".to_string(),
            &[0xFF, 0xFE, 0x00, 0x01],
            None,
            false,
            true,
        )
        .unwrap()
        .id;

    let response = serve_file(State(state.clone()), Path(png_id))
        .await
        .unwrap();
    assert_eq!(response.headers()[CONTENT_TYPE], "image/png");
    assert_eq!(response.headers()["x-content-type-options"], "nosniff");
    assert_eq!(
        response.headers()["content-security-policy"],
        "default-src 'none'; sandbox"
    );
    assert!(
        response.headers().get(CONTENT_DISPOSITION).is_none(),
        "sniffed images stay inline"
    );

    let response = serve_file(State(state.clone()), Path(txt_id))
        .await
        .unwrap();
    assert_eq!(response.headers()[CONTENT_TYPE], "text/plain");
    assert_eq!(response.headers()["x-content-type-options"], "nosniff");
    assert_eq!(
        response.headers()["content-security-policy"],
        "default-src 'none'; sandbox"
    );
    assert_eq!(response.headers()[CONTENT_DISPOSITION], "attachment");

    let response = serve_file(State(state.clone()), Path(bin_id))
        .await
        .unwrap();
    assert_eq!(response.headers()[CONTENT_TYPE], "application/octet-stream");
    assert_eq!(response.headers()[CONTENT_DISPOSITION], "attachment");
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(body.as_ref(), &[0xFF, 0xFE, 0x00, 0x01]);
}

#[test]
fn inline_safe_mime_set() {
    assert!(is_inline_safe_mime("image/png"));
    assert!(is_inline_safe_mime("audio/mpeg"));
    assert!(is_inline_safe_mime("video/webm"));
    assert!(!is_inline_safe_mime("application/octet-stream"));
    assert!(!is_inline_safe_mime("text/plain"));
    assert!(!is_inline_safe_mime("application/pdf"));
    assert!(!is_inline_safe_mime("image/svg+xml"));
}

#[tokio::test]
async fn file_named_redirects_and_identicon_serves_svg() {
    let response = serve_file_named(Path(("abc".to_string(), "hi.png".to_string()))).await;
    assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY);
    assert_eq!(response.headers()[LOCATION], "/files/abc");

    let response = identicon(Path("alice".to_string())).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[CONTENT_TYPE], "image/svg+xml");
}

// --- Phase 3 REST profile ------------------------------------------------

#[tokio::test]
async fn phase3_drive_files() {
    let (state, clock) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (_, bob_token) = signup_user(&state, "bob").await;

    let mut png = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];
    png.extend_from_slice(&[9, 9, 9]);
    let png_id = state
        .services
        .upload_media(&alice_id, "pic.png".to_string(), &png, None, false, true)
        .unwrap()
        .id;
    clock.advance_ms(1_000);
    let txt_id = state
        .services
        .upload_media(
            &alice_id,
            "hi.txt".to_string(),
            b"hello world",
            None,
            false,
            true,
        )
        .unwrap()
        .id;
    clock.advance_ms(1_000);

    // Newest first.
    let (status, files) = call(
        &state,
        "drive/files",
        serde_json::json!({}),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{files}");
    assert_eq!(
        files
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["id"].as_str().unwrap().to_string())
            .collect::<Vec<_>>(),
        vec![txt_id.clone(), png_id.clone()]
    );

    // MIME prefix filtering.
    let (status, images) = call(
        &state,
        "drive/files",
        serde_json::json!({ "type": "image/*" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{images}");
    assert_eq!(images.as_array().unwrap().len(), 1);
    assert_eq!(images[0]["id"], png_id);
    let (status, exact) = call(
        &state,
        "drive/files",
        serde_json::json!({ "type": "text/plain" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{exact}");
    assert_eq!(exact.as_array().unwrap().len(), 1);
    assert_eq!(exact[0]["id"], txt_id);

    // Pagination.
    let (status, page) = call(
        &state,
        "drive/files",
        serde_json::json!({ "limit": 1, "untilId": txt_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page.as_array().unwrap().len(), 1);
    assert_eq!(page[0]["id"], png_id);

    // Only the caller's own files.
    let (status, bobs) = call(
        &state,
        "drive/files",
        serde_json::json!({}),
        Some(&bob_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{bobs}");
    assert_eq!(bobs.as_array().unwrap().len(), 0);

    // Param validation.
    for (body, param) in [
        (serde_json::json!({ "sort": "newest" }), "sort"),
        (serde_json::json!({ "type": "not a mime!!" }), "type"),
        (serde_json::json!({ "limit": 0 }), "limit"),
    ] {
        let (status, value) = call(&state, "drive/files", body, Some(&alice_token)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "INVALID_PARAM", "param {param}");
    }
    // Folder-aware listing (gaps WP2a final wiring): a missing/foreign
    // folder matches nothing (→ `[]`), it is no longer rejected.
    let (status, value) = call(
        &state,
        "drive/files",
        serde_json::json!({ "folderId": "did00000000000001" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!((status, value), (StatusCode::OK, serde_json::json!([])));
    // All six Misskey sort variants are honored (inverted mapping).
    for sort in [
        "+createdAt",
        "-createdAt",
        "+name",
        "-name",
        "+size",
        "-size",
    ] {
        let (status, value) = call(
            &state,
            "drive/files",
            serde_json::json!({ "sort": sort }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "sort {sort}: {value}");
    }
    let (status, value) = call(
        &state,
        "drive/files",
        serde_json::json!({ "sort": "-createdAt" }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let (status, value) = call(
        &state,
        "drive/files",
        serde_json::json!({ "bogus": true }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "UNSUPPORTED_FIELD");

    // Auth: anonymous callers are rejected.
    let (status, _) = call(&state, "drive/files", serde_json::json!({}), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn phase3_drive_file_show_update_delete() {
    let (state, clock) = test_state();
    let (alice_id, alice_token) = signup_user(&state, "alice").await;
    let (_, bob_token) = signup_user(&state, "bob").await;
    let file_id = state
        .services
        .upload_media(
            &alice_id,
            "doc.txt".to_string(),
            b"file bytes",
            Some("original".to_string()),
            false,
            true,
        )
        .unwrap()
        .id;
    clock.advance_ms(1_000);

    let (status, shown) = call(
        &state,
        "drive/files/show",
        serde_json::json!({ "fileId": file_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shown}");
    assert_eq!(shown["name"], "doc.txt");
    assert_eq!(shown["comment"], "original");

    // Non-owners (and unknown ids) see NO_SUCH_FILE, never the row.
    for (token, fid) in [
        (bob_token.clone(), file_id.clone()),
        (alice_token.clone(), "mid00000000000099".to_string()),
    ] {
        let (status, value) = call(
            &state,
            "drive/files/show",
            serde_json::json!({ "fileId": fid }),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(value["code"], "NO_SUCH_FILE");
    }

    // Update metadata.
    let (status, updated) = call(
        &state,
        "drive/files/update",
        serde_json::json!({
            "fileId": file_id,
            "name": "renamed.txt",
            "isSensitive": true,
            "comment": "edited",
        }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_eq!(updated["name"], "renamed.txt");
    assert_eq!(updated["isSensitive"], true);
    assert_eq!(updated["comment"], "edited");

    // Invalid names are INVALID_FILE_NAME; overlong comments INVALID_PARAM.
    let long_name = "n".repeat(201);
    for bad in ["", "a/b", "a\\b", "..", long_name.as_str()] {
        let (status, value) = call(
            &state,
            "drive/files/update",
            serde_json::json!({ "fileId": file_id, "name": bad }),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}: {value}");
        assert_eq!(value["code"], "INVALID_FILE_NAME");
    }
    let (status, value) = call(
        &state,
        "drive/files/update",
        serde_json::json!({ "fileId": file_id, "comment": "c".repeat(513) }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "INVALID_PARAM");

    // Bob cannot update or delete Alice's file.
    for endpoint in ["drive/files/update", "drive/files/delete"] {
        let (status, value) = call(
            &state,
            endpoint,
            serde_json::json!({ "fileId": file_id }),
            Some(&bob_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{endpoint}: {value}");
        assert_eq!(value["code"], "NO_SUCH_FILE");
    }

    // Delete removes metadata; the row is gone afterwards.
    let (status, _) = call(
        &state,
        "drive/files/delete",
        serde_json::json!({ "fileId": file_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, value) = call(
        &state,
        "drive/files/show",
        serde_json::json!({ "fileId": file_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_FILE");
    // Deleting again is NO_SUCH_FILE (the row is already gone).
    let (status, value) = call(
        &state,
        "drive/files/delete",
        serde_json::json!({ "fileId": file_id }),
        Some(&alice_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["code"], "NO_SUCH_FILE");
}
