//! Minimal repro of the federation hang: handler-thread blocking HTTP
//! cross-call between two axum servers sharing one tokio runtime.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

fn blocking_get(url: &str) -> String {
    let rest = url.strip_prefix("http://").unwrap();
    let (host_port, path) = match rest.split_once('/') {
        Some((h, p)) => (h, format!("/{p}")),
        None => (rest, "/".to_string()),
    };
    let mut s = TcpStream::connect_timeout(
        &host_port.parse::<std::net::SocketAddr>().unwrap(),
        Duration::from_secs(10),
    )
    .unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).unwrap();
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match s.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => out.extend_from_slice(&buf[..n]),
            Err(e) => return format!("READ ERR: {e}"),
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minimal_cross_call() {
    // B: trivial responder.
    let app_b = axum::Router::new().route("/x", axum::routing::get(|| async { "b-ok" }));
    let lb = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let b_url = format!("http://{}/x", lb.local_addr().unwrap());
    let _hb = tokio::spawn(async move {
        let _ = axum::serve(lb, app_b).await;
    });

    // A: handler does a BLOCKING cross-call to B on the handler thread.
    let app_a = axum::Router::new().route(
        "/y",
        axum::routing::get(move || async move {
            let u = b_url.clone();
            let peer = tokio::task::spawn_blocking(move || blocking_get(&u))
                .await
                .unwrap();
            format!("a-ok peer={peer}")
        }),
    );
    let la = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a_url = format!("http://{}/y", la.local_addr().unwrap());
    let _ha = tokio::spawn(async move {
        let _ = axum::serve(la, app_a).await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Test thread parks in a blocking GET to A (mirrors api_post/client.send).
    let body = tokio::task::spawn_blocking(move || blocking_get(&a_url))
        .await
        .unwrap();
    println!("RESULT: {}", body.lines().next().unwrap_or(""));
    assert!(body.contains("a-ok"), "cross-call failed: {body}");
}
