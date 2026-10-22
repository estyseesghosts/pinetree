//! Pinetree <-> Pleroma <-> Akkoma federation: baseline posts/replies/
//! renotes/media/follows plus quote-post interop in both directions.
//!
//! Needs the rig from `/root/fedifedifedi/local-federation` (plus the
//! `pinetree.test` block documented in the state log) with `pq@pleroma.test`,
//! `aq@akkoma.test` (password `password123`) and a throwaway pinetree on
//! `http://pinetree.test` with `pt5`/`password123`.
//!
//! Without `PINETREE_ELIXIR_INTEROP=1` this prints a skip notice and passes,
//! so plain `cargo test` stays hermetic.
//!
//! Neither Pleroma nor Akkoma implements FEP-044f authorization (legacy
//! `quoteUrl`/`quoteUri` only, verified live against both): quotes involving
//! them stay `pending` (ours) / `unapproved` (theirs) — never `rejected`.
//! The full request->accept cycle is covered pinetree<->pinetree in
//! `tests/federation_interop.rs`.

use std::thread::sleep;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};

const DEADLINE: Duration = Duration::from_secs(120);

fn client() -> SafeHttpClient {
    SafeHttpClient::new(SafeHttpConfig {
        allow_private_ips: true,
        timeout: Duration::from_secs(60),
        ..Default::default()
    })
}

struct Node {
    name: &'static str,
    base: String,
    kind: Kind,
    token: String,
    user_id: String,
    username: &'static str,
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Pleroma,
    Akkoma,
    Pinetree,
}

fn raw(
    c: &SafeHttpClient,
    method: &str,
    url: &str,
    token: Option<&str>,
    body: Option<Value>,
) -> (u16, Value) {
    let bytes = body.map(|b| serde_json::to_vec(&b).expect("encode"));
    let bearer = token.map(|t| format!("Bearer {t}"));
    let mut headers: Vec<(&str, &str)> = vec![("Accept", "application/json")];
    if bytes.is_some() {
        headers.push(("Content-Type", "application/json"));
    }
    if let Some(b) = bearer.as_ref() {
        headers.push(("Authorization", b.as_str()));
    }
    let resp = c
        .send(method, url, &headers, bytes.as_deref(), 4 * 1024 * 1024)
        .expect("http send");
    let v: Value = serde_json::from_slice(&resp.body).unwrap_or(Value::Null);
    (resp.status, v)
}

fn poll<T>(desc: &str, mut f: impl FnMut() -> Option<T>) -> T {
    poll_with(desc, DEADLINE, f)
}

/// Extended-deadline poll for convergence under burst load: peer instances
/// (dev-configured Pleroma/Akkoma) can take minutes to drain a burst of
/// 15 posts + quotes through their federation queues. Only the final
/// convergence loop uses this; everything else stays on the tight deadline
/// so real stalls still fail fast.
fn poll_with<T>(desc: &str, deadline: Duration, mut f: impl FnMut() -> Option<T>) -> T {
    let start = Instant::now();
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(start.elapsed() < deadline, "timed out waiting for: {desc}");
        sleep(Duration::from_millis(1000));
    }
}

/// Mastodon password-grant token (Pleroma + Akkoma share the flow).
fn masto_token(c: &SafeHttpClient, base: &str, username: &str, password: &str) -> String {
    let (_, app) = raw(
        c,
        "POST",
        &format!("{base}/api/v1/apps"),
        None,
        Some(
            json!({"client_name": "quote-interop", "redirect_uris": "urn:ietf:wg:oauth:2.0:oob", "scopes": "read write follow"}),
        ),
    );
    let cid = app["client_id"].as_str().expect("client_id").to_string();
    let sec = app["client_secret"]
        .as_str()
        .expect("client_secret")
        .to_string();
    let form = format!(
        "grant_type=password&client_id={cid}&client_secret={sec}&username={username}&password={password}&scope=read+write+follow"
    );
    let bytes = form.into_bytes();
    let resp = c
        .send(
            "POST",
            &format!("{base}/oauth/token"),
            &[
                ("Content-Type", "application/x-www-form-urlencoded"),
                ("Accept", "application/json"),
            ],
            Some(&bytes),
            65536,
        )
        .expect("oauth token");
    assert!(
        (200..300).contains(&resp.status),
        "oauth token: {}",
        String::from_utf8_lossy(&resp.body)
    );
    let v: Value = serde_json::from_slice(&resp.body).expect("token json");
    v["access_token"]
        .as_str()
        .expect("access_token")
        .to_string()
}

fn masto_me(c: &SafeHttpClient, base: &str, token: &str) -> (String, String) {
    let (s, v) = raw(
        &c,
        "GET",
        &format!("{base}/api/v1/accounts/verify_credentials"),
        Some(token),
        None,
    );
    assert_eq!(s, 200, "verify_credentials: {v}");
    (
        v["id"].as_str().expect("id").to_string(),
        v["acct"].as_str().unwrap_or("?").to_string(),
    )
}

/// Post a plain status via the Mastodon API. Returns (status id, ap uri).
fn masto_post(c: &SafeHttpClient, n: &Node, text: &str) -> (String, String) {
    let (s, v) = raw(
        c,
        "POST",
        &format!("{}/api/v1/statuses", n.base),
        Some(&n.token),
        Some(json!({"status": text})),
    );
    assert_eq!(s, 200, "{} post: {v}", n.name);
    (
        v["id"].as_str().expect("id").to_string(),
        v["uri"].as_str().unwrap_or("").to_string(),
    )
}

/// Post a quote via `quoted_status_id` (Pleroma + Akkoma share the param).
fn masto_quote(c: &SafeHttpClient, n: &Node, text: &str, quoted_id: &str) -> (String, String) {
    let (s, v) = raw(
        c,
        "POST",
        &format!("{}/api/v1/statuses", n.base),
        Some(&n.token),
        Some(json!({"status": text, "quoted_status_id": quoted_id})),
    );
    assert_eq!(s, 200, "{} quote: {v}", n.name);
    (
        v["id"].as_str().expect("id").to_string(),
        v["uri"].as_str().unwrap_or("").to_string(),
    )
}

fn masto_status(c: &SafeHttpClient, n: &Node, id: &str) -> Value {
    let (s, v) = raw(
        &c,
        "GET",
        &format!("{}/api/v1/statuses/{id}", n.base),
        Some(&n.token),
        None,
    );
    assert_eq!(s, 200, "{} get status {id}: {v}", n.name);
    v
}

/// Home timeline texts (Mastodon) or note texts (pinetree).
fn timeline_texts(c: &SafeHttpClient, n: &Node) -> Vec<String> {
    match n.kind {
        Kind::Pinetree => {
            let (s, v) = raw(
                &c,
                "POST",
                &format!("{}/api/notes/timeline", n.base),
                Some(&n.token),
                Some(json!({"limit": 80})),
            );
            if s != 200 {
                return Vec::new();
            }
            v.as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|n| n["text"].as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default()
        }
        _ => {
            let (s, v) = raw(
                &c,
                "GET",
                &format!("{}/api/v1/timelines/home?limit=80", n.base),
                Some(&n.token),
                None,
            );
            if s != 200 {
                return Vec::new();
            }
            v.as_array()
                .map(|a| {
                    a.iter()
                        .map(|s| {
                            // Strip HTML tags for text matching. Pure
                            // Announce wrappers carry the original under
                            // `reblog` with empty top-level content.
                            let html = s["content"].as_str().unwrap_or("");
                            if html.trim().is_empty() {
                                if let Some(rb) = s.get("reblog") {
                                    let nested =
                                        rb.get("content").and_then(Value::as_str).unwrap_or("");
                                    return strip_tags(nested);
                                }
                            }
                            strip_tags(html)
                        })
                        .collect()
                })
                .unwrap_or_default()
        }
    }
}

/// Resolve `acct` on node `n` (triggers a remote fetch when needed).
fn resolve(c: &SafeHttpClient, n: &Node, acct: &str) -> Option<Value> {
    let url = match n.kind {
        Kind::Pinetree => format!("{}/api/users/show", n.base),
        _ => format!(
            "{}/api/v2/search?q={}&resolve=true",
            n.base,
            urlencoding(acct)
        ),
    };
    if n.kind == Kind::Pinetree {
        let user = acct.split('@').next().unwrap_or(acct);
        let host = acct.split('@').nth(1).unwrap_or("");
        let (s, v) = raw(
            &c,
            "POST",
            &url,
            Some(&n.token),
            Some(json!({"username": user, "host": host})),
        );
        return (s == 200).then_some(v);
    }
    let (s, v) = raw(&c, "GET", &url, Some(&n.token), None);
    if s != 200 {
        return None;
    }
    v["accounts"].as_array().and_then(|a| a.first().cloned())
}

fn strip_tags(html: &str) -> String {
    let mut out = String::new();
    let mut in_tag = false;
    for ch in html.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    out
}
fn urlencoding(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"@._~-".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn sign_in_all(c: &SafeHttpClient) -> Vec<Node> {
    let pq_token = masto_token(c, "http://pleroma.test", "pq@pleroma.test", "password123");
    let (pq_id, _) = masto_me(c, "http://pleroma.test", &pq_token);
    let aq_token = masto_token(c, "http://akkoma.test", "aq@akkoma.test", "password123");
    let (aq_id, _) = masto_me(c, "http://akkoma.test", &aq_token);
    let (s, v) = raw(
        c,
        "POST",
        "http://pinetree.test/api/signin",
        None,
        Some(json!({"username": "pt5", "password": "password123"})),
    );
    assert_eq!(s, 200, "pt5 signin: {v}");
    vec![
        Node {
            name: "pq",
            base: "http://pleroma.test".to_string(),
            kind: Kind::Pleroma,
            token: pq_token,
            user_id: pq_id,
            username: "pq",
        },
        Node {
            name: "aq",
            base: "http://akkoma.test".to_string(),
            kind: Kind::Akkoma,
            token: aq_token,
            user_id: aq_id,
            username: "aq",
        },
        Node {
            name: "pt5",
            base: "http://pinetree.test".to_string(),
            kind: Kind::Pinetree,
            token: v["token"].as_str().expect("token").to_string(),
            user_id: v["id"].as_str().expect("id").to_string(),
            username: "pt5",
        },
    ]
}

fn node<'a>(nodes: &'a [Node], name: &str) -> &'a Node {
    nodes.iter().find(|n| n.name == name).expect("node")
}

fn ap_get(c: &SafeHttpClient, url: &str) -> Value {
    let resp = c
        .send(
            "GET",
            url,
            &[("Accept", "application/activity+json")],
            None,
            65536,
        )
        .expect("ap get");
    assert!(
        (200..300).contains(&resp.status),
        "AP GET {url}: {}",
        resp.status
    );
    serde_json::from_slice(&resp.body).expect("ap json")
}

#[test]
fn elixir_quote_federation() {
    if std::env::var("PINETREE_ELIXIR_INTEROP").ok().as_deref() != Some("1") {
        eprintln!(
            "elixir_quote_federation: skipped (set PINETREE_ELIXIR_INTEROP=1 with the local-federation rig + pinetree.test up)"
        );
        return;
    }
    let c = client();
    let tag = format!(
        "{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    );
    let nodes = sign_in_all(&c);
    let (pq, aq, pt5) = (node(&nodes, "pq"), node(&nodes, "aq"), node(&nodes, "pt5"));

    // --- resolve every remote user from every node -------------------------
    for (v, acct) in [
        (pq, "aq@akkoma.test"),
        (pq, "pt5@pinetree.test"),
        (aq, "pq@pleroma.test"),
        (aq, "pt5@pinetree.test"),
        (pt5, "pq@pleroma.test"),
        (pt5, "aq@akkoma.test"),
    ] {
        poll(&format!("{} resolves {acct}", v.name), || {
            resolve(&c, v, acct)
        });
    }

    // --- follow matrix pinetree<->each (Mastodon account-id follow) --------
    // Throttling (429) is retried via poll; re-runs tolerate existing follows.
    for (v, target_acct) in [(pq, "pt5@pinetree.test"), (aq, "pt5@pinetree.test")] {
        let remote = poll(
            &format!("{} resolves {target_acct} for follow", v.name),
            || masto_search_account(&c, v, target_acct),
        );
        poll(&format!("{} follows {target_acct}", v.name), || {
            let (s, r) = raw(
                &c,
                "POST",
                &format!("{}/api/v1/accounts/{}/follow", v.base, remote),
                Some(&v.token),
                Some(json!({})),
            );
            let done = (200..300).contains(&s)
                || s == 400
                || r.to_string().to_lowercase().contains("already");
            done.then_some(())
        });
    }
    for (v, user, host) in [(pt5, "pq", "pleroma.test"), (pt5, "aq", "akkoma.test")] {
        let remote = poll(&format!("pt5 resolves {user}@{host}"), || {
            resolve(&c, v, &format!("{user}@{host}"))
        });
        let rid = remote["id"].as_str().expect("remote id").to_string();
        poll(&format!("pt5 follows {user}@{host}"), || {
            let (s, r) = raw(
                &c,
                "POST",
                &format!("{}/api/following/create", v.base),
                Some(&v.token),
                Some(json!({"userId": rid})),
            );
            let done = (200..300).contains(&s)
                || (s == 400 && r.to_string().contains("ALREADY_FOLLOWING"));
            done.then_some(())
        });
    }

    // Gate posting on follow establishment in BOTH directions (follows are
    // async activities; posting before the Accept lands loses the first
    // fan-out with no retroactive delivery).
    poll("pt5 has 2 followers", || {
        let (s, v) = raw(
            &c,
            "POST",
            &format!("{}/api/users/show", pt5.base),
            Some(&pt5.token),
            Some(json!({"userId": pt5.user_id})),
        );
        (s == 200 && v["followersCount"].as_u64() == Some(2)).then_some(())
    });
    for (v, me_acct) in [(pq, "pt5@pinetree.test"), (aq, "pt5@pinetree.test")] {
        poll(&format!("{} sees mutual follow with pt5", v.name), || {
            let id = masto_search_account(&c, v, me_acct)?;
            let (s, v2) = raw(
                &c,
                "GET",
                &format!("{}/api/v1/accounts/relationships?id[]={id}", v.base),
                Some(&v.token),
                None,
            );
            (s == 200)
                .then(|| {
                    v2.as_array()?.first().and_then(|r| {
                        (r["following"].as_bool() == Some(true)
                            && r["followed_by"].as_bool() == Some(true))
                        .then_some(())
                    })
                })
                .flatten()
        });
    }

    // --- baseline text posts reach every other node -------------------------
    let (_pq_id, pq_uri) = masto_post(&c, pq, &format!("baseline pq {tag}"));
    let (_aq_id, _aq_uri) = masto_post(&c, aq, &format!("baseline aq {tag}"));
    let (s, v) = raw(
        &c,
        "POST",
        &format!("{}/api/notes/create", pt5.base),
        Some(&pt5.token),
        Some(json!({"text": format!("baseline pt5 {tag}"), "visibility": "public"})),
    );
    assert_eq!(s, 200, "pt5 post: {v}");
    let _pt5_id = v["createdNote"]["id"].as_str().expect("id").to_string();
    poll(&format!("pt5 sees pq baseline"), || {
        timeline_texts(&c, pt5)
            .iter()
            .any(|t| t.contains(&format!("baseline pq {tag}")))
            .then_some(())
    });
    poll(&format!("pq sees pt5 baseline"), || {
        timeline_texts(&c, pq)
            .iter()
            .any(|t| t.contains(&format!("baseline pt5 {tag}")))
            .then_some(())
    });
    poll(&format!("aq sees pt5 baseline"), || {
        timeline_texts(&c, aq)
            .iter()
            .any(|t| t.contains(&format!("baseline pt5 {tag}")))
            .then_some(())
    });
    poll(&format!("pt5 sees aq baseline"), || {
        timeline_texts(&c, pt5)
            .iter()
            .any(|t| t.contains(&format!("baseline aq {tag}")))
            .then_some(())
    });

    // --- replies (to the viewer's local copy) --------------------------------
    let pt5_on_pq = poll("pq resolves pt5 baseline copy", || {
        masto_search_status(&c, pq, &format!("baseline pt5 {tag}"))
    });
    let (s, v) = raw(
        &c,
        "POST",
        &format!("{}/api/v1/statuses", pq.base),
        Some(&pq.token),
        Some(json!({"status": format!("reply pq->pt5 {tag}"), "in_reply_to_id": pt5_on_pq})),
    );
    assert_eq!(s, 200, "pq replies to pt5: {v}");
    poll(&format!("pt5 sees pq reply"), || {
        timeline_texts(&c, pt5)
            .iter()
            .any(|t| t.contains(&format!("reply pq->pt5 {tag}")))
            .then_some(())
    });

    // --- renote (reblog of the local copy) ------------------------------------
    let pt5_on_aq = poll("aq resolves pt5 baseline copy", || {
        masto_search_status(&c, aq, &format!("baseline pt5 {tag}"))
    });
    let (s, r) = raw(
        &c,
        "POST",
        &format!("{}/api/v1/statuses/{pt5_on_aq}/reblog", aq.base),
        Some(&aq.token),
        Some(json!({"visibility": "public"})),
    );
    assert!((200..300).contains(&s), "aq reblogs pt5: {s} {r}");

    // --- media attachment pleroma -> pinetree --------------------------------
    let png = tiny_png();
    let boundary = "QTBOUNDARY";
    let mut body = Vec::new();
    body.extend_from_slice(format!("--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"tiny.png\"\r\nContent-Type: image/png\r\n\r\n").as_bytes());
    body.extend_from_slice(&png);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    let resp = c
        .send(
            "POST",
            &format!("{}/api/v2/media", pq.base),
            &[
                ("Authorization", &format!("Bearer {}", pq.token)),
                (
                    "Content-Type",
                    &format!("multipart/form-data; boundary={boundary}"),
                ),
            ],
            Some(&body),
            4 * 1024 * 1024,
        )
        .expect("media upload");
    assert!(
        (200..300).contains(&resp.status),
        "media upload: {}",
        resp.status
    );
    let mv: Value = serde_json::from_slice(&resp.body).expect("media json");
    let mid = mv["id"].as_str().expect("media id").to_string();
    let (s, v) = raw(
        &c,
        "POST",
        &format!("{}/api/v1/statuses", pq.base),
        Some(&pq.token),
        Some(json!({"status": format!("media post pq {tag}"), "media_ids": [mid]})),
    );
    assert_eq!(s, 200, "pq media post: {v}");
    poll("pt5 receives pq media post with attachment", || {
        let (s, v) = raw(
            &c,
            "POST",
            &format!("{}/api/notes/timeline", pt5.base),
            Some(&pt5.token),
            Some(json!({"limit": 80})),
        );
        (s == 200)
            .then(|| {
                v.as_array().and_then(|a| {
                    a.iter()
                        .find(|n| {
                            n["text"]
                                .as_str()
                                .is_some_and(|t| t.contains(&format!("media post pq {tag}")))
                                && n["files"]
                                    .as_array()
                                    .map(|f| !f.is_empty())
                                    .unwrap_or(false)
                        })
                        .cloned()
                })
            })
            .flatten()
    });

    // --- quotes -------------------------------------------------------------
    // pt5 quotes pq's baseline (pending: Pleroma never answers QuoteRequest).
    let pq_local = poll("pt5 resolves pq baseline copy", || {
        let (s, v) = raw(
            &c,
            "POST",
            &format!("{}/api/notes/timeline", pt5.base),
            Some(&pt5.token),
            Some(json!({"limit": 80})),
        );
        (s == 200)
            .then(|| {
                v.as_array().and_then(|a| {
                    a.iter()
                        .find(|n| {
                            n["text"]
                                .as_str()
                                .is_some_and(|t| t.contains(&format!("baseline pq {tag}")))
                        })
                        .and_then(|n| n["id"].as_str().map(str::to_string))
                })
            })
            .flatten()
    });
    let (s, v) = raw(
        &c,
        "POST",
        &format!("{}/api/notes/create", pt5.base),
        Some(&pt5.token),
        Some(json!({"renoteId": pq_local, "text": format!("pt5 quotes pq {tag}")})),
    );
    assert_eq!(s, 200, "pt5 quotes pq: {v}");
    let pt5_quote_id = v["createdNote"]["id"].as_str().expect("id").to_string();
    assert_eq!(
        v["createdNote"]["quoteAuthorizationStatus"].as_str(),
        Some("pending")
    );
    // Still pending after a grace period: no rejection from a non-FEP peer.
    sleep(Duration::from_secs(10));
    let (s, v) = raw(
        &c,
        "POST",
        &format!("{}/api/notes/show", pt5.base),
        Some(&pt5.token),
        Some(json!({"noteId": pt5_quote_id})),
    );
    assert_eq!(s, 200);
    assert_eq!(
        v["quoteAuthorizationStatus"].as_str(),
        Some("pending"),
        "non-FEP peer never rejects"
    );

    // pq receives pt5's quote through the legacy fields.
    poll("pq receives pt5 quote", || {
        timeline_texts(&c, pq)
            .iter()
            .any(|t| t.contains(&format!("pt5 quotes pq {tag}")))
            .then_some(())
    });
    // Outbound AP shape of pt5's quote.
    let doc = ap_get(&c, &format!("http://pinetree.test/notes/{pt5_quote_id}"));
    assert_eq!(doc["quote"], pq_uri);
    assert_eq!(doc["_misskey_quote"], pq_uri);
    assert_eq!(doc["quoteUrl"], pq_uri);
    assert_eq!(doc["quoteUri"], pq_uri);
    assert!(
        doc.get("quoteAuthorization").is_none(),
        "no stamp without approval"
    );
    let content = doc["content"].as_str().unwrap_or("");
    assert_eq!(content.matches("RE:").count(), 1, "single textual fallback");

    // pq quotes pt5's baseline (legacy-only inbound to pinetree: unapproved).
    let pt5_remote_id = poll("pq resolves pt5 baseline", || {
        masto_search_status(&c, pq, &format!("baseline pt5 {tag}"))
    });
    let (pq_quote_id, pq_quote_uri) =
        masto_quote(&c, pq, &format!("pq quotes pt5 {tag}"), &pt5_remote_id);
    let _ = pq_quote_id;
    poll("pt5 recognizes pq legacy-only quote", || {
        let (s, v) = raw(
            &c,
            "POST",
            &format!("{}/api/notes/timeline", pt5.base),
            Some(&pt5.token),
            Some(json!({"limit": 80})),
        );
        (s == 200)
            .then(|| {
                v.as_array().and_then(|a| {
                    a.iter()
                        .find(|n| {
                            n["text"]
                                .as_str()
                                .is_some_and(|t| t.contains(&format!("pq quotes pt5 {tag}")))
                                && !n["renoteId"].is_null()
                                && n["quoteAuthorizationStatus"].as_str() == Some("unapproved")
                        })
                        .cloned()
                })
            })
            .flatten()
    });
    // The inbound AP object carried quoteUrl/quoteUri and no stamp.
    let pq_doc = ap_get(&c, &pq_quote_uri);
    assert!(pq_doc["quoteUrl"].as_str().is_some() || pq_doc["quoteUri"].as_str().is_some());
    assert!(
        pq_doc.get("quote").is_none(),
        "pleroma sends no FEP-044f quote"
    );
    assert!(pq_doc.get("quoteAuthorization").is_none());

    // aq quotes pt5's baseline (akkoma canonical quoteUri inbound).
    let pt5_on_aq = poll("aq resolves pt5 baseline", || {
        masto_search_status(&c, aq, &format!("baseline pt5 {tag}"))
    });
    let (_, aq_quote_uri) = masto_quote(&c, aq, &format!("aq quotes pt5 {tag}"), &pt5_on_aq);
    poll("pt5 recognizes aq legacy-only quote", || {
        let (s, v) = raw(
            &c,
            "POST",
            &format!("{}/api/notes/timeline", pt5.base),
            Some(&pt5.token),
            Some(json!({"limit": 80})),
        );
        (s == 200)
            .then(|| {
                v.as_array().and_then(|a| {
                    a.iter()
                        .find(|n| {
                            n["text"]
                                .as_str()
                                .is_some_and(|t| t.contains(&format!("aq quotes pt5 {tag}")))
                                && !n["renoteId"].is_null()
                                && n["quoteAuthorizationStatus"].as_str() == Some("unapproved")
                        })
                        .cloned()
                })
            })
            .flatten()
    });
    let aq_doc = ap_get(&c, &aq_quote_uri);
    // Akkoma's canonical field points at the quoted pinetree post; no FEP auth.
    assert_eq!(
        aq_doc["quoteUri"]
            .as_str()
            .map(|s| s.contains("pinetree.test")),
        Some(true)
    );
    assert!(
        aq_doc.get("quote").is_none(),
        "akkoma sends no FEP-044f quote"
    );
    assert!(aq_doc.get("quoteAuthorization").is_none());

    // pt5 quotes aq's baseline; aq receives it.
    let aq_local = poll("pt5 resolves aq baseline copy", || {
        let (s, v) = raw(
            &c,
            "POST",
            &format!("{}/api/notes/timeline", pt5.base),
            Some(&pt5.token),
            Some(json!({"limit": 80})),
        );
        (s == 200)
            .then(|| {
                v.as_array().and_then(|a| {
                    a.iter()
                        .find(|n| {
                            n["text"]
                                .as_str()
                                .is_some_and(|t| t.contains(&format!("baseline aq {tag}")))
                        })
                        .and_then(|n| n["id"].as_str().map(str::to_string))
                })
            })
            .flatten()
    });
    let (s, v) = raw(
        &c,
        "POST",
        &format!("{}/api/notes/create", pt5.base),
        Some(&pt5.token),
        Some(json!({"renoteId": aq_local, "text": format!("pt5 quotes aq {tag}")})),
    );
    assert_eq!(s, 200, "pt5 quotes aq: {v}");
    assert_eq!(
        v["createdNote"]["quoteAuthorizationStatus"].as_str(),
        Some("pending")
    );
    poll("aq receives pt5 quote", || {
        timeline_texts(&c, aq)
            .iter()
            .any(|t| t.contains(&format!("pt5 quotes aq {tag}")))
            .then_some(())
    });

    println!("ELIXIR_QUOTE_FEDERATION_OK tag={tag} pq_post={pq_uri} pt5_quote={pt5_quote_id}");
}

/// Resolve an account on a Mastodon-API node and return its account id.
fn masto_search_account(c: &SafeHttpClient, n: &Node, acct: &str) -> Option<String> {
    let (s, v) = raw(
        &c,
        "GET",
        &format!(
            "{}/api/v2/search?q={}&resolve=true&limit=10",
            n.base,
            urlencoding(acct)
        ),
        Some(&n.token),
        None,
    );
    if s != 200 {
        return None;
    }
    v["accounts"].as_array()?.iter().find_map(|a| {
        let a_acct = a["acct"].as_str().unwrap_or("");
        (a_acct == acct || a_acct.split('@').next() == acct.split('@').next())
            .then(|| a["id"].as_str().map(str::to_string))
            .flatten()
    })
}

/// Resolve a known AP URI to the viewer's local status id (URI-keyed
/// lookup with `resolve=true`, which fetches when needed). Unlike text
/// search this does not depend on the full-text index, which lags freshly
/// federated posts by minutes under burst load.
fn masto_search_uri(c: &SafeHttpClient, n: &Node, ap_uri: &str) -> Option<String> {
    let (s, v) = raw(
        &c,
        "GET",
        &format!(
            "{}/api/v2/search?q={}&resolve=true&limit=10",
            n.base,
            urlencoding(ap_uri)
        ),
        Some(&n.token),
        None,
    );
    if s != 200 {
        return None;
    }
    v["statuses"]
        .as_array()?
        .iter()
        .find_map(|st| {
            let uri = st["uri"].as_str().unwrap_or("");
            (uri == ap_uri)
                .then(|| st["id"].as_str().map(str::to_string))
                .flatten()
        })
        .or_else(|| {
            v["statuses"]
                .as_array()?
                .first()?
                .get("id")?
                .as_str()
                .map(str::to_string)
        })
}
fn masto_search_status(c: &SafeHttpClient, n: &Node, text: &str) -> Option<String> {
    let (s, v) = raw(
        &c,
        "GET",
        &format!(
            "{}/api/v2/search?q={}&resolve=true&limit=20",
            n.base,
            urlencoding(text)
        ),
        Some(&n.token),
        None,
    );
    if s != 200 {
        return None;
    }
    v["statuses"].as_array()?.iter().find_map(|st| {
        let content = st["content"].as_str().unwrap_or("");
        content
            .contains(text)
            .then(|| st["id"].as_str().map(str::to_string))
            .flatten()
    })
}

fn tiny_png() -> Vec<u8> {
    // 1x1 transparent PNG.
    vec![
        0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44,
        0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1F,
        0x15, 0xC4, 0x89, 0x00, 0x00, 0x00, 0x0A, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9C, 0x63, 0x00,
        0x01, 0x00, 0x00, 0x05, 0x00, 0x01, 0x0D, 0x0A, 0x2D, 0xB4, 0x00, 0x00, 0x00, 0x00, 0x49,
        0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
    ]
}

// ---------------------------------------------------------------------------
// Stress: sustained quote + post fan-out across the pinetree/pleroma/akkoma
// triangle with follow churn underneath, then convergence assertions.
//
// Gate: PINETREE_ELIXIR_STRESS=1 (separate from the federation test so the
// two never share server state). Thresholds are modest by design (throwaway
// users, ~40 items); durations and counts print at the end for the log.
// Limitation: "both storage adapters" is redb-only here — the FoundationDB
// adapter is still an empty stub (Phase 4 scope), so there is no second
// adapter to stress.
// ---------------------------------------------------------------------------

use std::sync::{Arc, Barrier};

fn pt_post(
    c: &SafeHttpClient,
    base: &str,
    token: &str,
    text: &str,
    renote: Option<&str>,
) -> String {
    let mut body = json!({"text": text, "visibility": "public"});
    if let Some(r) = renote {
        body["renoteId"] = json!(r);
    }
    let (s, v) = raw(
        c,
        "POST",
        &format!("{base}/api/notes/create"),
        Some(token),
        Some(body),
    );
    assert_eq!(s, 200, "pt post {text}: {v}");
    v["createdNote"]["id"].as_str().expect("id").to_string()
}

fn pt_find_copy(c: &SafeHttpClient, base: &str, token: &str, text: &str) -> Option<String> {
    let (s, v) = raw(
        c,
        "POST",
        &format!("{base}/api/notes/timeline"),
        Some(token),
        Some(json!({"limit": 100})),
    );
    if s != 200 {
        return None;
    }
    v.as_array()?.iter().find_map(|n| {
        n["text"]
            .as_str()
            .and_then(|t| {
                t.contains(text)
                    .then(|| n["id"].as_str().map(str::to_string))
            })
            .flatten()
    })
}

fn count_occurrences(haystack: &[String], needle: &str, is_quote: bool) -> usize {
    // Quotes match by prefix (server-appended `RT:` fallback); bursts and
    // the reply match exactly (a quote's text embeds its target's needle,
    // so substring counting would false-positive on every quoted burst).
    haystack
        .iter()
        .filter(|t| {
            let t = t.trim();
            if is_quote {
                t.starts_with(needle)
            } else {
                t == needle
            }
        })
        .count()
}

#[test]
fn elixir_quote_stress() {
    if std::env::var("PINETREE_ELIXIR_STRESS").ok().as_deref() != Some("1") {
        eprintln!("elixir_quote_stress: skipped (set PINETREE_ELIXIR_STRESS=1)");
        return;
    }
    let c = client();
    let tag = format!(
        "st{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    );
    let nodes = sign_in_all(&c);
    let (pq, aq, pt5) = (node(&nodes, "pq"), node(&nodes, "aq"), node(&nodes, "pt5"));
    // Spare churn pair (dedicated users; carries no assertions).
    let (s, v) = raw(
        &c,
        "POST",
        "http://pinetree.test/api/signup",
        None,
        Some(json!({"username": "pt6", "password": "password123"})),
    );
    assert!((200..300).contains(&s) || s == 400, "pt6 signup: {v}");
    let (s, v) = raw(
        &c,
        "POST",
        "http://pinetree.test/api/signin",
        None,
        Some(json!({"username": "pt6", "password": "password123"})),
    );
    assert_eq!(s, 200, "pt6 signin: {v}");
    let pt6_token = v["token"].as_str().expect("token").to_string();
    let pt6_id = v["id"].as_str().expect("id").to_string();
    // pq2 on pleroma via admin API: create through pleroma_ctl is shell-only;
    // reuse aq as the churn peer instead (churn edge pt6<->aq carries no
    // assertions, so aq's main-triangle assertions stay on the pt5 edge).
    let t0 = Instant::now();

    // Stable triangle follows (idempotent; already established by the
    // federation test, re-asserted here).
    for (v, target_acct) in [(pq, "pt5@pinetree.test"), (aq, "pt5@pinetree.test")] {
        let remote = poll(&format!("stress {} resolves {target_acct}", v.name), || {
            masto_search_account(&c, v, target_acct)
        });
        poll(&format!("stress {} follows {target_acct}", v.name), || {
            let (s, r) = raw(
                &c,
                "POST",
                &format!("{}/api/v1/accounts/{}/follow", v.base, remote),
                Some(&v.token),
                Some(json!({})),
            );
            ((200..300).contains(&s)
                || s == 400
                || r.to_string().to_lowercase().contains("already"))
            .then_some(())
        });
    }
    let pt5_remote_pq = poll("stress pt5 resolves pq", || {
        resolve(&c, pt5, "pq@pleroma.test")
    });
    let pt5_remote_aq = poll("stress pt5 resolves aq", || {
        resolve(&c, pt5, "aq@akkoma.test")
    });
    for rid in [
        pt5_remote_pq["id"].as_str().unwrap(),
        pt5_remote_aq["id"].as_str().unwrap(),
    ] {
        poll("stress pt5 follows elixir", || {
            let (s, r) = raw(
                &c,
                "POST",
                &format!("{}/api/following/create", pt5.base),
                Some(&pt5.token),
                Some(json!({"userId": rid})),
            );
            ((200..300).contains(&s) || (s == 400 && r.to_string().contains("ALREADY_FOLLOWING")))
                .then_some(())
        });
    }

    // Full mesh: convergence asserts every item on every node, so the
    // elixir pair needs its own edges too (the interop test only builds
    // pinetree<->each). Verified via relationships, not just the call.
    for (follower, target_acct) in [(aq, "pq@pleroma.test"), (pq, "aq@akkoma.test")] {
        let remote = poll(
            &format!("stress {} resolves {target_acct} for mesh", follower.name),
            || masto_search_account(&c, follower, target_acct),
        );
        poll(
            &format!("stress {} follows {target_acct}", follower.name),
            || {
                let (s, r) = raw(
                    &c,
                    "POST",
                    &format!("{}/api/v1/accounts/{}/follow", follower.base, remote),
                    Some(&follower.token),
                    Some(json!({})),
                );
                ((200..300).contains(&s)
                    || s == 400
                    || r.to_string().to_lowercase().contains("already"))
                .then_some(())
            },
        );
        let remote_again = remote.clone();
        poll(
            &format!("stress {} edge to {target_acct} established", follower.name),
            || {
                let (s, v2) = raw(
                    &c,
                    "GET",
                    &format!(
                        "{}/api/v1/accounts/relationships?id[]={}",
                        follower.base, remote_again
                    ),
                    Some(&follower.token),
                    None,
                );
                (s == 200)
                    .then(|| {
                        v2.as_array()?
                            .first()
                            .and_then(|r| (r["following"].as_bool() == Some(true)).then_some(()))
                    })
                    .flatten()
            },
        );
    }

    // Phase A: parallel text bursts (3 threads x 5 posts).
    // Barrier parties = 3 burst threads + 1 churn thread + main.
    let barrier = Arc::new(Barrier::new(5));
    let mut texts: Vec<String> = Vec::new();
    for i in 0..5 {
        texts.push(format!("{tag} burst-pq-{i}"));
        texts.push(format!("{tag} burst-aq-{i}"));
        texts.push(format!("{tag} burst-pt5-{i}"));
    }
    let mut handles = Vec::new();
    for (name, base, token, kind) in [
        (
            "pq",
            "http://pleroma.test".to_string(),
            pq.token.clone(),
            "masto",
        ),
        (
            "aq",
            "http://akkoma.test".to_string(),
            aq.token.clone(),
            "masto",
        ),
        (
            "pt5",
            "http://pinetree.test".to_string(),
            pt5.token.clone(),
            "pt",
        ),
    ] {
        let b = barrier.clone();
        let texts = texts.clone();
        handles.push(std::thread::spawn(move || {
            let c = client();
            b.wait();
            let mine: Vec<String> = texts
                .into_iter()
                .filter(|t| t.contains(&format!("burst-{name}-")))
                .collect();
            // Return (text, canonical AP URI) so Phase B resolves targets by
            // URI (no full-text-index lag) instead of searching by text.
            let mut posted: Vec<(String, String)> = Vec::new();
            for t in mine {
                let uri = if kind == "masto" {
                    let (s, v) = raw(
                        &c,
                        "POST",
                        &format!("{base}/api/v1/statuses"),
                        Some(&token),
                        Some(json!({"status": t})),
                    );
                    assert_eq!(s, 200, "burst post: {v}");
                    v["uri"].as_str().expect("posted status uri").to_string()
                } else {
                    let id = pt_post(&c, &base, &token, &t, None);
                    format!("http://pinetree.test/notes/{id}")
                };
                posted.push((t, uri));
                sleep(Duration::from_millis(200));
            }
            posted
        }));
    }
    // Churn thread: pt6 <-> aq follow/unfollow cycles underneath the burst.
    let churn = {
        let b = barrier.clone();
        std::thread::spawn(move || {
            let c = client();
            let aq_remote = poll("churn pt6 resolves aq", || {
                let (s, v) = raw(
                    &c,
                    "POST",
                    "http://pinetree.test/api/users/show",
                    Some(&pt6_token),
                    Some(json!({"username": "aq", "host": "akkoma.test"})),
                );
                (s == 200)
                    .then(|| v["id"].as_str().map(str::to_string))
                    .flatten()
            });
            b.wait();
            let mut cycles = 0;
            for _ in 0..4 {
                let (s, _) = raw(
                    &c,
                    "POST",
                    "http://pinetree.test/api/following/create",
                    Some(&pt6_token),
                    Some(json!({"userId": aq_remote})),
                );
                if (200..300).contains(&s) {
                    cycles += 1;
                }
                sleep(Duration::from_millis(800));
                let _ = raw(
                    &c,
                    "POST",
                    "http://pinetree.test/api/following/delete",
                    Some(&pt6_token),
                    Some(json!({"userId": aq_remote})),
                );
                sleep(Duration::from_millis(800));
            }
            // End followed so no dangling un-followed state surprises later runs.
            let _ = raw(
                &c,
                "POST",
                "http://pinetree.test/api/following/create",
                Some(&pt6_token),
                Some(json!({"userId": aq_remote})),
            );
            cycles
        })
    };
    barrier.wait();
    let mut burst_uris: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for h in handles {
        for (t, u) in h.join().expect("burst thread") {
            burst_uris.insert(t, u);
        }
    }
    let churn_cycles = churn.join().expect("churn thread");
    let dt_burst = t0.elapsed();
    println!("[stress] phase A: 15 posts + {churn_cycles} churn cycles in {dt_burst:?}");

    // Phase B: quotes of phase-A posts (sequential for target resolution).
    let t1 = Instant::now();
    let mut quote_texts: Vec<String> = Vec::new();
    // pt5 quotes 2 pq + 1 aq posts.
    for (i, needle) in [
        format!("{tag} burst-pq-0"),
        format!("{tag} burst-pq-1"),
        format!("{tag} burst-aq-0"),
    ]
    .into_iter()
    .enumerate()
    {
        let copy = poll(&format!("pt5 finds {needle}"), || {
            pt_find_copy(&c, &pt5.base, &pt5.token, &needle)
        });
        let qt = format!("{tag} quote-pt5-{i} of {needle}");
        pt_post(&c, &pt5.base, &pt5.token, &qt, Some(&copy));
        quote_texts.push(qt);
    }
    // pq quotes 2 pt5 posts; aq quotes 1 pt5 + 1 pq post. Targets resolve
    // by AP URI (collected at post time), never by text search.
    for (who, needle, qi) in [
        (pq, format!("{tag} burst-pt5-0"), 0),
        (pq, format!("{tag} burst-pt5-1"), 1),
        (aq, format!("{tag} burst-pt5-2"), 2),
        (aq, format!("{tag} burst-pq-2"), 3),
    ] {
        let ap_uri = burst_uris
            .get(&needle)
            .unwrap_or_else(|| panic!("no uri for {needle}"));
        let target = poll(&format!("{} resolves {needle}", who.name), || {
            masto_search_uri(&c, who, ap_uri)
        });
        let qt = format!("{tag} quote-{qi} of {needle}");
        masto_quote(&c, who, &qt, &target);
        quote_texts.push(qt);
    }
    // Reply burst: every node answers a remote post (texts unique per tag;
    // replies render verbatim everywhere, so they join `expected` for exact
    // matching — never the prefix set). Targets resolve the same way Phase
    // B quotes do: pt5 via its own timeline copy, elixir nodes by AP URI
    // (never text search — the Elixir full-text index lags bursts by
    // minutes).
    let mut reply_texts: Vec<String> = Vec::new();
    // pt5 answers pq's burst-pq-3 (kept from the original mix) and aq's
    // burst-aq-1.
    for needle in [format!("{tag} burst-pq-3"), format!("{tag} burst-aq-1")] {
        let target = poll(&format!("pt5 finds reply target {needle}"), || {
            pt_find_copy(&c, &pt5.base, &pt5.token, &needle)
        });
        let rt = if needle.contains("burst-pq-3") {
            format!("{tag} reply-pt5")
        } else {
            format!("{tag} reply-pt5-aq1")
        };
        let (s, v) = raw(
            &c,
            "POST",
            &format!("{}/api/notes/create", pt5.base),
            Some(&pt5.token),
            Some(json!({"text": rt.clone(), "replyId": target})),
        );
        assert_eq!(s, 200, "pt5 reply {rt}: {v}");
        reply_texts.push(rt);
    }
    // pq answers pt5's burst-pt5-0; aq answers pt5's burst-pt5-3 and pq's
    // burst-pq-4 (the aq->pq edge exercises the elixir mesh directly).
    for (who, needle, reply_tag) in [
        (pq, format!("{tag} burst-pt5-0"), "reply-pq-pt50"),
        (aq, format!("{tag} burst-pt5-3"), "reply-aq-pt53"),
        (aq, format!("{tag} burst-pq-4"), "reply-aq-pq4"),
    ] {
        let ap_uri = burst_uris
            .get(&needle)
            .unwrap_or_else(|| panic!("no uri for {needle}"));
        let target = poll(
            &format!("{} resolves reply target {needle}", who.name),
            || masto_search_uri(&c, who, ap_uri),
        );
        let rt = format!("{tag} {reply_tag}");
        let (s, v) = raw(
            &c,
            "POST",
            &format!("{}/api/v1/statuses", who.base),
            Some(&who.token),
            Some(json!({"status": rt.clone(), "in_reply_to_id": target})),
        );
        assert_eq!(s, 200, "{} reply {rt}: {v}", who.name);
        reply_texts.push(rt);
    }
    // A fourth pt5 quote (of pq's burst-pq-3) rounds out the mix. (Deliberately
    // not a pure renote/Announce: Pleroma silently absorbs a remote Announce
    // of a local post — inbox 200, never rendered — while Akkoma renders the
    // identical shape, so Announce visibility is peer-implementation-defined
    // and must not gate our suite. Announce delivery itself is covered by
    // unit tests, the 4-server renote phase, and the observed Akkoma render.)
    let copy3 = poll("pt5 finds renote target", || {
        pt_find_copy(&c, &pt5.base, &pt5.token, &format!("{tag} burst-pq-3"))
    });
    let qt3 = format!("{tag} quote-pt5-3 of {tag} burst-pq-3");
    pt_post(&c, &pt5.base, &pt5.token, &qt3, Some(&copy3));
    quote_texts.push(qt3);
    let dt_quotes = t1.elapsed();
    println!(
        "[stress] phase B: {} quotes + {} replies in {dt_quotes:?}",
        quote_texts.len(),
        reply_texts.len()
    );

    // Convergence: every node shows every burst text + every quote text +
    // every reply text.
    let t2 = Instant::now();
    let mut expected = texts.clone();
    expected.extend(quote_texts.clone());
    expected.extend(reply_texts.clone());
    // Bursts and replies render verbatim everywhere; elixir-authored
    // quotes carry their server-appended inline fallback (`RT: <url>`,
    // glued without a space after tag-stripping), so quotes match by
    // prefix while bursts/replies match exactly (a quote's text embeds its
    // target's needle, so substring matching would let a quote mask a
    // missing burst — hence split lists, not `contains`).
    let quote_set: std::collections::HashSet<&str> =
        quote_texts.iter().map(String::as_str).collect();
    for n in [&nodes[0], &nodes[1], &nodes[2]] {
        poll_with(
            &format!("{} converged on {} items", n.name, expected.len()),
            Duration::from_secs(600),
            || {
                let got = timeline_texts(&c, n);
                expected
                    .iter()
                    .all(|t| {
                        got.iter().any(|g| {
                            let g = g.trim();
                            if quote_set.contains(t.as_str()) {
                                g.starts_with(t.as_str())
                            } else {
                                g == t
                            }
                        })
                    })
                    .then_some(())
            },
        );
    }
    let dt_conv = t2.elapsed();

    // No duplicates from redelivery: each unique text appears exactly once
    // per viewer timeline (quotes carry distinct texts from their targets,
    // so the exact/prefix split from the convergence loop applies here too).
    let mut dups = 0;
    for n in [&nodes[0], &nodes[1], &nodes[2]] {
        let got = timeline_texts(&c, n);
        for t in expected.iter() {
            let k = count_occurrences(&got, t, quote_set.contains(t.as_str()));
            if k != 1 {
                println!("[stress] DUP on {}: {t} x{k}", n.name);
                dups += 1;
            }
        }
    }
    assert_eq!(dups, 0, "no duplicate deliveries");
    println!(
        "[stress] STRESS_OK items={} nodes=3 duplicates={dups} burst={dt_burst:?} quotes={dt_quotes:?} converge={dt_conv:?} total={:?}",
        expected.len(),
        t0.elapsed()
    );
}
