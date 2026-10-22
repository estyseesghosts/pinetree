//! Phase 2 Deliverable 4: live four-server interop. Real Misskey
//! (`./misskey`), real Sharkey (`./sharkey`) and two `pinetree` binaries
//! federate over loopback.
//!
//! This test needs the harness brought up by `scripts/smoke-phase2.sh` (or by
//! hand per `docs/phase-2-interop-howto.md`); without `PINETREE_INTEROP=1` it
//! prints a skip notice and passes, so plain `cargo test` stays hermetic.
//!
//! Nodes live on distinct loopback IPs, port 80 (Misskey's WebFinger builder
//! drops URL ports): mk `127.0.1.1`, sk `127.0.1.2`, p3 `127.0.1.3`,
//! p4 `127.0.1.4`. One user per node (`mk1`, `sk1`, `pt3`, `pt4`,
//! password `password123`). Flow: resolve all pairs, 12 follows + accepts,
//! public notes, reactions (ring), renotes (ring+2), undo reactions, undo
//! renotes, delete notes, unfollow all. Every cross-node assertion polls to a
//! deadline.

use std::thread::sleep;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Misskey,
    Sharkey,
    Pinetree,
}

struct Node {
    name: &'static str,
    base: String,
    host: String,
    username: &'static str,
    kind: Kind,
    token: String,
    user_id: String,
}

const ORDER: [&str; 4] = ["mk", "sk", "p3", "p4"];
const DEADLINE: Duration = Duration::from_secs(90);

fn client() -> SafeHttpClient {
    SafeHttpClient::new(SafeHttpConfig {
        allow_private_ips: true,
        timeout: Duration::from_secs(60),
        ..Default::default()
    })
}

fn call(
    c: &SafeHttpClient,
    base: &str,
    ep: &str,
    token: Option<&str>,
    body: &Value,
) -> (u16, Value) {
    let bytes = serde_json::to_vec(body).expect("encode");
    let bearer = token.map(|t| format!("Bearer {t}"));
    let mut headers: Vec<(&str, &str)> = vec![
        ("Content-Type", "application/json"),
        ("Accept", "application/json"),
    ];
    if let Some(b) = bearer.as_ref() {
        headers.push(("Authorization", b.as_str()));
    }
    let url = format!("{base}/api/{ep}");
    // Misskey-style rate limits (429 + retryAfter) are honoured like a client would.
    for _ in 0..20 {
        let resp = c
            .send("POST", &url, &headers, Some(&bytes), 8 * 1024 * 1024)
            .unwrap_or_else(|e| panic!("POST {url}: {e}"));
        let value: Value = serde_json::from_slice(&resp.body).unwrap_or(Value::Null);
        if resp.status == 429 {
            let wait = value["error"]["info"]["retryAfter"].as_u64().unwrap_or(1);
            sleep(Duration::from_secs(wait.clamp(1, 10)));
            continue;
        }
        return (resp.status, value);
    }
    panic!("POST {url}: still rate limited after retries");
}

fn poll<T>(desc: &str, mut f: impl FnMut() -> Option<T>) -> T {
    let start = Instant::now();
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(start.elapsed() < DEADLINE, "timed out waiting for: {desc}");
        sleep(Duration::from_millis(500));
    }
}

fn sign_in(
    c: &SafeHttpClient,
    name: &'static str,
    base: &str,
    host: &str,
    username: &'static str,
    kind: Kind,
) -> Node {
    let (ep, field) = match kind {
        Kind::Pinetree => ("signin", "token"),
        _ => ("signin-flow", "i"),
    };
    let (status, v) = call(
        c,
        base,
        ep,
        None,
        &json!({"username": username, "password": "password123"}),
    );
    assert_eq!(status, 200, "{name} sign-in: {v}");
    let token = v[field]
        .as_str()
        .unwrap_or_else(|| panic!("{name}: no {field} in {v}"))
        .to_string();
    let user_id = match kind {
        Kind::Pinetree => v["id"].as_str().expect("pt id").to_string(),
        _ => v["id"].as_str().expect("id").to_string(),
    };
    Node {
        name,
        base: base.to_string(),
        host: host.to_string(),
        username,
        kind,
        token,
        user_id,
    }
}

fn node<'a>(nodes: &'a [Node], name: &str) -> &'a Node {
    nodes.iter().find(|n| n.name == name).expect("node")
}

/// Own follower count (works on all three implementations).
fn followers_count(c: &SafeHttpClient, n: &Node) -> i64 {
    let (s, v) = call(
        c,
        &n.base,
        "users/show",
        Some(&n.token),
        &json!({"userId": n.user_id}),
    );
    assert_eq!(s, 200, "{} users/show self: {v}", n.name);
    v["followersCount"].as_i64().unwrap_or(-1)
}

/// Locate the viewer-local copy of `target`'s note with `text`.
fn find_note(
    c: &SafeHttpClient,
    viewer: &Node,
    target: &Node,
    target_id: &str,
    text: &str,
) -> Option<Value> {
    // pinetree lacks `users/notes`; its home timeline carries followed notes.
    let (s, v) = match viewer.kind {
        Kind::Pinetree => call(
            c,
            &viewer.base,
            "notes/timeline",
            Some(&viewer.token),
            &json!({"limit": 60}),
        ),
        _ => call(
            c,
            &viewer.base,
            "users/notes",
            Some(&viewer.token),
            &json!({"userId": target_id, "limit": 30}),
        ),
    };
    if s != 200 {
        return None;
    }
    v.as_array()?
        .iter()
        .find(|n| {
            n["text"].as_str().is_some_and(|t| t.contains(text))
                && n["user"]["username"].as_str() == Some(target.username)
        })
        .cloned()
}

fn show_note(c: &SafeHttpClient, n: &Node, id: &str) -> (u16, Value) {
    call(
        c,
        &n.base,
        "notes/show",
        Some(&n.token),
        &json!({"noteId": id}),
    )
}

/// Number of renotes of `id`, read from the owning node.
fn renote_count(c: &SafeHttpClient, n: &Node, id: &str) -> usize {
    match n.kind {
        // Misskey does not decrement `renoteCount` when an inbound Undo
        // deletes a remote renote; its renote list is the reliable oracle.
        Kind::Misskey | Kind::Sharkey => {
            let (s, v) = call(
                c,
                &n.base,
                "notes/renotes",
                Some(&n.token),
                &json!({"noteId": id, "limit": 50}),
            );
            assert_eq!(s, 200, "{} notes/renotes: {v}", n.name);
            v.as_array().map_or(0, Vec::len)
        }
        Kind::Pinetree => show_note(c, n, id).1["renoteCount"].as_u64().unwrap_or(0) as usize,
    }
}

#[test]
fn federation_interop_four_servers() {
    if std::env::var("PINETREE_INTEROP").ok().as_deref() != Some("1") {
        eprintln!(
            "federation_interop_four_servers: skipped (set PINETREE_INTEROP=1; see scripts/smoke-phase2.sh)"
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

    let nodes = vec![
        sign_in(
            &c,
            "mk",
            "http://127.0.1.1",
            "127.0.1.1",
            "mk1",
            Kind::Misskey,
        ),
        sign_in(
            &c,
            "sk",
            "http://127.0.1.2",
            "127.0.1.2",
            "sk1",
            Kind::Sharkey,
        ),
        sign_in(
            &c,
            "p3",
            "http://127.0.1.3",
            "127.0.1.3",
            "pt3",
            Kind::Pinetree,
        ),
        sign_in(
            &c,
            "p4",
            "http://127.0.1.4",
            "127.0.1.4",
            "pt4",
            Kind::Pinetree,
        ),
    ];

    // --- resolve every remote user from every node -------------------------
    let mut remote_id = std::collections::HashMap::new();
    for v in ORDER {
        for t in ORDER {
            if v == t {
                continue;
            }
            let (vn, tn) = (node(&nodes, v), node(&nodes, t));
            let id = poll(&format!("{v} resolves {t}"), || {
                let (s, r) = call(
                    &c,
                    &vn.base,
                    "users/show",
                    Some(&vn.token),
                    &json!({"username": tn.username, "host": tn.host}),
                );
                (s == 200)
                    .then(|| r["id"].as_str().map(str::to_string))
                    .flatten()
            });
            remote_id.insert((v, t), id);
        }
    }

    // --- follow matrix: all 12 edges become Accepted ------------------------
    for v in ORDER {
        for t in ORDER {
            if v == t {
                continue;
            }
            let vn = node(&nodes, v);
            let (s, r) = call(
                &c,
                &vn.base,
                "following/create",
                Some(&vn.token),
                &json!({"userId": remote_id[&(v, t)]}),
            );
            let already = s == 400 && r.to_string().contains("ALREADY_FOLLOWING");
            assert!(s == 200 || already, "{v} follows {t}: {s} {r}");
        }
    }
    for t in ORDER {
        let tn = node(&nodes, t);
        poll(&format!("{t} has 3 followers"), || {
            (followers_count(&c, tn) == 3).then_some(())
        });
    }

    // --- public notes reach all three other nodes ---------------------------
    let mut own_note = std::collections::HashMap::new();
    for t in ORDER {
        let tn = node(&nodes, t);
        let (s, r) = call(
            &c,
            &tn.base,
            "notes/create",
            Some(&tn.token),
            &json!({"text": format!("interop {t} {tag}"), "visibility": "public"}),
        );
        assert_eq!(s, 200, "{t} notes/create: {r}");
        own_note.insert(
            t,
            r["createdNote"]["id"]
                .as_str()
                .expect("note id")
                .to_string(),
        );
    }
    let mut local_copy = std::collections::HashMap::new();
    for v in ORDER {
        for t in ORDER {
            if v == t {
                continue;
            }
            let (vn, tn) = (node(&nodes, v), node(&nodes, t));
            let text = format!("interop {t} {tag}");
            let note = poll(&format!("{v} receives note of {t}"), || {
                find_note(&c, vn, tn, &remote_id[&(v, t)], &text)
            });
            local_copy.insert((v, t), note["id"].as_str().expect("id").to_string());
        }
    }

    // --- reactions: v reacts to next's note; owner sees it ------------------
    for (i, v) in ORDER.iter().enumerate() {
        let t = ORDER[(i + 1) % 4];
        let vn = node(&nodes, v);
        let (s, r) = call(
            &c,
            &vn.base,
            "notes/reactions/create",
            Some(&vn.token),
            &json!({"noteId": local_copy[&(*v, t)], "reaction": "👍"}),
        );
        assert!((200..300).contains(&s), "{v} reacts to {t}: {s} {r}");
    }
    for (i, _v) in ORDER.iter().enumerate() {
        let t = ORDER[(i + 1) % 4];
        let tn = node(&nodes, t);
        poll(&format!("{t} sees the reaction"), || {
            let (s, r) = show_note(&c, tn, &own_note[t]);
            (s == 200 && r["reactions"]["👍"].as_u64().unwrap_or(0) >= 1).then_some(())
        });
    }

    // --- renotes: v renotes the note of ORDER[i+2] --------------------------
    let mut renote_id = std::collections::HashMap::new();
    for (i, v) in ORDER.iter().enumerate() {
        let t = ORDER[(i + 2) % 4];
        let vn = node(&nodes, v);
        let (s, r) = call(
            &c,
            &vn.base,
            "notes/create",
            Some(&vn.token),
            &json!({"renoteId": local_copy[&(*v, t)]}),
        );
        assert_eq!(s, 200, "{v} renotes {t}: {r}");
        renote_id.insert(
            *v,
            r["createdNote"]["id"]
                .as_str()
                .expect("renote id")
                .to_string(),
        );
    }
    for (i, _v) in ORDER.iter().enumerate() {
        let t = ORDER[(i + 2) % 4];
        let tn = node(&nodes, t);
        poll(&format!("{t} sees 1 renote"), || {
            (renote_count(&c, tn, &own_note[t]) == 1).then_some(())
        });
    }

    // --- quotes: create/receive/render-shape + legacy fallback + auth cycle
    // Fresh quotable notes (the renote phase above leaves its notes live,
    // but dedicated notes keep quote assertions independent).
    let qtag = format!("quotable {tag}");
    let mut quotable_own = std::collections::HashMap::new();
    for t in ORDER {
        let tn = node(&nodes, t);
        let (s, r) = call(
            &c,
            &tn.base,
            "notes/create",
            Some(&tn.token),
            &json!({"text": format!("{qtag} {t}"), "visibility": "public"}),
        );
        assert_eq!(s, 200, "{t} quotable notes/create: {r}");
        quotable_own.insert(
            t,
            r["createdNote"]["id"]
                .as_str()
                .expect("note id")
                .to_string(),
        );
    }
    let mut quotable_copy = std::collections::HashMap::new();
    for v in ORDER {
        for t in ORDER {
            if v == t {
                continue;
            }
            let (vn, tn) = (node(&nodes, v), node(&nodes, t));
            let text = format!("{qtag} {t}");
            let note = poll(&format!("{v} receives quotable of {t}"), || {
                find_note(&c, vn, tn, &remote_id[&(v, t)], &text)
            });
            quotable_copy.insert((v, t), note["id"].as_str().expect("id").to_string());
        }
    }

    // p3 (pinetree) quotes mk's note: Pending (Misskey speaks no FEP-044f),
    // relation intact, extension present.
    let p3 = node(&nodes, "p3");
    let (s, r) = call(
        &c,
        &p3.base,
        "notes/create",
        Some(&p3.token),
        &json!({"renoteId": quotable_copy[&("p3", "mk")], "text": format!("p3 quotes mk {tag}")}),
    );
    assert_eq!(s, 200, "p3 quotes mk: {r}");
    assert_eq!(
        r["createdNote"]["renoteId"].as_str(),
        Some(quotable_copy[&("p3", "mk")].as_str())
    );
    assert_eq!(
        r["createdNote"]["quoteAuthorizationStatus"].as_str(),
        Some("pending")
    );

    // mk receives p3's quote via the legacy fallback fields and renders it
    // as a renote (pinetree -> Misskey direction).
    let mk = node(&nodes, "mk");
    poll("mk renders p3's quote with a renote", || {
        let (s, r) = call(
            &c,
            &mk.base,
            "users/notes",
            Some(&mk.token),
            &json!({"userId": remote_id[&("mk", "p3")], "limit": 30}),
        );
        (s == 200)
            .then(|| {
                r.as_array().and_then(|a| {
                    a.iter()
                        .find(|n| {
                            n["text"]
                                .as_str()
                                .is_some_and(|t| t.contains(&format!("p3 quotes mk {tag}")))
                                && !n["renoteId"].is_null()
                        })
                        .cloned()
                })
            })
            .flatten()
    });

    // mk quotes p3's note through the Misskey API (legacy-only: no FEP
    // fields, no auth artifacts). p3 must recognize it as an unapproved
    // quote — never dropped, never rejected.
    let (s, r) = call(
        &c,
        &mk.base,
        "notes/create",
        Some(&mk.token),
        &json!({"renoteId": quotable_copy[&("mk", "p3")], "text": format!("mk quotes p3 {tag}")}),
    );
    assert_eq!(s, 200, "mk quotes p3: {r}");
    poll("p3 recognizes mk's legacy-only quote", || {
        let (s, r) = call(
            &c,
            &p3.base,
            "notes/timeline",
            Some(&p3.token),
            &json!({"limit": 60}),
        );
        (s == 200)
            .then(|| {
                r.as_array().and_then(|a| {
                    a.iter()
                        .find(|n| {
                            n["text"]
                                .as_str()
                                .is_some_and(|t| t.contains(&format!("mk quotes p3 {tag}")))
                                && !n["renoteId"].is_null()
                                && n["quoteAuthorizationStatus"].as_str() == Some("unapproved")
                        })
                        .cloned()
                })
            })
            .flatten()
    });

    // sk (Sharkey triple-emit) quotes p4's note; p4 recognizes it the same way.
    let sk = node(&nodes, "sk");
    let p4 = node(&nodes, "p4");
    let (s, r) = call(
        &c,
        &sk.base,
        "notes/create",
        Some(&sk.token),
        &json!({"renoteId": quotable_copy[&("sk", "p4")], "text": format!("sk quotes p4 {tag}")}),
    );
    assert_eq!(s, 200, "sk quotes p4: {r}");
    poll("p4 recognizes sk's legacy-only quote", || {
        let (s, r) = call(
            &c,
            &p4.base,
            "notes/timeline",
            Some(&p4.token),
            &json!({"limit": 60}),
        );
        (s == 200)
            .then(|| {
                r.as_array().and_then(|a| {
                    a.iter()
                        .find(|n| {
                            n["text"]
                                .as_str()
                                .is_some_and(|t| t.contains(&format!("sk quotes p4 {tag}")))
                                && !n["renoteId"].is_null()
                                && n["quoteAuthorizationStatus"].as_str() == Some("unapproved")
                        })
                        .cloned()
                })
            })
            .flatten()
    });

    // pinetree <-> pinetree full cycle: p4 quotes p3's note (Pending), p3
    // auto-approves the public post, p4 converges to Approved with a
    // dereferenceable stamp.
    let (s, r) = call(
        &c,
        &p4.base,
        "notes/create",
        Some(&p4.token),
        &json!({"renoteId": quotable_copy[&("p4", "p3")], "text": format!("p4 quotes p3 {tag}")}),
    );
    assert_eq!(s, 200, "p4 quotes p3: {r}");
    let p4_quote_id = r["createdNote"]["id"].as_str().expect("id").to_string();
    assert_eq!(
        r["createdNote"]["quoteAuthorizationStatus"].as_str(),
        Some("pending")
    );
    poll("p4's quote converges to approved", || {
        let (s, r) = show_note(&c, p4, &p4_quote_id);
        (s == 200 && r["quoteAuthorizationStatus"].as_str() == Some("approved")).then_some(())
    });

    // Outbound AP shape of the approved quote: canonical FEP-044f fields,
    // legacy aliases, exactly one RE: fallback, stamp present, and an
    // advisory policy with no manualApproval.
    {
        let (s, _) = show_note(&c, p4, &p4_quote_id);
        assert_eq!(s, 200);
        let note_uri = format!("http://127.0.1.4/notes/{p4_quote_id}");
        let resp = c
            .send(
                "GET",
                &note_uri,
                &[("Accept", "application/activity+json")],
                None,
                65_536,
            )
            .expect("fetch quote AP object");
        assert!(
            (200..300).contains(&resp.status),
            "quote AP fetch: {}",
            resp.status
        );
        let doc: Value = serde_json::from_slice(&resp.body).expect("quote AP JSON");
        let target_uri = format!("http://127.0.1.3/notes/{}", quotable_own["p3"]);
        // The cached copy may carry the copy id; resolve either form.
        let target_copy_uri = format!("http://127.0.1.4/notes/{}", quotable_copy[&("p4", "p3")]);
        let quoted = doc["quote"].as_str().unwrap_or("");
        assert!(
            quoted == target_uri || quoted == target_copy_uri,
            "canonical quote target: {quoted}"
        );
        assert_eq!(doc["_misskey_quote"].as_str(), Some(quoted));
        assert_eq!(doc["quoteUrl"].as_str(), Some(quoted));
        assert_eq!(doc["quoteUri"].as_str(), Some(quoted));
        assert!(
            doc["quoteAuthorization"]
                .as_str()
                .is_some_and(|s| !s.is_empty()),
            "stamp present once approved"
        );
        assert!(
            doc["interactionPolicy"]["canQuote"]
                .get("manualApproval")
                .is_none()
        );
        let content = doc["content"].as_str().unwrap_or("");
        assert_eq!(content.matches("RE:").count(), 1, "single textual fallback");
        let links = doc["tag"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter(|t| t.get("type").and_then(Value::as_str) == Some("Link"))
                    .count()
            })
            .unwrap_or(0);
        assert_eq!(links, 1, "single e232 Link tag");
        // The stamp dereferences to a QuoteAuthorization for this quote.
        let stamp_uri = doc["quoteAuthorization"].as_str().unwrap().to_string();
        let stamp = c
            .send(
                "GET",
                &stamp_uri,
                &[("Accept", "application/activity+json")],
                None,
                65_536,
            )
            .expect("fetch stamp");
        assert!(
            (200..300).contains(&stamp.status),
            "stamp fetch: {}",
            stamp.status
        );
        let stamp_doc: Value = serde_json::from_slice(&stamp.body).expect("stamp JSON");
        assert_eq!(stamp_doc["type"], "QuoteAuthorization");
    }

    // --- undo reactions ------------------------------------------------------
    for (i, v) in ORDER.iter().enumerate() {
        let t = ORDER[(i + 1) % 4];
        let vn = node(&nodes, v);
        let (s, r) = call(
            &c,
            &vn.base,
            "notes/reactions/delete",
            Some(&vn.token),
            &json!({"noteId": local_copy[&(*v, t)]}),
        );
        assert!((200..300).contains(&s), "{v} unreacts {t}: {s} {r}");
    }
    for (i, _v) in ORDER.iter().enumerate() {
        let t = ORDER[(i + 1) % 4];
        let tn = node(&nodes, t);
        poll(&format!("{t} sees reaction removed"), || {
            let (s, r) = show_note(&c, tn, &own_note[t]);
            (s == 200 && r["reactions"]["👍"].as_u64().unwrap_or(0) == 0).then_some(())
        });
    }

    // --- undo renotes --------------------------------------------------------
    for v in ORDER {
        let vn = node(&nodes, v);
        let (s, r) = call(
            &c,
            &vn.base,
            "notes/delete",
            Some(&vn.token),
            &json!({"noteId": renote_id[v]}),
        );
        assert!((200..300).contains(&s), "{v} deletes renote: {s} {r}");
    }
    for (i, _v) in ORDER.iter().enumerate() {
        let t = ORDER[(i + 2) % 4];
        let tn = node(&nodes, t);
        poll(&format!("{t} sees renote removed"), || {
            (renote_count(&c, tn, &own_note[t]) == 0).then_some(())
        });
    }

    // --- delete notes: remote copies disappear ------------------------------
    for t in ORDER {
        let tn = node(&nodes, t);
        let (s, r) = call(
            &c,
            &tn.base,
            "notes/delete",
            Some(&tn.token),
            &json!({"noteId": own_note[t]}),
        );
        assert!((200..300).contains(&s), "{t} deletes note: {s} {r}");
    }
    for v in ORDER {
        for t in ORDER {
            if v == t {
                continue;
            }
            let vn = node(&nodes, v);
            poll(&format!("{v} drops deleted note of {t}"), || {
                let (s, _) = show_note(&c, vn, &local_copy[&(v, t)]);
                (s >= 400).then_some(())
            });
        }
    }

    // --- unfollow everything -------------------------------------------------
    // Sharkey's followings cache (30 min lifetime) can miss the remote Accept;
    // a local follow event (sk1 follows its local buddy sk2) invalidates it.
    {
        let sk = node(&nodes, "sk");
        let (s, r) = call(
            &c,
            &sk.base,
            "users/show",
            Some(&sk.token),
            &json!({"username": "sk2"}),
        );
        assert_eq!(s, 200, "sk2 lookup: {r}");
        let sk2 = r["id"].as_str().expect("sk2 id").to_string();
        let _ = call(
            &c,
            &sk.base,
            "following/create",
            Some(&sk.token),
            &json!({"userId": sk2}),
        );
        let _ = call(
            &c,
            &sk.base,
            "following/delete",
            Some(&sk.token),
            &json!({"userId": sk2}),
        );
    }
    for v in ORDER {
        for t in ORDER {
            if v == t {
                continue;
            }
            let vn = node(&nodes, v);
            // Sharkey's `following/delete` consults an in-memory followings
            // cache that can lag a remote Accept; retry until it catches up.
            poll(&format!("{v} unfollows {t}"), || {
                let (s, r) = call(
                    &c,
                    &vn.base,
                    "following/delete",
                    Some(&vn.token),
                    &json!({"userId": remote_id[&(v, t)]}),
                );
                if (200..300).contains(&s) {
                    return Some(());
                }
                assert!(
                    vn.kind == Kind::Sharkey && r.to_string().contains("NOT_FOLLOWING"),
                    "{v} unfollows {t}: {s} {r}"
                );
                None
            });
        }
    }
    for t in ORDER {
        let tn = node(&nodes, t);
        poll(&format!("{t} has 0 followers"), || {
            (followers_count(&c, tn) == 0).then_some(())
        });
    }

    // --- locked account: a Misskey user's follow stays pending until accepted
    {
        let (mk, p3) = (node(&nodes, "mk"), node(&nodes, "p3"));
        let (s, r) = call(
            &c,
            &p3.base,
            "i/update",
            Some(&p3.token),
            &json!({"isLocked": true}),
        );
        assert_eq!(s, 200, "p3 lock: {r}");
        let (s, r) = call(
            &c,
            &mk.base,
            "following/create",
            Some(&mk.token),
            &json!({"userId": remote_id[&("mk", "p3")]}),
        );
        assert!(s == 200 || s == 204, "mk follows locked p3: {s} {r}");
        let mk_uid = poll("p3 lists mk's pending request", || {
            let (s, r) = call(
                &c,
                &p3.base,
                "following/requests/list",
                Some(&p3.token),
                &json!({}),
            );
            (s == 200)
                .then(|| {
                    r.as_array().and_then(|a| {
                        a.first()
                            .map(|e| e["follower"]["id"].as_str().unwrap_or("").to_string())
                    })
                })
                .flatten()
        });
        assert_eq!(
            followers_count(&c, p3),
            0,
            "pending request must not count as a follower"
        );
        let (s, r) = call(
            &c,
            &p3.base,
            "following/requests/accept",
            Some(&p3.token),
            &json!({"userId": mk_uid}),
        );
        assert!((200..300).contains(&s), "p3 accepts mk: {s} {r}");
        poll("p3 has 1 follower after accept", || {
            (followers_count(&c, p3) == 1).then_some(())
        });
        poll("mk unfollows locked p3", || {
            let (s, _) = call(
                &c,
                &mk.base,
                "following/delete",
                Some(&mk.token),
                &json!({"userId": remote_id[&("mk", "p3")]}),
            );
            (200..300).contains(&s).then_some(())
        });
        poll("p3 back to 0 followers", || {
            (followers_count(&c, p3) == 0).then_some(())
        });
    }
    println!("INTEROP_OK");
}
