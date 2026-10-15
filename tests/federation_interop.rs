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

fn call(c: &SafeHttpClient, base: &str, ep: &str, token: Option<&str>, body: &Value) -> (u16, Value) {
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

fn sign_in(c: &SafeHttpClient, name: &'static str, base: &str, host: &str, username: &'static str, kind: Kind) -> Node {
    let (ep, field) = match kind {
        Kind::Pinetree => ("signin", "token"),
        _ => ("signin-flow", "i"),
    };
    let (status, v) = call(c, base, ep, None, &json!({"username": username, "password": "password123"}));
    assert_eq!(status, 200, "{name} sign-in: {v}");
    let token = v[field].as_str().unwrap_or_else(|| panic!("{name}: no {field} in {v}")).to_string();
    let user_id = match kind {
        Kind::Pinetree => v["id"].as_str().expect("pt id").to_string(),
        _ => v["id"].as_str().expect("id").to_string(),
    };
    Node { name, base: base.to_string(), host: host.to_string(), username, kind, token, user_id }
}

fn node<'a>(nodes: &'a [Node], name: &str) -> &'a Node {
    nodes.iter().find(|n| n.name == name).expect("node")
}

/// Own follower count (works on all three implementations).
fn followers_count(c: &SafeHttpClient, n: &Node) -> i64 {
    let (s, v) = call(c, &n.base, "users/show", Some(&n.token), &json!({"userId": n.user_id}));
    assert_eq!(s, 200, "{} users/show self: {v}", n.name);
    v["followersCount"].as_i64().unwrap_or(-1)
}

/// Locate the viewer-local copy of `target`'s note with `text`.
fn find_note(c: &SafeHttpClient, viewer: &Node, target: &Node, target_id: &str, text: &str) -> Option<Value> {
    // pinetree lacks `users/notes`; its home timeline carries followed notes.
    let (s, v) = match viewer.kind {
        Kind::Pinetree => call(c, &viewer.base, "notes/timeline", Some(&viewer.token), &json!({"limit": 60})),
        _ => call(c, &viewer.base, "users/notes", Some(&viewer.token), &json!({"userId": target_id, "limit": 30})),
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
    call(c, &n.base, "notes/show", Some(&n.token), &json!({"noteId": id}))
}

/// Number of renotes of `id`, read from the owning node.
fn renote_count(c: &SafeHttpClient, n: &Node, id: &str) -> usize {
    match n.kind {
        // Misskey does not decrement `renoteCount` when an inbound Undo
        // deletes a remote renote; its renote list is the reliable oracle.
        Kind::Misskey | Kind::Sharkey => {
            let (s, v) = call(c, &n.base, "notes/renotes", Some(&n.token), &json!({"noteId": id, "limit": 50}));
            assert_eq!(s, 200, "{} notes/renotes: {v}", n.name);
            v.as_array().map_or(0, Vec::len)
        }
        Kind::Pinetree => show_note(c, n, id).1["renoteCount"].as_u64().unwrap_or(0) as usize,
    }
}

#[test]
fn federation_interop_four_servers() {
    if std::env::var("PINETREE_INTEROP").ok().as_deref() != Some("1") {
        eprintln!("federation_interop_four_servers: skipped (set PINETREE_INTEROP=1; see scripts/smoke-phase2.sh)");
        return;
    }
    let c = client();
    let tag = format!("{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs());

    let nodes = vec![
        sign_in(&c, "mk", "http://127.0.1.1", "127.0.1.1", "mk1", Kind::Misskey),
        sign_in(&c, "sk", "http://127.0.1.2", "127.0.1.2", "sk1", Kind::Sharkey),
        sign_in(&c, "p3", "http://127.0.1.3", "127.0.1.3", "pt3", Kind::Pinetree),
        sign_in(&c, "p4", "http://127.0.1.4", "127.0.1.4", "pt4", Kind::Pinetree),
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
                let (s, r) = call(&c, &vn.base, "users/show", Some(&vn.token), &json!({"username": tn.username, "host": tn.host}));
                (s == 200).then(|| r["id"].as_str().map(str::to_string)).flatten()
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
            let (s, r) = call(&c, &vn.base, "following/create", Some(&vn.token), &json!({"userId": remote_id[&(v, t)]}));
            let already = s == 400 && r.to_string().contains("ALREADY_FOLLOWING");
            assert!(s == 200 || already, "{v} follows {t}: {s} {r}");
        }
    }
    for t in ORDER {
        let tn = node(&nodes, t);
        poll(&format!("{t} has 3 followers"), || (followers_count(&c, tn) == 3).then_some(()));
    }

    // --- public notes reach all three other nodes ---------------------------
    let mut own_note = std::collections::HashMap::new();
    for t in ORDER {
        let tn = node(&nodes, t);
        let (s, r) = call(&c, &tn.base, "notes/create", Some(&tn.token), &json!({"text": format!("interop {t} {tag}"), "visibility": "public"}));
        assert_eq!(s, 200, "{t} notes/create: {r}");
        own_note.insert(t, r["createdNote"]["id"].as_str().expect("note id").to_string());
    }
    let mut local_copy = std::collections::HashMap::new();
    for v in ORDER {
        for t in ORDER {
            if v == t {
                continue;
            }
            let (vn, tn) = (node(&nodes, v), node(&nodes, t));
            let text = format!("interop {t} {tag}");
            let note = poll(&format!("{v} receives note of {t}"), || find_note(&c, vn, tn, &remote_id[&(v, t)], &text));
            local_copy.insert((v, t), note["id"].as_str().expect("id").to_string());
        }
    }

    // --- reactions: v reacts to next's note; owner sees it ------------------
    for (i, v) in ORDER.iter().enumerate() {
        let t = ORDER[(i + 1) % 4];
        let vn = node(&nodes, v);
        let (s, r) = call(&c, &vn.base, "notes/reactions/create", Some(&vn.token), &json!({"noteId": local_copy[&(*v, t)], "reaction": "👍"}));
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
        let (s, r) = call(&c, &vn.base, "notes/create", Some(&vn.token), &json!({"renoteId": local_copy[&(*v, t)]}));
        assert_eq!(s, 200, "{v} renotes {t}: {r}");
        renote_id.insert(*v, r["createdNote"]["id"].as_str().expect("renote id").to_string());
    }
    for (i, _v) in ORDER.iter().enumerate() {
        let t = ORDER[(i + 2) % 4];
        let tn = node(&nodes, t);
        poll(&format!("{t} sees 1 renote"), || (renote_count(&c, tn, &own_note[t]) == 1).then_some(()));
    }

    // --- undo reactions ------------------------------------------------------
    for (i, v) in ORDER.iter().enumerate() {
        let t = ORDER[(i + 1) % 4];
        let vn = node(&nodes, v);
        let (s, r) = call(&c, &vn.base, "notes/reactions/delete", Some(&vn.token), &json!({"noteId": local_copy[&(*v, t)]}));
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
        let (s, r) = call(&c, &vn.base, "notes/delete", Some(&vn.token), &json!({"noteId": renote_id[v]}));
        assert!((200..300).contains(&s), "{v} deletes renote: {s} {r}");
    }
    for (i, _v) in ORDER.iter().enumerate() {
        let t = ORDER[(i + 2) % 4];
        let tn = node(&nodes, t);
        poll(&format!("{t} sees renote removed"), || (renote_count(&c, tn, &own_note[t]) == 0).then_some(()));
    }

    // --- delete notes: remote copies disappear ------------------------------
    for t in ORDER {
        let tn = node(&nodes, t);
        let (s, r) = call(&c, &tn.base, "notes/delete", Some(&tn.token), &json!({"noteId": own_note[t]}));
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
        let (s, r) = call(&c, &sk.base, "users/show", Some(&sk.token), &json!({"username": "sk2"}));
        assert_eq!(s, 200, "sk2 lookup: {r}");
        let sk2 = r["id"].as_str().expect("sk2 id").to_string();
        let _ = call(&c, &sk.base, "following/create", Some(&sk.token), &json!({"userId": sk2}));
        let _ = call(&c, &sk.base, "following/delete", Some(&sk.token), &json!({"userId": sk2}));
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
                let (s, r) = call(&c, &vn.base, "following/delete", Some(&vn.token), &json!({"userId": remote_id[&(v, t)]}));
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
        poll(&format!("{t} has 0 followers"), || (followers_count(&c, tn) == 0).then_some(()));
    }

    // --- locked account: a Misskey user's follow stays pending until accepted
    {
        let (mk, p3) = (node(&nodes, "mk"), node(&nodes, "p3"));
        let (s, r) = call(&c, &p3.base, "i/update", Some(&p3.token), &json!({"isLocked": true}));
        assert_eq!(s, 200, "p3 lock: {r}");
        let (s, r) = call(&c, &mk.base, "following/create", Some(&mk.token), &json!({"userId": remote_id[&("mk", "p3")]}));
        assert!(s == 200 || s == 204, "mk follows locked p3: {s} {r}");
        let mk_uid = poll("p3 lists mk's pending request", || {
            let (s, r) = call(&c, &p3.base, "following/requests/list", Some(&p3.token), &json!({}));
            (s == 200).then(|| r.as_array().and_then(|a| a.first().map(|e| e["follower"]["id"].as_str().unwrap_or("").to_string()))).flatten()
        });
        assert_eq!(followers_count(&c, p3), 0, "pending request must not count as a follower");
        let (s, r) = call(&c, &p3.base, "following/requests/accept", Some(&p3.token), &json!({"userId": mk_uid}));
        assert!((200..300).contains(&s), "p3 accepts mk: {s} {r}");
        poll("p3 has 1 follower after accept", || (followers_count(&c, p3) == 1).then_some(()));
        poll("mk unfollows locked p3", || {
            let (s, _) = call(&c, &mk.base, "following/delete", Some(&mk.token), &json!({"userId": remote_id[&("mk", "p3")]}));
            (200..300).contains(&s).then_some(())
        });
        poll("p3 back to 0 followers", || (followers_count(&c, p3) == 0).then_some(()));
    }
    println!("INTEROP_OK");
}
