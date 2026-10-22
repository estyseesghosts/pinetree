//! Phase 4 (P1) — bounded DNS resolver work (`docs/prod-ready-2.md` section 5).
//!
//! Required invariant: outstanding DNS work and resolver threads stay below
//! a configured hard limit even when every lookup stalls. Saturation fails
//! fast with a classified transient error and never spawns another thread;
//! a caller timeout abandons only the wait, never the slot, until the
//! stalled OS call actually returns.
//!
//! These tests drive the injectable [`DnsResolver`] surface through
//! [`BoundedDnsResolver`] (scripted stall/fail resolvers) and through
//! [`SafeHttpClient::send_with_resolver`], so no test depends on the live
//! network or the system resolver.

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig, SendErrorKind};
use pinetree::federation::retry::{
    BoundedDnsResolver, DESTINATION_TRACKER_STALE_MS, DestinationTracker, DnsResolver,
    DnsResolverPolicy, MAX_DESTINATION_TRACKER_ENTRIES,
};

/// Re-poll `cond` until it holds or `timeout` elapses (no fixed sleeps that
/// assume scheduler timing).
fn poll_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while !cond() {
        if start.elapsed() > timeout {
            return false;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    true
}

/// Resolver that parks every lookup until the test releases (or drops) the
/// gate, mimicking a hung system resolver. Dropping the release handle
/// unblocks parked lookups with a normal answer so slots drain.
struct StallResolver {
    calls: AtomicUsize,
    entered_tx: mpsc::Sender<()>,
    gate_rx: Mutex<mpsc::Receiver<()>>,
    addrs: Vec<SocketAddr>,
}

struct StallGate {
    entered_rx: mpsc::Receiver<()>,
    release_tx: mpsc::Sender<()>,
}

fn stall_resolver(addrs: Vec<SocketAddr>) -> (Arc<StallResolver>, StallGate) {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let resolver = Arc::new(StallResolver {
        calls: AtomicUsize::new(0),
        entered_tx,
        gate_rx: Mutex::new(release_rx),
        addrs,
    });
    (
        resolver,
        StallGate {
            entered_rx,
            release_tx,
        },
    )
}

impl DnsResolver for StallResolver {
    fn resolve(&self, _host: &str, _port: u16) -> io::Result<Vec<SocketAddr>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let _ = self.entered_tx.send(());
        let _ = self
            .gate_rx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .recv();
        Ok(self.addrs.clone())
    }
}

/// Resolver returning one canned answer (or one canned failure) per lookup.
struct ScriptResolver {
    addrs: Option<Vec<SocketAddr>>,
    failure: Option<String>,
    calls: AtomicUsize,
}

impl ScriptResolver {
    fn addrs(addrs: Vec<SocketAddr>) -> Self {
        Self {
            addrs: Some(addrs),
            failure: None,
            calls: AtomicUsize::new(0),
        }
    }

    fn failure(message: &str) -> Self {
        Self {
            addrs: None,
            failure: Some(message.to_string()),
            calls: AtomicUsize::new(0),
        }
    }
}

impl DnsResolver for ScriptResolver {
    fn resolve(&self, _host: &str, _port: u16) -> io::Result<Vec<SocketAddr>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match (&self.addrs, &self.failure) {
            (Some(addrs), _) => Ok(addrs.clone()),
            (_, Some(message)) => Err(io::Error::other(message.clone())),
            (None, None) => Err(io::Error::other(
                "failed to lookup address information: Name or service not known",
            )),
        }
    }
}

/// Host-keyed canned resolver for redirect tests.
struct MapResolver {
    map: HashMap<String, Vec<SocketAddr>>,
    calls: AtomicUsize,
}

impl DnsResolver for MapResolver {
    fn resolve(&self, host: &str, _port: u16) -> io::Result<Vec<SocketAddr>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.map.get(host).cloned().ok_or_else(|| {
            io::Error::other("failed to lookup address information: Name or service not known")
        })
    }
}

fn private_config() -> SafeHttpConfig {
    SafeHttpConfig {
        allow_private_ips: true,
        dns_timeout: Duration::from_secs(2),
        ..Default::default()
    }
}

fn strict_config() -> SafeHttpConfig {
    SafeHttpConfig {
        allow_private_ips: false,
        allowed_hosts: Vec::new(),
        dns_timeout: Duration::from_secs(2),
        ..Default::default()
    }
}

/// Read one HTTP request head, answer with `route`, then close.
fn serve_once(listener: TcpListener, route: impl Fn(&str) -> String + Send + 'static) {
    std::thread::spawn(move || {
        for _ in 0..8 {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
            let mut head = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                match stream.read(&mut chunk) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        head.extend_from_slice(&chunk[..n]);
                        if head.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                }
            }
            let request = String::from_utf8_lossy(&head).into_owned();
            let response = route(&request);
            if stream.write_all(response.as_bytes()).is_err() {
                return;
            }
        }
    });
}

#[test]
fn dns_timeout_releases_caller_but_holds_slot_until_worker_done() {
    let public: SocketAddr = "93.184.216.34:80".parse().unwrap();
    let (stall, gate) = stall_resolver(vec![public]);
    let pool = BoundedDnsResolver::with_resolver(stall.clone(), DnsResolverPolicy::new(4));

    let start = Instant::now();
    let err = pool
        .resolve("stall.test", 80, Duration::from_millis(100))
        .unwrap_err();
    assert_eq!(err.kind, SendErrorKind::Timeout, "got {err}");
    assert!(err.kind.is_transient(), "timeout must stay transient");
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "caller was not released by its deadline: {:?}",
        start.elapsed()
    );

    // Cancellation semantics: the timed-out caller is gone, but the slot
    // stays occupied while the stale worker is still blocked, so a timeout
    // alone can never free capacity for unbounded replacement work. The
    // worker may be slow to start under load, so wait for it to enter the
    // resolver before asserting the slot is still held.
    assert!(
        poll_until(Duration::from_secs(10), || stall
            .calls
            .load(Ordering::SeqCst)
            == 1),
        "stalled worker never entered the resolver"
    );
    assert_eq!(pool.in_flight(), 1, "slot released while worker stalled");

    // The late worker completes normally (its answer is discarded) and only
    // then is the slot released.
    drop(gate.release_tx);
    assert!(
        poll_until(Duration::from_secs(5), || pool.in_flight() == 0),
        "resolver slot leaked after stalled worker finished"
    );
}

#[test]
fn dns_saturation_fails_fast_without_spawning_replacement_work() {
    const SLOTS: usize = 4;
    let public: SocketAddr = "93.184.216.34:80".parse().unwrap();
    let (stall, gate) = stall_resolver(vec![public]);
    let pool = Arc::new(BoundedDnsResolver::with_resolver(
        stall.clone(),
        DnsResolverPolicy::new(SLOTS),
    ));

    // Occupy every slot with a stalled lookup.
    let mut workers = Vec::new();
    for _ in 0..SLOTS {
        let pool = Arc::clone(&pool);
        workers.push(std::thread::spawn(move || {
            let _ = pool.resolve("stall.test", 80, Duration::from_secs(30));
        }));
    }
    for _ in 0..SLOTS {
        gate.entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("stalled worker never entered the resolver");
    }
    assert_eq!(pool.in_flight(), SLOTS);

    // The pool is full: the next lookup fails fast without spawning.
    let start = Instant::now();
    let err = pool
        .resolve("one-too-many.test", 80, Duration::from_secs(30))
        .unwrap_err();
    assert!(
        err.kind.is_transient(),
        "saturation must be a transient/resource-busy error, got {:?}: {err}",
        err.kind
    );
    assert!(
        err.message.contains("saturated"),
        "saturation error must say so: {err}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "saturated lookup waited instead of failing fast: {:?}",
        start.elapsed()
    );
    assert_eq!(
        stall.calls.load(Ordering::SeqCst),
        SLOTS,
        "saturated lookup spawned resolver work"
    );
    assert_eq!(pool.in_flight(), SLOTS);

    // Releasing the stall drains every slot and the pool serves again.
    drop(gate.release_tx);
    for worker in workers {
        worker.join().expect("stalled worker panicked");
    }
    assert_eq!(pool.in_flight(), 0);
    let addrs = pool
        .resolve("recovered.test", 80, Duration::from_secs(5))
        .expect("pool must serve after drain");
    assert_eq!(addrs, vec![public]);
}

#[test]
fn dns_shutdown_rejects_new_lookups_while_in_flight_work_drains() {
    let public: SocketAddr = "93.184.216.34:80".parse().unwrap();
    let (stall, gate) = stall_resolver(vec![public]);
    let pool = Arc::new(BoundedDnsResolver::with_resolver(
        stall.clone(),
        DnsResolverPolicy::new(2),
    ));

    let parked = Arc::clone(&pool);
    let worker =
        std::thread::spawn(move || parked.resolve("stall.test", 80, Duration::from_secs(30)));
    gate.entered_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("worker never entered the resolver");
    assert_eq!(pool.in_flight(), 1);

    pool.shutdown();
    pool.shutdown(); // Idempotent: safe under concurrent teardown paths.
    let err = pool
        .resolve("after-shutdown.test", 80, Duration::from_secs(5))
        .unwrap_err();
    assert!(
        err.kind.is_transient(),
        "shutdown reject must be transient, got {:?}: {err}",
        err.kind
    );
    assert!(
        err.message.contains("shut down"),
        "shutdown error must say so: {err}"
    );
    assert_eq!(pool.in_flight(), 1, "shutdown disturbed in-flight work");

    // In-flight work still completes and drains; the pool stays closed.
    drop(gate.release_tx);
    let parked = worker.join().expect("parked worker panicked");
    assert_eq!(
        parked.expect("parked lookup must still succeed"),
        vec![public]
    );
    assert_eq!(pool.in_flight(), 0);
    assert!(
        pool.resolve("still-closed.test", 80, Duration::from_secs(1))
            .is_err(),
        "shut-down pool served a lookup"
    );
}

#[test]
fn dns_policy_default_matches_documented_hard_ceiling() {
    assert_eq!(
        DnsResolverPolicy::default().max_in_flight,
        pinetree::federation::retry::DEFAULT_MAX_DNS_IN_FLIGHT
    );
    // Degenerate policies clamp to one slot rather than zero (which would
    // reject everything) or unbounded.
    assert_eq!(DnsResolverPolicy::new(0).max_in_flight, 1);
}

#[test]
fn dns_scripted_ipv4_and_ipv6_answers_reach_local_servers() {
    // IPv4 answer through the injected resolver.
    let v4_listener = TcpListener::bind("127.0.0.1:0").expect("bind v4");
    let v4_addr = v4_listener.local_addr().expect("v4 addr");
    serve_once(v4_listener, |_| {
        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nv4".to_string()
    });
    let v4_resolver = Arc::new(ScriptResolver::addrs(vec![v4_addr]));
    let v4_pool = BoundedDnsResolver::with_resolver(v4_resolver.clone(), DnsResolverPolicy::new(4));
    let client = SafeHttpClient::new(private_config());
    let v4_resp = client
        .send_with_resolver(
            "GET",
            &format!("http://v4.test:{}/who", v4_addr.port()),
            &[],
            None,
            1024,
            &v4_pool,
        )
        .expect("v4 answer must connect");
    assert_eq!(v4_resp.status, 200);
    assert_eq!(v4_resp.body, b"v4");
    assert_eq!(v4_resolver.calls.load(Ordering::SeqCst), 1);

    // IPv6 answer through the injected resolver.
    let v6_listener = TcpListener::bind("[::1]:0").expect("bind v6");
    let v6_addr = v6_listener.local_addr().expect("v6 addr");
    serve_once(v6_listener, |_| {
        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nv6".to_string()
    });
    let v6_resolver = Arc::new(ScriptResolver::addrs(vec![v6_addr]));
    let v6_pool = BoundedDnsResolver::with_resolver(v6_resolver.clone(), DnsResolverPolicy::new(4));
    let v6_resp = client
        .send_with_resolver(
            "GET",
            &format!("http://v6.test:{}/who", v6_addr.port()),
            &[],
            None,
            1024,
            &v6_pool,
        )
        .expect("v6 answer must connect");
    assert_eq!(v6_resp.status, 200);
    assert_eq!(v6_resp.body, b"v6");
}

#[test]
fn dns_nxdomain_is_permanent_other_failures_transient() {
    let client = SafeHttpClient::new(strict_config());
    let nx_pool = BoundedDnsResolver::with_resolver(
        Arc::new(ScriptResolver::failure(
            "failed to lookup address information: Name or service not known",
        )),
        DnsResolverPolicy::new(4),
    );
    let nx_err = client
        .send_with_resolver("GET", "http://gone.test/inbox", &[], None, 1024, &nx_pool)
        .unwrap_err();
    assert_eq!(nx_err.kind, SendErrorKind::DnsNotFound, "got {nx_err}");
    assert!(!nx_err.kind.is_transient(), "NXDOMAIN must not retry");

    let flaky_pool = BoundedDnsResolver::with_resolver(
        Arc::new(ScriptResolver::failure(
            "failed to lookup address information: Temporary failure in name resolution",
        )),
        DnsResolverPolicy::new(4),
    );
    let flaky_err = client
        .send_with_resolver(
            "GET",
            "http://flaky.test/inbox",
            &[],
            None,
            1024,
            &flaky_pool,
        )
        .unwrap_err();
    assert_eq!(flaky_err.kind, SendErrorKind::Dns, "got {flaky_err}");
    assert!(flaky_err.kind.is_transient(), "transient DNS must retry");
}

#[test]
fn dns_injected_resolution_keeps_ssrf_validation_intact() {
    let private: SocketAddr = "127.0.0.1:9".parse().unwrap();
    let pool = BoundedDnsResolver::with_resolver(
        Arc::new(ScriptResolver::addrs(vec![private])),
        DnsResolverPolicy::new(4),
    );
    let client = SafeHttpClient::new(strict_config());
    let err = client
        .send_with_resolver("GET", "http://spoofed.test/inbox", &[], None, 1024, &pool)
        .unwrap_err();
    assert_eq!(err.kind, SendErrorKind::SsrfBlocked, "got {err}");
    assert!(!err.kind.is_transient(), "SSRF block must not retry");
}

#[test]
fn dns_redirect_targets_are_reresolved_and_revalidated() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind redirect peer");
    let addr = listener.local_addr().expect("redirect addr");
    let port = addr.port();
    serve_once(listener, move |req| {
        if req.starts_with("GET /start-blocked ") {
            format!(
                "HTTP/1.1 302 Found\r\nLocation: http://redirected.test:{port}/final\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
        } else if req.starts_with("GET /start-open ") {
            format!(
                "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:{port}/final\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
        } else {
            "HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\ndone".to_string()
        }
    });

    // First hop is an allowlisted literal (no DNS); the redirect target must
    // be re-resolved through the pool and re-checked against SSRF rules.
    let viresolver = Arc::new(MapResolver {
        map: HashMap::from([("redirected.test".to_string(), vec![addr])]),
        calls: AtomicUsize::new(0),
    });
    let pool = BoundedDnsResolver::with_resolver(viresolver.clone(), DnsResolverPolicy::new(4));
    let client = SafeHttpClient::new(SafeHttpConfig {
        allow_private_ips: false,
        allowed_hosts: vec!["127.0.0.1".to_string()],
        dns_timeout: Duration::from_secs(2),
        ..Default::default()
    });

    let blocked = client
        .send_with_resolver(
            "GET",
            &format!("http://127.0.0.1:{port}/start-blocked"),
            &[],
            None,
            1024,
            &pool,
        )
        .unwrap_err();
    assert_eq!(blocked.kind, SendErrorKind::SsrfBlocked, "got {blocked}");
    assert!(!blocked.kind.is_transient());
    assert_eq!(
        viresolver.calls.load(Ordering::SeqCst),
        1,
        "redirect target was not re-resolved through the pool"
    );

    // Control: a redirect back to the allowlisted literal serves normally and
    // needs no DNS at all.
    let ok = client
        .send_with_resolver(
            "GET",
            &format!("http://127.0.0.1:{port}/start-open"),
            &[],
            None,
            1024,
            &pool,
        )
        .expect("allowlisted redirect must serve");
    assert_eq!(ok.status, 200);
    assert_eq!(ok.body, b"done");
    assert_eq!(
        viresolver.calls.load(Ordering::SeqCst),
        1,
        "literal redirect must not consult DNS"
    );
}

#[test]
fn destination_tracker_stays_bounded_and_reclaims_stale_entries() {
    let tracker = DestinationTracker::new();
    let base = 1_700_000_000_000_i64;
    for index in 0..MAX_DESTINATION_TRACKER_ENTRIES + 500 {
        tracker.record_failure(&format!("http://host-{index}.example/inbox"), base, 1_000);
    }
    assert!(
        tracker.len() <= MAX_DESTINATION_TRACKER_ENTRIES,
        "tracker grew past its bound: {}",
        tracker.len()
    );

    // Far past every recorded cooldown, a fresh host reclaims stale entries
    // instead of evicting blindly or growing.
    let later = base + DESTINATION_TRACKER_STALE_MS + 60_000;
    tracker.record_failure("http://fresh-host.example/inbox", later, 1_000);
    assert!(
        tracker.len() <= 16,
        "stale tracker entries were not reclaimed: {}",
        tracker.len()
    );
    assert!(
        tracker
            .cooldown_until("http://fresh-host.example/inbox", later)
            .is_some(),
        "fresh host must hold a live cooldown"
    );
}
