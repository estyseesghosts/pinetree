use super::AppServices;

use crate::activitypub::signatures::{rsa_sha256_sign, rsa_sha256_verify};
use crate::application::ports::{
    ActivityDedup, AuthStore, BlobStore, Clock, IdGenerator, NoopStreamingSink,
};
use crate::domain::id::{AidxGenerator, ManualClock};
use crate::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use crate::media::storage::FsBlobStore;
use crate::persistence::redb::RedbStore;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) fn test_services(prefix: &str) -> AppServices {
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pinetree-cmd-test-{}-{prefix}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_760_400_000_000));
    let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
    let store = RedbStore::open(dir.join("test.redb"), clock.clone(), ids.clone()).unwrap();
    let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.join("blobs")).unwrap());
    AppServices {
        clock,
        ids,
        accounts: store.clone(),
        notes: store.clone(),
        follows: store.clone(),
        reactions: store.clone(),
        jobs: store.clone(),
        media: store.clone(),
        blobs,
        notifications: store.clone(),
        auth: store.clone(),
        streaming: Arc::new(NoopStreamingSink),
        dedup: store.clone() as Arc<dyn ActivityDedup>,
        federation_http: SafeHttpClient::new(SafeHttpConfig {
            allow_private_ips: true,
            ..Default::default()
        }),
        public_url: "https://example.local".to_string(),
        max_file_size: 10 * 1024 * 1024,
        preserved_usernames: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
    }
}

#[test]
fn actor_keypair_generation_rotation_and_remote_key_cache() {
    let services = test_services("keys");
    let alice = services.create_account("alice", None).unwrap();

    // Initial lazy generation creates and persists an RSA-2048 keypair.
    let kp1 = services.ensure_actor_keypair(&alice.id).unwrap();
    let kp1_again = services.ensure_actor_keypair(&alice.id).unwrap();
    assert_eq!(kp1, kp1_again, "ensure_actor_keypair is idempotent");
    assert!(kp1.public_key_pem.contains("BEGIN PUBLIC KEY"));
    assert!(kp1.private_key_pem.contains("BEGIN PRIVATE KEY"));

    let sig = rsa_sha256_sign(&kp1.private_key_pem, b"federation payload").unwrap();
    assert!(
        rsa_sha256_verify(&kp1.public_key_pem, b"federation payload", &sig).unwrap()
    );

    // Key rotation replaces the persisted keypair.
    let kp2 = services.rotate_actor_keypair(&alice.id).unwrap();
    assert_ne!(kp1.public_key_pem, kp2.public_key_pem);
    let fetched = services.auth.get_actor_keypair(&alice.id).unwrap().unwrap();
    assert_eq!(fetched.public_key_pem, kp2.public_key_pem);

    // System actor provisioning is idempotent and yields a valid keypair.
    let (sys1, sys_kp1) = services.ensure_system_actor().unwrap();
    let (sys2, sys_kp2) = services.ensure_system_actor().unwrap();
    assert_eq!(sys1.id, sys2.id);
    assert_eq!(sys_kp1, sys_kp2);
    assert!(sys1.is_bot);

    // Remote public key cache stores and retrieves by key_id.
    let cached = services
        .cache_remote_public_key(
            "https://remote.example/users/bob#main-key",
            "https://remote.example/users/bob",
            None,
            &kp1.public_key_pem,
        )
        .unwrap();
    let loaded = services
        .auth
        .get_remote_public_key("https://remote.example/users/bob#main-key")
        .unwrap()
        .unwrap();
    assert_eq!(loaded, cached);
}
