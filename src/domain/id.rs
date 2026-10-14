use std::fmt;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub const TIME2000: i64 = 946_684_800_000;
const BASE36_CHARS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainError {
    Validation(String),
    Conflict(String),
    NotFound(String),
    StorageUnavailable(String),
}

impl fmt::Display for DomainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DomainError::Validation(msg) => write!(f, "validation error: {msg}"),
            DomainError::Conflict(msg) => write!(f, "conflict: {msg}"),
            DomainError::NotFound(msg) => write!(f, "not found: {msg}"),
            DomainError::StorageUnavailable(msg) => write!(f, "storage unavailable: {msg}"),
        }
    }
}

impl std::error::Error for DomainError {}

pub type PortError = DomainError;

pub trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
}

#[derive(Debug, Default, Clone)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(TIME2000)
    }
}

#[derive(Debug)]
pub struct ManualClock {
    now_ms: AtomicI64,
}

impl ManualClock {
    pub fn new(initial_ms: i64) -> Self {
        Self {
            now_ms: AtomicI64::new(initial_ms),
        }
    }

    pub fn set_ms(&self, ms: i64) {
        self.now_ms.store(ms, Ordering::SeqCst);
    }

    pub fn advance_ms(&self, delta_ms: i64) -> i64 {
        self.now_ms.fetch_add(delta_ms, Ordering::SeqCst) + delta_ms
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> i64 {
        self.now_ms.load(Ordering::SeqCst)
    }
}

pub trait IdGenerator: Send + Sync {
    fn generate(&self) -> String;
    fn parse_time(&self, id: &str) -> Result<i64, PortError>;
}

pub fn is_valid_aidx(id: &str) -> bool {
    id.len() == 16
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || b.is_ascii_lowercase())
}

pub fn is_valid_misskey_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 32 && id.bytes().all(|b| b.is_ascii_alphanumeric())
}

pub fn parse_aidx_time(id: &str) -> Result<i64, PortError> {
    if !is_valid_aidx(id) {
        return Err(PortError::Validation(format!("invalid aidx id: {id}")));
    }
    let time_part = &id[0..8];
    let delta = i64::from_str_radix(time_part, 36)
        .map_err(|_| PortError::Validation(format!("invalid base36 time prefix in aidx: {id}")))?;
    Ok(delta + TIME2000)
}

fn format_base36_fixed(mut value: u64, width: usize) -> String {
    let mut buf = vec![b'0'; width];
    for idx in (0..width).rev() {
        let rem = (value % 36) as usize;
        buf[idx] = BASE36_CHARS[rem];
        value /= 36;
    }
    String::from_utf8(buf).expect("ascii base36")
}

pub struct AidxGenerator {
    clock: Arc<dyn Clock>,
    node_id: String,
    counter: AtomicU32,
}

impl AidxGenerator {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        let mut rng_bytes = [0u8; 4];
        for b in &mut rng_bytes {
            let idx = (rand::random::<u8>() as usize) % 36;
            *b = BASE36_CHARS[idx];
        }
        let node_id = String::from_utf8(rng_bytes.to_vec()).expect("ascii node_id");
        let initial_counter = rand::random::<u32>() % (36 * 36 * 36 * 36);
        Self {
            clock,
            node_id,
            counter: AtomicU32::new(initial_counter),
        }
    }

    pub fn with_node_id(clock: Arc<dyn Clock>, node_id: &str, initial_counter: u32) -> Self {
        assert_eq!(node_id.len(), 4);
        assert!(node_id
            .bytes()
            .all(|b| b.is_ascii_digit() || b.is_ascii_lowercase()));
        Self {
            clock,
            node_id: node_id.to_string(),
            counter: AtomicU32::new(initial_counter),
        }
    }
}

impl IdGenerator for AidxGenerator {
    fn generate(&self) -> String {
        let now = self.clock.now_ms();
        let delta = (now - TIME2000).max(0) as u64;
        let time_str = format_base36_fixed(delta, 8);
        let c = self.counter.fetch_add(1, Ordering::SeqCst).wrapping_add(1);
        let noise_str = format_base36_fixed((c % 1_679_616) as u64, 4);
        format!("{}{}{}", time_str, self.node_id, noise_str)
    }

    fn parse_time(&self, id: &str) -> Result<i64, PortError> {
        parse_aidx_time(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use regex::Regex;

    #[test]
    fn aidx_matches_regex_and_roundtrips_time() {
        let clock = Arc::new(ManualClock::new(1_760_400_000_123));
        let generator = AidxGenerator::with_node_id(clock.clone(), "ab12", 0);
        let re = Regex::new(r"^[0-9a-z]{16}$").unwrap();

        let id = generator.generate();
        assert!(re.is_match(&id), "id {id} did not match aidx regex");
        assert!(is_valid_aidx(&id));
        assert_eq!(generator.parse_time(&id).unwrap(), 1_760_400_000_123);
    }

    #[test]
    fn aidx_preserves_lexicographical_sort_order() {
        let clock = Arc::new(ManualClock::new(1_700_000_000_000));
        let generator = AidxGenerator::with_node_id(clock.clone(), "node", 0);

        let mut ids = Vec::new();
        for step in 0..50 {
            if step % 5 == 0 {
                clock.advance_ms(10);
            }
            ids.push(generator.generate());
        }

        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted, "aidx IDs must be lexicographically sorted");
    }

    #[test]
    fn parse_time_rejects_foreign_shapes() {
        let clock = Arc::new(ManualClock::new(1_700_000_000_000));
        let generator = AidxGenerator::new(clock);

        assert!(generator.parse_time("short").is_err());
        assert!(generator.parse_time("0123456789ABCDEF").is_err()); // uppercase rejected
        assert!(generator.parse_time("0123456789abcdef0").is_err()); // 17 chars rejected
        assert!(generator.parse_time("0123456789abcde-").is_err()); // non-alnum rejected
    }
}
