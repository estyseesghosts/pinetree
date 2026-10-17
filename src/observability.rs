//! Process-wide debug switches. The server logs with plain `eprintln!` (there is
//! no tracing dependency), so the opt-in federation debug lines follow that style.
//!
//! Enable with `--debug-federation` or `PINETREE_DEBUG_FEDERATION=1` (`true`,
//! `yes`, `on` also work, case-insensitive). Off by default. When off, each
//! `fed_debug!` call costs one relaxed atomic load and formats nothing.
//!
//! Line shape: `[fed-debug] ts=<unix-ms> <dir> <event> key=value...`, where
//! `<dir>` is `in` (inbound inbox) or `out` (outbound delivery). Untrusted
//! strings are printed with `{:?}` so control characters cannot forge lines.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Environment variable that enables federation debug logging (any non-empty
/// affirmative value; see [`env_enables_federation_debug`]).
pub const DEBUG_FEDERATION_ENV: &str = "PINETREE_DEBUG_FEDERATION";

static FEDERATION_DEBUG: AtomicBool = AtomicBool::new(false);

pub fn init() {}

/// Turn federation debug lines on or off for the whole process. Called once from
/// `main` after reading `--debug-federation` and the environment.
pub fn set_federation_debug(on: bool) {
    FEDERATION_DEBUG.store(on, Ordering::Relaxed);
}

pub fn federation_debug() -> bool {
    FEDERATION_DEBUG.load(Ordering::Relaxed)
}

/// Whether an environment value enables federation debug. Unset, empty, and any
/// value other than `1`/`true`/`yes`/`on` (case-insensitive) mean off.
pub fn env_enables_federation_debug(value: Option<&str>) -> bool {
    matches!(
        value.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

#[doc(hidden)]
pub fn emit_federation_debug(args: std::fmt::Arguments<'_>) {
    let ts_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    eprintln!("[fed-debug] ts={ts_ms} {args}");
}

/// `fed_debug!("in  reject reason={}", why)` writes one `[fed-debug]` line when
/// federation debug is on. Arguments are not evaluated when it is off.
#[macro_export]
macro_rules! fed_debug {
    ($($arg:tt)*) => {
        if $crate::observability::federation_debug() {
            $crate::observability::emit_federation_debug(format_args!($($arg)*));
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_value_parsing() {
        assert!(env_enables_federation_debug(Some("1")));
        assert!(env_enables_federation_debug(Some(" TRUE ")));
        assert!(env_enables_federation_debug(Some("yes")));
        assert!(env_enables_federation_debug(Some("on")));
        assert!(!env_enables_federation_debug(None));
        assert!(!env_enables_federation_debug(Some("")));
        assert!(!env_enables_federation_debug(Some("0")));
        assert!(!env_enables_federation_debug(Some("off")));
        assert!(!env_enables_federation_debug(Some("debug")));
    }

    #[test]
    fn switch_gates_the_macro() {
        set_federation_debug(false);
        assert!(!federation_debug());
        set_federation_debug(true);
        assert!(federation_debug());
        // Exercises the macro path; output goes to stderr.
        crate::fed_debug!("test line key={}", 1);
        set_federation_debug(false);
    }
}
