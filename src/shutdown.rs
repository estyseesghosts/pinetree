//! Process stop handling.
//!
//! A container runtime stops a container with `SIGTERM` and escalates to
//! `SIGKILL` once the grace period expires. Without a handler, the default
//! `SIGTERM` disposition kills the process outright: in-flight HTTP
//! responses are cut mid-body, streaming sockets drop without a close
//! frame, and a job that was mid-flight is abandoned until its lease
//! expires and another worker reclaims it.
//!
//! [`signal`] resolves on `SIGTERM` or `SIGINT` so the caller can hand it to
//! `axum::serve(..).with_graceful_shutdown(..)`. The server then stops
//! accepting connections, lets the in-flight ones finish, and only then
//! does the process exit.

/// Wait for a stop signal.
///
/// Resolves on `SIGTERM` (container stop, `kill`, systemd) or `SIGINT`
/// (Ctrl-C, `docker kill --signal=INT`). On a platform without `SIGTERM`
/// handling only `SIGINT` ends the wait.
pub async fn signal() {
    let ctrl_c = async {
        // A failed registration is not fatal here: the `SIGTERM` arm is the
        // one a container runtime uses.
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = terminate_signal();
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

/// Wait for `SIGTERM`.
///
/// If the handler cannot be installed the wait degrades to "never", so the
/// process keeps serving rather than exiting on a signal it never saw.
#[cfg(unix)]
async fn terminate_signal() {
    match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(mut sig) => {
            sig.recv().await;
        }
        Err(err) => {
            eprintln!("cannot install SIGTERM handler: {err}");
            std::future::pending::<()>().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn signal_does_not_resolve_without_a_signal() {
        // The wait must block: a server that shut down on its own would
        // drop every connection the moment it started.
        let wait = tokio::time::timeout(Duration::from_millis(150), signal()).await;
        assert!(wait.is_err(), "signal() resolved without a stop signal");
    }
}
