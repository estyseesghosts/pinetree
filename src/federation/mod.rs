pub mod inbox;
pub mod outbox;
pub mod delivery;
pub mod host_caps;
pub mod retry;
/// Generic authenticated-fetch gate: HTTP-signature verification on
/// inbound GETs plus the serve/403/404 decision interface. Clip/page tier
/// enforcement and (later) private-community membership checks are
/// resource callbacks above this gate — never below it.
pub mod fetch_gate;
