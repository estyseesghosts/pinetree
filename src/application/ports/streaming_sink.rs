use crate::domain::id::PortError;

pub trait StreamingSink: Send + Sync {
    fn publish(&self, event_type: &str, payload: &[u8]) -> Result<(), PortError>;
    /// Fan out one `main`-channel event. The default is a no-op so
    /// test doubles keep working; the broadcast hub overrides it.
    fn push_main(&self, _msg: crate::application::events::MainMessage) -> Result<(), PortError> {
        Ok(())
    }
    /// Fan out one broadcast-stream event (`{type, body}` to every
    /// connection — Misskey `onBroadcastMessage` parity). The default is a
    /// no-op so test doubles keep working; the broadcast hub overrides it.
    fn push_broadcast(&self, _event: &str, _body: serde_json::Value) -> Result<(), PortError> {
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct NoopStreamingSink;

impl StreamingSink for NoopStreamingSink {
    fn publish(&self, _event_type: &str, _payload: &[u8]) -> Result<(), PortError> {
        Ok(())
    }
}

// --- Batch M: moderation, registration policy, instance settings -----------
//
// New ports carry ONLY required methods (guide §2 rule 4): a backend that
// omits one must fail to compile. The atomicity contract (§2.3, conformance
// groups H/I) is structural like the rest of this file: pinetree ports have
// no transaction handle, so a moderation action's state change, its
// moderation-log row, and its outbound jobs are committed together by ONE
// port call — the compound methods below. `append` is only for log rows
// that are the whole action.
