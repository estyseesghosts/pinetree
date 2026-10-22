use serde_json::Value;

use super::super::payload::FederateDeliverPayload;
use super::super::recipients::fanout_is_split;
use crate::application::commands::AppServices;
use crate::application::ports::{NewJob, PortError};
use crate::federation::delivery::{SafeHttpClient, signed_post_activity};
use crate::federation::retry::{
    DESTINATION_BACKOFF_BASE_MS, DeliveryOutcome, DestinationTracker, classify_inbox_status,
};

pub(crate) fn fan_out(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    job_key: &str,
    sender_account_id: &str,
    inboxes: &[String],
    activity: &Value,
    per_inbox: &dyn Fn(&str) -> FederateDeliverPayload,
) -> Result<DeliveryOutcome, PortError> {
    if inboxes.is_empty() {
        return Ok(DeliveryOutcome::Delivered);
    }
    crate::fed_debug!(
        "out fanout type={} recipients={} split={}",
        activity.get("type").and_then(Value::as_str).unwrap_or("?"),
        inboxes.len(),
        fanout_is_split(inboxes.len())
    );
    let now_ms = services.clock.now_ms();
    if fanout_is_split(inboxes.len()) {
        let mut followups = Vec::with_capacity(inboxes.len());
        for inbox in inboxes {
            let not_before = tracker.cooldown_until(inbox, now_ms).unwrap_or(now_ms);
            followups.push(per_inbox_job(job_key, inbox, not_before, per_inbox)?);
        }
        return Ok(DeliveryOutcome::Split { followups });
    }

    let mut followups = Vec::new();
    for inbox in inboxes {
        match deliver_single(
            services,
            client,
            tracker,
            sender_account_id,
            inbox,
            activity,
        )? {
            DeliveryOutcome::Delivered => {}
            DeliveryOutcome::PermanentFailure(reason) => {
                eprintln!("dropping delivery to {inbox}: {reason}");
            }
            DeliveryOutcome::TransientFailure { retry_after_ms, .. } => {
                let not_before = retry_after_ms.unwrap_or(now_ms);
                followups.push(per_inbox_job(job_key, inbox, not_before, per_inbox)?);
            }
            DeliveryOutcome::Deferred { until_ms } => {
                followups.push(per_inbox_job(job_key, inbox, until_ms, per_inbox)?);
            }
            // `deliver_single` never produces a fan-out.
            DeliveryOutcome::Split { .. } => {}
        }
    }
    if followups.is_empty() {
        Ok(DeliveryOutcome::Delivered)
    } else {
        Ok(DeliveryOutcome::Split { followups })
    }
}

/// One-inbox job for `inbox`, keyed `{job_key}:{inbox}` so a re-run of the
/// activity job derives the same key for the same inbox.
pub(crate) fn per_inbox_job(
    job_key: &str,
    inbox: &str,
    not_before: i64,
    per_inbox: &dyn Fn(&str) -> FederateDeliverPayload,
) -> Result<NewJob, PortError> {
    per_inbox(inbox).into_job(format!("{job_key}:{inbox}"), not_before)
}

/// Send one signed activity to one inbox and classify the result.
///
/// A destination in cooldown is deferred: nothing is sent and the caller
/// decides how to reschedule. The tracker is updated for every real attempt.
pub(crate) fn deliver_single(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    sender_account_id: &str,
    inbox_url: &str,
    activity: &Value,
) -> Result<DeliveryOutcome, PortError> {
    // Instance block (WP5): check at delivery time, not enqueue time. Skip
    // without failing the job so it completes rather than retrying.
    if let Some(host) = crate::federation::host_caps::host_of_url(inbox_url) {
        if crate::domain::instance::host_is_blocked(&services.blocked_hosts(), &host) {
            crate::fed_debug!("out skip inbox={inbox_url:?} reason=blocked-host");
            return Ok(DeliveryOutcome::Delivered);
        }
    }
    let now_ms = services.clock.now_ms();
    let kind = activity.get("type").and_then(Value::as_str).unwrap_or("?");
    if let Some(until_ms) = tracker.cooldown_until(inbox_url, now_ms) {
        crate::fed_debug!(
            "out defer type={kind} inbox={inbox_url:?} until_ms={until_ms} reason=destination-cooldown"
        );
        return Ok(DeliveryOutcome::Deferred { until_ms });
    }
    let body = serde_json::to_vec(activity)
        .map_err(|e| PortError::Validation(format!("serialize activity: {e}")))?;
    let started = std::time::Instant::now();
    let sent = signed_post_activity(services, client, sender_account_id, inbox_url, &body);
    let elapsed_ms = started.elapsed().as_millis();
    match sent {
        Ok(resp) => {
            let reason = String::from_utf8_lossy(&resp.body).into_owned();
            let retry_after = resp.header("retry-after").map(str::to_owned);
            let outcome =
                classify_inbox_status(resp.status, reason.trim(), retry_after.as_deref(), now_ms);
            crate::fed_debug!(
                "out send type={kind} inbox={inbox_url:?} status={} ms={elapsed_ms} outcome={outcome:?}",
                resp.status
            );
            Ok(settle_outcome(tracker, inbox_url, now_ms, outcome))
        }
        Err(err) if err.kind.is_transient() => {
            crate::fed_debug!(
                "out send type={kind} inbox={inbox_url:?} error=transient kind={:?} ms={elapsed_ms} msg={:?}",
                err.kind,
                err.message
            );
            let outcome = DeliveryOutcome::TransientFailure {
                reason: err.message,
                retry_after_ms: None,
            };
            Ok(settle_outcome(tracker, inbox_url, now_ms, outcome))
        }
        Err(err) => {
            crate::fed_debug!(
                "out send type={kind} inbox={inbox_url:?} error=permanent kind={:?} ms={elapsed_ms} msg={:?}",
                err.kind,
                err.message
            );
            Ok(DeliveryOutcome::PermanentFailure(err.message))
        }
    }
}

/// Record a send's result in the destination tracker. A transient failure
/// starts or extends the host cooldown; its `retry_after_ms` becomes the later
/// of the server's `Retry-After` and that cooldown end.
pub(crate) fn settle_outcome(
    tracker: &DestinationTracker,
    inbox_url: &str,
    now_ms: i64,
    outcome: DeliveryOutcome,
) -> DeliveryOutcome {
    match outcome {
        DeliveryOutcome::Delivered => {
            tracker.record_success(inbox_url);
            DeliveryOutcome::Delivered
        }
        DeliveryOutcome::TransientFailure {
            reason,
            retry_after_ms,
        } => {
            let until = tracker.record_failure(inbox_url, now_ms, DESTINATION_BACKOFF_BASE_MS);
            DeliveryOutcome::TransientFailure {
                reason,
                retry_after_ms: Some(retry_after_ms.map_or(until, |t| t.max(until))),
            }
        }
        other => other,
    }
}
