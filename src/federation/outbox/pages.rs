//! Federated page delivery helpers (moved from `src/federation/outbox.rs`).
//! Durable outbound ActivityPub delivery payloads and job execution
//! (Phase 2 Slice D3; Phase 2.6 Slice 10 delivery reliability).
//!
//! State mutations commit a compact [`FederateDeliverPayload`] inside a
//! [`JobKind::FederateDeliver`] job in the same storage transaction as the
//! domain state change. The background worker resolves destination inboxes
//! (preferring `shared_inbox` when available and deduplicating per inbox URL)
//! and performs signed HTTP POSTs with retry/backoff.
//!
//! Fan-out: a multi-recipient activity job never retries as a whole. Its
//! recipients are walked page by page; small fan-outs are delivered inline and
//! each failed inbox becomes a one-inbox job (`inbox_override`); large fan-outs
//! become one-inbox jobs without any inline send.

use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::federation::delivery::SafeHttpClient;
use crate::federation::retry::{DeliveryOutcome, DestinationTracker};

use super::dispatch::{deliver_single, fan_out};
use super::payload::FederateDeliverPayload;
use super::recipients::collect_recipient_inboxes_for_author;

/// Deliver one fan-out activity to `inboxes`. The calling job has no
/// `inbox_override`.
///
/// - More than [`FANOUT_INLINE_MAX`] inboxes: nothing is sent here. Every
///   inbox (or its cooldown end, if later) becomes a one-inbox job, returned as
///   [`DeliveryOutcome::Split`].
/// - Otherwise each inbox is sent inline. A destination in cooldown, or one
///   with a transient failure, becomes a one-inbox job scheduled at the
///   cooldown end or the failure's `Retry-After`/backoff time. Permanent
///   failures are logged and dropped.
///
/// The activity job itself is never retried as a whole: once it returns
/// `Split` (or `Delivered`) it completes, so a single slow or failing inbox
/// cannot cause the already-delivered inboxes to be sent again.
#[allow(clippy::too_many_arguments)]
/// Shared `Create(Article)` / `Update(Article)` fan-out for federated
/// pages (mirrors the `NoteUpdated` arm: author must be local, page must
/// still exist and still federate, else the stale job converges quietly).
#[allow(clippy::too_many_arguments)]
pub(crate) fn deliver_page_create_or_update(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    job_key: &str,
    base: &str,
    page_id: &str,
    author_id: &str,
    updated: bool,
    inbox_override: Option<String>,
) -> Result<DeliveryOutcome, PortError> {
    use crate::activitypub::federated_collections::{
        federation_switch_on, render_page_activity, render_page_article,
    };
    let author = match services.accounts.find_by_id(author_id)? {
        Some(a) if a.is_local() => a,
        _ => return Ok(DeliveryOutcome::Delivered),
    };
    let page = match services.pages.get_page(page_id)? {
        Some(p) if p.owner_id == author_id => p,
        _ => return Ok(DeliveryOutcome::Delivered),
    };
    if !page.federate
        || page.tier != crate::domain::collection::CollectionTier::Listed
        || !federation_switch_on(services, "pages")
    {
        return Ok(DeliveryOutcome::Delivered);
    }
    let actor_uri = author.actor_url(base);
    let followers_uri = format!("{actor_uri}/followers");
    let article = render_page_article(services, &page)?;
    let activity_id = format!(
        "page-{}-{page_id}-{}",
        if updated { "update" } else { "create" },
        services.ids.generate()
    );
    let activity = render_page_activity(
        base,
        &activity_id,
        &actor_uri,
        &article,
        updated,
        &followers_uri,
    );
    match inbox_override {
        Some(inbox) => deliver_single(services, client, tracker, author_id, &inbox, &activity),
        None => {
            let inboxes = collect_recipient_inboxes_for_author(services, author_id, &[], true)?;
            let constructor = if updated {
                FederateDeliverPayload::PageUpdated {
                    page_id: page_id.to_string(),
                    author_id: author_id.to_string(),
                    inbox_override: None,
                }
            } else {
                FederateDeliverPayload::PageCreated {
                    page_id: page_id.to_string(),
                    author_id: author_id.to_string(),
                    inbox_override: None,
                }
            };
            fan_out(
                services,
                client,
                tracker,
                job_key,
                author_id,
                &inboxes,
                &activity,
                &|inbox: &str| match &constructor {
                    FederateDeliverPayload::PageUpdated { .. } => {
                        FederateDeliverPayload::PageUpdated {
                            page_id: page_id.to_string(),
                            author_id: author_id.to_string(),
                            inbox_override: Some(inbox.to_string()),
                        }
                    }
                    _ => FederateDeliverPayload::PageCreated {
                        page_id: page_id.to_string(),
                        author_id: author_id.to_string(),
                        inbox_override: Some(inbox.to_string()),
                    },
                },
            )
        }
    }
}

/// Shared `Delete(Article)` fan-out for local page deletes (same
/// recipients as Create/Update).
#[allow(clippy::too_many_arguments)]
pub(crate) fn deliver_page_delete(
    services: &AppServices,
    client: &SafeHttpClient,
    tracker: &DestinationTracker,
    job_key: &str,
    base: &str,
    page_id: &str,
    author_id: &str,
    page_uri: &str,
    inbox_override: Option<String>,
) -> Result<DeliveryOutcome, PortError> {
    use crate::activitypub::federated_collections::render_page_delete_activity;
    let author = match services.accounts.find_by_id(author_id)? {
        Some(a) if a.is_local() => a,
        _ => return Ok(DeliveryOutcome::Delivered),
    };
    let actor_uri = author.actor_url(base);
    let activity_id = format!("page-delete-{page_id}-{}", services.ids.generate());
    let activity = render_page_delete_activity(base, &activity_id, &actor_uri, page_uri);
    match inbox_override {
        Some(inbox) => deliver_single(services, client, tracker, author_id, &inbox, &activity),
        None => {
            let inboxes = collect_recipient_inboxes_for_author(services, author_id, &[], true)?;
            fan_out(
                services,
                client,
                tracker,
                job_key,
                author_id,
                &inboxes,
                &activity,
                &|inbox: &str| FederateDeliverPayload::PageDeleted {
                    page_id: page_id.to_string(),
                    author_id: author_id.to_string(),
                    page_uri: page_uri.to_string(),
                    inbox_override: Some(inbox.to_string()),
                },
            )
        }
    }
}
