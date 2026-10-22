//! WebFinger, host-meta, and NodeInfo (Phase 2 Slice A1).
//!
//! Pure render/resolution helpers plus the Axum handlers that serve them.
//! Route wiring lives in `crate::api::misskey::endpoints` (once slices
//! A1–A3 land); this module only exposes the handlers and helpers.
//!
//! Public IDs/URLs never expose node hostnames: actor URLs always take the
//! `{public_url}/users/{account_id}` form.

mod finger;
mod host_meta;
mod nodeinfo;
mod responses;

#[cfg(test)]
pub(crate) mod memory_mock;
#[cfg(test)]
pub(crate) mod memory_mock_extra;

pub use finger::{
    render_webfinger_jrd, render_webfinger_xrd, resolve_webfinger_resource, webfinger,
};
pub use host_meta::{host_meta_json, host_meta_xrd, render_host_meta_jrd, render_host_meta_xrd};
pub use nodeinfo::{
    nodeinfo_2_0, nodeinfo_2_1, render_nodeinfo, render_well_known_nodeinfo, well_known_nodeinfo,
};
pub use responses::{
    CONTENT_TYPE_JRD, CONTENT_TYPE_NODEINFO_2_0, CONTENT_TYPE_NODEINFO_2_1, CONTENT_TYPE_XRD,
    extract_host,
};
