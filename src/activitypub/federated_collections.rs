//! Federated clips + pages wire surface (`OrderedCollection` clips,
//! `Article` pages, fetch-gated serving, remote resolution).

mod clips;
mod pages;
mod remote_clips;
mod serving;

pub use clips::{
    CLIP_COLLECTION_PAGE_SIZE, clip_collection_uri, clips_index_uri, get_clip_collection,
    get_clips_index, render_clip_collection, render_clip_collection_page,
    render_clip_delete_activity, render_clip_membership_activity, render_clip_tombstone,
    render_clip_update_activity, render_clips_index, viewer_follows_author,
};
pub use pages::{
    get_page_article, page_uri, render_page_activity, render_page_article,
    render_page_delete_activity, render_page_html, render_page_like_activity,
};
pub use remote_clips::{
    REMOTE_CLIP_TTL, evict_remote_clip_cache, fetch_collection_owner, ingest_remote_clip_doc,
    remote_clip_cached_for, remote_clip_cached_uris, resolve_remote_clip,
    resolve_remote_clip_with_ttl,
};
pub use serving::federation_switch_on;

#[cfg(test)]
pub use remote_clips::forget_remote_clip_cache;
