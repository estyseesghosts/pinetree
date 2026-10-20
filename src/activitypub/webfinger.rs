//! WebFinger, host-meta, and NodeInfo (Phase 2 Slice A1).
//!
//! Pure render/resolution helpers plus the Axum handlers that serve them.
//! Route wiring lives in `crate::api::misskey::endpoints` (once slices
//! A1–A3 land); this module only exposes the handlers and helpers.
//!
//! Public IDs/URLs never expose node hostnames: actor URLs always take the
//! `{public_url}/users/{account_id}` form.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};

use crate::activitypub::PACKER_VERSION;
use crate::application::commands::AppServices;
use crate::application::ports::TimelineCursor;
use crate::domain::account::Account;

/// JRD content type for WebFinger and `host-meta.json`.
pub const CONTENT_TYPE_JRD: &str = "application/jrd+json; charset=utf-8";
/// XRD content type for `host-meta` and WebFinger XML.
pub const CONTENT_TYPE_XRD: &str = "application/xrd+xml; charset=utf-8";
/// NodeInfo 2.0 document content type.
pub const CONTENT_TYPE_NODEINFO_2_0: &str =
    "application/json; profile=\"http://nodeinfo.diaspora.software/ns/schema/2.0#\"; charset=utf-8";
/// NodeInfo 2.1 document content type.
pub const CONTENT_TYPE_NODEINFO_2_1: &str =
    "application/json; profile=\"http://nodeinfo.diaspora.software/ns/schema/2.1#\"; charset=utf-8";

const CACHE_WEBFINGER: &str = "public, max-age=180";
const CACHE_NODEINFO: &str = "public, max-age=300";

// --- Small helpers ---------------------------------------------------------

/// Lowercased `host[:port]` portion of a `public_url` such as
/// `https://example.local` or `https://example.local:3000/base`.
pub fn extract_host(public_url: &str) -> String {
    let without_scheme = match public_url.split_once("://") {
        Some((_, rest)) => rest,
        None => public_url,
    };
    let host = without_scheme.split('/').next().unwrap_or("");
    host.trim_end_matches(':').to_ascii_lowercase()
}

fn xml_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(ch),
        }
    }
    out
}

fn base_url(public_url: &str) -> &str {
    public_url.trim_end_matches('/')
}

fn cors_headers(headers: &mut HeaderMap) {
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
}

fn text_response(status: StatusCode, content_type: &'static str, body: String) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(content_type),
    );
    cors_headers(&mut headers);
    (status, headers, body).into_response()
}

fn json_response(status: StatusCode, content_type: &'static str, body: &Value) -> Response {
    text_response(
        status,
        content_type,
        serde_json::to_string(body).unwrap_or_else(|_| "{}".to_string()),
    )
}

fn error_response(status: StatusCode) -> Response {
    text_response(status, CONTENT_TYPE_JRD, String::new())
}

// --- host-meta -------------------------------------------------------------

/// XRD host-meta document pointing at this node's WebFinger template.
pub fn render_host_meta_xrd(public_url: &str) -> String {
    let base = base_url(public_url);
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><XRD xmlns=\"http://docs.oasis-open.org/ns/xri/xrd-1.0\"><Link rel=\"lrdd\" type=\"application/xrd+xml\" template=\"{base}/.well-known/webfinger?resource={{uri}}\"/></XRD>"
    )
}

/// JRD host-meta document pointing at this node's WebFinger template.
pub fn render_host_meta_jrd(public_url: &str) -> Value {
    let base = base_url(public_url);
    json!({
        "links": [
            {
                "rel": "lrdd",
                "type": "application/jrd+json",
                "template": format!("{base}/.well-known/webfinger?resource={{uri}}"),
            }
        ]
    })
}

/// `GET /.well-known/host-meta` (XRD).
pub async fn host_meta_xrd(State(services): State<Arc<AppServices>>) -> Response {
    text_response(
        StatusCode::OK,
        CONTENT_TYPE_XRD,
        render_host_meta_xrd(&services.public_url),
    )
}

/// `GET /.well-known/host-meta.json` (JRD).
pub async fn host_meta_json(State(services): State<Arc<AppServices>>) -> Response {
    json_response(
        StatusCode::OK,
        CONTENT_TYPE_JRD,
        &render_host_meta_jrd(&services.public_url),
    )
}

// --- webfinger -------------------------------------------------------------

fn account_visible(account: &Account) -> bool {
    account.is_local() && !account.is_suspended && !account.is_deleted
}

fn find_local_account(
    services: &AppServices,
    username: &str,
) -> Result<Option<Account>, StatusCode> {
    let username = username.trim();
    if username.is_empty() {
        return Ok(None);
    }
    let lower = username.to_ascii_lowercase();
    if let Some(acc) = services
        .accounts
        .find_by_username_and_host(&lower, None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        return Ok(Some(acc));
    }
    if lower == "system.actor" || lower == "instance.actor" {
        if let Some(acc) = services
            .accounts
            .find_by_username_and_host("system_actor", None)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        {
            return Ok(Some(acc));
        }
    }
    Ok(None)
}

/// Resolve a WebFinger `resource` to a visible local account.
///
/// Accepts (case-insensitively) `{base}/users/{id}`, `{base}/@{username}`,
/// `acct:{user}@{host}`, `{user}@{host}`, `acct:{user}`, and `{user}`.
/// A foreign `@{host}` is `422` (matching Misskey/Sharkey
/// `WellKnownServerService.ts`); unknown, remote, suspended, or deleted
/// accounts are `404`.
pub fn resolve_webfinger_resource(
    services: &AppServices,
    resource: &str,
) -> Result<Account, StatusCode> {
    let base = base_url(&services.public_url).to_string();
    let base_lower = base.to_ascii_lowercase();
    let host = extract_host(&base);
    let resource = resource.trim();
    if resource.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }

    // Optional `acct:` scheme (case-insensitive).
    let mut candidate = resource;
    if let Some(prefix) = candidate.get(.."acct:".len()) {
        if prefix.eq_ignore_ascii_case("acct:") {
            candidate = candidate["acct:".len()..].trim();
            if candidate.is_empty() {
                return Err(StatusCode::NOT_FOUND);
            }
        }
    }

    let candidate_lower = candidate.to_ascii_lowercase();
    let users_prefix = format!("{base_lower}/users/");
    let profile_prefix = format!("{base_lower}/@");
    let found = if candidate_lower
        .strip_prefix(users_prefix.as_str())
        .map(|_| &candidate[users_prefix.len()..])
        .is_some()
    {
        // `{base}/users/{id}`: the id keeps its given case (aidx ids are
        // lowercase, so this also matches a lowercased lookup).
        let rest = &candidate[users_prefix.len()..];
        let id = rest.split(['/', '?', '#']).next().unwrap_or("");
        if id.is_empty() {
            return Err(StatusCode::NOT_FOUND);
        }
        services
            .accounts
            .find_by_id(id)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    } else if let Some(rest) = candidate_lower
        .strip_prefix(profile_prefix.as_str())
        .map(|_| &candidate[profile_prefix.len()..])
    {
        // `{base}/@{username}`.
        let username = rest.split(['/', '?', '#']).next().unwrap_or("");
        find_local_account(services, username)?
    } else {
        // `acct:{user}@{h}`, `{user}@{h}`, `acct:{user}`, `{user}`, plus
        // the `system.actor` / `instance.actor` service accounts (resolved
        // through the same local username lookup when present).
        let bare = candidate.trim().trim_start_matches('@');
        if bare.is_empty() {
            return Err(StatusCode::NOT_FOUND);
        }
        if let Some((user, h)) = bare.split_once('@') {
            if h.contains('@') || user.trim().is_empty() || h.trim().is_empty() {
                return Err(StatusCode::NOT_FOUND);
            }
            if h.to_ascii_lowercase() != host.to_ascii_lowercase() {
                return Err(StatusCode::UNPROCESSABLE_ENTITY);
            }
            find_local_account(services, user.trim().trim_start_matches('@'))?
        } else {
            find_local_account(services, bare)?
        }
    };

    match found {
        Some(account) if account_visible(&account) => Ok(account),
        _ => Err(StatusCode::NOT_FOUND),
    }
}

/// JRD WebFinger document for a local account.
pub fn render_webfinger_jrd(account: &Account, public_url: &str) -> Value {
    let base = base_url(public_url);
    let host = extract_host(base);
    let actor = account.actor_url(base);
    json!({
        "subject": format!("acct:{}@{}", account.username, host),
        "aliases": [
            format!("{base}/@{}", account.username),
            actor.clone(),
        ],
        "links": [
            {
                "rel": "self",
                "type": "application/activity+json",
                "href": actor,
            },
            {
                "rel": "http://webfinger.net/rel/profile-page",
                "type": "text/html",
                "href": format!("{base}/@{}", account.username),
            },
            {
                "rel": "http://ostatus.org/schema/1.0/subscribe",
                "template": format!("{base}/authorize-follow?acct={{uri}}"),
            },
        ],
    })
}

/// XRD WebFinger document for a local account.
pub fn render_webfinger_xrd(account: &Account, public_url: &str) -> String {
    let base = base_url(public_url);
    let host = extract_host(base);
    let actor = account.actor_url(base);
    let subject = xml_escape(&format!("acct:{}@{host}", account.username));
    let profile_page = xml_escape(&format!("{base}/@{}", account.username));
    let actor_escaped = xml_escape(&actor);
    let subscribe = xml_escape(&format!("{base}/authorize-follow?acct={{uri}}"));
    let mut out = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><XRD xmlns=\"http://docs.oasis-open.org/ns/xri/xrd-1.0\">",
    );
    out.push_str(&format!("<Subject>{subject}</Subject>"));
    out.push_str(&format!(
        "<Link rel=\"self\" type=\"application/activity+json\" href=\"{actor_escaped}\"/>"
    ));
    out.push_str(&format!(
        "<Link rel=\"http://webfinger.net/rel/profile-page\" type=\"text/html\" href=\"{profile_page}\"/>"
    ));
    out.push_str(&format!(
        "<Link rel=\"http://ostatus.org/schema/1.0/subscribe\" template=\"{subscribe}\"/>"
    ));
    for alias in [format!("{base}/@{}", account.username), actor] {
        out.push_str(&format!("<Alias>{}</Alias>", xml_escape(&alias)));
    }
    out.push_str("</XRD>");
    out
}

/// True when the `Accept` header explicitly prefers `application/xrd+xml`
/// over the JRD/JSON types (a bare `*/*` stays on JRD).
fn wants_xrd(accept: Option<&str>) -> bool {
    let header = match accept {
        Some(value) if !value.trim().is_empty() => value,
        _ => return false,
    };
    let quality = |wanted: &str| -> f32 {
        let mut best: f32 = 0.0;
        for part in header.split(',') {
            let mut segments = part.split(';');
            let mime = segments.next().unwrap_or("").trim().to_ascii_lowercase();
            if mime != wanted {
                continue;
            }
            let mut q: f32 = 1.0;
            for param in segments {
                let param = param.trim();
                if let Some(value) = param
                    .strip_prefix("q=")
                    .or_else(|| param.strip_prefix("Q="))
                {
                    q = value.trim().parse().unwrap_or(1.0);
                }
            }
            best = best.max(q);
        }
        best
    };
    let xrd = quality("application/xrd+xml");
    xrd > 0.0 && xrd > quality("application/jrd+json") && xrd > quality("application/json")
}

/// `GET /.well-known/webfinger?resource=...`.
pub async fn webfinger(
    State(services): State<Arc<AppServices>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let resource = params.get("resource").map(String::as_str).unwrap_or("");
    if resource.trim().is_empty() {
        return error_response(StatusCode::BAD_REQUEST);
    }
    let account = match resolve_webfinger_resource(&services, resource) {
        Ok(account) => account,
        Err(status) => return error_response(status),
    };
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok());
    if wants_xrd(accept) {
        let mut response = text_response(
            StatusCode::OK,
            CONTENT_TYPE_XRD,
            render_webfinger_xrd(&account, &services.public_url),
        );
        response.headers_mut().insert(
            header::VARY,
            HeaderValue::from_static("Accept"),
        );
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static(CACHE_WEBFINGER),
        );
        response
    } else {
        let mut response = json_response(
            StatusCode::OK,
            CONTENT_TYPE_JRD,
            &render_webfinger_jrd(&account, &services.public_url),
        );
        response.headers_mut().insert(
            header::VARY,
            HeaderValue::from_static("Accept"),
        );
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static(CACHE_WEBFINGER),
        );
        response
    }
}

// --- nodeinfo --------------------------------------------------------------

/// `/.well-known/nodeinfo` link document.
pub fn render_well_known_nodeinfo(public_url: &str) -> Value {
    let base = base_url(public_url);
    json!({
        "links": [
            {
                "rel": "http://nodeinfo.diaspora.software/ns/schema/2.1",
                "href": format!("{base}/nodeinfo/2.1"),
            },
            {
                "rel": "http://nodeinfo.diaspora.software/ns/schema/2.0",
                "href": format!("{base}/nodeinfo/2.0"),
            },
        ]
    })
}

/// NodeInfo 2.0 / 2.1 document matching Misskey `NodeinfoServerService.ts`.
pub fn render_nodeinfo(services: &AppServices, version: &str) -> Value {
    let local_users = services
        .accounts
        .search_accounts("", 0, 10_000)
        .unwrap_or_default()
        .into_iter()
        .filter(|account| account.is_local())
        .count();
    let local_posts = services
        .notes
        .list_all_notes(&TimelineCursor::with_limit(10_000))
        .unwrap_or_default()
        .into_iter()
        .filter(|note| note.user_host.is_none())
        .count();

    let software = if version == "2.1" {
        json!({
            "name": "misskey",
            "version": PACKER_VERSION,
            "homepage": "https://misskey-hub.net",
            "repository": null,
        })
    } else {
        json!({
            "name": "misskey",
            "version": PACKER_VERSION,
        })
    };

    json!({
        "version": version,
        "software": software,
        "protocols": ["activitypub"],
        "services": {
            "inbound": [],
            "outbound": ["atom1.0", "rss2.0"],
        },
        "openRegistrations": true,
        "usage": {
            "users": {
                "total": local_users,
                "activeHalfyear": null,
                "activeMonth": null,
            },
            "localPosts": local_posts,
            "localComments": 0,
        },
        "metadata": {
            "nodeName": "Pinetree",
            "nodeDescription": null,
            "maintainer": { "name": null, "email": null },
            "langs": ["en"],
            "tosUrl": null,
            "repositoryUrl": null,
            "feedbackUrl": null,
            "disableRegistration": false,
            "disableLocalTimeline": false,
            "disableGlobalTimeline": false,
            "emailRequiredForSignup": false,
            "enableHcaptcha": false,
            "enableRecaptcha": false,
            "enableMcaptcha": false,
            "enableTurnstile": false,
            "maxNoteTextLength": 3000,
            "enableEmail": false,
            "enableServiceWorker": true,
            "proxyAccountName": null,
            "themeColor": "#86b300",
        },
    })
}

/// `GET /.well-known/nodeinfo`.
pub async fn well_known_nodeinfo(State(services): State<Arc<AppServices>>) -> Response {
    let mut response = json_response(
        StatusCode::OK,
        "application/json; charset=utf-8",
        &render_well_known_nodeinfo(&services.public_url),
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(CACHE_NODEINFO),
    );
    response
}

/// `GET /nodeinfo/2.0`.
pub async fn nodeinfo_2_0(State(services): State<Arc<AppServices>>) -> Response {
    let mut response = json_response(
        StatusCode::OK,
        CONTENT_TYPE_NODEINFO_2_0,
        &render_nodeinfo(&services, "2.0"),
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(CACHE_NODEINFO),
    );
    response
}

/// `GET /nodeinfo/2.1`.
pub async fn nodeinfo_2_1(State(services): State<Arc<AppServices>>) -> Response {
    let mut response = json_response(
        StatusCode::OK,
        CONTENT_TYPE_NODEINFO_2_1,
        &render_nodeinfo(&services, "2.1"),
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(CACHE_NODEINFO),
    );
    response
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};

    use axum::body::to_bytes;
    use axum::extract::{Query, State};

    use super::*;
    use crate::application::ports::{
        AccountStore, ActivityDedup, ApiTokenRecord, AuthStore, BlobStore, BlockStore,
        CreateAccountInput, CreateNoteInput, FollowStore, HostCapsStore, HostSoftware,
        InsertMediaInput, JobQueue,
        LegacyAuthSessionRecord, MediaStore, MiAuthSessionRecord, MuteStore, NewJob, NoteStore,
        NotificationStore, PollChoiceCount, PollStore, PortError, ReactionStore, StreamingSink, TimelineCursor,
        UpdateAccountPatch,
    };
    use crate::domain::blocking::Blocking;
    use crate::domain::muting::{Muting, RenoteMuting};
    use crate::domain::follow::{Follow, FollowNotify};
    use crate::domain::id::{AidxGenerator, ManualClock};
    use crate::domain::note::{Note, Visibility};
    use crate::domain::notification::{Notification, NotificationKind};
    use crate::domain::reaction::Reaction;
    use crate::domain::poll::{Poll, PollVote};
    use crate::federation::delivery::{SafeHttpClient, SafeHttpConfig};

    struct Mem {
        accounts: Mutex<HashMap<String, Account>>,
        notes: Mutex<HashMap<String, Note>>,
    }

    impl Mem {
        fn empty() -> Self {
            Self {
                accounts: Mutex::new(HashMap::new()),
                notes: Mutex::new(HashMap::new()),
            }
        }

        fn insert_account(&self, account: Account) {
            self.accounts
                .lock()
                .unwrap()
                .insert(account.id.clone(), account);
        }

        fn insert_note(&self, note: Note) {
            self.notes.lock().unwrap().insert(note.id.clone(), note);
        }
    }

    fn unavailable<T>(what: &str) -> Result<T, PortError> {
        Err(PortError::StorageUnavailable(format!("test stub: {what}")))
    }

    impl AccountStore for Mem {
        fn create_account(
            &self,
            _input: CreateAccountInput,
            _log: Option<&crate::domain::moderation::ModerationLogEntry>,
        ) -> Result<Account, PortError> {
            unavailable("create_account")
        }
        fn find_by_id(&self, id: &str) -> Result<Option<Account>, PortError> {
            Ok(self.accounts.lock().unwrap().get(id).cloned())
        }
        fn find_by_handle(&self, handle: &str) -> Result<Option<Account>, PortError> {
            let cleaned = handle.trim().trim_start_matches('@').to_ascii_lowercase();
            self.find_by_username_and_host(&cleaned, None)
        }
        fn find_by_username_and_host(
            &self,
            username: &str,
            host: Option<&str>,
        ) -> Result<Option<Account>, PortError> {
            let lower = username.trim().to_ascii_lowercase();
            Ok(self
                .accounts
                .lock()
                .unwrap()
                .values()
                .find(|account| {
                    account.username_lower == lower
                        && account.host.as_deref() == host
                })
                .cloned())
        }
        fn is_username_available(
            &self,
            _username: &str,
            _preserved: &[String],
        ) -> Result<bool, PortError> {
            Ok(true)
        }
        fn update_account(
            &self,
            _id: &str,
            _patch: UpdateAccountPatch,
            _jobs: Vec<NewJob>,
        ) -> Result<Account, PortError> {
            unavailable("update_account")
        }
        fn rename(&self, _id: &str, _new_handle: &str) -> Result<Account, PortError> {
            unavailable("rename")
        }
        fn get_owner(&self, _id: &str) -> Result<String, PortError> {
            Ok("test-node".to_string())
        }
        fn set_owner(&self, _id: &str, _node: &str, _proof: &str) -> Result<(), PortError> {
            Ok(())
        }
        fn search_accounts(
            &self,
            _query: &str,
            _offset: usize,
            _limit: usize,
        ) -> Result<Vec<Account>, PortError> {
            Ok(self.accounts.lock().unwrap().values().cloned().collect())
        }
        fn all_accounts(&self, after: Option<&str>, max_rows: usize) -> Result<(Vec<Account>, Option<String>), PortError> {
            let mut out: Vec<Account> = self.accounts.lock().unwrap().values().cloned().collect();
            out.sort_by(|a, b| a.id.cmp(&b.id));
            if let Some(a) = after {
                out.retain(|x| x.id.as_str() > a);
            }
            let next = if out.len() > max_rows { Some(out[max_rows - 1].id.clone()) } else { None };
            out.truncate(max_rows);
            Ok((out, next))
        }
        fn remote_shared_inboxes(
            &self,
            _after: Option<&str>,
            _max_rows: usize,
        ) -> Result<(Vec<String>, Option<String>), PortError> {
            Ok((Vec::new(), None))
        }
    }

    impl NoteStore for Mem {
        fn create_note(
            &self,
            _input: CreateNoteInput,
            _jobs: Vec<NewJob>,
        ) -> Result<Note, PortError> {
            unavailable("create_note")
        }
        fn get_note(&self, id: &str) -> Result<Option<Note>, PortError> {
            Ok(self.notes.lock().unwrap().get(id).cloned())
        }
        fn get_note_including_deleted(&self, id: &str) -> Result<Option<Note>, PortError> {
            self.get_note(id)
        }
        fn delete_note(&self, _id: &str, _jobs: Vec<NewJob>) -> Result<(), PortError> {
            unavailable("delete_note")
        }
        fn notes_by_author(
            &self,
            _author: &str,
            _cursor: &TimelineCursor,
        ) -> Result<Vec<Note>, PortError> {
            Ok(Vec::new())
        }
        fn list_all_notes(&self, _cursor: &TimelineCursor) -> Result<Vec<Note>, PortError> {
            Ok(self.notes.lock().unwrap().values().cloned().collect())
        }
        fn attach_remote_media(
            &self,
            _note_id: &str,
            _input: InsertMediaInput,
            _jobs: Vec<NewJob>,
        ) -> Result<crate::domain::media::MediaMeta, PortError> {
            unavailable("attach_remote_media")
        }
    }

    impl FollowStore for Mem {
        fn follow(
            &self,
            _a: &str,
            _b: &str,
            _with_replies: bool,
            _notify: Option<FollowNotify>,
            _jobs: Vec<NewJob>,
        ) -> Result<Follow, PortError> {
            unavailable("follow")
        }
        fn unfollow(&self, _a: &str, _b: &str, _jobs: Vec<NewJob>) -> Result<(), PortError> {
            unavailable("unfollow")
        }
        fn is_following(&self, _a: &str, _b: &str) -> Result<bool, PortError> {
            Ok(false)
        }
        fn get_follow(&self, _a: &str, _b: &str) -> Result<Option<Follow>, PortError> {
            Ok(None)
        }
        fn followers_of(
            &self,
            _id: &str,
            _cursor: Option<&str>,
            _limit: usize,
        ) -> Result<Vec<Follow>, PortError> {
            Ok(Vec::new())
        }
        fn following_of(
            &self,
            _id: &str,
            _cursor: Option<&str>,
            _limit: usize,
        ) -> Result<Vec<Follow>, PortError> {
            Ok(Vec::new())
        }
    }

    impl BlockStore for Mem {
        fn block(&self, _blocking: Blocking, _jobs: Vec<NewJob>) -> Result<Blocking, PortError> {
            unavailable("block")
        }
        fn unblock(&self, _a: &str, _b: &str, _jobs: Vec<NewJob>) -> Result<(), PortError> {
            unavailable("unblock")
        }
        fn get_blocking(&self, _a: &str, _b: &str) -> Result<Option<Blocking>, PortError> {
            Ok(None)
        }
        fn is_blocking(&self, _a: &str, _b: &str) -> Result<bool, PortError> {
            Ok(false)
        }
        fn blocking_ids_for(&self, _id: &str) -> Result<Vec<String>, PortError> {
            Ok(Vec::new())
        }
        fn blocked_by_ids_for(&self, _id: &str) -> Result<Vec<String>, PortError> {
            Ok(Vec::new())
        }
        fn list_blockings(
            &self,
            _id: &str,
            _cursor: &TimelineCursor,
        ) -> Result<Vec<Blocking>, PortError> {
            Ok(Vec::new())
        }
    }

    impl HostCapsStore for Mem {
        fn get_host_software(&self, _host: &str) -> Result<Option<HostSoftware>, PortError> {
            Ok(None)
        }
        fn put_host_software(&self, _entry: HostSoftware) -> Result<(), PortError> {
            Ok(())
        }
    }

    impl crate::application::ports::ModerationLogStore for Mem {
        fn set_suspended(
            &self,
            _account_id: &str,
            _suspended: bool,
            _log: crate::domain::moderation::ModerationLogEntry,
            _jobs: Vec<NewJob>,
        ) -> Result<Account, PortError> {
            unavailable("set_suspended")
        }
        fn set_role(
            &self,
            _account_id: &str,
            _is_admin: bool,
            _is_moderator: bool,
            _log: crate::domain::moderation::ModerationLogEntry,
        ) -> Result<Account, PortError> {
            unavailable("set_role")
        }
        fn update_account_logged(
            &self,
            _account_id: &str,
            _patch: UpdateAccountPatch,
            _log: crate::domain::moderation::ModerationLogEntry,
            _jobs: Vec<NewJob>,
        ) -> Result<Account, PortError> {
            unavailable("update_account_logged")
        }
        fn delete_account(
            &self,
            _account_id: &str,
            _log: Option<crate::domain::moderation::ModerationLogEntry>,
            _jobs: Vec<NewJob>,
        ) -> Result<(), PortError> {
            unavailable("delete_account")
        }
        fn append(
            &self,
            _entry: crate::domain::moderation::ModerationLogEntry,
        ) -> Result<(), PortError> {
            unavailable("append")
        }
        fn list_logs(
            &self,
            _filter: &crate::application::ports::ModerationLogFilter,
            _cursor: &TimelineCursor,
        ) -> Result<Vec<crate::domain::moderation::ModerationLogEntry>, PortError> {
            unavailable("list_logs")
        }
    }

    impl crate::application::ports::InstanceSettingsStore for Mem {
        fn get(&self) -> Result<crate::domain::instance::InstanceSettings, PortError> {
            unavailable("instance_settings get")
        }
        fn update(
            &self,
            _patch: crate::application::ports::InstanceSettingsPatch,
            _log: crate::domain::moderation::ModerationLogEntry,
        ) -> Result<crate::domain::instance::InstanceSettings, PortError> {
            unavailable("instance_settings update")
        }
    }

    impl crate::application::ports::InviteStore for Mem {
        fn create_invites(
            &self,
            _inputs: &[crate::application::ports::CreateInviteInput],
            _log: Option<&crate::domain::moderation::ModerationLogEntry>,
        ) -> Result<Vec<crate::domain::instance::InviteCode>, PortError> {
            unavailable("create_invites")
        }
        fn find_invite_by_id(
            &self,
            _id: &str,
        ) -> Result<Option<crate::domain::instance::InviteCode>, PortError> {
            unavailable("find_invite_by_id")
        }
        fn find_invite_by_code(
            &self,
            _code: &str,
        ) -> Result<Option<crate::domain::instance::InviteCode>, PortError> {
            unavailable("find_invite_by_code")
        }
        fn list_invites(
            &self,
            _filter: &crate::application::ports::InviteListFilter,
        ) -> Result<Vec<crate::domain::instance::InviteCode>, PortError> {
            unavailable("list_invites")
        }
        fn delete_invite(
            &self,
            _id: &str,
            _log: Option<&crate::domain::moderation::ModerationLogEntry>,
        ) -> Result<(), PortError> {
            unavailable("delete_invite")
        }
        fn consume_and_create_account(
            &self,
            _input: crate::application::ports::ConsumeInviteInput,
        ) -> Result<
            (
                crate::domain::account::Account,
                crate::domain::instance::InviteCode,
            ),
            PortError,
        > {
            unavailable("consume_and_create_account")
        }
    }

    impl crate::application::ports::AbuseReportStore for Mem {
        fn count_for_target(&self, _account_id: &str) -> Result<usize, PortError> { unavailable("count_for_target") }
        fn create(&self, _report: crate::domain::moderation::AbuseReport) -> Result<crate::domain::moderation::AbuseReport, PortError> { unavailable("create") }
        fn get(&self, _id: &str) -> Result<Option<crate::domain::moderation::AbuseReport>, PortError> { unavailable("get") }
        fn list(&self, _filter: &crate::application::ports::AbuseReportFilter, _cursor: &crate::application::ports::TimelineCursor) -> Result<Vec<crate::domain::moderation::AbuseReport>, PortError> { unavailable("list") }
        fn resolve(&self, _id: &str, _resolved_as: Option<crate::domain::moderation::AbuseReportResolution>, _assignee: Option<String>, _log: crate::domain::moderation::ModerationLogEntry, _jobs: Vec<crate::application::ports::NewJob>) -> Result<crate::domain::moderation::AbuseReport, PortError> { unavailable("resolve") }
        fn set_note(&self, _id: &str, _note: String, _log: Option<crate::domain::moderation::ModerationLogEntry>, _jobs: Vec<crate::application::ports::NewJob>) -> Result<crate::domain::moderation::AbuseReport, PortError> { unavailable("set_note") }
        fn mark_forwarded(&self, _id: &str, _log: crate::domain::moderation::ModerationLogEntry, _jobs: Vec<crate::application::ports::NewJob>) -> Result<crate::domain::moderation::AbuseReport, PortError> { unavailable("mark_forwarded") }
        fn create_flag_report(&self, _activity_id: &str, report: crate::domain::moderation::AbuseReport) -> Result<Option<crate::domain::moderation::AbuseReport>, PortError> { Ok(Some(report)) }
    }

    impl MuteStore for Mem {
        fn mute(
            &self,
            _a: &str,
            _b: &str,
            _expires: Option<i64>,
            _jobs: Vec<NewJob>,
        ) -> Result<Option<Muting>, PortError> {
            unavailable("mute")
        }
        fn unmute(&self, _a: &str, _b: &str, _jobs: Vec<NewJob>) -> Result<(), PortError> {
            unavailable("unmute")
        }
        fn get_muting(&self, _a: &str, _b: &str) -> Result<Option<Muting>, PortError> {
            Ok(None)
        }
        fn is_muting(&self, _a: &str, _b: &str) -> Result<bool, PortError> {
            Ok(false)
        }
        fn muted_ids_for(&self, _id: &str) -> Result<Vec<String>, PortError> {
            Ok(Vec::new())
        }
        fn list_mutings(
            &self,
            _id: &str,
            _cursor: &TimelineCursor,
        ) -> Result<Vec<Muting>, PortError> {
            Ok(Vec::new())
        }
        fn renote_mute(
            &self,
            _a: &str,
            _b: &str,
            _jobs: Vec<NewJob>,
        ) -> Result<RenoteMuting, PortError> {
            unavailable("renote_mute")
        }
        fn renote_unmute(&self, _a: &str, _b: &str, _jobs: Vec<NewJob>) -> Result<(), PortError> {
            unavailable("renote_unmute")
        }
        fn get_renote_muting(&self, _a: &str, _b: &str) -> Result<Option<RenoteMuting>, PortError> {
            Ok(None)
        }
        fn is_renote_muting(&self, _a: &str, _b: &str) -> Result<bool, PortError> {
            Ok(false)
        }
        fn renote_muted_ids_for(&self, _id: &str) -> Result<Vec<String>, PortError> {
            Ok(Vec::new())
        }
        fn list_renote_mutings(
            &self,
            _id: &str,
            _cursor: &TimelineCursor,
        ) -> Result<Vec<RenoteMuting>, PortError> {
            Ok(Vec::new())
        }
    }

    impl ReactionStore for Mem {
        fn react(
            &self,
            _user: &str,
            _note: &str,
            _reaction: &str,
            _reaction_id: &str,
            _upsert: bool,
            _jobs: Vec<NewJob>,
        ) -> Result<Reaction, PortError> {
            unavailable("react")
        }
        fn unreact(&self, _user: &str, _note: &str, _jobs: Vec<NewJob>) -> Result<(), PortError> {
            unavailable("unreact")
        }
        fn get_reaction(&self, _user: &str, _note: &str) -> Result<Option<Reaction>, PortError> {
            Ok(None)
        }
        fn reactions_by_user(&self, _user: &str) -> Result<Vec<Reaction>, PortError> {
            Ok(Vec::new())
        }
    }

    impl crate::application::ports::FavoriteStore for Mem {
        fn favorite_note(
            &self,
            _favorite: crate::domain::favorite::NoteFavorite,
            _jobs: Vec<NewJob>,
        ) -> Result<crate::domain::favorite::NoteFavorite, PortError> {
            unavailable("favorite_note")
        }
        fn unfavorite_note(&self, _user_id: &str, _note_id: &str) -> Result<bool, PortError> {
            Ok(false)
        }
        fn is_favorited(&self, _user_id: &str, _note_id: &str) -> Result<bool, PortError> {
            Ok(false)
        }
        fn favorites_by_user(
            &self,
            _user_id: &str,
            _cursor: &crate::application::ports::TimelineCursor,
        ) -> Result<Vec<crate::domain::favorite::NoteFavorite>, PortError> {
            Ok(Vec::new())
        }
    }

    impl crate::application::ports::EmojiStore for Mem {
        fn create_emoji(
            &self,
            _emoji: crate::domain::emoji::CustomEmoji,
            _log: crate::domain::moderation::ModerationLogEntry,
            _jobs: Vec<NewJob>,
        ) -> Result<crate::domain::emoji::CustomEmoji, PortError> {
            unavailable("create_emoji")
        }
        fn update_emoji(
            &self,
            _emoji: crate::domain::emoji::CustomEmoji,
            _log: crate::domain::moderation::ModerationLogEntry,
            _jobs: Vec<NewJob>,
        ) -> Result<crate::domain::emoji::CustomEmoji, PortError> {
            unavailable("update_emoji")
        }
        fn delete_emoji(
            &self,
            _id: &str,
            _log: crate::domain::moderation::ModerationLogEntry,
            _jobs: Vec<NewJob>,
        ) -> Result<bool, PortError> {
            Ok(false)
        }
        fn get_emoji(&self, _id: &str) -> Result<Option<crate::domain::emoji::CustomEmoji>, PortError> {
            Ok(None)
        }
        fn get_emoji_by_name(&self, _shortcode: &str) -> Result<Option<crate::domain::emoji::CustomEmoji>, PortError> {
            Ok(None)
        }
        fn list_emojis(
            &self,
            _query: Option<&str>,
            _cursor: &crate::application::ports::TimelineCursor,
        ) -> Result<Vec<crate::domain::emoji::CustomEmoji>, PortError> {
            Ok(Vec::new())
        }
        fn all_local_emojis(&self) -> Result<Vec<crate::domain::emoji::CustomEmoji>, PortError> {
            Ok(Vec::new())
        }
        fn upsert_remote_emoji(
            &self,
            emoji: crate::domain::emoji::RemoteEmoji,
        ) -> Result<crate::domain::emoji::RemoteEmoji, PortError> {
            Ok(emoji)
        }
        fn get_remote_emoji(
            &self,
            _host: &str,
            _shortcode: &str,
        ) -> Result<Option<crate::domain::emoji::RemoteEmoji>, PortError> {
            Ok(None)
        }
        fn all_remote_emojis(&self) -> Result<Vec<crate::domain::emoji::RemoteEmoji>, PortError> {
            Ok(Vec::new())
        }
        fn set_remote_emoji_media(
            &self,
            _host: &str,
            _shortcode: &str,
            _media_id: &str,
        ) -> Result<(), PortError> {
            Ok(())
        }
    }

    impl crate::application::ports::AntennaStore for Mem {
        fn create_antenna(
            &self,
            _antenna: crate::domain::antenna::Antenna,
            _jobs: Vec<NewJob>,
        ) -> Result<crate::domain::antenna::Antenna, PortError> {
            unavailable("create_antenna")
        }
        fn update_antenna(
            &self,
            _antenna: crate::domain::antenna::Antenna,
            _jobs: Vec<NewJob>,
        ) -> Result<crate::domain::antenna::Antenna, PortError> {
            unavailable("update_antenna")
        }
        fn delete_antenna(&self, _owner_id: &str, _id: &str) -> Result<bool, PortError> {
            Ok(false)
        }
        fn get_antenna(
            &self,
            _owner_id: &str,
            _id: &str,
        ) -> Result<Option<crate::domain::antenna::Antenna>, PortError> {
            Ok(None)
        }
        fn antennas_by_owner(
            &self,
            _owner_id: &str,
        ) -> Result<Vec<crate::domain::antenna::Antenna>, PortError> {
            Ok(Vec::new())
        }
        fn all_active_antennas(&self) -> Result<Vec<crate::domain::antenna::Antenna>, PortError> {
            Ok(Vec::new())
        }
        fn touch_antenna(&self, _owner_id: &str, _id: &str, _now_ms: i64) -> Result<(), PortError> {
            Ok(())
        }
    }

    impl crate::application::ports::AntennaNoteStore for Mem {
        fn append_antenna_note(&self, _antenna_id: &str, _note_id: &str) -> Result<(), PortError> {
            Ok(())
        }
        fn remove_antenna_note(&self, _antenna_id: &str, _note_id: &str) -> Result<(), PortError> {
            Ok(())
        }
        fn delete_antenna_notes(&self, _antenna_id: &str) -> Result<(), PortError> {
            Ok(())
        }
        fn antenna_notes(
            &self,
            _antenna_id: &str,
            _cursor: &crate::application::ports::TimelineCursor,
        ) -> Result<Vec<String>, PortError> {
            Ok(Vec::new())
        }
    }


    impl JobQueue for Mem {
        fn claim_next(
            &self,
            _lease_ms: i64,
        ) -> Result<Option<crate::application::ports::Job>, PortError> {
            Ok(None)
        }
        fn complete(&self, _job_id: &str) -> Result<(), PortError> {
            Ok(())
        }
        fn fail(&self, _job_id: &str, _retry_at_ms: i64) -> Result<(), PortError> {
            Ok(())
        }
        fn complete_with_followups(
            &self,
            _job_id: &str,
            _followups: Vec<NewJob>,
        ) -> Result<(), PortError> {
            Ok(())
        }
        fn enqueue_jobs(&self, _jobs: Vec<NewJob>) -> Result<(), PortError> {
            Ok(())
        }
        fn defer(&self, _job_id: &str, _not_before_ms: i64) -> Result<(), PortError> {
            Ok(())
        }
        fn get_job(
            &self,
            _job_id: &str,
        ) -> Result<Option<crate::application::ports::Job>, PortError> {
            Ok(None)
        }
        fn pending_count(&self) -> Result<usize, PortError> {
            Ok(0)
        }
    }

    impl PollStore for Mem {
        fn get_poll(&self, _note_id: &str) -> Result<Option<Poll>, PortError> {
            Ok(None)
        }
        fn votes_by_user_for_note(
            &self,
            _user_id: &str,
            _note_id: &str,
        ) -> Result<Vec<PollVote>, PortError> {
            Ok(Vec::new())
        }
        fn cast_vote(&self, _vote: PollVote, _jobs: Vec<NewJob>) -> Result<PollVote, PortError> {
            unavailable("cast_vote")
        }
        fn sync_poll_votes(
            &self,
            _note_id: &str,
            _counts_by_name: &[PollChoiceCount],
            _jobs: Vec<NewJob>,
        ) -> Result<Poll, PortError> {
            unavailable("sync_poll_votes")
        }
    }

    impl ActivityDedup for Mem {
        fn check_and_record(&self, _activity_id: &str) -> Result<bool, PortError> {
            Ok(false)
        }
        fn is_recorded(&self, _activity_id: &str) -> Result<bool, PortError> {
            Ok(false)
        }
        fn record_note_activity(
            &self,
            _activity_id: &str,
            _input: CreateNoteInput,
            _jobs: Vec<NewJob>,
        ) -> Result<Option<Note>, PortError> {
            Ok(None)
        }
        fn record_reject_follow_activity(
            &self,
            _activity_id: &str,
            _follower_id: &str,
            _followee_id: &str,
            _jobs: Vec<NewJob>,
        ) -> Result<bool, PortError> {
            Ok(false)
        }
    }

    impl MediaStore for Mem {
        fn insert_metadata(&self, _input: InsertMediaInput) -> Result<crate::domain::media::MediaMeta, PortError> {
            unavailable("insert_metadata")
        }
        fn get(&self, _id: &str) -> Result<Option<crate::domain::media::MediaMeta>, PortError> {
            Ok(None)
        }
        fn find_by_owner_and_hash(
            &self,
            _owner: &str,
            _hash: &str,
        ) -> Result<Option<crate::domain::media::MediaMeta>, PortError> {
            Ok(None)
        }
        fn find_by_source_url(
            &self,
            _source_url: &str,
        ) -> Result<Vec<crate::domain::media::MediaMeta>, PortError> {
            // Test double stores no remote-cache rows.
            Ok(Vec::new())
        }
        fn delete_metadata(&self, _id: &str) -> Result<(), PortError> {
            unavailable("delete_metadata")
        }
        fn list_by_owner(
            &self,
            _owner_id: &str,
        ) -> Result<Vec<crate::domain::media::MediaMeta>, PortError> {
            // Test double stores no media rows.
            Ok(Vec::new())
        }
        fn list_all_media(
            &self,
            _cursor: &crate::application::ports::TimelineCursor,
        ) -> Result<Vec<crate::domain::media::MediaMeta>, PortError> {
            Ok(Vec::new())
        }
    }

    impl BlobStore for Mem {
        fn put(&self, _hash: &str, _bytes: &[u8]) -> Result<(), PortError> {
            Ok(())
        }
        fn get(&self, _hash: &str) -> Result<Option<Vec<u8>>, PortError> {
            Ok(None)
        }
        fn delete(&self, _hash: &str) -> Result<(), PortError> {
            Ok(())
        }
    }

    impl NotificationStore for Mem {
        fn insert_notification(&self, _n: Notification) -> Result<Notification, PortError> {
            unavailable("insert_notification")
        }
        fn list_notifications(
            &self,
            _notifiee: &str,
            _limit: usize,
        ) -> Result<Vec<Notification>, PortError> {
            Ok(Vec::new())
        }
        fn mark_all_read(&self, _notifiee: &str) -> Result<u64, PortError> {
            Ok(0)
        }
        fn list_notifications_filtered(
            &self,
            _notifiee: &str,
            _cursor: &TimelineCursor,
            _include: &[NotificationKind],
            _exclude: &[NotificationKind],
        ) -> Result<Vec<Notification>, PortError> {
            Ok(Vec::new())
        }
    }

    impl AuthStore for Mem {
        fn set_password_hash(&self, _id: &str, _hash: &str) -> Result<(), PortError> {
            Ok(())
        }
        fn get_password_hash(&self, _id: &str) -> Result<Option<String>, PortError> {
            Ok(None)
        }
        fn store_token(&self, _record: ApiTokenRecord) -> Result<(), PortError> {
            Ok(())
        }
        fn get_token_by_hash(&self, _hash: &str) -> Result<Option<ApiTokenRecord>, PortError> {
            Ok(None)
        }
        fn delete_token_by_hash(&self, _token_hash: &str) -> Result<(), PortError> {
            Ok(())
        }
        fn save_miauth_session(&self, _s: MiAuthSessionRecord) -> Result<(), PortError> {
            Ok(())
        }
        fn get_miauth_session(
            &self,
            _id: &str,
        ) -> Result<Option<MiAuthSessionRecord>, PortError> {
            Ok(None)
        }
        fn consume_miauth_session(
            &self,
            _id: &str,
        ) -> Result<Option<MiAuthSessionRecord>, PortError> {
            Ok(None)
        }
        fn save_legacy_session(&self, _s: LegacyAuthSessionRecord) -> Result<(), PortError> {
            Ok(())
        }
        fn get_legacy_session(
            &self,
            _token: &str,
        ) -> Result<Option<LegacyAuthSessionRecord>, PortError> {
            Ok(None)
        }
        fn take_legacy_session(
            &self,
            _token: &str,
        ) -> Result<Option<LegacyAuthSessionRecord>, PortError> {
            Ok(None)
        }
    }

    impl StreamingSink for Mem {
        fn publish(&self, _event_type: &str, _payload: &[u8]) -> Result<(), PortError> {
            Ok(())
        }
    }

    fn test_state(public_url: &str) -> (Arc<AppServices>, Arc<Mem>) {
        let clock = Arc::new(ManualClock::new(1_760_400_000_123));
        let ids = Arc::new(AidxGenerator::with_node_id(clock.clone(), "test", 0));
        let mem = Arc::new(Mem::empty());
        let services = Arc::new(AppServices {
            clock: clock.clone(),
            ids: ids.clone(),
            accounts: mem.clone(),
            notes: mem.clone(),
            follows: mem.clone(),
            blocks: mem.clone(),
            mutes: mem.clone(),
            reactions: mem.clone(),
            favorites: mem.clone(),
            emojis: mem.clone(),
            antennas: mem.clone(),
            antenna_notes: mem.clone(),
            polls: mem.clone(),
            jobs: mem.clone(),
            media: mem.clone(),
            blobs: mem.clone(),
            notifications: mem.clone(),
            auth: mem.clone(),
            streaming: mem.clone(),
            dedup: mem.clone() as Arc<dyn ActivityDedup>,
            host_caps: mem.clone(),
            moderation: mem.clone(),
            instance_settings: mem.clone(),
            invites: mem.clone(),
            abuse_reports: mem.clone(),
            federation_http: SafeHttpClient::new(SafeHttpConfig {
                allow_private_ips: true,
                ..Default::default()
            }),
            public_url: public_url.to_string(),
            max_file_size: 10 * 1024 * 1024,
            preserved_usernames: Vec::new(),
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
            note_draft_limit: crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
            scheduled_note_limit: crate::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
            pin_limit: crate::domain::pinning::DEFAULT_PIN_LIMIT,
        });
        (services, mem)
    }

    fn alice(id: &str) -> Account {
        Account::new_local(id.to_string(), "alice".to_string()).unwrap()
    }

    fn local_note(id: &str, author_id: &str) -> Note {
        Note {
            id: id.to_string(),
            author_id: author_id.to_string(),
            text: Some("hello".to_string()),
            visibility: Visibility::Public,
            ..Default::default()
        }
    }

    #[test]
    fn extract_host_handles_ports_and_paths() {
        assert_eq!(extract_host("https://example.local"), "example.local");
        assert_eq!(extract_host("https://example.local/"), "example.local");
        assert_eq!(
            extract_host("https://Example.LOCAL:3000/base"),
            "example.local:3000"
        );
    }

    #[test]
    fn all_resource_forms_resolve_to_canonical_actor_url() {
        let (state, mem) = test_state("https://example.local");
        let account = alice("aid00000000000001");
        mem.insert_account(account.clone());
        let services = &state;
        let canonical = "https://example.local/users/aid00000000000001";

        let forms = [
            "acct:alice@example.local".to_string(),
            "alice@example.local".to_string(),
            "ACCT:ALICE@EXAMPLE.LOCAL".to_string(),
            "https://example.local/users/aid00000000000001".to_string(),
            "https://example.local/@alice".to_string(),
            "alice".to_string(),
            "acct:alice".to_string(),
        ];
        for resource in forms {
            let resolved = resolve_webfinger_resource(services, &resource)
                .unwrap_or_else(|_| panic!("expected {resource} to resolve"));
            assert_eq!(resolved.actor_url(&services.public_url), canonical, "{resource}");
        }

        // JRD aliases point at the canonical actor URL; no node hostname leaks.
        let jrd = render_webfinger_jrd(&account, &services.public_url);
        assert_eq!(jrd["subject"], "acct:alice@example.local");
        let aliases: Vec<String> =
            serde_json::from_value(jrd["aliases"].clone()).expect("aliases array");
        assert!(aliases.contains(&canonical.to_string()));
        let self_link = jrd["links"]
            .as_array()
            .unwrap()
            .iter()
            .find(|link| link["rel"] == "self")
            .expect("self link");
        assert_eq!(self_link["href"], canonical);
    }

    #[test]
    fn system_actor_resolves_when_present() {
        let (state, mem) = test_state("https://example.local");
        let mut service = alice("aid00000000000009");
        service.username = "system.actor".to_string();
        service.username_lower = "system.actor".to_string();
        mem.insert_account(service);
        let resolved =
            resolve_webfinger_resource(&state, "acct:system.actor@example.local")
                .expect("system.actor resolves");
        assert_eq!(
            resolved.actor_url(&state.public_url),
            "https://example.local/users/aid00000000000009"
        );
    }

    #[test]
    fn wrong_host_is_unprocessable() {
        let (state, mem) = test_state("https://example.local");
        mem.insert_account(alice("aid00000000000001"));
        assert_eq!(
            resolve_webfinger_resource(&state, "acct:alice@other.example")
                .unwrap_err(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            resolve_webfinger_resource(&state, "alice@other.example").unwrap_err(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }

    #[test]
    fn unknown_suspended_deleted_or_remote_are_not_found() {
        let (state, mem) = test_state("https://example.local");
        mem.insert_account(alice("aid00000000000001"));

        assert_eq!(
            resolve_webfinger_resource(&state, "acct:bob@example.local").unwrap_err(),
            StatusCode::NOT_FOUND
        );

        let mut suspended = alice("aid00000000000002");
        suspended.username = "suspended".to_string();
        suspended.username_lower = "suspended".to_string();
        suspended.is_suspended = true;
        mem.insert_account(suspended);
        assert_eq!(
            resolve_webfinger_resource(&state, "acct:suspended@example.local")
                .unwrap_err(),
            StatusCode::NOT_FOUND
        );

        let mut deleted = alice("aid00000000000003");
        deleted.username = "deleted".to_string();
        deleted.username_lower = "deleted".to_string();
        deleted.is_deleted = true;
        mem.insert_account(deleted);
        assert_eq!(
            resolve_webfinger_resource(&state, "acct:deleted@example.local").unwrap_err(),
            StatusCode::NOT_FOUND
        );

        let mut remote = alice("aid00000000000004");
        remote.username = "remote".to_string();
        remote.username_lower = "remote".to_string();
        remote.host = Some("remote.example".to_string());
        mem.insert_account(remote);
        // Remote accounts share no username namespace with local webfinger.
        assert_eq!(
            resolve_webfinger_resource(&state, "acct:remote@example.local").unwrap_err(),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn content_negotiation_prefers_explicit_xrd() {
        assert!(!wants_xrd(None));
        assert!(!wants_xrd(Some("")));
        assert!(!wants_xrd(Some("*/*")));
        assert!(!wants_xrd(Some("application/jrd+json")));
        assert!(wants_xrd(Some("application/xrd+xml")));
        assert!(wants_xrd(Some(
            "application/xrd+xml, application/jrd+json;q=0.5"
        )));
        assert!(!wants_xrd(Some(
            "application/jrd+json, application/xrd+xml;q=0.5"
        )));
        assert!(!wants_xrd(Some("application/xrd+xml;q=0")));
    }

    #[tokio::test]
    async fn webfinger_handler_defaults_to_jrd_and_serves_xrd_on_demand() {
        let (state, mem) = test_state("https://example.local");
        mem.insert_account(alice("aid00000000000001"));

        let mut params = HashMap::new();
        params.insert("resource".to_string(), "acct:alice@example.local".to_string());

        // Default: JRD.
        let response = webfinger(
            State(state.clone()),
            Query(params.clone()),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["content-type"],
            CONTENT_TYPE_JRD
        );
        assert_eq!(response.headers()["vary"], "Accept");
        assert_eq!(response.headers()["access-control-allow-origin"], "*");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let jrd: Value = serde_json::from_slice(&body).expect("valid JRD JSON");
        assert_eq!(jrd["subject"], "acct:alice@example.local");

        // Explicit XRD preference: XML.
        let mut accept = HeaderMap::new();
        accept.insert(header::ACCEPT, HeaderValue::from_static("application/xrd+xml"));
        let response = webfinger(State(state.clone()), Query(params.clone()), accept).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["content-type"],
            CONTENT_TYPE_XRD
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let xml = String::from_utf8(body.to_vec()).expect("utf-8 xml");
        assert!(xml.contains("<XRD"));
        assert!(xml.contains("https://example.local/users/aid00000000000001"));

        // Missing resource: 400. Unknown user: 404. Wrong host: 422.
        let response = webfinger(State(state.clone()), Query(HashMap::new()), HeaderMap::new()).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let mut unknown = HashMap::new();
        unknown.insert("resource".to_string(), "acct:nobody@example.local".to_string());
        let response = webfinger(State(state.clone()), Query(unknown), HeaderMap::new()).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let mut foreign = HashMap::new();
        foreign.insert("resource".to_string(), "acct:alice@other.example".to_string());
        let response = webfinger(State(state), Query(foreign), HeaderMap::new()).await;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[test]
    fn xrd_escapes_xml_specials() {
        let (state, _) = test_state("https://example.local");
        // Usernames cannot contain `&`, so exercise the escaper through a
        // hand-mutated account instead of store round-tripping.
        let mut tricky = alice("aid00000000000001");
        tricky.username = "alice&bob".to_string();
        let xml = render_webfinger_xrd(&tricky, &state.public_url);
        assert!(xml.contains("acct:alice&amp;bob@example.local"));
        assert!(!xml.contains("alice&bob"));
    }

    #[tokio::test]
    async fn host_meta_shapes() {
        let (state, _) = test_state("https://example.local");

        let response = host_meta_xrd(State(state.clone())).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], CONTENT_TYPE_XRD);
        assert_eq!(response.headers()["access-control-allow-origin"], "*");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let xml = String::from_utf8(body.to_vec()).unwrap();
        assert!(xml.contains(
            "template=\"https://example.local/.well-known/webfinger?resource={uri}\""
        ));

        let response = host_meta_json(State(state.clone())).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], CONTENT_TYPE_JRD);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let jrd: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            jrd["links"][0]["template"],
            "https://example.local/.well-known/webfinger?resource={uri}"
        );
    }

    #[tokio::test]
    async fn nodeinfo_shapes_and_counts() {
        let (state, mem) = test_state("https://example.local");
        mem.insert_account(alice("aid00000000000001"));
        let mut remote = alice("aid00000000000002");
        remote.username = "bob".to_string();
        remote.username_lower = "bob".to_string();
        remote.host = Some("remote.example".to_string());
        mem.insert_account(remote);
        mem.insert_note(local_note("note1", "aid00000000000001"));
        mem.insert_note(local_note("note2", "aid00000000000001"));
        let mut remote_note = local_note("note3", "aid00000000000002");
        remote_note.user_host = Some("remote.example".to_string());
        mem.insert_note(remote_note);

        let response = well_known_nodeinfo(State(state.clone())).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let doc: Value = serde_json::from_slice(&body).unwrap();
        let rels: HashSet<String> = doc["links"]
            .as_array()
            .unwrap()
            .iter()
            .map(|link| link["rel"].as_str().unwrap().to_string())
            .collect();
        assert!(rels.contains("http://nodeinfo.diaspora.software/ns/schema/2.0"));
        assert!(rels.contains("http://nodeinfo.diaspora.software/ns/schema/2.1"));

        let response = nodeinfo_2_0(State(state.clone())).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], CONTENT_TYPE_NODEINFO_2_0);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let doc: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(doc["version"], "2.0");
        assert_eq!(doc["software"], json!({"name": "misskey", "version": PACKER_VERSION}));
        assert_eq!(doc["protocols"], json!(["activitypub"]));
        assert_eq!(doc["services"]["outbound"], json!(["atom1.0", "rss2.0"]));
        assert_eq!(doc["openRegistrations"], true);
        // Only local users and local notes are counted.
        assert_eq!(doc["usage"]["users"]["total"], 1);
        assert_eq!(doc["usage"]["localPosts"], 2);
        assert_eq!(doc["usage"]["localComments"], 0);
        assert_eq!(doc["metadata"]["nodeName"], "Pinetree");
        assert_eq!(doc["metadata"]["maxNoteTextLength"], 3000);

        let response = nodeinfo_2_1(State(state.clone())).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], CONTENT_TYPE_NODEINFO_2_1);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let doc: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(doc["version"], "2.1");
        assert_eq!(doc["software"]["name"], "misskey");
        assert_eq!(doc["software"]["version"], PACKER_VERSION);
        assert_eq!(doc["protocols"], json!(["activitypub"]));
        assert_eq!(doc["services"]["outbound"], json!(["atom1.0", "rss2.0"]));
        assert_eq!(doc["openRegistrations"], true);
        assert_eq!(doc["usage"]["users"]["total"], 1);
        assert_eq!(doc["usage"]["localPosts"], 2);
        assert_eq!(doc["usage"]["localComments"], 0);
        assert_eq!(doc["metadata"]["nodeName"], "Pinetree");
        assert_eq!(doc["metadata"]["maxNoteTextLength"], 3000);

        // 2.1 carries the extra software link fields.
        let doc21 = render_nodeinfo(&state, "2.1");
        assert_eq!(doc21["software"]["homepage"], "https://misskey-hub.net");
        assert!(doc21["software"].get("repository").is_some());
    }
}
