use super::connection::run_connection;
use crate::api::misskey::auth::{authenticate, extract_raw_token};
use crate::api::misskey::endpoints::ApiState;
use crate::api::misskey::errors::ApiError;
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use std::collections::HashMap;

/// Owned caller snapshot for a streaming connection.
#[derive(Debug, Clone)]
pub(crate) struct CallerInfo {
    pub(crate) account_id: String,
    pub(crate) is_first_party: bool,
}

/// `GET /streaming` upgrade handler.
pub async fn streaming_handler(
    State(state): State<ApiState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let raw = token_for_upgrade(&headers, &query);
    let caller = match raw {
        None => None,
        Some(token) => {
            let services = state.services.clone();
            let resolved =
                tokio::task::spawn_blocking(move || authenticate(&services, &token)).await;
            match resolved {
                Err(_) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "streaming auth task failed",
                    )
                        .into_response();
                }
                Ok(Err(err)) => return err.into_response(),
                Ok(Ok(authenticated)) => {
                    // Misskey answers 401 (not 403) when the app token may
                    // not use the streaming API at all.
                    if authenticated.require_permission("read:account").is_err() {
                        return ApiError::authentication_failed().into_response();
                    }
                    Some(CallerInfo {
                        account_id: authenticated.account.id,
                        is_first_party: authenticated.is_first_party,
                    })
                }
            }
        }
    };
    ws.max_message_size(MAX_WS_MESSAGE_BYTES)
        .on_upgrade(move |socket| run_connection(socket, state, caller))
}

/// Extract the raw token for tests: `Authorization: Bearer` first, then the
/// `i` query parameter.
pub fn token_for_upgrade(headers: &HeaderMap, query: &HashMap<String, String>) -> Option<String> {
    extract_raw_token(
        headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok()),
        None,
        None,
    )
    .or_else(|| query.get("i").cloned().filter(|t| !t.is_empty()))
}

const MAX_WS_MESSAGE_BYTES: usize = 64 * 1024;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upgrade_token_prefers_bearer_over_query() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer header-token".parse().unwrap(),
        );
        let query: HashMap<String, String> = [("i".to_string(), "query-token".to_string())]
            .into_iter()
            .collect();
        assert_eq!(
            token_for_upgrade(&headers, &query),
            Some("header-token".to_string())
        );
        assert_eq!(
            token_for_upgrade(&HeaderMap::new(), &query),
            Some("query-token".to_string())
        );
        assert_eq!(token_for_upgrade(&HeaderMap::new(), &HashMap::new()), None);
    }
}
