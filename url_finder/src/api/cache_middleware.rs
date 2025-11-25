use crate::api_response::*;
use axum::{
    body::{self, Body},
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde_json::Value;
use std::sync::Arc;
use tokio::time::Instant;
use tracing::{debug, error};

use crate::AppState;

async fn cache_response(state: Arc<AppState>, cache_key: String, response: Response) -> Response {
    if response.status() != StatusCode::OK {
        return response;
    }

    let (parts, body) = response.into_parts();
    let body_bytes = match body::to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(e) => {
            error!("failed to read response body: {:?}", e);
            return internal_server_error(format!("Error processing response: {e}"))
                .into_response();
        }
    };

    if let Ok(json) = serde_json::from_slice::<Value>(&body_bytes) {
        state.cache.insert(cache_key, json).await;
    }

    Response::from_parts(parts, Body::from(body_bytes))
}

pub async fn cache_middleware(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Response {
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();
    let cache_key = format!("cache_{path}_{query}");

    if let Some(cached_json) = state.cache.get(&cache_key).await {
        debug!("cache hit for key: {}", cache_key);
        return ok_response(cached_json).into_response();
    }

    let start = Instant::now();
    let response = next.run(req).await;
    debug!("request duration time: {:?}", start.elapsed());

    cache_response(state, cache_key, response).await
}
