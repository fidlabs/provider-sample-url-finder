use std::{sync::Arc, time::Duration};

use axum::{
    Router,
    body::Body,
    http::Response,
    response::IntoResponse,
    routing::{get, post},
};
use tower_governor::{
    GovernorError, GovernorLayer, governor::GovernorConfigBuilder,
    key_extractor::SmartIpKeyExtractor,
};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::{AppState, api::*, api_response::*};

fn too_many_requests_error_handler(error: GovernorError) -> Response<Body> {
    match error {
        GovernorError::TooManyRequests { .. } => {
            tracing::warn!("Rate limit hit: {:?}", error);
            too_many_requests("Rate limit exceeded").into_response()
        }
        _ => {
            tracing::error!("Rate limit error: {:?}", error);
            internal_server_error("Rate limit error").into_response()
        }
    }
}

pub fn create_routes() -> Router<Arc<AppState>> {
    // more strict rate limiting for the sync routes
    let governor_secure_config = Arc::new(
        GovernorConfigBuilder::default()
            .per_millisecond(30)
            .burst_size(30)
            .key_extractor(SmartIpKeyExtractor)
            .finish()
            .unwrap(),
    );

    // less strict rate limiting for the async routes that will have internal queue anyway
    let governor_async_config = Arc::new(
        GovernorConfigBuilder::default()
            .per_millisecond(30)
            .burst_size(30)
            .key_extractor(SmartIpKeyExtractor)
            .finish()
            .unwrap(),
    );

    let governor_secure_limiter = governor_secure_config.limiter().clone();
    let governor_async_limiter = governor_async_config.limiter().clone();

    let interval = Duration::from_secs(60);

    // background task to clean up
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;

            tracing::debug!(
                "rate governor_secure_limiter storage size: {}",
                governor_secure_limiter.len()
            );
            tracing::debug!(
                "rate governor_async_limiter storage size: {}",
                governor_async_limiter.len()
            );

            governor_secure_limiter.retain_recent();
            governor_async_limiter.retain_recent();
        }
    });

    let swagger_routes = SwaggerUi::new("/").url("/api-doc/openapi.json", ApiDoc::openapi());

    let sync_routes = Router::new()
        .route("/url/find/{provider}", get(handle_find_url_sp))
        .route(
            "/url/find/{provider}/{client}",
            get(handle_find_url_sp_client),
        )
        .route(
            "/url/retrievability/{provider}/{client}",
            get(handle_find_retri_by_client_and_sp),
        )
        .route(
            "/url/retrievability/{provider}",
            get(handle_find_retri_by_sp),
        )
        .route("/url/client/{client}", get(handle_find_client))
        .layer(
            GovernorLayer::new(governor_secure_config.clone())
                .error_handler(too_many_requests_error_handler),
        );

    let async_routes = Router::new()
        .route("/jobs/{id}", get(handle_get_job))
        .route("/jobs", post(handle_create_job))
        .layer(
            GovernorLayer::new(governor_async_config.clone())
                .error_handler(too_many_requests_error_handler),
        );

    let healthcheck_route = Router::new()
        .route("/healthcheck", get(handle_healthcheck))
        .layer(
            GovernorLayer::new(governor_secure_config.clone())
                .error_handler(too_many_requests_error_handler),
        );

    Router::new()
        .merge(swagger_routes)
        .merge(sync_routes)
        .merge(async_routes)
        .merge(healthcheck_route)
}
