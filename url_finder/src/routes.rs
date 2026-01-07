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
    // Rate limiting: 200 req/s sustained, burst of 100
    let governor_config = Arc::new(
        GovernorConfigBuilder::default()
            .per_millisecond(5) // ~200 req/s
            .burst_size(100)
            .key_extractor(SmartIpKeyExtractor)
            .finish()
            .unwrap(),
    );

    let governor_limiter = governor_config.limiter().clone();
    let interval = Duration::from_secs(60);

    // background task to clean up
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            tracing::debug!(
                "rate governor_limiter storage size: {}",
                governor_limiter.len()
            );
            governor_limiter.retain_recent();
        }
    });

    let swagger_routes = SwaggerUi::new("/").url("/api-doc/openapi.json", ApiDoc::openapi());

    let legacy_api_routes = Router::new()
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
            GovernorLayer::new(governor_config.clone())
                .error_handler(too_many_requests_error_handler),
        );

    let providers_api_routes = Router::new()
        .route("/providers", get(providers::handle_list_providers))
        .route("/providers/bulk", post(providers::handle_bulk_providers))
        .route("/providers/{id}", get(providers::handle_get_provider))
        .route(
            "/providers/{id}/reset",
            post(providers::handle_reset_provider),
        )
        .route(
            "/providers/{id}/clients/{client_id}",
            get(providers::handle_get_provider_client),
        )
        .route(
            "/clients/{id}/providers",
            get(providers::handle_get_client_providers),
        )
        .layer(
            GovernorLayer::new(governor_config.clone())
                .error_handler(too_many_requests_error_handler),
        );

    let healthcheck_route = Router::new()
        .route("/healthcheck", get(handle_healthcheck))
        .layer(
            GovernorLayer::new(governor_config.clone())
                .error_handler(too_many_requests_error_handler),
        );

    Router::new()
        .merge(swagger_routes)
        .merge(legacy_api_routes)
        .merge(providers_api_routes)
        .merge(healthcheck_route)
}
