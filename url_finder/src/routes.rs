use std::{sync::Arc, time::Duration};

use axum::{
    Router,
    body::Body,
    http::Response,
    response::IntoResponse,
    routing::{get, post, put},
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

// Per-IP rate limits. Each route class has its own limiter, so one class cannot drain another's bucket.
const READ_RATE_PERIOD: Duration = Duration::from_millis(5); // ~200 req/s
const READ_RATE_BURST: u32 = 500;
const DEAL_UPSERT_RATE_PERIOD: Duration = Duration::from_millis(5); // ~200 req/s
const DEAL_UPSERT_RATE_BURST: u32 = 100;
const DEAL_RUN_RATE_PERIOD: Duration = Duration::from_secs(12);
const DEAL_RUN_RATE_BURST: u32 = 5;
const RATE_LIMITER_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

fn rate_limited(
    routes: Router<Arc<AppState>>,
    name: &'static str,
    period: Duration,
    burst_size: u32,
) -> Router<Arc<AppState>> {
    let governor_config = Arc::new(
        GovernorConfigBuilder::default()
            .period(period)
            .burst_size(burst_size)
            .key_extractor(SmartIpKeyExtractor)
            .finish()
            .unwrap(),
    );

    let governor_limiter = governor_config.limiter().clone();

    // background task to clean up
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(RATE_LIMITER_CLEANUP_INTERVAL).await;
            tracing::debug!(
                "rate governor_limiter {name} storage size: {}",
                governor_limiter.len()
            );
            governor_limiter.retain_recent();
        }
    });

    routes.layer(GovernorLayer::new(governor_config).error_handler(too_many_requests_error_handler))
}

pub fn create_routes() -> Router<Arc<AppState>> {
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
        .route("/url/client/{client}", get(handle_find_client));

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
            "/providers/{id}/history/retrievability",
            get(providers::handle_history_retrievability),
        )
        .route(
            "/providers/{id}/clients/{client_id}/history/retrievability",
            get(providers::handle_history_retrievability_client),
        )
        .route(
            "/providers/{id}/history/sli",
            get(providers::handle_history_sli),
        )
        .route(
            "/providers/{id}/clients/{client_id}/history/sli",
            get(providers::handle_history_sli_client),
        )
        .route(
            "/clients/{id}/providers",
            get(providers::handle_get_client_providers),
        );

    let deals_read_routes = Router::new()
        .route("/deals/{deal_id}", get(deals::handle_get_deal))
        .route("/deals/{deal_id}/latest", get(deals::handle_get_latest))
        .route(
            "/deals/{deal_id}/history/sli",
            get(deals::handle_deal_history_sli),
        )
        .route(
            "/deals/{deal_id}/runs/{run_id}",
            get(deals::handle_get_deal_run),
        )
        .route(
            "/deals/{deal_id}/runs/{run_id}/bms",
            get(deals::handle_get_deal_run_bms),
        );

    let healthcheck_route = Router::new().route("/healthcheck", get(handle_healthcheck));

    let deal_upsert_route = Router::new().route("/deals/{deal_id}", put(deals::handle_upsert_deal));

    let deal_run_route =
        Router::new().route("/deals/{deal_id}/runs", post(deals::handle_create_run));

    let read_routes = Router::new()
        .merge(legacy_api_routes)
        .merge(providers_api_routes)
        .merge(deals_read_routes)
        .merge(healthcheck_route);

    Router::new()
        .merge(swagger_routes)
        .merge(rate_limited(
            read_routes,
            "read",
            READ_RATE_PERIOD,
            READ_RATE_BURST,
        ))
        .merge(rate_limited(
            deal_upsert_route,
            "deal_upsert",
            DEAL_UPSERT_RATE_PERIOD,
            DEAL_UPSERT_RATE_BURST,
        ))
        .merge(rate_limited(
            deal_run_route,
            "deal_run",
            DEAL_RUN_RATE_PERIOD,
            DEAL_RUN_RATE_BURST,
        ))
}
