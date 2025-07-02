use std::{sync::Arc, time::Duration};

use axum::{
    body::Body,
    http::Response,
    middleware,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use common::api_response::*;
use tower_governor::{
    governor::GovernorConfigBuilder, key_extractor::SmartIpKeyExtractor, GovernorError,
    GovernorLayer,
};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::{api::*, AppState};

fn too_many_requests_error_handler(error: GovernorError) -> Response<Body> {
    tracing::error!("Rate limit error: {:?}", error);

    match error {
        GovernorError::TooManyRequests { .. } => {
            too_many_requests("Rate limit exceeded").into_response()
        }
        _ => internal_server_error("Rate limit error").into_response(),
    }
}

pub fn create_routes(app_state: Arc<AppState>) -> Router<Arc<AppState>> {
    // more strict rate limiting for the sync routes
    let governor_secure = Arc::new(
        GovernorConfigBuilder::default()
            .per_millisecond(30)
            .burst_size(30)
            .key_extractor(SmartIpKeyExtractor)
            .error_handler(too_many_requests_error_handler)
            .finish()
            .unwrap(),
    );

    // less strict rate limiting for the async routes that will have internal queue anyway
    let governor_async = Arc::new(
        GovernorConfigBuilder::default()
            .per_millisecond(30)
            .burst_size(30)
            .key_extractor(SmartIpKeyExtractor)
            .error_handler(too_many_requests_error_handler)
            .finish()
            .unwrap(),
    );

    let governor_secure_limiter = governor_secure.limiter().clone();
    let governor_async_limiter = governor_async.limiter().clone();

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
        .route("/url/find/:provider", get(handle_find_url_sp))
        .route(
            "/url/find/:provider/:client",
            get(handle_find_url_sp_client),
        )
        .route(
            "/url/retrievability/:provider/:client",
            get(handle_find_retri_by_client_and_sp),
        )
        .route(
            "/url/retrievability/:provider",
            get(handle_find_retri_by_sp),
        )
        .layer(middleware::from_fn_with_state(
            app_state.clone(),
            cache_middleware,
        ))
        .layer(GovernorLayer {
            config: governor_secure.clone(),
        });

    let async_routes = Router::new()
        .route("/jobs/:id", get(handle_get_job))
        .route("/jobs", post(handle_create_job))
        .layer(GovernorLayer {
            config: governor_async.clone(),
        });

    let healthcheck_route = Router::new()
        .route("/healthcheck", get(handle_healthcheck))
        .layer(GovernorLayer {
            config: governor_secure.clone(),
        });

    Router::new()
        .merge(swagger_routes)
        .merge(sync_routes)
        .merge(async_routes)
        .merge(healthcheck_route.clone())
}
