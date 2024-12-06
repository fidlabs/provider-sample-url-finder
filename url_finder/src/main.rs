use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use axum::{
    extract::{Request, State},
    middleware::{self, Next},
    response::Response,
    routing::post,
    Router,
};
use color_eyre::Result;
use config::CONFIG;
use deal_repo::DealRepository;
use tokio::{
    net::TcpListener,
    signal::unix::{signal, SignalKind},
};
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::api::*;

mod api;
mod cid_contact;
mod config;
mod deal_repo;
mod deal_service;
mod lotus_rpc;
mod multiaddr_parser;
mod pix_filspark;
mod url_tester;

pub struct AppState {
    pub deal_repo: DealRepository,
    pub active_requests: Arc<AtomicUsize>,
}

/// Active requests counter middleware.
/// Keeps track of the number of active requests.
/// The counter is used to gracefully shutdown the server.
async fn request_counter(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    state.active_requests.fetch_add(1, Ordering::SeqCst);
    let response = next.run(request).await;
    state.active_requests.fetch_sub(1, Ordering::SeqCst);

    response
}

#[tokio::main]
async fn main() -> Result<()> {
    info!("UrlFinder is starting...");

    color_eyre::install()?;

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new(CONFIG.log_level.clone())),
        )
        .init();

    let pool = sqlx::PgPool::connect(&CONFIG.db_url).await?;

    let active_requests = Arc::new(AtomicUsize::new(0));
    let app_state = Arc::new(AppState {
        deal_repo: DealRepository::new(pool),
        active_requests: active_requests.clone(),
    });

    let app = Router::new()
        .route("/url/find", post(handle_find_url))
        .merge(SwaggerUi::new("/swagger-ui").url("/api-doc/openapi.json", ApiDoc::openapi()))
        .layer(middleware::from_fn_with_state(
            app_state.clone(),
            request_counter,
        ))
        .with_state(app_state.clone());

    let server_addr = "0.0.0.0:3010".to_string();
    let listener = TcpListener::bind(&server_addr).await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(active_requests.clone()))
        .await?;

    info!("UrlFinder shut down gracefully");

    Ok(())
}

async fn shutdown_signal(active_requests: Arc<AtomicUsize>) {
    let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT signal handler failed");
    let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM signal handler failed");

    tokio::select! {
        _ = sigint.recv() => {
            info!("Received SIGINT signal, shutting down...");
        }
        _ = sigterm.recv() => {
            info!("Received SIGTERM signal, shutting down...");
        }
    }

    while active_requests.load(Ordering::SeqCst) > 0 {
        debug!(
            "Waiting for {} active requests to finish...",
            active_requests.load(Ordering::SeqCst)
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    info!("All active requests have been completed");
}
