use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use axum::{
    extract::{Request, State},
    middleware::{self, Next},
    response::Response,
};
use color_eyre::Result;
use config::CONFIG;
use deal_repo::DealRepository;
use routes::create_routes;
use tokio::{
    net::TcpListener,
    signal::unix::{signal, SignalKind},
};
use tower_http::cors::{Any, CorsLayer};
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;

use crate::api::*;
use crate::repository::*;

mod api;
mod background;
mod cid_contact;
mod config;
mod deal_repo;
mod deal_service;
mod lotus_rpc;
mod multiaddr_parser;
mod pix_filspark;
mod provider_endpoints;
mod repository;
mod routes;
mod url_tester;

pub struct AppState {
    pub deal_repo: Arc<DealRepository>,
    pub active_requests: Arc<AtomicUsize>,
    pub job_repo: Arc<JobRepository>,
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
        deal_repo: Arc::new(DealRepository::new(pool)),
        active_requests: active_requests.clone(),
        job_repo: Arc::new(JobRepository::new()),
    });

    // start the job handler in the background
    tokio::spawn(background::job_handler(
        app_state.job_repo.clone(),
        app_state.deal_repo.clone(),
    ));

    let allowed_origins = ["https://sp-tool.allocator.tech".parse().unwrap()];
    let cors = CorsLayer::new()
        .allow_origin(allowed_origins)
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = create_routes()
        .layer(middleware::from_fn_with_state(
            app_state.clone(),
            request_counter,
        ))
        .layer(cors)
        .with_state(app_state.clone());

    let server_addr = SocketAddr::from(([0, 0, 0, 0], 3010));
    let listener = TcpListener::bind(&server_addr).await?;

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
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
