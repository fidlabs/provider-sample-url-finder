use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use axum::{
    extract::{Request, State},
    middleware::{self, Next},
    response::Response,
};
use color_eyre::Result;
use config::CONFIG;
use moka::future::Cache;
use repository::DealRepository;
use routes::create_routes;
use tokio::{
    net::TcpListener,
    signal::unix::{SignalKind, signal},
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
mod lotus_rpc;
mod multiaddr_parser;
mod pix_filspark;
mod provider_endpoints;
mod repository;
mod routes;
mod services;
mod types;
mod url_tester;
mod utils;

pub struct AppState {
    pub deal_repo: Arc<DealRepository>,
    pub active_requests: Arc<AtomicUsize>,
    pub job_repo: Arc<JobRepository>,
    pub storage_provider_repo: Arc<StorageProviderRepository>,
    pub cache: Cache<String, serde_json::Value>,
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
    dotenvy::dotenv().ok();

    info!("UrlFinder is starting...");

    color_eyre::install()?;

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new(CONFIG.log_level.clone())),
        )
        .init();

    let pool = sqlx::PgPool::connect(&CONFIG.db_url).await?;
    let dmob_pool = sqlx::PgPool::connect(&CONFIG.dmob_db_url).await?;

    info!("Running database migrations...");
    sqlx::migrate!("../migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");
    info!("Database migrations applied successfully");

    let active_requests = Arc::new(AtomicUsize::new(0));

    let cache: Cache<String, serde_json::Value> = Cache::builder()
        .max_capacity(100_000) // arbitrary number, 6x current sp and client pair count, adjust as needed
        .time_to_live(std::time::Duration::from_secs(60 * 60 * 23)) // 23 hours, just shy of 1 day
        .build();

    let sp_repo = Arc::new(StorageProviderRepository::new(pool.clone()));
    let deal_repo = Arc::new(DealRepository::new(dmob_pool.clone()));
    let url_repo = Arc::new(UrlResultRepository::new(pool.clone()));

    let app_state = Arc::new(AppState {
        deal_repo: deal_repo.clone(),
        active_requests: active_requests.clone(),
        job_repo: Arc::new(JobRepository::new()),
        storage_provider_repo: sp_repo.clone(),
        cache,
    });

    // Start the provider discovery in the background
    tokio::spawn({
        let sp_repo = sp_repo.clone();
        let deal_repo = deal_repo.clone();
        async move {
            background::run_provider_discovery(sp_repo, deal_repo).await;
        }
    });

    // Start the URL discovery scheduler in the background
    tokio::spawn({
        let sp_repo = sp_repo.clone();
        let url_repo = url_repo.clone();
        let deal_repo = deal_repo.clone();
        async move {
            background::run_url_discovery_scheduler(sp_repo, url_repo, deal_repo).await;
        }
    });

    // Start the job handler in the background
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

    let app = create_routes(app_state.clone())
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
