use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    extract::{Request, State},
    middleware::{self, Next},
    response::Response,
};
use color_eyre::Result;
use tokio::{
    net::TcpListener,
    signal::unix::{SignalKind, signal},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tower_http::cors::{Any, CorsLayer};
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

use url_finder::{AppState, background, config::Config, repository::*, routes::create_routes};

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

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

    let config = Arc::new(Config::new_from_env()?);

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new(config.log_level.clone())),
        )
        .init();

    let pool = sqlx::PgPool::connect(&config.db_url).await?;
    let dmob_pool = sqlx::PgPool::connect(&config.dmob_db_url).await?;

    info!("Running database migrations...");
    sqlx::migrate!("../migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");
    info!("Database migrations applied successfully");

    let active_requests = Arc::new(AtomicUsize::new(0));
    let shutdown_token = CancellationToken::new();

    let sp_repo = Arc::new(StorageProviderRepository::new(pool.clone()));
    let deal_repo = Arc::new(DealRepository::new(dmob_pool.clone()));
    let deal_label_repo = Arc::new(DealLabelRepository::new(pool.clone()));
    let url_repo = Arc::new(UrlResultRepository::new(pool.clone()));
    let bms_result_repo = Arc::new(BmsBandwidthResultRepository::new(pool.clone()));
    let bms_client = Arc::new(url_finder::bms_client::BmsClient::new(
        config.bms_url.clone(),
    ));

    let provider_service = Arc::new(
        url_finder::services::provider_service::ProviderService::new(
            url_repo.clone(),
            bms_result_repo.clone(),
            sp_repo.clone(),
        ),
    );

    let app_state = Arc::new(AppState {
        deal_repo: deal_repo.clone(),
        deal_label_repo: deal_label_repo.clone(),
        active_requests: active_requests.clone(),
        storage_provider_repo: sp_repo.clone(),
        url_repo: url_repo.clone(),
        bms_repo: bms_result_repo.clone(),
        provider_service,
        config: config.clone(),
    });

    // Start the provider discovery in the background
    let provider_discovery_handle: JoinHandle<()> = tokio::spawn({
        let sp_repo = sp_repo.clone();
        let deal_repo = deal_repo.clone();
        let shutdown = shutdown_token.clone();
        async move {
            background::run_provider_discovery(sp_repo, deal_repo, shutdown).await;
        }
    });

    // Start the URL discovery scheduler in the background
    let url_discovery_handle: JoinHandle<()> = tokio::spawn({
        let sp_repo = sp_repo.clone();
        let url_repo = url_repo.clone();
        let deal_repo = deal_repo.clone();
        let deal_label_repo = deal_label_repo.clone();
        let config = config.clone();
        let shutdown = shutdown_token.clone();
        async move {
            background::run_url_discovery_scheduler(
                config,
                sp_repo,
                url_repo,
                deal_repo,
                deal_label_repo,
                shutdown,
            )
            .await;
        }
    });

    // Start the BMS scheduler in the background
    let bms_circuit_breaker = Arc::new(background::create_bms_circuit_breaker());
    let bms_scheduler_handle: JoinHandle<()> = tokio::spawn({
        let config = config.clone();
        let sp_repo = sp_repo.clone();
        let bms_result_repo = bms_result_repo.clone();
        let bms_client = bms_client.clone();
        let bms_circuit_breaker = bms_circuit_breaker.clone();
        let shutdown = shutdown_token.clone();
        async move {
            background::run_bms_scheduler(
                config,
                bms_client,
                bms_circuit_breaker,
                sp_repo,
                bms_result_repo,
                shutdown,
            )
            .await;
        }
    });

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
    .with_graceful_shutdown(shutdown_signal(
        active_requests.clone(),
        shutdown_token.clone(),
    ))
    .await?;

    // Await background task completion with timeout
    info!("Waiting for background tasks to complete...");
    let background_handles = vec![
        ("provider_discovery", provider_discovery_handle),
        ("url_discovery", url_discovery_handle),
        ("bms_scheduler", bms_scheduler_handle),
    ];

    for (name, handle) in background_handles {
        match tokio::time::timeout(SHUTDOWN_TIMEOUT, handle).await {
            Ok(Ok(())) => info!("Background task '{name}' completed successfully"),
            Ok(Err(e)) => error!("Background task '{name}' panicked: {e:?}"),
            Err(_) => warn!("Background task '{name}' did not complete within timeout"),
        }
    }

    info!("UrlFinder shut down gracefully");

    Ok(())
}

async fn shutdown_signal(active_requests: Arc<AtomicUsize>, shutdown_token: CancellationToken) {
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

    // Signal background tasks to stop
    info!("Signaling background tasks to stop...");
    shutdown_token.cancel();

    while active_requests.load(Ordering::SeqCst) > 0 {
        debug!(
            "Waiting for {} active requests to finish...",
            active_requests.load(Ordering::SeqCst)
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    info!("All active requests have been completed");
}
