#![allow(dead_code)]

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, migrate::MigrateDatabase};

pub use super::container::{POSTGRES_PASSWORD, POSTGRES_USER};

pub struct TestDatabases {
    pub app_pool: PgPool,
    pub app_db_name: String,
    pub postgres_host: String,
}

pub async fn setup_test_db_with_port(port: u16) -> TestDatabases {
    let postgres_host = format!("localhost:{port}");
    // test function name
    let test_name = std::thread::current()
        .name()
        .unwrap_or("unknown")
        .rsplit("::")
        .next()
        .unwrap_or("unknown")
        .to_lowercase();

    let app_db_name = format!("test_{test_name}");
    let app_db_url =
        format!("postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{postgres_host}/{app_db_name}");

    if let Err(e) = Postgres::drop_database(&app_db_url).await {
        let err_str = e.to_string();
        // "database does not exist" is expected on first run - not an error
        if !err_str.contains("does not exist") {
            tracing::warn!("Failed to drop test database {app_db_name}: {e}");
        }
    }

    Postgres::create_database(&app_db_url)
        .await
        .expect("Failed to create test database");

    let app_pool = PgPool::connect(&app_db_url)
        .await
        .expect("Failed to connect to test database");

    sqlx::migrate!("../migrations")
        .run(&app_pool)
        .await
        .expect("Failed to run migrations");

    let test_schema = include_str!("test_schema.sql");
    sqlx::query(test_schema)
        .execute(&app_pool)
        .await
        .expect("Failed to apply test schema");

    TestDatabases {
        app_pool,
        app_db_name,
        postgres_host,
    }
}

pub async fn seed_provider(app_pool: &PgPool, provider_id: &str) {
    sqlx::query(
        r#"INSERT INTO
                storage_providers (
                    provider_id
                )
           VALUES
                ($1)
           ON CONFLICT (provider_id) DO NOTHING"#,
    )
    .bind(provider_id)
    .execute(app_pool)
    .await
    .expect("Failed to insert provider");
}

pub async fn seed_deals(
    app_pool: &PgPool,
    provider_id: &str,
    client_id: Option<&str>,
    piece_cids: Vec<&str>,
) {
    for piece_cid in piece_cids {
        sqlx::query(
            r#"INSERT INTO
                    unified_verified_deal (
                        "providerId",
                        "clientId",
                        "pieceCid"
                    )
               VALUES
                    ($1, $2, $3)
            "#,
        )
        .bind(provider_id)
        .bind(client_id)
        .bind(piece_cid)
        .execute(app_pool)
        .await
        .expect("Failed to insert deal");
    }
}

pub async fn seed_url_result(
    app_pool: &PgPool,
    provider_id: &str,
    client_id: Option<&str>,
    working_url: Option<&str>,
    retrievability: Option<f64>,
    result_code: &str,
) {
    if let Some(r) = retrievability {
        assert!(
            (0.0..=100.0).contains(&r),
            "retrievability must be in range 0..=100, got {r}"
        );
    }
    let result_type = if client_id.is_some() {
        "ProviderClient"
    } else {
        "Provider"
    };

    sqlx::query(
        r#"INSERT INTO
                url_results (
                    provider_id,
                    client_id,
                    result_type,
                    working_url,
                    retrievability_percent,
                    result_code,
                    tested_at
                )
           VALUES
                ($1, $2, $3::discovery_type, $4, $5, $6::result_code, NOW())"#,
    )
    .bind(provider_id)
    .bind(client_id)
    .bind(result_type)
    .bind(working_url)
    .bind(retrievability)
    .bind(result_code)
    .execute(app_pool)
    .await
    .expect("Failed to insert url_result");
}

pub async fn seed_provider_with_url_status(
    app_pool: &PgPool,
    provider_id: &str,
    last_working_url: Option<&str>,
    is_consistent: Option<bool>,
) {
    sqlx::query(
        r#"INSERT INTO
                storage_providers (
                    provider_id,
                    last_working_url,
                    is_consistent
                )
           VALUES
                ($1, $2, $3)
           ON CONFLICT (provider_id) DO UPDATE SET
                last_working_url = $2,
                is_consistent = $3"#,
    )
    .bind(provider_id)
    .bind(last_working_url)
    .bind(is_consistent)
    .execute(app_pool)
    .await
    .expect("Failed to insert provider with url status");
}

#[allow(clippy::too_many_arguments)]
pub async fn seed_bms_bandwidth_result(
    app_pool: &PgPool,
    provider_id: &str,
    url_tested: &str,
    status: &str,
    ping_avg_ms: Option<f64>,
    head_avg_ms: Option<f64>,
    ttfb_ms: Option<f64>,
    download_speed_mbps: Option<f64>,
) {
    sqlx::query(
        r#"INSERT INTO
                bms_bandwidth_results (
                    provider_id,
                    bms_job_id,
                    url_tested,
                    routing_key,
                    worker_count,
                    status,
                    ping_avg_ms,
                    head_avg_ms,
                    ttfb_ms,
                    download_speed_mbps,
                    completed_at
                )
           VALUES
                ($1, gen_random_uuid(), $2, 'test-region', 1, $3, $4, $5, $6, $7, NOW())"#,
    )
    .bind(provider_id)
    .bind(url_tested)
    .bind(status)
    .bind(ping_avg_ms)
    .bind(head_avg_ms)
    .bind(ttfb_ms)
    .bind(download_speed_mbps)
    .execute(app_pool)
    .await
    .expect("Failed to insert bms_bandwidth_result");
}

#[allow(clippy::too_many_arguments)]
pub async fn seed_url_result_with_metadata(
    app_pool: &PgPool,
    provider_id: &str,
    client_id: Option<&str>,
    working_url: Option<&str>,
    retrievability: Option<f64>,
    car_retrievability: Option<f64>,
    full_piece_retrievability: Option<f64>,
    result_code: &str,
    tested_at: DateTime<Utc>,
    is_consistent: Option<bool>,
    is_reliable: Option<bool>,
    url_metadata: Option<serde_json::Value>,
) {
    if let Some(r) = retrievability {
        assert!(
            (0.0..=100.0).contains(&r),
            "retrievability must be in range 0..=100, got {r}"
        );
    }
    if let Some(r) = car_retrievability {
        assert!(
            (0.0..=100.0).contains(&r),
            "car_retrievability must be in range 0..=100, got {r}"
        );
    }
    if let Some(r) = full_piece_retrievability {
        assert!(
            (0.0..=100.0).contains(&r),
            "full_piece_retrievability must be in range 0..=100, got {r}"
        );
    }

    let result_type = if client_id.is_some() {
        "ProviderClient"
    } else {
        "Provider"
    };

    sqlx::query(
        r#"INSERT INTO
                url_results (
                    provider_id,
                    client_id,
                    result_type,
                    working_url,
                    retrievability_percent,
                    car_files_percent,
                    large_files_percent,
                    result_code,
                    tested_at,
                    is_consistent,
                    is_reliable,
                    url_metadata
                )
           VALUES
                ($1, $2, $3::discovery_type, $4, $5, $6, $7, $8::result_code, $9, $10, $11, $12)"#,
    )
    .bind(provider_id)
    .bind(client_id)
    .bind(result_type)
    .bind(working_url)
    .bind(retrievability)
    .bind(car_retrievability)
    .bind(full_piece_retrievability)
    .bind(result_code)
    .bind(tested_at)
    .bind(is_consistent)
    .bind(is_reliable)
    .bind(url_metadata)
    .execute(app_pool)
    .await
    .expect("Failed to insert url_result with metadata");
}

#[allow(clippy::too_many_arguments)]
pub async fn seed_url_result_at(
    app_pool: &PgPool,
    provider_id: &str,
    client_id: Option<&str>,
    working_url: Option<&str>,
    retrievability: Option<f64>,
    result_code: &str,
    tested_at: DateTime<Utc>,
    is_consistent: Option<bool>,
    is_reliable: Option<bool>,
) {
    if let Some(r) = retrievability {
        assert!(
            (0.0..=100.0).contains(&r),
            "retrievability must be in range 0..=100, got {r}"
        );
    }

    let result_type = if client_id.is_some() {
        "ProviderClient"
    } else {
        "Provider"
    };

    sqlx::query(
        r#"INSERT INTO
                url_results (
                    provider_id,
                    client_id,
                    result_type,
                    working_url,
                    retrievability_percent,
                    result_code,
                    tested_at,
                    is_consistent,
                    is_reliable
                )
           VALUES
                ($1, $2, $3::discovery_type, $4, $5, $6::result_code, $7, $8, $9)"#,
    )
    .bind(provider_id)
    .bind(client_id)
    .bind(result_type)
    .bind(working_url)
    .bind(retrievability)
    .bind(result_code)
    .bind(tested_at)
    .bind(is_consistent)
    .bind(is_reliable)
    .execute(app_pool)
    .await
    .expect("Failed to insert url_result with timestamp");
}
