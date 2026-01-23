use axum::http::StatusCode;
use chrono::{Duration, Utc};

use crate::common::*;

#[tokio::test]
async fn test_reset_url_discovery_schedule() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;

    // Set next_url_discovery_at to future (1 day from now)
    sqlx::query(
        r#"UPDATE storage_providers
           SET next_url_discovery_at = NOW() + INTERVAL '1 day'
           WHERE provider_id = $1"#,
    )
    .bind(TEST_PROVIDER_1_DB)
    .execute(&ctx.dbs.app_pool)
    .await
    .unwrap();

    let response = ctx
        .app
        .post(&format!(
            "/providers/{TEST_PROVIDER_1_API}/reset?schedule=url_discovery"
        ))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);

    // Verify schedule was reset to approximately now
    let row: (chrono::DateTime<Utc>,) = sqlx::query_as(
        r#"SELECT next_url_discovery_at FROM storage_providers WHERE provider_id = $1"#,
    )
    .bind(TEST_PROVIDER_1_DB)
    .fetch_one(&ctx.dbs.app_pool)
    .await
    .unwrap();

    let diff = Utc::now() - row.0;
    assert!(
        diff < Duration::seconds(5),
        "next_url_discovery_at should be reset to now, diff was {diff:?}"
    );
}

#[tokio::test]
async fn test_reset_bms_test_schedule() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;

    // Set next_bms_test_at to future
    sqlx::query(
        r#"UPDATE storage_providers
           SET next_bms_test_at = NOW() + INTERVAL '7 days'
           WHERE provider_id = $1"#,
    )
    .bind(TEST_PROVIDER_1_DB)
    .execute(&ctx.dbs.app_pool)
    .await
    .unwrap();

    let response = ctx
        .app
        .post(&format!(
            "/providers/{TEST_PROVIDER_1_API}/reset?schedule=bms_test"
        ))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);

    let row: (chrono::DateTime<Utc>,) =
        sqlx::query_as(r#"SELECT next_bms_test_at FROM storage_providers WHERE provider_id = $1"#)
            .bind(TEST_PROVIDER_1_DB)
            .fetch_one(&ctx.dbs.app_pool)
            .await
            .unwrap();

    let diff = Utc::now() - row.0;
    assert!(
        diff < Duration::seconds(5),
        "next_bms_test_at should be reset to now, diff was {diff:?}"
    );
}

#[tokio::test]
async fn test_reset_all_schedules() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;

    // Set both schedules to future
    sqlx::query(
        r#"UPDATE storage_providers
           SET next_url_discovery_at = NOW() + INTERVAL '1 day',
               next_bms_test_at = NOW() + INTERVAL '7 days'
           WHERE provider_id = $1"#,
    )
    .bind(TEST_PROVIDER_1_DB)
    .execute(&ctx.dbs.app_pool)
    .await
    .unwrap();

    let response = ctx
        .app
        .post(&format!(
            "/providers/{TEST_PROVIDER_1_API}/reset?schedule=all"
        ))
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);

    let row: (chrono::DateTime<Utc>, chrono::DateTime<Utc>) = sqlx::query_as(
        r#"SELECT next_url_discovery_at, next_bms_test_at
           FROM storage_providers WHERE provider_id = $1"#,
    )
    .bind(TEST_PROVIDER_1_DB)
    .fetch_one(&ctx.dbs.app_pool)
    .await
    .unwrap();

    let diff_url = Utc::now() - row.0;
    let diff_bms = Utc::now() - row.1;
    assert!(
        diff_url < Duration::seconds(5),
        "next_url_discovery_at should be reset"
    );
    assert!(
        diff_bms < Duration::seconds(5),
        "next_bms_test_at should be reset"
    );
}

#[tokio::test]
async fn test_reset_provider_not_found() {
    let ctx = TestContext::new().await;

    let response = ctx
        .app
        .post("/providers/f099999999/reset?schedule=url_discovery")
        .await;

    assert_eq!(response.status_code(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_reset_invalid_schedule_param() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;

    let response = ctx
        .app
        .post(&format!(
            "/providers/{TEST_PROVIDER_1_API}/reset?schedule=invalid"
        ))
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_reset_missing_schedule_param() {
    let ctx = TestContext::new().await;

    seed_provider(&ctx.dbs.app_pool, TEST_PROVIDER_1_DB).await;

    let response = ctx
        .app
        .post(&format!("/providers/{TEST_PROVIDER_1_API}/reset"))
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_reset_invalid_provider_address() {
    let ctx = TestContext::new().await;

    let response = ctx
        .app
        .post("/providers/invalid/reset?schedule=url_discovery")
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
}
