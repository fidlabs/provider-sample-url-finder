use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::sync::Arc;
use url_finder::{
    background::{
        create_bms_circuit_breaker, run_deal_sli_bms_result_poller_once,
        run_deal_sli_scheduler_once,
    },
    bms_client::BmsClient,
    config::Config,
    repository::{DealSliRepository, StorageProviderRepository},
    services::deal_sli_service::DealSliService,
};
use uuid::Uuid;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_partial_json, header, method, path},
};

use crate::common::*;

#[derive(sqlx::FromRow)]
struct StoredDealSliBmsJob {
    deal_id: String,
    run_id: Uuid,
    piece_index: i32,
    piece_cid: String,
    bms_job_id: Uuid,
    url_tested: String,
    status: String,
}

#[derive(sqlx::FromRow)]
struct StoredCompletedDealSliBmsJob {
    status: String,
    ping_avg_ms: Option<f64>,
    head_avg_ms: Option<f64>,
    ttfb_ms: Option<f64>,
    download_speed_mbps: Option<f64>,
    completed_at: Option<DateTime<Utc>>,
}

fn manifest_piece(piece_cid: &str, piece_size: u64, file_size: u64) -> Value {
    json!({
        "pieceType": "dag",
        "pieceCid": piece_cid,
        "pieceSize": piece_size,
        "fileSize": file_size,
        "rootCid": format!("bafy-{piece_cid}"),
        "storagePath": format!("{piece_cid}.car")
    })
}

async fn deal_request_with_manifest(ctx: &TestContext) -> Value {
    let manifest = json!([{
        "pieces": [
            manifest_piece("baga6ea4seaq", 1024, 16_000_000_000),
            manifest_piece("baga6ea4sear", 2048, 2048)
        ]
    }]);
    let manifest_body = manifest.to_string();

    Mock::given(method("GET"))
        .and(path("/scheduler-manifest.json"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_string(manifest_body.clone()),
        )
        .mount(&ctx.mocks.piece_server)
        .await;

    json!({
        "deal_version": "v2",
        "provider_id": "1234",
        "client": "5678",
        "deal_size_bytes": "3072",
        "manifest_hash": url_finder::services::deal_manifest::compute_manifest_hash(manifest_body.as_bytes()),
        "manifest_location": format!("{}/scheduler-manifest.json", ctx.mocks.piece_server_url()),
        "requirements": {
            "retrievability_bps": 9500,
            "bandwidth_mbps": 200,
            "latency_ms": 150
        }
    })
}

#[tokio::test]
async fn test_scheduler_runs_deal_sli_before_bms_and_links_successful_piece_jobs() {
    let ctx = TestContext::new().await;
    let request = deal_request_with_manifest(&ctx).await;

    ctx.app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await
        .assert_status_ok();

    ctx.mocks
        .setup_piece_retrieval_mock("baga6ea4seaq", true)
        .await;
    ctx.mocks
        .setup_piece_retrieval_mock("baga6ea4sear", false)
        .await;
    seed_provider_with_cached_endpoints(&ctx.dbs.app_pool, "1234", &[ctx.mocks.piece_server_url()])
        .await;

    let bms_mock = MockServer::start().await;
    let bms_job_id = Uuid::new_v4();
    Mock::given(method("POST"))
        .and(path("/jobs"))
        .and(header("content-type", "application/json"))
        .and(body_partial_json(json!({
            "url": format!("{}/piece/baga6ea4seaq", ctx.mocks.piece_server_url()),
            "worker_count": 10
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": bms_job_id,
            "status": "Pending",
            "url": format!("{}/piece/baga6ea4seaq", ctx.mocks.piece_server_url()),
            "routing_key": "us_east"
        })))
        .expect(1)
        .mount(&bms_mock)
        .await;

    let mut config = Config::new_for_test(
        "http://lotus.invalid".to_string(),
        "http://cid.invalid".to_string(),
    );
    config.bms_url = bms_mock.uri();
    let config = Arc::new(config);
    let deal_sli_repo = Arc::new(DealSliRepository::new(ctx.dbs.app_pool.clone()));
    let storage_provider_repo = Arc::new(StorageProviderRepository::new(ctx.dbs.app_pool.clone()));
    let deal_sli_service = Arc::new(DealSliService::new(
        deal_sli_repo.clone(),
        storage_provider_repo,
        config.clone(),
    ));
    let bms_client = Arc::new(BmsClient::new(config.bms_url.clone()));
    let circuit_breaker = Arc::new(create_bms_circuit_breaker());

    let stats = run_deal_sli_scheduler_once(
        &config,
        &deal_sli_service,
        &deal_sli_repo,
        &bms_client,
        &circuit_breaker,
    )
    .await
    .expect("scheduler tick should succeed");

    assert_eq!(stats.targets_processed, 1);
    assert_eq!(stats.bms_jobs_created, 1);

    let rows = sqlx::query_as::<_, StoredDealSliBmsJob>(
        r#"SELECT
                deal_id,
                run_id,
                piece_index,
                piece_cid,
                bms_job_id,
                url_tested,
                status
           FROM
                deal_sli_bms_jobs
           WHERE
                deal_id = $1
           ORDER BY
                piece_index ASC
        "#,
    )
    .bind("123")
    .fetch_all(&ctx.dbs.app_pool)
    .await
    .expect("linked BMS jobs should load");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].deal_id, "123");
    assert_eq!(rows[0].piece_index, 0);
    assert_eq!(rows[0].piece_cid, "baga6ea4seaq");
    assert_eq!(rows[0].bms_job_id, bms_job_id);
    assert!(rows[0].url_tested.ends_with("/piece/baga6ea4seaq"));
    assert_eq!(rows[0].status, "Pending");

    let linked_run_piece_count: i64 = sqlx::query_scalar(
        r#"SELECT
                COUNT(*)
           FROM
                deal_sli_piece_results
           WHERE
                run_id = $1
                AND piece_cid = $2
                AND success
        "#,
    )
    .bind(rows[0].run_id)
    .bind(&rows[0].piece_cid)
    .fetch_one(&ctx.dbs.app_pool)
    .await
    .expect("linked run piece result should load");
    assert_eq!(linked_run_piece_count, 1);

    let second_stats = run_deal_sli_scheduler_once(
        &config,
        &deal_sli_service,
        &deal_sli_repo,
        &bms_client,
        &circuit_breaker,
    )
    .await
    .expect("second scheduler tick should succeed");

    assert_eq!(second_stats.targets_processed, 0);
    assert_eq!(second_stats.bms_jobs_created, 0);
}

#[tokio::test]
async fn test_deal_sli_bms_result_poller_stores_completed_job_metrics() {
    let ctx = TestContext::new().await;
    let request = deal_request_with_manifest(&ctx).await;

    ctx.app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await
        .assert_status_ok();

    ctx.mocks
        .setup_piece_retrieval_mock("baga6ea4seaq", true)
        .await;
    ctx.mocks
        .setup_piece_retrieval_mock("baga6ea4sear", false)
        .await;
    seed_provider_with_cached_endpoints(&ctx.dbs.app_pool, "1234", &[ctx.mocks.piece_server_url()])
        .await;

    let bms_mock = MockServer::start().await;
    let bms_job_id = Uuid::new_v4();
    let subjob_id = Uuid::new_v4();
    Mock::given(method("POST"))
        .and(path("/jobs"))
        .and(header("content-type", "application/json"))
        .and(body_partial_json(json!({
            "url": format!("{}/piece/baga6ea4seaq", ctx.mocks.piece_server_url()),
            "worker_count": 10
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": bms_job_id,
            "status": "Pending",
            "url": format!("{}/piece/baga6ea4seaq", ctx.mocks.piece_server_url()),
            "routing_key": "us_east"
        })))
        .expect(1)
        .mount(&bms_mock)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/jobs/{bms_job_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": bms_job_id,
            "status": "Completed",
            "url": format!("{}/piece/baga6ea4seaq", ctx.mocks.piece_server_url()),
            "routing_key": "us_east",
            "details": {
                "worker_count": 10,
                "size_mb": 15
            },
            "sub_jobs": [
                {
                    "id": subjob_id,
                    "status": "Completed",
                    "worker_data": [
                        {
                            "ping": {"avg": 0.025, "min": 0.020, "max": 0.030},
                            "head": {"avg": 50.0, "min": 45.0, "max": 55.0},
                            "download": {
                                "download_speed": 500.0,
                                "time_to_first_byte_ms": 100.0,
                                "total_bytes": 104857600,
                                "elapsed_secs": 10.0
                            }
                        }
                    ]
                }
            ]
        })))
        .expect(1)
        .mount(&bms_mock)
        .await;

    let mut config = Config::new_for_test(
        "http://lotus.invalid".to_string(),
        "http://cid.invalid".to_string(),
    );
    config.bms_url = bms_mock.uri();
    let config = Arc::new(config);
    let deal_sli_repo = Arc::new(DealSliRepository::new(ctx.dbs.app_pool.clone()));
    let storage_provider_repo = Arc::new(StorageProviderRepository::new(ctx.dbs.app_pool.clone()));
    let deal_sli_service = Arc::new(DealSliService::new(
        deal_sli_repo.clone(),
        storage_provider_repo,
        config.clone(),
    ));
    let bms_client = Arc::new(BmsClient::new(config.bms_url.clone()));
    let circuit_breaker = Arc::new(create_bms_circuit_breaker());

    run_deal_sli_scheduler_once(
        &config,
        &deal_sli_service,
        &deal_sli_repo,
        &bms_client,
        &circuit_breaker,
    )
    .await
    .expect("scheduler tick should create a pending BMS job");

    run_deal_sli_bms_result_poller_once(&deal_sli_repo, &bms_client, &circuit_breaker)
        .await
        .expect("result poller should store completed metrics");
    run_deal_sli_bms_result_poller_once(&deal_sli_repo, &bms_client, &circuit_breaker)
        .await
        .expect("result poller should be idempotent after completion");

    let row = sqlx::query_as::<_, StoredCompletedDealSliBmsJob>(
        r#"SELECT
                status,
                ping_avg_ms::float8 AS ping_avg_ms,
                head_avg_ms::float8 AS head_avg_ms,
                ttfb_ms::float8 AS ttfb_ms,
                download_speed_mbps::float8 AS download_speed_mbps,
                completed_at
           FROM
                deal_sli_bms_jobs
           WHERE
                bms_job_id = $1
        "#,
    )
    .bind(bms_job_id)
    .fetch_one(&ctx.dbs.app_pool)
    .await
    .expect("completed BMS job should load");

    assert_eq!(row.status, "Completed");
    assert_eq!(row.ping_avg_ms, Some(25.0));
    assert_eq!(row.head_avg_ms, Some(50.0));
    assert_eq!(row.ttfb_ms, Some(100.0));
    assert_eq!(row.download_speed_mbps, Some(500.0));
    assert!(row.completed_at.is_some());

    let latest_response = ctx.app.get("/deals/123/latest").await;
    latest_response.assert_status_ok();
    let latest_body: Value = latest_response.json();
    assert_json_diff::assert_json_include!(
        actual: latest_body,
        expected: json!({
            "porep_slis": {
                "retrievability_bps": 5000,
                "bandwidth_mbps": 500,
                "latency_ms": 100,
                "indexing_pct": null
            },
            "bms_results": [
                {
                    "piece_index": 0,
                    "piece_cid": "baga6ea4seaq",
                    "bms_job_id": bms_job_id.to_string(),
                    "url_tested": format!("{}/piece/baga6ea4seaq", ctx.mocks.piece_server_url()),
                    "routing_key": "us_east",
                    "worker_count": 10,
                    "status": "Completed",
                    "ping_avg_ms": 25.0,
                    "head_avg_ms": 50.0,
                    "ttfb_ms": 100.0,
                    "download_speed_mbps": 500.0,
                    "error_message": null
                }
            ]
        })
    );
    assert!(
        latest_body["bms_results"][0]
            .get("completed_at")
            .and_then(Value::as_str)
            .is_some(),
        "completed BMS result should include completed_at"
    );
}

#[tokio::test]
async fn test_deal_sli_bms_result_poller_stores_failed_or_cancelled_job_without_fake_metrics() {
    for terminal_status in ["Failed", "Cancelled"] {
        let ctx = TestContext::new().await;
        let request = deal_request_with_manifest(&ctx).await;

        ctx.app
            .put("/deals/123")
            .authorization_bearer("test-token")
            .json(&request)
            .await
            .assert_status_ok();

        ctx.mocks
            .setup_piece_retrieval_mock("baga6ea4seaq", true)
            .await;
        ctx.mocks
            .setup_piece_retrieval_mock("baga6ea4sear", false)
            .await;
        seed_provider_with_cached_endpoints(
            &ctx.dbs.app_pool,
            "1234",
            &[ctx.mocks.piece_server_url()],
        )
        .await;

        let bms_mock = MockServer::start().await;
        let bms_job_id = Uuid::new_v4();
        Mock::given(method("POST"))
            .and(path("/jobs"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": bms_job_id,
                "status": "Pending",
                "url": format!("{}/piece/baga6ea4seaq", ctx.mocks.piece_server_url()),
                "routing_key": "us_east"
            })))
            .expect(1)
            .mount(&bms_mock)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/jobs/{bms_job_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": bms_job_id,
                "status": terminal_status,
                "url": format!("{}/piece/baga6ea4seaq", ctx.mocks.piece_server_url()),
                "routing_key": "us_east",
                "details": null,
                "sub_jobs": []
            })))
            .expect(1)
            .mount(&bms_mock)
            .await;

        let mut config = Config::new_for_test(
            "http://lotus.invalid".to_string(),
            "http://cid.invalid".to_string(),
        );
        config.bms_url = bms_mock.uri();
        let config = Arc::new(config);
        let deal_sli_repo = Arc::new(DealSliRepository::new(ctx.dbs.app_pool.clone()));
        let storage_provider_repo =
            Arc::new(StorageProviderRepository::new(ctx.dbs.app_pool.clone()));
        let deal_sli_service = Arc::new(DealSliService::new(
            deal_sli_repo.clone(),
            storage_provider_repo,
            config.clone(),
        ));
        let bms_client = Arc::new(BmsClient::new(config.bms_url.clone()));
        let circuit_breaker = Arc::new(create_bms_circuit_breaker());

        run_deal_sli_scheduler_once(
            &config,
            &deal_sli_service,
            &deal_sli_repo,
            &bms_client,
            &circuit_breaker,
        )
        .await
        .expect("scheduler tick should create a pending BMS job");
        run_deal_sli_bms_result_poller_once(&deal_sli_repo, &bms_client, &circuit_breaker)
            .await
            .expect("result poller should store terminal status");

        let latest_response = ctx.app.get("/deals/123/latest").await;
        latest_response.assert_status_ok();
        let latest_body: Value = latest_response.json();
        assert_json_diff::assert_json_include!(
            actual: latest_body,
            expected: json!({
                "porep_slis": {
                    "retrievability_bps": 5000,
                    "bandwidth_mbps": null,
                    "latency_ms": null,
                    "indexing_pct": null
                },
                "bms_results": [
                    {
                        "bms_job_id": bms_job_id.to_string(),
                        "status": terminal_status,
                        "ping_avg_ms": null,
                        "head_avg_ms": null,
                        "ttfb_ms": null,
                        "download_speed_mbps": null
                    }
                ]
            })
        );
    }
}

#[tokio::test]
async fn test_deal_sli_bms_result_poller_marks_stale_pending_job_timeout() {
    let ctx = TestContext::new().await;
    let request = deal_request_with_manifest(&ctx).await;

    ctx.app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await
        .assert_status_ok();

    ctx.mocks
        .setup_piece_retrieval_mock("baga6ea4seaq", true)
        .await;
    ctx.mocks
        .setup_piece_retrieval_mock("baga6ea4sear", false)
        .await;
    seed_provider_with_cached_endpoints(&ctx.dbs.app_pool, "1234", &[ctx.mocks.piece_server_url()])
        .await;

    let bms_mock = MockServer::start().await;
    let bms_job_id = Uuid::new_v4();
    Mock::given(method("POST"))
        .and(path("/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": bms_job_id,
            "status": "Pending",
            "url": format!("{}/piece/baga6ea4seaq", ctx.mocks.piece_server_url()),
            "routing_key": "us_east"
        })))
        .expect(1)
        .mount(&bms_mock)
        .await;

    let mut config = Config::new_for_test(
        "http://lotus.invalid".to_string(),
        "http://cid.invalid".to_string(),
    );
    config.bms_url = bms_mock.uri();
    let config = Arc::new(config);
    let deal_sli_repo = Arc::new(DealSliRepository::new(ctx.dbs.app_pool.clone()));
    let storage_provider_repo = Arc::new(StorageProviderRepository::new(ctx.dbs.app_pool.clone()));
    let deal_sli_service = Arc::new(DealSliService::new(
        deal_sli_repo.clone(),
        storage_provider_repo,
        config.clone(),
    ));
    let bms_client = Arc::new(BmsClient::new(config.bms_url.clone()));
    let circuit_breaker = Arc::new(create_bms_circuit_breaker());

    run_deal_sli_scheduler_once(
        &config,
        &deal_sli_service,
        &deal_sli_repo,
        &bms_client,
        &circuit_breaker,
    )
    .await
    .expect("scheduler tick should create a pending BMS job");

    sqlx::query(
        r#"UPDATE
                deal_sli_bms_jobs
           SET
                created_at = NOW() - INTERVAL '49 hours'
           WHERE
                bms_job_id = $1
        "#,
    )
    .bind(bms_job_id)
    .execute(&ctx.dbs.app_pool)
    .await
    .expect("pending job should be made stale");

    run_deal_sli_bms_result_poller_once(&deal_sli_repo, &bms_client, &circuit_breaker)
        .await
        .expect("result poller should mark stale jobs timed out");

    let row = sqlx::query_as::<_, StoredCompletedDealSliBmsJob>(
        r#"SELECT
                status,
                ping_avg_ms::float8 AS ping_avg_ms,
                head_avg_ms::float8 AS head_avg_ms,
                ttfb_ms::float8 AS ttfb_ms,
                download_speed_mbps::float8 AS download_speed_mbps,
                completed_at
           FROM
                deal_sli_bms_jobs
           WHERE
                bms_job_id = $1
        "#,
    )
    .bind(bms_job_id)
    .fetch_one(&ctx.dbs.app_pool)
    .await
    .expect("timed out BMS job should load");

    assert_eq!(row.status, "Timeout");
    assert!(row.ping_avg_ms.is_none());
    assert!(row.head_avg_ms.is_none());
    assert!(row.ttfb_ms.is_none());
    assert!(row.download_speed_mbps.is_none());
    assert!(row.completed_at.is_some());
}
