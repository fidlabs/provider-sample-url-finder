use assert_json_diff::assert_json_include;
use axum::http::StatusCode;
use serde_json::{Value, json};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, ResponseTemplate};

use crate::common::*;

#[derive(sqlx::FromRow)]
struct StoredDealSliRun {
    state: String,
    measurement_state: String,
    result_code: Option<url_finder::ResultCode>,
    piece_count: i32,
    success_count: i32,
    failed_count: i32,
}

#[derive(sqlx::FromRow)]
struct StoredDealSliPieceResult {
    piece_index: i32,
    piece_cid: String,
    url_tested: String,
    success: bool,
    result_code: Option<url_finder::ResultCode>,
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

fn default_manifest_pieces() -> Vec<Value> {
    vec![
        manifest_piece("baga6ea4seaq", 1024, 16_000_000_000),
        manifest_piece("baga6ea4sear", 2048, 2048),
    ]
}

async fn deal_request(ctx: &TestContext) -> Value {
    deal_request_with_manifest(ctx, "/manifest.json", "3072", default_manifest_pieces()).await
}

async fn deal_request_with_manifest(
    ctx: &TestContext,
    manifest_path: &str,
    deal_size_bytes: &str,
    pieces: Vec<Value>,
) -> Value {
    let manifest = json!([{ "pieces": pieces }]);
    let manifest_body = manifest.to_string();
    Mock::given(method("GET"))
        .and(path(manifest_path))
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
        "deal_size_bytes": deal_size_bytes,
        "manifest_hash": url_finder::services::deal_manifest::compute_manifest_hash(manifest_body.as_bytes()),
        "manifest_location": format!("{}{}", ctx.mocks.piece_server_url(), manifest_path),
        "requirements": {
            "retrievability_bps": 9500,
            "bandwidth_mbps": 200,
            "latency_ms": 150
        }
    })
}

async fn many_piece_deal_request(ctx: &TestContext, piece_count: usize) -> Value {
    let pieces = (0..piece_count)
        .map(|index| {
            manifest_piece(
                &format!("baga6ea4seaq{index:04}"),
                34_359_738_368,
                34_359_738_368,
            )
        })
        .collect::<Vec<_>>();
    let deal_size = (34_359_738_368_u128 * piece_count as u128).to_string();
    deal_request_with_manifest(ctx, "/large-manifest.json", &deal_size, pieces).await
}

async fn setup_piece_retrieval_with_total_size(
    ctx: &TestContext,
    piece_cid: &str,
    total_size: u64,
) {
    Mock::given(method("GET"))
        .and(path(format!("/piece/{piece_cid}")))
        .and(header("Range", "bytes=0-4095"))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header("etag", "\"mock-etag-12345\"")
                .insert_header("Content-Range", format!("bytes 0-4095/{total_size}"))
                .set_body_raw(vec![0u8; 4096], "application/piece"),
        )
        .mount(&ctx.mocks.piece_server)
        .await;
}

fn assert_deal_latest_has_no_legacy_url_metrics(body: &Value) {
    for field in [
        "large_files_percent",
        "car_files_percent",
        "sector_utilization_percent",
        "is_consistent",
        "performance",
    ] {
        assert!(
            body.get(field).is_none(),
            "{field} should not be part of Deal SLI latest response"
        );
    }
}

#[tokio::test]
async fn test_put_deal_persists_and_get_deal_returns_stored_target() {
    let ctx = TestContext::new().await;
    let request = deal_request(&ctx).await;

    let put_response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await;

    assert_eq!(put_response.status_code(), StatusCode::OK);

    let get_response = ctx.app.get("/deals/123").await;

    assert_eq!(get_response.status_code(), StatusCode::OK);
    let body: Value = get_response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "deal_id": "123",
            "deal_version": "v2",
            "provider_id": "1234",
            "client": "5678",
            "deal_size_bytes": "3072",
            "manifest_hash": request["manifest_hash"].as_str().unwrap(),
            "manifest_location": request["manifest_location"].as_str().unwrap(),
            "requirements": {
                "retrievability_bps": 9500,
                "bandwidth_mbps": 200,
                "latency_ms": 150
            },
            "pieces": [
                {
                    "piece_cid": "baga6ea4seaq",
                    "piece_size_bytes": "1024",
                    "file_size_bytes": "16000000000",
                    "root_cid": "bafy-baga6ea4seaq",
                    "storage_path": "baga6ea4seaq.car",
                    "piece_type": "dag"
                },
                {
                    "piece_cid": "baga6ea4sear",
                    "piece_size_bytes": "2048",
                    "file_size_bytes": "2048",
                    "root_cid": "bafy-baga6ea4sear",
                    "storage_path": "baga6ea4sear.car",
                    "piece_type": "dag"
                }
            ]
        })
    );
    assert!(body.get("created_at").and_then(Value::as_str).is_some());
    assert!(body.get("updated_at").and_then(Value::as_str).is_some());
}

#[tokio::test]
async fn test_put_deal_accepts_provider_address_and_stores_provider_id() {
    let ctx = TestContext::new().await;
    let mut request = deal_request(&ctx).await;
    request["provider_id"] = json!("f01234");

    let put_response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await;

    assert_eq!(put_response.status_code(), StatusCode::OK);
    let put_body: Value = put_response.json();
    assert_json_include!(
        actual: put_body,
        expected: json!({
            "provider_id": "1234"
        })
    );

    seed_provider(&ctx.dbs.app_pool, "1234").await;
    let run_response = ctx
        .app
        .post("/deals/123/runs")
        .authorization_bearer("test-token")
        .await;

    assert_eq!(run_response.status_code(), StatusCode::OK);
    let run_body: Value = run_response.json();
    assert_json_include!(
        actual: run_body,
        expected: json!({
            "measurement_state": "failed",
            "result_code": "MissingHttpAddrFromCidContact"
        })
    );
}

#[tokio::test]
async fn test_put_deal_with_invalid_decimal_deal_id_returns_bad_request() {
    let ctx = TestContext::new().await;
    let request = deal_request(&ctx).await;

    let response = ctx
        .app
        .put("/deals/not-a-decimal")
        .authorization_bearer("test-token")
        .json(&request)
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "INVALID_REQUEST"
        })
    );
}

#[tokio::test]
async fn test_put_deal_with_invalid_provider_id_returns_bad_request() {
    let ctx = TestContext::new().await;
    let mut request = deal_request(&ctx).await;
    request["provider_id"] = json!("f01abc");

    let response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "INVALID_REQUEST"
        })
    );
}

#[tokio::test]
async fn test_put_deal_with_unicode_numeric_provider_id_returns_bad_request() {
    let ctx = TestContext::new().await;
    let mut request = deal_request(&ctx).await;
    request["provider_id"] = json!("١٢٣٤");

    let response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "INVALID_REQUEST"
        })
    );
}

#[tokio::test]
async fn test_put_deal_rejects_unknown_pieces_field() {
    let ctx = TestContext::new().await;
    let mut request = deal_request(&ctx).await;
    request["pieces"] = json!([{ "piece_cid": "caller-owned-piece" }]);

    let response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "INVALID_REQUEST"
        })
    );
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|message| message.contains("unknown field") && message.contains("pieces")),
        "expected unknown pieces field error, got {body}"
    );
}

#[tokio::test]
async fn test_put_deal_rejects_unknown_requirement_field() {
    let ctx = TestContext::new().await;
    let mut request = deal_request(&ctx).await;
    request["requirements"]["minimum_magic"] = json!(42);

    let response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "INVALID_REQUEST"
        })
    );
    assert!(
        body["error"].as_str().is_some_and(
            |message| message.contains("unknown field") && message.contains("minimum_magic")
        ),
        "expected unknown requirement field error, got {body}"
    );
}

#[tokio::test]
async fn test_put_deal_with_fractional_piece_size_returns_bad_request() {
    let ctx = TestContext::new().await;
    let mut request = deal_request(&ctx).await;
    request["deal_size_bytes"] = json!("1024.5");

    let response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "INVALID_REQUEST"
        })
    );
}

#[tokio::test]
async fn test_put_deal_with_oversized_deal_size_returns_bad_request() {
    let ctx = TestContext::new().await;
    let mut request = deal_request(&ctx).await;
    request["deal_size_bytes"] = json!("1".repeat(79));

    let response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "INVALID_REQUEST"
        })
    );
}

#[tokio::test]
async fn test_put_deal_with_overflowing_requirement_returns_bad_request() {
    let ctx = TestContext::new().await;
    let mut request = deal_request(&ctx).await;
    request["requirements"]["bandwidth_mbps"] = json!(u32::MAX);

    let response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "INVALID_REQUEST"
        })
    );
}

#[tokio::test]
async fn test_put_deal_with_invalid_retrievability_requirement_returns_bad_request() {
    let ctx = TestContext::new().await;
    let mut request = deal_request(&ctx).await;
    request["requirements"]["retrievability_bps"] = json!(10_001);

    let response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "INVALID_REQUEST"
        })
    );
}

#[tokio::test]
async fn test_put_deal_with_manifest_hash_mismatch_returns_bad_request() {
    let ctx = TestContext::new().await;
    let mut request = deal_request(&ctx).await;
    request["manifest_hash"] = json!("00");

    let response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "INVALID_REQUEST"
        })
    );
}

#[tokio::test]
async fn test_put_deal_with_missing_manifest_file_size_returns_bad_request() {
    let ctx = TestContext::new().await;
    let request = deal_request_with_manifest(
        &ctx,
        "/missing-file-size-manifest.json",
        "1024",
        vec![json!({
            "pieceType": "dag",
            "pieceCid": "baga6ea4seaq",
            "pieceSize": 1024,
            "rootCid": "bafy-baga6ea4seaq",
            "storagePath": "baga6ea4seaq.car"
        })],
    )
    .await;

    let response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "INVALID_REQUEST"
        })
    );
}

#[tokio::test]
async fn test_get_deal_returns_not_found_when_target_missing() {
    let ctx = TestContext::new().await;

    let response = ctx.app.get("/deals/123").await;

    assert_eq!(response.status_code(), StatusCode::NOT_FOUND);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "NOT_FOUND"
        })
    );
}

#[tokio::test]
async fn test_get_latest_returns_missing_state_with_target_piece_count_when_no_runs_exist() {
    let ctx = TestContext::new().await;
    let request = deal_request(&ctx).await;

    ctx.app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await
        .assert_status_ok();

    let response = ctx.app.get("/deals/123/latest").await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "deal_id": "123",
            "measurement_state": "missing",
            "piece_count": 2,
            "success_count": 0,
            "failed_count": 0,
            "result_code": null
        })
    );
    assert_deal_latest_has_no_legacy_url_metrics(&body);
}

#[tokio::test]
async fn test_get_latest_returns_not_found_when_target_missing() {
    let ctx = TestContext::new().await;

    let response = ctx.app.get("/deals/123/latest").await;

    assert_eq!(response.status_code(), StatusCode::NOT_FOUND);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "NOT_FOUND"
        })
    );
}

#[tokio::test]
async fn test_get_latest_uses_stable_tie_breaker_for_matching_timestamps() {
    let ctx = TestContext::new().await;
    let request = deal_request(&ctx).await;

    ctx.app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await
        .assert_status_ok();

    sqlx::query(
        r#"INSERT INTO
                deal_sli_runs (
                    id,
                    deal_id,
                    state,
                    measurement_state,
                    started_at,
                    completed_at,
                    tested_at,
                    provider_id,
                    result_code,
                    piece_count,
                    success_count,
                    failed_count
                )
           VALUES
                ('00000000-0000-0000-0000-000000000001', '123', 'completed', 'failed', '2026-06-03 10:00:00+00', '2026-06-03 10:00:00+00', '2026-06-03 10:00:00+00', '1234', 'FailedToGetWorkingUrl', 2, 0, 2),
                ('00000000-0000-0000-0000-000000000002', '123', 'completed', 'fresh', '2026-06-03 10:00:00+00', '2026-06-03 10:00:00+00', '2026-06-03 10:00:00+00', '1234', 'Success', 2, 2, 0)
        "#,
    )
    .execute(&ctx.dbs.app_pool)
    .await
    .expect("tied runs should insert");

    let response = ctx.app.get("/deals/123/latest").await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "measurement_state": "fresh",
            "result_code": "Success",
            "success_count": 2,
            "failed_count": 0
        })
    );
}

#[tokio::test]
async fn test_post_run_without_cached_endpoints_persists_failed_run_and_latest_uses_it() {
    let ctx = TestContext::new().await;
    let request = deal_request(&ctx).await;

    ctx.app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await
        .assert_status_ok();

    seed_provider(&ctx.dbs.app_pool, "1234").await;

    let run_response = ctx
        .app
        .post("/deals/123/runs")
        .authorization_bearer("test-token")
        .await;

    assert_eq!(run_response.status_code(), StatusCode::OK);
    let run_body: Value = run_response.json();
    assert_json_include!(
        actual: run_body,
        expected: json!({
            "deal_id": "123",
            "measurement_state": "failed",
            "piece_count": 2,
            "success_count": 0,
            "failed_count": 0,
            "manifest_size_bytes": "3072",
            "deal_size_bytes": "3072",
            "content_matches_deal": true,
            "sampled_piece_count": 0,
            "result_code": "MissingHttpAddrFromCidContact"
        })
    );
    assert!(run_body.get("tested_at").and_then(Value::as_str).is_some());

    let row = sqlx::query_as::<_, StoredDealSliRun>(
        r#"SELECT
                state,
                measurement_state,
                result_code,
                piece_count,
                success_count,
                failed_count
           FROM
                deal_sli_runs
           WHERE
                deal_id = $1
        "#,
    )
    .bind("123")
    .fetch_one(&ctx.dbs.app_pool)
    .await
    .expect("stored run should exist");

    assert_eq!(row.state, "completed");
    assert_eq!(row.measurement_state, "failed");
    assert_eq!(
        row.result_code,
        Some(url_finder::ResultCode::MissingHttpAddrFromCidContact)
    );
    assert_eq!(row.piece_count, 2);
    assert_eq!(row.success_count, 0);
    assert_eq!(row.failed_count, 0);

    let latest_response = ctx.app.get("/deals/123/latest").await;

    assert_eq!(latest_response.status_code(), StatusCode::OK);
    let latest_body: Value = latest_response.json();
    assert_json_include!(
        actual: latest_body,
        expected: json!({
            "deal_id": "123",
            "measurement_state": "failed",
            "piece_count": 2,
            "success_count": 0,
            "failed_count": 0,
            "result_code": "MissingHttpAddrFromCidContact"
        })
    );
    assert_deal_latest_has_no_legacy_url_metrics(&latest_body);
}

#[tokio::test]
async fn test_post_run_with_cached_endpoint_tests_target_pieces_and_stores_piece_results() {
    let ctx = TestContext::new().await;
    let request = deal_request(&ctx).await;

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

    let run_response = ctx
        .app
        .post("/deals/123/runs")
        .authorization_bearer("test-token")
        .await;

    assert_eq!(run_response.status_code(), StatusCode::OK);
    let run_body: Value = run_response.json();
    assert_json_include!(
        actual: run_body,
        expected: json!({
            "deal_id": "123",
            "measurement_state": "fresh",
            "retrievability_percent": 50.0,
            "piece_count": 2,
            "sampled_piece_count": 2,
            "size_matched_percent": 50.0,
            "manifest_size_bytes": "3072",
            "deal_size_bytes": "3072",
            "content_matches_deal": true,
            "success_count": 1,
            "failed_count": 1,
            "result_code": "Success"
        })
    );
    assert_deal_latest_has_no_legacy_url_metrics(&run_body);
    assert!(
        run_body
            .get("working_url")
            .and_then(Value::as_str)
            .is_some_and(|url| url.ends_with("/piece/baga6ea4seaq"))
    );

    let piece_rows = sqlx::query_as::<_, StoredDealSliPieceResult>(
        r#"SELECT
                piece_index,
                piece_cid,
                url_tested,
                success,
                result_code
           FROM
                deal_sli_piece_results
           WHERE
                deal_id = $1
           ORDER BY
                piece_index ASC
        "#,
    )
    .bind("123")
    .fetch_all(&ctx.dbs.app_pool)
    .await
    .expect("piece result rows should load");

    assert_eq!(piece_rows.len(), 2);
    assert_eq!(piece_rows[0].piece_index, 0);
    assert_eq!(piece_rows[0].piece_cid, "baga6ea4seaq");
    assert!(piece_rows[0].success);
    assert_eq!(
        piece_rows[0].result_code,
        Some(url_finder::ResultCode::Success)
    );
    assert!(piece_rows[0].url_tested.ends_with("/piece/baga6ea4seaq"));
    assert_eq!(piece_rows[1].piece_index, 1);
    assert_eq!(piece_rows[1].piece_cid, "baga6ea4sear");
    assert!(!piece_rows[1].success);
    assert_eq!(
        piece_rows[1].result_code,
        Some(url_finder::ResultCode::FailedToGetWorkingUrl)
    );
}

#[tokio::test]
async fn test_post_run_measures_when_manifest_size_differs_from_deal_size() {
    let ctx = TestContext::new().await;
    let request = deal_request_with_manifest(
        &ctx,
        "/mismatch-manifest.json",
        "9999",
        default_manifest_pieces(),
    )
    .await;

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

    let run_response = ctx
        .app
        .post("/deals/123/runs")
        .authorization_bearer("test-token")
        .await;

    assert_eq!(run_response.status_code(), StatusCode::OK);
    let run_body: Value = run_response.json();
    assert_json_include!(
        actual: run_body,
        expected: json!({
            "measurement_state": "fresh",
            "manifest_size_bytes": "3072",
            "deal_size_bytes": "9999",
            "content_matches_deal": false,
            "sampled_piece_count": 2,
            "retrievability_percent": 50.0,
            "size_matched_percent": 50.0
        })
    );
    assert_deal_latest_has_no_legacy_url_metrics(&run_body);
}

#[tokio::test]
async fn test_post_run_wrong_size_responses_are_retrievable_but_not_successful() {
    let ctx = TestContext::new().await;
    let request = deal_request(&ctx).await;

    ctx.app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await
        .assert_status_ok();

    setup_piece_retrieval_with_total_size(&ctx, "baga6ea4seaq", 777).await;
    setup_piece_retrieval_with_total_size(&ctx, "baga6ea4sear", 777).await;
    seed_provider_with_cached_endpoints(&ctx.dbs.app_pool, "1234", &[ctx.mocks.piece_server_url()])
        .await;

    let run_response = ctx
        .app
        .post("/deals/123/runs")
        .authorization_bearer("test-token")
        .await;

    assert_eq!(run_response.status_code(), StatusCode::OK);
    let run_body: Value = run_response.json();
    assert_json_include!(
        actual: run_body,
        expected: json!({
            "measurement_state": "fresh",
            "retrievability_percent": 100.0,
            "size_matched_percent": 0.0,
            "success_count": 0,
            "failed_count": 2,
            "result_code": "FailedToGetWorkingUrl"
        })
    );
    assert_deal_latest_has_no_legacy_url_metrics(&run_body);
}

#[tokio::test]
async fn test_post_run_uses_active_manifest_snapshot_at_run_start() {
    let ctx = TestContext::new().await;
    let request = deal_request(&ctx).await;

    ctx.app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await
        .assert_status_ok();

    let original_snapshot_id: uuid::Uuid = sqlx::query_scalar(
        r#"SELECT
                active_manifest_snapshot_id
           FROM
                deal_sli_targets
           WHERE
                deal_id = $1
        "#,
    )
    .bind("123")
    .fetch_one(&ctx.dbs.app_pool)
    .await
    .expect("target should have active manifest snapshot");

    let new_snapshot_id: uuid::Uuid = sqlx::query_scalar(
        r#"INSERT INTO
                deal_sli_manifest_snapshots (
                    deal_id,
                    manifest_hash,
                    manifest_location,
                    raw_content,
                    parsed_content,
                    content_byte_length,
                    computed_hash
                )
           SELECT
                deal_id,
                manifest_hash,
                manifest_location,
                raw_content,
                parsed_content,
                content_byte_length,
                computed_hash
           FROM
                deal_sli_manifest_snapshots
           WHERE
                id = $1
           RETURNING
                id
        "#,
    )
    .bind(original_snapshot_id)
    .fetch_one(&ctx.dbs.app_pool)
    .await
    .expect("duplicate snapshot should insert");

    sqlx::query(
        r#"UPDATE
                deal_sli_targets
           SET
                active_manifest_snapshot_id = $2
           WHERE
                deal_id = $1
        "#,
    )
    .bind("123")
    .bind(new_snapshot_id)
    .execute(&ctx.dbs.app_pool)
    .await
    .expect("target snapshot should update");

    seed_provider_with_cached_endpoints(&ctx.dbs.app_pool, "1234", &[ctx.mocks.piece_server_url()])
        .await;

    let response = ctx
        .app
        .post("/deals/123/runs")
        .authorization_bearer("test-token")
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "manifest_snapshot_id": new_snapshot_id.to_string()
        })
    );
}

#[tokio::test]
async fn test_post_run_allows_large_single_endpoint_deal() {
    let ctx = TestContext::new().await;
    let request = many_piece_deal_request(&ctx, 1_499).await;

    ctx.app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await
        .assert_status_ok();

    seed_provider_with_cached_endpoints(&ctx.dbs.app_pool, "1234", &[ctx.mocks.piece_server_url()])
        .await;

    let response = ctx
        .app
        .post("/deals/123/runs")
        .authorization_bearer("test-token")
        .await;

    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "deal_id": "123",
            "measurement_state": "fresh",
            "piece_count": 1499,
            "sampled_piece_count": 100,
            "success_count": 0,
            "failed_count": 100,
            "result_code": "FailedToGetWorkingUrl"
        })
    );

    let piece_result_count: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*)
           FROM
                deal_sli_piece_results
           WHERE
                deal_id = $1
        "#,
    )
    .bind("123")
    .fetch_one(&ctx.dbs.app_pool)
    .await
    .expect("piece result count should load");

    assert_eq!(piece_result_count, 100);
}

#[tokio::test]
async fn test_post_run_rejects_oversized_synchronous_fanout() {
    let ctx = TestContext::new().await;
    let request = deal_request(&ctx).await;

    ctx.app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await
        .assert_status_ok();

    let endpoints = (0..1025)
        .map(|index| format!("http://127.0.0.1:{}/", 10_000 + index))
        .collect::<Vec<_>>();
    seed_provider_with_cached_endpoints(&ctx.dbs.app_pool, "1234", &endpoints).await;

    let response = ctx
        .app
        .post("/deals/123/runs")
        .authorization_bearer("test-token")
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "INVALID_REQUEST"
        })
    );
}

#[tokio::test]
async fn test_put_deal_rejects_piece_identity_change_after_no_endpoint_run_exists() {
    let ctx = TestContext::new().await;
    let request = deal_request(&ctx).await;

    ctx.app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&request)
        .await
        .assert_status_ok();

    seed_provider(&ctx.dbs.app_pool, "1234").await;
    ctx.app
        .post("/deals/123/runs")
        .authorization_bearer("test-token")
        .await
        .assert_status_ok();

    let changed_request = deal_request_with_manifest(
        &ctx,
        "/changed-manifest.json",
        "3072",
        vec![
            manifest_piece("baga6ea4seaz", 1024, 16_000_000_000),
            manifest_piece("baga6ea4sear", 2048, 2048),
        ],
    )
    .await;

    let response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&changed_request)
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "INVALID_REQUEST"
        })
    );
}

#[tokio::test]
async fn test_put_deal_rejects_piece_identity_change_after_piece_results_exist() {
    let ctx = TestContext::new().await;
    let request = deal_request(&ctx).await;

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
    ctx.app
        .post("/deals/123/runs")
        .authorization_bearer("test-token")
        .await
        .assert_status_ok();

    let changed_request = deal_request_with_manifest(
        &ctx,
        "/changed-manifest-results.json",
        "3072",
        vec![
            manifest_piece("baga6ea4seaz", 1024, 16_000_000_000),
            manifest_piece("baga6ea4sear", 2048, 2048),
        ],
    )
    .await;

    let response = ctx
        .app
        .put("/deals/123")
        .authorization_bearer("test-token")
        .json(&changed_request)
        .await;

    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: Value = response.json();
    assert_json_include!(
        actual: body,
        expected: json!({
            "error_code": "INVALID_REQUEST"
        })
    );
}
