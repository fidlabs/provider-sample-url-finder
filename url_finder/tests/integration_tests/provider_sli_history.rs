use assert_json_diff::assert_json_include;
use axum::http::StatusCode;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::{Value, json};
use sqlx::{Executor, PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

use crate::common::*;

struct UrlEventSeed<'a> {
    id: Uuid,
    provider_id: &'a str,
    client_id: Option<&'a str>,
    working_url: Option<&'a str>,
    retrievability_percent: Option<f64>,
    result_code: &'a str,
    error_code: Option<&'a str>,
    tested_at: DateTime<Utc>,
    is_consistent: Option<bool>,
    is_reliable: Option<bool>,
    url_metadata: Option<Value>,
    sector_utilization_percent: Option<f64>,
    car_files_percent: Option<f64>,
    large_files_percent: Option<f64>,
}

struct BmsEventSeed<'a> {
    id: Uuid,
    provider_id: &'a str,
    bms_job_id: Uuid,
    url_tested: &'a str,
    routing_key: &'a str,
    worker_count: i32,
    status: &'a str,
    ping_avg_ms: Option<f64>,
    head_avg_ms: Option<f64>,
    ttfb_ms: Option<f64>,
    download_speed_mbps: Option<f64>,
    created_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
}

async fn insert_url_event(pool: &PgPool, event: UrlEventSeed<'_>) {
    let result_type = if event.client_id.is_some() {
        "ProviderClient"
    } else {
        "Provider"
    };
    sqlx::query(
        r#"INSERT INTO url_results (
                id,
                provider_id,
                client_id,
                result_type,
                working_url,
                retrievability_percent,
                result_code,
                error_code,
                tested_at,
                is_consistent,
                is_reliable,
                url_metadata,
                sector_utilization_percent,
                car_files_percent,
                large_files_percent
            )
            VALUES (
                $1,
                $2,
                $3,
                $4::discovery_type,
                $5,
                $6,
                $7::result_code,
                $8::error_code,
                $9,
                $10,
                $11,
                $12,
                $13,
                $14,
                $15
            )"#,
    )
    .bind(event.id)
    .bind(event.provider_id)
    .bind(event.client_id)
    .bind(result_type)
    .bind(event.working_url)
    .bind(event.retrievability_percent)
    .bind(event.result_code)
    .bind(event.error_code)
    .bind(event.tested_at)
    .bind(event.is_consistent)
    .bind(event.is_reliable)
    .bind(event.url_metadata)
    .bind(event.sector_utilization_percent)
    .bind(event.car_files_percent)
    .bind(event.large_files_percent)
    .execute(pool)
    .await
    .expect("failed to insert URL history event");
}

async fn insert_bms_event(pool: &PgPool, event: BmsEventSeed<'_>) {
    sqlx::query(
        r#"INSERT INTO bms_bandwidth_results (
                id,
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
                created_at,
                completed_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)"#,
    )
    .bind(event.id)
    .bind(event.provider_id)
    .bind(event.bms_job_id)
    .bind(event.url_tested)
    .bind(event.routing_key)
    .bind(event.worker_count)
    .bind(event.status)
    .bind(event.ping_avg_ms)
    .bind(event.head_avg_ms)
    .bind(event.ttfb_ms)
    .bind(event.download_speed_mbps)
    .bind(event.created_at)
    .bind(event.completed_at)
    .execute(pool)
    .await
    .expect("failed to insert BMS history event");
}

fn only_events(body: &Value) -> Vec<&Value> {
    body["data"]
        .as_object()
        .expect("history data should be grouped by date")
        .values()
        .flat_map(|events| events.as_array().expect("date value should contain events"))
        .collect()
}

fn fixed_time(day: u32, hour: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, day, hour, 0, 0)
        .single()
        .expect("test timestamp should be valid")
}

fn url_seed<'a>(
    id: Uuid,
    provider_id: &'a str,
    client_id: Option<&'a str>,
    at: DateTime<Utc>,
) -> UrlEventSeed<'a> {
    UrlEventSeed {
        id,
        provider_id,
        client_id,
        working_url: Some(TEST_WORKING_URL),
        retrievability_percent: Some(95.0),
        result_code: "Success",
        error_code: None,
        tested_at: at,
        is_consistent: Some(true),
        is_reliable: Some(true),
        url_metadata: None,
        sector_utilization_percent: Some(80.0),
        car_files_percent: Some(90.0),
        large_files_percent: Some(85.0),
    }
}

fn bms_seed(id: Uuid, provider_id: &str, at: DateTime<Utc>) -> BmsEventSeed<'_> {
    BmsEventSeed {
        id,
        provider_id,
        bms_job_id: Uuid::new_v4(),
        url_tested: TEST_WORKING_URL,
        routing_key: "eu_west",
        worker_count: 3,
        status: "Completed",
        ping_avg_ms: Some(12.5),
        head_avg_ms: Some(25.0),
        ttfb_ms: Some(50.0),
        download_speed_mbps: Some(250.5),
        created_at: at,
        completed_at: Some(at),
    }
}

#[tokio::test]
async fn provider_history_paginates_same_timestamp_across_sources_without_gaps() {
    let ctx = TestContext::new().await;
    let at = fixed_time(8, 12);
    let url_high = Uuid::parse_str("00000000-0000-4000-8000-0000000000f0").unwrap();
    let url_low = Uuid::parse_str("00000000-0000-4000-8000-000000000010").unwrap();
    let bms_high = Uuid::parse_str("00000000-0000-4000-8000-0000000000ff").unwrap();
    let bms_low = Uuid::parse_str("00000000-0000-4000-8000-000000000001").unwrap();

    insert_url_event(
        &ctx.dbs.app_pool,
        url_seed(url_low, TEST_PROVIDER_1_DB, None, at),
    )
    .await;
    insert_url_event(
        &ctx.dbs.app_pool,
        url_seed(url_high, TEST_PROVIDER_1_DB, None, at),
    )
    .await;
    insert_bms_event(&ctx.dbs.app_pool, bms_seed(bms_low, TEST_PROVIDER_1_DB, at)).await;
    insert_bms_event(
        &ctx.dbs.app_pool,
        bms_seed(bms_high, TEST_PROVIDER_1_DB, at),
    )
    .await;

    let base = format!(
        "/providers/{TEST_PROVIDER_1_API}/history/sli?from=2026-09-08&to=2026-09-08&limit=1"
    );
    let mut cursor = None;
    let mut returned = Vec::new();
    for _ in 0..5 {
        let path = cursor
            .as_ref()
            .map_or_else(|| base.clone(), |cursor| format!("{base}&cursor={cursor}"));
        let response = ctx.app.get(&path).await;
        assert_eq!(response.status_code(), StatusCode::OK);
        let body: Value = response.json();
        let events = only_events(&body);
        assert!(events.len() <= 1);
        returned.extend(events.into_iter().map(|event| {
            (
                event["scope"].as_str().unwrap().to_string(),
                event["id"].as_str().unwrap().to_string(),
            )
        }));
        cursor = body["next_cursor"].as_str().map(ToString::to_string);
        if cursor.is_none() {
            break;
        }
    }

    assert_eq!(
        returned,
        vec![
            ("url".to_string(), url_high.to_string()),
            ("url".to_string(), url_low.to_string()),
            ("provider_bms".to_string(), bms_high.to_string()),
            ("provider_bms".to_string(), bms_low.to_string()),
        ]
    );
}

#[tokio::test]
async fn client_history_is_strict_and_retains_full_url_and_bms_values() {
    let ctx = TestContext::new().await;
    let url_at = fixed_time(8, 12);
    let bms_at = fixed_time(7, 9);
    let client_url_id = Uuid::new_v4();
    let completed_bms_id = Uuid::new_v4();
    let pending_bms_id = Uuid::new_v4();

    let mut client_url = url_seed(
        client_url_id,
        TEST_PROVIDER_1_DB,
        Some(TEST_CLIENT_ID_DB),
        url_at,
    );
    client_url.working_url = None;
    client_url.retrievability_percent = Some(12.34);
    client_url.result_code = "Error";
    client_url.error_code = Some("FailedToGetDeals");
    client_url.is_consistent = Some(false);
    client_url.is_reliable = None;
    client_url.url_metadata = Some(json!({"content_length": 4096}));
    client_url.sector_utilization_percent = Some(44.5);
    client_url.car_files_percent = None;
    client_url.large_files_percent = Some(88.8);
    insert_url_event(&ctx.dbs.app_pool, client_url).await;

    insert_url_event(
        &ctx.dbs.app_pool,
        url_seed(Uuid::new_v4(), TEST_PROVIDER_1_DB, None, url_at),
    )
    .await;
    insert_url_event(
        &ctx.dbs.app_pool,
        url_seed(
            Uuid::new_v4(),
            TEST_PROVIDER_1_DB,
            Some(TEST_CLIENT_2_ID_DB),
            url_at,
        ),
    )
    .await;
    insert_url_event(
        &ctx.dbs.app_pool,
        url_seed(
            Uuid::new_v4(),
            TEST_PROVIDER_2_DB,
            Some(TEST_CLIENT_ID_DB),
            url_at,
        ),
    )
    .await;

    insert_bms_event(
        &ctx.dbs.app_pool,
        bms_seed(completed_bms_id, TEST_PROVIDER_1_DB, bms_at),
    )
    .await;
    let mut pending = bms_seed(pending_bms_id, TEST_PROVIDER_1_DB, bms_at);
    pending.status = "Pending";
    pending.ping_avg_ms = None;
    pending.head_avg_ms = None;
    pending.ttfb_ms = None;
    pending.download_speed_mbps = None;
    pending.completed_at = None;
    insert_bms_event(&ctx.dbs.app_pool, pending).await;
    insert_bms_event(
        &ctx.dbs.app_pool,
        bms_seed(Uuid::new_v4(), TEST_PROVIDER_2_DB, bms_at),
    )
    .await;

    let response = ctx
        .app
        .get(&format!(
            "/providers/{TEST_PROVIDER_1_API}/clients/{TEST_CLIENT_ID_API}/history/sli?from=2026-09-07&to=2026-09-08"
        ))
        .await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Value = response.json();
    assert_eq!(body["provider_id"], TEST_PROVIDER_1_API);
    assert_eq!(body["client_id"], TEST_CLIENT_ID_API);
    assert_eq!(body["data"].as_object().unwrap().len(), 2);

    let events = only_events(&body);
    assert_eq!(events.len(), 3);
    let url = events
        .iter()
        .find(|event| event["id"] == client_url_id.to_string())
        .unwrap();
    assert_json_include!(
        actual: *url,
        expected: json!({
            "scope": "url",
            "provider_id": TEST_PROVIDER_1_API,
            "client_id": TEST_CLIENT_ID_API,
            "result_type": "ProviderClient",
            "working_url": null,
            "retrievability_percent": 12.34,
            "result_code": "Error",
            "error_code": "FailedToGetDeals",
            "is_consistent": false,
            "is_reliable": null,
            "url_metadata": {"content_length": 4096},
            "sector_utilization_percent": 44.5,
            "car_files_percent": null,
            "large_files_percent": 88.8
        })
    );
    assert_eq!(url["tested_at"], "2026-09-08T12:00:00Z");

    let completed = events
        .iter()
        .find(|event| event["id"] == completed_bms_id.to_string())
        .unwrap();
    assert_json_include!(
        actual: *completed,
        expected: json!({
            "scope": "provider_bms",
            "provider_id": TEST_PROVIDER_1_API,
            "url_tested": TEST_WORKING_URL,
            "routing_key": "eu_west",
            "worker_count": 3,
            "status": "Completed",
            "ping_avg_ms": 12.5,
            "head_avg_ms": 25.0,
            "ttfb_ms": 50.0,
            "download_speed_mbps": 250.5,
            "created_at": "2026-09-07T09:00:00Z",
            "completed_at": "2026-09-07T09:00:00Z"
        })
    );
    assert!(completed["bms_job_id"].as_str().is_some());

    let pending = events
        .iter()
        .find(|event| event["id"] == pending_bms_id.to_string())
        .unwrap();
    assert_json_include!(
        actual: *pending,
        expected: json!({
            "status": "Pending",
            "ping_avg_ms": null,
            "head_avg_ms": null,
            "ttfb_ms": null,
            "download_speed_mbps": null,
            "completed_at": null
        })
    );
}

#[tokio::test]
async fn provider_history_keeps_multiple_url_events_on_one_day() {
    let ctx = TestContext::new().await;
    insert_url_event(
        &ctx.dbs.app_pool,
        url_seed(Uuid::new_v4(), TEST_PROVIDER_1_DB, None, fixed_time(8, 10)),
    )
    .await;
    insert_url_event(
        &ctx.dbs.app_pool,
        url_seed(Uuid::new_v4(), TEST_PROVIDER_1_DB, None, fixed_time(8, 11)),
    )
    .await;

    let response = ctx
        .app
        .get(&format!(
            "/providers/{TEST_PROVIDER_1_API}/history/sli?from=2026-09-08&to=2026-09-08"
        ))
        .await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Value = response.json();
    assert_eq!(body["data"]["2026-09-08"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn provider_history_uses_utc_bounds_with_non_utc_database_timezone() {
    let ctx = TestContext::new().await;
    let database_url = format!(
        "postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{}/{}",
        ctx.dbs.postgres_host, ctx.dbs.app_db_name
    );
    let timezone_pool = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|connection, _| {
            Box::pin(async move {
                connection
                    .execute("SET TIME ZONE 'Pacific/Kiritimati'")
                    .await?;
                Ok(())
            })
        })
        .connect(&database_url)
        .await
        .unwrap();
    let timezone: String = sqlx::query_scalar("SHOW TIME ZONE")
        .fetch_one(&timezone_pool)
        .await
        .unwrap();
    assert_eq!(timezone, "Pacific/Kiritimati");

    insert_url_event(
        &timezone_pool,
        url_seed(Uuid::new_v4(), TEST_PROVIDER_1_DB, None, fixed_time(1, 12)),
    )
    .await;
    let timezone_dbs = TestDatabases {
        app_pool: timezone_pool,
        app_db_name: ctx.dbs.app_db_name.clone(),
        postgres_host: ctx.dbs.postgres_host.clone(),
    };
    let timezone_app = crate::common::test_app::create_test_app(&timezone_dbs, &ctx.mocks).await;
    let response = timezone_app
        .get(&format!(
            "/providers/{TEST_PROVIDER_1_API}/history/sli?from=2026-09-01&to=2026-09-01"
        ))
        .await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Value = response.json();
    assert_eq!(only_events(&body).len(), 1);
}

async fn assert_bad_request(ctx: &TestContext, path: &str, expected_code: Option<&str>) -> Value {
    let response = ctx.app.get(path).await;
    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST, "{path}");
    let body: Value = response.json();
    assert!(body["error"].is_string(), "{path}: {body}");
    if let Some(expected_code) = expected_code {
        assert_eq!(body["error_code"], expected_code, "{path}: {body}");
    }
    body
}

#[tokio::test]
async fn provider_history_rejects_invalid_dates_limits_and_cursors() {
    let ctx = TestContext::new().await;
    let base = format!("/providers/{TEST_PROVIDER_1_API}/history/sli");

    assert_bad_request(
        &ctx,
        "/providers/invalid/history/sli",
        Some("INVALID_ADDRESS"),
    )
    .await;
    assert_bad_request(
        &ctx,
        &format!("/providers/{TEST_PROVIDER_1_API}/clients/invalid/history/sli"),
        Some("INVALID_ADDRESS"),
    )
    .await;
    assert_bad_request(&ctx, &format!("{base}?from=not-a-date"), None).await;
    assert_bad_request(
        &ctx,
        &format!("{base}?from=2026-09-09&to=2026-09-08"),
        Some("INVALID_DATE_RANGE"),
    )
    .await;
    assert_bad_request(
        &ctx,
        &format!("{base}?from=2026-08-01&to=2026-09-08"),
        Some("DATE_RANGE_EXCEEDED"),
    )
    .await;
    assert_bad_request(
        &ctx,
        &format!("{base}?from=%2B262142-12-31&to=%2B262142-12-31"),
        None,
    )
    .await;
    for limit in ["0", "501", "9223372036854775808"] {
        assert_bad_request(
            &ctx,
            &format!("{base}?limit={limit}"),
            if limit == "9223372036854775808" {
                None
            } else {
                Some("INVALID_REQUEST")
            },
        )
        .await;
    }
    assert_bad_request(
        &ctx,
        &format!("{base}?cursor=not-hex"),
        Some("INVALID_REQUEST"),
    )
    .await;
    assert_bad_request(
        &ctx,
        &format!("{base}?cursor={}", "a".repeat(2049)),
        Some("INVALID_REQUEST"),
    )
    .await;
}

#[tokio::test]
async fn provider_history_cursor_is_bound_to_resource_filters_and_window() {
    let ctx = TestContext::new().await;
    let at = fixed_time(8, 12);
    insert_url_event(
        &ctx.dbs.app_pool,
        url_seed(Uuid::new_v4(), TEST_PROVIDER_1_DB, None, at),
    )
    .await;
    insert_url_event(
        &ctx.dbs.app_pool,
        url_seed(Uuid::new_v4(), TEST_PROVIDER_1_DB, None, at),
    )
    .await;

    let response = ctx
        .app
        .get(&format!(
            "/providers/{TEST_PROVIDER_1_API}/history/sli?from=2026-09-08&to=2026-09-08&limit=1"
        ))
        .await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Value = response.json();
    let cursor = body["next_cursor"].as_str().unwrap();

    assert_bad_request(
        &ctx,
        &format!(
            "/providers/{TEST_PROVIDER_2_API}/history/sli?from=2026-09-08&to=2026-09-08&cursor={cursor}"
        ),
        Some("INVALID_REQUEST"),
    )
    .await;
    assert_bad_request(
        &ctx,
        &format!(
            "/providers/{TEST_PROVIDER_1_API}/clients/{TEST_CLIENT_ID_API}/history/sli?from=2026-09-08&to=2026-09-08&cursor={cursor}"
        ),
        Some("INVALID_REQUEST"),
    )
    .await;
    assert_bad_request(
        &ctx,
        &format!(
            "/providers/{TEST_PROVIDER_1_API}/history/sli?from=2026-09-07&to=2026-09-08&cursor={cursor}"
        ),
        Some("INVALID_REQUEST"),
    )
    .await;

    let bytes = hex::decode(cursor).unwrap();
    let mut decoded: Value = serde_json::from_slice(&bytes).unwrap();
    decoded["at"] = json!("2026-09-09T00:00:00Z");
    let outside_cursor = hex::encode(serde_json::to_vec(&decoded).unwrap());
    assert_bad_request(
        &ctx,
        &format!(
            "/providers/{TEST_PROVIDER_1_API}/history/sli?from=2026-09-08&to=2026-09-08&cursor={outside_cursor}"
        ),
        Some("INVALID_REQUEST"),
    )
    .await;
}

#[tokio::test]
async fn provider_history_openapi_documents_query_contract_and_server_errors() {
    let ctx = TestContext::new().await;
    let response = ctx.app.get("/api-doc/openapi.json").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Value = response.json();
    for path in [
        "/providers/{id}/history/sli",
        "/providers/{id}/clients/{client_id}/history/sli",
    ] {
        let operation = &body["paths"][path]["get"];
        assert!(operation["responses"]["500"].is_object());
        let parameters = operation["parameters"].as_array().unwrap();
        for name in ["id", "from", "to", "limit", "cursor"] {
            let parameter = parameters
                .iter()
                .find(|parameter| parameter["name"] == name)
                .unwrap_or_else(|| panic!("missing {name} on {path}"));
            assert!(parameter["description"].is_string(), "{name} on {path}");
            if name != "cursor" {
                assert!(!parameter["example"].is_null(), "{name} on {path}");
            }
        }
        let limit = parameters
            .iter()
            .find(|parameter| parameter["name"] == "limit")
            .unwrap();
        assert_eq!(limit["schema"]["default"], 100);
        assert_eq!(limit["schema"]["minimum"], 1);
        assert_eq!(limit["schema"]["maximum"], 500);
    }
}
