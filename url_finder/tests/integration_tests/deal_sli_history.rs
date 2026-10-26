use std::collections::HashSet;

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

use crate::common::*;

const DEAL_ID: &str = "42";
const OTHER_DEAL_ID: &str = "43";
const SNAPSHOT_ID: &str = "10000000-0000-4000-8000-000000000001";
const RUN_1: &str = "20000000-0000-4000-8000-000000000001";
const RUN_2: &str = "20000000-0000-4000-8000-000000000002";
const RUN_3: &str = "20000000-0000-4000-8000-000000000003";
const RUN_4: &str = "20000000-0000-4000-8000-000000000004";

fn uuid(value: &str) -> Uuid {
    Uuid::parse_str(value).expect("test UUID must be valid")
}

fn timestamp(value: &str) -> DateTime<Utc> {
    value.parse().expect("test timestamp must be valid")
}

async fn seed_target(pool: &PgPool, deal_id: &str) -> Uuid {
    sqlx::query(
        r#"INSERT INTO deal_sli_targets (
                deal_id, deal_version, provider_id, client_id, deal_size_bytes,
                manifest_hash, manifest_location
            ) VALUES ($1, 'v2', '1234', '5678', 123456789012345678901234567890,
                'stored-hash', 'https://manifest.example/history.json')"#,
    )
    .bind(deal_id)
    .execute(pool)
    .await
    .expect("target seed should succeed");

    let snapshot_id = if deal_id == DEAL_ID {
        uuid(SNAPSHOT_ID)
    } else {
        Uuid::new_v4()
    };
    sqlx::query(
        r#"INSERT INTO deal_sli_manifest_snapshots (
                id, deal_id, manifest_hash, manifest_location, raw_content,
                parsed_content, content_byte_length, computed_hash
            ) VALUES ($1, $2, 'stored-hash', 'https://manifest.example/history.json',
                '{}', '{}'::jsonb, 2, 'stored-hash')"#,
    )
    .bind(snapshot_id)
    .bind(deal_id)
    .execute(pool)
    .await
    .expect("snapshot seed should succeed");
    sqlx::query("UPDATE deal_sli_targets SET active_manifest_snapshot_id = $2 WHERE deal_id = $1")
        .bind(deal_id)
        .bind(snapshot_id)
        .execute(pool)
        .await
        .expect("target snapshot update should succeed");

    for piece_index in 0..2_i32 {
        sqlx::query(
            r#"INSERT INTO deal_sli_pieces (
                    deal_id, piece_index, piece_cid, piece_size_bytes,
                    manifest_snapshot_id, file_size_bytes
                ) VALUES ($1, $2, $3, 2048, $4, 1024)"#,
        )
        .bind(deal_id)
        .bind(piece_index)
        .bind(format!("piece-{deal_id}-{piece_index}"))
        .bind(snapshot_id)
        .execute(pool)
        .await
        .expect("piece seed should succeed");
    }
    snapshot_id
}

async fn seed_full_run(
    pool: &PgPool,
    deal_id: &str,
    run_id: Uuid,
    snapshot_id: Uuid,
    started_at: DateTime<Utc>,
    retrievability: &str,
) {
    sqlx::query(
        r#"INSERT INTO deal_sli_runs (
                id, deal_id, state, measurement_state, started_at, completed_at,
                tested_at, provider_id, client_id, working_url,
                retrievability_percent, large_files_percent, car_files_percent,
                sector_utilization_percent, is_consistent, is_reliable,
                result_code, error_code, piece_count, success_count, failed_count,
                url_metadata, manifest_snapshot_id, deal_size_bytes,
                manifest_size_bytes, content_matches_deal, sampled_piece_count,
                size_matched_percent, avg_response_time_ms
            ) VALUES (
                $1, $2, 'completed', 'fresh', $3, $3 + interval '5 minutes',
                $3 + interval '1 minute', '1234', '5678',
                'https://provider.example/piece', $4::numeric,
                88.88, 77.77, 66.66, true, false,
                'Success'::result_code, NULL, 2, 1, 1,
                '{"stored":"run"}'::jsonb, $5,
                123456789012345678901234567890,
                123456789012345678901234567889, false, 2, 50.25, 123.45
            )"#,
    )
    .bind(run_id)
    .bind(deal_id)
    .bind(started_at)
    .bind(retrievability)
    .bind(snapshot_id)
    .execute(pool)
    .await
    .expect("run seed should succeed");
}

#[allow(clippy::too_many_arguments)]
async fn seed_bms(
    pool: &PgPool,
    deal_id: &str,
    run_id: Uuid,
    row_id: Uuid,
    job_id: Uuid,
    status: &str,
    speed: Option<&str>,
    ttfb: Option<&str>,
    error: Option<&str>,
    created_at: DateTime<Utc>,
) {
    sqlx::query(
        r#"INSERT INTO deal_sli_bms_jobs (
                id, deal_id, run_id, piece_index, piece_cid, bms_job_id,
                url_tested, routing_key, worker_count, status, ping_avg_ms,
                head_avg_ms, ttfb_ms, download_speed_mbps, error_message,
                created_at, completed_at
            ) VALUES (
                $1, $2, $3, 0, $4, $5,
                'https://provider.example/piece', 'us_east', 3, $6,
                CASE WHEN $6 = 'Completed' THEN 12.125 ELSE NULL END,
                CASE WHEN $6 = 'Completed' THEN 24.250 ELSE NULL END,
                $7::numeric, $8::numeric, $9, $10,
                CASE WHEN $6 = 'Pending' THEN NULL ELSE $10 + interval '1 minute' END
            )"#,
    )
    .bind(row_id)
    .bind(deal_id)
    .bind(run_id)
    .bind(format!("piece-{deal_id}-0"))
    .bind(job_id)
    .bind(status)
    .bind(ttfb)
    .bind(speed)
    .bind(error)
    .bind(created_at)
    .execute(pool)
    .await
    .expect("BMS seed should succeed");
}

async fn seed_piece_result(
    pool: &PgPool,
    run_id: Uuid,
    snapshot_id: Uuid,
    url_tested: &str,
    success: bool,
    nullable: bool,
) {
    sqlx::query(
        r#"INSERT INTO deal_sli_piece_results (
                run_id, deal_id, piece_index, piece_cid, url_tested, success,
                content_length, is_valid_car, result_code, tested_at,
                manifest_snapshot_id, file_size_bytes, observed_size_bytes,
                size_matched, manifest_response_time_ms
            ) VALUES (
                $1, $2, 0, 'piece-42-0', $3, $4,
                CASE WHEN $5 THEN NULL ELSE 4096 END,
                CASE WHEN $5 THEN NULL ELSE true END,
                CASE WHEN $5 THEN NULL ELSE 'Success'::result_code END,
                '2026-09-08T12:01:00Z', $6,
                99999999999999999999999999999999999999,
                CASE WHEN $5 THEN NULL ELSE 4096 END,
                CASE WHEN $5 THEN NULL ELSE true END,
                CASE WHEN $5 THEN NULL ELSE 125 END
            )"#,
    )
    .bind(run_id)
    .bind(DEAL_ID)
    .bind(url_tested)
    .bind(success)
    .bind(nullable)
    .bind(snapshot_id)
    .execute(pool)
    .await
    .expect("piece result seed should succeed");
}

async fn fetch_all_history_pages(ctx: &TestContext, limit: usize) -> Vec<Value> {
    let mut cursor: Option<String> = None;
    let mut runs = vec![];
    loop {
        let mut path =
            format!("/deals/{DEAL_ID}/history/sli?from=2026-09-01&to=2026-09-08&limit={limit}");
        if let Some(cursor) = cursor.as_ref() {
            path.push_str("&cursor=");
            path.push_str(cursor);
        }
        let response = ctx.app.get(&path).await;
        assert_eq!(response.status_code(), StatusCode::OK);
        let body: Value = response.json();
        for day in body["data"]
            .as_object()
            .expect("data should be an object")
            .values()
        {
            runs.extend(
                day.as_array()
                    .expect("day should contain runs")
                    .iter()
                    .cloned(),
            );
        }
        cursor = body["next_cursor"].as_str().map(ToString::to_string);
        if cursor.is_none() {
            break;
        }
    }
    runs
}

#[tokio::test]
async fn deal_history_is_complete_stable_and_uses_exact_run_bms() {
    let ctx = TestContext::new().await;
    let snapshot_id = seed_target(&ctx.dbs.app_pool, DEAL_ID).await;
    let same_time = timestamp("2026-09-08T12:00:00Z");
    seed_full_run(
        &ctx.dbs.app_pool,
        DEAL_ID,
        uuid(RUN_1),
        snapshot_id,
        same_time,
        "10.00",
    )
    .await;
    seed_full_run(
        &ctx.dbs.app_pool,
        DEAL_ID,
        uuid(RUN_2),
        snapshot_id,
        same_time,
        "20.00",
    )
    .await;
    seed_full_run(
        &ctx.dbs.app_pool,
        DEAL_ID,
        uuid(RUN_3),
        snapshot_id,
        timestamp("2026-09-08T18:00:00Z"),
        "30.00",
    )
    .await;
    seed_full_run(
        &ctx.dbs.app_pool,
        DEAL_ID,
        uuid(RUN_4),
        snapshot_id,
        timestamp("2026-09-01T00:00:00Z"),
        "95.12",
    )
    .await;
    seed_full_run(
        &ctx.dbs.app_pool,
        DEAL_ID,
        Uuid::new_v4(),
        snapshot_id,
        timestamp("2026-08-31T23:59:59Z"),
        "99.00",
    )
    .await;
    seed_full_run(
        &ctx.dbs.app_pool,
        DEAL_ID,
        Uuid::new_v4(),
        snapshot_id,
        timestamp("2026-09-09T00:00:00Z"),
        "99.00",
    )
    .await;

    seed_bms(
        &ctx.dbs.app_pool,
        DEAL_ID,
        uuid(RUN_4),
        uuid("30000000-0000-4000-8000-000000000001"),
        uuid("40000000-0000-4000-8000-000000000001"),
        "Completed",
        Some("250.90"),
        Some("100.10"),
        None,
        timestamp("2026-09-09T01:00:00Z"),
    )
    .await;
    seed_bms(
        &ctx.dbs.app_pool,
        DEAL_ID,
        uuid(RUN_4),
        uuid("30000000-0000-4000-8000-000000000002"),
        uuid("40000000-0000-4000-8000-000000000002"),
        "Completed",
        Some("200.80"),
        Some("101.20"),
        None,
        timestamp("2026-09-09T02:00:00Z"),
    )
    .await;
    seed_bms(
        &ctx.dbs.app_pool,
        DEAL_ID,
        uuid(RUN_4),
        uuid("30000000-0000-4000-8000-000000000003"),
        uuid("40000000-0000-4000-8000-000000000003"),
        "Failed",
        None,
        None,
        Some("worker failed"),
        timestamp("2026-09-09T03:00:00Z"),
    )
    .await;
    seed_bms(
        &ctx.dbs.app_pool,
        DEAL_ID,
        uuid(RUN_4),
        uuid("30000000-0000-4000-8000-000000000004"),
        uuid("40000000-0000-4000-8000-000000000004"),
        "Pending",
        None,
        None,
        None,
        timestamp("2026-09-09T04:00:00Z"),
    )
    .await;
    seed_bms(
        &ctx.dbs.app_pool,
        DEAL_ID,
        uuid(RUN_1),
        uuid("30000000-0000-4000-8000-000000000005"),
        uuid("40000000-0000-4000-8000-000000000005"),
        "Completed",
        Some("10.00"),
        Some("999.00"),
        None,
        timestamp("2026-09-08T12:02:00Z"),
    )
    .await;

    let runs = fetch_all_history_pages(&ctx, 2).await;
    let ids = runs
        .iter()
        .map(|run| run["id"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(&ids[..2], &[RUN_3, RUN_2]);
    assert_eq!(
        ids.iter().map(String::as_str).collect::<HashSet<_>>(),
        HashSet::from([RUN_1, RUN_2, RUN_3, RUN_4])
    );
    assert_eq!(ids.iter().collect::<HashSet<_>>().len(), ids.len());

    let old_run = runs.iter().find(|run| run["id"] == RUN_4).unwrap();
    assert_eq!(old_run["state"], "completed");
    assert_eq!(old_run["measurement_state"], "fresh");
    assert_eq!(old_run["provider_id"], "1234");
    assert_eq!(old_run["client_id"], "5678");
    assert_eq!(old_run["retrievability_percent"], 95.12);
    assert_eq!(old_run["large_files_percent"], 88.88);
    assert_eq!(old_run["car_files_percent"], 77.77);
    assert_eq!(old_run["sector_utilization_percent"], 66.66);
    assert_eq!(old_run["is_consistent"], true);
    assert_eq!(old_run["is_reliable"], false);
    assert_eq!(old_run["url_metadata"], json!({"stored": "run"}));
    assert_eq!(old_run["deal_size_bytes"], "123456789012345678901234567890");
    assert_eq!(
        old_run["manifest_size_bytes"],
        "123456789012345678901234567889"
    );
    assert_eq!(old_run["sampled_piece_count"], 2);
    assert_eq!(old_run["size_matched_percent"], 50.25);
    assert_eq!(old_run["avg_response_time_ms"], 123.45);
    assert_eq!(
        old_run["porep_slis"],
        json!({
            "retrievability_bps": 9512,
            "bandwidth_mbps": 200,
            "latency_ms": 102,
            "indexing_pct": null
        })
    );
    let bms = old_run["bms_results"].as_array().unwrap();
    assert_eq!(bms.len(), 4);
    assert!(bms.iter().any(|job| {
        job["status"] == "Failed"
            && job["error_message"] == "worker failed"
            && job["ttfb_ms"].is_null()
            && job["completed_at"].is_string()
    }));
    assert!(bms.iter().any(|job| {
        job["status"] == "Pending"
            && job["error_message"].is_null()
            && job["completed_at"].is_null()
    }));
    for job in bms {
        assert_eq!(job["deal_id"], DEAL_ID);
        assert_eq!(job["run_id"], RUN_4);
        assert!(job["id"].is_string());
        assert!(job["bms_job_id"].is_string());
        assert!(job["created_at"].is_string());
    }
}

#[tokio::test]
async fn deal_history_handles_unmeasured_unknown_and_invalid_requests() {
    let ctx = TestContext::new().await;
    seed_target(&ctx.dbs.app_pool, DEAL_ID).await;

    let response = ctx
        .app
        .get(&format!(
            "/deals/{DEAL_ID}/history/sli?from=2026-09-01&to=2026-09-08"
        ))
        .await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Value = response.json();
    assert_eq!(body["data"], json!({}));
    assert!(body["next_cursor"].is_null());

    for (path, status) in [
        (
            "/deals/999/history/sli?from=2026-09-01&to=2026-09-08",
            StatusCode::NOT_FOUND,
        ),
        (
            "/deals/not-decimal/history/sli?from=2026-09-01&to=2026-09-08",
            StatusCode::BAD_REQUEST,
        ),
        (
            "/deals/42/history/sli?from=2026-09-08&to=2026-09-01",
            StatusCode::BAD_REQUEST,
        ),
        (
            "/deals/42/history/sli?from=2026-08-01&to=2026-09-08",
            StatusCode::BAD_REQUEST,
        ),
        (
            "/deals/42/history/sli?from=2026-09-01&to=2026-09-08&limit=0",
            StatusCode::BAD_REQUEST,
        ),
        (
            "/deals/42/history/sli?from=2026-09-01&to=2026-09-08&limit=501",
            StatusCode::BAD_REQUEST,
        ),
        (
            "/deals/42/history/sli?from=2026-09-01&to=2026-09-08&cursor=bad",
            StatusCode::BAD_REQUEST,
        ),
    ] {
        assert_eq!(ctx.app.get(path).await.status_code(), status, "{path}");
    }
}

#[tokio::test]
async fn deal_history_cursor_rejects_filter_and_resource_reuse() {
    let ctx = TestContext::new().await;
    let snapshot_id = seed_target(&ctx.dbs.app_pool, DEAL_ID).await;
    seed_target(&ctx.dbs.app_pool, OTHER_DEAL_ID).await;
    for (run_id, started_at) in [
        (RUN_1, "2026-09-08T12:00:00Z"),
        (RUN_2, "2026-09-08T13:00:00Z"),
    ] {
        seed_full_run(
            &ctx.dbs.app_pool,
            DEAL_ID,
            uuid(run_id),
            snapshot_id,
            timestamp(started_at),
            "50.00",
        )
        .await;
    }
    let response = ctx
        .app
        .get(&format!(
            "/deals/{DEAL_ID}/history/sli?from=2026-09-01&to=2026-09-08&limit=1"
        ))
        .await;
    let body: Value = response.json();
    let cursor = body["next_cursor"].as_str().unwrap();

    for path in [
        format!(
            "/deals/{DEAL_ID}/history/sli?from=2026-09-02&to=2026-09-08&limit=1&cursor={cursor}"
        ),
        format!(
            "/deals/{OTHER_DEAL_ID}/history/sli?from=2026-09-01&to=2026-09-08&limit=1&cursor={cursor}"
        ),
    ] {
        assert_eq!(
            ctx.app.get(&path).await.status_code(),
            StatusCode::BAD_REQUEST
        );
    }
}

#[tokio::test]
async fn deal_run_detail_paginates_equal_piece_indexes_and_preserves_nulls() {
    let ctx = TestContext::new().await;
    let snapshot_id = seed_target(&ctx.dbs.app_pool, DEAL_ID).await;
    seed_full_run(
        &ctx.dbs.app_pool,
        DEAL_ID,
        uuid(RUN_1),
        snapshot_id,
        timestamp("2026-09-08T12:00:00Z"),
        "50.00",
    )
    .await;
    let long_url = format!("https://a.example/{}", "x".repeat(4_096));
    seed_piece_result(
        &ctx.dbs.app_pool,
        uuid(RUN_1),
        snapshot_id,
        &long_url,
        true,
        false,
    )
    .await;
    seed_piece_result(
        &ctx.dbs.app_pool,
        uuid(RUN_1),
        snapshot_id,
        "https://b.example/piece",
        false,
        true,
    )
    .await;

    let first = ctx
        .app
        .get(&format!("/deals/{DEAL_ID}/runs/{RUN_1}?limit=1"))
        .await;
    assert_eq!(first.status_code(), StatusCode::OK);
    let first_body: Value = first.json();
    assert_eq!(first_body["run"]["id"], RUN_1);
    assert_eq!(first_body["piece_results"].as_array().unwrap().len(), 1);
    let first_piece = &first_body["piece_results"][0];
    assert_eq!(first_piece["url_tested"], long_url);
    assert_eq!(
        first_piece["file_size_bytes"],
        "99999999999999999999999999999999999999"
    );
    let cursor = first_body["next_cursor"].as_str().unwrap();
    assert!(cursor.len() < 2_048);

    let second = ctx
        .app
        .get(&format!(
            "/deals/{DEAL_ID}/runs/{RUN_1}?limit=1&cursor={cursor}"
        ))
        .await;
    assert_eq!(second.status_code(), StatusCode::OK);
    let second_body: Value = second.json();
    let second_piece = &second_body["piece_results"][0];
    assert_eq!(second_piece["piece_index"], 0);
    assert_eq!(second_piece["url_tested"], "https://b.example/piece");
    for field in [
        "content_length",
        "is_valid_car",
        "result_code",
        "observed_size_bytes",
        "size_matched",
        "manifest_response_time_ms",
    ] {
        assert!(second_piece[field].is_null(), "{field}");
    }
    assert!(second_body["next_cursor"].is_null());
}

#[tokio::test]
async fn deal_run_detail_validates_uuid_scope_limit_and_cursor() {
    let ctx = TestContext::new().await;
    let snapshot_id = seed_target(&ctx.dbs.app_pool, DEAL_ID).await;
    seed_target(&ctx.dbs.app_pool, OTHER_DEAL_ID).await;
    seed_full_run(
        &ctx.dbs.app_pool,
        DEAL_ID,
        uuid(RUN_1),
        snapshot_id,
        timestamp("2026-09-08T12:00:00Z"),
        "50.00",
    )
    .await;
    seed_piece_result(
        &ctx.dbs.app_pool,
        uuid(RUN_1),
        snapshot_id,
        "https://a.example/piece",
        true,
        false,
    )
    .await;
    seed_piece_result(
        &ctx.dbs.app_pool,
        uuid(RUN_1),
        snapshot_id,
        "https://b.example/piece",
        true,
        false,
    )
    .await;
    let first = ctx
        .app
        .get(&format!("/deals/{DEAL_ID}/runs/{RUN_1}?limit=1"))
        .await;
    let body: Value = first.json();
    let cursor = body["next_cursor"].as_str().unwrap();

    for (path, status) in [
        (
            format!("/deals/{OTHER_DEAL_ID}/runs/{RUN_1}"),
            StatusCode::NOT_FOUND,
        ),
        (
            format!("/deals/{DEAL_ID}/runs/not-a-uuid"),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/deals/not-decimal/runs/{RUN_1}"),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/deals/{DEAL_ID}/runs/{RUN_1}?limit=501"),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/deals/{OTHER_DEAL_ID}/runs/{RUN_1}?cursor={cursor}"),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/deals/{DEAL_ID}/runs/{RUN_2}?cursor={cursor}"),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/deals/{DEAL_ID}/runs/{RUN_1}?cursor=bad"),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        assert_eq!(ctx.app.get(&path).await.status_code(), status, "{path}");
    }
}

#[tokio::test]
async fn deal_run_detail_returns_a_run_without_piece_results() {
    let ctx = TestContext::new().await;
    let snapshot_id = seed_target(&ctx.dbs.app_pool, DEAL_ID).await;
    seed_full_run(
        &ctx.dbs.app_pool,
        DEAL_ID,
        uuid(RUN_1),
        snapshot_id,
        timestamp("2026-09-08T12:00:00Z"),
        "50.00",
    )
    .await;

    let response = ctx.app.get(&format!("/deals/{DEAL_ID}/runs/{RUN_1}")).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Value = response.json();
    assert_eq!(body["run"]["id"], RUN_1);
    assert_eq!(body["piece_results"], json!([]));
    assert!(body["next_cursor"].is_null());
    assert_eq!(body["run"]["bms_results"], json!([]));
    assert!(body["run"]["bms_next_cursor"].is_null());
}

#[tokio::test]
async fn deal_bms_preview_is_bounded_and_cursor_reaches_every_job() {
    let ctx = TestContext::new().await;
    let snapshot_id = seed_target(&ctx.dbs.app_pool, DEAL_ID).await;
    let other_snapshot_id = seed_target(&ctx.dbs.app_pool, OTHER_DEAL_ID).await;
    seed_full_run(
        &ctx.dbs.app_pool,
        DEAL_ID,
        uuid(RUN_1),
        snapshot_id,
        timestamp("2026-09-08T12:00:00Z"),
        "95.12",
    )
    .await;
    seed_full_run(
        &ctx.dbs.app_pool,
        OTHER_DEAL_ID,
        uuid(RUN_2),
        other_snapshot_id,
        timestamp("2026-09-08T12:00:00Z"),
        "50.00",
    )
    .await;

    let shared_time = timestamp("2026-09-08T13:00:00Z");
    for index in 1..=12 {
        let row_id = uuid(&format!("30000000-0000-4000-8000-{index:012}"));
        let job_id = uuid(&format!("40000000-0000-4000-8000-{index:012}"));
        let (speed, ttfb) = if index == 1 {
            ("1.90", "999.10")
        } else {
            ("200.80", "100.10")
        };
        seed_bms(
            &ctx.dbs.app_pool,
            DEAL_ID,
            uuid(RUN_1),
            row_id,
            job_id,
            "Completed",
            Some(speed),
            Some(ttfb),
            None,
            shared_time,
        )
        .await;
    }
    seed_bms(
        &ctx.dbs.app_pool,
        OTHER_DEAL_ID,
        uuid(RUN_2),
        uuid("30000000-0000-4000-8000-000000000099"),
        uuid("40000000-0000-4000-8000-000000000099"),
        "Completed",
        Some("0.10"),
        Some("5000.10"),
        None,
        shared_time,
    )
    .await;

    let history_response = ctx
        .app
        .get(&format!(
            "/deals/{DEAL_ID}/history/sli?from=2026-09-08&to=2026-09-08"
        ))
        .await;
    assert_eq!(history_response.status_code(), StatusCode::OK);
    let history: Value = history_response.json();
    let run = &history["data"]["2026-09-08"][0];
    assert_eq!(run["bms_results"].as_array().unwrap().len(), 10);
    assert!(run["bms_next_cursor"].is_string());
    assert_eq!(
        run["porep_slis"],
        json!({
            "retrievability_bps": 9512,
            "bandwidth_mbps": 1,
            "latency_ms": 1000,
            "indexing_pct": null
        })
    );
    assert!(
        run["bms_results"]
            .as_array()
            .unwrap()
            .iter()
            .all(|job| job["download_speed_mbps"] != 1.9)
    );

    let detail_response = ctx.app.get(&format!("/deals/{DEAL_ID}/runs/{RUN_1}")).await;
    assert_eq!(detail_response.status_code(), StatusCode::OK);
    let detail: Value = detail_response.json();
    assert_eq!(detail["run"]["bms_results"], run["bms_results"]);
    assert_eq!(detail["run"]["bms_next_cursor"], run["bms_next_cursor"]);
    assert_eq!(detail["run"]["porep_slis"], run["porep_slis"]);

    let preview_cursor = run["bms_next_cursor"].as_str().unwrap();
    let remainder_response = ctx
        .app
        .get(&format!(
            "/deals/{DEAL_ID}/runs/{RUN_1}/bms?cursor={preview_cursor}"
        ))
        .await;
    assert_eq!(remainder_response.status_code(), StatusCode::OK);
    let remainder: Value = remainder_response.json();
    let remainder_ids = remainder["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|job| job["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        remainder_ids,
        vec![
            "30000000-0000-4000-8000-000000000002",
            "30000000-0000-4000-8000-000000000001"
        ]
    );
    assert!(remainder["next_cursor"].is_null());

    let mut cursor: Option<String> = None;
    let mut all_ids = vec![];
    loop {
        let mut path = format!("/deals/{DEAL_ID}/runs/{RUN_1}/bms?limit=4");
        if let Some(cursor) = cursor.as_ref() {
            path.push_str("&cursor=");
            path.push_str(cursor);
        }
        let response = ctx.app.get(&path).await;
        assert_eq!(response.status_code(), StatusCode::OK);
        let page: Value = response.json();
        all_ids.extend(
            page["data"]
                .as_array()
                .unwrap()
                .iter()
                .map(|job| job["id"].as_str().unwrap().to_string()),
        );
        cursor = page["next_cursor"].as_str().map(ToString::to_string);
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(all_ids.len(), 12);
    assert_eq!(all_ids.iter().collect::<HashSet<_>>().len(), 12);
    assert_eq!(
        all_ids.first().unwrap(),
        "30000000-0000-4000-8000-000000000012"
    );
    assert_eq!(
        all_ids.last().unwrap(),
        "30000000-0000-4000-8000-000000000001"
    );
}

#[tokio::test]
async fn deal_bms_history_validates_scope_uuid_limit_and_cursor() {
    let ctx = TestContext::new().await;
    let snapshot_id = seed_target(&ctx.dbs.app_pool, DEAL_ID).await;
    seed_target(&ctx.dbs.app_pool, OTHER_DEAL_ID).await;
    seed_full_run(
        &ctx.dbs.app_pool,
        DEAL_ID,
        uuid(RUN_1),
        snapshot_id,
        timestamp("2026-09-08T12:00:00Z"),
        "50.00",
    )
    .await;
    for index in 1..=2 {
        seed_bms(
            &ctx.dbs.app_pool,
            DEAL_ID,
            uuid(RUN_1),
            uuid(&format!("30000000-0000-4000-8000-{index:012}")),
            uuid(&format!("40000000-0000-4000-8000-{index:012}")),
            "Pending",
            None,
            None,
            None,
            timestamp("2026-09-08T13:00:00Z"),
        )
        .await;
    }
    let first = ctx
        .app
        .get(&format!("/deals/{DEAL_ID}/runs/{RUN_1}/bms?limit=1"))
        .await;
    let first_body: Value = first.json();
    let cursor = first_body["next_cursor"].as_str().unwrap();
    let oversized = "a".repeat(2_049);

    for (path, status) in [
        (
            format!("/deals/{OTHER_DEAL_ID}/runs/{RUN_1}/bms"),
            StatusCode::NOT_FOUND,
        ),
        (
            format!("/deals/{DEAL_ID}/runs/{RUN_2}/bms"),
            StatusCode::NOT_FOUND,
        ),
        (
            format!("/deals/{DEAL_ID}/runs/not-a-uuid/bms"),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/deals/{DEAL_ID}/runs/{RUN_1}/bms?limit=0"),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/deals/{DEAL_ID}/runs/{RUN_1}/bms?limit=501"),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/deals/{DEAL_ID}/runs/{RUN_1}/bms?cursor=bad"),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/deals/{DEAL_ID}/runs/{RUN_1}/bms?cursor={oversized}"),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/deals/{OTHER_DEAL_ID}/runs/{RUN_1}/bms?cursor={cursor}"),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/deals/{DEAL_ID}/runs/{RUN_2}/bms?cursor={cursor}"),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        assert_eq!(ctx.app.get(&path).await.status_code(), status, "{path}");
    }
}

#[tokio::test]
async fn deal_history_openapi_documents_paths_queries_and_full_schemas() {
    let ctx = TestContext::new().await;
    let response = ctx.app.get("/api-doc/openapi.json").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Value = response.json();

    let history = &body["paths"]["/deals/{deal_id}/history/sli"]["get"];
    assert!(history["responses"]["404"].is_object());
    assert!(history["responses"]["500"].is_object());
    let history_parameters = history["parameters"].as_array().unwrap();
    for name in ["deal_id", "from", "to", "limit", "cursor"] {
        assert!(history_parameters.iter().any(|value| value["name"] == name));
    }

    let detail = &body["paths"]["/deals/{deal_id}/runs/{run_id}"]["get"];
    assert!(detail["responses"]["404"].is_object());
    assert!(detail["responses"]["500"].is_object());
    let detail_parameters = detail["parameters"].as_array().unwrap();
    for name in ["deal_id", "run_id", "limit", "cursor"] {
        assert!(detail_parameters.iter().any(|value| value["name"] == name));
    }

    let bms = &body["paths"]["/deals/{deal_id}/runs/{run_id}/bms"]["get"];
    assert!(bms["responses"]["404"].is_object());
    assert!(bms["responses"]["500"].is_object());
    let bms_parameters = bms["parameters"].as_array().unwrap();
    for name in ["deal_id", "run_id", "limit", "cursor"] {
        assert!(bms_parameters.iter().any(|value| value["name"] == name));
    }

    let schemas = &body["components"]["schemas"];
    for schema in [
        "DealSliHistoryResponse",
        "DealSliRunDetailResponse",
        "DealSliRunResponse",
        "DealSliBmsHistoryResponse",
        "DealSliPieceResultResponse",
        "DealBmsHistoryQuery",
        "DealBmsHistoryResponse",
    ] {
        assert!(schemas[schema].is_object(), "missing {schema}");
    }
}
