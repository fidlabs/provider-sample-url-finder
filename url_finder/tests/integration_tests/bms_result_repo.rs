use sqlx::PgPool;
use url_finder::repository::BmsBandwidthResultRepository;
use url_finder::types::ProviderId;
use uuid::Uuid;

use crate::common::*;

async fn create_repo(pool: &PgPool) -> BmsBandwidthResultRepository {
    BmsBandwidthResultRepository::new(pool.clone())
}

fn provider_id(id: &str) -> ProviderId {
    ProviderId::new(id).unwrap()
}

#[tokio::test]
async fn test_insert_pending_creates_record() {
    let ctx = TestContext::new().await;
    let repo = create_repo(&ctx.dbs.app_pool).await;

    let job_id = Uuid::new_v4();
    let provider = provider_id(TEST_PROVIDER_1_DB);

    let result = repo
        .insert_pending(&provider, job_id, TEST_WORKING_URL, "us_east", 3)
        .await
        .expect("Failed to insert pending result");

    assert_eq!(result.provider_id, TEST_PROVIDER_1_DB);
    assert_eq!(result.bms_job_id, job_id);
    assert_eq!(result.url_tested, TEST_WORKING_URL);
    assert_eq!(result.routing_key, "us_east");
    assert_eq!(result.worker_count, 3);
    assert_eq!(result.status, "Pending");
    assert!(result.ping_avg_ms.is_none());
    assert!(result.head_avg_ms.is_none());
    assert!(result.ttfb_ms.is_none());
    assert!(result.download_speed_mbps.is_none());
    assert!(result.completed_at.is_none());
}

#[tokio::test]
async fn test_insert_pending_duplicate_job_id_fails() {
    let ctx = TestContext::new().await;
    let repo = create_repo(&ctx.dbs.app_pool).await;

    let job_id = Uuid::new_v4();
    let provider = provider_id(TEST_PROVIDER_1_DB);

    // First insert succeeds
    repo.insert_pending(&provider, job_id, TEST_WORKING_URL, "us_east", 3)
        .await
        .expect("First insert should succeed");

    // Second insert with same job_id should fail (UNIQUE constraint)
    let result = repo
        .insert_pending(&provider, job_id, TEST_WORKING_URL, "us_east", 3)
        .await;

    assert!(result.is_err(), "Duplicate job_id should fail");
}

#[tokio::test]
async fn test_get_pending_returns_only_pending() {
    let ctx = TestContext::new().await;
    let repo = create_repo(&ctx.dbs.app_pool).await;

    let provider = provider_id(TEST_PROVIDER_1_DB);
    let job1 = Uuid::new_v4();
    let job2 = Uuid::new_v4();

    // Insert two pending results
    repo.insert_pending(&provider, job1, TEST_WORKING_URL, "us_east", 3)
        .await
        .unwrap();
    repo.insert_pending(&provider, job2, TEST_WORKING_URL_2, "eu_west", 5)
        .await
        .unwrap();

    // Mark one as completed
    repo.update_completed(
        job1,
        "Completed",
        Some(10.0),
        Some(20.0),
        Some(30.0),
        Some(100.0),
    )
    .await
    .unwrap();

    // get_pending should only return job2
    let pending = repo.get_pending().await.unwrap();

    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].bms_job_id, job2);
    assert_eq!(pending[0].status, "Pending");
}

#[tokio::test]
async fn test_update_completed_sets_all_fields() {
    let ctx = TestContext::new().await;
    let repo = create_repo(&ctx.dbs.app_pool).await;

    let provider = provider_id(TEST_PROVIDER_1_DB);
    let job_id = Uuid::new_v4();

    repo.insert_pending(&provider, job_id, TEST_WORKING_URL, "us_east", 3)
        .await
        .unwrap();

    repo.update_completed(
        job_id,
        "Completed",
        Some(15.5),
        Some(25.0),
        Some(50.0),
        Some(500.0),
    )
    .await
    .unwrap();

    // Verify by fetching latest
    let result = repo
        .get_latest_for_provider(&provider)
        .await
        .unwrap()
        .expect("Should have result");

    assert_eq!(result.status, "Completed");
    assert!(result.completed_at.is_some());

    // Compare BigDecimal values as f64 (approximate)
    let ping = result
        .ping_avg_ms
        .map(|v| v.to_string().parse::<f64>().unwrap());
    let head = result
        .head_avg_ms
        .map(|v| v.to_string().parse::<f64>().unwrap());
    let ttfb = result
        .ttfb_ms
        .map(|v| v.to_string().parse::<f64>().unwrap());
    let speed = result
        .download_speed_mbps
        .map(|v| v.to_string().parse::<f64>().unwrap());

    assert_eq!(ping, Some(15.5));
    assert_eq!(head, Some(25.0));
    assert_eq!(ttfb, Some(50.0));
    assert_eq!(speed, Some(500.0));
}

#[tokio::test]
async fn test_update_completed_not_found_fails() {
    let ctx = TestContext::new().await;
    let repo = create_repo(&ctx.dbs.app_pool).await;

    let nonexistent_job = Uuid::new_v4();

    let result = repo
        .update_completed(
            nonexistent_job,
            "Completed",
            Some(10.0),
            Some(20.0),
            Some(30.0),
            Some(100.0),
        )
        .await;

    assert!(result.is_err(), "Update for non-existent job should fail");
}

#[tokio::test]
async fn test_get_latest_completed_excludes_pending() {
    let ctx = TestContext::new().await;
    let repo = create_repo(&ctx.dbs.app_pool).await;

    let provider = provider_id(TEST_PROVIDER_1_DB);

    // Insert a completed result first
    let job1 = Uuid::new_v4();
    repo.insert_pending(&provider, job1, TEST_WORKING_URL, "us_east", 3)
        .await
        .unwrap();
    repo.update_completed(
        job1,
        "Completed",
        Some(10.0),
        Some(20.0),
        Some(30.0),
        Some(100.0),
    )
    .await
    .unwrap();

    // Insert a pending result (more recent)
    let job2 = Uuid::new_v4();
    repo.insert_pending(&provider, job2, TEST_WORKING_URL_2, "eu_west", 5)
        .await
        .unwrap();

    // get_latest_completed should return job1, not job2
    let result = repo
        .get_latest_completed_for_provider(&provider)
        .await
        .unwrap()
        .expect("Should have completed result");

    assert_eq!(result.bms_job_id, job1);
    assert_eq!(result.status, "Completed");
}

#[tokio::test]
async fn test_get_latest_completed_returns_none_when_only_pending() {
    let ctx = TestContext::new().await;
    let repo = create_repo(&ctx.dbs.app_pool).await;

    let provider = provider_id(TEST_PROVIDER_1_DB);
    let job_id = Uuid::new_v4();

    // Insert only pending
    repo.insert_pending(&provider, job_id, TEST_WORKING_URL, "us_east", 3)
        .await
        .unwrap();

    let result = repo
        .get_latest_completed_for_provider(&provider)
        .await
        .unwrap();

    assert!(result.is_none());
}

#[tokio::test]
async fn test_get_latest_completed_for_providers_batch() {
    let ctx = TestContext::new().await;
    let repo = create_repo(&ctx.dbs.app_pool).await;

    let provider1 = provider_id(TEST_PROVIDER_1_DB);
    let provider2 = provider_id(TEST_PROVIDER_2_DB);

    // Provider 1: completed result
    let job1 = Uuid::new_v4();
    repo.insert_pending(&provider1, job1, TEST_WORKING_URL, "us_east", 3)
        .await
        .unwrap();
    repo.update_completed(
        job1,
        "Completed",
        Some(10.0),
        Some(20.0),
        Some(30.0),
        Some(100.0),
    )
    .await
    .unwrap();

    // Provider 2: completed result
    let job2 = Uuid::new_v4();
    repo.insert_pending(&provider2, job2, TEST_WORKING_URL_2, "eu_west", 5)
        .await
        .unwrap();
    repo.update_completed(
        job2,
        "Completed",
        Some(15.0),
        Some(25.0),
        Some(35.0),
        Some(200.0),
    )
    .await
    .unwrap();

    // Batch query
    let ids = vec![
        TEST_PROVIDER_1_DB.to_string(),
        TEST_PROVIDER_2_DB.to_string(),
    ];
    let results = repo.get_latest_completed_for_providers(&ids).await.unwrap();

    assert_eq!(results.len(), 2);

    let r1 = results.iter().find(|r| r.provider_id == TEST_PROVIDER_1_DB);
    let r2 = results.iter().find(|r| r.provider_id == TEST_PROVIDER_2_DB);

    assert!(r1.is_some());
    assert!(r2.is_some());
    assert_eq!(r1.unwrap().bms_job_id, job1);
    assert_eq!(r2.unwrap().bms_job_id, job2);
}

#[tokio::test]
async fn test_get_latest_completed_for_providers_empty_input() {
    let ctx = TestContext::new().await;
    let repo = create_repo(&ctx.dbs.app_pool).await;

    let results = repo.get_latest_completed_for_providers(&[]).await.unwrap();

    assert!(results.is_empty());
}

#[tokio::test]
async fn test_get_history_for_provider() {
    let ctx = TestContext::new().await;
    let repo = create_repo(&ctx.dbs.app_pool).await;

    let provider = provider_id(TEST_PROVIDER_1_DB);

    // Insert 3 results with small delays to ensure unique timestamps
    for i in 0..3 {
        let job_id = Uuid::new_v4();
        repo.insert_pending(&provider, job_id, TEST_WORKING_URL, "us_east", 3)
            .await
            .unwrap();
        repo.update_completed(job_id, "Completed", Some(10.0 + i as f64), None, None, None)
            .await
            .unwrap();
        // Sleep between iterations to ensure unique created_at timestamps
        // so the ordering assertion is meaningfully validated
        if i < 2 {
            tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
        }
    }

    // Get history with limit 2
    let history = repo.get_history_for_provider(&provider, 2).await.unwrap();

    assert_eq!(history.len(), 2);
    // Verify ordered by created_at DESC (most recent first)
    // Use strict inequality to confirm timestamps are actually different
    assert!(
        history[0].created_at > history[1].created_at,
        "History should be ordered by created_at DESC with distinct timestamps: first={:?}, second={:?}",
        history[0].created_at,
        history[1].created_at
    );
}
