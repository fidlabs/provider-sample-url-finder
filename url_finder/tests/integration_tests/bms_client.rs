use serde_json::json;
use url_finder::bms_client::BmsClient;
use uuid::Uuid;
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn create_bms_mock() -> (MockServer, BmsClient) {
    let mock = MockServer::start().await;
    let client = BmsClient::new(mock.uri());
    (mock, client)
}

// --- create_job tests ---

#[tokio::test]
async fn test_create_job_success() {
    let (mock, client) = create_bms_mock().await;

    let job_id = Uuid::new_v4();
    Mock::given(method("POST"))
        .and(path("/jobs"))
        .and(body_partial_json(json!({
            "url": "http://example.com/file",
            "routing_key": "us_east",
            "worker_count": 3
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": job_id,
            "status": "Pending",
            "url": "http://example.com/file",
            "routing_key": "us_east"
        })))
        .mount(&mock)
        .await;

    let result = client
        .create_job(
            "http://example.com/file".to_string(),
            3,
            Some("f012345".to_string()),
        )
        .await;

    assert!(result.is_ok());
    let job = result.unwrap();
    assert_eq!(job.id, job_id);
    assert_eq!(job.status, "Pending");
    assert_eq!(job.url, "http://example.com/file");
    assert_eq!(job.routing_key, "us_east");
}

#[tokio::test]
async fn test_create_job_with_entity() {
    let (mock, client) = create_bms_mock().await;

    let job_id = Uuid::new_v4();
    Mock::given(method("POST"))
        .and(path("/jobs"))
        .and(body_partial_json(json!({
            "url": "http://example.com/file",
            "entity": "f012345"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": job_id,
            "status": "Pending",
            "url": "http://example.com/file",
            "routing_key": "us_east"
        })))
        .mount(&mock)
        .await;

    let result = client
        .create_job(
            "http://example.com/file".to_string(),
            3,
            Some("f012345".to_string()),
        )
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_create_job_without_entity() {
    let (mock, client) = create_bms_mock().await;

    let job_id = Uuid::new_v4();
    Mock::given(method("POST"))
        .and(path("/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": job_id,
            "status": "Pending",
            "url": "http://example.com/file",
            "routing_key": "us_east"
        })))
        .mount(&mock)
        .await;

    let result = client
        .create_job("http://example.com/file".to_string(), 3, None)
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_create_job_bad_request() {
    let (mock, client) = create_bms_mock().await;

    Mock::given(method("POST"))
        .and(path("/jobs"))
        .respond_with(
            ResponseTemplate::new(400).set_body_json(json!({"error": "Invalid URL format"})),
        )
        .mount(&mock)
        .await;

    let result = client
        .create_job("not-a-valid-url".to_string(), 3, None)
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("400"), "Error should mention 400 status");
}

#[tokio::test]
async fn test_create_job_server_error() {
    let (mock, client) = create_bms_mock().await;

    Mock::given(method("POST"))
        .and(path("/jobs"))
        .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
        .mount(&mock)
        .await;

    let result = client
        .create_job("http://example.com/file".to_string(), 3, None)
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("500"), "Error should mention 500 status");
}

#[tokio::test]
async fn test_create_job_negative_worker_count() {
    let (_mock, client) = create_bms_mock().await;

    let result = client
        .create_job("http://example.com/file".to_string(), -1, None)
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("non-negative"),
        "Error should mention non-negative requirement"
    );
}

// --- get_job tests ---

#[tokio::test]
async fn test_get_job_success_pending() {
    let (mock, client) = create_bms_mock().await;

    let job_id = Uuid::new_v4();
    Mock::given(method("GET"))
        .and(path(format!("/jobs/{job_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": job_id,
            "status": "Pending",
            "url": "http://example.com/file",
            "routing_key": "us_east",
            "details": {
                "worker_count": 3,
                "size_mb": 100
            }
        })))
        .mount(&mock)
        .await;

    let result = client.get_job(job_id).await;

    assert!(result.is_ok());
    let job = result.unwrap();
    assert_eq!(job.id, job_id);
    assert_eq!(job.status, "Pending");
    assert!(!BmsClient::is_job_finished(&job.status));
}

#[tokio::test]
async fn test_get_job_success_completed_with_results() {
    let (mock, client) = create_bms_mock().await;

    let job_id = Uuid::new_v4();
    let subjob_id = Uuid::new_v4();

    Mock::given(method("GET"))
        .and(path(format!("/jobs/{job_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": job_id,
            "status": "Completed",
            "url": "http://example.com/file",
            "routing_key": "us_east",
            "details": {
                "worker_count": 3,
                "size_mb": 100
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
        .mount(&mock)
        .await;

    let result = client.get_job(job_id).await;

    assert!(result.is_ok());
    let job = result.unwrap();
    assert_eq!(job.id, job_id);
    assert_eq!(job.status, "Completed");
    assert!(BmsClient::is_job_finished(&job.status));

    let sub_jobs = job.sub_jobs.unwrap();
    assert_eq!(sub_jobs.len(), 1);
    assert_eq!(sub_jobs[0].status, "Completed");

    let worker_data = sub_jobs[0].worker_data.as_ref().unwrap();
    assert_eq!(worker_data.len(), 1);
    assert_eq!(worker_data[0].ping.as_ref().unwrap().avg, Some(0.025));
    assert_eq!(
        worker_data[0].download.as_ref().unwrap().download_speed,
        Some(500.0)
    );
}

#[tokio::test]
async fn test_get_job_not_found() {
    let (mock, client) = create_bms_mock().await;

    let job_id = Uuid::new_v4();
    Mock::given(method("GET"))
        .and(path(format!("/jobs/{job_id}")))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({"error": "Job not found"})))
        .mount(&mock)
        .await;

    let result = client.get_job(job_id).await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("404"), "Error should mention 404 status");
}

#[tokio::test]
async fn test_get_job_server_error() {
    let (mock, client) = create_bms_mock().await;

    let job_id = Uuid::new_v4();
    Mock::given(method("GET"))
        .and(path(format!("/jobs/{job_id}")))
        .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
        .mount(&mock)
        .await;

    let result = client.get_job(job_id).await;

    assert!(result.is_err());
}

// --- is_job_finished tests ---

#[tokio::test]
async fn test_is_job_finished_statuses() {
    assert!(BmsClient::is_job_finished("Completed"));
    assert!(BmsClient::is_job_finished("Failed"));
    assert!(BmsClient::is_job_finished("Cancelled"));
    assert!(!BmsClient::is_job_finished("Pending"));
    assert!(!BmsClient::is_job_finished("Running"));
    assert!(!BmsClient::is_job_finished("InProgress"));
    assert!(!BmsClient::is_job_finished("unknown"));
}
