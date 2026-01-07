use color_eyre::{Result, eyre::eyre};
use reqwest_middleware::ClientWithMiddleware;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::utils::build_reqwest_retry_client_with_config;

const BMS_ROUTING_KEY: &str = "us_east";

// BMS client timeouts - aggressive to fail fast on gateway timeouts
const BMS_MIN_RETRY_INTERVAL_MS: u64 = 1_000;
const BMS_MAX_RETRY_INTERVAL_MS: u64 = 5_000;
const BMS_MAX_RETRIES: u32 = 1; // Reduced from 3 - 504s indicate BMS is struggling
const BMS_CONNECT_TIMEOUT_MS: u64 = 5_000; // 5s connect timeout
const BMS_REQUEST_TIMEOUT_MS: u64 = 30_000; // 30s request timeout (less than typical gateway 60s)

#[derive(Debug, Clone, Serialize)]
pub struct CreateJobRequest {
    pub url: String,
    pub routing_key: String,
    pub worker_count: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BmsJob {
    pub id: Uuid,
    pub status: String,
    pub url: String,
    pub routing_key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BmsJobDetails {
    pub worker_count: Option<i64>,
    pub size_mb: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DownloadResult {
    pub download_speed: Option<f64>,
    pub time_to_first_byte_ms: Option<f64>,
    pub total_bytes: Option<i64>,
    pub elapsed_secs: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PingResult {
    pub avg: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HeadResult {
    pub avg: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WorkerData {
    pub download: Option<DownloadResult>,
    pub ping: Option<PingResult>,
    pub head: Option<HeadResult>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SubJob {
    pub id: Uuid,
    pub status: String,
    pub worker_data: Option<Vec<WorkerData>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BmsJobResponse {
    pub id: Uuid,
    pub status: String,
    pub url: String,
    pub routing_key: String,
    pub details: Option<BmsJobDetails>,
    pub sub_jobs: Option<Vec<SubJob>>,
}

#[derive(Clone)]
pub struct BmsClient {
    client: ClientWithMiddleware,
    base_url: String,
}

impl BmsClient {
    pub fn new(base_url: String) -> Self {
        Self {
            client: build_reqwest_retry_client_with_config(
                BMS_MIN_RETRY_INTERVAL_MS,
                BMS_MAX_RETRY_INTERVAL_MS,
                BMS_MAX_RETRIES,
                Some(BMS_CONNECT_TIMEOUT_MS),
                Some(BMS_REQUEST_TIMEOUT_MS),
            ),
            base_url,
        }
    }

    pub async fn create_job(
        &self,
        url: String,
        worker_count: i64,
        entity: Option<String>,
    ) -> Result<BmsJob> {
        if worker_count < 0 {
            return Err(eyre!(
                "worker_count must be non-negative, got {worker_count}"
            ));
        }

        let request = CreateJobRequest {
            url,
            routing_key: BMS_ROUTING_KEY.to_string(),
            worker_count,
            entity,
        };

        debug!("Creating BMS job: {:?}", request);

        let response = self
            .client
            .post(format!("{}/jobs", self.base_url))
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            warn!("BMS create job failed: {status} - {body}");
            return Err(eyre!("BMS create job failed: {status} - {body}"));
        }

        let result: BmsJob = response.json().await?;
        debug!("BMS job created: {:?}", result);
        Ok(result)
    }

    pub async fn get_job(&self, job_id: Uuid) -> Result<BmsJobResponse> {
        debug!("Fetching BMS job: {job_id}");

        let response = self
            .client
            .get(format!("{}/jobs/{job_id}", self.base_url))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            warn!("BMS get job failed: {status} - {body}");
            return Err(eyre!("BMS get job failed: {status} - {body}"));
        }

        let result: BmsJobResponse = response.json().await?;
        debug!("BMS job fetched: {} - status: {}", result.id, result.status);
        Ok(result)
    }

    pub fn is_job_finished(status: &str) -> bool {
        matches!(status, "Completed" | "Failed" | "Cancelled")
    }
}
