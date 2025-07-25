use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use chrono::{DateTime, Utc};
use color_eyre::{eyre::eyre, Result};
use serde::Serialize;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{ErrorCode, ResultCode};

#[derive(Clone, Serialize, ToSchema)]
pub struct ProviderResult {
    pub provider: String,
    pub client: Option<String>,
    pub working_url: Option<String>,
    pub retrievability: f64,
    pub result: ResultCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorCode>,
}

#[derive(Clone, Serialize, ToSchema)]
pub struct Job {
    pub id: Uuid,
    // working_url and retrievability are kept for FE compatibility
    pub working_url: Option<String>,
    pub retrievability: Option<i64>,
    pub results: Vec<ProviderResult>,
    pub provider: Option<String>,
    pub client: Option<String>,
    pub status: JobStatus,
    pub result: Option<ResultCode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorCode>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
impl Job {
    pub fn new(provider: Option<String>, client: Option<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            working_url: None,
            retrievability: None,
            results: Vec::new(),
            provider,
            client,
            status: JobStatus::Pending,
            result: None,
            error: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }
}

pub struct JobRepository {
    db: Arc<RwLock<HashMap<Uuid, Job>>>,
}
impl Default for JobRepository {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, ToSchema)]
pub enum JobStatus {
    Pending,
    Completed,
    Failed,
}

// TODO: Replace with a real database implementation
impl JobRepository {
    pub fn new() -> Self {
        Self {
            db: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn create_job(
        &self,
        provider: Option<String>,
        client: Option<String>,
    ) -> Result<Job> {
        let job = Job::new(provider, client);

        let mut db = self.db.write().unwrap();
        db.insert(job.id, job.clone());

        Ok(job)
    }

    pub async fn add_success_result(
        &self,
        job_id: Uuid,
        provider: String,
        client: Option<String>,
        working_url: Option<String>,
        retrievability: f64,
        result: ResultCode,
    ) {
        let mut db = self.db.write().unwrap();

        if let Some(job) = db.get_mut(&job_id) {
            let provider_result = ProviderResult {
                provider,
                client,
                working_url: working_url.clone(),
                retrievability,
                result,
                error: None,
            };

            job.results.push(provider_result);
            job.updated_at = Utc::now();
        }
    }

    pub async fn add_error_result(
        &self,
        job_id: Uuid,
        provider: String,
        client: Option<String>,
        error: Option<ErrorCode>,
        result: Option<ResultCode>,
    ) {
        let mut db = self.db.write().unwrap();

        if let Some(job) = db.get_mut(&job_id) {
            let provider_result = ProviderResult {
                provider,
                client,
                working_url: None,
                retrievability: 0.0,
                result: result.unwrap_or(ResultCode::Error),
                error,
            };

            job.results.push(provider_result);
            job.updated_at = Utc::now();
        }
    }

    pub async fn set_status(&self, job_id: Uuid, status: JobStatus) {
        let mut db = self.db.write().unwrap();

        if let Some(job) = db.get_mut(&job_id) {
            job.status = status;
            job.updated_at = Utc::now();
        }
    }

    pub async fn fail_job(
        &self,
        job_id: Uuid,
        result: Option<ResultCode>,
        error: Option<ErrorCode>,
    ) {
        let mut db = self.db.write().unwrap();

        if let Some(job) = db.get_mut(&job_id) {
            job.status = JobStatus::Failed;
            job.result = result;
            job.error = error;
            job.updated_at = Utc::now();
        }
    }

    pub async fn get_job(&self, id: Uuid) -> Result<Job> {
        let db = self.db.read().unwrap();

        db.get(&id).cloned().ok_or_else(|| eyre!("Job not found"))
    }

    pub async fn get_pending(&self) -> Vec<Job> {
        let db = self.db.read().unwrap();

        db.values()
            .filter(|job| job.status == JobStatus::Pending)
            .cloned()
            .collect()
    }

    pub async fn get_first_pending(&self) -> Option<Job> {
        let db = self.db.read().unwrap();

        db.values()
            .find(|job| job.status == JobStatus::Pending)
            .cloned()
    }
}
