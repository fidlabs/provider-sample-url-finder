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
pub struct Job {
    pub id: Uuid,
    pub working_url: Option<String>,
    pub retrievability: Option<i64>,
    pub provider: String,
    pub client: Option<String>,
    pub status: JobStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<ResultCode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorCode>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
impl Job {
    pub fn new(provider: String, client: Option<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            working_url: None,
            retrievability: None,
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

    pub async fn create_job(&self, provider: String, client: Option<String>) -> Result<Job> {
        let job = Job::new(provider, client);

        let mut db = self.db.write().unwrap();
        db.insert(job.id, job.clone());

        Ok(job)
    }

    pub async fn update_job_result(
        &self,
        job_id: Uuid,
        working_url: Option<String>,
        retrievability: f64,
    ) {
        let mut db = self.db.write().unwrap();

        if let Some(job) = db.get_mut(&job_id) {
            job.working_url = working_url;
            job.retrievability = Some(retrievability as i64);
            job.status = JobStatus::Completed;
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
