use chrono::{DateTime, Utc};
use color_eyre::Result;
use sqlx::PgPool;
use sqlx::types::BigDecimal;
use std::str::FromStr;
use uuid::Uuid;

use crate::types::ProviderId;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct BmsBandwidthResult {
    pub id: Uuid,
    pub provider_id: String,
    pub bms_job_id: Uuid,
    pub url_tested: String,
    pub routing_key: String,
    pub worker_count: i32,
    pub status: String,
    pub ping_avg_ms: Option<BigDecimal>,
    pub head_avg_ms: Option<BigDecimal>,
    pub ttfb_ms: Option<BigDecimal>,
    pub download_speed_mbps: Option<BigDecimal>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct NewBmsBandwidthResult {
    pub provider_id: ProviderId,
    pub bms_job_id: Uuid,
    pub url_tested: String,
    pub routing_key: String,
    pub worker_count: i32,
    pub status: String,
    pub ping_avg_ms: Option<f64>,
    pub head_avg_ms: Option<f64>,
    pub ttfb_ms: Option<f64>,
    pub download_speed_mbps: Option<f64>,
    pub completed_at: Option<DateTime<Utc>>,
}

fn f64_to_bigdecimal(val: Option<f64>) -> Option<BigDecimal> {
    val.and_then(|v| BigDecimal::from_str(&v.to_string()).ok())
}

#[derive(Clone)]
pub struct BmsBandwidthResultRepository {
    pool: PgPool,
}

impl BmsBandwidthResultRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn insert(&self, result: &NewBmsBandwidthResult) -> Result<BmsBandwidthResult> {
        let ping_avg_ms = f64_to_bigdecimal(result.ping_avg_ms);
        let head_avg_ms = f64_to_bigdecimal(result.head_avg_ms);
        let ttfb_ms = f64_to_bigdecimal(result.ttfb_ms);
        let download_speed_mbps = f64_to_bigdecimal(result.download_speed_mbps);

        Ok(sqlx::query_as!(
            BmsBandwidthResult,
            r#"INSERT INTO
                    bms_bandwidth_results (
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
                        completed_at
                    )
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
               RETURNING
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
            "#,
            result.provider_id.as_str(),
            result.bms_job_id,
            result.url_tested,
            result.routing_key,
            result.worker_count,
            result.status,
            ping_avg_ms,
            head_avg_ms,
            ttfb_ms,
            download_speed_mbps,
            result.completed_at
        )
        .fetch_one(&self.pool)
        .await?)
    }

    pub async fn insert_pending(
        &self,
        provider_id: &ProviderId,
        job_id: Uuid,
        url: &str,
        routing_key: &str,
        worker_count: i32,
    ) -> Result<BmsBandwidthResult> {
        Ok(sqlx::query_as!(
            BmsBandwidthResult,
            r#"INSERT INTO
                    bms_bandwidth_results (
                        provider_id,
                        bms_job_id,
                        url_tested,
                        routing_key,
                        worker_count,
                        status
                    )
               VALUES ($1, $2, $3, $4, $5, 'Pending')
               RETURNING
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
            "#,
            provider_id.as_str(),
            job_id,
            url,
            routing_key,
            worker_count
        )
        .fetch_one(&self.pool)
        .await?)
    }

    pub async fn get_pending(&self) -> Result<Vec<BmsBandwidthResult>> {
        Ok(sqlx::query_as!(
            BmsBandwidthResult,
            r#"SELECT
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
               FROM
                    bms_bandwidth_results
               WHERE
                    status = 'Pending'
               ORDER BY
                    created_at ASC,
                    id ASC
            "#
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn update_completed(
        &self,
        job_id: Uuid,
        status: &str,
        ping_avg_ms: Option<f64>,
        head_avg_ms: Option<f64>,
        ttfb_ms: Option<f64>,
        download_speed_mbps: Option<f64>,
    ) -> Result<()> {
        let ping = f64_to_bigdecimal(ping_avg_ms);
        let head = f64_to_bigdecimal(head_avg_ms);
        let ttfb = f64_to_bigdecimal(ttfb_ms);
        let speed = f64_to_bigdecimal(download_speed_mbps);

        let result = sqlx::query!(
            r#"UPDATE
                    bms_bandwidth_results
               SET
                    status = $2,
                    ping_avg_ms = $3,
                    head_avg_ms = $4,
                    ttfb_ms = $5,
                    download_speed_mbps = $6,
                    completed_at = NOW()
               WHERE
                    bms_job_id = $1
            "#,
            job_id,
            status,
            ping,
            head,
            ttfb,
            speed
        )
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(color_eyre::eyre::eyre!("BMS job not found: {job_id}"));
        }

        Ok(())
    }

    pub async fn get_latest_for_provider(
        &self,
        provider_id: &ProviderId,
    ) -> Result<Option<BmsBandwidthResult>> {
        Ok(sqlx::query_as!(
            BmsBandwidthResult,
            r#"SELECT
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
               FROM
                    bms_bandwidth_results
               WHERE
                    provider_id = $1
               ORDER BY
                    created_at DESC
               LIMIT 1
            "#,
            provider_id.as_str()
        )
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn get_latest_completed_for_provider(
        &self,
        provider_id: &ProviderId,
    ) -> Result<Option<BmsBandwidthResult>> {
        Ok(sqlx::query_as!(
            BmsBandwidthResult,
            r#"SELECT
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
               FROM
                    bms_bandwidth_results
               WHERE
                    provider_id = $1
                    AND status != 'Pending'
               ORDER BY
                    completed_at DESC NULLS LAST
               LIMIT 1
            "#,
            provider_id.as_str()
        )
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn get_latest_completed_for_providers(
        &self,
        provider_ids: &[String],
    ) -> Result<Vec<BmsBandwidthResult>> {
        if provider_ids.is_empty() {
            return Ok(vec![]);
        }

        Ok(sqlx::query_as!(
            BmsBandwidthResult,
            r#"SELECT DISTINCT ON (provider_id)
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
               FROM
                    bms_bandwidth_results
               WHERE
                    provider_id = ANY($1)
                    AND status != 'Pending'
               ORDER BY
                    provider_id,
                    completed_at DESC NULLS LAST
            "#,
            provider_ids
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn get_history_for_provider(
        &self,
        provider_id: &ProviderId,
        limit: i64,
    ) -> Result<Vec<BmsBandwidthResult>> {
        Ok(sqlx::query_as!(
            BmsBandwidthResult,
            r#"SELECT
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
               FROM
                    bms_bandwidth_results
               WHERE
                    provider_id = $1
               ORDER BY
                    created_at DESC
               LIMIT $2
            "#,
            provider_id.as_str(),
            limit
        )
        .fetch_all(&self.pool)
        .await?)
    }
}
