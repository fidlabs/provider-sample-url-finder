use chrono::{DateTime, Utc};
use color_eyre::Result;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::types::{ClientId, DiscoveryType, ErrorCode, ProviderId, ResultCode};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, sqlx::FromRow)]
pub struct UrlResult {
    pub id: Uuid,
    pub provider_id: ProviderId,
    pub client_id: Option<ClientId>,
    pub result_type: DiscoveryType,
    pub working_url: Option<String>,
    pub retrievability_percent: f64,
    pub result_code: ResultCode,
    pub error_code: Option<ErrorCode>,
    pub tested_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct UrlResultRepository {
    pool: PgPool,
}

impl UrlResultRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn insert_batch(&self, results: &[UrlResult]) -> Result<usize> {
        if results.is_empty() {
            return Ok(0);
        }

        let ids: Vec<Uuid> = results.iter().map(|r| r.id).collect();
        let provider_ids: Vec<String> = results
            .iter()
            .map(|r| r.provider_id.as_str().to_string())
            .collect();
        let client_ids: Vec<Option<String>> = results
            .iter()
            .map(|r| r.client_id.as_ref().map(|c| c.as_str().to_string()))
            .collect();
        let result_types: Vec<DiscoveryType> =
            results.iter().map(|r| r.result_type.clone()).collect();
        let working_urls: Vec<Option<String>> =
            results.iter().map(|r| r.working_url.clone()).collect();
        let retrievability_percents: Vec<f64> =
            results.iter().map(|r| r.retrievability_percent).collect();
        let result_codes: Vec<ResultCode> = results.iter().map(|r| r.result_code.clone()).collect();
        let error_codes: Vec<Option<ErrorCode>> =
            results.iter().map(|r| r.error_code.clone()).collect();
        let tested_ats: Vec<DateTime<Utc>> = results.iter().map(|r| r.tested_at).collect();

        let result = sqlx::query!(
            r#"INSERT INTO
                    url_results (id, provider_id, client_id, result_type, working_url, retrievability_percent, result_code, error_code, tested_at)
               SELECT
                    UNNEST($1::uuid[]),
                    UNNEST($2::text[]),
                    UNNEST($3::text[]),
                    UNNEST($4::discovery_type[]),
                    UNNEST($5::text[]),
                    UNNEST($6::double precision[]),
                    UNNEST($7::result_code[]),
                    UNNEST($8::error_code[]),
                    UNNEST($9::timestamptz[])
            "#,
            &ids as &[Uuid],
            &provider_ids as &[String],
            &client_ids as &[Option<String>],
            &result_types as &[DiscoveryType],
            &working_urls as &[Option<String>],
            &retrievability_percents as &[f64],
            &result_codes as &[ResultCode],
            &error_codes as &[Option<ErrorCode>],
            &tested_ats as &[DateTime<Utc>]
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() as usize)
    }
}
