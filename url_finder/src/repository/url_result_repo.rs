use chrono::{DateTime, Utc};
use color_eyre::Result;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::services::url_discovery_service::UrlDiscoveryResult;
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

impl From<UrlDiscoveryResult> for UrlResult {
    fn from(result: UrlDiscoveryResult) -> Self {
        Self {
            id: result.id,
            provider_id: result.provider_id,
            client_id: result.client_id,
            result_type: result.result_type,
            working_url: result.working_url,
            retrievability_percent: result.retrievability_percent,
            result_code: result.result_code,
            error_code: result.error_code,
            tested_at: result.tested_at,
        }
    }
}

#[derive(Clone)]
pub struct UrlResultRepository {
    pool: PgPool,
}

impl UrlResultRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn get_latest_for_provider(
        &self,
        provider_id: &ProviderId,
    ) -> Result<Option<UrlResult>> {
        let result = sqlx::query_as!(
            UrlResult,
            r#"SELECT
                    id,
                    provider_id AS "provider_id: ProviderId",
                    client_id AS "client_id: ClientId",
                    result_type AS "result_type: DiscoveryType",
                    working_url,
                    retrievability_percent::float8 AS "retrievability_percent!",
                    result_code AS "result_code: ResultCode",
                    error_code AS "error_code: ErrorCode",
                    tested_at
               FROM
                    url_results
               WHERE
                    provider_id = $1
                    AND result_type = 'Provider'
               ORDER BY
                    tested_at DESC
               LIMIT 1
            "#,
            provider_id.as_str()
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(result)
    }

    pub async fn get_latest_for_provider_client(
        &self,
        provider_id: &ProviderId,
        client_id: &ClientId,
    ) -> Result<Option<UrlResult>> {
        let result = sqlx::query_as!(
            UrlResult,
            r#"SELECT
                    id,
                    provider_id AS "provider_id: ProviderId",
                    client_id AS "client_id: ClientId",
                    result_type AS "result_type: DiscoveryType",
                    working_url,
                    retrievability_percent::float8 AS "retrievability_percent!",
                    result_code AS "result_code: ResultCode",
                    error_code AS "error_code: ErrorCode",
                    tested_at
               FROM
                    url_results
               WHERE
                    provider_id = $1
                    AND client_id = $2
                    AND result_type = 'ProviderClient'
               ORDER BY
                    tested_at DESC
               LIMIT 1
            "#,
            provider_id.as_str(),
            client_id.as_str()
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(result)
    }

    pub async fn get_latest_for_client_all_providers(
        &self,
        client_id: &ClientId,
    ) -> Result<Vec<UrlResult>> {
        let results = sqlx::query_as!(
            UrlResult,
            r#"SELECT DISTINCT ON (provider_id)
                    id,
                    provider_id AS "provider_id: ProviderId",
                    client_id AS "client_id: ClientId",
                    result_type AS "result_type: DiscoveryType",
                    working_url,
                    retrievability_percent::float8 AS "retrievability_percent!",
                    result_code AS "result_code: ResultCode",
                    error_code AS "error_code: ErrorCode",
                    tested_at
               FROM
                    url_results
               WHERE
                    client_id = $1
                    AND result_type = 'ProviderClient'
               ORDER BY
                    provider_id,
                    tested_at DESC
            "#,
            client_id.as_str()
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(results)
    }

    pub async fn insert_batch(&self, results: &[UrlResult]) -> Result<usize> {
        if results.is_empty() {
            return Ok(0);
        }

        let len = results.len();
        let mut ids: Vec<Uuid> = Vec::with_capacity(len);
        let mut provider_ids: Vec<String> = Vec::with_capacity(len);
        let mut client_ids: Vec<Option<String>> = Vec::with_capacity(len);
        let mut result_types: Vec<DiscoveryType> = Vec::with_capacity(len);
        let mut working_urls: Vec<Option<String>> = Vec::with_capacity(len);
        let mut retrievability_percents: Vec<f64> = Vec::with_capacity(len);
        let mut result_codes: Vec<ResultCode> = Vec::with_capacity(len);
        let mut error_codes: Vec<Option<ErrorCode>> = Vec::with_capacity(len);
        let mut tested_ats: Vec<DateTime<Utc>> = Vec::with_capacity(len);

        for result in results {
            ids.push(result.id);
            provider_ids.push(result.provider_id.as_str().to_string());
            client_ids.push(result.client_id.as_ref().map(|c| c.as_str().to_string()));
            result_types.push(result.result_type.clone());
            working_urls.push(result.working_url.clone());
            retrievability_percents.push(result.retrievability_percent);
            result_codes.push(result.result_code.clone());
            error_codes.push(result.error_code.clone());
            tested_ats.push(result.tested_at);
        }

        let result = sqlx::query!(
            r#"INSERT INTO
                    url_results (id, provider_id, client_id, result_type, working_url, retrievability_percent, result_code, error_code, tested_at)
               SELECT
                    a1, a2, a3, a4, a5, a6, a7, a8, a9
               FROM UNNEST(
                    $1::uuid[],
                    $2::text[],
                    $3::text[],
                    $4::discovery_type[],
                    $5::text[],
                    $6::double precision[],
                    $7::result_code[],
                    $8::error_code[],
                    $9::timestamptz[]
               ) AS t(a1, a2, a3, a4, a5, a6, a7, a8, a9)
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

        Ok(result.rows_affected().try_into()?)
    }
}
