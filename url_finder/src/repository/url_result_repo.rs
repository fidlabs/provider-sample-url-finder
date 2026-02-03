use chrono::{DateTime, NaiveDate, Utc};
use color_eyre::Result;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::services::url_discovery_service::UrlDiscoveryResult;
use crate::types::{ClientId, DiscoveryType, ErrorCode, ProviderId, ResultCode};

/// Filters for provider queries
#[derive(Debug, Clone, Default)]
pub struct ProviderFilters {
    /// Filter by last_working_url IS [NOT] NULL in storage_providers
    pub has_working_url: Option<bool>,
    /// Filter by is_consistent in storage_providers
    pub is_consistent: Option<bool>,
}

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
    pub is_consistent: Option<bool>,
    pub is_reliable: Option<bool>,
    pub url_metadata: Option<serde_json::Value>,
    pub sector_utilization_percent: Option<f64>,
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
            is_consistent: Some(result.is_consistent),
            is_reliable: Some(result.is_reliable),
            url_metadata: result.url_metadata,
            sector_utilization_percent: result.sector_utilization_percent,
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
pub struct HistoryRow {
    pub date: NaiveDate,
    pub retrievability_percent: f64,
    pub sector_utilization_percent: Option<f64>,
    pub is_consistent: Option<bool>,
    pub is_reliable: Option<bool>,
    pub working_url: Option<String>,
    pub result_code: ResultCode,
    pub error_code: Option<ErrorCode>,
    pub tested_at: DateTime<Utc>,
    pub url_metadata: Option<serde_json::Value>,
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
                    tested_at,
                    is_consistent,
                    is_reliable,
                    url_metadata,
                    sector_utilization_percent::float8 AS "sector_utilization_percent"
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
                    tested_at,
                    is_consistent,
                    is_reliable,
                    url_metadata,
                    sector_utilization_percent::float8 AS "sector_utilization_percent"
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
                    tested_at,
                    is_consistent,
                    is_reliable,
                    url_metadata,
                    sector_utilization_percent::float8 AS "sector_utilization_percent"
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

    pub async fn get_all_providers_paginated(
        &self,
        filters: &ProviderFilters,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<UrlResult>> {
        let results = sqlx::query_as!(
            UrlResult,
            r#"SELECT DISTINCT ON (ur.provider_id)
                    ur.id,
                    ur.provider_id AS "provider_id: ProviderId",
                    ur.client_id AS "client_id: ClientId",
                    ur.result_type AS "result_type: DiscoveryType",
                    ur.working_url,
                    ur.retrievability_percent::float8 AS "retrievability_percent!",
                    ur.result_code AS "result_code: ResultCode",
                    ur.error_code AS "error_code: ErrorCode",
                    ur.tested_at,
                    ur.is_consistent,
                    ur.is_reliable,
                    ur.url_metadata,
                    ur.sector_utilization_percent::float8 AS "sector_utilization_percent"
               FROM
                    url_results ur
               JOIN
                    storage_providers sp ON ur.provider_id = sp.provider_id
               WHERE
                    ur.result_type = 'Provider'
                    AND ($3::bool IS NULL OR (sp.last_working_url IS NOT NULL) = $3)
                    AND ($4::bool IS NULL OR sp.is_consistent = $4)
               ORDER BY
                    ur.provider_id,
                    ur.tested_at DESC
               LIMIT $1
               OFFSET $2
            "#,
            limit,
            offset,
            filters.has_working_url,
            filters.is_consistent
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(results)
    }

    pub async fn count_all_providers(&self, filters: &ProviderFilters) -> Result<i64> {
        let result = sqlx::query_scalar!(
            r#"SELECT
                    COUNT(DISTINCT ur.provider_id) AS "count!"
               FROM
                    url_results ur
               JOIN
                    storage_providers sp ON ur.provider_id = sp.provider_id
               WHERE
                    ur.result_type = 'Provider'
                    AND ($1::bool IS NULL OR (sp.last_working_url IS NOT NULL) = $1)
                    AND ($2::bool IS NULL OR sp.is_consistent = $2)
            "#,
            filters.has_working_url,
            filters.is_consistent
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(result)
    }

    pub async fn get_latest_for_providers(
        &self,
        provider_ids: &[String],
    ) -> Result<Vec<UrlResult>> {
        if provider_ids.is_empty() {
            return Ok(vec![]);
        }

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
                    tested_at,
                    is_consistent,
                    is_reliable,
                    url_metadata,
                    sector_utilization_percent::float8 AS "sector_utilization_percent"
               FROM
                    url_results
               WHERE
                    provider_id = ANY($1)
                    AND result_type = 'Provider'
               ORDER BY
                    provider_id,
                    tested_at DESC
            "#,
            provider_ids
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
        let mut is_consistents: Vec<Option<bool>> = Vec::with_capacity(len);
        let mut is_reliables: Vec<Option<bool>> = Vec::with_capacity(len);
        let mut url_metadatas: Vec<Option<serde_json::Value>> = Vec::with_capacity(len);
        let mut sector_utilization_percents: Vec<Option<f64>> = Vec::with_capacity(len);

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
            is_consistents.push(result.is_consistent);
            is_reliables.push(result.is_reliable);
            url_metadatas.push(result.url_metadata.clone());
            sector_utilization_percents.push(result.sector_utilization_percent);
        }

        let result = sqlx::query!(
            r#"INSERT INTO
                    url_results (id, provider_id, client_id, result_type, working_url, retrievability_percent, result_code, error_code, tested_at, is_consistent, is_reliable, url_metadata, sector_utilization_percent)
               SELECT
                    a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13
               FROM UNNEST(
                    $1::uuid[],
                    $2::text[],
                    $3::text[],
                    $4::discovery_type[],
                    $5::text[],
                    $6::double precision[],
                    $7::result_code[],
                    $8::error_code[],
                    $9::timestamptz[],
                    $10::bool[],
                    $11::bool[],
                    $12::jsonb[],
                    $13::double precision[]
               ) AS t(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13)
            "#,
            &ids as &[Uuid],
            &provider_ids as &[String],
            &client_ids as &[Option<String>],
            &result_types as &[DiscoveryType],
            &working_urls as &[Option<String>],
            &retrievability_percents as &[f64],
            &result_codes as &[ResultCode],
            &error_codes as &[Option<ErrorCode>],
            &tested_ats as &[DateTime<Utc>],
            &is_consistents as &[Option<bool>],
            &is_reliables as &[Option<bool>],
            &url_metadatas as &[Option<serde_json::Value>],
            &sector_utilization_percents as &[Option<f64>]
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected().try_into()?)
    }

    pub async fn get_history_for_provider(
        &self,
        provider_id: &ProviderId,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Vec<HistoryRow>> {
        let results = sqlx::query_as!(
            HistoryRow,
            r#"SELECT DISTINCT ON (DATE(tested_at))
                    DATE(tested_at) AS "date!",
                    retrievability_percent::float8 AS "retrievability_percent!",
                    sector_utilization_percent::float8 AS "sector_utilization_percent",
                    is_consistent,
                    is_reliable,
                    working_url,
                    result_code AS "result_code: ResultCode",
                    error_code AS "error_code: ErrorCode",
                    tested_at,
                    url_metadata
               FROM
                    url_results
               WHERE
                    provider_id = $1
                    AND result_type = 'Provider'
                    AND tested_at >= $2::date
                    AND tested_at < ($3::date + INTERVAL '1 day')
               ORDER BY
                    DATE(tested_at),
                    tested_at DESC
            "#,
            provider_id.as_str(),
            from,
            to
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(results)
    }

    pub async fn get_history_for_provider_client(
        &self,
        provider_id: &ProviderId,
        client_id: &ClientId,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Vec<HistoryRow>> {
        let results = sqlx::query_as!(
            HistoryRow,
            r#"SELECT DISTINCT ON (DATE(combined.tested_at))
                    DATE(combined.tested_at) AS "date!",
                    combined.retrievability_percent::float8 AS "retrievability_percent!",
                    combined.sector_utilization_percent::float8 AS "sector_utilization_percent",
                    combined.is_consistent,
                    combined.is_reliable,
                    combined.working_url,
                    combined.result_code AS "result_code!: ResultCode",
                    combined.error_code AS "error_code: ErrorCode",
                    combined.tested_at AS "tested_at!",
                    combined.url_metadata
               FROM (
                    SELECT *, 1 AS priority
                    FROM url_results
                    WHERE provider_id = $1
                      AND client_id = $2
                      AND result_type = 'ProviderClient'
                      AND tested_at >= $3::date
                      AND tested_at < ($4::date + INTERVAL '1 day')
                    UNION ALL
                    SELECT *, 2 AS priority
                    FROM url_results
                    WHERE provider_id = $1
                      AND result_type = 'Provider'
                      AND working_url IS NULL
                      AND tested_at >= $3::date
                      AND tested_at < ($4::date + INTERVAL '1 day')
               ) combined
               ORDER BY
                    DATE(combined.tested_at),
                    combined.priority ASC,
                    combined.tested_at DESC
            "#,
            provider_id.as_str(),
            client_id.as_str(),
            from,
            to
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(results)
    }
}
