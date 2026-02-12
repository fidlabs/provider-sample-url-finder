use chrono::{DateTime, Utc};
use color_eyre::Result;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::types::ProviderId;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, sqlx::FromRow)]
pub struct StorageProvider {
    pub id: Uuid,
    pub provider_id: ProviderId,
    pub peer_id: Option<String>,
    pub next_url_discovery_at: DateTime<Utc>,
    pub url_discovery_status: Option<String>,
    pub url_discovery_pending_since: Option<DateTime<Utc>>,
    pub last_working_url: Option<String>,
    pub next_bms_test_at: DateTime<Utc>,
    pub bms_test_status: Option<String>,
    pub bms_routing_key: Option<String>,
    pub last_bms_region_discovery_at: Option<DateTime<Utc>>,
    pub is_consistent: Option<bool>,
    pub is_reliable: Option<bool>,
    pub url_metadata: Option<serde_json::Value>,
    pub cached_http_endpoints: Option<Vec<String>>,
    pub endpoints_fetched_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct StorageProviderRepository {
    pool: PgPool,
}

impl StorageProviderRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn insert_batch_if_not_exists(&self, provider_ids: &[ProviderId]) -> Result<usize> {
        if provider_ids.is_empty() {
            return Ok(0);
        }

        let provider_ids_str: Vec<String> = provider_ids
            .iter()
            .map(|id| id.as_str().to_string())
            .collect();

        let result = sqlx::query!(
            r#"INSERT INTO
                    storage_providers (provider_id)
               SELECT
                    UNNEST($1::text[])
               ON CONFLICT DO NOTHING
            "#,
            &provider_ids_str
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() as usize)
    }

    pub async fn get_by_provider_id(
        &self,
        provider_id: &ProviderId,
    ) -> Result<Option<StorageProvider>> {
        Ok(sqlx::query_as!(
            StorageProvider,
            r#"SELECT
                    id,
                    provider_id AS "provider_id: ProviderId",
                    peer_id,
                    next_url_discovery_at,
                    url_discovery_status,
                    url_discovery_pending_since,
                    last_working_url,
                    next_bms_test_at,
                    bms_test_status,
                    bms_routing_key,
                    last_bms_region_discovery_at,
                    is_consistent,
                    is_reliable,
                    url_metadata,
                    cached_http_endpoints,
                    endpoints_fetched_at,
                    created_at,
                    updated_at
               FROM
                    storage_providers
               WHERE
                    provider_id = $1
            "#,
            provider_id as &ProviderId
        )
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn get_due_for_url_discovery(&self, limit: i64) -> Result<Vec<StorageProvider>> {
        Ok(sqlx::query_as!(
            StorageProvider,
            r#"SELECT
                    id,
                    provider_id AS "provider_id: ProviderId",
                    peer_id,
                    next_url_discovery_at,
                    url_discovery_status,
                    url_discovery_pending_since,
                    last_working_url,
                    next_bms_test_at,
                    bms_test_status,
                    bms_routing_key,
                    last_bms_region_discovery_at,
                    is_consistent,
                    is_reliable,
                    url_metadata,
                    cached_http_endpoints,
                    endpoints_fetched_at,
                    created_at,
                    updated_at
               FROM
                    storage_providers
               WHERE
                    cached_http_endpoints IS NOT NULL
                    AND (
                        (
                            next_url_discovery_at <= NOW()
                            AND url_discovery_status IS DISTINCT FROM 'pending'
                        )
                        OR
                        (
                            url_discovery_status = 'pending'
                            AND (
                                url_discovery_pending_since IS NULL
                                OR url_discovery_pending_since < NOW() - INTERVAL '60 minutes'
                            )
                        )
                    )
               ORDER BY
                    next_url_discovery_at ASC
               LIMIT $1
            "#,
            limit
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn set_url_discovery_pending(&self, provider_id: &ProviderId) -> Result<()> {
        sqlx::query!(
            r#"UPDATE
                    storage_providers
               SET
                    url_discovery_status = 'pending',
                    url_discovery_pending_since = NOW()
               WHERE
                    provider_id = $1
            "#,
            provider_id as &ProviderId
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn reset_url_discovery_pending(&self, provider_id: &ProviderId) -> Result<()> {
        sqlx::query!(
            r#"UPDATE
                    storage_providers
               SET
                    url_discovery_status = NULL,
                    url_discovery_pending_since = NULL
               WHERE
                    provider_id = $1
            "#,
            provider_id as &ProviderId,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_after_url_discovery(
        &self,
        provider_id: &ProviderId,
        last_working_url: Option<String>,
        is_consistent: Option<bool>,
        is_reliable: Option<bool>,
        url_metadata: Option<serde_json::Value>,
    ) -> Result<()> {
        sqlx::query!(
            r#"UPDATE
                    storage_providers
               SET
                    next_url_discovery_at = DATE_TRUNC('day', NOW()) + INTERVAL '1 day',
                    url_discovery_status = NULL,
                    url_discovery_pending_since = NULL,
                    last_working_url = $2,
                    is_consistent = $3,
                    is_reliable = $4,
                    url_metadata = $5,
                    updated_at = NOW()
               WHERE
                    provider_id = $1
            "#,
            provider_id as &ProviderId,
            last_working_url,
            is_consistent,
            is_reliable,
            url_metadata
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn clear_pending_and_reschedule(&self, provider_id: &ProviderId) -> Result<()> {
        sqlx::query!(
            r#"UPDATE
                    storage_providers
               SET
                    url_discovery_status = NULL,
                    url_discovery_pending_since = NULL,
                    next_url_discovery_at = NOW(),
                    updated_at = NOW()
               WHERE
                    provider_id = $1
            "#,
            provider_id as &ProviderId
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn reschedule_url_discovery_delayed(&self, provider_id: &ProviderId) -> Result<()> {
        sqlx::query!(
            r#"UPDATE
                    storage_providers
               SET
                    next_url_discovery_at = DATE_TRUNC('day', NOW()) + INTERVAL '1 day',
                    updated_at = NOW()
               WHERE
                    provider_id = $1
            "#,
            provider_id as &ProviderId,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_due_for_bms_test(&self, limit: i64) -> Result<Vec<StorageProvider>> {
        Ok(sqlx::query_as!(
            StorageProvider,
            r#"SELECT
                    id,
                    provider_id AS "provider_id: ProviderId",
                    peer_id,
                    next_url_discovery_at,
                    url_discovery_status,
                    url_discovery_pending_since,
                    last_working_url,
                    next_bms_test_at,
                    bms_test_status,
                    bms_routing_key,
                    last_bms_region_discovery_at,
                    is_consistent,
                    is_reliable,
                    url_metadata,
                    cached_http_endpoints,
                    endpoints_fetched_at,
                    created_at,
                    updated_at
               FROM
                    storage_providers
               WHERE
                    last_working_url IS NOT NULL
                    AND is_consistent = true
                    AND next_bms_test_at <= NOW()
               ORDER BY
                    next_bms_test_at ASC
               LIMIT $1
            "#,
            limit
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn schedule_next_bms_test(
        &self,
        provider_id: &ProviderId,
        interval_days: i64,
    ) -> Result<()> {
        sqlx::query!(
            r#"UPDATE
                    storage_providers
               SET
                    next_bms_test_at = NOW() + ($2 || ' days')::INTERVAL,
                    updated_at = NOW()
               WHERE
                    provider_id = $1
            "#,
            provider_id as &ProviderId,
            interval_days.to_string()
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn reset_url_discovery_schedule(
        &self,
        provider_id: &ProviderId,
    ) -> Result<Option<StorageProvider>> {
        Ok(sqlx::query_as!(
            StorageProvider,
            r#"UPDATE
                    storage_providers
               SET
                    next_url_discovery_at = NOW(),
                    updated_at = NOW()
               WHERE
                    provider_id = $1
               RETURNING
                    id,
                    provider_id AS "provider_id: ProviderId",
                    peer_id,
                    next_url_discovery_at,
                    url_discovery_status,
                    url_discovery_pending_since,
                    last_working_url,
                    next_bms_test_at,
                    bms_test_status,
                    bms_routing_key,
                    last_bms_region_discovery_at,
                    is_consistent,
                    is_reliable,
                    url_metadata,
                    cached_http_endpoints,
                    endpoints_fetched_at,
                    created_at,
                    updated_at
            "#,
            provider_id as &ProviderId
        )
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn reset_bms_test_schedule(
        &self,
        provider_id: &ProviderId,
    ) -> Result<Option<StorageProvider>> {
        Ok(sqlx::query_as!(
            StorageProvider,
            r#"UPDATE
                    storage_providers
               SET
                    next_bms_test_at = NOW(),
                    updated_at = NOW()
               WHERE
                    provider_id = $1
               RETURNING
                    id,
                    provider_id AS "provider_id: ProviderId",
                    peer_id,
                    next_url_discovery_at,
                    url_discovery_status,
                    url_discovery_pending_since,
                    last_working_url,
                    next_bms_test_at,
                    bms_test_status,
                    bms_routing_key,
                    last_bms_region_discovery_at,
                    is_consistent,
                    is_reliable,
                    url_metadata,
                    cached_http_endpoints,
                    endpoints_fetched_at,
                    created_at,
                    updated_at
            "#,
            provider_id as &ProviderId
        )
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn reset_all_schedules(
        &self,
        provider_id: &ProviderId,
    ) -> Result<Option<StorageProvider>> {
        Ok(sqlx::query_as!(
            StorageProvider,
            r#"UPDATE
                    storage_providers
               SET
                    next_url_discovery_at = NOW(),
                    next_bms_test_at = NOW(),
                    updated_at = NOW()
               WHERE
                    provider_id = $1
               RETURNING
                    id,
                    provider_id AS "provider_id: ProviderId",
                    peer_id,
                    next_url_discovery_at,
                    url_discovery_status,
                    url_discovery_pending_since,
                    last_working_url,
                    next_bms_test_at,
                    bms_test_status,
                    bms_routing_key,
                    last_bms_region_discovery_at,
                    is_consistent,
                    is_reliable,
                    url_metadata,
                    cached_http_endpoints,
                    endpoints_fetched_at,
                    created_at,
                    updated_at
            "#,
            provider_id as &ProviderId
        )
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn get_providers_needing_endpoints(
        &self,
        limit: i64,
    ) -> Result<Vec<StorageProvider>> {
        Ok(sqlx::query_as!(
            StorageProvider,
            r#"SELECT
                    id,
                    provider_id AS "provider_id: ProviderId",
                    peer_id,
                    next_url_discovery_at,
                    url_discovery_status,
                    url_discovery_pending_since,
                    last_working_url,
                    next_bms_test_at,
                    bms_test_status,
                    bms_routing_key,
                    last_bms_region_discovery_at,
                    is_consistent,
                    is_reliable,
                    url_metadata,
                    cached_http_endpoints,
                    endpoints_fetched_at,
                    created_at,
                    updated_at
               FROM
                    storage_providers
               WHERE
                    endpoints_fetched_at IS NULL
                    OR endpoints_fetched_at < DATE_TRUNC('day', NOW())
               ORDER BY
                    endpoints_fetched_at ASC NULLS FIRST
               LIMIT $1
            "#,
            limit
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn update_cached_endpoints(
        &self,
        provider_id: &ProviderId,
        peer_id: &str,
        http_endpoints: &[String],
    ) -> Result<()> {
        sqlx::query!(
            r#"UPDATE
                    storage_providers
               SET
                    peer_id = $2,
                    cached_http_endpoints = $3,
                    endpoints_fetched_at = NOW(),
                    next_url_discovery_at = NOW(),
                    updated_at = NOW()
               WHERE
                    provider_id = $1
            "#,
            provider_id as &ProviderId,
            peer_id,
            http_endpoints
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_endpoint_fetch_failed(&self, provider_id: &ProviderId) -> Result<()> {
        sqlx::query!(
            r#"UPDATE
                    storage_providers
               SET
                    endpoints_fetched_at = NOW(),
                    cached_http_endpoints = NULL,
                    next_url_discovery_at = DATE_TRUNC('day', NOW()) + INTERVAL '1 day',
                    updated_at = NOW()
               WHERE
                    provider_id = $1
            "#,
            provider_id as &ProviderId
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
