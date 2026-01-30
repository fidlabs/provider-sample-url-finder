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
    pub peer_id_fetched_at: Option<DateTime<Utc>>,
    pub next_url_discovery_at: DateTime<Utc>,
    pub url_discovery_status: Option<String>,
    pub url_discovery_pending_since: Option<DateTime<Utc>>,
    pub last_working_url: Option<String>,
    pub next_bms_test_at: DateTime<Utc>,
    pub bms_test_status: Option<String>,
    pub bms_routing_key: Option<String>,
    pub last_bms_region_discovery_at: Option<DateTime<Utc>>,
    pub is_consistent: bool,
    pub is_reliable: bool,
    pub url_metadata: Option<serde_json::Value>,
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
                    peer_id_fetched_at,
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
                    peer_id_fetched_at,
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
                    created_at,
                    updated_at
               FROM
                    storage_providers
               WHERE
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

    pub async fn update_after_url_discovery(
        &self,
        provider_id: &ProviderId,
        last_working_url: Option<String>,
        is_consistent: bool,
        is_reliable: bool,
        url_metadata: Option<serde_json::Value>,
    ) -> Result<()> {
        sqlx::query!(
            r#"UPDATE
                    storage_providers
               SET
                    next_url_discovery_at = NOW() + INTERVAL '1 day',
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

    pub async fn reschedule_url_discovery_delayed(
        &self,
        provider_id: &ProviderId,
        delay_seconds: i64,
    ) -> Result<()> {
        sqlx::query!(
            r#"UPDATE
                    storage_providers
               SET
                    next_url_discovery_at = NOW() + INTERVAL '1 second' * $2,
                    updated_at = NOW()
               WHERE
                    provider_id = $1
            "#,
            provider_id as &ProviderId,
            delay_seconds as f64
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
                    peer_id_fetched_at,
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
                    peer_id_fetched_at,
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
                    peer_id_fetched_at,
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
                    peer_id_fetched_at,
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
                    created_at,
                    updated_at
            "#,
            provider_id as &ProviderId
        )
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn get_providers_without_peer_id(&self, limit: i64) -> Result<Vec<StorageProvider>> {
        Ok(sqlx::query_as!(
            StorageProvider,
            r#"SELECT
                    id,
                    provider_id AS "provider_id: ProviderId",
                    peer_id,
                    peer_id_fetched_at,
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
                    created_at,
                    updated_at
               FROM
                    storage_providers
               WHERE
                    peer_id IS NULL
               ORDER BY
                    created_at ASC
               LIMIT $1
            "#,
            limit
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn get_providers_with_stale_peer_id(
        &self,
        limit: i64,
        stale_days: i64,
    ) -> Result<Vec<StorageProvider>> {
        Ok(sqlx::query_as!(
            StorageProvider,
            r#"SELECT
                    id,
                    provider_id AS "provider_id: ProviderId",
                    peer_id,
                    peer_id_fetched_at,
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
                    created_at,
                    updated_at
               FROM
                    storage_providers
               WHERE
                    peer_id IS NOT NULL
                    AND peer_id_fetched_at < NOW() - INTERVAL '1 day' * $2
               ORDER BY
                    peer_id_fetched_at ASC
               LIMIT $1
            "#,
            limit,
            stale_days as f64
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn update_peer_id(&self, provider_id: &ProviderId, peer_id: &str) -> Result<()> {
        sqlx::query!(
            r#"UPDATE
                    storage_providers
               SET
                    peer_id = $2,
                    peer_id_fetched_at = NOW(),
                    next_url_discovery_at = NOW(),
                    updated_at = NOW()
               WHERE
                    provider_id = $1
            "#,
            provider_id as &ProviderId,
            peer_id
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
