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
    pub next_url_discovery_at: DateTime<Utc>,
    pub url_discovery_status: Option<String>,
    pub url_discovery_pending_since: Option<DateTime<Utc>>,
    pub last_working_url: Option<String>,
    pub next_bms_test_at: DateTime<Utc>,
    pub bms_test_status: Option<String>,
    pub bms_routing_key: Option<String>,
    pub last_bms_region_discovery_at: Option<DateTime<Utc>>,
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

    #[allow(dead_code)]
    pub async fn insert_if_not_exists(&self, provider_id: &ProviderId) -> Result<()> {
        sqlx::query!(
            r#"INSERT INTO
                    storage_providers (provider_id)
               VALUES
                    ($1)
               ON CONFLICT DO NOTHING
            "#,
            provider_id as &ProviderId
        )
        .execute(&self.pool)
        .await?;
        Ok(())
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
                    next_url_discovery_at,
                    url_discovery_status,
                    url_discovery_pending_since,
                    last_working_url,
                    next_bms_test_at,
                    bms_test_status,
                    bms_routing_key,
                    last_bms_region_discovery_at,
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
                    next_url_discovery_at,
                    url_discovery_status,
                    url_discovery_pending_since,
                    last_working_url,
                    next_bms_test_at,
                    bms_test_status,
                    bms_routing_key,
                    last_bms_region_discovery_at,
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
                        AND url_discovery_pending_since < NOW() - INTERVAL '60 minutes'
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
    ) -> Result<()> {
        sqlx::query!(
            r#"UPDATE
                    storage_providers
               SET
                    next_url_discovery_at = NOW() + INTERVAL '1 day',
                    url_discovery_status = NULL,
                    url_discovery_pending_since = NULL,
                    last_working_url = $2,
                    updated_at = NOW()
               WHERE
                    provider_id = $1
            "#,
            provider_id as &ProviderId,
            last_working_url
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
