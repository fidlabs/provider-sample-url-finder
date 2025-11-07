use chrono::{DateTime, Utc};
use color_eyre::Result;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, sqlx::FromRow)]
pub struct StorageProvider {
    pub id: Uuid,
    pub provider_id: String,
    pub next_url_discovery_at: DateTime<Utc>,
    pub url_discovery_status: Option<String>,
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

    pub async fn insert_if_not_exists(&self, provider_id: &str) -> Result<()> {
        sqlx::query!(
            r#"INSERT INTO
                    storage_providers (provider_id)
               VALUES
                    ($1)
               ON CONFLICT DO NOTHING
            "#,
            provider_id
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_by_provider_id(&self, provider_id: &str) -> Result<Option<StorageProvider>> {
        Ok(sqlx::query_as!(
            StorageProvider,
            r#"SELECT 
                    id, 
                    provider_id, 
                    next_url_discovery_at, 
                    url_discovery_status, 
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
            provider_id
        )
        .fetch_optional(&self.pool)
        .await?)
    }
}
