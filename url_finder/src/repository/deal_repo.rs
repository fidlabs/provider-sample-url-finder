use color_eyre::Result;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Clone)]
pub struct DealRepository {
    pool: PgPool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UnifiedVerifiedDeal {
    pub id: i32,
    pub deal_id: i32,
    pub claim_id: i32,
    pub client_id: Option<String>,
    pub provider_id: Option<String>,
    pub piece_cid: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Provider {
    pub provider_id: Option<String>,
}

impl DealRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn get_deals_by_provider(
        &self,
        provider: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<UnifiedVerifiedDeal>, sqlx::Error> {
        let data = sqlx::query_as!(
            UnifiedVerifiedDeal,
            r#"
            SELECT
                id,
                "dealId" AS deal_id,
                "claimId" AS claim_id,
                "clientId" AS client_id,
                "providerId" AS provider_id,
                "pieceCid" AS piece_cid
            FROM unified_verified_deal
            WHERE 
                "providerId" = $1
            ORDER BY random()
            LIMIT $2
            OFFSET $3
            "#,
            provider,
            limit,
            offset,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    pub async fn get_deals_by_provider_and_client(
        &self,
        provider: &str,
        client: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<UnifiedVerifiedDeal>, sqlx::Error> {
        let data = sqlx::query_as!(
            UnifiedVerifiedDeal,
            r#"
            SELECT
                id,
                "dealId" AS deal_id,
                "claimId" AS claim_id,
                "clientId" AS client_id,
                "providerId" AS provider_id,
                "pieceCid" AS piece_cid
            FROM unified_verified_deal
            WHERE 
                "providerId" = $1
                AND "clientId" = $2
            ORDER BY random()
            LIMIT $3
            OFFSET $4
            "#,
            provider,
            client,
            limit,
            offset,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    pub async fn get_random_deals_by_provider_and_client(
        &self,
        provider: &str,
        client: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<UnifiedVerifiedDeal>, sqlx::Error> {
        let data = sqlx::query_as!(
            UnifiedVerifiedDeal,
            r#"
            SELECT
                id,
                "dealId" AS deal_id,
                "claimId" AS claim_id,
                "clientId" AS client_id,
                "providerId" AS provider_id,
                "pieceCid" AS piece_cid
            FROM unified_verified_deal
            WHERE 
                "providerId" = $1
                AND "clientId" = $2
            ORDER BY random()
            LIMIT $3
            OFFSET $4
            "#,
            provider,
            client,
            limit,
            offset,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    pub async fn get_random_deals_by_provider(
        &self,
        provider: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<UnifiedVerifiedDeal>, sqlx::Error> {
        let data = sqlx::query_as!(
            UnifiedVerifiedDeal,
            r#"
            SELECT
                id,
                "dealId" AS deal_id,
                "claimId" AS claim_id,
                "clientId" AS client_id,
                "providerId" AS provider_id,
                "pieceCid" AS piece_cid
            FROM unified_verified_deal
            WHERE 
                "providerId" = $1
            ORDER BY random()
            LIMIT $2
            OFFSET $3
            "#,
            provider,
            limit,
            offset,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    pub async fn get_distinct_providers_by_client(
        &self,
        client: &str,
    ) -> Result<Vec<Provider>, sqlx::Error> {
        let data = sqlx::query_as!(
            Provider,
            r#"
            SELECT DISTINCT
                "providerId" AS provider_id
            FROM
                unified_verified_deal
            WHERE
                "clientId" = $1
            "#,
            client,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    pub async fn get_distinct_providers(&self) -> Result<Vec<String>, sqlx::Error> {
        let providers = sqlx::query_scalar!(
            r#"SELECT DISTINCT
                    "providerId"
               FROM
                    unified_verified_deal
                WHERE
                    "providerId" IS NOT NULL
            "#
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .flatten()
        .collect();

        Ok(providers)
    }
}
