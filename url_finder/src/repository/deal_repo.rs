use color_eyre::Result;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::types::{ClientId, ProviderId};

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
        provider_id: &ProviderId,
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
            provider_id.as_str(),
            limit,
            offset,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    pub async fn get_deals_by_provider_and_client(
        &self,
        provider_id: &ProviderId,
        client_id: &ClientId,
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
            provider_id.as_str(),
            client_id.as_str(),
            limit,
            offset,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    pub async fn get_random_deals_by_provider_and_client(
        &self,
        provider_id: &ProviderId,
        client_id: &ClientId,
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
            provider_id.as_str(),
            client_id.as_str(),
            limit,
            offset,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    pub async fn get_random_deals_by_provider(
        &self,
        provider_id: &ProviderId,
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
            provider_id.as_str(),
            limit,
            offset,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    pub async fn get_distinct_providers_by_client(
        &self,
        client_id: &ClientId,
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
            client_id.as_str(),
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    pub async fn get_distinct_providers(&self) -> Result<Vec<ProviderId>, sqlx::Error> {
        let providers: Vec<String> = sqlx::query_scalar!(
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

        Ok(providers
            .into_iter()
            .filter_map(|s| ProviderId::new(s).ok())
            .collect())
    }

    pub async fn get_clients_for_provider(
        &self,
        provider_id: &ProviderId,
    ) -> Result<Vec<ClientId>, sqlx::Error> {
        let clients = sqlx::query_scalar!(
            r#"SELECT DISTINCT
                    "clientId"
               FROM
                    unified_verified_deal
               WHERE
                    "providerId" = $1
                    AND "clientId" IS NOT NULL
            "#,
            provider_id.as_str()
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(clients
            .into_iter()
            .flatten()
            .filter_map(|s| ClientId::new(s).ok())
            .collect())
    }
}
