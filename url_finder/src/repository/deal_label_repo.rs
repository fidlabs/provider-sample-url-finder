use color_eyre::Result;
use sqlx::PgPool;
use tracing::debug;

#[derive(Clone)]
pub struct DealLabelRepository {
    pool: PgPool,
}

#[derive(Debug, Clone)]
pub struct DealLabel {
    pub deal_id: i32,
    pub piece_cid: String,
    pub label_raw: Option<String>,
    pub payload_cid: Option<String>,
}

impl DealLabelRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Get cached label for a deal
    pub async fn get_by_deal_id(&self, deal_id: i32) -> Result<Option<DealLabel>, sqlx::Error> {
        let label = sqlx::query_as!(
            DealLabel,
            r#"
            SELECT
                deal_id,
                piece_cid,
                label_raw,
                payload_cid
            FROM
                deal_labels
            WHERE
                deal_id = $1
            "#,
            deal_id
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(label)
    }

    /// Get cached labels for multiple deals (batch lookup)
    pub async fn get_by_deal_ids(&self, deal_ids: &[i32]) -> Result<Vec<DealLabel>, sqlx::Error> {
        if deal_ids.is_empty() {
            return Ok(vec![]);
        }

        let labels = sqlx::query_as!(
            DealLabel,
            r#"
            SELECT
                deal_id,
                piece_cid,
                label_raw,
                payload_cid
            FROM
                deal_labels
            WHERE
                deal_id = ANY($1)
            "#,
            deal_ids
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(labels)
    }

    /// Insert or update a deal label in the cache
    pub async fn upsert(&self, label: &DealLabel) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            INSERT INTO deal_labels (deal_id, piece_cid, label_raw, payload_cid)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (deal_id) DO UPDATE SET
                piece_cid = EXCLUDED.piece_cid,
                label_raw = EXCLUDED.label_raw,
                payload_cid = EXCLUDED.payload_cid,
                fetched_at = NOW()
            "#,
            label.deal_id,
            label.piece_cid,
            label.label_raw,
            label.payload_cid
        )
        .execute(&self.pool)
        .await?;

        debug!("Cached label for deal_id={}", label.deal_id);
        Ok(())
    }
}

/// Parse label string to extract payload CID if valid format
pub fn parse_payload_cid(label: &str) -> Option<String> {
    let label = label.trim();
    if label.starts_with("bafy")
        || label.starts_with("bafk")
        || label.starts_with("bafyb")
        || label.starts_with("Qm")
    {
        Some(label.to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_payload_cid_valid() {
        assert!(parse_payload_cid("bafybeif123").is_some());
        assert!(parse_payload_cid("bafkreif123").is_some());
        assert!(parse_payload_cid("QmYwAPJzv5CZsnA").is_some());
    }

    #[test]
    fn test_parse_payload_cid_invalid() {
        assert!(parse_payload_cid("").is_none());
        assert!(parse_payload_cid("not-a-cid").is_none());
        assert!(parse_payload_cid("12345").is_none());
    }

    #[test]
    fn test_parse_payload_cid_with_whitespace() {
        assert!(parse_payload_cid("  bafybeif123  ").is_some());
    }
}
