mod bms_result_repo;
mod deal_repo;
mod deal_sli_repo;
mod storage_provider_repo;
mod url_result_repo;

use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Clone, Copy)]
pub struct HistoryCursorBoundary {
    pub at: DateTime<Utc>,
    pub id: Option<Uuid>,
    pub include_equal_at: bool,
}

impl HistoryCursorBoundary {
    pub fn within_source(at: DateTime<Utc>, id: Uuid) -> Self {
        Self {
            at,
            id: Some(id),
            include_equal_at: false,
        }
    }

    pub fn before_source(at: DateTime<Utc>) -> Self {
        Self {
            at,
            id: None,
            include_equal_at: false,
        }
    }

    pub fn after_source(at: DateTime<Utc>) -> Self {
        Self {
            at,
            id: None,
            include_equal_at: true,
        }
    }
}

pub use bms_result_repo::*;
pub use deal_repo::*;
pub use deal_sli_repo::*;
pub use storage_provider_repo::*;
pub use url_result_repo::*;
