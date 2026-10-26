use chrono::{DateTime, Days, NaiveDate, Utc};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    api_response::{
        ApiResponse, ErrorCode, bad_request_with_code, internal_server_error_with_code,
    },
    config::MAX_HISTORY_DAYS,
};

pub(crate) const DEFAULT_HISTORY_LIMIT: i64 = 100;
pub(crate) const MAX_HISTORY_LIMIT: i64 = 500;
pub(crate) const MAX_HISTORY_CURSOR_LENGTH: usize = 2048;

pub(crate) struct HistoryWindow {
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

pub(crate) fn history_window(
    from: Option<NaiveDate>,
    to: Option<NaiveDate>,
) -> Result<HistoryWindow, ApiResponse<()>> {
    let to = to.unwrap_or_else(|| Utc::now().date_naive());
    let from = match from {
        Some(from) => from,
        None => to
            .checked_sub_days(Days::new(MAX_HISTORY_DAYS as u64))
            .ok_or_else(|| invalid_date_boundary("Default history start date is out of range"))?,
    };

    if from > to {
        return Err(bad_request_with_code(
            ErrorCode::InvalidDateRange,
            "Parameter 'from' must be before or equal to 'to'",
        ));
    }

    let last_allowed = from.checked_add_days(Days::new(MAX_HISTORY_DAYS as u64));
    if last_allowed
        .map(|last_allowed| to > last_allowed)
        .unwrap_or(true)
    {
        return Err(bad_request_with_code(
            ErrorCode::DateRangeExceeded,
            format!("Date range exceeds maximum of {MAX_HISTORY_DAYS} days"),
        ));
    }

    let start = from
        .and_hms_opt(0, 0, 0)
        .map(|value| value.and_utc())
        .ok_or_else(|| invalid_date_boundary("History start date is out of range"))?;
    let end = to
        .succ_opt()
        .and_then(|value| value.and_hms_opt(0, 0, 0))
        .map(|value| value.and_utc())
        .ok_or_else(|| invalid_date_boundary("History end date is out of range"))?;

    Ok(HistoryWindow {
        from,
        to,
        start,
        end,
    })
}

pub(crate) fn history_limit(limit: Option<i64>) -> Result<i64, ApiResponse<()>> {
    let limit = limit.unwrap_or(DEFAULT_HISTORY_LIMIT);
    if !(1..=MAX_HISTORY_LIMIT).contains(&limit) {
        return Err(bad_request_with_code(
            ErrorCode::InvalidRequest,
            format!("Parameter 'limit' must be between 1 and {MAX_HISTORY_LIMIT}"),
        ));
    }
    Ok(limit)
}

pub(crate) fn decode_history_cursor<T: DeserializeOwned>(
    value: Option<&str>,
) -> Result<Option<T>, ApiResponse<()>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.len() > MAX_HISTORY_CURSOR_LENGTH {
        return Err(invalid_cursor());
    }
    let bytes = hex::decode(value).map_err(|_| invalid_cursor())?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| invalid_cursor())
}

pub(crate) fn encode_history_cursor<T: Serialize>(value: &T) -> Result<String, ApiResponse<()>> {
    serde_json::to_vec(value).map(hex::encode).map_err(|_| {
        internal_server_error_with_code(ErrorCode::InternalError, "Failed to encode history cursor")
    })
}

fn invalid_cursor() -> ApiResponse<()> {
    bad_request_with_code(ErrorCode::InvalidRequest, "Invalid history cursor")
}

fn invalid_date_boundary(message: &str) -> ApiResponse<()> {
    bad_request_with_code(ErrorCode::InvalidDateRange, message)
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::history_window;

    #[test]
    fn history_window_rejects_date_boundary_overflow() {
        assert!(history_window(Some(NaiveDate::MAX), Some(NaiveDate::MAX)).is_err());
        assert!(history_window(None, Some(NaiveDate::MIN)).is_err());
    }
}
