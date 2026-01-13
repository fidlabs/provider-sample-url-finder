use crate::config::RELIABILITY_TIMEOUT_THRESHOLD;
use crate::types::{InconsistencyType, ProviderAnalysis, UrlTestError, UrlTestResult};

pub fn analyze_results(results: &[UrlTestResult]) -> ProviderAnalysis {
    if results.is_empty() {
        return ProviderAnalysis::empty();
    }

    let total = results.len();
    let success_count = results.iter().filter(|r| r.success).count();
    let timeout_count = results
        .iter()
        .filter(|r| matches!(r.error, Some(UrlTestError::Timeout)))
        .count();

    // Count inconsistent results by type
    let mut inconsistent_count = 0;
    let mut gaming = 0;
    let mut both_failed = 0;
    let mut error_pages = 0;
    let mut size_mismatch = 0;

    for r in results.iter().filter(|r| !r.consistent) {
        inconsistent_count += 1;
        match r.inconsistency_type {
            Some(InconsistencyType::Gaming) => gaming += 1,
            Some(InconsistencyType::BothFailed) => both_failed += 1,
            Some(InconsistencyType::ErrorPages) => error_pages += 1,
            Some(InconsistencyType::SizeMismatch) => size_mismatch += 1,
            None => {} // Shouldn't happen if !consistent, but handle gracefully
        }
    }

    // Total requests = 2 per URL (double-tap)
    let total_requests = total * 2;
    let timeout_rate = timeout_count as f64 / total_requests as f64;

    ProviderAnalysis {
        retrievability_percent: (success_count as f64 / total as f64) * 100.0,
        is_consistent: inconsistent_count == 0,
        is_reliable: timeout_rate < RELIABILITY_TIMEOUT_THRESHOLD,
        sample_count: total,
        success_count,
        timeout_count,
        inconsistent_count,
        inconsistent_gaming: gaming,
        inconsistent_both_failed: both_failed,
        inconsistent_error_pages: error_pages,
        inconsistent_size_mismatch: size_mismatch,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::InconsistencyType;

    fn make_result(success: bool, consistent: bool, error: Option<UrlTestError>) -> UrlTestResult {
        UrlTestResult {
            url: "http://test".to_string(),
            success,
            consistent,
            inconsistency_type: if consistent {
                None
            } else {
                Some(InconsistencyType::Gaming)
            },
            content_length: Some(16_000_000_000),
            response_time_ms: 100,
            error,
        }
    }

    fn make_inconsistent(inconsistency_type: InconsistencyType) -> UrlTestResult {
        UrlTestResult {
            url: "http://test".to_string(),
            success: true,
            consistent: false,
            inconsistency_type: Some(inconsistency_type),
            content_length: Some(16_000_000_000),
            response_time_ms: 100,
            error: None,
        }
    }

    #[test]
    fn test_analyze_all_successful_consistent() {
        let results = vec![
            make_result(true, true, None),
            make_result(true, true, None),
            make_result(true, true, None),
        ];

        let analysis = analyze_results(&results);

        assert_eq!(analysis.retrievability_percent, 100.0);
        assert!(analysis.is_consistent);
        assert!(analysis.is_reliable);
        assert_eq!(analysis.sample_count, 3);
        assert_eq!(analysis.success_count, 3);
    }

    #[test]
    fn test_analyze_one_inconsistent_fails_all() {
        let results = vec![
            make_result(true, true, None),
            make_result(true, false, None), // Inconsistent
            make_result(true, true, None),
        ];

        let analysis = analyze_results(&results);

        assert!(!analysis.is_consistent); // One bad = all bad
    }

    #[test]
    fn test_analyze_high_timeout_rate() {
        let results = vec![
            make_result(false, true, Some(UrlTestError::Timeout)),
            make_result(false, true, Some(UrlTestError::Timeout)),
            make_result(true, true, None),
        ];

        let analysis = analyze_results(&results);

        // 2 timeouts / 6 total requests = 33% > 30% threshold
        assert!(!analysis.is_reliable);
    }

    #[test]
    fn test_analyze_empty_results() {
        let results: Vec<UrlTestResult> = vec![];
        let analysis = analyze_results(&results);

        assert_eq!(analysis.retrievability_percent, 0.0);
        // Empty results should NOT claim consistency or reliability
        // since no verification was performed
        assert!(!analysis.is_consistent);
        assert!(!analysis.is_reliable);
    }

    #[test]
    fn test_analyze_inconsistent_breakdown() {
        let results = vec![
            make_result(true, true, None), // consistent
            make_inconsistent(InconsistencyType::Gaming),
            make_inconsistent(InconsistencyType::Gaming),
            make_inconsistent(InconsistencyType::BothFailed),
            make_inconsistent(InconsistencyType::ErrorPages),
            make_inconsistent(InconsistencyType::SizeMismatch),
        ];

        let analysis = analyze_results(&results);

        assert_eq!(analysis.sample_count, 6);
        assert_eq!(analysis.inconsistent_count, 5);
        assert_eq!(analysis.inconsistent_gaming, 2);
        assert_eq!(analysis.inconsistent_both_failed, 1);
        assert_eq!(analysis.inconsistent_error_pages, 1);
        assert_eq!(analysis.inconsistent_size_mismatch, 1);
        assert!(!analysis.is_consistent);
    }

    #[test]
    fn test_analyze_all_consistent_has_zero_breakdown() {
        let results = vec![make_result(true, true, None), make_result(true, true, None)];

        let analysis = analyze_results(&results);

        assert_eq!(analysis.inconsistent_count, 0);
        assert_eq!(analysis.inconsistent_gaming, 0);
        assert_eq!(analysis.inconsistent_both_failed, 0);
        assert_eq!(analysis.inconsistent_error_pages, 0);
        assert_eq!(analysis.inconsistent_size_mismatch, 0);
        assert!(analysis.is_consistent);
    }
}
