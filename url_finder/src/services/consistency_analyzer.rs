use crate::config::RELIABILITY_TIMEOUT_THRESHOLD;
use crate::types::{ProviderAnalysis, UrlTestError, UrlTestResult};

pub fn analyze_results(results: &[UrlTestResult]) -> ProviderAnalysis {
    if results.is_empty() {
        return ProviderAnalysis::empty();
    }

    let total = results.len();
    let success_count = results.iter().filter(|r| r.success).count();
    let inconsistent_count = results.iter().filter(|r| !r.consistent).count();
    let timeout_count = results
        .iter()
        .filter(|r| matches!(r.error, Some(UrlTestError::Timeout)))
        .count();

    // Total requests = 2 per URL (double-tap)
    let total_requests = total * 2;
    let timeout_rate = timeout_count as f64 / total_requests as f64;

    ProviderAnalysis {
        retrievability_percent: (success_count as f64 / total as f64) * 100.0,
        is_consistent: inconsistent_count == 0, // Consider false positive and very small treshold for flaky connections
        is_reliable: timeout_rate < RELIABILITY_TIMEOUT_THRESHOLD,
        sample_count: total,
        success_count,
        timeout_count,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_result(success: bool, consistent: bool, error: Option<UrlTestError>) -> UrlTestResult {
        UrlTestResult {
            url: "http://test".to_string(),
            success,
            consistent,
            content_length: Some(16_000_000_000),
            response_time_ms: 100,
            error,
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
}
