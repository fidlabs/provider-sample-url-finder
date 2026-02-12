use crate::config::RELIABILITY_TIMEOUT_THRESHOLD;
use crate::types::{InconsistencyType, ProviderAnalysis, UrlTestError, UrlTestResult};

pub fn analyze_results(results: &[UrlTestResult]) -> ProviderAnalysis {
    if results.is_empty() {
        return ProviderAnalysis::empty();
    }

    let total = results.len();
    let success_count = results.iter().filter(|r| r.success).count();
    let valid_car_count = results.iter().filter(|r| r.is_valid_car).count();
    let timeout_count = results
        .iter()
        .filter(|r| matches!(r.error, Some(UrlTestError::Timeout)))
        .count();

    let mut inconsistent_count = 0;
    let mut warm_up = 0;
    let mut flaky = 0;
    let mut small_responses = 0;
    let mut size_mismatch = 0;

    for r in results.iter().filter(|r| !r.consistent) {
        inconsistent_count += 1;
        match r.inconsistency_type {
            Some(InconsistencyType::WarmUp) => warm_up += 1,
            Some(InconsistencyType::Flaky) => flaky += 1,
            Some(InconsistencyType::SmallResponses) => small_responses += 1,
            Some(InconsistencyType::SizeMismatch) => size_mismatch += 1,
            None => {}
        }
    }

    let total_requests = total * 2;
    let timeout_rate = timeout_count as f64 / total_requests as f64;

    let http_responded_count = results
        .iter()
        .filter(|r| r.success || !r.consistent)
        .count();
    let failed_count = total - http_responded_count;

    ProviderAnalysis {
        retrievability_percent: (http_responded_count as f64 / total as f64) * 100.0,
        car_files_percent: (valid_car_count as f64 / total as f64) * 100.0,
        large_files_percent: (success_count as f64 / total as f64) * 100.0,
        is_consistent: inconsistent_count == 0,
        is_reliable: timeout_rate < RELIABILITY_TIMEOUT_THRESHOLD,
        sample_count: total,
        success_count,
        timeout_count,
        inconsistent_count,
        inconsistent_warm_up: warm_up,
        inconsistent_flaky: flaky,
        inconsistent_small_responses: small_responses,
        inconsistent_size_mismatch: size_mismatch,
        http_responded_count,
        failed_count,
        valid_car_count,
        small_car_count: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::InconsistencyType;

    fn make_result(
        success: bool,
        consistent: bool,
        error: Option<UrlTestError>,
        is_valid_car: bool,
    ) -> UrlTestResult {
        UrlTestResult {
            url: "http://test".to_string(),
            success,
            consistent,
            inconsistency_type: if consistent {
                None
            } else {
                Some(InconsistencyType::WarmUp)
            },
            content_length: Some(16_000_000_000),
            response_time_ms: 100,
            error,
            is_valid_car,
            root_cid: None,
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
            is_valid_car: false,
            root_cid: None,
        }
    }

    #[test]
    fn test_analyze_all_successful_consistent() {
        let results = vec![
            make_result(true, true, None, false),
            make_result(true, true, None, false),
            make_result(true, true, None, false),
        ];

        let analysis = analyze_results(&results);

        assert_eq!(analysis.retrievability_percent, 100.0);
        assert_eq!(analysis.large_files_percent, 100.0);
        assert_eq!(analysis.car_files_percent, 0.0);
        assert!(analysis.is_consistent);
        assert!(analysis.is_reliable);
        assert_eq!(analysis.sample_count, 3);
        assert_eq!(analysis.success_count, 3);
        assert_eq!(analysis.http_responded_count, 3);
        assert_eq!(analysis.failed_count, 0);
        assert_eq!(analysis.valid_car_count, 0);
        assert_eq!(analysis.small_car_count, 0);
    }

    #[test]
    fn test_analyze_one_inconsistent_still_reachable() {
        // All succeed but one is inconsistent -- lenient retri stays 100%
        // because all responded (success=true implies HTTP response)
        let results = vec![
            make_result(true, true, None, false),
            make_result(true, false, None, false), // Inconsistent but still responded
            make_result(true, true, None, false),
        ];

        let analysis = analyze_results(&results);

        assert_eq!(analysis.retrievability_percent, 100.0);
        assert_eq!(analysis.large_files_percent, 100.0);
        assert!(!analysis.is_consistent); // One bad = all bad
    }

    #[test]
    fn test_analyze_high_timeout_rate() {
        let results = vec![
            make_result(false, true, Some(UrlTestError::Timeout), false),
            make_result(false, true, Some(UrlTestError::Timeout), false),
            make_result(true, true, None, false),
        ];

        let analysis = analyze_results(&results);

        // 2 timeouts / 6 total requests = 33% > 30% threshold
        assert!(!analysis.is_reliable);
        // Only 1 of 3 responded at all
        let expected_one_third = (1.0_f64 / 3.0) * 100.0;
        assert_eq!(analysis.retrievability_percent, expected_one_third);
        assert_eq!(analysis.large_files_percent, expected_one_third);
        assert_eq!(analysis.car_files_percent, 0.0);
        assert_eq!(analysis.http_responded_count, 1);
        assert_eq!(analysis.failed_count, 2);
        assert_eq!(analysis.valid_car_count, 0);
    }

    #[test]
    fn test_analyze_empty_results() {
        let results: Vec<UrlTestResult> = vec![];
        let analysis = analyze_results(&results);

        assert_eq!(analysis.retrievability_percent, 0.0);
        assert_eq!(analysis.large_files_percent, 0.0);
        assert_eq!(analysis.car_files_percent, 0.0);
        assert!(!analysis.is_consistent);
        assert!(!analysis.is_reliable);
    }

    #[test]
    fn test_analyze_inconsistent_breakdown() {
        let results = vec![
            make_result(true, true, None, false),
            make_inconsistent(InconsistencyType::WarmUp),
            make_inconsistent(InconsistencyType::WarmUp),
            make_inconsistent(InconsistencyType::Flaky),
            make_inconsistent(InconsistencyType::SmallResponses),
            make_inconsistent(InconsistencyType::SizeMismatch),
        ];

        let analysis = analyze_results(&results);

        assert_eq!(analysis.sample_count, 6);
        assert_eq!(analysis.inconsistent_count, 5);
        assert_eq!(analysis.inconsistent_warm_up, 2);
        assert_eq!(analysis.inconsistent_flaky, 1);
        assert_eq!(analysis.inconsistent_small_responses, 1);
        assert_eq!(analysis.inconsistent_size_mismatch, 1);
        assert!(!analysis.is_consistent);
        // All 6 responded (all success=true), so lenient retri = 100%
        assert_eq!(analysis.retrievability_percent, 100.0);
        assert_eq!(analysis.large_files_percent, 100.0);
    }

    #[test]
    fn test_analyze_all_consistent_has_zero_breakdown() {
        let results = vec![
            make_result(true, true, None, false),
            make_result(true, true, None, false),
        ];

        let analysis = analyze_results(&results);

        assert_eq!(analysis.inconsistent_count, 0);
        assert_eq!(analysis.inconsistent_warm_up, 0);
        assert_eq!(analysis.inconsistent_flaky, 0);
        assert_eq!(analysis.inconsistent_small_responses, 0);
        assert_eq!(analysis.inconsistent_size_mismatch, 0);
        assert!(analysis.is_consistent);
    }

    #[test]
    fn test_car_retrievability_counted() {
        let results = vec![
            make_result(true, true, None, true),  // valid CAR
            make_result(true, true, None, true),  // valid CAR
            make_result(true, true, None, false), // not CAR
        ];

        let analysis = analyze_results(&results);

        let expected_two_thirds = (2.0_f64 / 3.0) * 100.0;
        assert_eq!(analysis.car_files_percent, expected_two_thirds);
        assert_eq!(analysis.large_files_percent, 100.0);
        assert_eq!(analysis.retrievability_percent, 100.0);
        assert_eq!(analysis.valid_car_count, 2);
    }

    #[test]
    fn test_lenient_vs_strict_retrievability() {
        // Scenario: 2 success, 1 inconsistent-only (success=false, consistent=false)
        // This tests that lenient counts inconsistent-only results but strict doesn't
        let results = vec![
            make_result(true, true, None, false),
            make_result(true, true, None, false),
            UrlTestResult {
                url: "http://test".to_string(),
                success: false,
                consistent: false,
                inconsistency_type: Some(InconsistencyType::SmallResponses),
                content_length: Some(1000),
                response_time_ms: 100,
                error: None,
                is_valid_car: false,
                root_cid: None,
            },
        ];

        let analysis = analyze_results(&results);

        // Lenient: 2 success + 1 inconsistent-only = 3 responded out of 3
        assert_eq!(analysis.retrievability_percent, 100.0);
        // Strict: only 2 success out of 3
        let expected_two_thirds = (2.0_f64 / 3.0) * 100.0;
        assert_eq!(analysis.large_files_percent, expected_two_thirds);
        assert_eq!(analysis.http_responded_count, 3);
        assert_eq!(analysis.failed_count, 0);
    }

    #[test]
    fn test_failed_not_counted_in_lenient() {
        // Scenario: 1 success, 2 complete failures (no response at all)
        let results = vec![
            make_result(true, true, None, false),
            make_result(false, true, Some(UrlTestError::ConnectionRefused), false),
            make_result(false, true, Some(UrlTestError::Timeout), false),
        ];

        let analysis = analyze_results(&results);

        // Lenient: only 1 responded (success=true, consistent=true)
        // The 2 failures have success=false, consistent=true -> not counted
        let expected_one_third = (1.0_f64 / 3.0) * 100.0;
        assert_eq!(analysis.retrievability_percent, expected_one_third);
        assert_eq!(analysis.large_files_percent, expected_one_third);
        assert_eq!(analysis.http_responded_count, 1);
        assert_eq!(analysis.failed_count, 2);
    }
}
