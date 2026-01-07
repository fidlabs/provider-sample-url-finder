# ADR-001: Content Size Validation for URL Discovery

## Status

Proposed

## Date

2024-12-17

## Context

### Problem Statement

The URL Finder service discovers HTTP endpoints for Filecoin Storage Providers and tests file retrievability. Currently, a URL is considered "working" if it:
- Returns HTTP 2xx status
- Has `content-type: application/octet-stream` or `application/piece`
- Has an `etag` header present

However, this validation is insufficient to detect dishonest or misconfigured providers. A provider could serve a tiny placeholder file (e.g., 1KB) that passes all current validation checks but clearly cannot be a legitimate Filecoin piece (which should be at least 16GB for standard sectors).

### Current System Analysis

**URL Testing Flow** (`url_tester.rs`):
```rust
// Current validation - does NOT check content size
if status.is_success()
    && matches!(
        content_type,
        Some("application/octet-stream") | Some("application/piece")
    )
    && etag.is_some()
{
    success_clone.fetch_add(1, Ordering::SeqCst);
    Some(url)
}
```

**Data Storage** (`url_results` table):
- `working_url TEXT` - stores a sample working URL
- `retrievability_percent NUMERIC(5,2)` - percentage of URLs that pass validation
- `result_code` - Success, FailedToGetWorkingUrl, NoDealsFound, etc.

**Gap**: No mechanism exists to:
1. Capture `Content-Length` from HTTP responses
2. Distinguish between "truly working" and "reachable but invalid" URLs
3. Track providers serving suspiciously small content
4. Query which providers have this problem

### Requirements

1. **Content size validation**: Filter out URLs where `content-length < 8GB` threshold
2. **Track invalid URLs separately**: URLs that are reachable (200, correct headers) but have content too small
3. **Evidence storage**: Store ONE invalid URL per discovery run as evidence of the problem
4. **Query capability**: API to find providers with "reachable but invalid" results
5. **Missing Content-Length**: Treat as invalid (suspicious behavior)

## Decision

### Approach: Extend Existing `url_results` Table

Store content validation results in the existing `url_results` table rather than creating a separate table.

**Rationale**:
- Keeps all URL discovery outcomes in one place
- Simpler schema evolution (no foreign keys needed)
- Existing queries and pagination continue to work
- A "reachable but invalid" result is still a URL discovery outcome

### New Result Code

Add `ReachableButInvalid` to the `ResultCode` enum to represent URLs that respond correctly but fail content size validation.

### Content-Length Threshold

**Default**: 8 GB (8,589,934,592 bytes)

**Rationale**:
- Filecoin sectors are typically 32GB or 64GB
- 8GB is conservative - catches obvious fakes while allowing some legitimate smaller pieces
- Configurable via `MIN_CONTENT_LENGTH_BYTES` environment variable

### Missing Content-Length Header

**Decision**: Treat as invalid

**Rationale**:
- Legitimate piece servers should know and report content size
- Missing header is suspicious behavior
- Better to flag for investigation than assume valid

### Evidence Storage

**Decision**: Store ONE invalid URL per discovery run

**Rationale**:
- One example is sufficient evidence of the problem
- Reduces storage overhead
- Avoids storing hundreds of invalid URLs per provider

### Data Scope for Queries

**Decision**: Latest result per provider only

**Rationale**:
- Simpler queries and API
- Current state is most actionable
- Historical tracking can be added later if needed

## Technical Design

### Database Schema Changes

```sql
-- New result code variant
ALTER TYPE result_code ADD VALUE 'ReachableButInvalid';

-- New columns on url_results
ALTER TABLE url_results ADD COLUMN content_length BIGINT NULL;
ALTER TABLE url_results ADD COLUMN invalid_evidence_url TEXT NULL;

-- Index for efficient querying of invalid providers
CREATE INDEX idx_url_results_invalid
    ON url_results(result_code, provider_id, tested_at DESC)
    WHERE result_code = 'ReachableButInvalid';
```

### URL Tester Changes

New return structure:
```rust
pub struct RetrievabilityResult {
    pub valid_url: Option<String>,
    pub valid_content_length: Option<u64>,
    pub invalid_evidence: Option<InvalidEvidence>,
    pub retrievability_percent: f64,
    pub total_tested: usize,
    pub valid_count: usize,
    pub reachable_but_invalid_count: usize,
}

pub struct InvalidEvidence {
    pub url: String,
    pub content_length: Option<u64>,
}
```

Modified validation logic:
1. Check HTTP status, content-type, etag (existing)
2. Extract `Content-Length` header
3. If present and >= threshold → valid
4. If present and < threshold → reachable-but-invalid
5. If missing → reachable-but-invalid
6. Track first invalid URL as evidence

### Result Code Assignment

```
if valid_url exists:
    result_code = Success
else if invalid_evidence exists:
    result_code = ReachableButInvalid
else:
    result_code = FailedToGetWorkingUrl
```

### API Endpoint

```
GET /providers/invalid?limit=100&offset=0

Response:
{
  "providers": [
    {
      "provider_id": "f0123456",
      "invalid_evidence_url": "http://1.2.3.4:3001/piece/bafyxxx",
      "content_length": 1024,
      "tested_at": "2024-12-17T00:00:00Z"
    }
  ],
  "total": 42,
  "limit": 100,
  "offset": 0
}
```

## Files to Modify

| File | Change |
|------|--------|
| `migrations/YYYYMMDDHHMMSS_add_content_size_validation.up.sql` | Schema changes |
| `url_finder/src/config.rs` | Add `min_content_length_bytes` config |
| `url_finder/src/types.rs` | Add `ReachableButInvalid` to ResultCode |
| `url_finder/src/url_tester.rs` | Content-length capture and validation |
| `url_finder/src/services/url_discovery_service.rs` | Handle new result structure |
| `url_finder/src/repository/url_result_repo.rs` | Update queries, add invalid providers query |
| `url_finder/src/api/providers/list_invalid_providers.rs` | New endpoint |
| `url_finder/src/api/providers/types.rs` | Response types |
| `url_finder/src/api/providers/mod.rs` | Export handler |
| `url_finder/src/routes.rs` | Add route |

## Implementation Order

1. Database migration (schema changes)
2. Configuration (threshold setting)
3. Types (ResultCode variant)
4. URL Tester (content-length capture + validation)
5. URL Discovery Service (handle new result structure)
6. Repository (update queries)
7. API (new endpoint)
8. Tests

## Testing Strategy

1. **Unit tests for URL tester**:
   - Mock responses with various content-length values
   - Test threshold boundaries (8GB-1, 8GB, 8GB+1)
   - Test missing content-length header

2. **Integration tests**:
   - Wiremock server returning small content-length
   - Verify correct result_code assignment
   - Verify only one evidence URL stored per run

3. **Repository tests**:
   - Test new query methods
   - Test batch insert with new columns
   - Test pagination for invalid providers list

4. **API tests**:
   - Test `/providers/invalid` endpoint
   - Verify response format

## Consequences

### Positive

- Detects dishonest providers serving placeholder content
- Provides evidence for compliance reporting
- Minimal schema changes (extends existing table)
- Configurable threshold for different use cases
- Clean separation: valid vs reachable-but-invalid vs unreachable

### Negative

- Adds complexity to URL testing logic
- Requires migration (but backwards compatible)
- Content-Length header can be spoofed (but still useful signal)

### Risks

- **False positives**: Legitimate small pieces might be flagged
  - Mitigation: 8GB threshold is conservative
  - Mitigation: Configurable via environment variable

- **Performance**: Additional header parsing
  - Mitigation: Negligible overhead (already reading headers)

## Alternatives Considered

### Alternative 1: Separate Table for Invalid URLs

Create `invalid_url_results` table.

**Rejected because**:
- Fragments URL discovery data
- Requires JOINs for complete picture
- More complex queries

### Alternative 2: Download and Measure Body

Actually download content to verify size.

**Rejected because**:
- Massive bandwidth cost
- Slow (downloading 16GB+ per URL)
- Content-Length header is sufficient signal

### Alternative 3: Multiple Threshold Levels

Warning (4GB), Error (8GB), Critical (16GB).

**Rejected because**:
- Adds complexity without clear benefit
- Single threshold is simpler to reason about
- Can be revisited later if needed

## References

- Filecoin sector sizes: 32GB, 64GB standard
- Current URL tester: `url_finder/src/url_tester.rs`
- Current schema: `migrations/20251110050249_create_url_results.up.sql`
