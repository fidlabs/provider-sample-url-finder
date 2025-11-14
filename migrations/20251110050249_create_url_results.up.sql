CREATE TYPE discovery_type AS ENUM ('Provider', 'ProviderClient');

CREATE TYPE result_code AS ENUM (
    'NoCidContactData',
    'MissingAddrFromCidContact',
    'MissingHttpAddrFromCidContact',
    'FailedToGetWorkingUrl',
    'NoDealsFound',
    'TimedOut',
    'Success',
    'JobCreated',
    'Error'
);

CREATE TYPE error_code AS ENUM (
    'NoProviderOrClient',
    'NoProvidersFound',
    'FailedToRetrieveCidContactData',
    'FailedToGetPeerId',
    'FailedToGetDeals'
);

CREATE TABLE url_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider_id VARCHAR(255) NOT NULL,
    client_id VARCHAR(255),
    result_type discovery_type NOT NULL,

    working_url TEXT,
    retrievability_percent DOUBLE PRECISION NOT NULL DEFAULT 0.0,

    result_code result_code NOT NULL,
    error_code error_code,

    tested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_url_results_provider ON url_results(provider_id, tested_at DESC);
CREATE INDEX idx_url_results_pair ON url_results(provider_id, client_id, tested_at DESC);
CREATE INDEX idx_url_results_client ON url_results(client_id, tested_at DESC)
    WHERE client_id IS NOT NULL;
CREATE INDEX idx_url_results_tested_at ON url_results(tested_at DESC);
CREATE INDEX idx_url_results_result_type ON url_results(result_type, tested_at DESC);
