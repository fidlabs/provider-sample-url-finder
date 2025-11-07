-- Create storage_providers table
CREATE TABLE storage_providers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider_id VARCHAR(255) NOT NULL UNIQUE,

    -- URL discovery schedule
    next_url_discovery_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    url_discovery_status VARCHAR(50),
    last_working_url TEXT,

    -- BMS test schedule
    next_bms_test_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    bms_test_status VARCHAR(50),

    -- BMS region optimization
    bms_routing_key VARCHAR(50),
    last_bms_region_discovery_at TIMESTAMPTZ,

    -- Metadata
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_sp_next_url_discovery ON storage_providers(next_url_discovery_at)
    WHERE url_discovery_status IS DISTINCT FROM 'pending';

CREATE INDEX idx_sp_next_bms_test ON storage_providers(next_bms_test_at)
    WHERE bms_test_status IS DISTINCT FROM 'pending';

CREATE INDEX idx_sp_provider_id ON storage_providers(provider_id);
