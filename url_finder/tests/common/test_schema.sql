-- DMOB schema for testing purposes 

CREATE TABLE IF NOT EXISTS unified_verified_deal (
    id SERIAL PRIMARY KEY,
    "dealId" INTEGER NOT NULL DEFAULT 0,
    "claimId" INTEGER NOT NULL DEFAULT 0,
    "clientId" TEXT,
    "providerId" TEXT,
    "pieceCid" TEXT
);
