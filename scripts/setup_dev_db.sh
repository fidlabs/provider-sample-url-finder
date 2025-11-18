#!/bin/bash
set -e

# Load DATABASE_URL from .env
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# SQL file name or default
FILE_NAME=${1:-"sample_deals.sql"}

if [ -z "$DATABASE_URL" ]; then
    echo "ERROR: DATABASE_URL not set in .env"
    exit 1
fi

echo "Setting up development database..."

# Create unified_verified_deal table (matches DMOB schema)
psql "$DATABASE_URL" <<EOF
-- Drop if exists (for clean resets)
DROP TABLE IF EXISTS unified_verified_deal CASCADE;

-- Create table matching DMOB schema
CREATE TABLE unified_verified_deal (
    id SERIAL PRIMARY KEY,
    "dealId" INTEGER NOT NULL DEFAULT 0,
    "claimId" INTEGER NOT NULL DEFAULT 0,
    type VARCHAR,
    "clientId" VARCHAR,
    "providerId" VARCHAR,
    "sectorId" VARCHAR,
    "pieceCid" VARCHAR,
    "pieceSize" NUMERIC,
    "termMax" NUMERIC,
    "termMin" NUMERIC,
    "termStart" NUMERIC,
    "slashedEpoch" NUMERIC NOT NULL DEFAULT 0,
    "processedSlashedEpoch" INTEGER NOT NULL DEFAULT 0,
    removed BOOLEAN DEFAULT false,
    "createdAt" TIMESTAMP NOT NULL DEFAULT NOW(),
    "updatedAt" TIMESTAMP NOT NULL DEFAULT NOW(),
    "dcSource" VARCHAR
);

-- Create indexes matching DMOB
CREATE INDEX unified_verified_deal_claimid_index ON unified_verified_deal("claimId");
CREATE INDEX unified_verified_deal_clientid_index ON unified_verified_deal("clientId");
CREATE INDEX unified_verified_deal_dealid_index ON unified_verified_deal("dealId");
CREATE INDEX unified_verified_deal_piececid_index ON unified_verified_deal("pieceCid");
CREATE INDEX unified_verified_deal_providerid_index ON unified_verified_deal("providerId");
CREATE INDEX unified_verified_deal_providerid_piececid_sectorid_index
    ON unified_verified_deal("providerId", "pieceCid", "sectorId");
CREATE INDEX unified_verified_deal_sectorid_index ON unified_verified_deal("sectorId");

EOF

echo "Table created. Seeding sample data..."

# Seed with sample data
psql "$DATABASE_URL" < scripts/sql/$FILE_NAME

echo "✓ Development database setup complete!"
echo "✓ unified_verified_deal table created and seeded"
