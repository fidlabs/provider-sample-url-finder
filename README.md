# Random Piece Availability measurement

Random Piece Availability (RPA) measures whether Filecoin storage provider data
is retrievable over HTTP. It discovers provider endpoints, tests piece URLs, and
stores retrievability and bandwidth results for consumers that need current
provider or deal-level availability data.

The service also exposes the Deal SLI API used by PoRep Market. PoRep Market can
register a deal target with a manifest location and manifest hash. RPA fetches
and verifies that manifest, derives the deal pieces from it, measures sampled
piece retrievability against cached provider endpoints, and stores the latest
deal SLI state for oracle or market integrations.

## Running Locally

Create a local environment file from the example:

```bash
cp .env.example .env
```

Start Postgres, run migrations, and seed local development data:

```bash
make init-dev
```

Run the service in Docker:

```bash
make run
```

For SSH-tunneled DMOB testing, start only Postgres with `make run-db` and run
the Rust binary on the host so `DMOB_DATABASE_URL` can point at the tunnel.
Configuration fields and local defaults are documented in `.env.example`.

The application listens on port `3010`:

- Swagger UI: `http://localhost:3010/`
- OpenAPI JSON: `http://localhost:3010/api-doc/openapi.json`
- Healthcheck: `http://localhost:3010/healthcheck`

## How The Service Works

RPA keeps two related measurement flows.

The provider flow discovers HTTP endpoints for storage providers and measures
retrievability for pieces selected from the deal database. It gets provider peer
IDs from Lotus RPC, fetches advertised endpoints from `cid.contact` and filspark,
tests piece URLs, and stores provider-level retrievability, URL, and BMS metrics.

The Deal SLI flow is deal-first. A caller registers a PoRep deal target through
the Deal SLI API. The target includes the provider, optional client, deal size,
manifest hash, manifest URL, and optional SLI requirements. RPA verifies the
manifest hash, stores a manifest snapshot, derives the measurable pieces, and
then records scheduled or manually triggered measurements for that deal.

Deal SLI measurements use ranged GET checks against cached provider endpoints.
The latest response reports deal state, retrievability, manifest size matching,
piece counts, a working URL when one was found, and BMS-derived PoRep SLI values
when bandwidth jobs have completed.

## API Overview

Swagger is the source of truth for request and response fields. The main API
groups are:

- `/deals/*` - Deal SLI API for PoRep Market and oracle integrations.
- `/providers/*` and `/clients/*` - provider and client views over stored
  retrievability, URL, and BMS data.
- `/url/*` - legacy URL Finder endpoints.

Write endpoints in the Deal SLI API require bearer authentication. Set
`AUTH_TOKEN` in `.env`; Swagger labels those endpoints with `bearer_auth`.

## Development

Common commands:

```bash
make init-dev
make run
make test
make prepare
```

`make prepare` runs SQLx prepare, formatting, and clippy. Run it before pushing
or opening a PR.

Integration tests use testcontainers for Postgres and wiremock for external HTTP
services.
