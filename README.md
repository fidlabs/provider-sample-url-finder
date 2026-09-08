# Random Piece Availability measurement

Random Piece Availability (RPA) measures whether Filecoin storage provider data
is retrievable over HTTP. It discovers provider endpoints, tests piece URLs, and
stores retrievability and bandwidth results for consumers that need current
or historical provider and deal-level availability data.

The service also exposes the Deal SLI API used by PoRep Market. PoRep Market can
register a deal target with a manifest location and manifest hash. RPA fetches
and verifies that manifest, derives the deal pieces from it, measures sampled
piece retrievability against cached provider endpoints, and stores deal SLI
runs for oracle or market integrations.

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

## Historical SLI API

Historical reads return the stored measurements grouped by UTC day. They keep
every record in the requested page, including multiple measurements on the same
day. They do not calculate daily averages or replace earlier measurements with
the last measurement of the day.

| Endpoint | Stored data |
| --- | --- |
| `GET /providers/{id}/history/sli` | Provider URL measurements and provider BMS jobs |
| `GET /providers/{id}/clients/{client_id}/history/sli` | URL measurements for that provider/client pair and separately identified provider-scoped BMS jobs |
| `GET /deals/{deal_id}/history/sli` | Deal runs with a first page of their own BMS jobs and derived PoRep SLI values |
| `GET /deals/{deal_id}/runs/{run_id}` | One stored run, its first BMS page, and paginated piece/URL results |
| `GET /deals/{deal_id}/runs/{run_id}/bms` | All BMS jobs for that run, through cursor pagination |

These GET endpoints use the same public read access as the existing provider and
deal GET endpoints. They do not trigger measurements or contact BMS.

### Dates and pagination

Use `from` and `to` in `YYYY-MM-DD` format. Both dates are inclusive, with
boundaries at midnight UTC. Omitting dates uses the default 30-day lookback.
The difference between `from` and `to` must not exceed 30 days; older windows
are valid. This is a query limit, not a statement about database retention.

`limit` defaults to 100 and accepts values from 1 to 500. Follow `next_cursor`
until it is `null`, keeping the same resource and date filters. History pages
select newest records first with a stable tie-breaker. A day can span multiple
pages:
combine entries with the same date when assembling a complete daily view.
Days with no records are omitted. Missing metric values stay `null`.
When starting without explicit dates, use the returned `from` and `to` on
subsequent pages so the window does not change at midnight. Treat day keys as
dates rather than relying on JSON object property order.

Provider pages share one limit across URL measurements and BMS jobs. URL records
use `tested_at`; BMS jobs use `created_at`, so a BMS job remains in its creation
day when it completes later. Deal history uses the run's `started_at`; all BMS
jobs attached to that run remain with it, even when they complete on another
day. BMS statuses and metric values may change while pending jobs finish, so
pagination does not freeze a snapshot of those values.

Each deal run embeds the first 10 BMS jobs and a `bms_next_cursor`. When that
cursor is not `null`, pass it to `/deals/{deal_id}/runs/{run_id}/bms` to read
the remaining jobs. Follow that endpoint's `next_cursor` until it is `null`.
The BMS endpoint uses the same default limit of 100 and maximum of 500, ordered
by `created_at` and ID descending. To export a complete history, follow both
the history's `next_cursor` and each run's `bms_next_cursor`. This bounds the
response without discarding stored BMS results.

### Interpreting BMS and SLI values

Provider BMS jobs belong to a provider, not to a client or a particular URL
measurement. The provider/client endpoint therefore returns BMS as explicitly
provider-scoped data. Its presence does not prove that the job measured that
client's data. A day can contain BMS jobs without URL measurements.

Deal BMS jobs carry a `run_id`. Historical run responses use only BMS jobs linked
to that exact run. Pending and failed jobs are included; derived PoRep metrics
use only completed jobs with a measured value. These aggregates include all
completed jobs for the run, including jobs beyond the embedded first page:

- `retrievability_bps`: stored retrievability percentage multiplied by 100.
- `bandwidth_mbps`: minimum completed BMS download speed, rounded down.
- `latency_ms`: maximum completed BMS TTFB, rounded up.
- `indexing_pct`: `null`, because indexing is not measured.

The run's `avg_response_time_ms` is the ranged-GET check duration and is separate
from BMS TTFB. SLI requirements on a registered target are requirements, not
measurement results. A historical run retains its manifest snapshot ID and
stored size checks.

| Field | Interpretation |
| --- | --- |
| Provider `retrievability_percent` | Percentage of sampled URL tests classified as reachable by the provider analyzer |
| Provider `large_files_percent` | Percentage with a successful response meeting the provider size threshold |
| Provider `car_files_percent` | Percentage with a recognized valid CAR header |
| Provider `sector_utilization_percent` | Mean observed content length divided by piece size, as a percentage |
| Provider `is_consistent` / `is_reliable` | Double-tap consistency and timeout classification for that measurement, not historical uptime |
| Deal `retrievability_percent` | Percentage of sampled pieces with a retrievable URL |
| Deal `size_matched_percent` | Percentage of sampled pieces whose double-tap size checks match the manifest |
| Deal `content_matches_deal` | Whether the sum of manifest file sizes equals the registered deal size; not a proof of the complete file contents |
| Deal `success_count` / piece `success` | Exact size-match success, which is stricter than retrievability |
| BMS `ping_avg_ms`, `head_avg_ms`, `ttfb_ms`, `download_speed_mbps` | Retained ping, HEAD, time-to-first-byte, and download-speed measurements |

Use `sampled_piece_count` alongside deal percentages: a run samples up to 100
manifest pieces. Legacy provider-style fields can be `null` on deal runs because
the manifest measurement flow does not measure them.

The history exposes what RPA retained. Provider URL measurements are aggregates;
individual URL/tap results are not stored for that flow. Deal piece results are
combined double-tap results. BMS persistence keeps four selected metrics from
the first worker of the last completed subjob with worker data, not every
worker/subjob measurement. Some interrupted attempts and infrastructure errors
exist only in logs. The API cannot reconstruct data that was not stored.

The older `/history/retrievability` endpoints retain their existing behavior:
the last URL result per day, without BMS history.

### Request examples

Fetch all stored provider SLI and BMS records for a date window, starting with
a page of up to 100 records:

```bash
curl --get 'http://localhost:3010/providers/f01000/history/sli' \
  --data-urlencode 'from=2026-09-01' \
  --data-urlencode 'to=2026-09-08' \
  --data-urlencode 'limit=100'
```

Filter URL measurements to a provider/client pair. BMS remains provider-scoped:

```bash
curl --get 'http://localhost:3010/providers/f01000/clients/f02000/history/sli' \
  --data-urlencode 'from=2026-09-01' \
  --data-urlencode 'to=2026-09-08'
```

Fetch deal history, including the first BMS page for each returned run:

```bash
curl --get 'http://localhost:3010/deals/42/history/sli' \
  --data-urlencode 'from=2026-09-01' \
  --data-urlencode 'to=2026-09-08' \
  --data-urlencode 'limit=100'
```

For the next page, copy the returned `next_cursor` without interpreting it and
repeat the same URL and date filters:

```bash
curl --get 'http://localhost:3010/deals/42/history/sli' \
  --data-urlencode 'from=2026-09-01' \
  --data-urlencode 'to=2026-09-08' \
  --data-urlencode 'limit=100' \
  --data-urlencode 'cursor=<next_cursor from the previous response>'
```

Inspect a returned run's piece/URL results. Replace the example UUID with a run
ID from that deal's history; a run belonging to another deal returns 404:

```bash
curl --get 'http://localhost:3010/deals/42/runs/00000000-0000-4000-8000-000000000001' \
  --data-urlencode 'limit=100'
```

Continue beyond the run's embedded BMS page using its `bms_next_cursor`:

```bash
curl --get 'http://localhost:3010/deals/42/runs/00000000-0000-4000-8000-000000000001/bms' \
  --data-urlencode 'limit=100' \
  --data-urlencode 'cursor=<bms_next_cursor from the run>'
```

Omit the cursor to read BMS jobs from the beginning. The BMS response contains
`deal_id`, `run_id`, `data` with full BMS records, and `next_cursor`.

Run-detail pagination applies to piece/URL results. Reuse that endpoint's
`next_cursor` on the same deal and run. Piece results are ordered by
`piece_index`, then `url_tested`, both ascending. A registered deal with no runs
has an empty history; an unknown deal returns 404. Invalid date ranges, limits, and
cursors return 400.

### Provider response example

This example contains one URL measurement and one completed BMS job. `scope`
identifies the record type; BMS has no `client_id`.

```json
{
  "provider_id": "f01000",
  "from": "2026-09-01",
  "to": "2026-09-08",
  "data": {
    "2026-09-08": [
      {
        "scope": "url",
        "id": "00000000-0000-4000-8000-000000000001",
        "provider_id": "f01000",
        "client_id": null,
        "result_type": "Provider",
        "working_url": "https://provider.example/piece/piece-cid",
        "retrievability_percent": 95.0,
        "result_code": "Success",
        "error_code": null,
        "tested_at": "2026-09-08T12:00:00Z",
        "is_consistent": true,
        "is_reliable": true,
        "url_metadata": null,
        "sector_utilization_percent": 80.0,
        "car_files_percent": 95.0,
        "large_files_percent": 95.0
      },
      {
        "scope": "provider_bms",
        "id": "00000000-0000-4000-8000-000000000002",
        "provider_id": "f01000",
        "bms_job_id": "00000000-0000-4000-8000-000000000003",
        "url_tested": "https://provider.example/piece/piece-cid",
        "routing_key": "us_east",
        "worker_count": 1,
        "status": "Completed",
        "ping_avg_ms": 12.5,
        "head_avg_ms": 30.0,
        "ttfb_ms": 80.0,
        "download_speed_mbps": 250.5,
        "created_at": "2026-09-08T11:00:00Z",
        "completed_at": "2026-09-08T11:05:00Z"
      }
    ]
  },
  "next_cursor": null
}
```

### Deal response example

Each run includes its stored context and its own BMS jobs. This completed URL
run still has a pending BMS job, so bandwidth and latency remain `null`.

```json
{
  "deal_id": "42",
  "from": "2026-09-01",
  "to": "2026-09-08",
  "data": {
    "2026-09-08": [
      {
        "id": "00000000-0000-4000-8000-000000000001",
        "deal_id": "42",
        "state": "completed",
        "measurement_state": "fresh",
        "started_at": "2026-09-08T12:00:00Z",
        "completed_at": "2026-09-08T12:00:10Z",
        "tested_at": "2026-09-08T12:00:10Z",
        "provider_id": "1000",
        "client_id": "2000",
        "working_url": "https://provider.example/piece/piece-cid",
        "retrievability_percent": 100.0,
        "large_files_percent": null,
        "car_files_percent": null,
        "sector_utilization_percent": null,
        "manifest_snapshot_id": "00000000-0000-4000-8000-000000000004",
        "deal_size_bytes": "1073741824",
        "manifest_size_bytes": "1073741824",
        "content_matches_deal": true,
        "sampled_piece_count": 1,
        "size_matched_percent": 100.0,
        "avg_response_time_ms": 125.0,
        "is_consistent": null,
        "is_reliable": true,
        "result_code": "Success",
        "error_code": null,
        "piece_count": 1,
        "success_count": 1,
        "failed_count": 0,
        "url_metadata": null,
        "porep_slis": {
          "retrievability_bps": 10000,
          "bandwidth_mbps": null,
          "latency_ms": null,
          "indexing_pct": null
        },
        "bms_results": [
          {
            "id": "00000000-0000-4000-8000-000000000002",
            "deal_id": "42",
            "run_id": "00000000-0000-4000-8000-000000000001",
            "piece_index": 0,
            "piece_cid": "piece-cid",
            "bms_job_id": "00000000-0000-4000-8000-000000000003",
            "url_tested": "https://provider.example/piece/piece-cid",
            "routing_key": "us_east",
            "worker_count": 1,
            "status": "Pending",
            "ping_avg_ms": null,
            "head_avg_ms": null,
            "ttfb_ms": null,
            "download_speed_mbps": null,
            "error_message": null,
            "created_at": "2026-09-08T12:00:11Z",
            "completed_at": null
          }
        ],
        "bms_next_cursor": null
      }
    ]
  },
  "next_cursor": null
}
```

The run-detail response has `run`, `piece_results`, and `next_cursor` fields.
`run` has the same fields as a run above. Each `piece_results` entry has this
shape:

```json
{
  "run_id": "00000000-0000-4000-8000-000000000001",
  "deal_id": "42",
  "piece_index": 0,
  "piece_cid": "piece-cid",
  "url_tested": "https://provider.example/piece/piece-cid",
  "success": true,
  "content_length": 1073741824,
  "is_valid_car": false,
  "result_code": "Success",
  "tested_at": "2026-09-08T12:00:10Z",
  "manifest_snapshot_id": "00000000-0000-4000-8000-000000000004",
  "file_size_bytes": "1073741824",
  "observed_size_bytes": 1073741824,
  "size_matched": true,
  "manifest_response_time_ms": 125
}
```

The manifest measurement flow stores `is_valid_car: false` without performing
the provider flow's CAR check. Do not interpret it as a measured CAR failure.

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
