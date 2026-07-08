---
title: Rate Limits & Quotas
description: How the connectors stay inside Salesforce API allocations.
sidebar_position: 4
---

# Rate Limits & Quotas

Salesforce enforces a rolling 24-hour API-call pool (15,000 calls on Developer Edition;
100,000+ on Enterprise) shared across REST, Bulk, and SOAP. The connectors are designed
to stay well inside it.

## The rate governor

`sf-core` polls `GET /services/data/vXX.X/limits/` (at most every 5 minutes) and tracks:

| Limit | Used by |
|---|---|
| `DailyApiRequests` | REST calls (describe, gap re-query, composite writes, limits itself) |
| `DailyBulkV2QueryJobs` | Snapshot, polling, and resync query jobs (10,000/24h) |
| `DailyBulkApiBatches` | Bulk ingest (15,000/24h shared with Bulk 1.0) |

Before consuming quota, the governor checks the remaining fraction; below the 5% reserve
it fails the operation with a **retryable** error, so the task backs off with exponential
backoff + jitter instead of burning the remainder of the pool. Local consumption between
refreshes is tracked so the estimate stays honest.

## Retry classification

Transient failures are retried within `sf.request.max.retries.time.ms` (exponential
backoff with jitter):

- HTTP `429` and all `5xx`
- HTTP `403` with `REQUEST_LIMIT_EXCEEDED`
- gRPC `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`
- Network timeouts and connection resets
- `401`/`INVALID_SESSION_ID` triggers one transparent re-authentication

Everything else (validation errors, `INVALID_FIELD`, …) fails fast — per-record where
possible (DLQ), otherwise the task.

## Keeping API usage low

- **Event-driven mode is nearly free** — the Pub/Sub gRPC stream doesn't consume the
  REST API pool; describe results are cached with `If-Modified-Since` (304s are cheap).
- **`sf.full.record.on.update=false`** (default) avoids one REST call per UPDATE event.
- **Polling mode** costs one Bulk query job per object per interval — with the default
  30s interval that's ~2,880 jobs/day per object against the 10,000/day allocation; size
  `sf.poll.interval.ms` accordingly.
- **Sink**: prefer `sf.write.mode=auto` — large batches use one Bulk job instead of many
  Composite calls (a Composite call writes up to 200 records per API call).
