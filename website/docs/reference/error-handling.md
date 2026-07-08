---
title: Error Handling & DLQ
description: Retries, dead-letter queues, and failure modes.
sidebar_position: 2
---

# Error Handling & DLQ

## Failure classes

| Class | Examples | Handling |
|---|---|---|
| Transient | 429/5xx, `REQUEST_LIMIT_EXCEEDED`, gRPC `UNAVAILABLE`, timeouts | Retried with exponential backoff + jitter within `sf.request.max.retries.time.ms` |
| Session expiry | 401, `INVALID_SESSION_ID` | One transparent re-auth, then retry |
| Per-record | Bulk `failedResults` rows, Composite per-record errors, schema/type mismatches, Big Object update/delete | Reported to the errant-record reporter (DLQ); task behaviour per `behavior.on.api.errors` |
| Fatal | Invalid config, failed Bulk jobs, `sf.gap.recovery=fail` on gaps | Task fails with a descriptive `ConnectException` |

## Dead-letter queue (sinks)

Both sinks use Kafka Connect's **native errant-record reporter** (Connect ≥ 2.6) instead
of proprietary reporter topics:

```json
"errors.tolerance": "all",
"errors.deadletterqueue.topic.name": "dlq-salesforce-sink",
"errors.deadletterqueue.context.headers.enable": "true",
"errors.deadletterqueue.topic.replication.factor": "3"
```

Every rejected record lands on the DLQ with the original payload; with context headers
enabled, the failure reason (e.g. `REQUIRED_FIELD_MISSING: Name is required`) travels in
the `__connect.errors.*` headers.

`behavior.on.api.errors` decides what happens *after* reporting:

- `fail` — stop the task (default; nothing is silently dropped)
- `log` — warn and continue
- `ignore` — continue silently

Bulk `failedResults` rows are correlated back to their originating Kafka records by
Id/external-ID match so DLQ entries reference the right record.

## Source-side errors

- Per-SObject pipelines are isolated: a failing object logs its error and other objects
  keep flowing; the task fails only when it has nothing else to deliver.
- Un-decodable events or missing `ChangeEventHeader` fail the task rather than dropping
  data (source connectors have no safe skip).
- `errors.tolerance=all` on the source applies only to conversion/transform stages —
  beware that it can drop records; prefer fixing the converter.

## Monitoring signals

- Standard Connect task metrics (`connector-task-metrics`, `source-record-write-rate`,
  `sink-record-read-rate`) work as usual.
- Watch the logs for `Retryable failure ... backing off` (transient churn),
  `quota ... nearly exhausted` (rate governor engaged), and `Gap event` (CDC gaps
  recovered).
