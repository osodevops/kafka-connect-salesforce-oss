# PRD-02 — Salesforce SObject Sink Connector

**Status:** Draft for build
**Replaces (Confluent):** SObject Sink, Bulk API 1.0 Sink, Bulk API 2.0 Sink
**Depends on:** PRD-00 (sf-core)
**Clean-room note:** Specified from Salesforce Bulk API 2.0 / REST docs + Confluent public behaviour docs. Own `sf.*` config namespace.

---

## 1. Objective
A Kafka Connect **sink** connector that writes Kafka records into Salesforce SObjects (create/update/upsert/delete), using Bulk API 2.0 for throughput and optionally REST/Composite for low latency. Parity with Confluent's three sink connectors ([SObject Sink](https://docs.confluent.io/kafka-connectors/salesforce/current/sobjects_sink/salesforce_sobject_sink_connector_config.html), [Bulk 2.0 Sink](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-sink.html)).

## 2. Scope
### In scope
- Operations: insert, update, upsert (external ID), delete; per-topic → SObject mapping.
- Bulk API 2.0 ingest jobs (CSV) as the default write path; REST/Composite for small/low-latency batches.
- Big Object (`__b`) insert support.
- Event-type-driven operation (`_EventType` created/updated/deleted → insert/update/delete) with override.
- Cross-org sync via external ID field.
- Multi-SObject per instance; multiple tasks.
- DLQ + error handling.

### Out of scope
- Reading from Salesforce (PRD-01). CSFLE (phase 2).

## 3. User stories
- Sync `Lead` changes from org A (via PRD-01 source) into org B via external-ID upsert.
- Bulk-load millions of records from a Kafka topic into a custom object nightly.
- Insert audit rows into a Big Object.

## 4. Functional requirements

### 4.1 Operation mapping
- Default: map record `_EventType` (`created`→insert, `updated`→update, `deleted`→delete) — parity with Confluent SObject/Bulk sinks.
- `sf.<obj>.override.event.type=true` + `sf.<obj>.operation = insert|update|upsert|delete` forces one operation regardless of `_EventType`.
- Read-only Salesforce fields (createable=false/updateable=false) are silently excluded (matches Confluent; not an error).

### 4.2 External-ID / cross-org
- `sf.<obj>.use.custom.id.field=true` + `sf.<obj>.custom.id.field.name=<ExtId__c>` for update/delete/upsert across orgs. `Id` is only valid in the originating org; inserts ignore `Id`. External ID must be marked external in both orgs ([SObject sink quickstart](https://docs.confluent.io/kafka-connectors/salesforce/current/sobjects_sink/salesforce_sobject_sink_connector_quickstart.html)).
- Bulk API supports upsert only via an External ID field.

### 4.3 Write paths
- **Bulk 2.0 (default, high volume):** build CSV, `POST /jobs/ingest` (operation, object, `externalIdFieldName` for upsert), `PUT .../batches`, `PATCH state=UploadComplete`, poll `JobComplete`, read `successfulResults`/`failedResults`/`unprocessedrecords`. Failed rows → DLQ ([Bulk 2.0 ingest](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/bulk_api_2_0_ingest.htm)).
- **REST/Composite (optional, low latency):** `sf.write.mode=rest` uses Composite sObtree/collections for small batches (≤200 records) to cut latency vs Bulk job overhead.
- `sf.write.mode = bulk2 | rest | auto` (default `auto`: rest under a threshold, bulk2 above).

### 4.4 Big Objects (`__b`)
- `sf.<obj>.type = standard_custom | big_object`. Big Objects are insert-only; route `_EventType` updated/deleted to error topic; relationship fields unsupported (parity with Bulk 2.0 Sink).

### 4.5 Multi-SObject & mapping
- `sf.objects` list, each mapped to one or more topics via `sf.<obj>.topics`. No single topic maps to multiple SObjects.
- `topics` / `topics.regex` select source topics.
- Multiple tasks supported (`tasks.max`) — more tasks reduce consumer lag.

### 4.6 Field handling
- `sf.<obj>.ignore.fields` (comma list), `sf.<obj>.ignore.reference.fields` (bool), `sf.<obj>.skip.relationship.fields` (bool). Relationship (lookup via external ID) supported when both relationship flags allow it (parity with Bulk 2.0 Sink; polymorphic lookups not supported).

## 5. Configuration (selected)
| Property | Type | Default | Description |
|---|---|---|---|
| `sf.auth.*` | — | — | Same as PRD-00 (client_credentials/jwt/password) |
| `sf.instance.url` / `sf.api.version` | string | — / `60.0` | Endpoint / version |
| `topics` / `topics.regex` | list/string | — | Source topics |
| `sf.objects` | list | — | Target SObjects (≤5 default) |
| `sf.<obj>.topics` | list | — | Topic(s) feeding this object |
| `sf.<obj>.operation` | enum | `insert` | insert/update/upsert/delete |
| `sf.<obj>.override.event.type` | bool | false | Ignore `_EventType`, force operation |
| `sf.<obj>.use.custom.id.field` | bool | false | Use external ID |
| `sf.<obj>.custom.id.field.name` | string | — | External ID field API name |
| `sf.<obj>.type` | enum | `standard_custom` | `standard_custom` \| `big_object` |
| `sf.<obj>.ignore.fields` | list | — | Fields to skip |
| `sf.<obj>.ignore.reference.fields` | bool | false | Skip reference fields |
| `sf.write.mode` | enum | `auto` | `bulk2` \| `rest` \| `auto` |
| `sf.write.rest.threshold` | int | 200 | Max records to use REST path in `auto` |
| `behavior.on.api.errors` | enum | `fail` | `fail` \| `log` \| `ignore` |
| `errors.deadletterqueue.topic.name` | string | `dlq-${connector}` | DLQ |
| `sf.max.batch.records` | int | 10000 | Records per Bulk batch (≤10k Bulk1 / internal 10k Bulk2) |
| `sf.request.max.retries.time.ms` | long | 30000 | Retry budget |

## 6. Delivery & error handling
- **At-least-once.** Restarts may duplicate (use upsert/external ID for idempotency where possible).
- Per-record failures from Bulk `failedResults` → DLQ with error reason; `behavior.on.api.errors` controls fail/log/ignore.
- Reported errors: duplicate inserts, update/delete/upsert of non-existent/deleted Id (parity with Bulk 1.0 sink).
- Use Kafka Connect's native errant-record reporter for DLQ (replaces Confluent's proprietary reporter topics; optionally offer success/error reporter topics for parity).

## 7. Rate limits
- Bulk 2.0: 150M records/24h, 150MB/job (upload ≤100MB raw CSV due to base64), 10k query jobs/24h; internal batches retried up to 20× before job Failed ([Bulk 2.0 limits](https://developer.salesforce.com/docs/atlas.en-us.salesforce_app_limits_cheatsheet.meta/salesforce_app_limits_cheatsheet/salesforce_app_limits_platform_bulkapi.htm)). Governor throttles to stay under 24h API pool.

## 8. Acceptance criteria
- [ ] Insert/update/delete driven by `_EventType`; override works.
- [ ] External-ID upsert syncs records across two orgs.
- [ ] Bulk 2.0 job lifecycle handled; failed rows land in DLQ with reasons.
- [ ] REST path used for small batches in `auto` mode; Bulk path for large.
- [ ] Big Object insert works; update/delete routed to error topic.
- [ ] Read-only fields excluded without failing the batch.
- [ ] Multiple tasks scale throughput.
