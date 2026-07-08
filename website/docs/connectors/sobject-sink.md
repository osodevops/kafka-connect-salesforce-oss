---
title: SObject Sink Connector
description: Write Kafka records into Salesforce SObjects — insert, update, upsert, delete, Big Objects.
sidebar_position: 2
---

# Salesforce SObject Sink Connector

`sh.oso.salesforce.sink.SalesforceSinkConnector`

Writes Kafka records into Salesforce SObjects using **Bulk API 2.0** ingest jobs for
throughput or **REST Composite** calls for low latency (`sf.write.mode=auto` picks per
batch size).

## Operation mapping

By default the operation is driven by the record's `_EventType` field (the shape emitted
by the [source connector](source.md)):

| `_EventType` | Operation | With `sf.<obj>.use.custom.id.field=true` |
|---|---|---|
| `created` | insert | upsert (external ID) |
| `updated` | update (by `Id`) | upsert (external ID) |
| `deleted` | delete | delete (Id resolved via external ID) |

Set `sf.<obj>.override.event.type=true` + `sf.<obj>.operation` to force a single
operation regardless of `_EventType`.

Read-only Salesforce fields (`createable=false` / `updateable=false` in the object
describe) are **silently excluded** — a record carrying `CreatedDate` or `IsDeleted`
doesn't fail the batch. `_EventType`/`_ObjectType` and configured `ignore.fields` are
also stripped.

## Cross-org sync with external IDs

`Id` values are only valid in the originating org. For org-to-org sync, mark a field as
an External ID in the target org and configure:

```properties
sf.Lead.use.custom.id.field=true
sf.Lead.custom.id.field.name=External_Id__c
```

All creates/updates become idempotent upserts keyed on the external ID — this also makes
at-least-once redelivery safe.

## Big Objects

`sf.<obj>.type=big_object` enables Big Object (`__b`) targets. Big Objects are
**insert-only**: records with `_EventType` of `updated`/`deleted` are routed to the DLQ
instead of being written.

## Error handling & DLQ

Per-record failures (Bulk `failedResults` rows, Composite per-record errors, mapping
failures) are reported to Kafka Connect's **errant-record reporter** — configure
`errors.tolerance=all` + `errors.deadletterqueue.topic.name` to land them in a DLQ with
the failure reason. `behavior.on.api.errors` then controls the task:

| Value | Behaviour |
|---|---|
| `fail` (default) | Report failures, then fail the task |
| `log` | Report failures, log a warning, keep going |
| `ignore` | Report failures silently |

## Configuration reference

:::tip Full property reference
Every property with types, defaults, and valid values — generated from the connector's ConfigDef:
[SObject Sink Configuration](../reference/configuration/sobject-sink.md).
:::

Auth properties (`sf.auth.grant.type`, `sf.instance.url`, `sf.consumer.key`, …) are the
same as the [source connector](source.md#connection--auth).

### Global

| Property | Type | Default | Description |
|---|---|---|---|
| `topics` / `topics.regex` | list/string | — | Source Kafka topics |
| `sf.objects` | list | — | Target SObject API names |
| `sf.write.mode` | enum | `auto` | `bulk2`, `rest`, or `auto` (REST at or below the threshold) |
| `sf.write.rest.threshold` | int | `200` | Max records for the REST path in `auto` mode |
| `behavior.on.api.errors` | enum | `fail` | `fail`, `log`, or `ignore` |
| `sf.max.batch.records` | int | `10000` | Records per Bulk ingest job |
| `sf.request.max.retries.time.ms` | long | `30000` | Retry budget |
| `sf.api.version` | string | `60.0` | API version |

### Per-object (`sf.<object>.*`)

| Property | Type | Default | Description |
|---|---|---|---|
| `sf.<obj>.topics` | list | — | Kafka topic(s) feeding this object (a topic maps to exactly one object) |
| `sf.<obj>.operation` | enum | `insert` | `insert`, `update`, `upsert`, or `delete` |
| `sf.<obj>.override.event.type` | boolean | `false` | Ignore `_EventType`; force `sf.<obj>.operation` |
| `sf.<obj>.use.custom.id.field` | boolean | `false` | Use an external ID for upsert/delete |
| `sf.<obj>.custom.id.field.name` | string | — | External ID field API name |
| `sf.<obj>.type` | enum | `standard_custom` | `standard_custom` or `big_object` |
| `sf.<obj>.ignore.fields` | list | — | Record fields to skip |

## Example — cross-org Lead sync

```json
{
  "name": "salesforce-lead-sink",
  "config": {
    "connector.class": "sh.oso.salesforce.sink.SalesforceSinkConnector",
    "tasks.max": "2",
    "topics": "salesforce.Lead",
    "sf.auth.grant.type": "client_credentials",
    "sf.instance.url": "https://target-org.my.salesforce.com",
    "sf.consumer.key": "...",
    "sf.consumer.secret": "...",
    "sf.objects": "Lead",
    "sf.Lead.topics": "salesforce.Lead",
    "sf.Lead.use.custom.id.field": "true",
    "sf.Lead.custom.id.field.name": "External_Id__c",
    "sf.write.mode": "auto",
    "behavior.on.api.errors": "log",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "dlq-salesforce-lead-sink",
    "errors.deadletterqueue.context.headers.enable": "true"
  }
}
```
