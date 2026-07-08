---
title: Source Connector
description: Unified Salesforce source — Bulk 2.0 snapshot, Pub/Sub CDC streaming, and periodic polling.
sidebar_position: 1
---

# Salesforce Source Connector

`sh.oso.salesforce.source.SalesforceSourceConnector`

Streams Salesforce data into Kafka with three ingestion modes per SObject:

| Mode | API | Behaviour |
|---|---|---|
| **Historical snapshot** | Bulk API 2.0 query | Optional one-time initial load before the real-time mode starts |
| **Event-driven sync** (default) | Pub/Sub API `Subscribe` on CDC channels | Real-time create/update/delete/undelete with replayId checkpointing |
| **Periodic polling** | Bulk API 2.0 query | `SystemModstamp`-cursor polling for orgs/objects without CDC |

Each SObject in `sf.sobjects` gets its own Kafka topic (`{sf.topic.prefix}.{SObject}`),
its own offset state, schema cache, and Pub/Sub subscription — a failure on one object
does not affect the others. SObjects are distributed round-robin across tasks; `tasks.max`
above the SObject count is capped.

## Record shape

- **Key**: the Salesforce record Id (string).
- **Value**: a Struct with the object's fields plus `_EventType`
  (`created`/`updated`/`deleted`) and `_ObjectType` for compatibility with consumers of
  Confluent's connectors and with the [SObject sink](sobject-sink.md).
- **Headers** (event-driven mode): `sf.change.type`, `sf.entity`, `sf.commit.timestamp`,
  `sf.commit.number`, `sf.transaction.key`, `sf.changed.fields`, `sf.nulled.fields`
  (CDC bitmap fields decoded to field names).
- With `sf.emit.tombstone.on.delete=true`, every DELETE additionally emits a tombstone
  (null-value) record for log-compacted topics.

## Snapshot → stream seamless handoff

When both snapshot and event-driven mode are enabled the connector guarantees **no gap
and no duplicates** across the boundary:

1. Before the snapshot starts it opens a probe subscription with `replay_preset=LATEST`
   and records the current replayId.
2. The Bulk 2.0 snapshot runs to completion; the completion timestamp is recorded.
3. The live subscription starts from the probed replayId (`CUSTOM`), so every change that
   happened *during* the snapshot is replayed.
4. Replayed events whose commit timestamp precedes the snapshot completion are dropped —
   the snapshot already delivered their post-image.

## Gap and overflow recovery

CDC gap events (`GAP_CREATE`/`GAP_UPDATE`/…) carry no field data. The connector
reconstructs affected records by REST re-query. `GAP_OVERFLOW` (too many changes in one
transaction) and expired replayIds apply the `sf.gap.recovery` strategy:

| Strategy | Behaviour |
|---|---|
| `resync` (default) | Incremental Bulk 2.0 re-sync from the last watermark minus `sf.gap.resync.buffer.ms` |
| `latest` | Skip and resubscribe from newest (data loss accepted) |
| `fail` | Stop the task |

## Configuration reference

:::tip Full property reference
Every property with types, defaults, and valid values — generated from the connector's ConfigDef:
[Source Connector Configuration](../reference/configuration/source.md).
:::

### Connection & auth

| Property | Type | Default | Description |
|---|---|---|---|
| `sf.auth.grant.type` | enum | `client_credentials` | `client_credentials`, `jwt_bearer`, or `password` (legacy) |
| `sf.instance.url` | string | — | The org's My Domain URL |
| `sf.consumer.key` | password | — | Connected App consumer key |
| `sf.consumer.secret` | password | — | Consumer secret (client_credentials/password) |
| `sf.username` | string | — | Username (jwt_bearer/password) |
| `sf.password` / `sf.password.token` | password | — | Legacy password flow credentials |
| `sf.jwt.keystore.path` | string | — | RS256 signing key: PEM (PKCS#8), JKS, or PKCS12 |
| `sf.jwt.keystore.password` | password | — | Keystore password |
| `sf.jwt.audience` | string | `https://login.salesforce.com` | JWT `aud` claim (`https://test.salesforce.com` for sandboxes) |
| `sf.api.version` | string | `60.0` | Salesforce API version |

### Objects & topics

| Property | Type | Default | Description |
|---|---|---|---|
| `sf.sobjects` | list | — | SObject API names to source (exact casing) |
| `sf.sobjects.max` | int | `5` | Maximum SObjects per connector instance |
| `sf.topic.prefix` | string | — | Topics are `{prefix}.{SObject}` |

### Modes & behaviour

| Property | Type | Default | Description |
|---|---|---|---|
| `sf.snapshot.enabled` | boolean | `true` | Run a historical Bulk 2.0 snapshot first |
| `sf.snapshot.since` | date | — | Snapshot lower bound on `CreatedDate` (default: all records) |
| `sf.realtime.mode` | enum | `event_driven` | `event_driven` or `polling` |
| `sf.event.start` | enum | `latest` | Cold-start replay: `latest` (new events) or `all` (72h retention) |
| `sf.gap.recovery` | enum | `resync` | `resync`, `latest`, or `fail` |
| `sf.gap.resync.buffer.ms` | long | `60000` | Safety buffer subtracted from the resync watermark |
| `sf.full.record.on.update` | boolean | `false` | Fetch the full post-image via REST on every UPDATE (extra API call per update) |
| `sf.emit.tombstone.on.delete` | boolean | `false` | Emit a tombstone record on DELETE (event-driven only) |
| `sf.include.deleted` | boolean | `false` | Polling mode: include soft-deleted records via `queryAll` |
| `sf.poll.interval.ms` | int | `30000` | Polling interval (minimum 8700) |
| `sf.result.max.rows` | int | `1000` | Bulk result rows fetched per request |
| `sf.skip.epoch.conversion.fields` | list | — | Date/datetime fields to keep as ISO-8601 strings |
| `sf.pubsub.batch.size` | int | `100` | Pub/Sub flow-control batch size (`num_requested`, max 100) |
| `sf.request.max.retries.time.ms` | long | `30000` | Total retry-time budget for transient failures |

### Endpoint overrides (testing / proxies)

| Property | Default | Description |
|---|---|---|
| `sf.pubsub.endpoint` | `api.pubsub.salesforce.com:443` | Pub/Sub API endpoint |
| `sf.pubsub.plaintext` | `false` | Plaintext gRPC (testing only) |
| `sf.token.endpoint` | — | OAuth token endpoint override (testing only) |

## Example

```json
{
  "name": "salesforce-source",
  "config": {
    "connector.class": "sh.oso.salesforce.source.SalesforceSourceConnector",
    "tasks.max": "3",
    "sf.auth.grant.type": "jwt_bearer",
    "sf.instance.url": "https://acme.my.salesforce.com",
    "sf.consumer.key": "${file:/secrets/sf.properties:consumer.key}",
    "sf.username": "integration@acme.com",
    "sf.jwt.keystore.path": "/secrets/sf-key.pem",
    "sf.sobjects": "Account,Contact,Opportunity",
    "sf.topic.prefix": "salesforce",
    "sf.snapshot.enabled": "true",
    "sf.snapshot.since": "2024-01-01",
    "sf.emit.tombstone.on.delete": "true",
    "sf.gap.recovery": "resync"
  }
}
```
