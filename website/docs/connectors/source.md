---
title: Source Connector
description: Unified Salesforce source — Bulk 2.0 snapshot, Pub/Sub CDC streaming, periodic polling, and Platform Event streaming.
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
does not affect the others. The same connector can also stream
[Platform Events](#platform-events) listed in `sf.platform.events`. SObjects and platform
events are distributed round-robin across tasks; `tasks.max` above their combined count is
capped.

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

## Platform Events

:::info Available from version 0.2.1
:::

List custom platform events (API names ending in `__e`) in `sf.platform.events` to stream
them over the Pub/Sub API. Each event gets its own subscription, offset and topic,
`{sf.topic.prefix}.{EventName}`. Platform events can share a connector with `sf.sobjects`
or be configured on their own; they always stream over Pub/Sub, whatever
`sf.realtime.mode` says for SObjects.

```json
{
  "name": "salesforce-platform-events",
  "config": {
    "connector.class": "sh.oso.salesforce.source.SalesforceSourceConnector",
    "tasks.max": "1",
    "sf.auth.grant.type": "client_credentials",
    "sf.instance.url": "https://acme.my.salesforce.com",
    "sf.consumer.key": "${file:/secrets/sf.properties:consumer.key}",
    "sf.consumer.secret": "${file:/secrets/sf.properties:consumer.secret}",
    "sf.platform.events": "Order_Shipped__e,Integration_Message__e",
    "sf.topic.prefix": "salesforce",
    "sf.event.start": "latest"
  }
}
```

### Record shape

Each event becomes one record carrying **every field defined on the event, exactly as it
was published**. The connector makes no calls back to Salesforce for platform events.

| Part | Content |
|---|---|
| Key | `null` (platform events are messages, not records) |
| Value | Every field of the event (`CreatedDate` as epoch milliseconds, `CreatedById`, your custom `__c` fields), plus `ReplayId` (the replay position as a decimal string), `EventUuid`, `_ObjectType` (the event API name) and `_EventType` (the Pub/Sub schema ID of the event definition) |
| Headers | None |

This is the shape consumers of Confluent's Platform Event source already read, so they keep
working after a [migration](../migration/confluent.md). For example, with `JsonConverter`
and `schemas.enable=false`:

```json
{
  "CreatedDate": 1767225600000,
  "CreatedById": "005xx000001X8UzAAK",
  "Message_Type__c": "UpsertOpportunity",
  "Payload__c": "{\"recordId\":\"006xx000001AbCdEF\",\"stage\":\"Closed Won\",\"lines\":[...]}",
  "ReplayId": "4093842",
  "EventUuid": "3f2b9c7e-1d4a-4e8b-9a6f-0c5d7e2b1a90",
  "_ObjectType": "Integration_Message__e",
  "_EventType": "Qm7tVx2pLk9sRb4nWc8yDg"
}
```

When an admin changes the event definition (for example adds a field), Salesforce issues a
new schema ID. The connector decodes every event with its own schema, so old and new events
can be mixed in one stream; `_EventType` changes and the new field appears. The Connect
schema keeps the event's name, so Schema Registry sees a new version of the same subject.

### Do Platform Events need hydrating?

No. A platform event is not a pointer to a record: it carries whatever fields the
publisher defines and populates, up to 1 MB per event (long text area fields hold up to
131,072 characters). If your events arrive with only a record ID, the event was *designed*
that way, and there are better fixes than calling Salesforce back for every event:

1. **Publish the data in the event (recommended).** Add the fields consumers need to the
   event definition and populate them in the Flow or Apex that publishes it; a common
   pattern is to serialise the whole business record as JSON into one long text area field.
   This costs no API calls, and the event records the data as it was when the event fired,
   not as it looks when a later lookup runs. Set the event to **Publish After Commit** so
   it is only published once the transaction that changed the data has committed.
2. **Stream record changes with Change Data Capture instead.** If what you need is "this
   record changed", list the object in `sf.sobjects`. CDC update events carry only the
   changed fields; `sf.full.record.on.update=true` makes the connector fetch the full
   record over REST for every update (one API call per update, counted against the daily
   limit).
3. **Look records up downstream.** If the publisher cannot change, enrich the stream in a
   stream processor or service that batches lookups through the REST or Composite API, so
   Salesforce API limits and latency never hold up event delivery.

The connector deliberately does not look up records named in a platform event: it would
spend API quota on every event and could return a newer state than the event saw.

### Offsets, replay and recovery

- The source partition is `{"platformEvent": "<EventName>"}`; the offset holds the replay
  position of the last emitted event, so restarts resume exactly after it.
- `sf.event.start` sets the cold start: `latest` (new events only) or `all` (everything
  Salesforce retains, 72 hours).
- When a stored position has expired, `sf.gap.recovery` applies. Platform events cannot be
  re-queried, so `resync` resubscribes from the earliest retained event, `latest` from the
  newest, and `fail` fails the task and keeps the offset.
- Transient stream errors, expired sessions and missing events or permissions are handled
  as described in [Offsets & Recovery](../concepts/offsets-and-recovery.md).

Delivery is at least once: after a crash, events since the last committed offset are
delivered again. Deduplicate on `EventUuid` where that matters.

### Ordering and keys

With a null key, records spread across the topic's partitions, so ordering holds per
partition only. If consumers need events in order, use a single-partition topic, or key
the records by a business field with the standard SMTs:

```json
"transforms": "key,extract",
"transforms.key.type": "org.apache.kafka.connect.transforms.ValueToKey",
"transforms.key.fields": "Order_Number__c",
"transforms.extract.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
"transforms.extract.field": "Order_Number__c"
```

Each platform event is one Pub/Sub subscription. Events delivered to the connector count
against the org's Pub/Sub API event delivery allocation; if Salesforce reports the
allocation as exhausted (`RESOURCE_EXHAUSTED`), the connector backs off and retries.

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
| `sf.sobjects` | list | — | SObject API names to source (exact casing); required unless `sf.platform.events` is set |
| `sf.platform.events` | list | — | Platform event API names to stream (each ending in `__e`); see [Platform Events](#platform-events) |
| `sf.sobjects.max` | int | `5` | Maximum SObjects per connector instance |
| `sf.topic.prefix` | string | — | Topics are `{prefix}.{SObject}` and `{prefix}.{EventName}` |

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
