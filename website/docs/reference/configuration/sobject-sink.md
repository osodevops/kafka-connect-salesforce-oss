---
title: "SObject Sink Configuration"
description: "Every configuration property of the Salesforce SObject sink connector, including the per-object sf.<object>.* keys."
sidebar_label: "SObject Sink"
sidebar_position: 2
---

<!-- GENERATED from the connector's ConfigDef by ConfigDocsGeneratorTest.
     Do not edit by hand: change the ConfigDef documentation instead. -->

# SObject Sink Configuration

`sh.oso.salesforce.sink.SalesforceSinkConnector`

Property-by-property reference, generated from the connector's `ConfigDef` — guaranteed to match the release you are running. Usage guide: [connector documentation](/connectors/sobject-sink).

Select input with the standard framework properties `topics` or `topics.regex`.
For dead-letter queues use `errors.tolerance=all` with `errors.deadletterqueue.topic.name`.

## High importance

### `sf.auth.grant.type`

OAuth flow.

- **Type:** string
- **Default:** `client_credentials`
- **Valid values:** [client_credentials, jwt_bearer, password]
- **Importance:** high

### `sf.instance.url`

My Domain URL.

- **Type:** string
- **Default:** null
- **Importance:** high

### `sf.consumer.key`

Consumer key.

- **Type:** password
- **Default:** null
- **Importance:** high

### `sf.consumer.secret`

Consumer secret.

- **Type:** password
- **Default:** null
- **Importance:** high

### `sf.objects`

Target SObject API names; map topics via sf.&lt;object&gt;.topics.

- **Type:** list
- **Default:** none — **required**
- **Importance:** high

## Medium importance

### `sf.username`

Username (jwt_bearer/password).

- **Type:** string
- **Default:** null
- **Importance:** medium

### `sf.jwt.keystore.path`

RS256 key path.

- **Type:** string
- **Default:** null
- **Importance:** medium

### `sf.jwt.keystore.password`

Keystore password.

- **Type:** password
- **Default:** null
- **Importance:** medium

### `sf.api.version`

API version.

- **Type:** string
- **Default:** `60.0`
- **Importance:** medium

### `sf.write.mode`

bulk2 (Bulk API 2.0), rest (Composite), or auto (rest under the threshold).

- **Type:** string
- **Default:** `auto`
- **Valid values:** [bulk2, rest, auto]
- **Importance:** medium

### `behavior.on.api.errors`

Per-record API failures: fail the task, log and continue, or ignore.

- **Type:** string
- **Default:** `fail`
- **Valid values:** [fail, log, ignore]
- **Importance:** medium

### `sf.request.max.retries.time.ms`

Retry time budget.

- **Type:** long
- **Default:** `30000`
- **Importance:** medium

## Low importance

### `sf.password`

Password (legacy flow).

- **Type:** password
- **Default:** null
- **Importance:** low

### `sf.password.token`

Security token (legacy flow).

- **Type:** password
- **Default:** null
- **Importance:** low

### `sf.jwt.audience`

JWT aud claim.

- **Type:** string
- **Default:** null
- **Importance:** low

### `sf.write.rest.threshold`

Max records for the REST path in auto mode.

- **Type:** int
- **Default:** `200`
- **Valid values:** [1,...,200]
- **Importance:** low

### `sf.max.batch.records`

Records per Bulk ingest job.

- **Type:** int
- **Default:** `10000`
- **Valid values:** [1,...]
- **Importance:** low

### `sf.token.endpoint`

OAuth token endpoint override (testing only).

- **Type:** string
- **Default:** null
- **Importance:** low

## Per-object properties (`sf.<object>.*`)

Each SObject listed in `sf.objects` is configured with dynamic keys, where `<object>`
is the exact API name (e.g. `sf.Lead.topics`). These are read from the raw connector
config and therefore do not appear in `config()` validation output.

### `sf.<object>.topics`

Kafka topic(s) feeding this object. Each topic maps to exactly one SObject.

- **Type:** list
- **Default:** none — required when more than one object is configured

### `sf.<object>.operation`

Write operation used when `_EventType` is absent or overridden: `insert`, `update`,
`upsert`, or `delete`.

- **Type:** string
- **Default:** `insert`

### `sf.<object>.override.event.type`

Ignore the record's `_EventType` field and always apply `sf.<object>.operation`.

- **Type:** boolean
- **Default:** `false`

### `sf.<object>.use.custom.id.field`

Address records by an external ID field instead of the Salesforce `Id` — enables
idempotent cross-org upserts and deletes.

- **Type:** boolean
- **Default:** `false`

### `sf.<object>.custom.id.field.name`

API name of the external ID field (must be marked as an External ID in the target org).

- **Type:** string
- **Default:** none — required when `sf.<object>.use.custom.id.field=true`

### `sf.<object>.type`

`standard_custom` for regular objects, or `big_object` for Big Objects (`__b`).
Big Objects are insert-only; update/delete records are routed to the DLQ.

- **Type:** string
- **Default:** `standard_custom`

### `sf.<object>.ignore.fields`

Comma-separated record fields to exclude from writes.

- **Type:** list
- **Default:** "" (empty)
