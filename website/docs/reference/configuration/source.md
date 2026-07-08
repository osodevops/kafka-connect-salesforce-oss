---
title: "Source Connector Configuration"
description: "Every configuration property of the Salesforce source connector, with types, defaults, and valid values."
sidebar_label: "Source Connector"
sidebar_position: 1
---

<!-- GENERATED from the connector's ConfigDef by ConfigDocsGeneratorTest.
     Do not edit by hand: change the ConfigDef documentation instead. -->

# Source Connector Configuration

`sh.oso.salesforce.source.SalesforceSourceConnector`

Property-by-property reference, generated from the connector's `ConfigDef` — guaranteed to match the release you are running. Usage guide: [connector documentation](/connectors/source).

Framework properties (`name`, `tasks.max`, converters, `errors.*`) follow the
standard [Kafka Connect worker/connector configuration](https://kafka.apache.org/documentation/#connectconfigs).
Topics are derived per SObject as `{sf.topic.prefix}.{SObject}` — there is no `topics` property on the source.

## High importance

### `sf.auth.grant.type`

OAuth flow: client_credentials, jwt_bearer, or password (legacy).

- **Type:** string
- **Default:** `client_credentials`
- **Valid values:** [client_credentials, jwt_bearer, password]
- **Importance:** high

### `sf.instance.url`

The org's My Domain URL, e.g. https://acme.my.salesforce.com.

- **Type:** string
- **Default:** null
- **Importance:** high

### `sf.consumer.key`

Connected app consumer key.

- **Type:** password
- **Default:** null
- **Importance:** high

### `sf.consumer.secret`

Connected app consumer secret.

- **Type:** password
- **Default:** null
- **Importance:** high

### `sf.sobjects`

SObject API names to source (exact casing).

- **Type:** list
- **Default:** none — **required**
- **Importance:** high

### `sf.topic.prefix`

Kafka topics are &#123;prefix&#125;.&#123;SObject&#125;.

- **Type:** string
- **Default:** none — **required**
- **Importance:** high

### `sf.realtime.mode`

Continuous ingestion mode after the snapshot.

- **Type:** string
- **Default:** `event_driven`
- **Valid values:** [event_driven, polling]
- **Importance:** high

## Medium importance

### `sf.username`

Username (jwt_bearer/password flows).

- **Type:** string
- **Default:** null
- **Importance:** medium

### `sf.jwt.keystore.path`

Path to the RS256 signing key: PEM (PKCS#8), JKS, or PKCS12.

- **Type:** string
- **Default:** null
- **Importance:** medium

### `sf.jwt.keystore.password`

Keystore password.

- **Type:** password
- **Default:** null
- **Importance:** medium

### `sf.api.version`

Salesforce API version.

- **Type:** string
- **Default:** `60.0`
- **Importance:** medium

### `sf.snapshot.enabled`

Run a historical Bulk 2.0 snapshot before the real-time mode.

- **Type:** boolean
- **Default:** `true`
- **Importance:** medium

### `sf.snapshot.since`

Snapshot lower bound on CreatedDate (ISO-8601 date or datetime). Default: all records.

- **Type:** string
- **Default:** null
- **Importance:** medium

### `sf.event.start`

Cold-start replay position for event_driven mode.

- **Type:** string
- **Default:** `latest`
- **Valid values:** [latest, all]
- **Importance:** medium

### `sf.gap.recovery`

Behaviour on gap/overflow events or replay loss.

- **Type:** string
- **Default:** `resync`
- **Valid values:** [resync, latest, fail]
- **Importance:** medium

### `sf.emit.tombstone.on.delete`

Emit a tombstone (null value) record on DELETE in event_driven mode.

- **Type:** boolean
- **Default:** `false`
- **Importance:** medium

### `sf.poll.interval.ms`

Polling-mode interval in milliseconds.

- **Type:** int
- **Default:** `30000`
- **Valid values:** [8700,...]
- **Importance:** medium

### `sf.request.max.retries.time.ms`

Total time budget for retrying transient Salesforce failures.

- **Type:** long
- **Default:** `30000`
- **Importance:** medium

## Low importance

### `sf.password`

Password (legacy password flow).

- **Type:** password
- **Default:** null
- **Importance:** low

### `sf.password.token`

Security token appended to the password (legacy password flow).

- **Type:** password
- **Default:** null
- **Importance:** low

### `sf.jwt.audience`

JWT aud claim; defaults to https://login.salesforce.com.

- **Type:** string
- **Default:** null
- **Importance:** low

### `sf.sobjects.max`

Maximum SObjects per connector instance.

- **Type:** int
- **Default:** `5`
- **Valid values:** [1,...]
- **Importance:** low

### `sf.gap.resync.buffer.ms`

Safety buffer subtracted from the watermark on gap resync.

- **Type:** long
- **Default:** `60000`
- **Importance:** low

### `sf.full.record.on.update`

Fetch the full post-image via REST on every UPDATE (one extra API call per update).

- **Type:** boolean
- **Default:** `false`
- **Importance:** low

### `sf.include.deleted`

Polling mode: include soft-deleted records via queryAll.

- **Type:** boolean
- **Default:** `false`
- **Importance:** low

### `sf.result.max.rows`

Maximum Bulk result rows fetched per request.

- **Type:** int
- **Default:** `1000`
- **Valid values:** [1,...]
- **Importance:** low

### `sf.skip.epoch.conversion.fields`

Date/datetime fields to keep as ISO-8601 strings instead of epoch millis.

- **Type:** list
- **Default:** "" (empty)
- **Importance:** low

### `sf.pubsub.batch.size`

Pub/Sub Subscribe flow-control batch size (num_requested).

- **Type:** int
- **Default:** `100`
- **Valid values:** [1,...,100]
- **Importance:** low

### `sf.pubsub.endpoint`

Pub/Sub API endpoint host:port.

- **Type:** string
- **Default:** `api.pubsub.salesforce.com:443`
- **Importance:** low

### `sf.pubsub.plaintext`

Use plaintext gRPC (testing only).

- **Type:** boolean
- **Default:** `false`
- **Importance:** low

### `sf.token.endpoint`

OAuth token endpoint override (testing only).

- **Type:** string
- **Default:** null
- **Importance:** low
