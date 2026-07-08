---
title: "Platform Event Sink Configuration"
description: "Every configuration property of the Salesforce Platform Event sink connector."
sidebar_label: "Platform Event Sink"
sidebar_position: 3
---

<!-- GENERATED from the connector's ConfigDef by ConfigDocsGeneratorTest.
     Do not edit by hand: change the ConfigDef documentation instead. -->

# Platform Event Sink Configuration

`sh.oso.salesforce.pesink.SalesforcePlatformEventSinkConnector`

Property-by-property reference, generated from the connector's `ConfigDef` — guaranteed to match the release you are running. Usage guide: [connector documentation](/connectors/platform-event-sink).

Select input with the standard framework properties `topics` or `topics.regex`.

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

### `sf.platform.event.name`

Target platform event API name (…__e); must already exist in Salesforce.

- **Type:** string
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

### `sf.publish.mode`

Publish via the Pub/Sub API (preferred) or REST.

- **Type:** string
- **Default:** `pubsub`
- **Valid values:** [pubsub, rest]
- **Importance:** medium

### `behavior.on.api.errors`

Per-event publish failures: fail the task, log and continue, or ignore.

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
