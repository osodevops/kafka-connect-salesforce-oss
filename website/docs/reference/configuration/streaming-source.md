---
title: "Streaming Source Configuration"
description: "Every configuration property of the legacy CometD streaming source connector."
sidebar_label: "Streaming Source"
sidebar_position: 4
---

<!-- GENERATED from the connector's ConfigDef by ConfigDocsGeneratorTest.
     Do not edit by hand: change the ConfigDef documentation instead. -->

# Streaming Source Configuration

`sh.oso.salesforce.streaming.SalesforceStreamingSourceConnector`

Property-by-property reference, generated from the connector's `ConfigDef` — guaranteed to match the release you are running. Usage guide: [connector documentation](/connectors/streaming-source).

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

### `sf.channel.type`

Streaming channel type.

- **Type:** string
- **Default:** `cdc`
- **Valid values:** [pushtopic, cdc, platform_event]
- **Importance:** high

### `kafka.topic`

Target Kafka topic; may reference $&#123;_EventType&#125; and $&#123;_ObjectType&#125;.

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

API version (&gt;= 37.0 for durable replay).

- **Type:** string
- **Default:** `60.0`
- **Importance:** medium

### `sf.object`

SObject backing an auto-created PushTopic.

- **Type:** string
- **Default:** null
- **Importance:** medium

### `sf.pushtopic.name`

PushTopic name.

- **Type:** string
- **Default:** null
- **Importance:** medium

### `sf.pushtopic.create`

Auto-create the PushTopic from the sf.object describe when missing.

- **Type:** boolean
- **Default:** `true`
- **Importance:** medium

### `sf.cdc.channel`

CDC channel: ChangeEvents, &lt;Entity&gt;ChangeEvent, or a custom __chn channel.

- **Type:** string
- **Default:** `ChangeEvents`
- **Importance:** medium

### `sf.platform.event.name`

Platform event API name (…__e).

- **Type:** string
- **Default:** null
- **Importance:** medium

### `sf.event.start`

Cold start: latest (replayId -1) or all retained events (replayId -2).

- **Type:** string
- **Default:** `latest`
- **Valid values:** [latest, all]
- **Importance:** medium

### `sf.invalid.replay.behaviour`

Recovery when a stored replayId has expired or is invalid.

- **Type:** string
- **Default:** `all`
- **Valid values:** [all, latest]
- **Importance:** medium

### `sf.request.max.retries.time.ms`

Retry time budget.

- **Type:** long
- **Default:** `900000`
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

### `sf.pushtopic.notify.create`

Notify on create.

- **Type:** boolean
- **Default:** `true`
- **Importance:** low

### `sf.pushtopic.notify.update`

Notify on update.

- **Type:** boolean
- **Default:** `true`
- **Importance:** low

### `sf.pushtopic.notify.delete`

Notify on delete.

- **Type:** boolean
- **Default:** `true`
- **Importance:** low

### `sf.pushtopic.notify.undelete`

Notify on undelete.

- **Type:** boolean
- **Default:** `true`
- **Importance:** low

### `sf.channel.entities`

Entity filter for multi-entity CDC channels.

- **Type:** list
- **Default:** "" (empty)
- **Importance:** low

### `sf.connection.timeout.ms`

CometD connect timeout.

- **Type:** long
- **Default:** `30000`
- **Importance:** low

### `sf.token.endpoint`

OAuth token endpoint override (testing only).

- **Type:** string
- **Default:** null
- **Importance:** low

### `sf.cometd.endpoint`

CometD endpoint override (testing only); default &#123;instanceUrl&#125;/cometd/&#123;version&#125;/.

- **Type:** string
- **Default:** null
- **Importance:** low
