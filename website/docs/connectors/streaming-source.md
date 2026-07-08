---
title: Streaming Source (Legacy CometD)
description: PushTopic, CDC, and Platform Event streaming over the legacy Streaming API for orgs without Pub/Sub access.
sidebar_position: 4
---

# Legacy Streaming Source Connector

`sh.oso.salesforce.streaming.SalesforceStreamingSourceConnector`

A compatibility fallback using the legacy **Streaming API (CometD/Bayeux over HTTP
long-polling)** for orgs where the Pub/Sub API is unavailable (e.g. Government Cloud).
For everything else, prefer the [unified source connector](source.md).

Supports one subscription per connector (`tasks.max` is effectively 1) on any of the
three channel types:

| `sf.channel.type` | Channel | Notes |
|---|---|---|
| `pushtopic` | `/topic/<name>` | Auto-creates the PushTopic from the object describe when missing |
| `cdc` (default) | `/data/<channel>` | `ChangeEvents` (all entities), `<Entity>ChangeEvent`, or a custom `__chn` channel |
| `platform_event` | `/event/<name>__e` | Event must pre-exist |

## Replay & checkpointing

Events carry a numeric `replayId` that the connector checkpoints in Kafka Connect's
offset store. On restart it resubscribes with the stored replayId (durable streaming,
API v37.0+). Cold starts use `sf.event.start`: `latest` (replayId −1) or `all`
(replayId −2, everything retained — 24h for PushTopics, 72h for CDC/platform events).

When Salesforce rejects a stored replayId as expired/invalid, the connector resubscribes
according to `sf.invalid.replay.behaviour` (`all` or `latest`).

## Record shape

Values are schemaless maps with `_EventType` and `_ObjectType` added. The target topic
comes from `kafka.topic`, which may reference `${_EventType}` and `${_ObjectType}` —
e.g. `kafka.topic=sf.${_ObjectType}`.

## Configuration reference

:::tip Full property reference
[Streaming Source Configuration](../reference/configuration/streaming-source.md) — generated from the connector's ConfigDef.
:::

Auth properties are shared with the other connectors — see the
[source connector](source.md#connection--auth).

| Property | Type | Default | Description |
|---|---|---|---|
| `sf.channel.type` | enum | `cdc` | `pushtopic`, `cdc`, or `platform_event` |
| `sf.object` | string | — | SObject backing an auto-created PushTopic |
| `sf.pushtopic.name` | string | — | PushTopic name (required for `pushtopic`) |
| `sf.pushtopic.create` | boolean | `true` | Auto-create the PushTopic when missing |
| `sf.pushtopic.notify.create` | boolean | `true` | Notify on create |
| `sf.pushtopic.notify.update` | boolean | `true` | Notify on update |
| `sf.pushtopic.notify.delete` | boolean | `true` | Notify on delete |
| `sf.pushtopic.notify.undelete` | boolean | `true` | Notify on undelete |
| `sf.cdc.channel` | string | `ChangeEvents` | CDC channel name |
| `sf.channel.entities` | list | — | Entity filter for multi-entity CDC channels |
| `sf.platform.event.name` | string | — | Platform event API name (`.*__e`) |
| `sf.event.start` | enum | `latest` | `latest` (−1) or `all` (−2) |
| `sf.invalid.replay.behaviour` | enum | `all` | Recovery when the stored replayId expired |
| `kafka.topic` | string | — | Target topic template |
| `sf.connection.timeout.ms` | long | `30000` | CometD handshake/subscribe timeout |
| `sf.request.max.retries.time.ms` | long | `900000` | Retry budget |

## Example — PushTopic

```json
{
  "name": "salesforce-streaming",
  "config": {
    "connector.class": "sh.oso.salesforce.streaming.SalesforceStreamingSourceConnector",
    "sf.auth.grant.type": "client_credentials",
    "sf.instance.url": "https://acme.my.salesforce.com",
    "sf.consumer.key": "...",
    "sf.consumer.secret": "...",
    "sf.channel.type": "pushtopic",
    "sf.object": "Account",
    "sf.pushtopic.name": "AccountUpdates",
    "kafka.topic": "sf.streaming.accounts",
    "sf.event.start": "all"
  }
}
```
