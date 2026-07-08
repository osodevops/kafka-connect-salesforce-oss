---
title: Migrating from Confluent
description: Config mapping from Confluent's Salesforce connectors to the OSS suite.
sidebar_position: 1
---

# Migrating from Confluent

The suite reaches functional parity with Confluent's eleven Salesforce connectors using
four, and was specified clean-room from public documentation — the config namespace is
deliberately different (`sf.*`). This page maps the common properties.

## Which connector replaces what

| Confluent connector | Replacement | Notes |
|---|---|---|
| Salesforce CDC Source | [Source](../connectors/source.md) (event-driven mode) | Pub/Sub transport instead of CometD |
| Salesforce Platform Event Source | [Source](../connectors/source.md) (event topics) | |
| Salesforce Bulk API 1.0 / 2.0 Source | [Source](../connectors/source.md) (snapshot/polling modes) | Bulk 2.0 only |
| Salesforce Source V2 | [Source](../connectors/source.md) | Same architecture: Pub/Sub + Bulk 2.0 |
| Salesforce PushTopic Source | [Streaming source](../connectors/streaming-source.md) or Source | PushTopics only exist on CometD |
| Salesforce SObject Sink | [SObject sink](../connectors/sobject-sink.md) | |
| Salesforce Bulk API 1.0 / 2.0 Sink | [SObject sink](../connectors/sobject-sink.md) | |
| Salesforce Platform Event Sink | [Platform Event sink](../connectors/platform-event-sink.md) | |

## Config mapping — source (Source V2 / CDC / Bulk)

| Confluent | OSS |
|---|---|
| `salesforce.grant.type` | `sf.auth.grant.type` |
| `salesforce.instance` | `sf.instance.url` |
| `salesforce.consumer.key` / `.secret` | `sf.consumer.key` / `sf.consumer.secret` |
| `salesforce.username` / `.password` (+ token) | `sf.username` / `sf.password` / `sf.password.token` |
| `sobject.names` | `sf.sobjects` |
| `topic.prefix` | `sf.topic.prefix` |
| `historical.snapshot` | `sf.snapshot.enabled` |
| `real.time.ingestion.mode` | `sf.realtime.mode` |
| `event.sync.start.point` | `sf.event.start` |
| `gap.event.recovery` | `sf.gap.recovery` |
| `cdc.full.record.on.update` | `sf.full.record.on.update` |
| `emit.tombstone.on.delete` | `sf.emit.tombstone.on.delete` |
| `poll.interval.ms` | `sf.poll.interval.ms` |
| `request.max.retries.time.ms` | `sf.request.max.retries.time.ms` |

## Config mapping — SObject / Bulk sinks

| Confluent | OSS |
|---|---|
| `salesforce.object` | `sf.objects` (+ `sf.<obj>.topics`) |
| `override.event.type` + `salesforce.sink.object.operation` | `sf.<obj>.override.event.type` + `sf.<obj>.operation` |
| `salesforce.use.custom.id.field` | `sf.<obj>.use.custom.id.field` |
| `salesforce.custom.id.field.name` | `sf.<obj>.custom.id.field.name` |
| `salesforce.ignore.fields` | `sf.<obj>.ignore.fields` |
| `behavior.on.api.errors` | `behavior.on.api.errors` (same) |
| reporter topics (`reporter.result.topic.*`) | Connect-native DLQ (`errors.deadletterqueue.*`) |

## Config mapping — Platform Event sink

| Confluent | OSS |
|---|---|
| `salesforce.platform.event.name` | `sf.platform.event.name` |
| `behavior.on.api.errors` | `behavior.on.api.errors` |

## Behavioural differences to review

- **Record shape is compatible**: values carry `_EventType`/`_ObjectType` like
  Confluent's connectors, so existing consumers and sink pipelines keep working. CDC
  metadata additionally arrives in `sf.*` record headers.
- **Offsets don't transfer.** Plan a cutover: either accept a replay window (start the
  OSS source with `sf.event.start=all` inside 72h of stopping the old connector), or run
  a fresh snapshot.
- **DLQ instead of reporter topics.** Success/error reporter topics are replaced by
  Kafka Connect's native errant-record reporter.
- **Bulk API 1.0 is not implemented** (retired by Salesforce); Bulk 2.0 covers those
  workloads.
- **CSFLE** (client-side field-level encryption) is not yet supported.
