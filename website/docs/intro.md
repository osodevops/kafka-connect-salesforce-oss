---
title: Introduction
description: Open-source, Pub/Sub API-first Salesforce connectors for Apache Kafka Connect.
slug: /
sidebar_position: 1
---

# Kafka Connect Salesforce

**OSO Kafka Connect Salesforce** is an open-source (Apache-2.0) suite of Salesforce
connectors for Apache Kafka Connect. It is a modern, **Pub/Sub API-first** alternative to
Confluent's proprietary Salesforce connectors — four connectors deliver functional parity
with Confluent's eleven, built clean-room from Salesforce's public APIs.

## The connectors

| Connector | Direction | Salesforce APIs | Replaces (Confluent) |
|---|---|---|---|
| [Source](connectors/source.md) | Salesforce → Kafka | Pub/Sub API (gRPC) + Bulk API 2.0 + REST | PushTopic Source, CDC Source, Platform Event Source, Bulk API 1.0/2.0 Source, Source V2 |
| [SObject Sink](connectors/sobject-sink.md) | Kafka → Salesforce | Bulk API 2.0 + REST Composite | SObject Sink, Bulk API 1.0/2.0 Sink |
| [Platform Event Sink](connectors/platform-event-sink.md) | Kafka → Salesforce | Pub/Sub API Publish (+ REST) | Platform Event Sink |
| [Streaming Source (legacy)](connectors/streaming-source.md) | Salesforce → Kafka | Streaming API (CometD/Bayeux) | PushTopic Source on orgs without Pub/Sub API |

## Why Pub/Sub API first?

Salesforce recommends the Pub/Sub API (gRPC + Avro) over the retiring CometD Streaming
API, and Confluent's own newest connector (Source V2) made the same move. No other
maintained open-source connector uses it. The Pub/Sub API gives you:

- A single interface for subscribe, publish, and schema retrieval
- Efficient binary Avro over HTTP/2 with pull-based flow control
- Durable replay within the 72-hour event retention window

## Highlights

- **Snapshot → stream seamless handoff** — bootstrap topics with a historical Bulk API 2.0
  load, then transition to live Change Data Capture with no gap and no duplicates.
- **Gap recovery** — CDC gap/overflow events trigger targeted REST re-query or incremental
  Bulk resync instead of silent data loss.
- **At-least-once everywhere**, with replayIds and Bulk cursors checkpointed in Kafka
  Connect's offset store.
- **External-ID upsert** for idempotent cross-org sync.
- **Quota-aware** — a rate governor polls `/limits/` and backs off before your 24-hour API
  pool is exhausted.
- **Fully testable locally** — the repo ships a fake Salesforce (HTTP + gRPC + CometD) so
  every behaviour is verified without a Salesforce org.

## Where to go next

- **New here?** Start with [Getting Started](getting-started/index.md).
- **Configuring a connector?** Jump to the per-connector reference under *Connectors*.
- **Coming from Confluent?** See [Migrating from Confluent](migration/confluent.md).
- **Contributing?** See [Building](development/building.md) and
  [Local testing](development/local-testing.md).
