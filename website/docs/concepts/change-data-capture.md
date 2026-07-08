---
title: "Salesforce Change Data Capture (CDC) to Kafka"
description: "How Salesforce Change Data Capture works and how to stream CDC events into Apache Kafka: channels, ChangeEventHeader, gap events, retention, and connector setup."
slug: /salesforce-change-data-capture
sidebar_label: "Change Data Capture"
keywords:
  - salesforce change data capture
  - salesforce cdc kafka
  - change data capture kafka
  - salesforce cdc
  - changeeventheader
sidebar_position: 5
---

# Salesforce Change Data Capture (CDC) to Kafka

**Salesforce Change Data Capture (CDC)** publishes a change event to the Salesforce event
bus every time a record is created, updated, deleted, or undeleted. Streaming those
events into Apache Kafka gives you a near-real-time replica of your CRM data for
analytics, microservices, caches, and search indexes — without polling the REST API.

This page explains how CDC works and how the
[Salesforce Kafka connector](../connectors/source.md) consumes it.

## How Salesforce CDC works

1. You enable CDC per object under **Setup → Change Data Capture** (standard and custom
   objects).
2. Every committed DML change publishes a **change event** to a channel:
   - `/data/<Entity>ChangeEvent` — one entity (e.g. `/data/AccountChangeEvent`)
   - `/data/ChangeEvents` — all selected entities combined
   - `/data/<Name>__chn` — a custom channel you compose
3. Subscribers consume the channel via the **Pub/Sub API** (gRPC + Avro, recommended) or
   the legacy Streaming API (CometD).
4. Events are retained in the event bus for **72 hours**; each carries an opaque
   `replayId` so a subscriber can resume exactly where it stopped.

## Anatomy of a change event

Every event contains the changed fields plus a `ChangeEventHeader`:

| Header field | Meaning |
|---|---|
| `changeType` | `CREATE`, `UPDATE`, `DELETE`, `UNDELETE` (+ `GAP_*`, `GAP_OVERFLOW`) |
| `entityName` | The SObject, e.g. `Account` |
| `recordIds` | Affected record Ids (bulk DML can carry several) |
| `commitTimestamp` / `commitNumber` | Transaction commit position |
| `transactionKey` | Groups events from one transaction |
| `changedFields` / `nulledFields` | Bitmap-encoded field lists (the connector decodes these to names) |

Two behaviours surprise most first-time consumers:

- **UPDATE events carry only the changed fields** — everything else is null. Consumers
  either merge deltas, or the connector can fetch the full post-image per update
  (`sf.full.record.on.update=true`, at the cost of one REST call per update).
- **Gap events carry no field data at all.** When Salesforce can't render a normal event
  (oversized events, internal errors, direct database changes), it sends a `GAP_*` event
  with just the header. The connector automatically re-queries those records via REST and
  re-emits them; `GAP_OVERFLOW` triggers an incremental Bulk re-sync.

## Streaming CDC into Kafka

With the [source connector](../connectors/source.md), each object's change stream lands
on its own Kafka topic with the record Id as the key:

```json
{
  "connector.class": "sh.oso.salesforce.source.SalesforceSourceConnector",
  "sf.sobjects": "Account,Contact,Opportunity",
  "sf.topic.prefix": "salesforce",
  "sf.snapshot.enabled": "true",
  "sf.realtime.mode": "event_driven"
}
```

- `salesforce.Account`, `salesforce.Contact`, … receive ordered change records with
  `_EventType` (`created`/`updated`/`deleted`) and decoded CDC metadata in headers.
- Enable `sf.snapshot.enabled` to backfill history first — the connector's
  [snapshot handoff](../connectors/source.md#snapshot--stream-seamless-handoff)
  guarantees no gap and no duplicates between the backfill and the live stream.
- Enable `sf.emit.tombstone.on.delete` for log-compacted topics that should forget
  deleted records.

## CDC vs the alternatives

| Approach | Latency | API cost | Deletes visible? | History |
|---|---|---|---|---|
| **CDC via Pub/Sub API** | Seconds | Minimal (gRPC stream) | Yes (incl. undelete) | 72h replay |
| Polling with `SystemModstamp` (Bulk 2.0) | Poll interval | 1 query job per poll | Only with `queryAll` | Cursor-based |
| PushTopics (legacy Streaming API) | Seconds | Low | Yes | 24h replay, being retired |

Use CDC wherever it's available; the connector's polling mode covers objects that don't
support change events, and the [legacy CometD connector](../connectors/streaming-source.md)
covers orgs without Pub/Sub API access.

## Operational notes

- **Allocations:** orgs have daily CDC event allocations (edition-dependent; add-ons
  raise them). Monitor delivery usage in Setup if you enable many high-churn objects.
- **Bulk loads:** pausing CDC-heavy pipelines during mass DML avoids flooding the event
  bus; the connector's snapshot/resync path is the better tool for backfills.
- **Downtime:** restarts inside 72 hours replay from the stored replayId; longer outages
  trigger the connector's [gap recovery](offsets-and-recovery.md).
