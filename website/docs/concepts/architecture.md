---
title: Architecture
description: How sf-core and the four connectors fit together.
sidebar_position: 1
---

# Architecture

```
                    ┌─────────────────────────────────────────────┐
                    │       kafka-connect-salesforce (repo)        │
                    ├─────────────────────────────────────────────┤
   Salesforce       │  sf-core (shared library)                    │      Kafka
   ──────────       │   • Auth: Client Credentials, JWT Bearer     │      ─────
   Pub/Sub gRPC ◄───┤   • Pub/Sub client (Subscribe/Publish/       │
   Bulk API 2.0 ◄───┤     GetSchema/GetTopic, replay handling)     ├──► SourceRecord
   REST/SOQL    ◄───┤   • Bulk 2.0 client (query + ingest jobs)    │      (any converter)
   CometD (opt) ◄───┤   • REST/SOQL client + describe cache        │
                    │   • Avro/describe → Connect schema mapping   │
                    │   • Rate-limit governor (/limits polling)    │
                    ├─────────────────────────────────────────────┤
                    │  Connectors (thin):                          │
                    │   • SalesforceSourceConnector/Task           │◄── SinkRecord
                    │   • SalesforceSinkConnector/Task             │
                    │   • SalesforcePlatformEventSinkConnector     │
                    │   • SalesforceStreamingSourceConnector (opt) │
                    └─────────────────────────────────────────────┘
```

## Design principles

- **One shared `sf-core` library** owns all Salesforce protocol logic — auth and session
  refresh, the Pub/Sub gRPC client, Bulk 2.0 job lifecycles, REST/SOQL with describe
  caching, schema mapping, retry classification, and quota governance. The connectors
  stay thin and testable.
- **Checkpointing via Kafka Connect's offset store.** The Pub/Sub `replayId` (opaque
  bytes, stored base64) and Bulk `SystemModstamp` cursor live in each record's
  `sourceOffset`, exactly as the framework intends. No external state.
- **Per-SObject isolation.** The source runs an independent state machine per SObject —
  its own subscription, schema cache, Bulk jobs, and offsets — so one failing object
  can't stall the rest.
- **Pluggable converters.** Records are standard Connect Structs (or schemaless maps for
  the CometD source); use any key/value converter — JSON, Avro with Schema Registry or
  Apicurio, Protobuf.
- **Clean room.** Everything is implemented from Salesforce's public API documentation
  with an independent `sf.*` config namespace. No Confluent code or config strings.

## The source state machine

Each SObject pipeline moves through up to three modes, persisted in its offset so
restarts resume in the right place:

```
SNAPSHOT (Bulk 2.0)  ──►  EVENT_DRIVEN (Pub/Sub CDC)
        │                        ▲ gap events → REST re-query / Bulk resync
        └──────────►  POLLING (Bulk 2.0 + SystemModstamp cursor)
```

The [snapshot→stream handoff](../connectors/source.md#snapshot--stream-seamless-handoff)
uses a replayId probe plus commit-timestamp dedup to make the transition lossless and
duplicate-free.

## Salesforce API cheat sheet

| API | Used for | Key limits |
|---|---|---|
| OAuth token endpoint | All auth | Client Credentials requires My Domain |
| Pub/Sub API (gRPC) | CDC + platform-event streaming, event publish | 72h retention, `num_requested` ≤ 100 |
| Bulk API 2.0 | Snapshot, polling, high-volume writes | 150M records/24h, 150MB/job, 10k query jobs/24h |
| REST/SOQL | Describe, gap re-query, low-latency writes, `/limits/` | Counts against the 24h API pool |
| Streaming API (CometD) | Legacy fallback only | 24h/72h retention, being retired |
