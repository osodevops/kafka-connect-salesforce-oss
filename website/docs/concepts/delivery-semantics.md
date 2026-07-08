---
title: Delivery Semantics
description: At-least-once delivery, deduplication, and idempotency strategies.
sidebar_position: 2
---

# Delivery Semantics

All connectors are **at-least-once**, matching the guarantees of Confluent's Salesforce
connectors and of the underlying Salesforce APIs.

## Source connectors

- Offsets (replayId / Bulk cursor) are committed by the Connect framework *after*
  records are produced to Kafka. A crash between produce and commit re-delivers records
  on restart — duplicates are possible, loss is not.
- Restarting within the 72-hour event-retention window resumes from the stored replayId
  with no missed events. Beyond it, the configured
  [gap recovery](offsets-and-recovery.md) applies.
- The snapshot→stream handoff drops replayed events already covered by the snapshot
  (commit timestamp before snapshot completion), so the boundary itself introduces
  neither loss nor duplicates.
- Polling mode advances its `SystemModstamp` cursor only after a full polling window has
  been emitted; a restart mid-window re-emits the window.

**Consumer guidance:** deduplicate on record key (the Salesforce Id) plus the
`sf.commit.number`/`sf.commit.timestamp` headers, or make downstream processing
idempotent.

## Sink connectors

- `put()` writes synchronously; offsets only advance after Salesforce accepts the batch.
  Redelivery after a crash can re-write records.
- **Make writes idempotent with external-ID upsert** (`sf.<obj>.use.custom.id.field`) —
  re-delivered upserts converge to the same row. Plain inserts, by contrast, can create
  duplicates on redelivery.
- Platform events cannot be rolled back once published; duplicates are possible on retry.
  Subscribers should key on a business identifier carried in the event payload.

## Ordering

- Event-driven source records preserve Salesforce commit order per CDC channel.
- Records for one SObject go to one topic; use a key-based partitioner (default) so all
  changes for a record Id land in the same partition in order.
- Bulk snapshot/polling results are unordered (Bulk 2.0 forbids `ORDER BY`).
