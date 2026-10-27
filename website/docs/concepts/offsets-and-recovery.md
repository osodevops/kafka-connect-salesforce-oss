---
title: Offsets & Recovery
description: What the connectors checkpoint, and how they recover from downtime and gap events.
sidebar_position: 3
---

# Offsets & Recovery

## What is stored

The source connector keeps one offset per SObject (source partition
`{"sobject": "<name>"}`):

```json
{
  "offsetVersion": "1",
  "mode": "EVENT_DRIVEN",
  "replayId": "AAAAAAAABDI=",
  "probeReplayId": "AAAAAAAABCg=",
  "snapshotCompletionTimestamp": 1767200000000,
  "lastSystemModstamp": "2026-07-01T00:00:00.000Z",
  "commitTimestamp": 1767201234567
}
```

- `replayId` — the Pub/Sub position (opaque bytes, base64), resumed with
  `replay_preset=CUSTOM`.
- `probeReplayId` — the pre-snapshot probe used for the handoff; kept until the first
  live event is processed.
- `lastSystemModstamp` — the polling-mode cursor.
- `commitTimestamp` — the last CDC commit seen; the watermark for gap resync.

The legacy CometD source stores `{"replayId": <long>}` per channel.

## Recovery scenarios

| Scenario | Behaviour |
|---|---|
| Worker restart / rebalance | Resume from stored offsets; no action needed |
| Downtime < 72h (event-driven) | Replay from `replayId` — no loss, possible duplicates |
| Downtime > 72h / expired replayId | `sf.gap.recovery`: `resync` (record the stream's current position, then an incremental Bulk re-sync from the watermark − buffer, then resume from that position), `latest` (resume from the newest event; events during the downtime are lost), or `fail` (the task fails and the stored offset is kept, so a restart fails the same way until an operator acts) |
| Transient stream failure (`UNAVAILABLE`, stream reset, stream closed by Salesforce) | The pipeline resubscribes from its last position with exponential backoff (500 ms doubling to 60 s, with jitter); other pipelines in the task keep running |
| Session expired (`UNAUTHENTICATED`) | The rejected session is dropped, the connector re-authenticates and resubscribes; three rejections in a row fail the task |
| Missing channel or permission (`NOT_FOUND`, `PERMISSION_DENIED`), undecodable event | The task fails with the topic, status and a hint (for example "Enable Change Data Capture for Contact"); records already produced by other pipelines in the same poll are delivered first |
| CDC gap event (`GAP_*`) | Affected record Ids re-queried via REST and re-emitted |
| CDC overflow (`GAP_OVERFLOW`) | Per `sf.gap.recovery` (overflow doesn't identify records) |
| Restart mid-snapshot | Snapshot restarts from the beginning (at-least-once); the original probe replayId is retained so the handoff still misses nothing |

## Resetting offsets

To re-consume from scratch, delete the connector, remove its offsets, and re-create it.
With Kafka Connect ≥ 3.6 you can use the offsets REST API:

```bash
# stop the connector, then
curl -X DELETE localhost:8083/connectors/salesforce-source/offsets
```

Per-SObject resets are possible by patching only that partition's offset. After a reset,
the cold-start position follows `sf.event.start` (`latest` or `all`) and, if enabled, a
fresh snapshot runs first.
