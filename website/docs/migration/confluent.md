---
title: Migrating from Confluent
description: Step-by-step migration from Confluent's proprietary Salesforce connectors to the open-source alternative - config translation script, zero-loss cutover, and verifiable evidence.
sidebar_position: 1
keywords:
  - confluent salesforce connector
  - confluent alternatives
  - open source alternative to confluent salesforce connector
  - migrate from confluent
  - confluent pricing
---

# Migrating from Confluent

This suite is the **open-source alternative to Confluent's Salesforce connectors**: four
Apache-2.0 connectors with functional parity to Confluent's eleven, built clean-room from
Salesforce's public APIs. This guide takes you from a running Confluent connector to the
OSS equivalent with **a config-translation script, a zero-loss cutover procedure, and
machine-readable evidence that the migration worked**.

## Why teams migrate

- **License cost** — Confluent's Salesforce connectors are separately licensed premium
  connectors (self-managed) or metered per-task (Cloud). The OSS suite is Apache-2.0:
  no license, no per-connector fees, no licensing topics.
- **No lock-in** — runs on any Kafka Connect: Apache Kafka, MSK Connect, Strimzi, or
  Confluent Platform itself.
- **Modern transport** — Pub/Sub API (gRPC) + Bulk API 2.0, the same architecture as
  Confluent's newest Source V2, including for teams still on the older CometD-based
  connectors.

## What replaces what

| Confluent connector | Replacement | Notes |
|---|---|---|
| Salesforce CDC Source | [Source](../connectors/source.md) (event-driven mode) | Pub/Sub transport instead of CometD |
| Salesforce Platform Event Source | [Source](../connectors/source.md) | |
| Salesforce Bulk API 1.0 / 2.0 Source | [Source](../connectors/source.md) (snapshot/polling) | Bulk 2.0 only |
| Salesforce Source V2 | [Source](../connectors/source.md) | Same architecture |
| Salesforce PushTopic Source | [Streaming source](../connectors/streaming-source.md) | PushTopics only exist on CometD |
| Salesforce SObject / Bulk API Sinks | [SObject sink](../connectors/sobject-sink.md) | |
| Salesforce Platform Event Sink | [Platform Event sink](../connectors/platform-event-sink.md) | |

## Migration at a glance

1. [Translate your connector configs](#step-1--translate-your-configs) — scripted, ~minutes
2. [Rehearse in staging](#step-2--rehearse-in-staging)
3. [Prepare consumers for the overlap window](#step-3--prepare-consumers)
4. [Cut over](#step-4--cut-over) — inside Salesforce's 72-hour retention window
5. [Verify with evidence](#step-5--verify-with-evidence) — scripted
6. [Keep a rollback path](#step-6--rollback-plan)

## Step 1 — Translate your configs

The repo ships a translator that converts any Confluent Salesforce connector config into
the OSS equivalent and reports every property it mapped, dropped, or needs a decision on:

```bash
# straight from the Connect REST API
curl -s http://connect:8083/connectors/sf-cdc-prod \
  | tools/confluent-migration/confluent2oss.py - -o sf-cdc-prod-oss.json
```

```text
======================================================================
migration report
======================================================================
connector.class: io.confluent.salesforce.SalesforceCdcSourceConnector
                 -> sh.oso.salesforce.source.SalesforceSourceConnector
mapped   salesforce.instance -> sf.instance.url
mapped   sobject.names -> sf.sobjects
value    sf.auth.grant.type: CLIENT_CREDENTIALS -> client_credentials
dropped  confluent.license (no license required (Apache-2.0))
MANUAL   reporter.error.topic.name: use errors.deadletterqueue.topic.name instead
```

It exits non-zero when anything needs manual attention. Typical manual items:
Confluent reporter topics → Connect's native DLQ (`errors.deadletterqueue.*`), and
CSFLE (not yet supported — keep those fields unencrypted or hold that connector back).

## Step 2 — Rehearse in staging

Point the translated config at a sandbox org (`sf.jwt.audience=https://test.salesforce.com`)
and your staging Kafka. Two things to confirm:

- **Record shape**: values carry the same `_EventType`/`_ObjectType` convention as
  Confluent's connectors, so existing consumers and sink pipelines generally work
  unchanged. Diff a handful of records; if a consumer depends on a Confluent-specific
  field, patch it now or bridge with an SMT.
- **Converters**: the OSS connectors emit standard Connect Structs — keep whatever
  key/value converters you use today.

## Step 3 — Prepare consumers

Offsets are **not transferable** between connector implementations — the Confluent
connector's source offsets are opaque to any other connector class (and vice versa).
The zero-loss strategy is therefore *replay with overlap*: the OSS connector re-reads a
window of events the old connector already delivered.

That means a short burst of **duplicates, never gaps**. Before cutover, confirm your
consumers are idempotent — dedupe on the record key (Salesforce Id) plus the
`sf.commit.timestamp` header (or `SystemModstamp` field). Sinks that upsert by external
ID are already safe.

## Step 4 — Cut over

For CDC / Platform Event sources (72-hour event retention):

```bash
T0=$(date -u +%Y-%m-%dT%H:%M:%SZ)                            # record the cutover time
curl -X DELETE http://connect:8083/connectors/sf-cdc-prod    # stop the old connector
curl -X POST -H 'Content-Type: application/json' \
     --data @sf-cdc-prod-oss.json http://connect:8083/connectors
```

with the translated config containing:

```json
"sf.snapshot.enabled": "false",
"sf.event.start": "all"
```

`sf.event.start=all` replays everything Salesforce retained (up to 72h), which covers
both the overlap and any events that fire during the connector gap — **do the cutover
well inside 72 hours of stopping the old connector**. From then on the OSS connector
checkpoints its own replayIds and the window never matters again.

Variants:

- **Long-lapsed or brand-new topics**: enable `sf.snapshot.enabled=true` instead — a
  Bulk 2.0 snapshot backfills history, then the
  [seamless handoff](../connectors/source.md#snapshot--stream-seamless-handoff) takes
  over live changes with no gap and no duplicates.
- **Bulk/polling sources**: set `sf.snapshot.since` to a timestamp safely before the old
  connector's last successful poll; the modstamp cursor takes over after one cycle.
- **Sinks**: reset the consumer group offset of the new sink connector
  (`connect-<name>` group) to the old sink's committed position — sink offsets are plain
  Kafka consumer offsets and *do* carry over. Use external-ID upsert during the switch.

## Step 5 — Verify with evidence

Trust, but generate a report. The repo ships a verifier that cross-checks **what
Salesforce says changed** against **what actually landed in Kafka**, and writes an
evidence file (counts, missing IDs, SHA-256 of the delivered key set) you can attach to
the change ticket:

```bash
# 1. dump the topic since cutover
kafka-console-consumer.sh --bootstrap-server broker:9092 \
  --topic salesforce.Account --from-beginning \
  --property print.key=true --property key.separator=$'\t' > dump.tsv

# 2. compare against Salesforce (live query, or an `sf data query --json` export)
tools/confluent-migration/verify_cutover.py \
  --sobject Account \
  --instance-url https://acme.my.salesforce.com --access-token "$TOKEN" \
  --since "$T0" --topic-dump dump.tsv --out migration-evidence-account.json
```

```json
{
  "salesforceRecordsChanged": 1841,
  "kafkaRecordsDelivered": 1907,
  "uniqueKafkaKeys": 1841,
  "duplicateDeliveries": 66,
  "missingRecordIds": [],
  "deliveredKeySetSha256": "6b0f6f2b…",
  "verdict": "PASS: every changed Salesforce record reached Kafka"
}
```

The exit code is non-zero if any changed record never arrived — wire it into your
runbook as a gate.

**The procedure itself is proven in CI.** Every build runs
`ConfluentCutoverMigrationTest`, which executes this exact cutover (old connector era →
gap → OSS start with `sf.event.start=all`) against the protocol-accurate local Salesforce
fake and asserts zero loss, bounded duplicates, and monotonic replayId continuity — then
publishes `migration-evidence.json` as a CI artifact on
[every CI run](https://github.com/osodevops/kafka-connect-salesforce-oss/actions).

## Step 6 — Rollback plan

Keep the old connector's JSON. Rolling back is the same procedure in reverse: delete the
OSS connector, re-create the Confluent connector, and accept the same
replay-with-overlap window (Confluent's CDC connector also resumes via replayId /
its configured start point). Consumers already dedupe, so a rollback is boring — which
is the point.

:::warning Licensing during overlap
Confluent's connectors are proprietary. Running them (including during a side-by-side
window) is only permitted under an active license/subscription — plan the overlap while
you're still licensed, and confirm terms with your Confluent agreement.
:::

## Appendix — property mapping tables

### Source (Source V2 / CDC / Bulk)

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

### SObject / Bulk sinks

| Confluent | OSS |
|---|---|
| `salesforce.object` | `sf.objects` (+ `sf.<obj>.topics`) |
| `override.event.type` + `salesforce.sink.object.operation` | `sf.<obj>.override.event.type` + `sf.<obj>.operation` |
| `salesforce.use.custom.id.field` | `sf.<obj>.use.custom.id.field` |
| `salesforce.custom.id.field.name` | `sf.<obj>.custom.id.field.name` |
| `salesforce.ignore.fields` | `sf.<obj>.ignore.fields` |
| `behavior.on.api.errors` | `behavior.on.api.errors` (same) |
| reporter topics (`reporter.*`) | Connect-native DLQ (`errors.deadletterqueue.*`) |

### Platform Event sink

| Confluent | OSS |
|---|---|
| `salesforce.platform.event.name` | `sf.platform.event.name` |
| `behavior.on.api.errors` | `behavior.on.api.errors` |

### Behavioural differences to review

- Record shape is compatible (`_EventType`/`_ObjectType`); CDC metadata additionally
  arrives in `sf.*` record headers.
- Source offsets don't transfer (hence the replay cutover above); sink consumer-group
  offsets do.
- Bulk API 1.0 is not implemented (retired by Salesforce); Bulk 2.0 covers those
  workloads.
- CSFLE (client-side field-level encryption) is not yet supported.
