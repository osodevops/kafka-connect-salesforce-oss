# PRD-01 — Salesforce Source Connector (unified)

**Status:** Draft for build
**Replaces (Confluent):** PushTopic Source, CDC Source, Platform Event Source, Bulk API 1.0 Source, Bulk API 2.0 Source, Source V2
**Depends on:** PRD-00 (sf-core)
**Clean-room note:** Specified from Salesforce public API docs + Confluent public behaviour docs. Uses its own `sf.*` config namespace. No Confluent code/config strings copied verbatim.

---

## 1. Objective

A single Kafka Connect **source** connector that streams Salesforce data into Kafka with three ingestion modes and an optional historical snapshot, achieving parity with all six Confluent Salesforce source connectors. Mirrors the capabilities of Confluent's unified Source V2 while adding legacy-mode compatibility ([Source V2 reference](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-source-v2.html)).

## 2. Scope

### In scope
- Modes: **Historical Snapshot** (Bulk 2.0), **Event-Driven Sync** (Pub/Sub CDC + Platform Events), **Periodic Polling** (Bulk 2.0 with modstamp cursor).
- Multi-SObject per instance (each with own topic, offset, schema cache, subscription).
- Seamless snapshot→stream handoff with zero data loss.
- Gap/overflow event recovery.
- create/update/delete/undelete change types; tombstones on delete.
- Auth: Client Credentials, JWT Bearer, (legacy) Username-Password.
- Output via pluggable converters (Avro/JSON/JSON-SR/Protobuf).

### Out of scope (this PRD)
- Legacy CometD transport (see PRD-04) — used only when Pub/Sub unavailable.
- Sink behaviour (PRD-02/03).
- CSFLE (phase 2).

## 3. User stories
- As a data engineer, I stream all changes to `Account`, `Contact`, `Opportunity` into per-object Kafka topics in real time.
- As a data engineer, I bootstrap topics with a historical load then transition to live CDC with no gap and no duplicates.
- As an integrator on an older org without Pub/Sub, I poll SObjects on a schedule via Bulk 2.0.
- As an operator, after downtime longer than 72h I get an automatic incremental resync instead of silent data loss.

## 4. Functional requirements

### 4.1 Ingestion modes
| Mode | API | Behaviour |
|---|---|---|
| Historical Snapshot | Bulk API 2.0 query | One-time initial load from `sf.snapshot.since` (CreatedDate). Optional; runs before real-time mode. |
| Event-Driven Sync (default) | Pub/Sub `Subscribe` on CDC channels | Real-time create/update/delete/undelete. replayId checkpointing. |
| Periodic Polling | Bulk API 2.0 query | Poll every `sf.poll.interval.ms` using a monotonic cursor field (`SystemModstamp` default). For orgs/objects without CDC. |

`sf.realtime.mode = event_driven | polling` selects the continuous mode after snapshot. Confluent parity: Source V2 offers exactly these two real-time modes plus optional snapshot ([Source V2](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-source-v2.html)).

### 4.2 Multi-SObject
- `sf.sobjects` = comma-separated API names (exact casing). Default max 5 (configurable via `sf.sobjects.max`; Confluent hard-caps at 5).
- Topic naming: `{sf.topic.prefix}.{SObject}` (e.g. `salesforce.Account`).
- Each SObject processed with **independent state**: schema cache, offset, bulk job, Pub/Sub subscription — isolate failures per object.
- `tasks.max` capped at number of SObjects; extra tasks idle (log INFO). Round-robin SObjects across tasks.

### 4.3 Change events (Event-Driven Sync)
- Subscribe to per-entity CDC channel `/data/<Entity>ChangeEvent` (or combined `/data/ChangeEvents`).
- Decode `ChangeEventHeader`: `changeType`, `entityName`, `recordIds`, `commitTimestamp`, `commitNumber`, `diffFields`, `changeOrigin`, `transactionKey` ([ChangeEventHeader](https://developer.salesforce.com/docs/atlas.en-us.change_data_capture.meta/change_data_capture/cdc_event_fields_header.htm)).
- Emit metadata: record headers `sf.change.type`, `sf.entity`, `sf.changed.fields`, `sf.nulled.fields`; and `_EventType`/`_ObjectType` value fields for Confluent-consumer compatibility.
- `sf.emit.tombstone.on.delete` (default false): on DELETE emit a null-value / non-null-key tombstone record (Event-Driven only) — parity with Source V2.
- `sf.full.record.on.update` (default false): CDC UPDATE events carry only changed fields; when true, do one REST fetch per UPDATE to attach the full post-image. Warn: extra API call per update — quota risk.
- Decode CDC hex bitmaps for changed/nulled fields into the headers above (Source V2 does this).

### 4.4 Gap & overflow recovery
- On `GAP_CREATE/UPDATE/DELETE/UNDELETE` or `GAP_OVERFLOW` (header only, no fields): re-query the affected `recordIds` via REST/Bulk to reconstruct the record ([Gap Events](https://developer.salesforce.com/docs/atlas.en-us.change_data_capture.meta/change_data_capture/cdc_other_events_gap.htm)).
- `sf.gap.recovery = resync | latest | fail` (default `resync`):
  - `resync`: incremental Bulk 2.0 re-sync of the affected SObject from `max(last CDC commit ts, snapshot completion ts) − sf.gap.resync.buffer.ms`. If no watermark, full re-fetch.
  - `latest`: skip gap, resubscribe from newest (data loss accepted).
  - `fail`: stop the task.

### 4.5 Snapshot → stream seamless handoff (zero data loss)
This is the hardest requirement; mirror Source V2's approach:
1. During snapshot, open a parallel Pub/Sub subscription with `replay_preset=LATEST`, record the first replayId (probe), do not emit from it.
2. Run Bulk 2.0 snapshot to completion; record `snapshotCompletionTimestamp`.
3. At handoff, open the real subscription with `replay_preset=CUSTOM` at the probed replayId.
4. **Dedup:** drop any CDC event whose change/commit timestamp is strictly before `snapshotCompletionTimestamp`.
5. Continue on the same topic. At-least-once overall; no loss.

### 4.6 Cold-start replay position
- `sf.event.start = latest | all` (default `latest`):
  - `latest` → Pub/Sub `LATEST` preset (new events only).
  - `all` → `EARLIEST` preset (replay within 72h retention).
- After first emitted event, resume from stored replayId via `CUSTOM` preset. Resetting offsets falls back to `sf.event.start`.

### 4.7 Polling mode
- Bulk 2.0 query per interval; SOQL `SELECT <fields> FROM <obj> WHERE SystemModstamp > :cursor ORDER BY` disallowed (ORDER BY disables PK chunking) — track cursor and rely on modstamp filter.
- `sf.include.deleted` uses `queryAll` to include recycle-bin records.
- Custom SOQL support (`sf.<obj>.query.*`) with the same restrictions Salesforce enforces: must include `Id` + cursor field; no subqueries/GROUP BY/ORDER BY/OFFSET/TYPEOF/aggregates; no compound (Address/Location) fields ([Bulk 2.0 query rules](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/queries.htm)).

### 4.8 Schema & data types
- Event-Driven: Avro schema from Pub/Sub `GetSchema` (cache by schema ID; refetch on change). Map Avro → Connect `Schema`/`Struct`.
- Bulk/polling: build schema from REST `sobjects/<X>/describe/` (cache; use `If-Modified-Since` → 304).
- Date/datetime: default epoch millis (`int64`); `sf.skip.epoch.conversion.fields` keeps ISO-8601 strings (note schema-evolution risk). Parity with Source V2.
- Compound fields (Address, Location) not populated — expose component fields only (matches Salesforce Bulk behaviour).

## 5. Configuration (own namespace; migration mapping in appendix)

| Property | Type | Default | Description |
|---|---|---|---|
| `sf.auth.grant.type` | enum | `client_credentials` | `client_credentials`, `jwt_bearer`, `password` (legacy) |
| `sf.instance.url` | string | — | My Domain URL (required for client_credentials/jwt) |
| `sf.consumer.key` | password | — | Connected App consumer key |
| `sf.consumer.secret` | password | — | Consumer secret (client_credentials/password) |
| `sf.username` | string | — | For jwt_bearer/password |
| `sf.password` / `sf.password.token` | password | — | Legacy password + security token |
| `sf.jwt.keystore.path` / `.password` | string/password | — | JWT Bearer keystore |
| `sf.api.version` | string | `60.0` | Target API version (≥ v47 for Bulk 2.0) |
| `sf.sobjects` | list | — | SObject API names (≤ `sf.sobjects.max`) |
| `sf.sobjects.max` | int | 5 | Max SObjects per instance |
| `sf.topic.prefix` | string | — | Topics = `{prefix}.{SObject}` |
| `sf.snapshot.enabled` | bool | true | Historical Bulk 2.0 snapshot |
| `sf.snapshot.since` | string(date) | today | Snapshot CreatedDate start |
| `sf.realtime.mode` | enum | `event_driven` | `event_driven` \| `polling` |
| `sf.event.start` | enum | `latest` | `latest` \| `all` |
| `sf.gap.recovery` | enum | `resync` | `resync` \| `latest` \| `fail` |
| `sf.gap.resync.buffer.ms` | long | 60000 | Safety buffer subtracted on resync |
| `sf.full.record.on.update` | bool | false | REST fetch full post-image on UPDATE |
| `sf.emit.tombstone.on.delete` | bool | false | Tombstone on DELETE |
| `sf.include.deleted` | bool | false | `queryAll` includes soft-deleted |
| `sf.poll.interval.ms` | int | 30000 | Polling interval (≥ 8700) |
| `sf.result.max.rows` | int | 1000 | Max Bulk rows per result set (split across SObjects) |
| `sf.skip.epoch.conversion.fields` | list | — | Keep as ISO-8601 |
| `sf.request.max.retries.time.ms` | long | 30000 | Retry budget |

## 6. Delivery, offsets, restart
- **At-least-once.** Duplicates possible on restart — documented.
- **`sourceOffset` per SObject** (JSON): `mode` (snapshot/polling/event_driven), `offsetVersion`, `lastId`, `lastSystemModstamp`, `jobId`, `locator`, `snapshotCompletionTimestamp`, `replayId` (base64 bytes), `commitTimestamp`, `commitNumber`. Mirrors Source V2's offset structure.
- Events outside 72h retention → apply `sf.gap.recovery`.
- Per-SObject offset reset resets only that object.

## 7. Error handling & rate limits
- Retry HTTP 429/5xx, Bulk `403 REQUEST_LIMIT_EXCEEDED`, gRPC `UNAVAILABLE`/`DEADLINE_EXCEEDED`/`RESOURCE_EXHAUSTED`, network timeouts — exponential backoff + jitter, capped by `sf.request.max.retries.time.ms`.
- Poll `GET /services/data/vXX.X/limits/` to pre-empt quota exhaustion; back off proactively.
- `errors.tolerance` supported; warn that `all` on a source can drop records.

## 8. Non-functional
- Independent per-SObject failure isolation. Graceful pause/resume. Structured logging (never log tokens; no curl-command logging of auth headers). Metrics: events/sec, replay lag, API calls used vs quota, snapshot progress.

## 9. Acceptance criteria
- [ ] Streams CDC for ≥3 SObjects to per-object topics with replayId checkpointing.
- [ ] Snapshot→stream handoff produces no gap and no duplicate across the boundary (verified with a controlled change during snapshot).
- [ ] Restart within 72h resumes with no missed events; >72h triggers configured gap recovery.
- [ ] Gap event triggers re-query and reconstructs the record.
- [ ] Polling mode captures inserts/updates via modstamp cursor incl. soft-deletes when enabled.
- [ ] Client Credentials + JWT Bearer auth both work against a My Domain org.
- [ ] Output round-trips through Avro/JSON converters.

## Appendix — Confluent → OSS config mapping (for migrators)
| Confluent | OSS |
|---|---|
| `salesforce.grant.type` | `sf.auth.grant.type` |
| `salesforce.instance` | `sf.instance.url` |
| `sobject.names` | `sf.sobjects` |
| `topic.prefix` | `sf.topic.prefix` |
| `historical.snapshot` | `sf.snapshot.enabled` |
| `real.time.ingestion.mode` | `sf.realtime.mode` |
| `event.sync.start.point` | `sf.event.start` |
| `gap.event.recovery` | `sf.gap.recovery` |
| `cdc.full.record.on.update` | `sf.full.record.on.update` |
| `emit.tombstone.on.delete` | `sf.emit.tombstone.on.delete` |
