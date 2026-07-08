# PRD-04 — Legacy CometD Streaming Source Connector (optional / compatibility)

**Status:** Draft — build only if orgs without Pub/Sub access must be supported
**Replaces (Confluent):** PushTopic Source, and CDC/Platform-Event Source on orgs lacking Pub/Sub
**Depends on:** PRD-00 (sf-core)
**Clean-room note:** Specified from Salesforce Streaming API docs + Confluent public behaviour docs. Own `sf.*` config namespace.

---

## 1. Objective
A Kafka Connect **source** connector using the legacy **Streaming API (CometD/Bayeux)** to subscribe to PushTopics, CDC channels, or Platform Events for orgs/editions where the Pub/Sub API is unavailable (e.g. Government Cloud, or policy constraints). This is a **compatibility fallback** — PRD-01 (Pub/Sub) is the strategic default.

## 2. Why this exists / why it's optional
- Salesforce recommends Pub/Sub API over Streaming API and is retiring old Streaming versions (23.0–36.0) ([retirement](https://help.salesforce.com/s/articleView?id=000396440&type=1)). Pub/Sub is the future.
- But CometD still works on v37.0+ (Durable Streaming) and is the only real-time option on some orgs. Build this only if a target customer lacks Pub/Sub.

## 3. Scope
### In scope
- CometD subscription to: PushTopics (`/topic/<name>`), CDC (`/data/<Entity>ChangeEvent`, `/data/ChangeEvents`), Platform Events (`/event/<name>__e`).
- PushTopic auto-create (like Confluent's PushTopic source) from sObject describe.
- replayId durable checkpointing.
- Auth: Client Credentials, JWT Bearer, (legacy) Username-Password.

### Out of scope
- Bulk snapshot/polling (use PRD-01). Sinks.

## 4. Functional requirements

### 4.1 Transport
- CometD/Bayeux over HTTP long-polling at `https://<my-domain>/cometd/<version>/` (v37.0+ for durable replay) ([Message Durability](https://developer.salesforce.com/docs/atlas.en-us.api_streaming.meta/api_streaming/using_streaming_api_durability.htm)).
- Handshake/subscribe/connect loop; reconnect within 40s to avoid client timeout.

### 4.2 Channel types
- `sf.channel.type = pushtopic | cdc | platform_event`.
- **PushTopic:** `sf.pushtopic.name`; auto-create if `sf.pushtopic.create=true` from `sf.object` describe; toggle notify on create/update/delete/undelete (parity with Confluent PushTopic source).
- **CDC:** `sf.cdc.channel` (single-entity, combined `ChangeEvents`, or custom `__chn`); `sf.channel.entities` to filter.
- **Platform Event:** `sf.platform.event.name` (`.*__e$`), must pre-exist.

### 4.3 Replay / checkpoint
- `sf.event.start = latest | all`: `latest` → replayId `-1` (new only); `all` → replayId `-2` (all retained: 24h standard / 72h high-volume/CDC).
- Store replayId (opaque bytes) in `sourceOffset`. On invalid/expired replayId: `sf.invalid.replay.behaviour = all | latest`.
- Restart within retention window resumes with no loss; beyond it, events lost; unexpected failure may duplicate (parity with Confluent).

### 4.4 Metadata & schema
- Emit `_EventType`/`_ObjectType` value fields; topic template may reference them.
- PushTopic/CDC events delivered as JSON (CometD) → map to Connect schema.

## 5. Configuration (selected)
| Property | Type | Default | Description |
|---|---|---|---|
| `sf.auth.*` / `sf.instance.url` / `sf.api.version` | — | — / — / `60.0` | Auth + endpoint (≥37.0 for durable) |
| `sf.channel.type` | enum | `cdc` | `pushtopic` \| `cdc` \| `platform_event` |
| `sf.object` | string | — | SObject (PushTopic create) |
| `sf.pushtopic.name` / `.create` | string/bool | — / true | PushTopic name / auto-create |
| `sf.pushtopic.notify.{create,update,delete,undelete}` | bool | true | PushTopic notify toggles |
| `sf.cdc.channel` | string | `ChangeEvents` | CDC channel |
| `sf.channel.entities` | list | — | Entities to filter (multi-entity channels) |
| `sf.platform.event.name` | string | — | `.*__e$` |
| `sf.event.start` | enum | `latest` | `latest` \| `all` |
| `sf.invalid.replay.behaviour` | enum | `all` | `all` \| `latest` |
| `kafka.topic` | string | — | Topic template (`_EventType`, `_ObjectType`) |
| `sf.connection.timeout.ms` | long | 30000 | CometD connect timeout |
| `sf.request.max.retries.time.ms` | long | 900000 | Retry budget (15 min, matches legacy default) |

## 6. Delivery & limits
- At-least-once via replayId. `tasks.max=1` (one subscription per connector, like Confluent). Retention 24h/72h. No real-time schema change (restart to resync schema).

## 7. Acceptance criteria
- [ ] Subscribes to a PushTopic (auto-created) and streams CRUD events with replayId checkpointing.
- [ ] Subscribes to a CDC channel and a Platform Event channel.
- [ ] Restart within retention resumes with no missed events.
- [ ] Invalid replayId handled per configured behaviour.
- [ ] Client Credentials + JWT auth work.
