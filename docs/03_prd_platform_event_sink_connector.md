# PRD-03 — Salesforce Platform Event Sink Connector

**Status:** Draft for build
**Replaces (Confluent):** Platform Event Sink
**Depends on:** PRD-00 (sf-core)
**Clean-room note:** Specified from Salesforce Platform Events / Pub/Sub API docs + Confluent public behaviour docs. Own `sf.*` config namespace.

---

## 1. Objective
A Kafka Connect **sink** connector that publishes Kafka records to Salesforce **Platform Events** (`*__e`). It only publishes events — it cannot insert/update/delete standard/custom objects (that is PRD-02). Parity with Confluent's Platform Event Sink ([config](https://docs.confluent.io/kafka-connectors/salesforce/current/platformevents_sink/salesforce_platformevent_sink_connector_config.html)).

## 2. Scope
### In scope
- Publish to a named platform event (`.*__e`) that must pre-exist in Salesforce.
- Publish via **Pub/Sub API** `Publish`/`PublishStream` (preferred) with REST fallback.
- Map Kafka record fields → event fields; multiple tasks.
- Error handling + DLQ.

### Out of scope
- Defining/creating platform events (must exist in Salesforce). Object CRUD (PRD-02).

## 3. User stories
- Emit a `Order_Shipped__e` platform event into Salesforce whenever an order-shipped record lands on a Kafka topic, to trigger downstream Salesforce automation.
- Bridge events from an external system into Salesforce's event bus for Flow/Apex subscribers.

## 4. Functional requirements

### 4.1 Publishing
- **Primary path — Pub/Sub API:** encode record as Avro per the event's schema (from `GetSchema`), call `Publish` (unary batch) or `PublishStream` (bidirectional, higher rate). `CreatedDate`/`CreatedById` required in payload. Read per-event `PublishResult` for final success/failure ([Publish RPC](https://developer.salesforce.com/docs/atlas.en-us.platform_events.meta/platform_events/platform_events_publish_pubsub_api.htm)).
- **Fallback path — REST:** `POST /services/data/vXX.X/sobjects/<Event>__e/` (single) or `POST /composite/` (multiple) ([REST publish](https://developer.salesforce.com/docs/atlas.en-us.platform_events.meta/platform_events/platform_events_publish_api.htm)).
- `sf.publish.mode = pubsub | rest` (default `pubsub`).

### 4.2 Event resolution & mapping
- `sf.platform.event.name` must match `.*__e$` (e.g. `LoginEvent__e`).
- Map record value fields → event fields by name. Only Checkbox/Date/Date-Time/Number/Text/Text-Area field types exist on platform events — validate types against the event schema.
- Input record structure should match the platform-event source output (parity requirement).

### 4.3 Publish behaviour semantics
- Document Salesforce's Publish Immediately vs Publish After Commit distinction (defined at the event, not the connector) so users understand delivery guarantees ([publish behaviour](https://developer.salesforce.com/docs/atlas.en-us.platform_events.meta/platform_events/platform_events_considerations_decoupled_processes.htm)).
- Published events cannot be rolled back — at-least-once, duplicates possible on retry.

### 4.4 Multi-task
- Supports `tasks.max > 1` (Confluent's Platform Event Sink supports multiple tasks).

## 5. Configuration (selected)
| Property | Type | Default | Description |
|---|---|---|---|
| `sf.auth.*` / `sf.instance.url` / `sf.api.version` | — | — / — / `60.0` | Auth + endpoint (PRD-00) |
| `topics` / `topics.regex` | list/string | — | Source topics |
| `sf.platform.event.name` | string | — | Target event, `.*__e$` |
| `sf.publish.mode` | enum | `pubsub` | `pubsub` \| `rest` |
| `behavior.on.api.errors` | enum | `fail` | `fail` \| `log` \| `ignore` |
| `errors.deadletterqueue.topic.name` | string | `dlq-${connector}` | DLQ |
| `sf.request.max.retries.time.ms` | long | 30000 | Retry budget |

## 6. Delivery & error handling
- **At-least-once.** Per-event `PublishResult` (Pub/Sub) or REST response drives success/failure; failures → DLQ per `behavior.on.api.errors`.
- Retry on transient gRPC/HTTP errors with backoff; respect API quota governor.

## 7. Rate limits
- Subject to org platform-event publishing limits and the 24h API pool (esp. free dev orgs). Governor throttles ([limits](https://docs.confluent.io/cloud/current/connectors/limits.html)).

## 8. Acceptance criteria
- [ ] Publishes a record to a pre-defined `*__e` event via Pub/Sub, confirmed by a Salesforce subscriber.
- [ ] REST fallback publishes single + composite batches.
- [ ] Field-type validation rejects/report mismatches cleanly.
- [ ] Failed publishes land in DLQ with reason.
- [ ] Multiple tasks increase publish throughput.
