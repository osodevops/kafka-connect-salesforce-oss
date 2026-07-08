# Confluent Salesforce Kafka Connectors — Implementation-Grade Reference

Clean-room documentation of every Confluent Salesforce connector (self-managed Confluent Platform + fully-managed Confluent Cloud), captured for building feature-parity open-source alternatives. This describes **behavior, configuration, and semantics** only — no proprietary source code.

Every factual value below links to the exact Confluent documentation page it was taken from. Where a value could not be confirmed from a fetched page, it is marked **n.a.**

---

## Summary Comparison Table

| # | Connector | Class name | Src/Sink | Platform | Underlying Salesforce API | Delivery | Max SObjects/instance | tasks.max | Data formats | Notes |
|---|-----------|-----------|----------|----------|---------------------------|----------|-----------------------|-----------|--------------|-------|
| 1 | Salesforce PushTopic Source | `io.confluent.salesforce.SalesforceSourceConnector` (self-managed); Cloud plugin exists | Source | Both | Streaming API / CometD (PushTopics) | At-least-once | 1 PushTopic/object | 1 | Avro, JSON, JSON_SR, Protobuf | Dynamically creates PushTopics; replayId offset ([config](https://docs.confluent.io/kafka-connectors/salesforce/current/pushtopics/salesforce_pushtopic_source_connector_config.html), [limits](https://docs.confluent.io/cloud/current/connectors/limits.html)) |
| 2 | Salesforce CDC Source | `io.confluent.salesforce.SalesforceCdcSourceConnector` | Source | Both | Streaming API / CometD (Change Data Capture channels) | At-least-once | 1 channel (multi-entity via `salesforce.channel.entities`) | 1 | Avro, JSON, JSON_SR, Protobuf | replayId; 24h/72h retention ([config](https://docs.confluent.io/kafka-connectors/salesforce/current/change-data-capture/salesforce_cdc_source_connector_config.html)) |
| 3 | Salesforce Platform Event Source | `io.confluent.salesforce.SalesforcePlatformEventSourceConnector` | Source | Both | Streaming API / CometD (Platform Events) | At-least-once | up to 5 platform events (Cloud) | 1 | Avro, JSON, JSON_SR, Protobuf | Events must pre-exist; `__e` suffix ([config](https://docs.confluent.io/kafka-connectors/salesforce/current/platformevents/salesforce_platformevent_source_connector_config.html), [overview](https://docs.confluent.io/kafka-connectors/salesforce/current/platformevents/overview.html)) |
| 4 | Salesforce Bulk API Source | `io.confluent.connect.salesforce.SalesforceBulkApiSourceConnector` | Source | Self-managed | Bulk API 1.0 | At-least-once (polling) | 1 (`salesforce.object`) | n.a. | Avro, JSON, JSON_SR, Protobuf | PK-chunking; `salesforce.since` ([config](https://docs.confluent.io/kafka-connectors/salesforce/current/salesforce_bulk_api/source/configuration_options.html)) |
| 5 | Salesforce Bulk API 2.0 Source | `SalesforceBulkApiV2Source` | Source | Cloud only | Bulk API 2.0 | At-least-once | up to 5 | 1 | Avro, JSON, JSON_SR, Protobuf | Custom SOQL; API v65.0 ([Cloud](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-source.html)) |
| 6 | Salesforce Source V2 | `SalesforceSourceV2` | Source | Cloud only | Pub/Sub API (gRPC/HTTP2 CDC) + Bulk API 2.0 + REST | Zero data loss / at-least-once | up to 5 | ≤ #SObjects | Avro, JSON_SR, Protobuf | Unified snapshot+CDC+polling ([Cloud](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-source-v2.html)) |
| 7 | Salesforce SObject Sink | `io.confluent.salesforce.SalesforceSObjectSinkConnector` | Sink | Both | REST API | At-least-once | up to 5 (Cloud) | multi | matches source | insert/update/upsert/delete; batching API 42.0+ ([config](https://docs.confluent.io/kafka-connectors/salesforce/current/sobjects_sink/salesforce_sobject_sink_connector_config.html), [limits](https://docs.confluent.io/cloud/current/connectors/limits.html)) |
| 8 | Salesforce Platform Event Sink | `io.confluent.salesforce.SalesforcePlatformEventSinkConnector` | Sink | Both | REST API (publish Platform Events) | At-least-once | up to 5 events (Cloud) | multi | matches source | Publishes `__e` events ([config](https://docs.confluent.io/kafka-connectors/salesforce/current/platformevents_sink/salesforce_platformevent_sink_connector_config.html), [limits](https://docs.confluent.io/cloud/current/connectors/limits.html)) |
| 9 | Salesforce Bulk API Sink | `io.confluent.connect.salesforce.SalesforceBulkApiSinkConnector` | Sink | Self-managed | Bulk API 1.0 | At-least-once | 1 (`salesforce.object`) | n.a. | matches source | API v64.0; upsert w/ external ID ([overview](https://docs.confluent.io/kafka-connectors/salesforce/current/salesforce_bulk_api/sink/overview.html), [config](https://docs.confluent.io/kafka-connectors/salesforce/current/salesforce_bulk_api/sink/configuration_options.html)) |
| 10 | Salesforce Bulk API 2.0 Sink | `SalesforceBulkApiV2Sink` | Sink | Cloud only | Bulk API 2.0 | At-least-once | up to 5 | multi | Avro, JSON_SR, Protobuf | Big Objects, relationship fields; API v65.0 ([Cloud](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-sink.html)) |

> Notes on class names: The self-managed CDC/PushTopic/PlatformEvent/SObject connectors are packaged under the `io.confluent.salesforce.*` namespace (confirmed for CDC in the config page and for SObject Sink / Platform Event Sink in quick-start examples), while the two Bulk API 1.0 connectors are under `io.confluent.connect.salesforce.*` ([CDC config](https://docs.confluent.io/kafka-connectors/salesforce/current/change-data-capture/salesforce_cdc_source_connector_config.html), [SObject sink quickstart](https://docs.confluent.io/kafka-connectors/salesforce/current/sobjects_sink/salesforce_sobject_sink_connector_quickstart.html), [Platform Event Sink config](https://docs.confluent.io/kafka-connectors/salesforce/current/platformevents_sink/salesforce_platformevent_sink_connector_config.html), [Bulk API sink overview](https://docs.confluent.io/kafka-connectors/salesforce/current/salesforce_bulk_api/sink/overview.html)). The exact PushTopic Source class was not printed on its config page; the task-provided `io.confluent.salesforce.SalesforcePushTopicSourceConnector` is used as the reference name (**self-reported class string not verified on a fetched page** — marked as such below). Cloud plugin names (`SalesforceSourceV2`, `SalesforceBulkApiV2Source`, `SalesforceBulkApiV2Sink`) come from the CLI examples on their Cloud pages.

---

## Shared / Cross-Connector Behavior

These behaviors and property groups are common to all (or most) Salesforce connectors and are documented once here to avoid repetition. Each connector section notes deviations.

### Licensing (self-managed Confluent Platform only)
All self-managed Salesforce connectors are proprietary and use a 30-day trial without a key; after that a subscription license is required. Each Salesforce connector counts as a **separate connector type** for Connector Pack licensing even though they share one installation artifact ([overview](https://docs.confluent.io/kafka-connectors/salesforce/current/overview.html)). License is stored in the `_confluent-command` topic ([PushTopic config](https://docs.confluent.io/kafka-connectors/salesforce/current/pushtopics/salesforce_pushtopic_source_connector_config.html)).

Common license/topic properties (identical across self-managed connectors):

| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `confluent.topic.bootstrap.servers` | list | — | high | Bootstrap servers for the licensing Kafka cluster |
| `confluent.topic` | string | `_confluent-command` | low | Name of the Confluent config/licensing topic |
| `confluent.topic.replication.factor` | int | 3 | low | RF for the licensing topic (set to 1 for single-broker dev) |
| `confluent.license` | string | "" | high | Enterprise license key; empty = 30-day trial |
| `confluent.topic.ssl.truststore.location` | string | null | high | Truststore path |
| `confluent.topic.ssl.truststore.password` | password | null | high | Truststore password |
| `confluent.topic.ssl.keystore.location` | string | null | high | Keystore path (two-way auth) |
| `confluent.topic.ssl.keystore.password` | password | null | high | Keystore password |
| `confluent.topic.ssl.key.password` | password | null | high | Private key password |
| `confluent.topic.security.protocol` | string | PLAINTEXT | medium | PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL |

Source: [PushTopic config](https://docs.confluent.io/kafka-connectors/salesforce/current/pushtopics/salesforce_pushtopic_source_connector_config.html). License-topic ACLs require CREATE+DESCRIBE on the cluster and DESCRIBE+READ+WRITE on `_confluent-command` (or DESCRIBE+READ for read-only) ([same page](https://docs.confluent.io/kafka-connectors/salesforce/current/pushtopics/salesforce_pushtopic_source_connector_config.html)).

### CSFLE / CSPE (Client-Side Field Level Encryption / per-event)
Supported by all modern Salesforce connectors. Self-managed properties ([CDC config](https://docs.confluent.io/kafka-connectors/salesforce/current/change-data-capture/salesforce_cdc_source_connector_config.html)):

| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `csfle.enabled` | boolean | False | — | Enable CSFLE for the connector |
| `auto.register.schemas` | boolean | true | medium | Register schema with Schema Registry |
| `use.latest.version` | boolean | true | medium | When `auto.register.schemas=false`, use latest subject version instead of deriving schema |

Cloud connectors add `sr.service.account.id` (Schema Registry service account) and, for sinks, `csfle.onFailure` (`ERROR` fails and writes ciphertext to DLQ; `NONE` writes ciphertext undecrypted). **Warning: DLQ writes are plaintext — do not use DLQ with CSFLE/CSPE** ([Bulk API 2.0 Sink](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-sink.html)).

### Auto topic creation (self-managed source connectors)
Standard Kafka Connect group-based topic creation is supported: `topic.creation.groups`, `topic.creation.$alias.replication.factor` (required for `default`), `topic.creation.$alias.partitions` (required for `default`), `topic.creation.$alias.include`, `topic.creation.$alias.exclude`, `topic.creation.$alias.${kafkaTopicSpecificConfigName}` ([PushTopic config](https://docs.confluent.io/kafka-connectors/salesforce/current/pushtopics/salesforce_pushtopic_source_connector_config.html)).

### Metadata fields on source records
Streaming-source records carry two metadata fields on every record: `_EventType` (created/updated/deleted) and `_ObjectType`. The `kafka.topic` value is a template that can reference any schema field, e.g. `salesforce.${_ObjectType}.${_EventType}` ([PushTopic config](https://docs.confluent.io/kafka-connectors/salesforce/current/pushtopics/salesforce_pushtopic_source_connector_config.html), [CDC config](https://docs.confluent.io/kafka-connectors/salesforce/current/change-data-capture/salesforce_cdc_source_connector_config.html)).

### Retry semantics
Streaming connectors retry failed post-auth requests with exponentially-growing randomized backoff until `request.max.retries.time.ms` (default 900000 ms = 15 min) is exhausted, after which the request fails and the task likely fails ([PushTopic config](https://docs.confluent.io/kafka-connectors/salesforce/current/pushtopics/salesforce_pushtopic_source_connector_config.html)). Cloud V2/Bulk-2.0 connectors instead default to a `request.max.retries.time.ms` (max retry time) of 30000 ms, minimum 1000 ms ([Bulk API 2.0 Source](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-source.html)).

---

## 1. Salesforce PushTopic Source Connector

- **Class:** self-managed source. Task's reference string `io.confluent.salesforce.SalesforcePushTopicSourceConnector` is **not printed on the fetched config page** (the config page did not show a `connector.class` line); the self-managed Salesforce source is elsewhere referenced as `io.confluent.salesforce.SalesforceSourceConnector`. Treat the exact class string as **n.a. (unverified)** and confirm against the plugin JAR.
- **Type / Platform:** Source; available both self-managed and Confluent Cloud ([config page notes a Cloud variant](https://docs.confluent.io/kafka-connectors/salesforce/current/pushtopics/salesforce_pushtopic_source_connector_config.html)).
- **Underlying API:** Salesforce Streaming API over CometD, using **PushTopics**. PushTopics provide subscribe to create/update/delete/undelete events on SObjects ([overview](https://docs.confluent.io/kafka-connectors/salesforce/current/overview.html)).
- **Behavior:** The connector dynamically creates PushTopics if needed at launch ([overview](https://docs.confluent.io/kafka-connectors/salesforce/current/overview.html)). It subscribes to a single SObject and emits create/update/delete/undelete events (each toggleable). Streaming only (no historical snapshot).

### Authentication
Username + password + security token, OAuth2 consumer key/secret, and OAuth2 JWT bearer flow (keystore path + password). Properties: `salesforce.consumer.key` (string, high), `salesforce.consumer.secret` (password, high), `salesforce.password` (password, high), `salesforce.password.token` (password, high), `salesforce.username` (string, high), `salesforce.jwt.keystore.password` (password, null, medium), `salesforce.jwt.keystore.path` (string, null, medium), `salesforce.instance` (string, blank default, valid https/http URL, high) ([config](https://docs.confluent.io/kafka-connectors/salesforce/current/pushtopics/salesforce_pushtopic_source_connector_config.html)).

### Connection properties
| Property | Type | Default | Valid values | Importance | Description |
|---|---|---|---|---|---|
| `http.proxy` | string | null | `<host>:<port>` | medium | HTTP(S) proxy |
| `connection.timeout` | long | 30000 | [5000,…,600000] | low | Wait time connecting to streaming endpoint |
| `curl.logging` | boolean | false | — | low | Logs equivalent curl commands (security risk — exposes auth header) |
| `connection.max.message.size` | int | null | [0,…,2147483647] | low | Max long-poll message size in bytes |
| `request.max.retries.time.ms` | long | 900000 | [1,…] | low | Max retry duration (15 min) |
| `salesforce.version` | string | latest | regex `^(latest\|[d.]+)$` | low | Salesforce API version |

### Kafka properties
| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `kafka.topic` | string | — | high | Topic template; may use `_EventType`, `_ObjectType` |
| `kafka.topic.lowercase` | boolean | true | high | Lowercase the resolved topic name |

### Salesforce Streaming properties
| Property | Type | Default | Valid values | Importance | Description |
|---|---|---|---|---|---|
| `salesforce.object` | string | — | — | high | SObject to create the PushTopic for |
| `salesforce.push.topic.name` | string | — | — | high | PushTopic to subscribe to (created if `salesforce.push.topic.create=true`) |
| `salesforce.initial.start` | string | latest | regex `^(all\|latest)$` | high | `all` = replayId -2 (all events last 24h); `latest` = replayId -1 (only new) |
| `salesforce.push.topic.create` | boolean | true | — | low | Create the PushTopic if it does not exist |
| `salesforce.push.topic.notify.create` | boolean | true | — | low | PushTopic responds to creates |
| `salesforce.push.topic.notify.delete` | boolean | true | — | low | PushTopic responds to deletes |
| `salesforce.push.topic.notify.undelete` | boolean | true | — | low | PushTopic responds to undeletes |
| `salesforce.push.topic.notify.update` | boolean | true | — | low | PushTopic responds to updates |

Source for the whole table: [PushTopic source config](https://docs.confluent.io/kafka-connectors/salesforce/current/pushtopics/salesforce_pushtopic_source_connector_config.html). Plus all shared license + auto-topic-creation properties.

### Delivery / offset / restart
At-least-once. The connector periodically records the **replayId** of the last record written to Kafka. If stopped and restarted within 24h it resumes with no missed events; if stopped >24h, Salesforce discards events before they can be read. On unexpected failure the last replayId may not have been recorded, so on restart from the last recorded replayId **some events may be duplicated**. When paused, the connector keeps fetching from Salesforce but does not send to Kafka until resumed ([limits](https://docs.confluent.io/cloud/current/connectors/limits.html)).

### Limits
One task per connector (`tasks.max=1`); multiple connectors can run ([limits](https://docs.confluent.io/cloud/current/connectors/limits.html)).

---

## 2. Salesforce Change Data Capture (CDC) Source Connector

- **Class:** `io.confluent.salesforce.SalesforceCdcSourceConnector` ([config](https://docs.confluent.io/kafka-connectors/salesforce/current/change-data-capture/salesforce_cdc_source_connector_config.html)).
- **Type / Platform:** Source; self-managed and Confluent Cloud (config page notes Cloud is separate).
- **Underlying API:** Salesforce Streaming API / CometD over Change Data Capture channels. Subscribes to a single-entity channel, the default `ChangeEvents` channel, or a custom channel ([config](https://docs.confluent.io/kafka-connectors/salesforce/current/change-data-capture/salesforce_cdc_source_connector_config.html)).
- **Behavior:** Monitors Salesforce records for changes (create/update/delete/undelete surfaced as CDC events). Streaming only. Does not support real-time platform-event schema changes — restart connector to sync schema changes. Does not parse fields where Salesforce sends only field-diffs on updated records ([limits](https://docs.confluent.io/cloud/current/connectors/limits.html)).

### Authentication (Connection)
| Property | Type | Default | Valid values | Importance | Description |
|---|---|---|---|---|---|
| `salesforce.consumer.key` | string | — | — | high | OAuth consumer key |
| `salesforce.consumer.secret` | password | — | — | high | OAuth consumer secret |
| `salesforce.password` | password | — | — | high | Salesforce password |
| `salesforce.password.token` | password | — | — | high | Security token |
| `salesforce.username` | string | — | — | high | Salesforce username |
| `salesforce.jwt.keystore.password` | password | null | — | medium | JWT keystore password (OAuth JWT bearer) |
| `salesforce.jwt.keystore.path` | string | null | — | medium | JWT keystore path |
| `salesforce.instance` | string | blank | https/http URL | high | Salesforce endpoint |
| `http.proxy` | string | null | `<host>:<port>` | medium | Proxy |
| `connection.timeout` | long | 30000 | [5000,…,600000] | low | Streaming connect timeout |
| `curl.logging` | boolean | false | — | low | Curl command logging (security risk) |
| `connection.max.message.size` | int | null | [0,…,2147483647] | low | Max long-poll message size |
| `request.max.retries.time.ms` | long | 900000 | [1,…] | low | Max retry duration |
| `salesforce.version` | string | latest | regex `^(latest\|[d.]+)$` | low | API version |

### Kafka
| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `kafka.topic` | string | — | high | Topic template; `_EventType`, `_ObjectType` metadata fields available |
| `kafka.topic.lowercase` | boolean | true | high | Lowercase topic name |

### Salesforce Streaming (CDC-specific)
| Property | Type | Default | Valid values | Importance | Description |
|---|---|---|---|---|---|
| `salesforce.cdc.channel` | string | — | — | high | Channel to subscribe to (single-entity, default `ChangeEvents`, or custom). Renamed from `salesforce.cdc.name` |
| `salesforce.channel.entities` | list | — | — | medium | Entities from the channel to process (N/A for single-entity channel) |
| `salesforce.initial.start` | string | latest | regex `^(all\|latest)$` | high | `all` = replayId -2 (all events last 24h); `latest` = replayId -1 (new only) |
| `invalid.replay.id.behaviour` | string | all | `all`, `latest` | — | Fallback when replayId is invalid/expired. `all` reprocesses all DB events (risk of duplicates); `latest` only new (risk of missing). Retention: 24h standard / 72h high-volume/CDC |

Source: [CDC source config](https://docs.confluent.io/kafka-connectors/salesforce/current/change-data-capture/salesforce_cdc_source_connector_config.html). Plus shared CSFLE, auto-topic-creation, and license properties.

### Delivery / offset / restart
At-least-once via replayId. Salesforce retains events 24h (standard-volume) / 72h (high-volume/CDC); events outside the window are unrecoverable ([config](https://docs.confluent.io/kafka-connectors/salesforce/current/change-data-capture/salesforce_cdc_source_connector_config.html)).

### Limits
User must have **View All Data**. A valid schema must exist in Schema Registry for schema-based formats. Does not support real-time platform-event schema changes (restart to sync). When paused, keeps fetching but does not send until resumed. Does not parse field-diff-only updates ([limits](https://docs.confluent.io/cloud/current/connectors/limits.html)).

---

## 3. Salesforce Platform Event Source Connector

- **Class:** `io.confluent.salesforce.SalesforcePlatformEventSourceConnector` ([overview](https://docs.confluent.io/kafka-connectors/salesforce/current/platformevents/overview.html)).
- **Type / Platform:** Source; self-managed and Cloud.
- **Underlying API:** Streaming API / CometD subscribing to user-defined **Platform Events**. Events must be pre-defined/published in Salesforce (unlike PushTopics which are auto-created) ([overview](https://docs.confluent.io/kafka-connectors/salesforce/current/overview.html)). Event name must end with `__e` ([config](https://docs.confluent.io/kafka-connectors/salesforce/current/platformevents/salesforce_platformevent_source_connector_config.html)).
- **Supported API version:** Salesforce up to API version 65.0 ([overview](https://docs.confluent.io/kafka-connectors/salesforce/current/platformevents/overview.html)).

### Authentication (Connection)
| Property | Type | Default | Valid values | Importance | Description |
|---|---|---|---|---|---|
| `salesforce.consumer.key` | string | — | — | high | OAuth consumer key |
| `salesforce.consumer.secret` | password | — | — | high | OAuth consumer secret |
| `salesforce.password` | password | — | — | high | Password |
| `salesforce.password.token` | password | — | — | high | Security token |
| `salesforce.username` | string | — | — | high | Username |
| `salesforce.jwt.keystore.password` | password | null | — | medium | JWT keystore password |
| `salesforce.jwt.keystore.path` | string | null | — | medium | JWT keystore path |
| `salesforce.instance` | string | blank | https/http URL | high | Endpoint |
| `http.proxy` | string | null | `<host>:<port>` | medium | Proxy |
| `http.proxy.auth.scheme` | string | NONE | [NONE, BASIC] | medium | Proxy auth scheme |
| `http.proxy.user` | string | null | — | medium | Proxy user (BASIC only, HTTP proxy) |
| `http.proxy.password` | password | null | — | medium | Proxy password (BASIC only) |
| `connection.timeout` | long | 30000 | [5000,…,600000] | low | Connect timeout |
| `curl.logging` | boolean | false | — | low | Curl logging (risk) |
| `connection.max.message.size` | int | null | [0,…,2147483647] | low | Max long-poll message size |
| `request.max.retries.time.ms` | long | 900000 | [1,…] | low | Max retry duration |
| `salesforce.version` | string | latest | regex `^(latest\|[d.]+)$` | low | API version |

### Kafka
| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `kafka.topic` | string | — | high | Topic template (`_EventType`, `_ObjectType`) |
| `kafka.topic.lowercase` | boolean | true | high | Lowercase topic |

### Salesforce Streaming (Platform Event-specific)
| Property | Type | Default | Valid values | Importance | Description |
|---|---|---|---|---|---|
| `salesforce.platform.event.name` | string | — | ends with `__e` | high | Platform event to subscribe to (e.g. `LoginEvent__e`) |
| `salesforce.initial.start` | string | latest | regex `^(all\|latest)$` | high | `all` = replayId -2; `latest` = replayId -1 |
| `invalid.replay.id.behaviour` | string | all | `all`, `latest` | — | Fallback for invalid/expired replayId (same semantics as CDC) |

Source: [Platform Event source config](https://docs.confluent.io/kafka-connectors/salesforce/current/platformevents/salesforce_platformevent_source_connector_config.html). Plus shared CSFLE, auto-topic-creation, and license properties.

### Delivery / offset / restart
At-least-once via replayId. Restart within 24h (standard-volume) / 72h (high-volume) resumes with no missed events; longer gaps lose events; unexpected failure may duplicate on restart ([overview](https://docs.confluent.io/kafka-connectors/salesforce/current/platformevents/overview.html)).

### Limits
One task per connector; up to 5 platform events. Custom channels not supported for standard platform events or legacy standard-volume custom platform events (so multiple events not supported in that case). No real-time schema-change support (restart to sync). Paused connector keeps fetching but does not send until resumed. Does not parse field-diff-only updates ([limits](https://docs.confluent.io/cloud/current/connectors/limits.html)).

---

## 4. Salesforce Bulk API Source Connector (Bulk API 1.0, self-managed)

- **Class:** `io.confluent.connect.salesforce.SalesforceBulkApiSourceConnector` ([config](https://docs.confluent.io/kafka-connectors/salesforce/current/salesforce_bulk_api/source/configuration_options.html)).
- **Type / Platform:** Source; self-managed only.
- **Underlying API:** Salesforce **Bulk API 1.0**.
- **Behavior:** Polls a single SObject for records created after `salesforce.since`, publishing to a single Kafka topic. Supports PK-chunking batching for large tables. Poll-based (not streaming) ([config](https://docs.confluent.io/kafka-connectors/salesforce/current/salesforce_bulk_api/source/configuration_options.html)).

### Connection
| Property | Type | Default | Valid values | Importance | Description |
|---|---|---|---|---|---|
| `salesforce.username` | string | — | non-empty | high | Username |
| `salesforce.password` | password | — | — | high | Password |
| `salesforce.password.token` | password | — | — | high | Security token |
| `salesforce.instance` | string | blank | https/http URI | high | Endpoint from auth response |
| `salesforce.object` | string | — | non-empty | high | SObject name |
| `poll.interval.ms` | long | 10000 | [8700,…,2147483647] | high | How often to query Salesforce for new records |
| `batch.enable` | boolean | true | — | medium | Enable PK-chunking batching |
| `batch.max.rows` | int | 100000 | [1,…,250000] | medium | Max rows per Bulk API batch |
| `salesforce.since` | string | current date | `yyyy-MM-dd` | high | Pull records created after this date |
| `request.max.retries.time.ms` | long | 900000 | [1,…] | low | Max retry duration (15 min) |
| `http.proxy` | string | null | — | low | Proxy host:port |
| `http.proxy.auth.scheme` | string | NONE | NTLM, NONE, BASIC | low | Proxy auth scheme |
| `http.proxy.user` | string | "" | — | low | Proxy user |
| `http.proxy.password` | password | [hidden] | — | low | Proxy password |
| `http.proxy.auth.ntlm.domain` | string | null | — | low | NTLM domain |

### Kafka
| Property | Type | Valid values | Importance | Description |
|---|---|---|---|---|
| `kafka.topic` | string | topic-name regex `[a-zA-Z0-9._-]{1,249}` | high | Single destination topic |

Source: [Bulk API source config](https://docs.confluent.io/kafka-connectors/salesforce/current/salesforce_bulk_api/source/configuration_options.html). Plus auto-topic-creation, CSFLE (`csfle.enabled`, `auto.register.schemas`, `use.latest.version`), and license properties.

### Delivery / offset
At-least-once (poll cursor by `CreatedDate`/`salesforce.since`). Data formats: Avro, JSON, JSON_SR, Protobuf (via converters). Authentication: username+password+token only shown (no JWT/consumer-key on this 1.0 source page).

---

## 5. Salesforce Bulk API 2.0 Source Connector (Confluent Cloud)

- **Class / plugin:** `SalesforceBulkApiV2Source` ([Cloud](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-source.html)).
- **Type / Platform:** Source; fully-managed Cloud only.
- **Underlying API:** Salesforce **Bulk API 2.0**; up to Salesforce API version 65.0 ([Cloud](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-source.html)).
- **Behavior:** Pulls records and captures changes by periodic Bulk 2.0 queries. Supports up to 5 SObjects, each routed to its own topic. Supports custom SOQL queries. Records with a time ≥ offset `time` are (re)selected on reset ([Cloud](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-source.html)).

### Delivery / offset / restart
At-least-once; restarts may produce duplicates. Periodically records last query time to the Connect offset topic; on restart it may re-fetch objects with `LastModifiedDate` later than the last queried time. One task per connector ([Cloud](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-source.html), [limits](https://docs.confluent.io/cloud/current/connectors/limits.html)). Offset JSON fields: `time` (required, `yyyy-MM-dd'T'hh:mm:ss'Z'`), `jobId` (optional Bulk v2 job ID, not in Kafka records), `locator` (optional next-batch pointer) ([Cloud](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-source.html)).

### Authentication
`salesforce.grant.type` = PASSWORD (default), CLIENT_CREDENTIALS, or JWT_BEARER. Requirements per grant:
- JWT_BEARER: username, consumer key, JWT keystore file, JWT keystore password
- PASSWORD: username, password, password token, consumer key, consumer secret
- CLIENT_CREDENTIALS: consumer key, consumer secret, and Salesforce domain URL in `salesforce.instance` (default `https://login.salesforce.com` does NOT work); requires Client Credentials flow enabled and an integration user assigned ([Cloud](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-source.html)).

### Configuration properties (fully-managed)
**Kafka credentials:** `name` (string, ≤64 chars, high), `kafka.auth.mode` (KAFKA_API_KEY | SERVICE_ACCOUNT, high), `kafka.api.key` (password, high), `kafka.api.secret` (password, high), `kafka.service.account.id` (string, high).

**Schema:** `schema.context.name` (string, default `default`, medium).

**Salesforce connection:**
| Property | Type | Default | Valid values | Importance |
|---|---|---|---|---|
| `salesforce.grant.type` | string | PASSWORD | PASSWORD/CLIENT_CREDENTIALS/JWT_BEARER | high |
| `salesforce.instance` | string | `https://login.salesforce.com` | — | high |
| `salesforce.username` | string | — | — | high |
| `salesforce.password` | password | — | — | high |
| `salesforce.password.token` | password | — | — | high |
| `salesforce.consumer.key` | password | — | — | high |
| `salesforce.consumer.secret` | password | — | — | medium |
| `salesforce.jwt.keystore.file` | password | [hidden] | Base64 `data:text/plain;base64,...` | medium |
| `salesforce.jwt.keystore.password` | password | — | — | medium |
| `salesforce.object.num` | int | 1 | [1,…,5] | high |
| `poll.interval.ms` | int | 30000 | [8700,…,10800000] | medium |
| `result.max.rows` | int | 1000 | [1,…,3000] | medium |

Note: `result.max.rows` (range 1–3000) is split equally across the configured SObject queries; `poll.interval.ms` timer resets on restart mid-interval ([Cloud](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-source.html)).

**Per-object configuration (object1..objectN, N=1..5):**
| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `salesforce.objectN.enable.custom.query` | boolean | false | medium | Enable custom SOQL for SObjectN |
| `salesforce.objectN.custom.query.fields` | string | — | medium | Comma-separated fields (must include `Id` + offset field) |
| `salesforce.objectN` | string | — | high | SObjectN to read |
| `salesforce.objectN.topic` | string | "" | high | Destination topic for SObjectN |
| `salesforce.objectN.custom.query.where` | password | — | medium | WHERE conditions (no WHERE keyword); must include initial time bound; do not constrain offset field |
| `salesforce.objectN.offset.field.name` | string | LastModifiedDate | medium | Monotonic date/datetime offset field |
| `salesforce.objectN.since` | string | — | medium | `CreatedDate` (UTC, `yyyy-MM-dd`) start |
| `salesforce.objectN.include.deleted.records` | boolean | false | medium | Use `queryAll` to include soft-deleted (recycle bin) records |
| `salesforce.objectN.skip.epoch.conversion.fields` | string | — | medium | Date/datetime fields kept as ISO 8601 strings (schema change risk) |

**Output / additional:** `output.data.format` (AVRO, JSON, JSON_SR, PROTOBUF); JSON converter helpers (`value.converter.replace.null.with.default`, `value.converter.schemas.enable`, `value.converter.decimal.format` BASE64/NUMERIC, `value.converter.ignore.default.for.nullables`, subject-name and schema-ID serializer strategies); `errors.tolerance` (none/all — WARNING: `all` on source may lose data); auto-restart `auto.restart.on.user.error` (default true); `request.max.retries.time.ms` (max retry time, default 30000, min 1000). Source: [Cloud Bulk API 2.0 Source](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-source.html).

### Custom query rules
Fields must include `Id` and the offset field; compound fields (Address, Location) unsupported. WHERE must include an initial bound (e.g. `CreatedDate > 2024-01-01T00:00:00Z`); don't constrain the offset field. No subqueries, GROUP BY, ORDER BY, HAVING, OFFSET, TYPEOF, or aggregate functions. Enabling/modifying custom query resets offset + creates a new partition ([Cloud](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-source.html)).

### Limits
One task per connector. Non-compound fields only; Bulk Query drops address/geolocation fields. Up to 5 SObjects per instance ([limits](https://docs.confluent.io/cloud/current/connectors/limits.html)).

---

## 6. Salesforce Source V2 Connector (Confluent Cloud) — unified

- **Class / plugin:** `SalesforceSourceV2` ([Cloud](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-source-v2.html)).
- **Type / Platform:** Source; fully-managed Cloud only.
- **Underlying APIs:** **Pub/Sub API** (gRPC, HTTP/2, binary Avro) for CDC — the recommended replacement for legacy CometD; **Bulk API 2.0** for historical snapshot and periodic polling; **REST API** for full-record-on-update fetches ([Cloud](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-source-v2.html)).
- **Behavior:** Unified connector: optional historical snapshot (Bulk 2.0) → then one of two real-time modes: Event-Driven Sync (Pub/Sub CDC, default) or Periodic Polling (Bulk 2.0). Snapshot→streaming handoff is seamless with **zero data loss**. Up to 5 SObjects, each with own topic (`{prefix}.{SObject}`), independent Pub/Sub channel, schema cache, offset, bulk job, and subscription ([Cloud](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-source-v2.html)).

### Key behaviors
- **Full record on update:** `cdc.full.record.on.update=true` calls REST once per UPDATE to fetch full post-image (Event-Driven Sync only); CREATE already full, DELETE has no payload. Extra API call/event — quota risk at high update volumes.
- **GAP event reconciliation** (`gap.event.recovery`): `Resync` (default) — incremental Bulk 2.0 re-sync of the affected SObject from later of (last CDC commit, snapshot completion) minus `gap.resync.buffer.ms`; `Latest` — skip gap, resubscribe from newest (loses gap events); `Fail` — stop task. If no watermark exists, Resync falls back to a full Bulk 2.0 re-fetch.
- **Streaming initial start** (`event.sync.start.point`): `latest` (Pub/Sub `LATEST` preset, new only) default; `all` (`EARLIEST`, replay ≤72h retention buffer). After first emitted event, resumes from stored replayId via `CUSTOM` preset; resetting offsets falls back to `event.sync.start.point`.
- **Handoff dedup:** parallel `LATEST` probe subscription stores first replayId during snapshot; at handoff opens `CUSTOM` subscription; CDC events with change timestamp strictly before snapshot completion are dropped.
- **Changed/nulled field detection:** decodes CDC hex bitmaps into headers `source.changed.fields` and `cdc.nulled.fields`.
- **Tombstones:** `emit.tombstone.on.delete=true` → null-value/non-null-key records on DELETE (Event-Driven Sync only), default false.
- **Soft-delete:** `include.deleted.records=true` uses Bulk `queryAll` in snapshot/polling modes; CDC surfaces deletes as DELETE events.
- **Date/datetime:** default epoch millis (`int64`); `skip.epoch.conversion.fields` keeps ISO 8601 (`string`) — schema evolution risk.

All from [Cloud Source V2 page](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-source-v2.html).

### Authentication
`salesforce.grant.type` = PASSWORD, CLIENT_CREDENTIALS, or JWT_BEARER (same requirements as Bulk 2.0 Source above). For CLIENT_CREDENTIALS use domain URL in `salesforce.instance` (default `https://login.salesforce.com` fails); enable Client Credentials flow + integration user. When Event-Driven Sync is used, CDC must be explicitly enabled for every SObject in `sobject.names`; the Salesforce user needs View All Data, API Enabled, View Setup and Configuration, and Read on target objects ([Cloud](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-source-v2.html)).

### Configuration properties (selected; fully-managed)
| Property | Type | Default | Valid values | Importance | Description |
|---|---|---|---|---|---|
| `connector.class` | — | `SalesforceSourceV2` | — | — | Plugin name |
| `sobject.names` | list | — | — | high | Comma-separated SObject API names (up to 5), exact casing |
| `topic.prefix` | string | — | — | high | Topics named `{prefix}.{SObject}` |
| `historical.snapshot` | boolean | true | — | high | Bulk 2.0 initial load from `since` |
| `since` | string | today | `yyyy-MM-dd` | medium | Snapshot CreatedDate start (only if snapshot enabled) |
| `real.time.ingestion.mode` | string | `Event-Driven Sync (using Pub/Sub API)` | Event-Driven Sync / Periodic Polling (using Bulk API 2.0) | high | Continuous ingestion mode |
| `event.sync.start.point` | string | latest | all, latest | medium | Cold-start replay position |
| `gap.event.recovery` | string | resync | resync, latest, fail | high | GAP handling |
| `cdc.full.record.on.update` | boolean | false | — | medium | Full record on UPDATE (Event-Driven Sync only) |
| `emit.tombstone.on.delete` | boolean | false | — | medium | Tombstones on DELETE (Event-Driven Sync only) |
| `gap.resync.buffer.ms` | long | 60000 | — | low | Safety buffer subtracted during resync |
| `poll.interval.ms` | — | 500 | — | — | Wait for new change events when none returned |
| `result.max.rows` (Max records per result set) | — | 1000 | — | — | Max Bulk 2.0 records per result set across SObjects |
| `include.deleted.records` | boolean | — | — | — | `queryAll` include soft-deleted |
| `skip.epoch.conversion.fields` | string | — | — | — | Keep as ISO 8601 |
| `output.data.format` | string | — | AVRO, JSON_SR, PROTOBUF | — | Output value format (Schema Registry required) |
| `errors.tolerance` | string | none | none, all | low | `all` may lose data on source |
| `request.max.retries.time.ms` (Max retry time) | — | 30000 | ≥1000 | — | Retry budget |
| `auto.restart.on.user.error` | boolean | true | — | — | Auto-restart on user-actionable errors |

Source: [Cloud Source V2 page](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-source-v2.html). Data + null-with-default converter helpers and schema-ID/subject-name strategies also present (same list as Bulk 2.0 Source).

### Delivery / offset / restart
Zero-data-loss handoff; at-least-once overall. One offset entry per SObject with `mode` ∈ {Historical Snapshot, Periodic Polling, Event-Driven Sync}. Offset JSON fields: `mode` (required), `offset.version`=1 (required), `lastId`, `lastSystemModstamp` (`yyyy-MM-dd'T'HH:mm:ss.SSSZ`), `jobId`, `locator` (Bulk modes), `snapshotCompletionTimestamp`, `replayId` (base64 Pub/Sub replay ID), `commitTimestamp`, `commitNumber` (Event-Driven Sync). Events outside 72h retention are discarded → `gap.event.recovery` applied. PATCH updates only listed SObjects; DELETE resets all; per-SObject null offset resets one ([Cloud](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-source-v2.html)).

### Retries
Classifies by HTTP/gRPC status; retries HTTP 429/5xx, Bulk daily-quota 403 `REQUEST_LIMIT_EXCEEDED`, gRPC `UNAVAILABLE`/`DEADLINE_EXCEEDED`/`RESOURCE_EXHAUSTED`, network timeouts — exponential backoff + jitter, capped by `request.max.retries.time.ms` (default 30s) ([Cloud](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-source-v2.html)).

### Limits
No custom SOQL. No Big Objects (`__b`) or External Objects (`__x`). `tasks.max` capped at #SObjects (extra slots idle, logged INFO); tasks round-robin across SObjects; no reduction to single task when streaming begins. Compound fields (Address, Location) not populated — only component fields. No per-SObject customization — all SObjects share connector settings ([limits](https://docs.confluent.io/cloud/current/connectors/limits.html)).

---

## 7. Salesforce SObject Sink Connector

- **Class:** `io.confluent.salesforce.SalesforceSObjectSinkConnector` ([SObject sink quickstart](https://docs.confluent.io/kafka-connectors/salesforce/current/sobjects_sink/salesforce_sobject_sink_connector_quickstart.html)).
- **Type / Platform:** Sink; self-managed and Cloud.
- **Underlying API:** Salesforce REST API. Consumes Kafka topics and performs create/update/delete/upsert on SObjects ([overview](https://docs.confluent.io/kafka-connectors/salesforce/current/overview.html)). Requires input record format identical to the PushTopic/CDC source output ([overview](https://docs.confluent.io/kafka-connectors/salesforce/current/overview.html)).
- **Behavior:** By default maps `_EventType` (created/updated/deleted) to insert/update/delete on the SObject; override with `override.event.type=true` + `salesforce.sink.object.operation`. Read-only Salesforce fields (`creatable=false`/`updatable=false`) are silently excluded, not failed (this is not configurable, per the Bulk sink; SObject sink follows the same family behavior) ([Bulk sink overview](https://docs.confluent.io/kafka-connectors/salesforce/current/salesforce_bulk_api/sink/overview.html)).

### Connection / auth
| Property | Type | Default | Valid values | Importance | Description |
|---|---|---|---|---|---|
| `salesforce.consumer.key` | string | — | — | high | OAuth consumer key |
| `salesforce.consumer.secret` | password | — | — | high | OAuth consumer secret |
| `salesforce.password` | password | — | — | high | Password |
| `salesforce.username` | string | — | — | high | Username |
| `salesforce.jwt.keystore.password` | password | null | — | medium | JWT keystore password |
| `salesforce.jwt.keystore.path` | string | null | — | medium | JWT keystore path |
| `salesforce.instance` | string | blank | https/http URL | high | Endpoint |
| `salesforce.password.token` | password | null | — | high | Security token |
| `http.proxy` | string | null | `<host>:<port>` | medium | Proxy |
| `http.proxy.auth.scheme` | string | NONE | NONE, NTLM, BASIC | medium | Proxy auth scheme |
| `http.proxy.user` | string | NONE | — | medium | Proxy user |
| `http.proxy.password` | password | null | — | medium | Proxy password |
| `http.proxy.auth.ntlm.domain` | string | null | — | medium | NTLM domain |
| `connection.timeout` | long | 30000 | [5000,…,600000] | low | Connect timeout |
| `curl.logging` | boolean | false | — | low | Curl logging (risk) |
| `request.max.retries.time.ms` | long | 900000 | [1,…] | low | Max retry duration |
| `salesforce.version` | string | latest | regex `^(latest\|[d.]+)$` | low | API version |

### SObject sink behavior
| Property | Type | Default | Valid values | Importance | Description |
|---|---|---|---|---|---|
| `salesforce.object` | string | — | — | high | Target SObject |
| `topics` | string | — | — | high | Source Kafka topic(s) |
| `behavior.on.api.errors` | string | fail | regex `^(log\|ignore\|fail)$` | low | fail stops; ignore skips; log logs+continues |
| `override.event.type` | boolean | false | — | low | Override `_EventType` with `salesforce.sink.object.operation` |
| `salesforce.custom.id.field.name` | string | null | — | low | Custom external ID field; used when `salesforce.use.custom.id.field=true` |
| `salesforce.ignore.fields` | string | "" | — | low | Fields to ignore when pushing |
| `salesforce.ignore.reference.fields` | boolean | false | — | low | Prevent reference-type field writes |
| `salesforce.sink.object.operation` | string | insert | regex `^(insert\|update\|upsert\|delete)$` | low | Operation when override enabled |
| `salesforce.use.custom.id.field` | boolean | false | — | low | Use custom external ID for all operations |
| `skip.relationship.fields` | boolean | true | true, false | — | Skip relationship fields on write |
| `topics.regex` | string | null | — | low | Java regex for source topics |

### Connect Reporter (result/error topics)
`reporter.result.topic.name` (string, `${connector}-success`, medium), `reporter.result.topic.replication.factor` (short, 3, medium), `reporter.result.topic.partitions` (int, 1, medium), `reporter.error.topic.name` (string, `${connector}-error`, medium), `reporter.error.topic.replication.factor` (short, 3, medium), `reporter.error.topic.partitions` (int, 1, medium), `reporter.bootstrap.servers` (list, non-empty, high). Formatter: `reporter.{result,error}.topic.{key,value}.format` (json, [string,json], medium) and matching `.schemas.cache.size` (int, 128, [0,…,2048]) and `.schemas.enable` (boolean, false). Source: [SObject sink config](https://docs.confluent.io/kafka-connectors/salesforce/current/sobjects_sink/salesforce_sobject_sink_connector_config.html). Plus shared CSFLE + license properties.

### ID semantics / cross-org (family behavior)
`Id` is only valid within the originating org; inserts ignore `Id`. For update/delete/upsert across orgs use `salesforce.use.custom.id.field=true` + `salesforce.custom.id.field.name=<externalIdField>`, and the custom ID must be marked external in both orgs ([Bulk sink overview](https://docs.confluent.io/kafka-connectors/salesforce/current/salesforce_bulk_api/sink/overview.html), [SObject sink quickstart](https://docs.confluent.io/kafka-connectors/salesforce/current/sobjects_sink/salesforce_sobject_sink_connector_quickstart.html)).

### Limits
Subject to Salesforce 24h API limits (esp. free dev orgs) and org-type limits. Up to 5 SObjects/instance. Batching requires API version 42.0+; upsert with batching requires 46.0+; deletion by external ID in batches unsupported → falls back to individual processing; max 200 records/batch ([limits](https://docs.confluent.io/cloud/current/connectors/limits.html)).

---

## 8. Salesforce Platform Event Sink Connector

- **Class:** `io.confluent.salesforce.SalesforcePlatformEventSinkConnector` ([Platform Event Sink config](https://docs.confluent.io/kafka-connectors/salesforce/current/platformevents_sink/salesforce_platformevent_sink_connector_config.html), [overview](https://docs.confluent.io/kafka-connectors/salesforce/current/platformevents_sink/overview.html)).
- **Type / Platform:** Sink; self-managed and Cloud.
- **Underlying API:** REST API to **publish Platform Events**. Cannot insert/update/delete standard/custom objects — only publishes events. Requires input records in the same format as the Platform Events source output ([overview](https://docs.confluent.io/kafka-connectors/salesforce/current/overview.html)). Event name must match `.*__e$`. Platform event definitions must pre-exist in Salesforce ([config](https://docs.confluent.io/kafka-connectors/salesforce/current/platformevents_sink/salesforce_platformevent_sink_connector_config.html), [overview](https://docs.confluent.io/kafka-connectors/salesforce/current/platformevents_sink/overview.html)).
- **Supports multiple tasks:** yes, via `tasks.max` ([overview](https://docs.confluent.io/kafka-connectors/salesforce/current/platformevents_sink/overview.html)).

### Connection / auth
| Property | Type | Default | Valid values | Importance | Description |
|---|---|---|---|---|---|
| `salesforce.consumer.key` | string | — | — | high | OAuth consumer key |
| `salesforce.consumer.secret` | password | — | — | high | OAuth consumer secret |
| `salesforce.password` | password | — | — | high | Password |
| `salesforce.username` | string | — | — | high | Username |
| `salesforce.instance` | string | blank | https/http URL | high | Endpoint |
| `salesforce.password.token` | password | null | — | high | Security token |
| `salesforce.jwt.keystore.password` | password | null | — | medium | JWT keystore password |
| `salesforce.jwt.keystore.path` | string | null | — | medium | JWT keystore path |
| `http.proxy` | string | null | `<host>:<port>` | medium | Proxy |
| `http.proxy.auth.scheme` | string | NONE | NONE, NTLM, BASIC | medium | Proxy auth scheme |
| `http.proxy.user` | string | NONE | — | medium | Proxy user |
| `http.proxy.password` | password | null | — | medium | Proxy password |
| `http.proxy.auth.ntlm.domain` | string | null | — | medium | NTLM domain |
| `connection.timeout` | long | 30000 | [5000,…,600000] | low | Connect timeout |
| `curl.logging` | boolean | false | — | low | Curl logging (risk) |
| `request.max.retries.time.ms` | long | 900000 | [1,…] | low | Max retry duration |
| `salesforce.version` | string | latest | regex `^(latest\|[d.]+)$` | low | API version |
| `behavior.on.api.errors` | string | fail | regex `^(log\|ignore\|fail)$` | low | Error handling |

JWT bearer requires `salesforce.username`, `salesforce.consumer.key`, `salesforce.jwt.keystore.path`, `salesforce.jwt.keystore.password` ([overview](https://docs.confluent.io/kafka-connectors/salesforce/current/platformevents_sink/overview.html)).

### Salesforce Streaming
| Property | Type | Valid values | Importance | Description |
|---|---|---|---|---|
| `salesforce.platform.event.name` | string | matches `.*__e$` | high | Platform event to publish to (e.g. `LoginEvent__e`) |

### Connect Reporter + license
Same Connect Reporter block as the SObject sink (result/error topic name/RF/partitions, `reporter.bootstrap.servers`, formatter + JSON formatter cache/enable) and the shared license properties ([config](https://docs.confluent.io/kafka-connectors/salesforce/current/platformevents_sink/salesforce_platformevent_sink_connector_config.html)). `topics` (list) selects source Kafka topics ([overview](https://docs.confluent.io/kafka-connectors/salesforce/current/platformevents_sink/overview.html)).

> Correction note: An earlier extraction attempt of the Platform Event Sink overview produced a fabricated property list (`salesforce.event.id`, `salesforce.batch.max.records`, `salesforce.mapping`, `salesforce.version=34.0`, etc.). Those are **not** confirmed on the authoritative config page and should be disregarded. The verified config page ([here](https://docs.confluent.io/kafka-connectors/salesforce/current/platformevents_sink/salesforce_platformevent_sink_connector_config.html)) shows `salesforce.version` default `latest` and no batch/mapping properties.

### Limits
Subject to Salesforce 24h API-call limits (esp. free dev orgs) and org-type limits. Up to 5 events/instance ([limits](https://docs.confluent.io/cloud/current/connectors/limits.html)).

---

## 9. Salesforce Bulk API Sink Connector (Bulk API 1.0, self-managed)

- **Class:** `io.confluent.connect.salesforce.SalesforceBulkApiSinkConnector` ([overview](https://docs.confluent.io/kafka-connectors/salesforce/current/salesforce_bulk_api/sink/overview.html), [config](https://docs.confluent.io/kafka-connectors/salesforce/current/salesforce_bulk_api/sink/configuration_options.html)).
- **Type / Platform:** Sink; self-managed only.
- **Underlying API:** Salesforce **Bulk API 1.0**. Performs insert/update/delete (and upsert with external ID) on SObjects from Kafka records ([overview](https://docs.confluent.io/kafka-connectors/salesforce/current/salesforce_bulk_api/sink/overview.html)).
- **Supported API version:** up to Salesforce API version 64.0 ([overview](https://docs.confluent.io/kafka-connectors/salesforce/current/salesforce_bulk_api/sink/overview.html)).
- **Behavior:** Input format must match the Bulk API/PushTopic source output. Maps `_EventType` created/updated/deleted → insert/update/delete unless overridden. Read-only fields silently excluded (not configurable). Does not support relation fields. Bulk API supports upsert only with an External ID field ([overview](https://docs.confluent.io/kafka-connectors/salesforce/current/salesforce_bulk_api/sink/overview.html)).

### Authentication
| Property | Type | Valid values | Importance | Description |
|---|---|---|---|---|
| `salesforce.username` | string | non-empty | high | Username |
| `salesforce.password` | password | — | high | Password |
| `salesforce.password.token` | password | — | high | Security token |

(Note: this 1.0 sink config page lists only username+password+token for auth — no consumer key/JWT shown.)

### Salesforce instance / object
| Property | Type | Default | Valid values | Importance | Description |
|---|---|---|---|---|---|
| `salesforce.instance` | string | blank | https/http URI | high | Endpoint |
| `salesforce.version` | string | 48.0 | — | high | Salesforce version (config default 48.0; overview says up to 64.0 supported) |
| `salesforce.object` | string | — | non-empty | high | Target SObject |
| `salesforce.sink.object.operation` | string | INSERT | UPSERT, INSERT, UPDATE, DELETE | low | Operation when `override.event.type=true` |
| `override.event.type` | boolean | false | — | low | Override `_EventType` |
| `salesforce.custom.id.field.name` | string | null | — | low | Custom external ID field (INSERT/UPSERT) |
| `salesforce.use.custom.id.field` | boolean | false | — | low | Use custom external ID |
| `salesforce.ignore.fields` | list | "" | — | low | Fields to ignore |
| `salesforce.ignore.reference.fields` | boolean | false | — | low | Prevent reference-type writes |

### Proxy
`http.proxy` (string, null, low), `http.proxy.auth.scheme` (string, NONE, [NTLM, NONE, BASIC], low), `http.proxy.user` (string, "", low), `http.proxy.password` (password, [hidden], low), `http.proxy.auth.ntlm.domain` (string, null, low).

### Error handling
`behavior.on.api.errors` (string, fail, [fail, log, ignore], low) — fail stops; ignore next record; log logs+continues.

### Connect Reporter + CSFLE + license
Same Connect Reporter block, CSFLE (`csfle.enabled`, `auto.register.schemas`, `use.latest.version`), and license properties as documented in the shared section. Source: [Bulk API sink config](https://docs.confluent.io/kafka-connectors/salesforce/current/salesforce_bulk_api/sink/configuration_options.html).

### Errors / considerations
Unexpected errors reported for duplicate inserts, update/delete/upsert of nonexistent Id, or operating on a previously-deleted Id. Limited by number of batches, records per batch, and batch length ([overview](https://docs.confluent.io/kafka-connectors/salesforce/current/salesforce_bulk_api/sink/overview.html)).

---

## 10. Salesforce Bulk API 2.0 Sink Connector (Confluent Cloud)

- **Class / plugin:** `SalesforceBulkApiV2Sink` ([Cloud](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-sink.html)).
- **Type / Platform:** Sink; fully-managed Cloud only.
- **Underlying API:** Salesforce **Bulk API 2.0**; up to Salesforce API version 65.0 ([Cloud](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-sink.html)).
- **Behavior:** insert/update/delete on SObjects from Kafka topics. At-least-once. Supports multiple tasks (more tasks reduce consumer lag). Supports Salesforce relationship fields (Lookup via External ID — not polymorphic). Supports Big Objects (`__b`, insert-only; `_EventType` updated/deleted routed to error topic; omit `_EventType`; relationship fields unsupported; non-indexed fields need FLS grants). Up to 5 SObjects, each topic mapped to exactly one SObject ([Cloud](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-sink.html)).
- **Data formats:** Avro, JSON_SR, Protobuf input (Schema Registry required).

### Authentication
`salesforce.grant.type` = PASSWORD (default), JWT_BEARER, or CLIENT_CREDENTIALS (same per-grant requirements as the Bulk 2.0 source) ([Cloud](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-sink.html)).

### Configuration properties (fully-managed)
**Topics/DLQ/reporter:** `topics` (list, high), `topics.regex` (string, low), `errors.deadletterqueue.topic.name` (string, `dlq-${connector}`, low), `reporter.result.topic.name` (string, `success-${connector}`, low), `reporter.error.topic.name` (string, `error-${connector}`, low).

**Schema / input:** `schema.context.name` (string, default, medium), `input.data.format` (AVRO, JSON_SR, PROTOBUF, high).

**Kafka credentials:** `name` (string ≤64, high), `kafka.auth.mode` (KAFKA_API_KEY|SERVICE_ACCOUNT, high), `kafka.api.key` (password, high), `kafka.service.account.id` (string, high), `kafka.api.secret` (password, high).

**Salesforce connection:**
| Property | Type | Default | Valid | Importance |
|---|---|---|---|---|
| `salesforce.grant.type` | string | PASSWORD | PASSWORD/CLIENT_CREDENTIALS/JWT_BEARER | high |
| `salesforce.instance` | string | `https://login.salesforce.com` | — | high |
| `salesforce.username` | string | — | — | high |
| `salesforce.password` | password | — | — | high |
| `salesforce.password.token` | password | — | — | high |
| `salesforce.consumer.key` | password | — | — | high |
| `salesforce.consumer.secret` | password | — | — | medium |
| `salesforce.jwt.keystore.file` | password | [hidden] | Base64 | medium |
| `salesforce.jwt.keystore.password` | password | — | — | medium |
| `salesforce.object.type` | string | STANDARD_OR_CUSTOM_OBJECT | STANDARD_OR_CUSTOM_OBJECT, BIG_OBJECT | high |
| `salesforce.object.num` | int | 1 | [1,…,5] | high |
| `salesforce.version` | string | 65.0 | — | low |

**Per-object (object1..N):** `salesforce.objectN` (string, high), `salesforce.objectN.topics` (list, "", high), `salesforce.objectN.override.event.type` (boolean, false, low), `salesforce.objectN.sink.object.operation` (string, insert, low), `salesforce.objectN.ignore.fields` (list, "", low), `salesforce.objectN.ignore.reference.fields` (boolean, false, low), `salesforce.objectN.use.custom.id.field` (boolean, low), `salesforce.objectN.custom.id.field.name` (string, low), `skip.objectN.relationship.fields` (boolean, low — set false + `ignore.reference.fields=false` to enable relationship fields).

**Error/consumer/connection:** `behavior.on.api.errors` (fail | ignore [default]), `max.timeout.ms` (default 200000 — max wait for all batch ops), `max.poll.interval.ms` (default 300000), `max.poll.records` (default 500), `request.max.retries.time.ms` (Max retry time, default 30000, min 1000), `errors.tolerance` (all/none), `auto.restart.on.user.error` (default true). Converter/subject-name/schema-ID strategies as with other Cloud connectors. Source: [Cloud Bulk API 2.0 Sink](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-sink.html).

### Delivery
At-least-once; restarts may duplicate. Supports multiple tasks ([Cloud](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-sink.html)).

### Limits
Salesforce imposes 24h-window limits; exceeding fails the connector. Daily limits on records/batches/data size; org-type limits. Up to 5 objects/instance ([limits](https://docs.confluent.io/cloud/current/connectors/limits.html)).

---

## 11. Other connectors found in the docs index

Beyond the 10 above, the Confluent docs index surfaces these additional standalone fully-managed Cloud plugins (each a distinct connector; the unified Source V2 is intended to supersede several):

- **Salesforce CDC Source (Confluent Cloud)** — fully-managed cloud sibling of connector #2 (the self-managed CDC config page explicitly points cloud users to a separate cloud page). Uses CometD CDC channels. Limits documented on the [limits page](https://docs.confluent.io/cloud/current/connectors/limits.html) (View All Data required; no real-time schema changes; one task per connector class family).
- **Salesforce Platform Event Source (Confluent Cloud)** — fully-managed cloud sibling of #3, at `cc-salesforce-platform-event-source.html` (surfaced in search) ([search result URL](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-platform-event-source.html)). Up to 5 platform events, one task per connector ([limits](https://docs.confluent.io/cloud/current/connectors/limits.html)).
- **Salesforce PushTopic Source (Confluent Cloud)** — fully-managed cloud sibling of #1 (config page references a Cloud variant). One task per connector; at-least-once with replayId ([limits](https://docs.confluent.io/cloud/current/connectors/limits.html)).
- **Salesforce SObject Sink (Confluent Cloud)** and **Salesforce Platform Event Sink (Confluent Cloud)** — cloud siblings of #7 and #8, listed on the [limits page](https://docs.confluent.io/cloud/current/connectors/limits.html) with the up-to-5-objects/events constraints.

The seven **Connector-Pack license types** enumerated by Confluent for self-managed deployments are: Salesforce PushTopic Source, Salesforce CDC Source, Salesforce Platform Event Source, Salesforce Platform Event Sink, Salesforce SObject Sink, Salesforce Bulk API Source, and Salesforce Bulk API Sink ([overview](https://docs.confluent.io/kafka-connectors/salesforce/current/overview.html)).

---

## Appendix A — Authentication Methods Cross-Reference

| Auth method | PushTopic Src | CDC Src | PlatEvent Src | Bulk1.0 Src | Bulk2.0 Src (Cloud) | Source V2 | SObject Sink | PlatEvent Sink | Bulk1.0 Sink | Bulk2.0 Sink (Cloud) |
|---|---|---|---|---|---|---|---|---|---|---|
| Username + password + token | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| OAuth2 consumer key/secret | ✅ | ✅ | ✅ | n.a. (page shows only user/pass) | ✅ | ✅ | ✅ | ✅ | n.a. | ✅ |
| OAuth2 JWT bearer (keystore) | ✅ | ✅ | ✅ | n.a. | ✅ | ✅ | ✅ | ✅ | n.a. | ✅ |
| OAuth2 Client Credentials flow | n.a. | n.a. | n.a. | n.a. | ✅ | ✅ | n.a. | n.a. | n.a. | ✅ |

Sources: self-managed pages ([PushTopic](https://docs.confluent.io/kafka-connectors/salesforce/current/pushtopics/salesforce_pushtopic_source_connector_config.html), [CDC](https://docs.confluent.io/kafka-connectors/salesforce/current/change-data-capture/salesforce_cdc_source_connector_config.html), [PlatEvent Src](https://docs.confluent.io/kafka-connectors/salesforce/current/platformevents/salesforce_platformevent_source_connector_config.html), [Bulk1.0 Src](https://docs.confluent.io/kafka-connectors/salesforce/current/salesforce_bulk_api/source/configuration_options.html), [SObject Sink](https://docs.confluent.io/kafka-connectors/salesforce/current/sobjects_sink/salesforce_sobject_sink_connector_config.html), [PlatEvent Sink](https://docs.confluent.io/kafka-connectors/salesforce/current/platformevents_sink/salesforce_platformevent_sink_connector_config.html), [Bulk1.0 Sink](https://docs.confluent.io/kafka-connectors/salesforce/current/salesforce_bulk_api/sink/configuration_options.html)); Cloud pages ([Bulk2.0 Src](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-source.html), [Source V2](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-source-v2.html), [Bulk2.0 Sink](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-sink.html)).

## Appendix B — Supported Salesforce API Version Cross-Reference

| Connector | Supported API version | Source |
|---|---|---|
| PushTopic / CDC / Platform Event Source (self-managed) | `salesforce.version` default `latest`; Platform Events "up to 65.0" | [PushTopic](https://docs.confluent.io/kafka-connectors/salesforce/current/pushtopics/salesforce_pushtopic_source_connector_config.html), [PlatEvent overview](https://docs.confluent.io/kafka-connectors/salesforce/current/platformevents/overview.html) |
| Bulk API 1.0 Sink | up to 64.0 (config default `48.0`) | [overview](https://docs.confluent.io/kafka-connectors/salesforce/current/salesforce_bulk_api/sink/overview.html), [config](https://docs.confluent.io/kafka-connectors/salesforce/current/salesforce_bulk_api/sink/configuration_options.html) |
| SObject Sink / Platform Event Sink (self-managed) | `salesforce.version` default `latest` | [SObject config](https://docs.confluent.io/kafka-connectors/salesforce/current/sobjects_sink/salesforce_sobject_sink_connector_config.html), [PlatEvent Sink config](https://docs.confluent.io/kafka-connectors/salesforce/current/platformevents_sink/salesforce_platformevent_sink_connector_config.html) |
| Bulk API 2.0 Source / Sink, Source V2 (Cloud) | up to 65.0 | [Bulk2.0 Src](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-source.html), [Bulk2.0 Sink](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-sink.html) |

## Appendix C — Data Formats & Metadata

- Self-managed connectors use standard Kafka Connect converters; Avro, JSON (schemaless), JSON Schema, and Protobuf all supported via Schema Registry. Cloud connectors: Bulk 2.0 Source supports AVRO/JSON/JSON_SR/PROTOBUF; Source V2 and Bulk 2.0 Sink support AVRO/JSON_SR/PROTOBUF (Schema Registry required) ([Bulk2.0 Src](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-source.html), [Source V2](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-source-v2.html), [Bulk2.0 Sink](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-bulk-api-v2-sink.html)).
- Streaming source records carry `_EventType` (created/updated/deleted) and `_ObjectType` metadata fields ([PushTopic config](https://docs.confluent.io/kafka-connectors/salesforce/current/pushtopics/salesforce_pushtopic_source_connector_config.html)).
- Source V2 exposes changed/nulled field names as Kafka headers `source.changed.fields` and `cdc.nulled.fields` ([Source V2](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-source-v2.html)).
- Sink connectors expect input records in the same structure as the corresponding source output ([overview](https://docs.confluent.io/kafka-connectors/salesforce/current/overview.html)).

---

*All values above are sourced from the linked Confluent documentation pages fetched during this research. Values that could not be confirmed from a fetched page are explicitly marked "n.a." Two items warrant verification against the plugin JARs before implementation: (1) the exact PushTopic Source `connector.class` string, and (2) the earlier (rejected) fabricated Platform Event Sink property list.*
