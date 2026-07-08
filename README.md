# Kafka Connect Salesforce (OSS)

Open-source, Apache-2.0 Salesforce connectors for Apache Kafka Connect — a modern,
**Pub/Sub API-first** alternative to Confluent's proprietary Salesforce connector suite.
Four connectors deliver feature parity with Confluent's eleven:

| Module | Connector class | What it does |
|---|---|---|
| `connect-salesforce-source` | `sh.oso.salesforce.source.SalesforceSourceConnector` | Unified source: historical snapshot (Bulk API 2.0) + real-time CDC/Platform Events (Pub/Sub API gRPC) or periodic polling, multi-SObject, seamless snapshot→stream handoff, gap recovery |
| `connect-salesforce-sink` | `sh.oso.salesforce.sink.SalesforceSinkConnector` | SObject sink: insert/update/upsert (external ID)/delete via Bulk API 2.0 or REST Composite, Big Objects, DLQ |
| `connect-salesforce-pe-sink` | `sh.oso.salesforce.pesink.SalesforcePlatformEventSinkConnector` | Publishes Kafka records as Platform Events (`*__e`) via Pub/Sub API with REST fallback |
| `connect-salesforce-streaming-source` | `sh.oso.salesforce.streaming.SalesforceStreamingSourceConnector` | Legacy CometD/Bayeux source (PushTopics, CDC, Platform Events) for orgs without Pub/Sub API access |

All Salesforce protocol logic lives in the shared **`sf-core`** library: OAuth
(Client Credentials, JWT Bearer, legacy Username-Password), Pub/Sub API gRPC client with
Avro decoding and replayId checkpointing, Bulk API 2.0 query + ingest clients, REST/SOQL
client with describe caching, Avro/describe → Connect schema mapping, and a rate-limit
governor that polls `/limits/` and backs off before quota exhaustion.

## Build

Requires JDK 17+ and Maven 3.6.3+:

```bash
mvn clean verify
```

Each connector module produces a Kafka Connect plugin ZIP under
`<module>/target/*-kafka-connect-plugin.zip` — unzip onto your worker's `plugin.path`.

## Quickstart

See [`examples/`](examples/) for a docker-compose stack (Kafka + Connect with the plugins
mounted) and ready-to-POST connector configs for all three primary connectors.

Salesforce-side setup: a Connected App with the Client Credentials flow enabled (run-as an
API-Only integration user) — `sf.instance.url` must be the org's **My Domain** URL;
`login.salesforce.com` is not valid for that flow. JWT Bearer is supported via
`sf.auth.grant.type=jwt_bearer` + `sf.jwt.keystore.path` (PEM/JKS/PKCS12).

## Delivery semantics

At-least-once everywhere (matching Confluent's connectors). The source checkpoints the
Pub/Sub `replayId` (opaque bytes) and Bulk `SystemModstamp` cursor in Kafka Connect's
offset store; restarts inside the 72-hour event-retention window resume without loss, and
older restarts trigger the configured `sf.gap.recovery` (incremental Bulk resync by
default). Sinks are idempotent when you use external-ID upsert.

## Documentation

Full documentation (connector config references, concepts, migration guide) lives in
[`website/`](website/) and is served with Docusaurus via GitHub Pages
(`.github/workflows/docs-deploy.yml`). Run it locally with:

```bash
cd website && npm install && npm start
```

## Testing

Everything is testable locally with no Salesforce org — see
[`docs/testing_strategy.md`](docs/testing_strategy.md). The `sf-core` test-jar ships a
fake Salesforce: a WireMock-based HTTP surface (OAuth/REST/Bulk 2.0 job lifecycle), an
in-process gRPC implementation of the Pub/Sub API (`eventbus.v1.PubSub`) with replay
semantics, and a Bayeux/CometD protocol stub with the Salesforce replay extension.

`mvn clean verify` also runs the **docker end-to-end tests** (`e2e-tests/`): a real
Kafka broker plus a real Kafka Connect worker load the packaged plugin directories and
run the source path (fake CDC → Kafka topic) and sink path (Kafka topic → fake Bulk
ingest) against the harness. Skip them with `-DskipE2E` when Docker is unavailable.

## Releasing

Releases are automated with release-please (conventional commits) and published to Maven
Central (Sonatype Central Portal, `sh.oso` namespace) by `.github/workflows/release.yml`.
Required repository secrets: `CENTRAL_USERNAME`, `CENTRAL_TOKEN`, `MAVEN_GPG_PRIVATE_KEY`,
`MAVEN_GPG_PASSPHRASE`. Connector plugin ZIPs are attached to each GitHub release.

## Design docs

The clean-room research and PRDs this implementation follows live in [`docs/`](docs/) —
start with `docs/00_feasibility_and_cleanroom_report.md`. This code was written from
Salesforce's public API documentation only; it contains no Confluent code and uses its own
`sf.*` configuration namespace (a Confluent→OSS config migration table is in each PRD).

## License

Apache License 2.0 — see [LICENSE](LICENSE).
