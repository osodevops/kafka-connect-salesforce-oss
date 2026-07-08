<p align="center">
  <h1 align="center">kafka-connect-salesforce</h1>
  <p align="center">
    Open-source Salesforce connectors for Apache Kafka — Pub/Sub API, Bulk API 2.0, Kafka Connect
  </p>
</p>

<p align="center">
  <a href="https://github.com/osodevops/kafka-connect-salesforce-oss/actions/workflows/ci.yml">
    <img src="https://github.com/osodevops/kafka-connect-salesforce-oss/actions/workflows/ci.yml/badge.svg" alt="CI Status">
  </a>
  <a href="https://github.com/osodevops/kafka-connect-salesforce-oss/blob/main/LICENSE">
    <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License: Apache-2.0">
  </a>
  <a href="https://github.com/osodevops/kafka-connect-salesforce-oss/releases">
    <img src="https://img.shields.io/github/v/release/osodevops/kafka-connect-salesforce-oss" alt="Release">
  </a>
</p>

---

**kafka-connect-salesforce** is a production-grade, Apache-2.0 suite of Salesforce connectors for Kafka Connect — the **open-source alternative to Confluent's proprietary Salesforce connectors**. Four connectors deliver functional parity with Confluent's eleven, built clean-room on Salesforce's modern **Pub/Sub API (gRPC)** and **Bulk API 2.0**.

## Features

- **Real-time CDC streaming** — Change Data Capture over the Pub/Sub API with replayId checkpointing and 72h replay
- **Zero-loss snapshot handoff** — Bulk API 2.0 historical backfill that transitions to live CDC with no gap and no duplicates
- **Salesforce sinks** — insert/update/external-ID upsert/delete via Bulk 2.0 or REST Composite, Big Objects, native DLQ
- **Platform Events both ways** — consume them into Kafka, publish them from Kafka to trigger Flows and Apex
- **Gap recovery** — CDC gap/overflow events re-queried or resynced automatically instead of silently lost
- **Quota-aware** — a rate governor polls `/limits/` and backs off before your 24h API pool is exhausted
- **Runs anywhere** — Apache Kafka, MSK Connect, Strimzi, or Confluent Platform; any converter (JSON, Avro, Protobuf)
- **Legacy fallback** — CometD streaming connector for orgs without Pub/Sub API access
- **Verified migration path** — scripted Confluent config translation with CI-proven, evidence-generating cutover

## Installation

Download a connector plugin ZIP from [GitHub Releases](https://github.com/osodevops/kafka-connect-salesforce-oss/releases), unzip it onto your Connect worker's `plugin.path`, and restart the worker.

Or build from source (JDK 17+, Maven 3.6.3+):

```bash
mvn clean package -DskipTests
# ZIPs land in */target/*-kafka-connect-plugin.zip
```

Library artifacts are published to Maven Central under the `sh.oso` group.

## Quick Start

```bash
cd examples
mkdir -p plugins && for z in ../*/target/*-kafka-connect-plugin.zip; do unzip -o "$z" -d plugins/; done
docker compose up -d

curl -s -X POST -H 'Content-Type: application/json' \
  --data @source-connector.json localhost:8083/connectors
```

Create an Account in Salesforce and watch the change event arrive:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 --topic salesforce.Account --from-beginning
```

Full walkthrough (Connected App setup, OAuth flows, connector configs): **[the documentation](https://salesforcekafkaconnector.com/)**.

## Example configuration

Stream CDC for three SObjects (with a historical Bulk 2.0 backfill first) into per-object topics:

```json
{
  "name": "salesforce-source",
  "config": {
    "connector.class": "sh.oso.salesforce.source.SalesforceSourceConnector",
    "tasks.max": "3",
    "sf.auth.grant.type": "client_credentials",
    "sf.instance.url": "https://acme.my.salesforce.com",
    "sf.consumer.key": "${file:/secrets/sf.properties:consumer.key}",
    "sf.consumer.secret": "${file:/secrets/sf.properties:consumer.secret}",
    "sf.sobjects": "Account,Contact,Opportunity",
    "sf.topic.prefix": "salesforce",
    "sf.snapshot.enabled": "true",
    "sf.realtime.mode": "event_driven",
    "sf.emit.tombstone.on.delete": "true",
    "sf.gap.recovery": "resync"
  }
}
```

Write records back into Salesforce with idempotent external-ID upserts:

```json
{
  "name": "salesforce-lead-sink",
  "config": {
    "connector.class": "sh.oso.salesforce.sink.SalesforceSinkConnector",
    "tasks.max": "2",
    "topics": "salesforce.Lead",
    "sf.auth.grant.type": "client_credentials",
    "sf.instance.url": "https://target-org.my.salesforce.com",
    "sf.consumer.key": "${file:/secrets/sf.properties:consumer.key}",
    "sf.consumer.secret": "${file:/secrets/sf.properties:consumer.secret}",
    "sf.objects": "Lead",
    "sf.Lead.topics": "salesforce.Lead",
    "sf.Lead.use.custom.id.field": "true",
    "sf.Lead.custom.id.field.name": "External_Id__c",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "dlq-salesforce-lead-sink"
  }
}
```

Every property is documented in the **[configuration reference](https://salesforcekafkaconnector.com/reference/configuration/source)** — generated from the connectors' `ConfigDef`, so it always matches the release.

## The connectors

| Connector | Direction | Salesforce APIs |
|---|---|---|
| **Source** ([docs](https://salesforcekafkaconnector.com/connectors/source)) | Salesforce → Kafka | Pub/Sub API (CDC + Platform Events), Bulk API 2.0, REST |
| **SObject Sink** ([docs](https://salesforcekafkaconnector.com/connectors/sobject-sink)) | Kafka → Salesforce | Bulk API 2.0, REST Composite |
| **Platform Event Sink** ([docs](https://salesforcekafkaconnector.com/connectors/platform-event-sink)) | Kafka → Salesforce | Pub/Sub API Publish, REST |
| **Streaming Source** ([docs](https://salesforcekafkaconnector.com/connectors/streaming-source)) | Salesforce → Kafka | Streaming API (legacy CometD) |

All Salesforce protocol logic lives in the shared **`sf-core`** library; the connectors stay thin.

## Migrating from Confluent

A scripted path with verifiable evidence:

```bash
# translate your existing connector config
curl -s http://connect:8083/connectors/sf-cdc-prod \
  | tools/confluent-migration/confluent2oss.py - -o sf-cdc-prod-oss.json

# after cutover, prove nothing was lost
tools/confluent-migration/verify_cutover.py --sobject Account \
  --instance-url https://acme.my.salesforce.com --access-token "$TOKEN" \
  --since "$T0" --topic-dump dump.tsv
```

The zero-loss cutover procedure is executed in CI on every build, publishing a `migration-evidence` artifact (event counts, SHA-256 digests, replayId continuity). See the **[migration guide](https://salesforcekafkaconnector.com/migration/confluent)**.

## Testing

Everything runs locally with **no Salesforce org**: the `sf-core` test-jar ships a fake Salesforce (WireMock HTTP surface with the full Bulk 2.0 job lifecycle, an in-process gRPC implementation of the Pub/Sub API with replay semantics, and a CometD/Bayeux protocol stub).

```bash
mvn clean verify              # unit + integration + docker e2e (Kafka + Connect worker)
mvn clean verify -DskipE2E    # without Docker
```

## Documentation

Docs are built with Docusaurus from [`website/`](website/) and served at **[salesforcekafkaconnector.com](https://salesforcekafkaconnector.com/)**. Design/clean-room documents live in [`docs/`](docs/).

## Contributing

We welcome contributions of all kinds!

- **Report Bugs:** Found a bug? Open an [issue on GitHub](https://github.com/osodevops/kafka-connect-salesforce-oss/issues).
- **Suggest Features:** Have an idea? [Open a feature request](https://github.com/osodevops/kafka-connect-salesforce-oss/issues/new).
- **Contribute Code:** Check out our [good first issues](https://github.com/osodevops/kafka-connect-salesforce-oss/labels/good%20first%20issue) for beginner-friendly tasks — the [local testing guide](https://salesforcekafkaconnector.com/development/local-testing) gets you a full fake-Salesforce dev loop with no Salesforce org.
- **Improve Docs:** The site lives in [`website/`](website/) — docs pull requests are very welcome.

Releases use [conventional commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, …) — release-please turns them into versions and changelogs automatically.

## Commercial Support

[OSO](https://oso.sh) are Apache Kafka specialists. If you need help running Salesforce↔Kafka pipelines in production — architecture reviews, migration from Confluent, 24/7 support — [talk to us](https://oso.sh/contact/).

## License

kafka-connect-salesforce is licensed under the [Apache License 2.0](LICENSE) © [OSO](https://oso.sh).

This is an independent open-source project, not affiliated with, endorsed, or sponsored by Salesforce, Inc. or the Apache Software Foundation. Salesforce is a trademark of Salesforce, Inc. Apache, Apache Kafka, and Kafka are trademarks of the Apache Software Foundation.

---

<p align="center">
  Made with ❤️ by <a href="https://oso.sh">OSO</a>
</p>
