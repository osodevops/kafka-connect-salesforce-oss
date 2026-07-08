---
title: Building
description: Build the connectors from source.
sidebar_position: 1
---

# Building

Requirements: **JDK 17+** and **Maven 3.6.3+**.

```bash
git clone https://github.com/osodevops/kafka-connect-salesforce-oss.git
cd kafka-connect-salesforce-oss
mvn clean verify
```

`verify` compiles everything, generates the Pub/Sub gRPC stubs from Salesforce's
CC0-licensed `pubsub_api.proto`, runs the full test suite against the local fake
Salesforce, runs the docker end-to-end tests, and packages each connector as a Kafka
Connect plugin ZIP:

```
connect-salesforce-source/target/connect-salesforce-source-<version>-kafka-connect-plugin.zip
connect-salesforce-sink/target/connect-salesforce-sink-<version>-kafka-connect-plugin.zip
connect-salesforce-pe-sink/target/connect-salesforce-pe-sink-<version>-kafka-connect-plugin.zip
connect-salesforce-streaming-source/target/connect-salesforce-streaming-source-<version>-kafka-connect-plugin.zip
```

Each ZIP contains a `lib/` directory with the connector and its runtime dependencies
(`connect-api` excluded — the worker provides it). Unzip onto `plugin.path`.

Useful flags:

```bash
mvn clean package -DskipTests    # just build the ZIPs
mvn clean verify -DskipE2E       # skip the docker e2e tests
mvn -pl sf-core test             # one module's tests
```

## Module layout

| Module | Contents |
|---|---|
| `sf-core` | Shared Salesforce client library + the fake-Salesforce test harness (test-jar) |
| `connect-salesforce-source` | Unified source connector |
| `connect-salesforce-sink` | SObject sink connector |
| `connect-salesforce-pe-sink` | Platform Event sink connector |
| `connect-salesforce-streaming-source` | Legacy CometD source connector |
| `e2e-tests` | Docker end-to-end tests (not published) |
| `website` | This documentation site (Docusaurus) |
