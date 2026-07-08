---
title: Getting Started
description: Install the connectors and stream your first Salesforce records into Kafka.
sidebar_position: 1
---

# Getting Started

Three steps take you from nothing to Salesforce change events flowing into Kafka:

1. **[Set up Salesforce](salesforce-setup.md)** — create a Connected App with the Client
   Credentials (or JWT Bearer) OAuth flow and enable Change Data Capture for the objects
   you want to stream.
2. **Install the plugins** — download a connector ZIP from
   [GitHub Releases](https://github.com/osodevops/kafka-connect-salesforce-oss/releases)
   (or build with `mvn clean package`), unzip it onto your Connect worker's
   `plugin.path`, and restart the worker.
3. **[Run the quickstart](quickstart.md)** — a docker-compose stack plus a ready-to-POST
   connector config.

## Requirements

| Requirement | Version |
|---|---|
| Apache Kafka / Kafka Connect | 3.x (2.6+ for DLQ support) |
| Java (Connect worker) | 17+ |
| Salesforce edition | Enterprise, Performance, Unlimited, or Developer (Pub/Sub API) |
| Salesforce API version | v60.0 default; configurable (Bulk API 2.0 requires ≥ v47.0) |

Orgs without Pub/Sub API access (e.g. Government Cloud) can use the
[legacy Streaming source](../connectors/streaming-source.md) or the source connector's
polling mode.

## Verify the plugin installation

After restarting the worker, the connector classes should appear in the plugin list:

```bash
curl -s localhost:8083/connector-plugins | jq '.[].class' | grep salesforce
"sh.oso.salesforce.source.SalesforceSourceConnector"
"sh.oso.salesforce.sink.SalesforceSinkConnector"
"sh.oso.salesforce.pesink.SalesforcePlatformEventSinkConnector"
"sh.oso.salesforce.streaming.SalesforceStreamingSourceConnector"
```
