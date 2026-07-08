---
title: Platform Event Sink Connector
description: Publish Kafka records as Salesforce Platform Events via the Pub/Sub API.
sidebar_position: 3
---

# Salesforce Platform Event Sink Connector

`sh.oso.salesforce.pesink.SalesforcePlatformEventSinkConnector`

Publishes Kafka records to a Salesforce **Platform Event** (`*__e`) — bridging external
events into Salesforce's event bus for Flow, Apex triggers, and other subscribers. It
publishes events only; for object CRUD use the [SObject sink](sobject-sink.md).

## How it works

- **Pub/Sub mode (default)**: the connector fetches the event's Avro schema via
  `GetSchema`, maps record fields to event fields **by name with type validation**,
  Avro-encodes each record, and calls the `Publish` RPC. Per-event `PublishResult`s are
  final — failed events go to the DLQ.
- **REST mode** (`sf.publish.mode=rest`): posts records via
  `POST /composite/sobjects` for batches.

`CreatedDate` and `CreatedById` are required in Pub/Sub publish payloads; the connector
fills them automatically when the record doesn't carry them.

Records that don't match the event schema (missing required fields, wrong types) are
reported to the errant-record reporter without failing the whole batch — publish results
and mapping failures both respect `behavior.on.api.errors`.

:::note Publish behaviour
Whether an event is published immediately or after the transaction commits is defined on
the **event definition** in Salesforce (Publish Immediately vs Publish After Commit), not
by the connector. Published events cannot be rolled back — delivery is at-least-once and
duplicates are possible on retry.
:::

## Configuration reference

:::tip Full property reference
[Platform Event Sink Configuration](../reference/configuration/platform-event-sink.md) — generated from the connector's ConfigDef.
:::

Auth properties are shared with the other connectors — see the
[source connector](source.md#connection--auth).

| Property | Type | Default | Description |
|---|---|---|---|
| `topics` / `topics.regex` | list/string | — | Source Kafka topics |
| `sf.platform.event.name` | string | — | Target event API name; must match `.*__e` and pre-exist |
| `sf.publish.mode` | enum | `pubsub` | `pubsub` (Publish RPC) or `rest` (Composite) |
| `behavior.on.api.errors` | enum | `fail` | `fail`, `log`, or `ignore` |
| `sf.request.max.retries.time.ms` | long | `30000` | Retry budget |
| `sf.pubsub.endpoint` | string | `api.pubsub.salesforce.com:443` | Pub/Sub endpoint override |
| `sf.pubsub.plaintext` | boolean | `false` | Plaintext gRPC (testing only) |

## Example

```json
{
  "name": "order-shipped-events",
  "config": {
    "connector.class": "sh.oso.salesforce.pesink.SalesforcePlatformEventSinkConnector",
    "tasks.max": "2",
    "topics": "orders.shipped",
    "sf.auth.grant.type": "client_credentials",
    "sf.instance.url": "https://acme.my.salesforce.com",
    "sf.consumer.key": "...",
    "sf.consumer.secret": "...",
    "sf.platform.event.name": "Order_Shipped__e",
    "behavior.on.api.errors": "log",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "dlq-order-shipped"
  }
}
```

A record `{"OrderNumber__c": "ORD-42", "Carrier__c": "DHL"}` on `orders.shipped` becomes
an `Order_Shipped__e` event that Salesforce Flows and Apex triggers can subscribe to.
