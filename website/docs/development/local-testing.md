---
title: Local Testing
description: The fake Salesforce harness — test every connector without a Salesforce org.
sidebar_position: 2
---

# Local Testing

There is no "LocalStack for Salesforce", so this project ships its own **fake
Salesforce**: `MockSalesforceServer` and `FakePubSubServer` are exported from `sf-core`'s
test-jar (`sh.oso.salesforce.testing`), and the CometD stub lives in the streaming source
module's tests. Every
connector behaviour — including the snapshot handoff, gap recovery, and replay resume —
is verified against it in CI, with no Salesforce org and no secrets.

## The harness

| Component | Simulates | How |
|---|---|---|
| `MockSalesforceServer` | OAuth token endpoint, REST (query/describe/limits/composite), the full Bulk API 2.0 job lifecycle | WireMock + a stateful job-store transformer. The token response's `instance_url` points back at the mock, so an authenticated client routes all traffic to it |
| `FakePubSubServer` | The Pub/Sub API (`eventbus.v1.PubSub`) | grpc-java service with in-memory topics, real Avro encoding, monotonic Salesforce-shaped replayIds (8-byte position plus partition bytes), `LATEST`/`EARLIEST`/`CUSTOM` replay semantics, `num_requested` flow control, and fault injection (subscribe and mid-stream failures, retention expiry, token revocation, schema evolution, undecodable payloads, per-event publish failures) |
| `BayeuxStubServer` | The legacy Streaming API's CometD endpoint | Protocol-level stub of handshake/connect/subscribe with the Salesforce replay extension, retained-event buffer, and invalid-replayId rejection |

Every connector exposes endpoint overrides that make this possible (and double as proxy
settings): `sf.token.endpoint`, `sf.pubsub.endpoint` + `sf.pubsub.plaintext`, and
`sf.cometd.endpoint`.

## Test tiers

| Tier | What runs | Command |
|---|---|---|
| Unit + integration | Clients and connector tasks against the fakes — real HTTP, real gRPC, real CometD | `mvn test` |
| End-to-end (docker) | A real Kafka broker + Connect worker loads the packaged plugin ZIP contents and runs both data paths against the fakes | `mvn verify` (skip with `-DskipE2E`) |
| Real org (manual) | Acceptance checks against a Developer Edition org | not in CI |

The e2e tier proves plugin packaging, classloader isolation, and REST-API config
plumbing — the things task-level tests can't.

## Writing a test against the harness

```java
MockSalesforceServer sf = new MockSalesforceServer().start();
sf.stubDescribe("Account", TestSchemas.accountDescribeJson());

FakePubSubServer pubsub = new FakePubSubServer().startOnFreePort();
Schema schema = TestSchemas.accountChangeEventSchema();
pubsub.createTopic("/data/AccountChangeEvent", schema);

// simulate a change in Salesforce
pubsub.publishEvent("/data/AccountChangeEvent",
    TestSchemas.accountChangeEvent(schema, "UPDATE", "001A", commitTs, "Acme"));

// point a connector at the fakes
props.put("sf.instance.url", sf.baseUrl());
props.put("sf.token.endpoint", sf.baseUrl() + "/services/oauth2/token");
props.put("sf.pubsub.endpoint", pubsub.endpoint());
props.put("sf.pubsub.plaintext", "true");
```

Fault injection:

```java
pubsub.failNextSubscribe(Status.UNAUTHENTICATED, "sfdc...subscription.auth.error");
pubsub.failNextSubscribe(topic, Status.UNAVAILABLE, null);  // next subscribe on one topic
pubsub.breakSubscriptions(topic, Status.UNAVAILABLE, null); // fail live streams mid-flight
pubsub.expireEvents(topic, 2);                              // first 2 events past retention
pubsub.revokeAccessToken(pubsub.lastAccessToken());         // expired session
sf.rotateToken();                                           // next OAuth call issues a new token
pubsub.updateTopicSchema(topic, evolvedSchema);             // field added mid-stream
pubsub.publishRawEvent(topic, new byte[0]);                 // undecodable payload
pubsub.setFirstKeepaliveDelay(Duration.ofSeconds(2));       // Salesforce-like delayed first response
pubsub.setPublishFailer(e -> "LIMIT_EXCEEDED");             // per-event publish errors
sf.bulk().setIngestRowFailer(row ->                          // per-row Bulk failures
    row.contains("Bad") ? "REQUIRED_FIELD_MISSING" : null);
```

## Testing against a real org

For final acceptance, point a connector at a free Developer Edition org (JWT Bearer
recommended). Enable CDC for your objects first. Keep real-org runs out of per-PR CI —
event delivery timing makes them flaky and scratch-org allocations are tight.
