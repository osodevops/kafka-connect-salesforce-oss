# Testing Strategy — Simulating Salesforce Locally

There is no "LocalStack for Salesforce". The strategy (validated against how Apache Camel's
`camel-salesforce` — the most mature OSS Salesforce integration — tests) is a **composite fake**,
plus an optional real-org tier.

## Test tiers

| Tier | What runs | Salesforce | Kafka |
|---|---|---|---|
| Unit | pure logic (schema mapping, CSV, config, offsets) | none | none |
| Integration (default CI) | sf-core clients + connector tasks | **local fake harness** | none (tasks driven directly) |
| E2E (CI, docker) | full connector in a Connect worker | local fake harness | Testcontainers Kafka + Connect |
| Real-org (nightly/manual) | acceptance criteria against a Developer Edition org | `sf org login jwt` | Testcontainers |

## The fake harness (sf-core test-jar, package `sh.oso.salesforce.testing`)

1. **`MockSalesforceServer`** (WireMock `org.wiremock:wiremock`):
   - `POST /services/oauth2/token` for client_credentials / jwt-bearer / password; the returned
     `instance_url` points back at WireMock so all subsequent REST/Bulk calls hit the fake.
   - REST: `/query`, `/queryAll` (with `nextRecordsUrl` paging), `/sobjects/<X>/describe/`
     (with `If-Modified-Since` → 304), `/limits/`, `/composite/sobjects`.
   - Bulk 2.0: stateful job lifecycle (query + ingest) implemented with an in-memory job store —
     create → upload → UploadComplete → InProgress → JobComplete → paged CSV results with
     `Sforce-Locator` headers.
   - Fault injection: 401 INVALID_SESSION_ID (re-auth), 403 REQUEST_LIMIT_EXCEEDED, 5xx.
2. **`FakePubSubServer`** (grpc-java `PubSubGrpc.PubSubImplBase`, in-process or plaintext port):
   - In-memory topics with monotonically increasing replayIds (stored/asserted as opaque bytes).
   - `GetTopic` / `GetSchema` (canned CDC + Platform Event Avro schemas), `Publish`,
     `Subscribe` honoring `ReplayPreset.{LATEST,EARLIEST,CUSTOM}` and `num_requested` flow control.
   - Fault injection via gRPC status + `error-code` trailers (e.g. invalid replay id, auth error),
     mirroring how the real service signals errors.
3. **`BayeuxStubServer`** (Jetty): protocol-level stub of `/meta/handshake`, `/meta/connect`,
   `/meta/subscribe` with the Salesforce replay extension (`ext.replay`), ring-buffered events,
   `-1`/`-2`/custom replayId semantics, and `403 Unknown client` reconnect scenarios.
   Lives in `connect-salesforce-streaming-source` tests.

**Design prerequisite (implemented):** every endpoint is overridable — token endpoint, instance
URL (comes from the token response), Pub/Sub `host:port` + plaintext toggle. This is what makes
the connectors testable without Salesforce.

## Kafka side

- Task-level tests drive `SourceTask.poll()` / `SinkTask.put()` directly — no Kafka.
- E2E uses Testcontainers (Kafka/Redpanda + a Connect worker with the plugin ZIP mounted),
  with the fake harness reachable via `host.testcontainers.internal`.

## Real-org tier (optional, not in default CI)

Persistent free Developer Edition org + JWT auth (`sf org login jwt`) on a scheduled GitHub Actions
workflow; tests tagged `ManualIT`. Scratch orgs are real cloud orgs (not local), rate-limited, and
slow — use only to test org-shape provisioning. Camel keeps real-org Pub/Sub tests out of CI for
the same reason.
