# PRD-00 — `sf-core` Shared Salesforce Client Library

**Status:** Draft for build — **Phase 0 foundation; all connectors depend on this**
**Depends on:** nothing (foundation)
**Clean-room note:** Specified from Salesforce public API docs. Own namespace. No Confluent code.

---

## 1. Objective
A single, well-tested JVM library that encapsulates all Salesforce protocol logic so the four connectors (PRD-01…04) stay thin. It owns auth, the Pub/Sub client, the Bulk API 2.0 client, the REST/SOQL client, schema mapping, and rate-limit governance.

## 2. Why a shared library
- All connectors share auth, schema mapping, rate governance, and error classification. Centralising avoids drift and duplicated bugs.
- Enables unit/integration testing of Salesforce logic independent of Kafka Connect.
- Could later power a Debezium-style embedded engine or non-Kafka consumers.

## 3. Modules

### 3.1 Auth (`sf-core-auth`)
- **Client Credentials flow:** `POST {myDomain}/services/oauth2/token` with `grant_type=client_credentials`, `client_id`, `client_secret`. Reject `login/test.salesforce.com` (must be My Domain). Requires "Run As" API-Only integration user configured Salesforce-side ([Client Credentials](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_client_credentials_flow.htm&type=5)).
- **JWT Bearer flow:** build RS256-signed JWT (`iss`=client_id, `sub`=username, `aud`=login/test host, `exp`, optional `jti`), `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer`, `assertion=<jwt>`. No refresh token issued — re-mint on expiry ([JWT Bearer](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_jwt_flow.htm&type=5)).
- **Username-Password (legacy fallback):** `grant_type=password`, security token appended to password. Blocked by default in orgs created Summer '23+ — mark deprecated ([Username-Password](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_username_password_flow.htm&type=5)).
- Manage token lifecycle: cache `access_token` + `instance_url`, detect 401/expiry, re-authenticate transparently. Never log tokens.

### 3.2 Pub/Sub client (`sf-core-pubsub`)
- gRPC over HTTP/2 to `api.pubsub.salesforce.com:443`. Generate stubs from Salesforce's published `.proto` (CC0-1.0) ([forcedotcom/pub-sub-api](https://github.com/forcedotcom/pub-sub-api)).
- Implement `Subscribe` (bidirectional, pull-based flow control, `num_requested` ≤ 100), `Publish`, `PublishStream`, `GetSchema`, `GetTopic`. Pass `accesstoken`/`instanceurl`/`tenantid` as gRPC metadata.
- Avro decode using cached schema (by schema ID) via `GetSchema`; expose decoded records + `ChangeEventHeader`.
- Replay: accept `replay_preset` (LATEST/EARLIEST/CUSTOM) + `replay_id` (opaque bytes). Surface replayId to caller for checkpointing. First FetchRequest sets topic + replay; repositioning requires a new Subscribe.
- 72h retention awareness; surface "replay id too old" for gap recovery.

### 3.3 Bulk API 2.0 client (`sf-core-bulk`)
- **Query jobs:** `POST /jobs/query` → poll `GET /jobs/query/{id}` for `JobComplete` → `GET /jobs/query/{id}/results?locator=&maxRecords=` paginate until header `Sforce-Locator: null`. Use the same API version to create + read (else 409). CSV results, compressed. Parse `Sforce-NumberOfRecords` ([Bulk 2.0 query](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/queries.htm)).
- **Ingest jobs:** `POST /jobs/ingest` (object, operation, `externalIdFieldName`, lineEnding) → `PUT .../batches` (CSV) → `PATCH state=UploadComplete` → poll `JobComplete` → read `successfulResults`/`failedResults`/`unprocessedrecords` ([Bulk 2.0 ingest](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/bulk_api_2_0_ingest.htm)).
- Enforce SOQL restrictions for query jobs (no ORDER BY/GROUP BY/OFFSET/TYPEOF/subqueries/aggregates/compound fields).
- Track caps: 150M records/24h, 150MB/job (≤100MB raw CSV), 10k query jobs/24h.

### 3.4 REST/SOQL client (`sf-core-rest`)
- `GET /query/?q=` with `nextRecordsUrl` paging; `queryAll` for soft-deleted.
- `GET /sobjects/<X>/describe/` for schema; cache with `If-Modified-Since` → 304 handling.
- `POST /sobjects/<X>/` and `/composite/` for low-latency writes and event publishing fallback.
- `GET /limits/` for live quota.

### 3.5 Schema mapping (`sf-core-schema`)
- Avro (Pub/Sub) → Kafka Connect `Schema`/`Struct`.
- Salesforce `describe` field metadata → Connect schema for Bulk/REST paths.
- Date/datetime policy: epoch-millis `int64` default; ISO-8601 string for opt-out fields. Handle nullability, decimals, compound-field decomposition (Address/Location → components).
- Schema cache keyed by (SObject, schema ID / describe mtime); evolve on change.

### 3.6 Rate-limit governor (`sf-core-limits`)
- Periodically read `/limits/`; track `DailyApiRequests`, `DailyBulkV2QueryJobs`, `DailyBulkApiBatches`.
- Proactive backoff before exhaustion; classify retryable errors: HTTP 429/5xx, `403 REQUEST_LIMIT_EXCEEDED`, gRPC `UNAVAILABLE`/`DEADLINE_EXCEEDED`/`RESOURCE_EXHAUSTED`, network timeouts. Exponential backoff + jitter with a caller-supplied budget.

## 4. Cross-cutting requirements
- **Config validation** helpers reused by all connectors.
- **Observability:** metrics hooks (API calls used, replay lag, job durations); structured logging; strict secret redaction (no token/curl logging).
- **Testing:** unit tests with mocked HTTP/gRPC; integration tests against a Salesforce Developer Edition org (CI secret). Contract tests for auth flows.
- **Licensing:** Apache-2.0; only permissive deps (gRPC, Avro/avro4k, HTTP client, JOSE). Avoid Confluent Community-Licensed artifacts in core (converters are user-supplied at the Connect layer).
- **API version:** default v60.0, configurable; guard against retired versions (21–30).

## 5. Public API sketch (language-agnostic)
```
SalesforceAuth.authenticate(config) -> Session{accessToken, instanceUrl}
PubSubClient(session).subscribe(topic, replay) -> stream<ChangeEvent{header, payload, replayId}>
PubSubClient(session).publish(topic, events) -> List<PublishResult>
BulkClient(session).startQuery(soql) -> jobId; fetchResults(jobId, locator) -> {records, nextLocator}
BulkClient(session).ingest(object, operation, csv, extId) -> {jobId}; results(jobId) -> {success, failed, unprocessed}
RestClient(session).query(soql) / describe(object) / limits()
SchemaMapper.toConnect(avroSchema | describe) -> ConnectSchema
LimitGovernor.acquire(apiType) / backoff()
```

## 6. Acceptance criteria
- [ ] Authenticates via Client Credentials and JWT Bearer against a My Domain org; auto-refreshes on 401.
- [ ] Pub/Sub subscribe decodes Avro CDC events with correct `ChangeEventHeader` and surfaces replayId.
- [ ] Pub/Sub publish returns per-event results.
- [ ] Bulk 2.0 query paginates via locator to completion; ingest reports success/failed/unprocessed.
- [ ] REST query paginates via `nextRecordsUrl`; describe uses 304 caching.
- [ ] Schema mapper produces valid Connect schemas incl. compound-field decomposition and date policy.
- [ ] Rate governor backs off on `REQUEST_LIMIT_EXCEEDED` and stays under 24h pool.
- [ ] Zero Confluent-licensed dependencies in core; Apache-2.0 throughout.
