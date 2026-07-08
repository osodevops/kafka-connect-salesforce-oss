# Salesforce APIs & OSS Kafka Connector Architecture

Research to inform building an **open-source alternative to Confluent's proprietary Salesforce Kafka connectors**. Every factual claim below carries an inline source link to the page it was verified against. Where a specific detail could not be confirmed from a fetched page, it is marked `n.a.`

Confluent's proprietary suite (for scope reference) comprises seven separately-licensed connectors: PushTopic Source, Change Data Capture Source, Platform Event Source, Platform Event Sink, SObject Sink, Bulk API Source, and Bulk API Sink ([Confluent Salesforce Connector overview](https://docs.confluent.io/kafka-connectors/salesforce/current/overview.html)).

---

# PART A — Underlying Salesforce APIs

## Global notes on versioning & endpoints
- All modern Salesforce data APIs (REST, Bulk 2.0, Query, Limits, Composite) live under `/services/data/vXX.X/` on the org's My Domain host, e.g. `https://MyDomainName.my.salesforce.com/services/data/v67.0/...` ([REST API Reference list](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_list.htm)).
- The list of available versions is retrievable **without authentication** via `GET /services/data/` ([List Available REST API Versions](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/dome_versions.htm)).
- Latest versions at time of research: Summer '26 = **v67.0**, Spring '26 = v66.0, Winter '26 = v65.0 ([List Available REST API Versions](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/dome_versions.htm)).
- **Retirement gotcha:** REST/SOAP/Bulk/Tooling API versions **21.0–30.0 are retired** as of Summer '25; calls to them fail with "endpoint not found". Versions 7.0–20.0 were retired in Summer '22 ([Platform API Versions 21.0–30.0 Retirement](https://help.salesforce.com/s/articleView?id=000389618&language=en_US&type=1)).

---

## A1. OAuth Flows

### Client Credentials Flow (recommended server-to-server replacement)
- Token endpoint: `POST /services/oauth2/token` ([OAuth 2.0 Client Credentials Flow](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_client_credentials_flow.htm&type=5)).
- **Gotcha:** requests to `https://login.salesforce.com` and `https://test.salesforce.com` are **not supported** for this flow — you must use the org's **My Domain URL** (e.g. `https://MyCompany.my.salesforce.com`) ([OAuth 2.0 Client Credentials Flow](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_client_credentials_flow.htm&type=5); [Migrate from Username-Password to Client Credentials](https://help.salesforce.com/s/articleView?id=000886201&language=en_US&type=1)).
- Required params: `grant_type=client_credentials`, `client_id` (consumer key), `client_secret` (consumer secret). No username/password sent ([OAuth 2.0 Client Credentials Flow](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_client_credentials_flow.htm&type=5)).
- Response: `access_token`, `instance_url`, `id`, `token_type` (Bearer), `scope`, `issued_at`, `signature` ([OAuth 2.0 Client Credentials Flow](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_client_credentials_flow.htm&type=5)).
- Requires a designated **"Run As" integration user** configured on the Connected App's Client Credentials policy; that user should have the **API Only** permission ([Migrate from Username-Password to Client Credentials](https://help.salesforce.com/s/articleView?id=000886201&language=en_US&type=1)).
- **Multi-user gotcha:** you cannot migrate to client credentials if a single org uses multiple usernames — use JWT Bearer instead ([Migrate from Username-Password to Client Credentials](https://help.salesforce.com/s/articleView?id=000886201&language=en_US&type=1)).

### JWT Bearer Flow (recommended for multiple-username / certificate-based)
- Token endpoint: `POST /services/oauth2/token`; `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer`; `assertion=<the full JWT>` ([OAuth 2.0 JWT Bearer Flow](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_jwt_flow.htm&type=5)).
- JWT signed with **RSA SHA256 (RS256)** using an uploaded X509 certificate (cert ≤ 4 KB; use DER encoding if larger) ([OAuth 2.0 JWT Bearer Flow](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_jwt_flow.htm&type=5)).
- Claims: `iss` = client_id; `sub` = username; `aud` = `https://login.salesforce.com` or `https://test.salesforce.com`; `exp` = epoch seconds (3-min clock-skew buffer). Optional `jti` prevents replay ([OAuth 2.0 JWT Bearer Flow](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_jwt_flow.htm&type=5)).
- Response: `access_token`, `scope`, `instance_url`, `id`, `token_type`. **No refresh token is ever issued**, and you cannot specify scopes in the JWT ([OAuth 2.0 JWT Bearer Flow](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_jwt_flow.htm&type=5)).

### Username-Password Flow (being blocked / deprecated — avoid)
- `grant_type=password` plus `client_id`, `client_secret`, `username`, and `password` **with the security token appended to the password** (concatenate password + token) ([OAuth 2.0 Username-Password Flow](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_username_password_flow.htm&type=5)).
- No scopes, no refresh tokens; tokens are Session IDs that can't be introspected ([OAuth 2.0 Username-Password Flow (xcloud)](https://help.salesforce.com/s/articleView?id=xcloud.remoteaccess_oauth_username_password_flow.htm&language=en_US&type=5)).
- **Deprecation/blocking status:** Salesforce is deprecating this flow and recommends migrating to Client Credentials (server-to-server) or JWT Bearer (multiple usernames) ([Migrate from Username-Password to Client Credentials](https://help.salesforce.com/s/articleView?id=000886201&language=en_US&type=1)). Orgs created in **Summer '23 or later block it by default**; enabling requires "Allow OAuth Username-Password Flows" in OAuth and OpenID Connect Settings ([Username-Password Flow Blocked by Default in New Orgs](https://help.salesforce.com/s/articleView?id=release-notes.rn_security_username-password_flow_blocked_by_default.htm&language=en_US&release=244&type=5)). Newer **External Client Apps do not support this flow at all** ([Block Authorization Flows to Improve Security](https://help.salesforce.com/s/articleView?id=xcloud.remoteaccess_disable_username_password_flow.htm&language=en_US&type=5)). When blocked, the API returns `{"error":"invalid_grant","error_description":"authentication failure"}` and Login History shows "Username-Password Flow Disabled" ([Nerd @ Work analysis](https://blog.enree.co/2025/09/oauth-username-password-flow-disabled-in-salesforce-what-it-means-what-to-do)).

**Recommendation for the connector:** implement **Client Credentials** and **JWT Bearer** flows against the org's **My Domain** token endpoint; do not rely on username-password.

---

## A2. REST API (SOQL Query, Describe, Versioning)

### Query / queryMore (nextRecordsUrl)
- Resource: `GET /services/data/vXX.X/query/?q=<URL-encoded SOQL>` ([Execute a SOQL Query](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/dome_query.htm)).
- Response shape: `{ "done": bool, "totalSize": int, "records": [...], "nextRecordsUrl": "..." }` ([Execute a SOQL Query](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/dome_query.htm)).
- **queryMore pagination**: when `done=false`, follow `nextRecordsUrl` (e.g. `/services/data/v66.0/query/01gD0000002HU6KIAW-2000`) with a plain GET (no params) and repeat until `done=true` ([Execute a SOQL Query](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/dome_query.htm)). Each record carries `attributes.type` and `attributes.url`. Default page/batch-size numbers are `n.a.` on the fetched page (commonly 2000, inferable from the locator suffix `-2000`).

### sObject Describe (schema)
- Resource: `GET /services/data/vXX.X/sobjects/<SObject>/describe/` returns full metadata: fields, field types, URLs, and child relationships (JSON or XML) ([sObject Describe](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_sobject_describe.htm)).
- Supports `If-Modified-Since` / `If-Unmodified-Since` headers; returns `304 Not Modified` if metadata unchanged — useful for cheap schema-change polling ([sObject Describe](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_sobject_describe.htm)).

### Rate limits
- Total inbound API calls per 24-hour rolling window: Developer Edition = 15,000; Enterprise/Unlimited = `100,000 + (licenses × per-license allocation) + add-ons`. REST, SOAP, Bulk, Bulk 2.0, and most Connect REST calls all count toward this pool ([API Request Limits and Allocations](https://developer.salesforce.com/docs/atlas.en-us.salesforce_app_limits_cheatsheet.meta/salesforce_app_limits_cheatsheet/salesforce_app_limits_platform_api.htm)).
- Combined URI + headers per REST call ≤ 16,384 bytes; max SOQL run time 120s ([API Request Limits and Allocations](https://developer.salesforce.com/docs/atlas.en-us.salesforce_app_limits_cheatsheet.meta/salesforce_app_limits_cheatsheet/salesforce_app_limits_platform_api.htm)).
- Live remaining quota is queryable via `GET /services/data/vXX.X/limits/` ([List Org Limits](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/dome_limits.htm)).

---

## A3. Bulk API 1.0

- Job/batch lifecycle: create a job (specifies one operation + one object), submit batches (each batch = one CSV/XML/JSON file for a single object), close the job. Each 200-record chunk of DML runs as a separate transaction ([Limits | Bulk API](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/asynch_api_concepts_limits.htm)).
- Per-batch caps: **max 10,000 records/batch**, **10 MB max file size/batch**, **10,000,000 characters** of data per batch ([Bulk API Limits and Allocations](https://developer.salesforce.com/docs/atlas.en-us.salesforce_app_limits_cheatsheet.meta/salesforce_app_limits_cheatsheet/salesforce_app_limits_platform_bulkapi.htm)).
- **PK-chunking** (query jobs): enabled with the `Sforce-Enable-PKChunking` request header. Splits large-table queries into successive `WHERE Id >= X AND Id < Y` ranges. Default `chunkSize=100,000`, max `250,000`. Header fields: `chunkSize`, `parent` (for Sharing/History objects, e.g. `parent=Case`), `startRow` (restart from a specific record ID) ([PK Chunking](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/async_api_headers_enable_pk_chunking.htm)).
  - Only works with a fixed set of supported objects (Account, Contact, Lead, Opportunity, Case, Task, User, custom objects, etc.) plus their Sharing/History tables ([PK Chunking](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/async_api_headers_enable_pk_chunking.htm)).
  - **Gotchas:** using an `ORDER BY` or filtering on an "id"-named field effectively disables chunking; soft-deleted record IDs are counted in boundaries but omitted from results (so chunks can be smaller than `chunkSize`); each chunk is a separate batch counting toward the daily batch limit and must be downloaded separately; original batch status becomes `NOT_PROCESSED` on success or `FAILED` on chunking failure. Salesforce recommends enabling PK-chunking for tables >10M records ([PK Chunking](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/async_api_headers_enable_pk_chunking.htm)).
- **Shared batch allocation**: up to **15,000 batches per rolling 24h**, shared across Bulk API and Bulk API 2.0 (exposed as `DailyBulkApiBatches`, max 15,000) ([Bulk API Limits](https://developer.salesforce.com/docs/atlas.en-us.salesforce_app_limits_cheatsheet.meta/salesforce_app_limits_cheatsheet/salesforce_app_limits_platform_bulkapi.htm); [List Org Limits](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/dome_limits.htm)).
- Job status + batch results are retained **7 days** then permanently deleted ([Limits | Bulk API](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/asynch_api_concepts_limits.htm)).
- **V1 retirement gotcha:** Bulk API v1 job endpoints on API versions 21–30 are retired (see A2 versioning) ([Platform API Versions 21.0–30.0 Retirement](https://help.salesforce.com/s/articleView?id=000389618&language=en_US&type=1)).

---

## A4. Bulk API 2.0

Bulk API 2.0 is part of REST; root `https://<my-domain>.my.salesforce.com/services/data/vXX.0/jobs` ([Accessing Object Data with Salesforce Platform APIs](https://developer.salesforce.com/blogs/2024/04/accessing-object-data-with-salesforce-platform-apis)). Available in API v47.0+ ([Understanding Bulk API 2.0 Query](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/queries.htm)).

### Ingest workflow (insert/update/upsert/delete/hardDelete)
1. `POST /services/data/vXX.X/jobs/ingest` — create job (body: `object`, `operation`, `contentType:"CSV"`, `lineEnding`, `externalIdFieldName` for upsert). Returns job ID + `contentUrl`.
2. `PUT /services/data/vXX.X/jobs/ingest/{jobId}/batches` — upload CSV (Content-Type `text/csv`).
3. `PATCH /services/data/vXX.X/jobs/ingest/{jobId}` with `{"state":"UploadComplete"}` — triggers processing.
4. `GET /services/data/vXX.X/jobs/ingest/{jobId}` — poll for state `JobComplete`.
5. `GET .../successfulResults`, `.../failedResults`, `.../unprocessedrecords`.
([Slim Down with the New Bulk API v2](https://developer.salesforce.com/blogs/2017/12/slim-new-bulk-api-v2); [Bulk API 2.0 Ingest](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/bulk_api_2_0_ingest.htm))

### Query workflow
- URIs: `POST /jobs/query` (create), `GET /jobs/query/{id}` (status), `GET /jobs/query/{id}/results` (results), `PATCH` (abort), `DELETE` (delete) ([Understanding Bulk API 2.0 Query](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/queries.htm)).
- **Locator-based pagination**: `GET /jobs/query/{id}/results?locator=<loc>&maxRecords=<n>`. First request omits `locator`. The response header `Sforce-Locator` gives the next locator and `Sforce-NumberOfRecords` the count in that set; iterate until `Sforce-Locator` = the string `null`. Only `text/csv` results are supported; results are always compressed ([Get Results for a Query Job](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/query_get_job_results.htm); [Understanding Bulk API 2.0 Query](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/queries.htm)).
- **Gotcha:** you must retrieve results using the **same API version** used to create the job or you get a 409 error ([Get Results for a Query Job](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/query_get_job_results.htm)). Auto PK-chunking is applied where supported; `LIMIT`/`ORDER BY` disable it. Unsupported SOQL: `GROUP BY`, `OFFSET`, `TYPEOF`, aggregate functions, compound address/geolocation fields, parent-to-child subqueries ([Understanding Bulk API 2.0 Query](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/queries.htm)).

### Limits & caps
- **Max records uploaded per rolling 24h: 150,000,000** (older docs cite 100M; the current cheat sheet states 150M = 15,000 batches × 10,000) ([Bulk API and Bulk API 2.0 Limits](https://developer.salesforce.com/docs/atlas.en-us.salesforce_app_limits_cheatsheet.meta/salesforce_app_limits_cheatsheet/salesforce_app_limits_platform_bulkapi.htm)).
- **Max file size: 150 MB per job of base64-encoded content.** Because upload data is base64-converted (~50% size increase), Salesforce recommends uploading ≤100 MB of raw CSV ([Bulk API and Bulk API 2.0 Limits](https://developer.salesforce.com/docs/atlas.en-us.salesforce_app_limits_cheatsheet.meta/salesforce_app_limits_cheatsheet/salesforce_app_limits_platform_bulkapi.htm)).
- Query jobs: **10,000 per 24h** (`DailyBulkV2QueryJobs`); extract up to ~1TB/24h ([Bulk API Limits](https://developer.salesforce.com/docs/atlas.en-us.salesforce_app_limits_cheatsheet.meta/salesforce_app_limits_cheatsheet/salesforce_app_limits_platform_bulkapi.htm); [List Org Limits](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/dome_limits.htm); [Accessing Object Data with Salesforce Platform APIs](https://developer.salesforce.com/blogs/2024/04/accessing-object-data-with-salesforce-platform-apis)).
- Internal batching: Salesforce creates one batch per 10,000 records; a batch that can't process in 5 minutes fails and is retried up to **20 times**, after which the whole job moves to `Failed` ([Understanding Bulk API 2.0 Ingest](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/datafiles_understanding_bulk2_ingest.htm)).
- Bulk 2.0 is recommended for operations >2,000 records; below that use synchronous REST/SOAP ([Bulk API 2.0 Developer Guide PDF](https://resources.docs.salesforce.com/latest/latest/en-us/sfdc/pdf/api_asynch.pdf)).
- **Checkpointing for a connector:** Bulk 2.0 has no server-side replay; a source connector must checkpoint on a monotonic field (e.g. `SystemModstamp`/`LastModifiedDate`) between polling cycles. `n.a.` (no built-in cursor).

---

## A5. Streaming API (CometD / Bayeux) — legacy

- Transport: **CometD / Bayeux over HTTP long-polling**; endpoint `https://MyDomainName.my.salesforce.com/cometd/<version>/` (e.g. `/cometd/66.0/`) ([Message Durability](https://developer.salesforce.com/docs/atlas.en-us.api_streaming.meta/api_streaming/using_streaming_api_durability.htm)).
- Channels: PushTopics (`/topic/<name>`), generic (`/u/<name>`), platform events (`/event/<name>__e`), CDC (`/data/ChangeEvents` etc.).
- **Retention:** PushTopic events, generic events, and standard-volume events stored **24 hours**; high-volume events (platform events + CDC) stored **72 hours**. After retention, events are purged; no guarantee beyond 72h ([Message Durability](https://developer.salesforce.com/docs/atlas.en-us.api_streaming.meta/api_streaming/using_streaming_api_durability.htm)).
- **Replay / checkpoint (`replayId`):** each event carries an opaque `ReplayId` (position in stream). Durable streaming (API v37.0+) lets a subscriber resubscribe with a saved replayId. Replay options: `-1` (new events only, default), `-2` (all retained + new, use sparingly), or a specific replayId (all events after that one). Implemented via a Salesforce CometD replay extension (JS + Java samples) ([Message Durability](https://developer.salesforce.com/docs/atlas.en-us.api_streaming.meta/api_streaming/using_streaming_api_durability.htm)).
- **Gotchas:** replayId is **not guaranteed contiguous** and **not guaranteed unique** across org migrations/maintenance — use `EventUuid` for unique identification, and store replayId as bytes (don't assume it's numeric). Client times out if it doesn't reconnect within 40s ([Message Durability](https://developer.salesforce.com/docs/atlas.en-us.api_streaming.meta/api_streaming/using_streaming_api_durability.htm)).
- **Retirement timeline:** Streaming API versions **23.0–36.0 are being retired (scheduled Winter '25)**; use v37.0+ (Durable Streaming). Post-retirement, requests route to the latest version ([Streaming API Versions 23.0–36.0 Retirement](https://help.salesforce.com/s/articleView?id=000396440&type=1)). Salesforce broadly recommends Pub/Sub API over CometD for new apps (see A6).

---

## A6. Pub/Sub API (gRPC) — modern, recommended

- **Transport:** gRPC over HTTP/2; binary event messages in **Apache Avro** format ([Get Started | Pub/Sub API](https://developer.salesforce.com/docs/platform/pub-sub-api/guide/intro.html); [Pub/Sub API overview](https://developer.salesforce.com/docs/platform/pub-sub-api/overview)).
- **Endpoint:** global `api.pubsub.salesforce.com:443` or `:7443` (region-specific endpoint available; allowlist the whole domain, not IPs) ([Pub/Sub API Endpoints](https://developer.salesforce.com/docs/platform/pub-sub-api/guide/pub-sub-endpoints.html); community confirms `api.pubsub.salesforce.com:7443` ([Diving into Salesforce's Pub/Sub API](https://www.linkedin.com/pulse/diving-salesforces-pubsub-api-heroforce))).
- **RPC methods:**
  - `Subscribe(stream FetchRequest) returns (stream FetchResponse)` — bidirectional, **pull-based with flow control**. Client sets `num_requested` (max **100**) per FetchRequest; server reports `pending_num_requested`. First FetchRequest sets the topic and replay option ([Subscribe RPC](https://developer.salesforce.com/docs/platform/pub-sub-api/references/methods/subscribe-rpc.html)).
  - `Publish(PublishRequest) returns (PublishResponse)` — unary; synchronous batch publish returning per-event `PublishResult` (final result, not queueing). `CreatedDate`/`CreatedById` required in payload ([Publish RPC](https://developer.salesforce.com/docs/platform/pub-sub-api/references/methods/publish-rpc.html); [Publish Event Messages with Pub/Sub API](https://developer.salesforce.com/docs/atlas.en-us.platform_events.meta/platform_events/platform_events_publish_pubsub_api.htm)).
  - `PublishStream(stream PublishRequest) returns (stream PublishResponse)` — bidirectional, higher publish rate ([Publish Event Messages with Pub/Sub API](https://developer.salesforce.com/docs/atlas.en-us.platform_events.meta/platform_events/platform_events_publish_pubsub_api.htm)).
  - `GetSchema(SchemaRequest) returns (SchemaResponse)` — returns Avro schema by schema ID; cache it and re-fetch only when the schema ID changes ([GetSchema RPC](https://developer.salesforce.com/docs/platform/pub-sub-api/references)).
  - `GetTopic(TopicRequest) returns (TopicInfo)` — returns topic name, schema ID, and publish/subscribe authorization ([GetTopic RPC](https://developer.salesforce.com/docs/platform/pub-sub-api/references/methods/gettopic-rpc.html)).
  - `ManagedSubscribe` (Beta) — server-side replay tracking: commit the last processed ReplayId on the server so you don't manage a client-side replay store; **max 200 managed subscriptions per org** ([Subscribe RPC](https://developer.salesforce.com/docs/platform/pub-sub-api/references/methods/subscribe-rpc.html); [Salesforce API Guide 2026](https://unified.to/blog/salesforce_api_a_complete_guide_2026)).
- **Replay / retention:** platform events and CDC events retained in the event bus for **3 days (72h)**. Subscribe at any position via `replay_preset` + `replay_id` in the first FetchRequest; subsequent FetchRequests' replay options are ignored (must re-call Subscribe to reposition) ([Subscribe RPC](https://developer.salesforce.com/docs/platform/pub-sub-api/references/methods/subscribe-rpc.html)).
- **Why Salesforce recommends it over CometD:** single interface for publish + subscribe + schema retrieval; efficient binary Avro over HTTP/2; flow control; 11 gRPC languages; final publish results. Streaming API delivers the whole message as JSON, whereas Pub/Sub delivers the payload as Avro binary (schema + replayId + payload retrieved separately) — Salesforce explicitly says: "we recommend you use Pub/Sub API instead of Streaming API" ([Streaming API vs. Pub/Sub API](https://developer.salesforce.com/docs/atlas.en-us.api_streaming.meta/api_streaming/pubsub_api_streaming_api_comparison.htm); [Differences Between Change Events Received with Streaming API vs. Pub/Sub API](https://developer.salesforce.com/docs/atlas.en-us.api_streaming.meta/api_streaming/cdc_event_diff_pubsub_cometd.htm)).
- **Availability:** Enterprise, Performance, Unlimited, Developer editions (not Government Cloud) ([Get Started | Pub/Sub API](https://developer.salesforce.com/docs/platform/pub-sub-api/guide/intro.html)).
- **Auth:** pass access token + tenant/instance ID as gRPC metadata (`accesstoken`, `instanceurl`, `tenantid`) ([Configure Client Parameters](https://developer.salesforce.com/docs/platform/pub-sub-api/guide/qs-java-parameters.html)).

---

## A7. Change Data Capture (CDC)

- CDC publishes change events to the event bus on record create/update/delete/undelete; subscribe via Pub/Sub API or CometD. Channels: standard combined `/data/ChangeEvents`, per-entity `/data/<Entity>ChangeEvent`, custom `/data/<Name>__chn` ([Configure Client Parameters](https://developer.salesforce.com/docs/platform/pub-sub-api/guide/qs-java-parameters.html); [Message Durability](https://developer.salesforce.com/docs/atlas.en-us.api_streaming.meta/api_streaming/using_streaming_api_durability.htm)).
- **`ChangeEventHeader` fields:** `changeType`, `entityName`, `recordIds`, `changeOrigin`, `transactionKey`, `sequenceNumber`, `commitTimestamp`, `commitNumber`, `commitUser`, `diffFields` ([ChangeEventHeader Fields](https://developer.salesforce.com/docs/atlas.en-us.change_data_capture.meta/change_data_capture/cdc_event_fields_header.htm)).
- **`changeType` values:** `CREATE`, `UPDATE`, `DELETE`, `UNDELETE`, plus gap/overflow types `GAP_CREATE`, `GAP_UPDATE`, `GAP_DELETE`, `GAP_UNDELETE`, `GAP_OVERFLOW` ([ChangeEventHeader Fields](https://developer.salesforce.com/docs/atlas.en-us.change_data_capture.meta/change_data_capture/cdc_event_fields_header.htm)).
- **Gap events:** sent when Salesforce can't generate a normal change event — event >1 MB, certain custom-field type conversions, internal errors, or changes applied directly in the DB outside an app-server transaction (archiving, cleanup jobs). A gap event carries header info (changeType + recordIds) but **no record fields**; subscribers should re-query the record by ID. `transactionKey` may be a DB transaction ID or an app-server transaction ID depending on cause ([Gap Events](https://developer.salesforce.com/docs/atlas.en-us.change_data_capture.meta/change_data_capture/cdc_other_events_gap.htm)).
- **Overflow events** (`GAP_OVERFLOW`): emitted when too many changes occur in one transaction ([Overflow Events](https://developer.salesforce.com/docs/atlas.en-us.change_data_capture.meta/change_data_capture/cdc_other_events_overflow.htm)).
- **Retention: 72 hours** ([Gap Events](https://developer.salesforce.com/docs/atlas.en-us.change_data_capture.meta/change_data_capture/cdc_other_events_gap.htm); [Message Durability](https://developer.salesforce.com/docs/atlas.en-us.api_streaming.meta/api_streaming/using_streaming_api_durability.htm)).
- **Replay:** same replayId model as Streaming/Pub/Sub (opaque, non-contiguous). Example CDC event has `changeType:"CREATE"`, `entityName:"Account"`, `commitTimestamp`, `commitNumber`, `replayId:6421` ([Message Durability](https://developer.salesforce.com/docs/atlas.en-us.api_streaming.meta/api_streaming/using_streaming_api_durability.htm)).
- **Gotcha:** disable CDC before large bulk updates to avoid mass event publishing and delivery delays ([CDC Event Delivery Delays](https://help.salesforce.com/s/articleView?id=005317091&language=en_US&type=1)).

---

## A8. Platform Events

- **Definition:** a platform event is modeled like a custom sObject; the system appends the **`__e` suffix** to the API name (e.g. object "Low Ink" → `Low_Ink__e`). Standard platform events (e.g. `AssetTokenEvent`) have no suffix. Only Checkbox, Date, Date/Time, Number, Text, Text Area field types are allowed. Stored as `CustomObject` metadata with `eventType` (`HighVolume`) and `publishBehavior` ([Define and Manage Platform Events](https://help.salesforce.com/s/articleView?id=platform.platform_events.htm&language=en_US&type=5); [Migrate Platform Event Definitions with Metadata API](https://developer.salesforce.com/docs/atlas.en-us.platform_events.meta/platform_events/platform_events_metadata_api.htm)).
- **Publish behavior:** *Publish Immediately* (default; published when the publish call executes, decoupled from the transaction — good for logging) vs *Publish After Commit* (published only after the transaction commits — required when subscribers need committed data) ([Define and Publish Platform Events (Trailhead)](https://trailhead.salesforce.com/content/learn/modules/platform_events_basics/platform_events_define_publish); [Decoupled Publishing and Subscription](https://developer.salesforce.com/docs/atlas.en-us.platform_events.meta/platform_events/platform_events_considerations_decoupled_processes.htm)).
- **Publish methods:**
  - Apex: `EventBus.publish(List<Event__e>)` returning `Database.SaveResult[]`; final result via Apex publish callbacks (`EventBus.EventPublishSuccessCallback` / `FailureCallback`) ([Track Platform Event Publishing Using Apex Callbacks](https://developer.salesforce.com/blogs/2024/08/track-platform-event-publishing-using-apex-callbacks)).
  - Flow (Create Records element).
  - REST API: `POST /services/data/vXX.X/sobjects/<Event>__e/` with the field JSON; multiple events via `POST /composite/` ([Publish Event Messages with Salesforce APIs](https://developer.salesforce.com/docs/atlas.en-us.platform_events.meta/platform_events/platform_events_publish_api.htm)).
  - SOAP API, Bulk API 2.0 (insert like sObjects).
  - **Pub/Sub API** (`Publish`/`PublishStream`) — recommended for external apps ([Publish Event Messages with Pub/Sub API](https://developer.salesforce.com/docs/atlas.en-us.platform_events.meta/platform_events/platform_events_publish_pubsub_api.htm)).
- **Subscribe channels:** `/event/<Event>__e` (CometD or Pub/Sub); Apex `after insert` triggers on the event object ([Configure Client Parameters](https://developer.salesforce.com/docs/platform/pub-sub-api/guide/qs-java-parameters.html); [Platform Events in Salesforce (Apex Hours)](https://www.apexhours.com/platform-events-in-salesforce/)).
- **High-volume vs standard:** high-volume platform events (and CDC) are retained **72 hours**; standard-volume events (pre-Spring '19 only) are no longer available and were 24h ([Message Durability](https://developer.salesforce.com/docs/atlas.en-us.api_streaming.meta/api_streaming/using_streaming_api_durability.htm)).
- **Gotchas:** you cannot query platform events via SOQL/SOSL; published events can't be rolled back; only `after insert` triggers are supported; when using publish callbacks with event correlation, create events via `sObjectType.newSObject(null, true)` to initialize `EventUuid` before publishing ([Platform Events in Salesforce (Apex Hours)](https://www.apexhours.com/platform-events-in-salesforce/); [Track Platform Event Publishing Using Apex Callbacks](https://developer.salesforce.com/blogs/2024/08/track-platform-event-publishing-using-apex-callbacks)).

---

# PART B — How OSS Kafka Connectors Are Built

## B1. Kafka Connect framework

- **Java/JVM framework.** Confluent's developer guide shows all connector code as Java classes using standard Java types (`java.lang.Integer`, `Map`, `Collection`) and Java I/O; the built-in file connectors live in the `org.apache.kafka.connect.file` package ([Guide for Kafka Connector Developers](https://docs.confluent.io/platform/current/connect/devguide.html)).
- **Core interfaces:** implement `Connector` + `Task`. Sources extend `SourceConnector`/`SourceTask`; sinks extend `SinkConnector`/`SinkTask` ([Guide for Kafka Connector Developers](https://docs.confluent.io/platform/current/connect/devguide.html)).
  - `SourceTask.poll()` returns `List<SourceRecord>`; `commit()`/`commitRecord()` acknowledge offsets in the source system after write to Kafka.
  - `SinkTask.put(Collection<SinkRecord>)`, `flush(Map<TopicPartition,Long>)`, `open()`/`close()` (partition rebalance).
- **Offset storage:** each `SourceRecord` carries a `sourcePartition` map and `sourceOffset` map; the framework periodically commits these so a task resumes after failure. On restart, read prior offsets via `context.offsetStorageReader().offset(partitionMap)` ([Guide for Kafka Connector Developers](https://docs.confluent.io/platform/current/connect/devguide.html)). **This is exactly where a Salesforce replayId or Bulk cursor would be stored.**
- **Converters:** pluggable `Converter` (e.g. `AvroConverter`, `JsonConverter`, `StringConverter`) translate between the runtime `org.apache.kafka.connect.data` format (`Schema`, `Struct`, `SchemaBuilder`) and serialized `byte[]` ([Guide for Kafka Connector Developers](https://docs.confluent.io/platform/current/connect/devguide.html)).
- **SMTs (Single Message Transforms):** `Transformation` plugin type; also `Predicate`, `HeaderConverter`. In CP 8.3 (Kafka 4.3) all plugin types implement `ConnectPlugin` ([Guide for Kafka Connector Developers](https://docs.confluent.io/platform/current/connect/devguide.html)).
- **Packaging:** distributed as a tarball/ZIP of JARs on the `plugin.path`, or an uber-JAR; must not bundle the Connect API/runtime ([Guide for Kafka Connector Developers](https://docs.confluent.io/platform/current/connect/devguide.html)).
- **JVM-only, but any JVM language works:** the API is `org.apache.kafka:connect-api` and is consumed as ordinary JVM bytecode, so Scala/Kotlin implementations are viable (see B5).

## B2. Debezium

- **CDC platform built on Kafka Connect**, reusing Kafka + Kafka Connect for durability, ordering, and fault tolerance; connectors run as Kafka Connect **source** connectors (one per upstream DB server, typically one topic per table). Also offers an **embedded engine** to run a connector inside an app without Kafka Connect ([debezium/debezium GitHub](https://github.com/debezium/debezium)).
- **Language:** **Java 93.6%** ([debezium/debezium GitHub](https://github.com/debezium/debezium)).
- **License:** **Apache-2.0** ([debezium/debezium GitHub](https://github.com/debezium/debezium)).
- **Structure:** modular — generic modules (`debezium-connector-common`, `debezium-connector-jdbc`, `debezium-connector-binlog`) plus DB-specific modules (`mysql`, `postgres`, `oracle`, `sqlserver`, `mongodb`, `mariadb`) ([debezium/debezium GitHub](https://github.com/debezium/debezium)). There is no first-party Debezium Salesforce connector, but its architecture (source connector + offset-based checkpointing + embedded engine) is the canonical reference for a CDC-style connector.

## B3. Existing OSS Salesforce Kafka connectors

| Project | Language | SF API(s) | License | Maintenance | Notes |
|---|---|---|---|---|---|
| [jcustenborder/kafka-connect-salesforce](https://mvnrepository.com/artifact/com.github.jcustenborder.kafka.connect) | Java | Streaming API (PushTopic) | Apache 2.0 | Stale — last Maven release **May 18, 2018** ([MVNRepository](https://mvnrepository.com/artifact/com.github.jcustenborder.kafka.connect)) | Original community SF source connector; `SalesforceSourceConnector`/`SalesforceSourceTask` in package `com.github.jcustenborder.kafka.connect.salesforce`. Repo now returns 404 (removed/relocated). |
| [Entanet/kafka-salesforce-connect](https://github.com/Entanet/kafka-salesforce-connect) | **Java 100%** | Streaming API (PushTopic) | **Apache 2.0** | Low activity (last commit ~2018, 11★, 2 contributors) | Fork of jcustenborder; auto-creates PushTopic from sObject describe when `salesforce.push.topic.create=true`. Uses username + consumer key/secret + password + security token auth. |
| [asitm9/kafka-connect-salesforce-bulkapiv2](https://github.com/asitm9/kafka-connect-salesforce-bulkapiv2) | **Java 100%** | **Bulk API 2.0** | `n.a.` (no license stated) | Minimal (2★, no releases, ~2020) | Proof-of-concept Bulk API 2.0 connector. |
| [vrudenskyi/kafka-connect-pollable-source](https://github.com/vrudenskyi/kafka-connect-pollable-source) (+ [api-clients](https://github.com/vrudenskyi/kafka-connect-api-clients)) | Java | REST — EventLog + SObject polling | `n.a.` | ~2019 | Generic pollable source; `EventLogPollableAPIClient` + `SobjectPollableAPIClient`. |
| [nodefluent/salesforce-kafka-connect](https://github.com/nodefluent/salesforce-kafka-connect) | **Node.js** (not JVM) | REST + Bulk options | `n.a.` | Old (~2017) | Not a JVM Kafka Connect plugin — runs on the Node kafka-connect port. Listed as ⚠️ in [conduktor/awesome-kafka-connect](https://github.com/conduktor/awesome-kafka-connect). |
| [stanlemon/kafka-connect-salesforce](https://github.com/stanlemon/kafka-connect-salesforce/blob/master/README.md) | Java | Streaming API | `n.a.` | **Archived (read-only, Mar 2020)** | Config `salesforce.version=latest`. |

**Summary:** the OSS landscape is fragmented, mostly abandoned, JVM/Java-centric, and dominated by the legacy **Streaming API / PushTopic** approach. Stack Overflow confirms no fully-maintained OSS Salesforce Kafka connector exists — only customizable source snippets ([Consuming a Kafka Topic from Salesforce (SO)](https://stackoverflow.com/questions/58634520/consuming-a-kafka-topic-from-salesforce)). **No maintained OSS connector uses the modern Pub/Sub API — a clear gap the new project can fill.**

## B4. Apache Camel — camel-salesforce & Camel Kafka Connector

- **Camel Salesforce component** (`camel-salesforce`, since Camel 2.12): Java-based, communicates via generated Java DTOs (companion Maven plugin generates them). Supports **producer and consumer** endpoints ([Salesforce — Apache Camel](https://camel.apache.org/components/4.10.x/salesforce-component.html)).
- **SF APIs wrapped:** REST API, Apex REST API, **Bulk API**, **Bulk 2 API**, **Pub/Sub API**, **Streaming API**, Reports API ([Salesforce — Apache Camel](https://camel.apache.org/components/4.10.x/salesforce-component.html)). This is the **most complete OSS coverage of Salesforce APIs on the JVM** and is Apache 2.0 (Apache Camel project).
- **OAuth flows supported:** Username-Password, Refresh Token, **JWT Bearer**, **Client Credentials** (`authenticationType` = `USERNAME_PASSWORD`/`REFRESH_TOKEN`/`CLIENT_CREDENTIALS`/`JWT`). Config keys: `clientId`, `clientSecret`, `userName`, `password`, `refreshToken`, `keystore`, `loginUrl`, `instanceUrl`, `jwtAudience` ([Salesforce — Apache Camel](https://camel.apache.org/components/4.10.x/salesforce-component.html)).
- **Camel Kafka Connector** ([apache/camel-kafka-connector](https://github.com/apache/camel-kafka-connector)) wraps Camel components (including camel-salesforce) as Kafka Connect source/sink connectors — a viable route to reuse Camel's SF API coverage inside Kafka Connect (issues note a `CamelSalesforcesourceSourceConnector`). Historically the Camel SF source connector has had disconnect issues ([camel-kafka-connector issue #1339](https://github.com/apache/camel-kafka-connector/issues/1339)).

## B5. Scala for Kafka connectors (stream-reactor) & Kotlin

- **Scala is a proven choice.** **Lenses.io stream-reactor** is a large collection of production Kafka Connect connectors written primarily in **Scala** — language breakdown **Scala 76.0% / Java 23.8%**, **Apache-2.0**, actively maintained (v10.0.0, 96 releases) ([lensesio/stream-reactor GitHub](https://github.com/lensesio/stream-reactor)). It demonstrates that a real-world, actively-maintained connector suite can be built in Scala on the JVM Connect API.
- **Kotlin is also viable.** A published guide shows building Kafka Connect source connectors in **Kotlin** (Gradle `kotlin("jvm")` + `org.apache.kafka:connect-api`), leveraging full Java interop and implementing `SourceConnector` directly ([Building Kafka Connectors with Kotlin](https://es.slideshare.net/slideshow/building-kafka-connectors-with-kotlin-a-stepbystep-guide-to-creation-and-deployment/267076329)).
- **Java** remains the default: Debezium, jcustenborder, Entanet, Camel, and the Connect framework itself are Java.

## B6. Salesforce JVM SDKs / Pub/Sub gRPC stubs

- **Pub/Sub API is language-agnostic gRPC + protobuf.** The official [forcedotcom/pub-sub-api](https://github.com/forcedotcom/pub-sub-api) repo ships the critical `.proto` file and code samples in **Java, Python, Go** (repo is Java 67.3% / Go 16.6% / Python 15.9%); license **CC0-1.0**. You generate client stubs from the `.proto` and pair them with an Avro library (Apache Avro Java for the JVM, or **avro4k** for Kotlin) ([forcedotcom/pub-sub-api GitHub](https://github.com/forcedotcom/pub-sub-api)). gRPC officially supports 11 languages ([forcedotcom/pub-sub-api GitHub](https://github.com/forcedotcom/pub-sub-api)).
- **Java SOAP libraries (official-ish):** `com.force.api:force-wsc` (WSC/Web Service Connector, includes `wsdlc` to compile Partner/Enterprise WSDLs) and `com.force.api:force-partner-api` provide `PartnerConnection`, `ConnectorConfig`, `login()` — the traditional JVM path for SOAP + Bulk metadata ([Partner WSDL SOAP integration example (Qiita)](https://qiita.com/hakozaki/items/be4c2416117076eb036f)).
- **REST/Bulk on JVM:** no single official Salesforce Java REST SDK is mandated — the REST/Bulk 2.0 APIs are plain HTTPS/JSON, so any JVM HTTP client (OkHttp, java.net.http) plus `force-wsc` for SOAP-only needs suffices. (No maintained first-party Java REST SDK confirmed — `n.a.`)

---

## Part B Conclusion — Language / Framework Assessment

Kafka Connect is a **JVM framework** consumed through the `org.apache.kafka:connect-api` Java artifact; the connector must ultimately produce JVM bytecode implementing `SourceConnector`/`SourceTask` (and optionally `SinkConnector`/`SinkTask`) and be packaged as JARs on the `plugin.path` ([Guide for Kafka Connector Developers](https://docs.confluent.io/platform/current/connect/devguide.html)). Given that constraint, three realistic options:

1. **Java** — lowest friction and the ecosystem default. Every reference implementation (Kafka Connect itself, Debezium, camel-salesforce, all existing OSS SF connectors) is Java. Direct use of Apache Avro Java, gRPC-Java stubs for Pub/Sub API, and `force-wsc` for SOAP. Best for contributor familiarity and long-term maintenance. **Recommended primary choice.**

2. **Scala** — proven at scale by Lenses **stream-reactor** (Scala 76%), Apache-2.0, actively maintained ([lensesio/stream-reactor GitHub](https://github.com/lensesio/stream-reactor)). Gives concise, functional code and strong typing; interoperates with the Java Connect API and gRPC/Avro Java libraries. Viable, but a smaller Kafka-Connect contributor pool and heavier build/tooling than Java. Best if the team already has Scala expertise.

3. **Kotlin** — modern, concise, 100% Java-interop on the JVM; demonstrated for Kafka Connect source connectors ([Building Kafka Connectors with Kotlin](https://es.slideshare.net/slideshow/building-kafka-connectors-with-kotlin-a-stepbystep-guide-to-creation-and-deployment/267076329)), with `avro4k` available for Avro. Fewer public precedents than Java/Scala but low risk given full interop. Good middle ground if the team prefers Kotlin ergonomics.

**Net recommendation:** build on **Java** (or Kotlin if the team prefers it) targeting the **Pub/Sub API (gRPC+Avro)** for real-time CDC/Platform-Event sourcing and **Bulk API 2.0** for large initial loads/backfills, authenticating via **Client Credentials / JWT Bearer**. Store the Pub/Sub `replayId` (as bytes) and Bulk cursor in Kafka Connect's `sourceOffset`. The biggest OSS gap is that **no maintained connector uses Pub/Sub API** — the existing ones are stuck on the retiring CometD Streaming API — so a modern Pub/Sub-based, Apache-2.0 connector is a differentiated, defensible open-source alternative to Confluent's proprietary suite.
