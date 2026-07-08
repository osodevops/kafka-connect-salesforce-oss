# Open-Source Salesforce ↔ Kafka Connectors — Feasibility & Clean-Room Strategy

**Prepared for:** OSO / Keito.ai engineering
**Goal:** Determine whether Confluent's proprietary Salesforce Kafka connector suite can be legally and technically re-implemented as a maintained open-source alternative, and define the architecture and program of work (PRDs) to do so.
**TL;DR:** Yes. It is both legal (clean-room, from public Salesforce + Confluent docs) and technically tractable. The right move is not to clone Confluent's 7–11 legacy connectors one-for-one, but to build a **smaller, modern connector set built on the Salesforce Pub/Sub API (gRPC) + Bulk API 2.0**, which achieves full feature parity while filling a real market gap: no maintained OSS connector uses Pub/Sub API today.

---

## 1. Executive summary

Confluent ships a proprietary, separately-licensed Salesforce connector suite. On self-managed Confluent Platform it is 7 license-counted connector types; on Confluent Cloud there are additional fully-managed variants, including a newer unified **Source V2** connector. In total there are 11 distinct connector plugins ([Confluent overview](https://docs.confluent.io/kafka-connectors/salesforce/current/overview.html), [Cloud connectors list](https://docs.confluent.io/cloud/current/connectors/overview.html)).

Three findings drive the strategy:

1. **The connectors are thin adapters over public Salesforce APIs.** Every capability maps to a documented Salesforce API (Streaming/CometD, Pub/Sub gRPC, REST, Bulk API 1.0/2.0). There is no proprietary Salesforce protocol involved — the "moat" is engineering effort and packaging, not secret knowledge. This makes a clean-room re-implementation straightforward from public docs.

2. **Confluent's own newest connector (Source V2) points to the future.** It abandons CometD in favour of the **Pub/Sub API** for CDC/events and **Bulk API 2.0** for snapshots/polling ([Source V2 docs](https://docs.confluent.io/cloud/current/connectors/cc-salesforce-source-v2.html)). Salesforce itself recommends Pub/Sub API over the retiring Streaming API ([Streaming vs Pub/Sub](https://developer.salesforce.com/docs/atlas.en-us.api_streaming.meta/api_streaming/pubsub_api_streaming_api_comparison.htm)). We should skip the legacy path where possible.

3. **The OSS field is wide open.** Existing OSS Salesforce Kafka connectors (jcustenborder, Entanet, stanlemon, asitm9) are almost all abandoned, Java-only, and stuck on the legacy CometD Streaming API ([OSS survey below](#7-competitive-oss-landscape)). None uses Pub/Sub API. A maintained, Apache-2.0, Pub/Sub-first connector would be genuinely differentiated.

**Recommendation:** Build **4 connectors** that deliver parity with Confluent's 11:

| New OSS connector | Replaces (Confluent) | Salesforce API |
|---|---|---|
| **SF Source** (unified) | PushTopic Source, CDC Source, Platform Event Source, Bulk API 1.0/2.0 Source, Source V2 | Pub/Sub API (CDC + Platform Events) + Bulk API 2.0 (snapshot/poll) + REST |
| **SF SObject Sink** | SObject Sink, Bulk API 1.0/2.0 Sink | Bulk API 2.0 (+ REST/Composite for low-latency) |
| **SF Platform Event Sink** | Platform Event Sink | Pub/Sub API Publish |
| **SF Legacy Streaming Source** (optional) | PushTopic Source, CDC/PE Source on old orgs | CometD (only for orgs without Pub/Sub access) |

This is fewer moving parts, matches Confluent's own direction, and is future-proof.

---

## 2. Is this legal? Clean-room & IP assessment

**Short answer: yes, if you follow a disciplined clean-room process and never look at Confluent's source or decompiled bytecode.**

### 2.1 What you are and aren't allowed to copy

- **APIs and observable behaviour are not copyrightable in a way that blocks re-implementation.** Re-implementing an interface/behaviour to interoperate is well-established (the *Google v. Oracle*, 2021, fair-use ruling on API re-implementation is the guiding precedent for API surface reuse in the US). You are targeting the **Salesforce** APIs — which are public and documented by Salesforce, not Confluent — so you are interoperating with Salesforce, not cloning Confluent.
- **Configuration property names are a grey area.** Confluent's exact property strings (e.g. `salesforce.push.topic.notify.undelete`) are part of their product's config surface. Functional, descriptive config keys are low-risk, but to be safe and to avoid any "derived from their docs" appearance, **design your own config namespace** (e.g. `sf.*` / `connector.*`) and document the mapping for migrators separately. Do **not** copy their property descriptions verbatim into your docs.
- **Confluent's connector source code and decompiled JARs are off-limits.** The self-managed connectors are proprietary/licensed ([overview](https://docs.confluent.io/kafka-connectors/salesforce/current/overview.html)). Never decompile, never read leaked source, never copy code.

### 2.2 Clean-room process to follow

1. **Specification team** (this research + these PRDs) reads only: Salesforce public developer docs and Confluent's public *documentation* (behaviour and config descriptions), and writes behaviour specs in our own words. That is what this deliverable is.
2. **Implementation team** builds from the specs and the Salesforce API docs only. They must not read Confluent's code, decompile plugins, or copy config descriptions.
3. Keep a written record (these PRDs + git history) showing the design was derived from Salesforce docs and independent specification, not from Confluent's implementation.
4. Use only permissively-licensed dependencies (see §5).

### 2.3 Salesforce-side constraints (not a blocker, but plan for them)

- Salesforce APIs are public and free to call within org limits; there is no Salesforce license barrier to building a client. You interoperate as any integration would.
- The Pub/Sub API `.proto` is published by Salesforce under CC0-1.0 ([forcedotcom/pub-sub-api](https://github.com/forcedotcom/pub-sub-api)) — explicitly free to use.

**Conclusion:** Clean-room re-implementation targeting Salesforce's public APIs, with our own config namespace and independently-written docs, is low legal risk. The main discipline is: never touch Confluent code, and don't lift their config strings/descriptions verbatim.

---

## 3. Recommended technology stack

Kafka Connect is a **JVM framework** — a connector must produce JVM bytecode implementing `SourceConnector`/`SourceTask`/`SinkConnector`/`SinkTask` and ship as JARs on the `plugin.path` ([Kafka Connect dev guide](https://docs.confluent.io/platform/current/connect/devguide.html)). Any JVM language works.

### 3.1 Language

You asked me not to assume Scala and to recommend based on how OSS connectors are actually built. My recommendation:

- **Primary: Kotlin (or Java).** Every reference implementation — Kafka Connect itself, Debezium (Java 93.6%, Apache-2.0), camel-salesforce, all existing OSS SF connectors — is Java ([Debezium](https://github.com/debezium/debezium)). Kotlin gives you modern ergonomics with 100% Java interop, gRPC-Kotlin and `avro4k` support, and demonstrated Kafka Connect precedent ([Kotlin Kafka Connect guide](https://es.slideshare.net/slideshow/building-kafka-connectors-with-kotlin-a-stepbystep-guide-to-creation-and-deployment/267076329)). It maximises the contributor pool.
- **Viable alternative: Scala.** Lenses.io **stream-reactor** proves a large, actively-maintained connector suite in Scala (Scala 76%, Apache-2.0, 96 releases) ([stream-reactor](https://github.com/lensesio/stream-reactor)). Choose Scala only if the team already lives in Scala — the trade-off is a smaller Kafka-Connect contributor pool and heavier tooling.

**Net:** Kotlin for reach and maintainability; Scala acceptable if it matches your team's fluency. Java is the safe default. This program of work is written to be language-agnostic — the PRDs specify behaviour, not language.

### 3.2 Core dependencies (all permissive licenses)

- `org.apache.kafka:connect-api` (Apache-2.0) — the Connect SPI. Do not bundle it.
- **Pub/Sub API:** generate gRPC stubs from Salesforce's `.proto` (CC0-1.0) + gRPC-Java/Kotlin + Apache Avro (Java) or avro4k.
- **REST / Bulk 2.0:** plain HTTPS/JSON — any JVM HTTP client (java.net.http, OkHttp) + a JSON lib. No first-party Salesforce Java REST SDK is required.
- **SOAP (only if needed for metadata):** `com.force.api:force-wsc`.
- **Auth:** a JWT/RS256 library (e.g. Nimbus JOSE) for JWT Bearer; standard OAuth token calls otherwise.
- **Schema Registry:** Confluent's `kafka-connect-avro-converter` etc. are Confluent Community License — for a fully-OSS story, support Apicurio Registry / plain Avro as well, and let users choose converters (converters are pluggable and user-supplied anyway).

---

## 4. Target architecture

```
                    ┌─────────────────────────────────────────────┐
                    │            sf-kafka-connect (repo)           │
                    ├─────────────────────────────────────────────┤
   Salesforce       │  sf-core (shared)                            │      Kafka
   ──────────       │   • Auth: Client Credentials, JWT Bearer     │      ─────
   Pub/Sub gRPC ◄───┤   • Pub/Sub client (Subscribe/Publish/       │
   Bulk API 2.0 ◄───┤     GetSchema/GetTopic, replay store)        ├──► SourceRecord
   REST/SOQL    ◄───┤   • Bulk 2.0 client (ingest+query jobs)      │      (Avro/JSON/
   CometD (opt) ◄───┤   • REST/SOQL client + describe/schema cache │       Protobuf)
                    │   • Avro→Connect Schema mapper               │
                    │   • Rate-limit governor (/limits polling)    │
                    ├─────────────────────────────────────────────┤
                    │  Connectors (thin):                          │
                    │   • SalesforceSourceConnector/Task           │◄── SinkRecord
                    │   • SalesforceSObjectSinkConnector/Task      │
                    │   • SalesforcePlatformEventSinkConnector/Task│
                    │   • SalesforceStreamingSourceConnector (opt) │
                    └─────────────────────────────────────────────┘
```

### 4.1 Design principles
- **One shared `sf-core` library** holds all Salesforce protocol logic (auth, Pub/Sub, Bulk 2.0, REST, schema mapping, rate governance). Connectors are thin.
- **Checkpointing via Kafka Connect `sourceOffset`.** Store the Pub/Sub `replayId` (as bytes — it is opaque and non-contiguous) and Bulk cursor (`SystemModstamp`/`LastModifiedDate` + `jobId`/`locator`) in `sourceOffset`, exactly as the framework intends ([Connect dev guide](https://docs.confluent.io/platform/current/connect/devguide.html)).
- **Schema handling:** fetch Avro schema by ID from Pub/Sub `GetSchema`, cache it, map to Connect `Schema`/`Struct`. For Bulk, use REST `describe` (with `If-Modified-Since` 304 caching) to build schemas.
- **Delivery semantics:** at-least-once (matches Confluent). Document dedup guidance (EventUuid) for consumers.
- **Rate-limit governance:** poll `GET /services/data/vXX.X/limits/` and back off before hitting the 24h API pool; classify retryable errors (HTTP 429/5xx, gRPC UNAVAILABLE/RESOURCE_EXHAUSTED, `REQUEST_LIMIT_EXCEEDED`).

---

## 5. Feature-parity matrix (Confluent → OSS)

| Capability | Confluent connector(s) | OSS connector | Salesforce API used | Parity notes |
|---|---|---|---|---|
| Real-time change capture (create/update/delete/undelete) | CDC Source, PushTopic Source | SF Source (Event-Driven mode) | Pub/Sub API `Subscribe` on CDC channels | Full parity; modern transport. replayId → sourceOffset |
| Platform Event ingestion | Platform Event Source | SF Source (event mode) | Pub/Sub `Subscribe` on `/event/*__e` | Full parity |
| Historical snapshot / bulk backfill | Bulk API 1.0/2.0 Source | SF Source (snapshot mode) | Bulk API 2.0 query jobs, locator paging | Full parity; 2.0 only (1.0 retiring) |
| Periodic polling of SObjects | Bulk API 1.0/2.0 Source | SF Source (polling mode) | Bulk API 2.0 query + `SystemModstamp` cursor | Full parity |
| Snapshot→stream seamless handoff (zero loss) | Source V2 | SF Source | Bulk 2.0 → Pub/Sub, watermark dedup | Full parity (the hard part — see PRD-01) |
| Gap-event recovery | Source V2 | SF Source | Re-query by ID / incremental resync | Full parity |
| Multi-SObject per instance | Source V2, Bulk 2.0 (≤5) | SF Source | independent state per SObject | Parity or better (make limit configurable) |
| Write to SObjects (insert/update/upsert/delete) | SObject Sink, Bulk 1.0/2.0 Sink | SF SObject Sink | Bulk API 2.0 ingest (+ REST/Composite for low latency) | Full parity, incl. external-ID upsert |
| Big Objects (`__b`) sink | Bulk 2.0 Sink | SF SObject Sink | Bulk 2.0 insert | Parity |
| Publish Platform Events | Platform Event Sink | SF Platform Event Sink | Pub/Sub `Publish`/`PublishStream` (or REST) | Full parity |
| Auth: Client Credentials / JWT / user-pass | all | sf-core | OAuth token endpoint | Parity; default to Client Credentials + JWT |
| Output formats Avro/JSON/JSON-SR/Protobuf | all | all | pluggable converters | Parity (user supplies converter) |
| CSFLE / field encryption | modern connectors | (phase 2) | SMT/converter | Deferred; document |
| DLQ / error reporter topics | sinks | sinks | Connect error handling | Parity via Connect's native errant-record reporter |

**Conclusion: full functional parity is achievable with 4 connectors.** The only Confluent extras worth deferring to phase 2 are CSFLE/CSPE (client-side encryption) and the Confluent-specific "reporter" success/error topic format (Kafka Connect's built-in DLQ + errant-record reporter covers the important cases).

---

## 6. Underlying Salesforce APIs — implementation cheat sheet

Full detail with source links is in `sf_apis_and_oss_architecture.md`. Key facts the implementation team must internalise:

- **Auth:** Use My-Domain token endpoint `POST /services/oauth2/token`. **Client Credentials** (`grant_type=client_credentials`, needs a "Run As" API-Only integration user; `login.salesforce.com` is NOT accepted — must use My Domain) and **JWT Bearer** (RS256, no refresh token). Username-Password is deprecated and blocked by default in orgs created Summer '23+ — support it only as a legacy fallback ([OAuth flows](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_client_credentials_flow.htm&type=5)).
- **Pub/Sub API:** gRPC/HTTP-2, `api.pubsub.salesforce.com:443`. `Subscribe` is pull-based with flow control (`num_requested` ≤ 100). Events are binary Avro; fetch schema via `GetSchema` and cache by schema ID. Retention 72h. replayId is opaque bytes, non-contiguous — store as bytes ([Subscribe RPC](https://developer.salesforce.com/docs/platform/pub-sub-api/references/methods/subscribe-rpc.html)).
- **CDC:** `ChangeEventHeader` carries `changeType` (CREATE/UPDATE/DELETE/UNDELETE + GAP_* + GAP_OVERFLOW), `entityName`, `recordIds`, `commitTimestamp`, `commitNumber`, `diffFields`. **Gap events** have no fields — re-query by ID ([Gap Events](https://developer.salesforce.com/docs/atlas.en-us.change_data_capture.meta/change_data_capture/cdc_other_events_gap.htm)).
- **Bulk API 2.0:** `POST /jobs/query` → poll `JobComplete` → `GET /jobs/query/{id}/results?locator=&maxRecords=` paginate until `Sforce-Locator: null`. Same API version to create + read results (else 409). No server-side replay — checkpoint on `SystemModstamp`. Limits: 150M records/24h, 150MB/job, 10k query jobs/24h ([Bulk 2.0 query](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/queries.htm)).
- **REST/SOQL:** `GET /query/?q=` with `nextRecordsUrl` paging; `sobjects/<X>/describe/` for schema (supports 304). API pool: 15k/24h Dev Edition, 100k+ Enterprise ([API limits](https://developer.salesforce.com/docs/atlas.en-us.salesforce_app_limits_cheatsheet.meta/salesforce_app_limits_cheatsheet/salesforce_app_limits_platform_api.htm)).
- **Version hygiene:** target API v60.0+ (Confluent's cloud connectors run up to v65.0). API versions 21–30 are retired ([retirement](https://help.salesforce.com/s/articleView?id=000389618&language=en_US&type=1)).

---

## 7. Competitive OSS landscape

| Project | Language | SF API | License | State |
|---|---|---|---|---|
| jcustenborder/kafka-connect-salesforce | Java | Streaming (PushTopic) | Apache-2.0 | Abandoned (2018), repo 404 |
| Entanet/kafka-salesforce-connect | Java | Streaming (PushTopic) | Apache-2.0 | Low activity (~2018) |
| asitm9/kafka-connect-salesforce-bulkapiv2 | Java | Bulk 2.0 | none stated | PoC only |
| stanlemon/kafka-connect-salesforce | Java | Streaming | none stated | Archived (2020) |
| nodefluent/salesforce-kafka-connect | Node.js | REST/Bulk | none stated | Old (~2017), not a JVM plugin |
| camel-salesforce (via camel-kafka-connector) | Java | REST/Bulk/Bulk2/**Pub-Sub**/Streaming | Apache-2.0 | Maintained; most complete SF API coverage on JVM |

Sources in `sf_apis_and_oss_architecture.md`. **Takeaways:** (1) no maintained standalone OSS SF connector uses Pub/Sub API — clear differentiation; (2) `camel-salesforce` is the closest thing to a maintained, permissive, Pub/Sub-capable SF client on the JVM and is worth evaluating as either a dependency or a reference (Apache-2.0). Consider whether wrapping camel-salesforce accelerates v1 vs. building `sf-core` from scratch (trade-off: speed vs. control/footprint).

---

## 8. Recommended program of work (PRDs)

The detailed PRDs are separate files:

- **PRD-01 — Salesforce Source Connector** (`01_prd_source_connector.md`): the flagship. Unified snapshot + CDC/event streaming + polling, multi-SObject, gap recovery, seamless handoff. Highest complexity, highest value.
- **PRD-02 — Salesforce SObject Sink Connector** (`02_prd_sobject_sink_connector.md`): Bulk 2.0 writes, CRUD + external-ID upsert, Big Objects, DLQ.
- **PRD-03 — Salesforce Platform Event Sink Connector** (`03_prd_platform_event_sink_connector.md`): publish events via Pub/Sub.
- **PRD-04 — Legacy CometD Streaming Source Connector** (`04_prd_legacy_streaming_source_connector.md`): optional fallback for orgs without Pub/Sub access.
- **PRD-00 — sf-core shared library** (`05_prd_sf_core_library.md`): auth, clients, schema mapping, rate governance — the foundation all connectors depend on.

### Suggested delivery phases
1. **Phase 0 (foundation):** sf-core auth + Pub/Sub client + Bulk 2.0 client + schema mapping (PRD-00).
2. **Phase 1 (MVP source):** SF Source in Event-Driven (Pub/Sub CDC) mode, single + multi SObject, replayId checkpointing (PRD-01, streaming path).
3. **Phase 2 (source completeness):** snapshot mode, polling mode, seamless handoff, gap recovery (PRD-01, full).
4. **Phase 3 (sinks):** SObject Sink + Platform Event Sink (PRD-02, PRD-03).
5. **Phase 4 (compat & polish):** legacy CometD source (PRD-04), CSFLE, migration guide from Confluent config.

---

## 9. Key risks & mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Salesforce API quota exhaustion | Connector fails / throttled | Rate governor polling `/limits/`; Bulk 2.0 for volume; backoff on `REQUEST_LIMIT_EXCEEDED` |
| Seamless snapshot→stream handoff (dedup) | Duplicate or lost records | Watermark: probe LATEST replayId during snapshot; drop CDC events with commit ts < snapshot completion (mirrors Source V2) |
| Gap/overflow events lose field data | Missing changes | Re-query by recordId on GAP_*; incremental resync fallback |
| replayId opacity / non-contiguity | Bad checkpoint logic | Treat as opaque bytes; never assume numeric/contiguous; use EventUuid for dedup |
| Pub/Sub not available (edition/Gov Cloud) | Some orgs can't use event mode | Ship legacy CometD source (PRD-04) + Bulk polling mode |
| Schema evolution (Avro schema ID changes) | Deserialization breaks | Cache schema by ID; refetch on new ID; map to evolving Connect schema |
| Confluent config-string similarity (IP) | Legal appearance | Own `sf.*` namespace; independently-written docs; documented migration mapping |
| Schema Registry converter licensing | "Not fully OSS" | Support Apicurio / plain Avro; converters are user-pluggable |

---

## 10. Verdict

Re-implementing Confluent's Salesforce connectors as maintained open source is **feasible and low-risk** if done clean-room from Salesforce's public APIs. The smart play is not a 1:1 clone but a **modern, Pub/Sub-first, 4-connector suite** that reaches full feature parity while being future-proof and differentiated (no OSS competitor uses Pub/Sub API today). Language should be **Kotlin or Java** (Scala acceptable if the team prefers it), building all Salesforce logic into a shared `sf-core` and keeping the connectors thin.

*Supporting research: `confluent_connectors_detail.md` (all 11 Confluent connectors, config-level detail) and `sf_apis_and_oss_architecture.md` (Salesforce APIs + OSS architecture), both fully source-linked.*
