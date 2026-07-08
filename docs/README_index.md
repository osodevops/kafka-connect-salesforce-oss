# Open-Source Salesforce ↔ Kafka Connectors — Research & PRD Package

Clean-room research and product requirements for building an open-source alternative to Confluent's proprietary Salesforce Kafka connector suite.

## Read in this order

1. **`00_feasibility_and_cleanroom_report.md`** — Start here. Feasibility verdict, IP/clean-room strategy, tech-stack recommendation (Kotlin/Java over Scala, with reasoning), target architecture, full feature-parity matrix (Confluent's 11 → 4 OSS connectors), risks, and phased program of work.

## PRDs (the program of work)

2. **`05_prd_sf_core_library.md`** (PRD-00) — `sf-core` shared library. **Phase 0 foundation** — auth, Pub/Sub client, Bulk 2.0 client, REST/SOQL, schema mapping, rate governor. Everything depends on this.
3. **`01_prd_source_connector.md`** (PRD-01) — Flagship unified Source connector: snapshot + CDC/event streaming (Pub/Sub) + polling, multi-SObject, gap recovery, seamless handoff. Replaces 6 Confluent source connectors.
4. **`02_prd_sobject_sink_connector.md`** (PRD-02) — SObject Sink: CRUD + external-ID upsert via Bulk 2.0/REST, Big Objects, DLQ. Replaces 3 Confluent sinks.
5. **`03_prd_platform_event_sink_connector.md`** (PRD-03) — Platform Event Sink: publish events via Pub/Sub API.
6. **`04_prd_legacy_streaming_source_connector.md`** (PRD-04) — Optional CometD fallback for orgs without Pub/Sub access.

## Supporting research (fully source-linked)

- **`confluent_connectors_detail.md`** — Implementation-grade reference for all 11 Confluent Salesforce connectors: class names, complete config property tables, auth, delivery/offset/replay semantics, limits, API versions. Every value linked to a Confluent doc page.
- **`sf_apis_and_oss_architecture.md`** — The underlying Salesforce APIs (OAuth flows, REST/SOQL, Bulk 1.0/2.0, CometD Streaming, Pub/Sub gRPC, CDC, Platform Events) at implementation depth, plus a survey of how existing OSS Kafka connectors are built.

## Headline recommendation

Build a **modern, Pub/Sub-first, 4-connector suite** (Kotlin or Java) that reaches full parity with Confluent's 11 connectors. No maintained OSS Salesforce connector uses the Pub/Sub API today — that is the differentiation. Do it clean-room from Salesforce's public APIs, with your own config namespace; never touch Confluent's code.
