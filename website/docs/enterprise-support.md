---
title: Enterprise Support
description: Production support for the open-source Salesforce connectors from OSO, the team that builds them - what the subscription covers, response targets and how to get it.
slug: /enterprise-support
sidebar_position: 90
---

# Enterprise Support

The connectors are open source and free to run. If you run them in production and want someone accountable for them, OSO, the team that builds and maintains the connectors, offers an annual **Enterprise support subscription**. This page sets out what it covers so you can decide whether it is right for you. There are no licence keys and nothing changes in the software: the subscription buys support and maintenance commitments, and if it ends the connectors keep running.

## Community or enterprise

| | Community | Enterprise support subscription |
|---|---|---|
| Who | Everyone | Subscribers |
| Channel | [GitHub issues](https://github.com/osodevops/kafka-connect-salesforce-oss/issues) and [discussions](https://github.com/osodevops/kafka-connect-salesforce-oss/discussions) | support@oso.sh, the OSO support portal, and a shared Slack Connect or Microsoft Teams channel |
| Response | Best effort, no commitment | P1 within 60 minutes, P2 within 4 hours, P3 one business day, P4 two business days |
| Who answers | Maintainers, as time allows | The engineers who write the code, directly; no first-line triage layer, no outsourced helpdesk |
| Fixes | Next release | Patch releases for your P1 and P2 defects |

## What the subscription includes

| Component | Detail |
|---|---|
| Maintained releases | Security patches, dependency vulnerability updates and critical defect fixes for the supported versions, published as releases with plugin ZIPs. A defect you raise at P1 or P2 is fixed in a patch release rather than left for the next feature release. |
| Salesforce release readiness | Salesforce ships three releases a year. The supported connector versions are tested against each one during Salesforce's sandbox preview window, and you are told in writing of anything you need to act on before the release reaches your production orgs. |
| Kafka Connect compatibility | Verification against each Apache Kafka 3.x release in the supported window, on Apache Kafka, Amazon MSK Connect, Strimzi and Confluent Platform. |
| Production support | Support during business hours with the priority definitions, response targets, monthly reporting and escalation below. |
| Onboarding review | Once, at the start of the subscription: a review of each connector configuration, the authentication flow and secret handling, Salesforce API quota headroom, your offsets, replay, snapshot handoff and dead letter queue runbook, and an upgrade to the current release. |
| Upgrade guidance | Release notes that flag anything requiring action, and help planning and executing upgrades. |
| Roadmap | Your issues are prioritised over unsubscribed issues, and your feature requests are recorded and reviewed at each quarterly service review. |
| Confluent migration guidance | Questions about the [documented migration path](/migration/confluent), the configuration translator and the cutover verification are within support. Hands-on participation in a cutover is available as engineering days. |

## What is covered

Support covers the four connectors and their integration with your environment:

- Installation of the plugins on Kafka Connect workers, including Amazon MSK Connect custom plugins and Strimzi `KafkaConnect` image builds, and upgrades between releases
- Connector configuration for the [source](/connectors/source), the [SObject](/connectors/sobject-sink) and [Platform Event](/connectors/platform-event-sink) sinks and the [legacy streaming source](/connectors/streaming-source), including topic mapping, SObject selection, snapshot, polling and event-driven modes
- [Authentication](/reference/authentication): the client credentials and JWT bearer flows, Connected App requirements as they relate to the connectors, and secret handling on the Connect worker
- [Offsets and recovery](/concepts/offsets-and-recovery): replayId checkpointing, the 72-hour replay window, snapshot to stream handoff, gap and overflow recovery, and offset resets
- [Error handling](/reference/error-handling): dead letter queue configuration, retry classification and Salesforce API error interpretation
- Throughput and quota: task sizing, batch sizes, the [rate governor](/concepts/rate-limits) and Salesforce API limits
- Diagnosis of connector failures, performance problems and unexpected behaviour, including suspected missing or duplicated records
- Salesforce API changes, deprecations and seasonal release impacts on the connectors
- Converter and [schema registry](/reference/schemas) questions as they relate to the records the connectors produce and consume
- Security advisories and patches for the connectors and their bundled dependencies

## What is not covered

These remain with you or the relevant vendor. Support for the Kafka and Kafka Connect platform itself is available from OSO as a separate service.

- Operation of the Apache Kafka brokers and the Kafka Connect clusters themselves, and of MSK, Strimzi or Confluent Platform as products
- Salesforce org administration: users, permission sets, Connected App creation, enabling Change Data Capture channels and defining Platform Events, beyond confirming what the connectors need
- Schema registry and converter operation beyond the integration with the connectors
- The applications and data flows on either side of the topics
- Confluent's own Salesforce connectors
- Custom development and feature work, available as engineering days

## Hours and response targets

Business hours are 08:00 to 18:00 UK time, Monday to Friday, excluding UK public holidays. Response targets are measured within those hours; a ticket raised outside them is picked up at the start of the next business period. OSO does not operate a staffed 24x7 desk: out-of-hours P1 call-out and on-call cover for agreed days, such as a production cutover or a Salesforce release weekend, are available as priced options.

| Priority | Definition | Response | Resolution target |
|---|---|---|---|
| P1 Critical | A production connector has stopped or is failing with no workaround; a source outage threatens to outlast the 72-hour Pub/Sub replay window; records are suspected lost, duplicated or corrupted in production; or a connector is endangering the org's API quota | Within 60 minutes | Workaround or recovery path within 4 hours; patch release as soon as practicable |
| P2 High | A production pipeline is degraded or partially failing (one SObject of several, a filling dead letter queue, throughput below need); a workaround exists but is not sustainable; a rehearsal or cutover is blocked ahead of a scheduled change | Within 4 hours | Within 1 business day |
| P3 Medium | A non-critical function is impaired, a production issue has an acceptable workaround, or a non-production environment is blocked | Within 1 business day | Within 3 business days |
| P4 Low | Questions, guidance, documentation and feature requests | Within 2 business days | Within 5 business days or the next scheduled release |

Every P1 receives a documented root cause analysis within five business days of resolution. Escalation runs from the responding engineer to a lead engineer and then to the CTO. Subscribers receive a monthly report against these targets and a quarterly service review.

## Supported versions

| Component | Supported |
|---|---|
| kafka-connect-salesforce | The current release and the previous minor release, with security patches for both. |
| Apache Kafka Connect | Kafka 3.x, built and tested against 3.9; Java 17 and 21 on the worker. |
| Connect runtimes | Apache Kafka, Amazon MSK Connect, Strimzi and Confluent Platform. |
| Salesforce | Pub/Sub API, Bulk API 2.0, REST and, for the legacy source, the Streaming API; the API version is configurable per connector. |

## Security and maintenance commitments

Vulnerability reports are acknowledged within two business days and assessed within five; critical and high severity issues in a supported version are fixed, or a mitigation published, within ten business days, and subscribers are notified directly. Releases are cut by an automated process from `main`, tested on Java 17 and 21 against the fake Salesforce shipped in the repository, signed and published to Maven Central with the plugin ZIPs attached to the GitHub release. At least two OSO engineers hold release rights. The connectors never contact OSO, and OSO has no access to your clusters, orgs or data.

The full policy is published in the repository as [SUPPORT.md](https://github.com/osodevops/kafka-connect-salesforce-oss/blob/main/SUPPORT.md) and [SECURITY.md](https://github.com/osodevops/kafka-connect-salesforce-oss/blob/main/SECURITY.md).

## How to get it

The subscription is a flat annual fee covering all four connectors on every Kafka Connect cluster you run, production and non-production, against every Salesforce org, with no per-connector or per-task count. Email [sales@oso.sh](mailto:sales@oso.sh) or [book a 30-minute call](https://meetings-eu1.hubspot.com/sion-smith) and tell us which Kafka distribution you run Connect on, how many orgs and environments are involved, which connectors you use or plan to use, and whether you are migrating from Confluent's connectors. You will get a written proposal with the service levels above and a price for your estate.
