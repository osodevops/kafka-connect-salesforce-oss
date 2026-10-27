---
title: Platform Events Walkthrough
description: Stream Salesforce Platform Events into Kafka end to end, with the records a real org produces and how replay, schema changes, restarts and recovery work.
sidebar_position: 4
keywords:
  - salesforce platform events kafka
  - platform event source connector
  - pub/sub api replay id
---

# Platform Events walkthrough

This walkthrough streams a custom Platform Event from a Salesforce org into Kafka with the
[source connector](../connectors/source.md#platform-events), then explains what happens
underneath. Every record and log line on this page was captured from a real Developer
Edition org running connector version 0.2.2. Org-specific values (My Domain, user IDs,
usernames) are replaced with placeholders; record shapes, replay IDs and schema IDs are
exactly as Salesforce produced them.

You will need a Salesforce org (a free [Developer Edition](https://developer.salesforce.com/signup)
works), a Kafka Connect worker with the source connector plugin installed, and `curl`.

## 1. Create the platform event

In **Setup → Platform Events → New Platform Event**:

| Setting | Value |
|---|---|
| Label / API name | `Integration Message` / `Integration_Message__e` |
| Event type | High Volume |
| Publish behavior | **Publish After Commit** (the event is only sent once the transaction that changed your data commits) |

Then add the fields your consumers need. A platform event is a message, not a pointer to a
record: carry the data in the event.

| Field | Type | Purpose |
|---|---|---|
| `Message_Type__c` | Text (80) | What happened, e.g. `UpsertOpportunity` |
| `Sequence__c` | Number (18, 0) | Publisher-side sequence |
| `Payload__c` | Long Text Area (131,072) | The business record, serialised as JSON |

:::tip Long text payloads
A long text area holds up to 131,072 characters, within Salesforce's 1 MB per event. In the
verification run a 121,396-character JSON payload arrived in Kafka intact.
:::

## 2. Create a connected app for the client credentials flow

The connector authenticates server to server with the OAuth client credentials flow.

1. **Setup → App Manager → New Connected App**: enable OAuth settings, add the scope
   **Manage user data via APIs (api)**, set any callback URL (it is not used by this flow)
   and tick **Enable Client Credentials Flow**. Save, then copy the **Consumer Key** and
   **Consumer Secret** (Salesforce emails you a verification code to reveal them).
2. **Setup → Manage Connected Apps → your app → Edit Policies**: under
   **Client Credentials Flow**, set **Run As** to the integration user. This is a separate
   step from creating the app, and the flow does not work without it.
3. Give the Run As user **Read** access to `Integration_Message__e` (Object Settings in its
   permission set). System administrators already have it.

New apps can take a few minutes to become usable. Check the flow before configuring Kafka:

```bash
curl -s https://acme.my.salesforce.com/services/oauth2/token \
  -d grant_type=client_credentials \
  -d client_id="$SF_CONSUMER_KEY" -d client_secret="$SF_CONSUMER_SECRET"
```

```json
{"access_token":"00D...","instance_url":"https://acme.my.salesforce.com","token_type":"Bearer","scope":"api", ...}
```

If your org no longer allows new connected apps, create an **External Client App** with the
same OAuth settings and client credentials flow instead. See
[Troubleshooting](#troubleshooting) for the errors you may meet.

## 3. Configure Kafka Connect

Keep the consumer key and secret out of connector configs. Put them in a file on the worker:

```properties title="/secrets/sf.properties"
consumer.key=3MVG9...
consumer.secret=...
```

and enable the file config provider in the worker configuration:

```properties
config.providers=file
config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
```

Then create the connector:

```json
{
  "name": "salesforce-platform-events",
  "config": {
    "connector.class": "sh.oso.salesforce.source.SalesforceSourceConnector",
    "tasks.max": "1",
    "sf.auth.grant.type": "client_credentials",
    "sf.instance.url": "https://acme.my.salesforce.com",
    "sf.consumer.key": "${file:/secrets/sf.properties:consumer.key}",
    "sf.consumer.secret": "${file:/secrets/sf.properties:consumer.secret}",
    "sf.platform.events": "Integration_Message__e",
    "sf.topic.prefix": "salesforce",
    "sf.event.start": "all",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
```

```bash
curl -s -X POST -H 'Content-Type: application/json' \
  --data @salesforce-platform-events.json http://connect:8083/connectors
```

`sf.event.start=all` replays everything Salesforce still retains (72 hours) on first start;
use `latest` to begin with new events only. The worker log shows the subscription:

```text
INFO Pipeline for platform event Integration_Message__e starting (sf.event.start=ALL)
INFO Subscribed to /event/Integration_Message__e (EARLIEST)
```

## 4. Publish an event

From a Flow or Apex in production; for this walkthrough, the REST API with the same token:

```bash
curl -s -X POST https://acme.my.salesforce.com/services/data/v60.0/sobjects/Integration_Message__e/ \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"Message_Type__c":"UpsertOpportunity","Sequence__c":8,
       "Payload__c":"{\"recordId\":\"006xx000001AbCdEF\",\"name\":\"Example Holdings Ltd\",\"stage\":\"Closed Won\",\"amount\":125000,\"currency\":\"GBP\"}"}'
```

```json
{"id":"e00xx0000000001AAA","success":true,
 "errors":[{"statusCode":"OPERATION_ENQUEUED","message":"34efb95c-4b84-4945-ab20-847160bd596c","fields":[]}]}
```

`OPERATION_ENQUEUED` is the normal response: the event was accepted, and the message is the
event's `EventUuid`.

## 5. Read it from Kafka

```bash
kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic salesforce.Integration_Message__e --from-beginning --property print.key=true
```

The record, as captured (key first):

```json
null	{
  "CreatedDate": 1793100007587,
  "CreatedById": "005xx000001X8UzAAK",
  "Message_Type__c": "UpsertOpportunity",
  "Sequence__c": 8.0,
  "Payload__c": "{\"recordId\": \"006xx000001AbCdEF\", \"name\": \"Example Holdings Ltd\", \"stage\": \"Closed Won\", \"amount\": 125000, \"currency\": \"GBP\"}",
  "Initiator_Id__c": "005xx000002Y9Vz",
  "ReplayId": "1589437",
  "EventUuid": "34efb95c-4b84-4945-ab20-847160bd596c",
  "_ObjectType": "Integration_Message__e",
  "_EventType": "gwAOxdYOpO76X__EiKM2hg"
}
```

Every field of the event arrives as published; the connector makes no call back to
Salesforce. `EventUuid` matches the publish response above.

## How it works

### One subscription per event

Each event in `sf.platform.events` gets its own Pub/Sub API `Subscribe` stream, its own
offset and its own topic, `{sf.topic.prefix}.{EventName}`. Platform events can share a
connector with CDC objects (`sf.sobjects`); a failure on one stream does not stop the others.

### The record

| Part | Content |
|---|---|
| Key | `null`: platform events are messages, not records. Use the `ValueToKey` SMT if you need a key (see [Ordering and keys](../connectors/source.md#ordering-and-keys)). |
| Event fields | Exactly as published. `CreatedDate` is epoch milliseconds; `CreatedById` is the publishing user. |
| `ReplayId` | The event's position on the event bus, as a decimal string. |
| `EventUuid` | Salesforce's unique ID for the event: deduplicate on it. |
| `_ObjectType` | The event API name. |
| `_EventType` | The Pub/Sub schema ID of the event definition the event was published with. |

This is the record shape Confluent's Platform Event source produces, so consumers migrating
from it keep working.

### Replay IDs and offsets

Salesforce identifies each event's position with an opaque replay ID. On a real org it is 29
bytes: an 8-byte position followed by bytes identifying the event bus partition. The
connector stores the full bytes as its offset and hands them back to Salesforce on restart,
so it resumes exactly after the last event it delivered:

```bash
curl -s http://connect:8083/connectors/salesforce-platform-events/offsets
```

```json
{"offsets":[{"partition":{"platformEvent":"Integration_Message__e"},
             "offset":{"replayId":"AAAAAAAYQL0AE04tY29yZTEuc2ZkYy04dGd0dDU=","offsetVersion":"1"}}]}
```

The `ReplayId` field in the record is the 8-byte position as a number (`1589437` above). It is
the same number the legacy Streaming API reports for the event, and is for display and
correlation; resuming always uses the stored bytes.

In the verification run, restarting the Connect worker container mid-stream resumed with every
event delivered once: 7 of 7 events, 7 distinct `EventUuid`s.

### Schema changes

When an admin changes the event definition, Salesforce issues a new schema ID. The connector
decodes every event with the schema it was published with, so old and new events can share a
stream, and the next record shows the change. In the verification run, adding
`Initiator_Id__c` to the event while the connector was running produced:

| | `_EventType` | `Initiator_Id__c` |
|---|---|---|
| Last event before the change | `P1Ss4vbHmCZ6kV-7an5DAg` | absent |
| First event after the change | `gwAOxdYOpO76X__EiKM2hg` | `005TESTINITIATOR` |

The Connect schema keeps the event's name, so Schema Registry treats the change as a new
version of the same subject.

### Delivery guarantees

Delivery is at least once. After a crash, events since the last committed offset are
delivered again; deduplicate on `EventUuid` where it matters. Salesforce retains platform
events for 72 hours: a connector stopped for longer than that loses its position, and
`sf.gap.recovery` decides what happens (`resync` resumes from the earliest retained event,
`latest` from the newest, `fail` stops the task and keeps the offset).

### Connections and recovery

The stream reconnects by itself: a dropped connection or a stream closed by Salesforce
resumes from the stored position with backoff, and an expired session is renewed with the
client credentials flow. An event that does not exist, or that the Run As user cannot read,
fails the task with a message naming the event. See
[Offsets & Recovery](../concepts/offsets-and-recovery.md).

### Platform events and CDC together

The same connector can stream CDC for objects listed in `sf.sobjects`, with an optional Bulk
API 2.0 snapshot first. Before the snapshot, the connector records the CDC stream's current
position so the live stream resumes from it with no gap. Salesforce reports that position
when an event arrives, or with its keepalive after about 270 seconds on a quiet channel, so a
snapshot can take a few minutes to start; other streams keep flowing meanwhile. In the
verification run Salesforce answered after about two minutes:

```text
11:17:48 INFO Probing the current position of /data/AccountChangeEvent; Salesforce answers when an event arrives or with its keepalive, within about 270 s
11:19:52 INFO Snapshot for Account started (job 750xx000000AbCdEF)
11:19:56 INFO Snapshot for Account complete; transitioning to EVENT_DRIVEN
```

A live Account update then arrived with the changed fields in the record headers:

```text
sf.change.type:UPDATE, sf.entity:Account, sf.changed.fields:Name,Industry,LastModifiedDate
key 001xx000003DGb2AAG  {"Id":"001xx000003DGb2AAG","Name":"OSO Snapshot One Ltd (renamed)","Industry":"Consulting","_EventType":"updated","_ObjectType":"Account", ...}
```

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Token request returns `invalid_grant: no client credentials user enabled` | The connected app has no Run As user for the client credentials flow. Set it under **Manage Connected Apps → Edit Policies → Client Credentials Flow**. |
| Token request returns `invalid_client_id` right after creating the app | The app has not propagated yet; wait a few minutes. |
| Editing the app's policies fails with "You must enter a value" and no visible empty field | Salesforce sometimes requires the hidden Single Logout URL on apps created through the API. Enter any URL; single logout stays disabled. |
| Task fails with `NOT_FOUND ... Check that X__e exists` | The API name is wrong (it is case sensitive and ends in `__e`) or the event does not exist in this org. |
| Task fails with `PERMISSION_DENIED` | The Run As user cannot read the event. Grant Read in its permission set. |
| A CDC snapshot takes a few minutes to start | Expected on a quiet org: the connector waits for Salesforce to report the stream position (up to about 270 seconds). |
| Events carry only a record ID | The event was designed that way. See [Do Platform Events need hydrating?](../connectors/source.md#do-platform-events-need-hydrating). |
