---
title: Quickstart
description: Run Kafka, Kafka Connect, and the Salesforce source connector locally with docker compose.
sidebar_position: 3
---

# Quickstart

The repository ships a docker-compose stack under
[`examples/`](https://github.com/osodevops/kafka-connect-salesforce-oss/tree/main/examples)
with a single-node Kafka (KRaft) broker and a Connect worker.

## 1. Build and unpack the plugins

```bash
git clone https://github.com/osodevops/kafka-connect-salesforce-oss.git
cd kafka-connect-salesforce-oss
mvn clean package -DskipTests

cd examples
mkdir -p plugins
for z in ../*/target/*-kafka-connect-plugin.zip; do unzip -o "$z" -d plugins/; done
```

## 2. Start the stack

```bash
docker compose up -d
curl -s localhost:8083/connector-plugins | jq '.[].class' | grep salesforce
```

## 3. Register the source connector

Edit `source-connector.json` with your org's My Domain URL and Connected App credentials,
then:

```bash
curl -s -X POST -H 'Content-Type: application/json' \
  --data @source-connector.json localhost:8083/connectors | jq
curl -s localhost:8083/connectors/salesforce-source/status | jq
```

The default config snapshots each SObject via Bulk API 2.0 and then hands off to live
CDC streaming:

```json
{
  "name": "salesforce-source",
  "config": {
    "connector.class": "sh.oso.salesforce.source.SalesforceSourceConnector",
    "tasks.max": "2",
    "sf.auth.grant.type": "client_credentials",
    "sf.instance.url": "https://YOUR_DOMAIN.my.salesforce.com",
    "sf.consumer.key": "...",
    "sf.consumer.secret": "...",
    "sf.sobjects": "Account,Contact,Opportunity",
    "sf.topic.prefix": "salesforce",
    "sf.snapshot.enabled": "true",
    "sf.realtime.mode": "event_driven"
  }
}
```

## 4. Watch records arrive

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 \
  --topic salesforce.Account --from-beginning \
  --property print.key=true
```

Create or edit an Account in Salesforce and the change event appears within seconds:

```json
"001XX000003GZkQYAW"	{"Id":"001XX000003GZkQYAW","Name":"Acme Ltd","_EventType":"updated","_ObjectType":"Account", ...}
```

## Next steps

- Tune the [source connector configuration](../connectors/source.md)
- Write records back with the [SObject sink](../connectors/sobject-sink.md)
- Understand [delivery semantics](../concepts/delivery-semantics.md)
