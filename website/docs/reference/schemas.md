---
title: Schemas & Data Types
description: How Salesforce types map to Kafka Connect schemas.
sidebar_position: 3
---

# Schemas & Data Types

## Event-driven records (Pub/Sub)

CDC events arrive as binary Avro. The connector fetches the Avro schema by ID via
`GetSchema` (cached; refetched when the schema ID changes, so schema evolution is
handled) and maps it to a Connect Struct schema:

| Avro | Connect |
|---|---|
| `record` | `STRUCT` |
| `union [null, T]` | optional `T` |
| `string` / `enum` | `STRING` |
| `long` / `int` | `INT64` / `INT32` |
| `double` / `float` | `FLOAT64` / `FLOAT32` |
| `boolean` | `BOOLEAN` |
| `bytes` / `fixed` | `BYTES` |
| `array` / `map` | `ARRAY` / `MAP` |

CDC date/datetime fields are epoch-milliseconds `INT64` on the wire already. All value
fields are optional because CDC UPDATE events carry only changed fields.

## Bulk / polling records (describe-derived)

For snapshot and polling modes the schema is built from
`GET /sobjects/<X>/describe/` (cached with `If-Modified-Since`):

| Salesforce type | Connect |
|---|---|
| `id`, `string`, `picklist`, `reference`, `textarea`, `phone`, `url`, `email`, … | optional `STRING` |
| `boolean` | optional `BOOLEAN` |
| `int` | optional `INT32` |
| `double`, `currency`, `percent` | optional `FLOAT64` |
| `date`, `datetime`, `time` | optional `INT64` (epoch millis) — see below |
| `base64` | optional `BYTES` (excluded from Bulk queries) |
| `address`, `location` (compound) | **skipped** — component fields (`BillingCity`, …) carry the data |

### Date/datetime policy

Temporal fields default to **epoch-milliseconds `INT64`** (dates at midnight UTC,
time-only fields as millis since midnight). To keep specific fields as ISO-8601 strings,
list them in `sf.skip.epoch.conversion.fields` — note the schema-evolution implication:
changing this list changes the field's type.

The sink converts epoch `INT64` values back to Salesforce ISO-8601 datetime/date strings
using the target field's describe type.

## Converters

Records are plain Connect Structs — use whichever converter fits your platform:

```properties
# JSON without embedded schemas
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false

# Avro with a schema registry (Confluent or Apicurio)
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=http://schema-registry:8081
```

For a fully open-source stack, Apicurio Registry's Avro converter works out of the box —
converters are user-supplied and none is bundled with the plugins.
