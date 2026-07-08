#!/usr/bin/env python3
"""Translate a Confluent Salesforce connector config into the OSS equivalent.

Reads a Kafka Connect connector JSON (either the {"name","config"} envelope used by
the Connect REST API or a bare config map), detects which Confluent Salesforce
connector it is, and emits the equivalent configuration for the open-source
sh.oso connectors together with a migration report listing every property that
was translated, dropped, or needs manual attention.

Usage:
    ./confluent2oss.py old-connector.json                 # print translated JSON
    ./confluent2oss.py old-connector.json -o new.json     # write file + report to stderr
    curl -s http://connect:8083/connectors/NAME | ./confluent2oss.py -

No dependencies beyond Python 3.9+.
"""

import argparse
import json
import sys

SOURCE_CLASS = "sh.oso.salesforce.source.SalesforceSourceConnector"
SINK_CLASS = "sh.oso.salesforce.sink.SalesforceSinkConnector"
PE_SINK_CLASS = "sh.oso.salesforce.pesink.SalesforcePlatformEventSinkConnector"
STREAMING_CLASS = "sh.oso.salesforce.streaming.SalesforceStreamingSourceConnector"

# Confluent connector class -> (target class, kind)
CLASS_MAP = {
    "io.confluent.salesforce.SalesforceCdcSourceConnector": (SOURCE_CLASS, "source"),
    "io.confluent.salesforce.SalesforcePlatformEventSourceConnector": (SOURCE_CLASS, "source"),
    "io.confluent.connect.salesforce.SalesforceBulkApiSourceConnector": (SOURCE_CLASS, "source-polling"),
    "io.confluent.connect.salesforce.SalesforceBulkApiV2SourceConnector": (SOURCE_CLASS, "source-polling"),
    "SalesforceSourceV2": (SOURCE_CLASS, "source"),  # Confluent Cloud plugin name
    "io.confluent.salesforce.SalesforcePushTopicSourceConnector": (STREAMING_CLASS, "pushtopic"),
    "io.confluent.connect.salesforce.SalesforceSObjectSinkConnector": (SINK_CLASS, "sink"),
    "io.confluent.connect.salesforce.SalesforceBulkApiSinkConnector": (SINK_CLASS, "sink"),
    "io.confluent.connect.salesforce.SalesforceBulkApiV2SinkConnector": (SINK_CLASS, "sink"),
    "io.confluent.salesforce.SalesforcePlatformEventSinkConnector": (PE_SINK_CLASS, "pe-sink"),
}

# Shared auth/connection property translations (Confluent key -> OSS key).
AUTH_MAP = {
    "salesforce.grant.type": "sf.auth.grant.type",
    "salesforce.instance": "sf.instance.url",
    "salesforce.consumer.key": "sf.consumer.key",
    "salesforce.consumer.secret": "sf.consumer.secret",
    "salesforce.username": "sf.username",
    "salesforce.password": "sf.password",
    "salesforce.password.token": "sf.password.token",
    "salesforce.jwt.keystore.path": "sf.jwt.keystore.path",
    "salesforce.jwt.keystore.password": "sf.jwt.keystore.password",
}

SOURCE_MAP = {
    "sobject.names": "sf.sobjects",
    "salesforce.object": "sf.sobjects",
    "topic.prefix": "sf.topic.prefix",
    "historical.snapshot": "sf.snapshot.enabled",
    "real.time.ingestion.mode": "sf.realtime.mode",
    "event.sync.start.point": "sf.event.start",
    "gap.event.recovery": "sf.gap.recovery",
    "cdc.full.record.on.update": "sf.full.record.on.update",
    "emit.tombstone.on.delete": "sf.emit.tombstone.on.delete",
    "poll.interval.ms": "sf.poll.interval.ms",
    "salesforce.since": "sf.snapshot.since",
    "request.max.retries.time.ms": "sf.request.max.retries.time.ms",
}

SINK_MAP = {
    "behavior.on.api.errors": "behavior.on.api.errors",
    "salesforce.max.batch.size": "sf.max.batch.records",
    "request.max.retries.time.ms": "sf.request.max.retries.time.ms",
}

PE_SINK_MAP = {
    "salesforce.platform.event.name": "sf.platform.event.name",
    "behavior.on.api.errors": "behavior.on.api.errors",
    "request.max.retries.time.ms": "sf.request.max.retries.time.ms",
}

# Properties that have no OSS equivalent and are silently obsolete.
DROPPED = {
    "confluent.topic.bootstrap.servers": "Confluent licensing topic - not needed",
    "confluent.topic.replication.factor": "Confluent licensing topic - not needed",
    "confluent.license": "no license required (Apache-2.0)",
    "salesforce.version": "use sf.api.version (numeric, e.g. 60.0)",
    "connection.max.message.size": "CometD tuning - not applicable to Pub/Sub transport",
    "connection.timeout": "see sf.connection.timeout.ms on the streaming connector",
}

# Properties that require a human decision.
MANUAL = {
    "reporter.result.topic.name": "Confluent reporter topics are replaced by the Connect DLQ "
                                  "(errors.deadletterqueue.topic.name + errors.tolerance=all)",
    "reporter.error.topic.name": "use errors.deadletterqueue.topic.name instead",
    "csfle.enabled": "CSFLE is not yet supported by the OSS suite",
    "kafka.topic.lowercase": "OSS topics are {sf.topic.prefix}.{SObject} - adjust consumers or add a RegexRouter SMT",
}

PASSTHROUGH_PREFIXES = (
    "transforms", "predicates", "key.converter", "value.converter", "header.converter",
    "errors.", "topics", "tasks.max", "name",
)

# Confluent enum values -> OSS enum values, per OSS key.
VALUE_NORMALIZERS = {
    "sf.auth.grant.type": {
        "CLIENT_CREDENTIALS": "client_credentials",
        "JWT_BEARER": "jwt_bearer",
        "PASSWORD": "password",
    },
    "sf.event.start": {
        "LATEST": "latest",
        "ALL_RETAINED": "all",
        "ALL": "all",
    },
    "sf.realtime.mode": {
        "EVENT_DRIVEN": "event_driven",
        "PERIODIC_POLLING": "polling",
        "POLLING": "polling",
    },
    "sf.gap.recovery": {
        "RESYNC": "resync",
        "LATEST": "latest",
        "FAIL": "fail",
    },
}


def normalize(key, value, notes):
    mapping = VALUE_NORMALIZERS.get(key)
    if mapping and isinstance(value, str) and value in mapping:
        notes.append(f"value    {key}: {value} -> {mapping[value]}")
        return mapping[value]
    return value


def translate(config: dict) -> tuple[dict, list[str]]:
    notes = []
    src_class = config.get("connector.class", "")
    target = CLASS_MAP.get(src_class)
    if target is None:
        matches = [v for k, v in CLASS_MAP.items() if k.split(".")[-1] == src_class.split(".")[-1]]
        if matches:
            target = matches[0]
        else:
            raise SystemExit(f"error: unrecognised Confluent Salesforce connector class: {src_class!r}")
    target_class, kind = target
    out = {"connector.class": target_class}
    notes.append(f"connector.class: {src_class} -> {target_class}")

    prop_map = dict(AUTH_MAP)
    if kind in ("source", "source-polling"):
        prop_map.update(SOURCE_MAP)
    elif kind == "sink":
        prop_map.update(SINK_MAP)
    elif kind == "pe-sink":
        prop_map.update(PE_SINK_MAP)

    sink_object = config.get("salesforce.object")

    for key, value in config.items():
        if key == "connector.class":
            continue
        if key in prop_map and not (kind == "sink" and key == "salesforce.object"):
            out[prop_map[key]] = normalize(prop_map[key], value, notes)
            notes.append(f"mapped   {key} -> {prop_map[key]}")
        elif key in DROPPED:
            notes.append(f"dropped  {key} ({DROPPED[key]})")
        elif key in MANUAL:
            notes.append(f"MANUAL   {key}: {MANUAL[key]}")
        elif kind == "sink" and translate_sink_property(key, value, sink_object, out, notes):
            pass
        elif key.startswith(PASSTHROUGH_PREFIXES):
            out[key] = value
            notes.append(f"kept     {key} (framework property)")
        else:
            notes.append(f"MANUAL   {key}: not recognised - check the OSS config reference")

    apply_defaults(kind, config, out, notes)
    return out, notes


def translate_sink_property(key, value, sink_object, out, notes) -> bool:
    if sink_object is None:
        return False
    per_object = {
        "salesforce.object": None,
        "salesforce.sink.object.operation": f"sf.{sink_object}.operation",
        "override.event.type": f"sf.{sink_object}.override.event.type",
        "salesforce.use.custom.id.field": f"sf.{sink_object}.use.custom.id.field",
        "salesforce.custom.id.field.name": f"sf.{sink_object}.custom.id.field.name",
        "salesforce.ignore.fields": f"sf.{sink_object}.ignore.fields",
        "salesforce.ignore.reference.fields": f"sf.{sink_object}.ignore.reference.fields",
    }
    if key == "salesforce.object":
        out["sf.objects"] = value
        notes.append(f"mapped   salesforce.object -> sf.objects (+ per-object sf.{value}.* keys)")
        return True
    if key in per_object and per_object[key]:
        out[per_object[key]] = value
        notes.append(f"mapped   {key} -> {per_object[key]}")
        return True
    return False


def apply_defaults(kind, config, out, notes):
    if kind == "source-polling":
        out.setdefault("sf.realtime.mode", "polling")
        notes.append("set      sf.realtime.mode=polling (Bulk API source connectors poll)")
    if kind in ("source", "source-polling") and "sf.topic.prefix" not in out:
        topic = config.get("kafka.topic")
        if topic:
            out["sf.topic.prefix"] = topic.rsplit(".", 1)[0] if "." in topic else topic
            notes.append(f"derived  sf.topic.prefix={out['sf.topic.prefix']} from kafka.topic={topic} - REVIEW")
        else:
            notes.append("MANUAL   sf.topic.prefix is required (topics become {prefix}.{SObject})")
    if kind == "sink":
        obj = config.get("salesforce.object")
        if obj and config.get("topics") and f"sf.{obj}.topics" not in out:
            out[f"sf.{obj}.topics"] = config["topics"]
            notes.append(f"set      sf.{obj}.topics={config['topics']}")
    if kind == "pushtopic":
        out.setdefault("sf.channel.type", "pushtopic")
        for ckey, okey in [("salesforce.push.topic.name", "sf.pushtopic.name"),
                           ("salesforce.object", "sf.object"),
                           ("kafka.topic", "kafka.topic"),
                           ("salesforce.push.topic.create", "sf.pushtopic.create"),
                           ("salesforce.initial.start", "sf.event.start")]:
            if ckey in config:
                out[okey] = config[ckey]
                notes.append(f"mapped   {ckey} -> {okey}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("input", help="Confluent connector JSON file, or - for stdin")
    parser.add_argument("-o", "--output", help="write translated config JSON here (default: stdout)")
    parser.add_argument("--name-suffix", default="-oss", help="suffix appended to the connector name")
    args = parser.parse_args()

    raw = sys.stdin.read() if args.input == "-" else open(args.input).read()
    doc = json.loads(raw)
    name = doc.get("name", "salesforce-connector")
    config = doc.get("config", doc)

    translated, notes = translate(dict(config))
    envelope = {"name": name + args.name_suffix, "config": translated}
    rendered = json.dumps(envelope, indent=2)

    manual = [n for n in notes if n.startswith("MANUAL")]
    report = ["", "=" * 70, "migration report", "=" * 70, *notes, "",
              f"{len(manual)} propert{'y needs' if len(manual) == 1 else 'ies need'} manual attention."
              if manual else "no manual follow-ups detected.", ""]

    if args.output:
        with open(args.output, "w") as f:
            f.write(rendered + "\n")
        print("\n".join(report), file=sys.stderr)
        print(f"wrote {args.output}", file=sys.stderr)
    else:
        print(rendered)
        print("\n".join(report), file=sys.stderr)
    sys.exit(2 if manual else 0)


if __name__ == "__main__":
    main()
