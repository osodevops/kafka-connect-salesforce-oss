#!/usr/bin/env python3
"""Post-cutover verification: prove the OSS connector missed nothing.

Compares the records Salesforce says changed around your cutover window against
what actually landed on the Kafka topic, and writes a machine-readable evidence
report (SHA-256 digests included) you can attach to a change ticket.

Salesforce side (pick one):
  --sf-export FILE       JSON array of records (each with Id + SystemModstamp),
                         e.g. exported with Salesforce CLI:
                         sf data query -q "SELECT Id, SystemModstamp FROM Account \
                            WHERE SystemModstamp >= 2026-07-08T00:00:00Z" --json
  --instance-url + --access-token + --sobject + --since
                         live REST query (no dependencies, uses urllib)

Kafka side:
  --topic-dump FILE      JSONL dump of the topic from the cutover onwards:
                         kafka-console-consumer.sh --bootstrap-server B --topic T \
                            --from-beginning --property print.key=true \
                            --property key.separator=$'\\t' > dump.tsv

Example:
  ./verify_cutover.py --sobject Account \
      --instance-url https://acme.my.salesforce.com --access-token $TOKEN \
      --since 2026-07-08T09:00:00Z --topic-dump dump.tsv \
      --out migration-evidence-account.json

Exit codes: 0 = PASS (no missing records), 1 = FAIL (gaps found), 2 = usage error.
"""

import argparse
import hashlib
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone


def fetch_salesforce_records(instance_url, token, sobject, since, api_version):
    soql = (f"SELECT Id, SystemModstamp FROM {sobject} "
            f"WHERE SystemModstamp >= {since}")
    url = (f"{instance_url.rstrip('/')}/services/data/v{api_version}/query/"
           f"?q={urllib.parse.quote(soql)}")
    records = []
    while url:
        request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(request) as response:
            payload = json.load(response)
        records.extend(payload.get("records", []))
        next_url = payload.get("nextRecordsUrl")
        url = f"{instance_url.rstrip('/')}{next_url}" if next_url and not payload.get("done") else None
    return records


def load_topic_keys(path):
    """Reads a console-consumer dump; accepts 'key<TAB>json' lines or bare JSON values."""
    keys = []
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            key = None
            if "\t" in line:
                key = line.split("\t", 1)[0]
                if key in ("null", ""):
                    key = None
            if key is None:
                try:
                    value = json.loads(line.split("\t", 1)[-1])
                    key = value.get("Id") if isinstance(value, dict) else None
                except json.JSONDecodeError:
                    continue
            if key:
                keys.append(key)
    return keys


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sobject", required=True)
    parser.add_argument("--topic-dump", required=True)
    parser.add_argument("--sf-export")
    parser.add_argument("--instance-url")
    parser.add_argument("--access-token")
    parser.add_argument("--since", help="ISO-8601 lower bound (start of cutover window minus a buffer)")
    parser.add_argument("--api-version", default="60.0")
    parser.add_argument("--out", default="migration-evidence.json")
    args = parser.parse_args()

    if args.sf_export:
        with open(args.sf_export) as f:
            doc = json.load(f)
        if isinstance(doc, list):
            sf_records = doc
        else:
            sf_records = doc.get("result", {}).get("records") or doc.get("records") or []
    elif args.instance_url and args.access_token and args.since:
        sf_records = fetch_salesforce_records(
            args.instance_url, args.access_token, args.sobject, args.since, args.api_version)
    else:
        parser.error("provide --sf-export, or --instance-url + --access-token + --since")

    expected = {r["Id"]: r.get("SystemModstamp") for r in sf_records if r.get("Id")}
    delivered = load_topic_keys(args.topic_dump)
    delivered_set = set(delivered)

    missing = sorted(record_id for record_id in expected if record_id not in delivered_set)
    duplicates = len(delivered) - len(delivered_set)
    digest = hashlib.sha256("\n".join(sorted(delivered_set)).encode()).hexdigest()

    evidence = {
        "report": "confluent-cutover-migration-evidence",
        "schemaVersion": 1,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "sobject": args.sobject,
        "window": {"since": args.since},
        "salesforceRecordsChanged": len(expected),
        "kafkaRecordsDelivered": len(delivered),
        "uniqueKafkaKeys": len(delivered_set),
        "duplicateDeliveries": duplicates,
        "missingRecordIds": missing,
        "deliveredKeySetSha256": digest,
        "verdict": "PASS: every changed Salesforce record reached Kafka"
                   if not missing else
                   f"FAIL: {len(missing)} changed record(s) never reached Kafka",
        "note": "duplicates are expected with at-least-once cutover; "
                "consumers dedupe on (Id, commit timestamp/SystemModstamp)",
    }
    with open(args.out, "w") as f:
        json.dump(evidence, f, indent=2)
    print(f"{evidence['verdict']}  ->  {args.out}")
    sys.exit(1 if missing else 0)


if __name__ == "__main__":
    main()
