# Confluent → OSS migration tooling

Two dependency-free Python 3.9+ scripts supporting the
[Migrating from Confluent guide](../../website/docs/migration/confluent.md).

## `confluent2oss.py` — config translator

Converts any Confluent Salesforce connector config (CDC/PushTopic/Platform Event/Bulk
sources, SObject/Bulk/Platform Event sinks) into the equivalent `sh.oso` connector
config, with a per-property migration report.

```bash
curl -s http://connect:8083/connectors/sf-cdc-prod | ./confluent2oss.py - -o new.json
```

- maps property names *and* normalizes enum values (`CLIENT_CREDENTIALS` → `client_credentials`)
- drops obsolete Confluent licensing properties
- flags anything needing a human decision (`reporter.*` topics → Connect DLQ, CSFLE)
- exit code `2` when manual follow-ups exist — safe to gate in scripts

## `verify_cutover.py` — post-cutover evidence

Cross-checks the records Salesforce reports as changed since your cutover against the
keys that actually landed on the Kafka topic, then writes a machine-readable evidence
report (counts, missing IDs, SHA-256 of the delivered key set).

```bash
./verify_cutover.py --sobject Account \
  --instance-url https://acme.my.salesforce.com --access-token "$TOKEN" \
  --since 2026-07-08T09:00:00Z --topic-dump dump.tsv --out evidence.json
```

Exit `0` = PASS (no missing records), `1` = FAIL with the missing IDs listed in the
report. Accepts a live REST query (shown above) or an offline `sf data query --json`
export via `--sf-export`.

## CI-proven procedure

The cutover procedure itself is executed on every build by
`ConfluentCutoverMigrationTest` (connect-salesforce-source), which asserts zero event
loss, bounded duplicates, and replayId continuity across a simulated cutover, and
uploads `migration-evidence.json` as a CI artifact.
