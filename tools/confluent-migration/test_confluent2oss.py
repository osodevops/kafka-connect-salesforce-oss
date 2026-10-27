"""Tests for confluent2oss.py: python3 -m unittest discover -s tools/confluent-migration -p 'test_*.py' -v"""
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCRIPT = HERE / "confluent2oss.py"
_spec = importlib.util.spec_from_file_location("confluent2oss", SCRIPT)
c2o = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(c2o)

# Shaped like a Confluent Cloud Platform Event source config; all values are synthetic.
CLOUD_PE_SOURCE = {
    "connector.class": "SalesforcePlatformEventSource",
    "kafka.auth.mode": "SERVICE_ACCOUNT",
    "kafka.service.account.id": "sa-abc123",
    "schema.context.name": "default",
    "salesforce.grant.type": "JWT_BEARER",
    "salesforce.instance": "https://example.my.salesforce.com",
    "salesforce.consumer.key": "CONSUMER-KEY",
    "salesforce.username": "integration@example.com",
    "salesforce.jwt.keystore.path": "/secrets/sf-key.pem",
    "salesforce.platform.event.name": "Order_Shipped__e",
    "kafka.topic": "orders-shipped",
    "salesforce.initial.start": "latest",
    "connection.timeout": "30000",
    "request.max.retries.time.ms": "30000",
    "output.data.format": "JSON",
    "errors.tolerance": "all",
    "tasks.max": "1",
}


def manual(notes):
    return [n for n in notes if n.startswith("MANUAL")]


def run_cli(config, *args):
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(config, f)
    result = subprocess.run([sys.executable, "-I", str(SCRIPT), f.name, *args], capture_output=True, text=True)
    Path(f.name).unlink()
    return result


class PlatformEventSourceTest(unittest.TestCase):

    def test_pe_source_on_prem_maps_event_and_start(self):
        out, notes = c2o.translate({
            "connector.class": "io.confluent.salesforce.SalesforcePlatformEventSourceConnector",
            "salesforce.platform.event.name": "Order_Shipped__e",
            "salesforce.initial.start": "all",
            "salesforce.instance": "https://example.my.salesforce.com",
            "salesforce.consumer.key": "k",
            "salesforce.consumer.secret": "s",
            "kafka.topic": "salesforce.Order_Shipped__e",
        })
        self.assertEqual(out["connector.class"], c2o.SOURCE_CLASS)
        self.assertEqual(out["sf.platform.events"], "Order_Shipped__e")
        self.assertEqual(out["sf.event.start"], "all")
        self.assertNotIn("sf.sobjects", out)
        self.assertEqual(manual(notes), [])

    def test_cloud_pe_source_translates_without_manual_steps(self):
        out, notes = c2o.translate(dict(CLOUD_PE_SOURCE))
        self.assertEqual(manual(notes), [])
        self.assertEqual(out["sf.platform.events"], "Order_Shipped__e")
        self.assertEqual(out["sf.auth.grant.type"], "jwt_bearer")
        self.assertEqual(out["sf.jwt.keystore.path"], "/secrets/sf-key.pem")
        self.assertEqual(out["value.converter"], "org.apache.kafka.connect.json.JsonConverter")
        self.assertEqual(out["value.converter.schemas.enable"], "false")

    def test_topic_matching_prefix_needs_no_router(self):
        config = dict(CLOUD_PE_SOURCE, **{"kafka.topic": "sf.prod.Order_Shipped__e"})
        out, _ = c2o.translate(config)
        self.assertEqual(out["sf.topic.prefix"], "sf.prod")
        self.assertNotIn("transforms", out)

    def test_exact_topic_adds_router_first(self):
        config = dict(CLOUD_PE_SOURCE, **{
            "transforms": "mask",
            "transforms.mask.type": "org.apache.kafka.connect.transforms.MaskField$Value",
            "transforms.mask.fields": "Carrier__c",
        })
        out, notes = c2o.translate(config)
        self.assertEqual(out["transforms"], "sfPlatformEventTopic,mask")
        self.assertEqual(out["transforms.sfPlatformEventTopic.type"],
                         "org.apache.kafka.connect.transforms.RegexRouter")
        self.assertEqual(out["transforms.sfPlatformEventTopic.regex"], r"^salesforce\.Order_Shipped__e$")
        self.assertEqual(out["transforms.sfPlatformEventTopic.replacement"], "orders-shipped")
        self.assertEqual(out["transforms.mask.fields"], "Carrier__c")
        self.assertEqual(manual(notes), [])

    def test_router_alias_avoids_collision(self):
        config = dict(CLOUD_PE_SOURCE, **{"transforms": "sfPlatformEventTopic",
                                          "transforms.sfPlatformEventTopic.type": "x.Y"})
        out, _ = c2o.translate(config)
        self.assertEqual(out["transforms"], "sfPlatformEventTopic2,sfPlatformEventTopic")

    def test_topic_template_is_manual(self):
        config = dict(CLOUD_PE_SOURCE, **{"kafka.topic": "sf.${_ObjectType}"})
        out, notes = c2o.translate(config)
        self.assertTrue(any("templates are not supported" in n for n in manual(notes)), notes)
        self.assertNotIn("transforms", out)

    def test_output_format_manual_guidance(self):
        config = dict(CLOUD_PE_SOURCE, **{"output.data.format": "AVRO"})
        out, notes = c2o.translate(config)
        [note] = [n for n in manual(notes) if "output.data.format" in n]
        self.assertIn("io.confluent.connect.avro.AvroConverter", note)
        self.assertIn("Schema Registry may reject", note)
        self.assertNotIn("value.converter", out)

    def test_cloud_secrets_dropped_not_echoed(self):
        config = dict(CLOUD_PE_SOURCE, **{"kafka.auth.mode": "KAFKA_API_KEY",
                                          "kafka.api.key": "APIKEY-SHOULD-NOT-APPEAR",
                                          "kafka.api.secret": "APISECRET-SHOULD-NOT-APPEAR"})
        out, notes = c2o.translate(config)
        for key in ("kafka.auth.mode", "kafka.api.key", "kafka.api.secret", "kafka.service.account.id"):
            self.assertNotIn(key, out)
        report = "\n".join(notes)
        self.assertNotIn("SHOULD-NOT-APPEAR", report)
        self.assertNotIn("SHOULD-NOT-APPEAR", json.dumps(out))

    def test_jwt_without_key_path_is_manual(self):
        config = dict(CLOUD_PE_SOURCE)
        del config["salesforce.jwt.keystore.path"]
        _, notes = c2o.translate(config)
        self.assertTrue(any("sf.jwt.keystore.path is required" in n for n in manual(notes)), notes)

    def test_cli_exits_zero_and_keeps_names_consistent(self):
        result = run_cli({"name": "orders-pe", "config": dict(CLOUD_PE_SOURCE, name="orders-pe")})
        self.assertEqual(result.returncode, 0, result.stderr)
        doc = json.loads(result.stdout)
        self.assertEqual(doc["name"], "orders-pe-oss")
        self.assertEqual(doc["config"]["name"], "orders-pe-oss")
        self.assertIn("no manual follow-ups detected", result.stderr)


class ClassNameTest(unittest.TestCase):

    def test_cloud_short_names_resolve(self):
        expected = {
            "SalesforceSourceV2": (c2o.SOURCE_CLASS, "source"),
            "SalesforceCdcSource": (c2o.SOURCE_CLASS, "source"),
            "SalesforcePlatformEventSource": (c2o.SOURCE_CLASS, "pe-source"),
            "SalesforcePlatformEventSink": (c2o.PE_SINK_CLASS, "pe-sink"),
            "SalesforcePushTopicSource": (c2o.STREAMING_CLASS, "pushtopic"),
            "SalesforceBulkApiSource": (c2o.SOURCE_CLASS, "source-polling"),
            "SalesforceBulkApiV2Source": (c2o.SOURCE_CLASS, "source-polling"),
            "SalesforceSObjectSink": (c2o.SINK_CLASS, "sink"),
            "SalesforceBulkApiV2Sink": (c2o.SINK_CLASS, "sink"),
        }
        for name, target in expected.items():
            with self.subTest(name=name):
                self.assertEqual(c2o.CLASS_MAP[name], target)
                out, _ = c2o.translate({"connector.class": name, "kafka.topic": "t.Account"})
                self.assertEqual(out["connector.class"], target[0])

    def test_unknown_class_errors(self):
        with self.assertRaises(SystemExit) as raised:
            c2o.translate({"connector.class": "com.example.NotSalesforce"})
        self.assertIn("unrecognised", str(raised.exception))
        self.assertEqual(run_cli({"connector.class": "com.example.NotSalesforce"}).returncode, 1)


class ExistingKindsTest(unittest.TestCase):

    def test_pushtopic_exits_zero_when_fully_mapped(self):
        result = run_cli({
            "connector.class": "io.confluent.salesforce.SalesforcePushTopicSourceConnector",
            "salesforce.instance": "https://example.my.salesforce.com",
            "salesforce.consumer.key": "k",
            "salesforce.consumer.secret": "s",
            "salesforce.push.topic.name": "AccountUpdates",
            "salesforce.object": "Account",
            "kafka.topic": "sf.accounts",
            "salesforce.initial.start": "all",
            "connection.timeout": "30000",
        })
        self.assertEqual(result.returncode, 0, result.stderr)
        out = json.loads(result.stdout)["config"]
        self.assertEqual(out["sf.channel.type"], "pushtopic")
        self.assertEqual(out["sf.pushtopic.name"], "AccountUpdates")
        self.assertEqual(out["kafka.topic"], "sf.accounts")
        self.assertEqual(out["sf.event.start"], "all")
        self.assertEqual(out["sf.connection.timeout.ms"], "30000")

    def test_reporter_topics_on_source_explain_dlq_is_sink_only(self):
        _, notes = c2o.translate(dict(CLOUD_PE_SOURCE, **{"reporter.error.topic.name": "errors"}))
        [note] = [n for n in manual(notes) if "reporter.error.topic.name" in n]
        self.assertIn("no source-side equivalent", note)
        _, sink_notes = c2o.translate({"connector.class": "SalesforceSObjectSink",
                                       "reporter.error.topic.name": "errors"})
        self.assertTrue(any("errors.deadletterqueue.topic.name" in n for n in manual(sink_notes)))

    def test_cdc_translation_unchanged(self):
        out, notes = c2o.translate({
            "connector.class": "io.confluent.salesforce.SalesforceCdcSourceConnector",
            "salesforce.instance": "https://example.my.salesforce.com",
            "salesforce.consumer.key": "k",
            "salesforce.consumer.secret": "s",
            "sobject.names": "Account,Contact",
            "kafka.topic": "salesforce.Account",
            "event.sync.start.point": "ALL_RETAINED",
        })
        self.assertEqual(out["sf.sobjects"], "Account,Contact")
        self.assertEqual(out["sf.event.start"], "all")
        self.assertEqual(out["sf.topic.prefix"], "salesforce")
        self.assertNotIn("sf.platform.events", out)
        # kafka.topic is consumed by the prefix derivation, not reported as unrecognised
        self.assertFalse(any("kafka.topic: not recognised" in n for n in notes), notes)


if __name__ == "__main__":
    unittest.main()
