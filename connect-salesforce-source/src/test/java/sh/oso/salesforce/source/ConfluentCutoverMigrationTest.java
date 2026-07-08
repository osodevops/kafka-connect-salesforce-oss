package sh.oso.salesforce.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.salesforce.testing.FakePubSubServer;
import sh.oso.salesforce.testing.MockSalesforceServer;
import sh.oso.salesforce.testing.TestSchemas;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Executable proof of the documented Confluent → OSS cutover procedure
 * (docs: /migration/confluent): stop the old connector, then start this suite's
 * source connector with {@code sf.event.start=all} inside the 72-hour retention
 * window. The guarantee under test: <b>no event is lost across the cutover</b> —
 * events delivered before the cutover and events arriving during the connector
 * gap are all present afterwards; overlap duplicates are bounded and identifiable
 * by (record Id, commit timestamp).
 *
 * <p>Every run writes a machine-readable evidence report to
 * {@code target/migration-evidence.json} (uploaded as a CI artifact), in the
 * spirit of OSO Kafka Backup's verification evidence.</p>
 */
class ConfluentCutoverMigrationTest {

    static final String TOPIC = "/data/AccountChangeEvent";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    MockSalesforceServer sf;
    FakePubSubServer pubsub;
    TaskHarness harness;
    Schema schema;

    @BeforeEach
    void setUp() throws Exception {
        sf = new MockSalesforceServer().start();
        sf.stubDescribe("Account", TestSchemas.accountDescribeJson());
        pubsub = new FakePubSubServer().startOnFreePort();
        schema = TestSchemas.accountChangeEventSchema();
        pubsub.createTopic(TOPIC, schema);
        harness = new TaskHarness(sf, pubsub)
                .with(SourceConfig.SNAPSHOT_ENABLED, "false")
                .with(SourceConfig.EVENT_START, "all"); // the documented cutover setting
    }

    @AfterEach
    void tearDown() {
        harness.close();
        pubsub.close();
        sf.close();
    }

    @Test
    void cutoverLosesNothingAndDuplicatesAreIdentifiable() throws Exception {
        // ---- Phase 1: the "Confluent era". These events were already delivered to
        // Kafka by the old connector and still sit inside Salesforce's retention.
        long t1 = Instant.now().toEpochMilli() - 60_000;
        publish("CREATE", "001A", t1, "Acme");
        publish("UPDATE", "001A", t1 + 1_000, "Acme Ltd");
        Set<String> deliveredByOldConnector = Set.of(
                key("001A", t1), key("001A", t1 + 1_000));

        // ---- Phase 2: cutover gap. The old connector is stopped; changes keep
        // happening in Salesforce with no connector running.
        long cutoverAt = Instant.now().toEpochMilli();
        publish("UPDATE", "001B", cutoverAt + 500, "Beta");
        publish("DELETE", "001C", cutoverAt + 900, "Gamma");

        // ---- Phase 3: the OSS connector starts with sf.event.start=all and
        // replays everything Salesforce retained.
        harness.start();
        List<SourceRecord> records = harness.pollUntil(4, 10_000);

        // ---- Verification: zero loss, duplicates identifiable.
        Map<String, SourceRecord> byKey = new HashMap<>();
        Set<String> seen = new LinkedHashSet<>();
        for (SourceRecord record : records) {
            Struct value = (Struct) record.value();
            long commitTs = (Long) record.headers().lastWithName("sf.commit.timestamp").value();
            String dedupeKey = key((String) record.key(), commitTs);
            seen.add(dedupeKey);
            byKey.put(dedupeKey, record);
            assertThat(value.getString("_ObjectType")).isEqualTo("Account");
        }
        // 1. Nothing lost: every event in the retention window was re-delivered,
        //    including those that arrived while no connector was running.
        assertThat(seen).containsAll(deliveredByOldConnector);
        assertThat(seen).contains(key("001B", cutoverAt + 500), key("001C", cutoverAt + 900));
        // 2. Duplicates are exactly the pre-cutover overlap, and each is
        //    identifiable by (record Id, commit timestamp) for consumer dedup.
        Set<String> expectedDuplicates = deliveredByOldConnector;
        Set<String> freshEvents = new LinkedHashSet<>(seen);
        freshEvents.removeAll(expectedDuplicates);
        assertThat(freshEvents).hasSize(2);
        // 3. Replay position is monotonic (replayId continuity across the boundary).
        List<byte[]> replayIds = records.stream()
                .map(r -> java.util.Base64.getDecoder().decode((String) r.sourceOffset().get("replayId")))
                .toList();
        for (int i = 1; i < replayIds.size(); i++) {
            assertThat(new java.math.BigInteger(1, replayIds.get(i)))
                    .isGreaterThan(new java.math.BigInteger(1, replayIds.get(i - 1)));
        }

        writeEvidence(records, deliveredByOldConnector, cutoverAt);
    }

    private void publish(String changeType, String recordId, long commitTs, String name) {
        pubsub.publishEvent(TOPIC,
                TestSchemas.accountChangeEvent(schema, changeType, recordId, commitTs, name));
    }

    private static String key(String recordId, long commitTs) {
        return recordId + "@" + commitTs;
    }

    /** Evidence report: what was verified, over what data, with payload digests. */
    private void writeEvidence(List<SourceRecord> records, Set<String> preCutover, long cutoverAt)
            throws Exception {
        ObjectNode evidence = MAPPER.createObjectNode();
        evidence.put("report", "confluent-cutover-migration-evidence");
        evidence.put("schemaVersion", 1);
        evidence.put("generatedAt", Instant.now().toString());
        evidence.put("procedure",
                "stop old connector -> start sh.oso source with sf.event.start=all inside 72h retention");
        evidence.put("cutoverAtEpochMs", cutoverAt);
        evidence.put("eventsBeforeCutover", preCutover.size());
        evidence.put("eventsDuringGap", 2);
        evidence.put("eventsDeliveredAfterCutover", records.size());
        evidence.put("eventsLost", 0);
        evidence.put("verdict", "PASS: zero loss; duplicates limited to pre-cutover overlap");
        ArrayNode delivered = evidence.putArray("delivered");
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        for (SourceRecord record : records) {
            ObjectNode entry = delivered.addObject();
            entry.put("key", (String) record.key());
            entry.put("commitTimestamp",
                    (Long) record.headers().lastWithName("sf.commit.timestamp").value());
            entry.put("changeType",
                    String.valueOf(record.headers().lastWithName("sf.change.type").value()));
            entry.put("valueSha256", java.util.HexFormat.of().formatHex(
                    sha256.digest(String.valueOf(record.value()).getBytes(StandardCharsets.UTF_8))));
        }
        Path out = Path.of("target", "migration-evidence.json");
        Files.createDirectories(out.getParent());
        Files.writeString(out, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(evidence));
    }
}
