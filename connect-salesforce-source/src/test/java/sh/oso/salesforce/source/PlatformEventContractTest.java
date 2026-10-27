package sh.oso.salesforce.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.salesforce.testing.FakePubSubServer;
import sh.oso.salesforce.testing.MockSalesforceServer;
import sh.oso.salesforce.testing.TestSchemas;

import java.io.InputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The platform event record contract, as a Connect worker serialises it with
 * {@code JsonConverter} ({@code schemas.enable=false}): field names and JSON types must match the
 * reference record ({@code contract/confluent-platform-event-record.json}), whose standard fields
 * ({@code ReplayId} string, {@code CreatedDate} epoch-millis number, {@code EventUuid},
 * {@code _ObjectType}, {@code _EventType}) follow the shape Confluent's Platform Event source
 * produces, so its consumers keep working after a migration. The key must be null.
 *
 * <p>Every run writes {@code target/platform-event-evidence.json} (uploaded by CI).</p>
 */
class PlatformEventContractTest {

    static final ObjectMapper MAPPER = new ObjectMapper();
    static final String EVENT = "Integration_Message__e";
    static final String CHANNEL = "/event/" + EVENT;

    MockSalesforceServer sf;
    FakePubSubServer pubsub;
    TaskHarness harness;

    @BeforeEach
    void setUp() throws Exception {
        sf = new MockSalesforceServer().start();
        pubsub = new FakePubSubServer().startOnFreePort();
        harness = new TaskHarness(sf, pubsub)
                .without(SourceConfig.SOBJECTS)
                .with(SourceConfig.PLATFORM_EVENTS, EVENT)
                .with(SourceConfig.EVENT_START, "all");
    }

    @AfterEach
    void tearDown() {
        harness.close();
        pubsub.close();
        sf.close();
    }

    @Test
    void jsonRenderingMatchesTheConfluentRecordContract() throws Exception {
        Schema schema = TestSchemas.integrationMessageEventSchema();
        String schemaId = pubsub.createTopic(CHANNEL, schema);
        String payload = TestSchemas.samplePayloadJson("006xx000001AbCdEF", 50_000);
        byte[] replayId = pubsub.publishEvent(CHANNEL,
                TestSchemas.integrationMessage(schema, "UpsertOpportunity", 7, payload));

        harness.start();
        List<SourceRecord> records = harness.pollUntil(1, 5000);
        assertThat(records).hasSize(1);
        SourceRecord record = records.get(0);

        JsonConverter converter = new JsonConverter();
        converter.configure(Map.of("schemas.enable", "false"), false);
        JsonNode rendered = MAPPER.readTree(
                converter.fromConnectData(record.topic(), record.valueSchema(), record.value()));
        JsonNode reference;
        try (InputStream in = getClass().getResourceAsStream("/contract/confluent-platform-event-record.json")) {
            reference = MAPPER.readTree(in);
        }

        ObjectNode evidence = MAPPER.createObjectNode();
        evidence.put("report", "platform-event-record-contract-evidence");
        evidence.put("schemaVersion", 1);
        evidence.put("generatedAt", Instant.now().toString());
        evidence.put("procedure", "publish a platform event to the fake Pub/Sub API; stream it with "
                + "sh.oso.salesforce.source.SalesforceSourceConnector (sf.platform.events); render the record "
                + "with JsonConverter (schemas.enable=false); compare with the reference record contract");
        evidence.put("topic", record.topic());
        evidence.put("payloadCharacters", payload.length());
        ArrayNode checks = evidence.putArray("checks");
        boolean pass = true;
        pass &= check(checks, "key is null", "null", String.valueOf(record.key()));
        pass &= check(checks, "field names", fieldNames(reference).toString(), fieldNames(rendered).toString());
        for (Iterator<String> names = reference.fieldNames(); names.hasNext(); ) {
            String name = names.next();
            JsonNode actual = rendered.path(name);
            pass &= check(checks, "type of " + name, reference.get(name).getNodeType().name(),
                    actual.getNodeType().name());
        }
        pass &= check(checks, "Payload__c delivered intact", Integer.toString(payload.length()),
                Integer.toString(rendered.path("Payload__c").asText().length()));
        pass &= check(checks, "ReplayId is the decimal replay position",
                new BigInteger(1, replayId).toString(), rendered.path("ReplayId").asText());
        pass &= check(checks, "EventUuid is the event's UUID",
                pubsub.eventId(CHANNEL, replayId), rendered.path("EventUuid").asText());
        pass &= check(checks, "_ObjectType is the event API name", EVENT, rendered.path("_ObjectType").asText());
        pass &= check(checks, "_EventType is the Pub/Sub schema ID", schemaId, rendered.path("_EventType").asText());
        ObjectNode sample = rendered.deepCopy();
        sample.put("Payload__c", payload.substring(0, 120) + "... (" + payload.length() + " characters)");
        evidence.set("renderedRecord", sample);
        evidence.put("verdict", pass ? "PASS" : "FAIL");

        Path out = Path.of("target", "platform-event-evidence.json");
        Files.createDirectories(out.getParent());
        Files.writeString(out, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(evidence));

        assertThat(checks).allSatisfy(c -> assertThat(c.get("pass").asBoolean())
                .as(c.get("check").asText() + ": expected " + c.get("expected") + ", got " + c.get("actual"))
                .isTrue());
    }

    private static boolean check(ArrayNode checks, String name, String expected, String actual) {
        boolean pass = expected.equals(actual);
        ObjectNode entry = checks.addObject();
        entry.put("check", name);
        entry.put("expected", expected);
        entry.put("actual", actual);
        entry.put("pass", pass);
        return pass;
    }

    private static TreeSet<String> fieldNames(JsonNode node) {
        TreeSet<String> names = new TreeSet<>();
        node.fieldNames().forEachRemaining(names::add);
        return names;
    }
}
