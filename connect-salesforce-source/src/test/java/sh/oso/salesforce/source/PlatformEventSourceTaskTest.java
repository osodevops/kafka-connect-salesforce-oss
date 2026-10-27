package sh.oso.salesforce.source;

import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import io.grpc.Status;
import org.apache.avro.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.salesforce.pubsub.ReplayIds;
import sh.oso.salesforce.testing.FakePubSubServer;
import sh.oso.salesforce.testing.MockSalesforceServer;
import sh.oso.salesforce.testing.TestSchemas;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Platform event streams on the unified source: record contract, replay, recovery and isolation. */
class PlatformEventSourceTaskTest {

    static final String EVENT = "Integration_Message__e";
    static final String CHANNEL = "/event/" + EVENT;
    static final String KAFKA_TOPIC = "salesforce." + EVENT;

    MockSalesforceServer sf;
    FakePubSubServer pubsub;
    TaskHarness harness;
    Schema schema;
    String schemaId;

    @BeforeEach
    void setUp() throws Exception {
        sf = new MockSalesforceServer().start();
        pubsub = new FakePubSubServer().startOnFreePort();
        schema = TestSchemas.integrationMessageEventSchema();
        schemaId = pubsub.createTopic(CHANNEL, schema);
        harness = new TaskHarness(sf, pubsub)
                .without(SourceConfig.SOBJECTS)
                .with(SourceConfig.PLATFORM_EVENTS, EVENT)
                .with(SourceConfig.EVENT_START, "latest");
    }

    @AfterEach
    void tearDown() {
        harness.close();
        pubsub.close();
        sf.close();
    }

    private byte[] publish(String messageType, double sequence) {
        return pubsub.publishEvent(CHANNEL, TestSchemas.integrationMessage(schema, messageType, sequence,
                TestSchemas.samplePayloadJson("006xx" + (long) sequence, 200)));
    }

    private void startStreaming() throws Exception {
        harness.start();
        harness.poll(); // establishes the LATEST subscription
        Thread.sleep(200);
    }

    private static List<Double> sequences(List<SourceRecord> records) {
        return records.stream().map(r -> ((Struct) r.value()).getFloat64("Sequence__c")).toList();
    }

    @Test
    void streamsPlatformEventWithConfluentCompatibleShape() throws Exception {
        startStreaming();
        String payload = TestSchemas.samplePayloadJson("006xx000001AbCdEF", 20_000);
        byte[] replayId = pubsub.publishEvent(CHANNEL,
                TestSchemas.integrationMessage(schema, "UpsertOpportunity", 7, payload));

        List<SourceRecord> records = harness.pollUntil(1, 5000);
        assertThat(records).hasSize(1);
        SourceRecord record = records.get(0);
        assertThat(record.topic()).isEqualTo(KAFKA_TOPIC);
        assertThat(record.key()).isNull();
        assertThat(record.keySchema()).isNull();
        assertThat(record.timestamp()).isNull();
        assertThat(record.headers()).isEmpty();

        Struct value = (Struct) record.value();
        assertThat(value.schema().name()).isEqualTo(EVENT);
        // Every field of the event, exactly as published: no lookup back to Salesforce.
        assertThat(value.getInt64("CreatedDate")).isEqualTo(1_767_225_600_000L);
        assertThat(value.getString("CreatedById")).isEqualTo("005xx000001X8UzAAK");
        assertThat(value.getString("Message_Type__c")).isEqualTo("UpsertOpportunity");
        assertThat(value.getString("Payload__c")).isEqualTo(payload).hasSizeGreaterThan(20_000);
        // Metadata in the shape consumers of Confluent's Platform Event source expect.
        assertThat(value.getString("ReplayId")).isEqualTo(new BigInteger(1, replayId).toString());
        assertThat(value.getString("EventUuid")).isEqualTo(pubsub.eventId(CHANNEL, replayId));
        assertThat(value.getString("_ObjectType")).isEqualTo(EVENT);
        assertThat(value.getString("_EventType")).isEqualTo(schemaId);

        assertThat(record.sourcePartition()).isEqualTo(Map.of("platformEvent", EVENT));
        assertThat(record.sourceOffset().get("replayId")).isEqualTo(ReplayIds.toBase64(replayId));
    }

    @Test
    void startsFromLatestByDefault() throws Exception {
        publish("BeforeStart", 1);
        startStreaming();
        publish("AfterStart", 2);

        assertThat(sequences(harness.pollUntil(1, 5000))).containsExactly(2.0);
        assertThat(harness.pollUntil(1, 500)).isEmpty();
    }

    @Test
    void replaysRetainedWithEventStartAll() throws Exception {
        harness.with(SourceConfig.EVENT_START, "all");
        publish("A", 1);
        publish("B", 2);
        publish("C", 3);

        harness.start();
        assertThat(sequences(harness.pollUntil(3, 5000))).containsExactly(1.0, 2.0, 3.0);
    }

    @Test
    void resumesAcrossRestartWithoutLossOrDuplicates() throws Exception {
        startStreaming();
        publish("A", 1);
        assertThat(sequences(harness.pollUntil(1, 5000))).containsExactly(1.0);

        harness.close();
        publish("WhileStopped", 2);
        harness.start();
        publish("AfterRestart", 3);

        assertThat(sequences(harness.pollUntil(2, 5000))).containsExactly(2.0, 3.0);
        assertThat(harness.pollUntil(1, 500)).isEmpty();
    }

    @Test
    void schemaEvolutionMidStreamProducesNewSchemaAndEventType() throws Exception {
        startStreaming();
        publish("V1", 1);
        SourceRecord before = harness.pollUntil(1, 5000).get(0);

        Schema v2 = TestSchemas.integrationMessageEventSchemaV2();
        String v2SchemaId = pubsub.updateTopicSchema(CHANNEL, v2);
        org.apache.avro.generic.GenericRecord event = TestSchemas.integrationMessage(v2, "V2", 2, "{}");
        event.put("Initiator_Id__c", "005xx000002Y9VzBBL");
        pubsub.publishEvent(CHANNEL, event);
        SourceRecord after = harness.pollUntil(1, 5000).get(0);

        Struct v1Value = (Struct) before.value();
        Struct v2Value = (Struct) after.value();
        assertThat(v1Value.schema().field("Initiator_Id__c")).isNull();
        assertThat(v2Value.getString("Initiator_Id__c")).isEqualTo("005xx000002Y9VzBBL");
        assertThat(v1Value.getString("_EventType")).isEqualTo(schemaId);
        assertThat(v2Value.getString("_EventType")).isEqualTo(v2SchemaId).isNotEqualTo(schemaId);
        assertThat(v2Value.schema().name()).isEqualTo(v1Value.schema().name());

        // A replay spanning both versions decodes each event with its own schema.
        harness.close();
        TaskHarness replay = new TaskHarness(sf, pubsub)
                .without(SourceConfig.SOBJECTS)
                .with(SourceConfig.PLATFORM_EVENTS, EVENT)
                .with(SourceConfig.EVENT_START, "all");
        try (replay) {
            replay.start();
            List<SourceRecord> both = replay.pollUntil(2, 5000);
            assertThat(both).extracting(r -> ((Struct) r.value()).getString("_EventType"))
                    .containsExactly(schemaId, v2SchemaId);
        }
    }

    private void consumeOneThenExpireTheNext() throws Exception {
        startStreaming();
        publish("A", 1);
        assertThat(harness.pollUntil(1, 5000)).hasSize(1);
        harness.close(); // down for longer than retention
        publish("B", 2);
        publish("C", 3);
        pubsub.expireEvents(CHANNEL, 2); // A and B are gone; resuming after A needs B
    }

    @Test
    void expiredReplayWithResyncResumesFromEarliestRetained() throws Exception {
        harness.with(SourceConfig.GAP_RECOVERY, "resync");
        consumeOneThenExpireTheNext();

        harness.start();
        assertThat(sequences(harness.pollUntil(1, 10_000))).containsExactly(3.0);
    }

    @Test
    void expiredReplayWithLatestSkipsToNewest() throws Exception {
        harness.with(SourceConfig.GAP_RECOVERY, "latest").with(SourceConfig.EVENT_START, "all");
        consumeOneThenExpireTheNext();

        harness.start();
        assertThat(harness.pollUntil(1, 1500)).isEmpty();
        publish("D", 4);
        assertThat(sequences(harness.pollUntil(1, 10_000))).containsExactly(4.0);
    }

    @Test
    void expiredReplayWithFailFailsAndKeepsOffset() throws Exception {
        harness.with(SourceConfig.GAP_RECOVERY, "fail");
        consumeOneThenExpireTheNext();
        Map<String, Object> stored = harness.committedOffset(PlatformEventOffset.partition(EVENT));

        harness.start();
        long deadline = System.currentTimeMillis() + 5000;
        Throwable failure = null;
        while (failure == null && System.currentTimeMillis() < deadline) {
            try {
                harness.poll();
            } catch (FatalPipelineException e) {
                failure = e;
            }
        }
        assertThat(failure).hasMessageContaining("platform event " + EVENT)
                .hasMessageContaining("sf.gap.recovery=fail");
        assertThat(harness.committedOffset(PlatformEventOffset.partition(EVENT))).isEqualTo(stored);
    }

    @Test
    void transientStreamFailureResumesWithoutLoss() throws Exception {
        startStreaming();
        publish("A", 1);
        assertThat(sequences(harness.pollUntil(1, 5000))).containsExactly(1.0);

        pubsub.breakSubscriptions(CHANNEL, Status.UNAVAILABLE, null);
        publish("B", 2);
        publish("C", 3);

        assertThat(sequences(harness.pollUntil(2, 10_000))).containsExactly(2.0, 3.0);
        assertThat(harness.pollUntil(1, 700)).isEmpty();
    }

    @Test
    void mixedSObjectAndPlatformEventInOneTask() throws Exception {
        Schema cdc = TestSchemas.accountChangeEventSchema();
        pubsub.createTopic("/data/AccountChangeEvent", cdc);
        harness.with(SourceConfig.SOBJECTS, "Account").with(SourceConfig.SNAPSHOT_ENABLED, "false");
        startStreaming();

        pubsub.publishEvent("/data/AccountChangeEvent",
                TestSchemas.accountChangeEvent(cdc, "CREATE", "001A", 5000, "Acme"));
        publish("A", 1);

        List<SourceRecord> records = harness.pollUntil(2, 5000);
        assertThat(records).extracting(SourceRecord::topic)
                .containsExactlyInAnyOrder("salesforce.Account", KAFKA_TOPIC);
        assertThat(records).filteredOn(r -> r.topic().equals("salesforce.Account"))
                .extracting(SourceRecord::key).containsExactly("001A");
    }

    @Test
    void platformEventsStreamInPollingMode() throws Exception {
        harness.with(SourceConfig.REALTIME_MODE, "polling");
        startStreaming();
        publish("A", 1);

        assertThat(sequences(harness.pollUntil(1, 5000))).containsExactly(1.0);
    }

    @Test
    void platformEventOnlyMakesNoRestOrBulkCalls() throws Exception {
        startStreaming();
        publish("A", 1);
        assertThat(harness.pollUntil(1, 5000)).hasSize(1);

        List<String> calls = new ArrayList<>();
        for (ServeEvent event : sf.wireMock().getAllServeEvents()) {
            calls.add(event.getRequest().getMethod() + " " + event.getRequest().getUrl());
        }
        assertThat(calls).allMatch(call -> call.equals("POST /services/oauth2/token"));
    }

    @Test
    void unknownPlatformEventFailsWithClearMessage() {
        harness.with(SourceConfig.PLATFORM_EVENTS, "Missing_Event__e");
        harness.start();

        assertThatThrownBy(harness::poll)
                .isInstanceOf(FatalPipelineException.class)
                .hasMessageContaining("/event/Missing_Event__e")
                .hasMessageContaining("NOT_FOUND")
                .hasMessageContaining("Check that Missing_Event__e exists");
    }
}
