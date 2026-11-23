package sh.oso.salesforce.source;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Replay positions on a quiet object. The event bus position moves on with activity that never
 * reaches a single-entity channel, so the position of the last delivered event ages out of
 * retention while the stream is healthy. The connector follows the position Salesforce reports
 * in keepalives, and with {@code sf.heartbeat.interval.ms} set persists it for restarts.
 */
class IdleReplayPositionTest {

    static final String TOPIC = "/data/AccountChangeEvent";
    static final String HEARTBEAT_TOPIC = "salesforce.heartbeat";

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
                .with(SourceConfig.EVENT_START, "latest")
                .with(SourceConfig.GAP_RECOVERY, "resync");
    }

    @AfterEach
    void tearDown() {
        harness.close();
        pubsub.close();
        sf.close();
    }

    private byte[] publish(String id, long commitTimestamp) {
        return pubsub.publishEvent(TOPIC, TestSchemas.accountChangeEvent(schema, "UPDATE", id, commitTimestamp, id));
    }

    private void startStreaming() throws Exception {
        harness.start();
        harness.poll(); // establishes the LATEST subscription
        Thread.sleep(200);
    }

    private List<SourceRecord> pollFor(long millis) throws Exception {
        List<SourceRecord> out = new ArrayList<>();
        long deadline = System.currentTimeMillis() + millis;
        while (System.currentTimeMillis() < deadline) {
            out.addAll(harness.poll());
        }
        return out;
    }

    /** Bus activity elsewhere moves the position on; Salesforce reports it in a keepalive. */
    private byte[] idleKeepalive() {
        byte[] position = pubsub.advanceReplayPosition(TOPIC);
        pubsub.sendKeepalive(TOPIC);
        return position;
    }

    private static List<Object> keys(List<SourceRecord> records) {
        return records.stream().map(SourceRecord::key).toList();
    }

    @Test
    void reconnectOnAnIdleObjectResumesFromTheKeepalivePositionNotTheLastEvent() throws Exception {
        startStreaming();
        publish("001A", 5000);
        assertThat(harness.pollUntil(1, 5000)).hasSize(1);

        // Days pass without an Account change: the bus moves on and 001A's position expires.
        byte[] keepalive = idleKeepalive();
        assertThat(pollFor(500)).isEmpty(); // no heartbeat records unless configured
        pubsub.expirePositionsBefore(TOPIC, keepalive);

        // Salesforce closes the stream (routine GOAWAY); the task resubscribes.
        pubsub.breakSubscriptions(TOPIC, Status.UNAVAILABLE, null);
        publish("001B", 6000);

        assertThat(keys(harness.pollUntil(1, 10_000))).containsExactly("001B");
        // The position was still valid: no gap recovery, no Bulk resync.
        assertThat(sf.bulk().allQueryJobs()).isEmpty();
    }

    @Test
    void heartbeatPersistsTheKeepalivePositionSoARestartResumesWithoutRecovery() throws Exception {
        harness.with(SourceConfig.HEARTBEAT_INTERVAL_MS, "100");
        startStreaming();
        publish("001A", 5000);
        assertThat(harness.pollUntil(1, 5000)).hasSize(1);

        byte[] keepalive = idleKeepalive();
        List<SourceRecord> heartbeats = harness.pollUntil(1, 5000);

        assertThat(heartbeats).hasSize(1);
        SourceRecord heartbeat = heartbeats.get(0);
        assertThat(heartbeat.topic()).isEqualTo(HEARTBEAT_TOPIC);
        assertThat(heartbeat.key()).isEqualTo("Account");
        assertThat(heartbeat.sourcePartition()).isEqualTo(SObjectOffset.partition("Account"));
        assertThat(heartbeat.sourceOffset().get("replayId")).isEqualTo(ReplayIds.toBase64(keepalive));
        Struct value = (Struct) heartbeat.value();
        assertThat(value.getString("name")).isEqualTo("Account");
        assertThat(value.getString("channel")).isEqualTo(TOPIC);
        assertThat(value.getString("replayId")).isEqualTo(ReplayIds.toDisplayString(keepalive));
        assertThat(value.getInt64("ts_ms")).isPositive();
        assertThat(harness.committedOffset(SObjectOffset.partition("Account")).get("replayId"))
                .isEqualTo(ReplayIds.toBase64(keepalive));

        harness.close();
        pubsub.expirePositionsBefore(TOPIC, keepalive); // 001A's position is gone
        publish("001B", 6000);
        harness.start();

        assertThat(keys(harness.pollUntil(1, 10_000))).containsExactly("001B");
        assertThat(sf.bulk().allQueryJobs()).isEmpty();
    }

    @Test
    void heartbeatIsNotEmittedWhenThePositionHasNotMoved() throws Exception {
        harness.with(SourceConfig.HEARTBEAT_INTERVAL_MS, "100");
        startStreaming();
        publish("001A", 5000);
        assertThat(harness.pollUntil(1, 5000)).hasSize(1);

        pubsub.sendKeepalive(TOPIC); // reports 001A's own position: nothing new to persist
        assertThat(pollFor(700)).isEmpty();
    }

    @Test
    void heartbeatsAreLimitedToOnePerInterval() throws Exception {
        harness.with(SourceConfig.HEARTBEAT_INTERVAL_MS, "60000");
        harness.start();
        // The position Salesforce acknowledges the new subscription with is persisted at once,
        // so a restart resumes from it instead of skipping to the newest event again.
        assertThat(harness.pollUntil(1, 5000)).extracting(SourceRecord::topic).containsExactly(HEARTBEAT_TOPIC);

        idleKeepalive();
        idleKeepalive();
        assertThat(pollFor(700)).isEmpty();
    }

    @Test
    void heartbeatTopicIsConfigurable() throws Exception {
        harness.with(SourceConfig.HEARTBEAT_INTERVAL_MS, "100")
                .with(SourceConfig.HEARTBEAT_TOPIC, "ops.salesforce-heartbeat");
        startStreaming();

        idleKeepalive();
        assertThat(harness.pollUntil(1, 5000)).extracting(SourceRecord::topic)
                .containsExactly("ops.salesforce-heartbeat");
    }

    @Test
    void eventsPublishedAfterAKeepaliveAreStillDelivered() throws Exception {
        harness.with(SourceConfig.HEARTBEAT_INTERVAL_MS, "100");
        startStreaming();

        idleKeepalive();
        publish("001A", 5000);
        idleKeepalive();
        publish("001B", 6000);

        List<SourceRecord> records = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 10_000;
        while (keys(records).stream().filter(k -> !"Account".equals(k)).count() < 2
                && System.currentTimeMillis() < deadline) {
            records.addAll(harness.poll());
        }
        List<Object> dataKeys = records.stream().filter(r -> !HEARTBEAT_TOPIC.equals(r.topic()))
                .map(SourceRecord::key).toList();
        assertThat(dataKeys).containsExactly("001A", "001B");
    }

    @Test
    void platformEventHeartbeatPersistsTheKeepalivePosition() throws Exception {
        String event = "Integration_Message__e";
        String channel = "/event/" + event;
        Schema eventSchema = TestSchemas.integrationMessageEventSchema();
        pubsub.createTopic(channel, eventSchema);
        harness.without(SourceConfig.SOBJECTS)
                .with(SourceConfig.PLATFORM_EVENTS, event)
                .with(SourceConfig.HEARTBEAT_INTERVAL_MS, "100");
        startStreaming();

        byte[] keepalive = pubsub.advanceReplayPosition(channel);
        pubsub.sendKeepalive(channel);
        List<SourceRecord> heartbeats = harness.pollUntil(1, 5000);

        assertThat(heartbeats).hasSize(1);
        assertThat(heartbeats.get(0).topic()).isEqualTo(HEARTBEAT_TOPIC);
        assertThat(heartbeats.get(0).key()).isEqualTo(event);
        assertThat(heartbeats.get(0).sourcePartition()).isEqualTo(Map.of("platformEvent", event));
        assertThat(heartbeats.get(0).sourceOffset().get("replayId")).isEqualTo(ReplayIds.toBase64(keepalive));

        harness.close();
        pubsub.expirePositionsBefore(channel, keepalive);
        pubsub.publishEvent(channel, TestSchemas.integrationMessage(eventSchema, "Upsert", 1, "{}"));
        harness.start();

        List<SourceRecord> resumed = harness.pollUntil(1, 10_000);
        assertThat(resumed).hasSize(1);
        assertThat(((Struct) resumed.get(0).value()).getFloat64("Sequence__c")).isEqualTo(1.0);
    }
}
