package sh.oso.salesforce.source;

import org.apache.avro.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.salesforce.testing.FakePubSubServer;
import sh.oso.salesforce.testing.MockSalesforceServer;
import sh.oso.salesforce.testing.TestSchemas;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotHandoffTest {

    static final String TOPIC = "/data/AccountChangeEvent";

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
        harness = new TaskHarness(sf, pubsub).with(SourceConfig.SNAPSHOT_ENABLED, "true");
    }

    @AfterEach
    void tearDown() {
        harness.close();
        pubsub.close();
        sf.close();
    }

    @Test
    void snapshotThenSeamlessHandoffWithoutDuplicateOrLoss() throws Exception {
        sf.enqueueBulkQueryResultPages(List.of(
                "\"Id\",\"Name\",\"SystemModstamp\"\n"
                        + "\"001S1\",\"Snap One\",\"2024-01-01T00:00:00.000+0000\"\n"
                        + "\"001S2\",\"Snap Two\",\"2024-01-01T00:00:00.000+0000\"\n"));

        harness.start();
        // Snapshot rows arrive first.
        List<SourceRecord> snapshotRecords = harness.pollUntil(2, 5000);
        assertThat(snapshotRecords).hasSize(2);
        assertThat(((Struct) snapshotRecords.get(0).value()).getString("_EventType")).isEqualTo("created");

        // An event committed BEFORE snapshot completion (already captured by the snapshot):
        // replayed via the probe subscription but deduplicated by commit timestamp.
        pubsub.publishEvent(TOPIC, TestSchemas.accountChangeEvent(
                schema, "UPDATE", "001S1", System.currentTimeMillis() - 60_000, "Stale"));
        // An event committed after snapshot completion: must flow through.
        pubsub.publishEvent(TOPIC, TestSchemas.accountChangeEvent(
                schema, "UPDATE", "001S2", System.currentTimeMillis() + 60_000, "Fresh"));

        List<SourceRecord> streamRecords = harness.pollUntil(1, 5000);
        assertThat(streamRecords).hasSize(1);
        assertThat(streamRecords.get(0).key()).isEqualTo("001S2");
        assertThat(((Struct) streamRecords.get(0).value()).getString("Name")).isEqualTo("Fresh");
    }

    @Test
    void eventsDuringSnapshotAreReplayedFromProbe() throws Exception {
        // Delay snapshot completion by one extra poll so an event lands mid-snapshot.
        sf.enqueueBulkQueryResultPages(List.of(
                "\"Id\",\"Name\"\n\"001S1\",\"Snap\"\n",
                "\"Id\",\"Name\"\n\"001S3\",\"Snap Late\"\n"));

        harness.start();
        List<SourceRecord> first = harness.pollUntil(1, 5000);
        assertThat(first).isNotEmpty();
        // Mid-snapshot CDC activity with a commit timestamp after (simulated) completion.
        pubsub.publishEvent(TOPIC, TestSchemas.accountChangeEvent(
                schema, "CREATE", "001MID", System.currentTimeMillis() + 60_000, "Mid Snapshot"));

        // Drain the rest of the snapshot, then the handoff must replay the mid-snapshot event.
        List<SourceRecord> rest = harness.pollUntil(2, 5000);
        assertThat(rest.stream().map(SourceRecord::key)).contains("001S3", "001MID");
    }
}
