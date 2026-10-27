package sh.oso.salesforce.source;

import io.grpc.Status;
import org.apache.avro.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.salesforce.testing.FakePubSubServer;
import sh.oso.salesforce.testing.MockSalesforceServer;
import sh.oso.salesforce.testing.TestSchemas;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pub/Sub stream failures on the CDC path: transient errors and expired sessions recover
 * without loss, a lost replay position applies {@code sf.gap.recovery}, and unrecoverable
 * errors fail the task with an actionable message instead of looping.
 */
class PubSubFailureRecoveryTest {

    static final String TOPIC = "/data/AccountChangeEvent";
    static final String CONTACT_TOPIC = "/data/ContactChangeEvent";

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
                .with(SourceConfig.EVENT_START, "latest");
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

    private static List<Object> keys(List<SourceRecord> records) {
        return records.stream().map(SourceRecord::key).toList();
    }

    private static BigInteger replayPosition(SourceRecord record) {
        return new BigInteger(1, Base64.getDecoder().decode((String) record.sourceOffset().get("replayId")));
    }

    /** Polls until the task throws, collecting records delivered before the failure. */
    private ConnectException pollUntilFailure(List<SourceRecord> delivered, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            try {
                delivered.addAll(harness.poll());
            } catch (ConnectException e) {
                return e;
            }
        }
        throw new AssertionError("task did not fail within " + timeoutMillis + " ms");
    }

    @Test
    void transientUnavailableMidStreamResubscribesWithoutLossOrDuplicates() throws Exception {
        startStreaming();
        publish("001A", 5000);
        List<SourceRecord> first = harness.pollUntil(1, 5000);
        assertThat(keys(first)).containsExactly("001A");

        pubsub.breakSubscriptions(TOPIC, Status.UNAVAILABLE, null);
        publish("001B", 6000);
        publish("001C", 7000);

        List<SourceRecord> after = harness.pollUntil(2, 10_000);
        assertThat(keys(after)).containsExactly("001B", "001C");
        assertThat(replayPosition(after.get(0))).isGreaterThan(replayPosition(first.get(0)));
        assertThat(replayPosition(after.get(1))).isGreaterThan(replayPosition(after.get(0)));
        assertThat(harness.pollUntil(1, 700)).isEmpty();
    }

    @Test
    void streamBrokenBeforeFirstEventResumesFromItsLatestPosition() throws Exception {
        startStreaming();
        // LATEST stream breaks before delivering anything; resubscribing LATEST would skip
        // what is published before the new stream is open.
        pubsub.breakSubscriptions(TOPIC, Status.UNAVAILABLE, null);
        publish("001A", 5000);

        assertThat(keys(harness.pollUntil(1, 10_000))).containsExactly("001A");
    }

    @Test
    void unavailableOnSubscribeIsRetriedWithBackoff() throws Exception {
        harness.with(SourceConfig.EVENT_START, "all");
        publish("001A", 5000);
        pubsub.failNextSubscribe(TOPIC, Status.UNAVAILABLE, null);

        harness.start();
        List<SourceRecord> records = harness.pollUntil(1, 10_000);
        assertThat(keys(records)).containsExactly("001A");
        // Without backoff the task would resubscribe on every poll (about every 200 ms).
        assertThat(pubsub.subscribeCalls()).isBetween(2, 3);
    }

    @Test
    void unauthenticatedInvalidatesSessionAndResumes() throws Exception {
        startStreaming();
        publish("001A", 5000);
        assertThat(harness.pollUntil(1, 5000)).hasSize(1);

        String expired = pubsub.lastAccessToken();
        pubsub.revokeAccessToken(expired);
        String fresh = sf.rotateToken();
        int tokenRequests = sf.tokenRequests();
        pubsub.breakSubscriptions(TOPIC, Status.UNAUTHENTICATED, FakePubSubServer.AUTH_ERROR);
        publish("001B", 6000);

        assertThat(keys(harness.pollUntil(1, 10_000))).containsExactly("001B");
        assertThat(sf.tokenRequests()).isGreaterThan(tokenRequests);
        assertThat(pubsub.lastAccessToken()).isEqualTo(fresh).isNotEqualTo(expired);
    }

    @Test
    void repeatedUnauthenticatedFailsTask() throws Exception {
        startStreaming();
        // Re-authentication keeps returning the revoked token, as with a deactivated user.
        pubsub.revokeAccessToken(pubsub.lastAccessToken());
        pubsub.breakSubscriptions(TOPIC, Status.UNAUTHENTICATED, FakePubSubServer.AUTH_ERROR);

        ConnectException failure = pollUntilFailure(new ArrayList<>(), 15_000);
        assertThat(failure).isInstanceOf(FatalPipelineException.class)
                .hasMessageContaining("unauthenticated 3 times in a row");
    }

    @Test
    void notFoundFailsTaskWithClearMessageAndNoResync() throws Exception {
        harness.with(SourceConfig.SOBJECTS, "Contact").with(SourceConfig.GAP_RECOVERY, "resync");
        harness.start();

        assertThatThrownBy(harness::poll)
                .isInstanceOf(FatalPipelineException.class)
                .hasMessageContaining(CONTACT_TOPIC)
                .hasMessageContaining("NOT_FOUND")
                .hasMessageContaining("Enable Change Data Capture for Contact");
        // Sticky: no resubscribe loop and no Bulk resync on a configuration error.
        assertThatThrownBy(harness::poll).isInstanceOf(FatalPipelineException.class);
        assertThat(pubsub.subscribeCalls()).isEqualTo(1);
        assertThat(sf.bulk().allQueryJobs()).isEmpty();
    }

    @Test
    void expiredReplayWithResyncRunsBulkResyncThenResumesFromProbe() throws Exception {
        harness.with(SourceConfig.GAP_RECOVERY, "resync");
        startStreaming();
        publish("001A", 5000);
        assertThat(harness.pollUntil(1, 5000)).hasSize(1);

        harness.close(); // down for longer than retention
        publish("001B", 6000);
        publish("001C", 7000);
        pubsub.expireEvents(TOPIC, 2); // 001A and 001B are gone; the stored position needs 001B
        sf.enqueueBulkQueryResultPages(List.of(
                "\"Id\",\"Name\"\n\"001B\",\"Resynced B\"\n\"001C\",\"Resynced C\"\n"));

        harness.start();
        List<SourceRecord> resynced = harness.pollUntil(2, 10_000);
        assertThat(keys(resynced)).containsExactly("001B", "001C");
        assertThat(((Struct) resynced.get(0).value()).getString("Name")).isEqualTo("Resynced B");
        assertThat(sf.bulk().allQueryJobs()).hasSize(1);
        assertThat(sf.bulk().allQueryJobs().get(0).soql).contains("SystemModstamp >=");

        // Streaming resumes from the probe taken before the resync: only new events follow.
        publish("001D", 8000);
        assertThat(keys(harness.pollUntil(1, 10_000))).containsExactly("001D");
    }

    @Test
    void expiredReplayWithLatestSkipsToNewest() throws Exception {
        harness.with(SourceConfig.GAP_RECOVERY, "latest").with(SourceConfig.EVENT_START, "all");
        publish("001A", 5000);
        harness.start();
        assertThat(keys(harness.pollUntil(1, 5000))).containsExactly("001A");

        harness.close();
        publish("001B", 6000);
        publish("001C", 7000);
        pubsub.expireEvents(TOPIC, 2);

        harness.start();
        assertThat(harness.pollUntil(1, 1500)).isEmpty(); // recovery, then a LATEST subscription
        publish("001D", 8000);
        // sf.event.start=all must not turn the recovery into a replay of everything retained (001C).
        assertThat(keys(harness.pollUntil(1, 10_000))).containsExactly("001D");
    }

    @Test
    void expiredReplayWithFailFailsAndKeepsOffset() throws Exception {
        harness.with(SourceConfig.GAP_RECOVERY, "fail");
        startStreaming();
        publish("001A", 5000);
        assertThat(harness.pollUntil(1, 5000)).hasSize(1);
        Map<String, Object> stored = harness.committedOffset(SObjectOffset.partition("Account"));

        harness.close();
        publish("001B", 6000);
        pubsub.expireEvents(TOPIC, 2);

        harness.start();
        assertThat(pollUntilFailure(new ArrayList<>(), 5000))
                .isInstanceOf(FatalPipelineException.class)
                .hasMessageContaining("sf.gap.recovery=fail");
        assertThat(harness.committedOffset(SObjectOffset.partition("Account"))).isEqualTo(stored);

        harness.restart();
        assertThat(pollUntilFailure(new ArrayList<>(), 5000)).isInstanceOf(FatalPipelineException.class);
    }

    @Test
    void fatalPipelineFlushesOtherPipelinesThenFailsTask() throws Exception {
        pubsub.createTopic(CONTACT_TOPIC, schema);
        harness.with(SourceConfig.SOBJECTS, "Account,Contact");
        harness.start();
        harness.poll();
        Thread.sleep(200);

        publish("001A", 5000);
        Thread.sleep(300); // buffered in the Account stream, not yet polled
        pubsub.breakSubscriptions(CONTACT_TOPIC, Status.PERMISSION_DENIED, null);

        List<SourceRecord> flushed = harness.poll();
        assertThat(keys(flushed)).containsExactly("001A");
        assertThatThrownBy(harness::poll)
                .isInstanceOf(FatalPipelineException.class)
                .hasMessageContaining("PERMISSION_DENIED")
                .hasMessageContaining("permission to subscribe");
    }

    @Test
    void snapshotProbeOnMissingCdcChannelFailsFast() throws Exception {
        harness.with(SourceConfig.SOBJECTS, "Contact").with(SourceConfig.SNAPSHOT_ENABLED, "true");
        harness.start();

        long started = System.currentTimeMillis();
        assertThat(pollUntilFailure(new ArrayList<>(), 5000))
                .isInstanceOf(FatalPipelineException.class)
                .hasMessageContaining("Enable Change Data Capture for Contact");
        assertThat(System.currentTimeMillis() - started).isLessThan(5000);
        assertThat(sf.bulk().allQueryJobs()).isEmpty();
    }
}
