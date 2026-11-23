package sh.oso.salesforce.source;

import com.github.tomakehurst.wiremock.stubbing.StubMapping;
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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A REST or Bulk failure inside one pipeline must not stop the others. A transient failure
 * (HTTP 5xx, 429, a dropped connection) is retried with backoff for that pipeline only and loses
 * no events; a failure retrying cannot fix fails the task with a message naming the SObject.
 */
class PipelineFailureIsolationTest {

    static final String ACCOUNT_TOPIC = "/data/AccountChangeEvent";
    static final String CONTACT_TOPIC = "/data/ContactChangeEvent";
    static final String ACCOUNT_DESCRIBE = "/services/data/v[^/]+/sobjects/Account/describe/";

    MockSalesforceServer sf;
    FakePubSubServer pubsub;
    TaskHarness harness;
    Schema schema;

    @BeforeEach
    void setUp() throws Exception {
        sf = new MockSalesforceServer().start();
        sf.stubDescribe("Account", TestSchemas.accountDescribeJson());
        sf.stubDescribe("Contact", TestSchemas.accountDescribeJson());
        pubsub = new FakePubSubServer().startOnFreePort();
        schema = TestSchemas.accountChangeEventSchema();
        pubsub.createTopic(ACCOUNT_TOPIC, schema);
        pubsub.createTopic(CONTACT_TOPIC, schema);
        harness = new TaskHarness(sf, pubsub)
                .with(SourceConfig.SOBJECTS, "Account,Contact")
                .with(SourceConfig.SNAPSHOT_ENABLED, "false")
                .with(SourceConfig.EVENT_START, "latest");
    }

    @AfterEach
    void tearDown() {
        harness.close();
        pubsub.close();
        sf.close();
    }

    private byte[] publish(String topic, String id, long commitTimestamp, String name) {
        return pubsub.publishEvent(topic, TestSchemas.accountChangeEvent(schema, "UPDATE", id, commitTimestamp, name));
    }

    private void startStreaming() throws Exception {
        harness.start();
        harness.poll(); // establishes the LATEST subscriptions
        Thread.sleep(200);
    }

    private StubMapping unavailable(com.github.tomakehurst.wiremock.client.MappingBuilder request) {
        return sf.wireMock().stubFor(request.atPriority(1)
                .willReturn(aResponse().withStatus(503).withBody(
                        "[{\"errorCode\":\"SERVER_UNAVAILABLE\",\"message\":\"Service unavailable\"}]")));
    }

    private static List<Object> keys(List<SourceRecord> records) {
        return records.stream().map(SourceRecord::key).toList();
    }

    private static BigInteger replayPosition(SourceRecord record) {
        return new BigInteger(1, Base64.getDecoder().decode((String) record.sourceOffset().get("replayId")));
    }

    /** Polls for the given time; any exception from the task fails the test. */
    private List<SourceRecord> pollFor(long millis) throws Exception {
        List<SourceRecord> out = new ArrayList<>();
        long deadline = System.currentTimeMillis() + millis;
        while (System.currentTimeMillis() < deadline) {
            out.addAll(harness.poll());
        }
        return out;
    }

    private ConnectException pollUntilFailure(long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            try {
                harness.poll();
            } catch (ConnectException e) {
                return e;
            }
        }
        throw new AssertionError("Task did not fail within " + timeoutMillis + " ms");
    }

    @Test
    void transientDescribeFailureDuringResyncIsRetriedWhileOtherSObjectsKeepStreaming() throws Exception {
        harness.with(SourceConfig.GAP_RECOVERY, "resync");
        startStreaming();
        publish(ACCOUNT_TOPIC, "001A", 5000, "A");
        publish(CONTACT_TOPIC, "003W", 5500, "W");
        assertThat(harness.pollUntil(2, 5000)).hasSize(2); // both SObjects have a stored position

        harness.close(); // down for longer than retention
        publish(ACCOUNT_TOPIC, "001B", 6000, "B");
        pubsub.expireEvents(ACCOUNT_TOPIC, 2); // Account's stored position is no longer replayable
        StubMapping outage = unavailable(get(urlPathMatching(ACCOUNT_DESCRIBE)));
        sf.enqueueBulkQueryResultPages(List.of("\"Id\",\"Name\"\n\"001B\",\"Resynced B\"\n"));

        harness.start();
        publish(CONTACT_TOPIC, "003X", 6500, "X");
        // Account's recovery keeps failing; the task stays up and Contact keeps flowing.
        List<SourceRecord> duringOutage = pollFor(2500);
        assertThat(keys(duringOutage)).containsExactly("003X");
        assertThat(duringOutage.get(0).topic()).isEqualTo("salesforce.Contact");
        assertThat(sf.bulk().allQueryJobs()).isEmpty();

        sf.wireMock().removeStub(outage);
        List<SourceRecord> resynced = harness.pollUntil(1, 15_000);
        assertThat(keys(resynced)).containsExactly("001B");
        assertThat(((Struct) resynced.get(0).value()).getString("Name")).isEqualTo("Resynced B");
        assertThat(sf.bulk().allQueryJobs()).hasSize(1);

        // Streaming resumes from the probe taken before the resync.
        publish(ACCOUNT_TOPIC, "001C", 7000, "C");
        assertThat(keys(harness.pollUntil(1, 10_000))).containsExactly("001C");
    }

    @Test
    void permanentFailureDuringResyncFailsTheTaskNamingTheSObject() throws Exception {
        harness.with(SourceConfig.GAP_RECOVERY, "resync");
        startStreaming();
        publish(ACCOUNT_TOPIC, "001A", 5000, "A");
        publish(CONTACT_TOPIC, "003W", 5500, "W");
        assertThat(harness.pollUntil(2, 5000)).hasSize(2); // both SObjects have a stored position

        harness.close();
        publish(ACCOUNT_TOPIC, "001B", 6000, "B");
        pubsub.expireEvents(ACCOUNT_TOPIC, 2);
        sf.wireMock().stubFor(get(urlPathMatching(ACCOUNT_DESCRIBE)).atPriority(1)
                .willReturn(aResponse().withStatus(404).withBody(
                        "[{\"errorCode\":\"NOT_FOUND\",\"message\":\"The requested resource does not exist\"}]")));

        harness.start();
        publish(CONTACT_TOPIC, "003X", 6500, "X");
        List<SourceRecord> delivered = new ArrayList<>();
        ConnectException failure = null;
        long deadline = System.currentTimeMillis() + 10_000;
        while (failure == null && System.currentTimeMillis() < deadline) {
            try {
                delivered.addAll(harness.poll());
            } catch (ConnectException e) {
                failure = e;
            }
        }
        assertThat(failure).isInstanceOf(FatalPipelineException.class)
                .hasMessageContaining("Account")
                .hasMessageContaining("HTTP 404");
        // What the healthy pipeline produced is handed over before the task fails.
        assertThat(keys(delivered)).containsExactly("003X");
    }

    @Test
    void transientRetrieveFailureMidBatchLosesNoEventsAndEmitsNoDuplicates() throws Exception {
        harness.with(SourceConfig.SOBJECTS, "Account").with(SourceConfig.FULL_RECORD_ON_UPDATE, "true");
        for (String id : List.of("001A", "001B", "001C")) {
            sf.wireMock().stubFor(get(urlPathMatching("/services/data/v[^/]+/sobjects/Account/" + id))
                    .willReturn(okJson("{\"Id\":\"" + id + "\",\"Name\":\"Full " + id + "\"}")));
        }
        StubMapping outage = unavailable(get(urlPathMatching("/services/data/v[^/]+/sobjects/Account/001B")));
        startStreaming();

        publish(ACCOUNT_TOPIC, "001A", 5000, null);
        publish(ACCOUNT_TOPIC, "001B", 6000, null);
        publish(ACCOUNT_TOPIC, "001C", 7000, null);

        List<SourceRecord> all = new ArrayList<>(pollFor(2500));
        // 001B cannot be completed yet; nothing after it may be emitted ahead of it.
        assertThat(keys(all)).containsExactly("001A");

        sf.wireMock().removeStub(outage);
        long deadline = System.currentTimeMillis() + 15_000;
        while (all.size() < 3 && System.currentTimeMillis() < deadline) {
            all.addAll(harness.poll());
        }
        assertThat(keys(all)).containsExactly("001A", "001B", "001C");
        assertThat(all).extracting(r -> ((Struct) r.value()).getString("Name"))
                .containsExactly("Full 001A", "Full 001B", "Full 001C");
        assertThat(replayPosition(all.get(0))).isLessThan(replayPosition(all.get(1)));
        assertThat(replayPosition(all.get(1))).isLessThan(replayPosition(all.get(2)));
    }

    @Test
    void transientBulkFailureWhenStartingASnapshotIsRetried() throws Exception {
        harness.with(SourceConfig.SOBJECTS, "Account").with(SourceConfig.SNAPSHOT_ENABLED, "true");
        StubMapping outage = unavailable(post(urlPathMatching("/services/data/v[^/]+/jobs/query")));
        sf.enqueueBulkQueryResultPages(List.of("\"Id\",\"Name\"\n\"001S\",\"Snapshot\"\n"));

        harness.start();
        assertThat(pollFor(2000)).isEmpty();

        sf.wireMock().removeStub(outage);
        assertThat(keys(harness.pollUntil(1, 15_000))).containsExactly("001S");
        assertThat(sf.bulk().allQueryJobs()).hasSize(1);
    }

    @Test
    void permanentFailureWhenStartingASnapshotFailsTheTask() throws Exception {
        harness.with(SourceConfig.SOBJECTS, "Account").with(SourceConfig.SNAPSHOT_ENABLED, "true");
        sf.wireMock().stubFor(post(urlPathMatching("/services/data/v[^/]+/jobs/query")).atPriority(1)
                .willReturn(aResponse().withStatus(400).withBody(
                        "[{\"errorCode\":\"INVALIDENTITY\",\"message\":\"sObject type 'Account' is not supported\"}]")));

        harness.start();
        assertThat(pollUntilFailure(10_000)).isInstanceOf(FatalPipelineException.class)
                .hasMessageContaining("Account")
                .hasMessageContaining("INVALIDENTITY");
    }
}
