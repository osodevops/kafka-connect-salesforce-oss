package sh.oso.salesforce.source;

import org.apache.avro.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import sh.oso.salesforce.testing.FakePubSubServer;
import sh.oso.salesforce.testing.MockSalesforceServer;
import sh.oso.salesforce.testing.TestSchemas;

import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A custom object can be listed in {@code sf.sobjects} by API name ({@code My_Object__c}) or by
 * change event entity name ({@code My_Object__}). Either way the connector subscribes to
 * {@code /data/My_Object__ChangeEvent}, calls REST and SOQL with {@code My_Object__c}, and keeps
 * the topic and offset partition named after the value as configured.
 */
class CustomObjectSourceTaskTest {

    static final String CHANNEL = "/data/My_Object__ChangeEvent";
    static final String API_NAME = "My_Object__c";

    MockSalesforceServer sf;
    FakePubSubServer pubsub;
    TaskHarness harness;
    Schema schema;

    @BeforeEach
    void setUp() throws Exception {
        sf = new MockSalesforceServer().start();
        // Only the API name has a describe; any other spelling gets the fake's 404.
        sf.stubDescribe(API_NAME, TestSchemas.accountDescribeJson());
        pubsub = new FakePubSubServer().startOnFreePort();
        schema = TestSchemas.accountChangeEventSchema();
        pubsub.createTopic(CHANNEL, schema);
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

    private void publish(String changeType, String id, long commitTimestamp, String name) {
        pubsub.publishEvent(CHANNEL, TestSchemas.accountChangeEvent(schema, changeType, id, commitTimestamp, name));
    }

    private void startStreaming() throws Exception {
        harness.start();
        harness.poll(); // establishes the LATEST subscription
        Thread.sleep(200);
    }

    @ParameterizedTest
    @ValueSource(strings = {"My_Object__", "My_Object__c"})
    void streamsChangeEventChannelAndFetchesFullRecordByApiName(String configured) throws Exception {
        sf.wireMock().stubFor(get(urlPathMatching("/services/data/v[^/]+/sobjects/" + API_NAME + "/a00A"))
                .willReturn(okJson("{\"Id\":\"a00A\",\"Name\":\"Full record\"}")));
        harness.with(SourceConfig.SOBJECTS, configured)
                .with(SourceConfig.FULL_RECORD_ON_UPDATE, "true");
        startStreaming();
        assertThat(pubsub.activeSubscribers(CHANNEL)).isEqualTo(1);

        publish("UPDATE", "a00A", 5000, null); // partial event: Name absent, filled from REST
        List<SourceRecord> records = harness.pollUntil(1, 5000);

        assertThat(records).hasSize(1);
        SourceRecord record = records.get(0);
        assertThat(record.topic()).isEqualTo("salesforce." + configured);
        assertThat(record.sourcePartition()).isEqualTo(Map.of("sobject", configured));
        assertThat(((Struct) record.value()).getString("Name")).isEqualTo("Full record");
        sf.wireMock().verify(getRequestedFor(
                urlPathMatching("/services/data/v[^/]+/sobjects/" + API_NAME + "/a00A")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"My_Object__", "My_Object__c"})
    void rejectedReplayWithResyncDescribesAndQueriesApiName(String configured) throws Exception {
        harness.with(SourceConfig.SOBJECTS, configured).with(SourceConfig.GAP_RECOVERY, "resync");
        startStreaming();
        publish("UPDATE", "a00A", 5000, "A");
        assertThat(harness.pollUntil(1, 5000)).hasSize(1);

        harness.close(); // down for longer than retention
        publish("UPDATE", "a00B", 6000, "B");
        pubsub.expireEvents(CHANNEL, 2); // the stored position is no longer replayable
        sf.enqueueBulkQueryResultPages(List.of("\"Id\",\"Name\"\n\"a00B\",\"Resynced B\"\n"));

        harness.start();
        List<SourceRecord> resynced = harness.pollUntil(1, 10_000);

        // Before the fix, My_Object__ was described as-is: HTTP 404 and the task failed.
        assertThat(resynced).extracting(SourceRecord::key).containsExactly("a00B");
        assertThat(resynced.get(0).topic()).isEqualTo("salesforce." + configured);
        assertThat(sf.bulk().allQueryJobs()).hasSize(1);
        assertThat(sf.bulk().allQueryJobs().get(0).soql).contains(" FROM " + API_NAME + " WHERE");
        sf.wireMock().verify(getRequestedFor(
                urlPathMatching("/services/data/v[^/]+/sobjects/" + API_NAME + "/describe/")));

        // The task is still alive and streaming resumes from the probe.
        publish("UPDATE", "a00C", 7000, "C");
        assertThat(harness.pollUntil(1, 10_000)).extracting(SourceRecord::key).containsExactly("a00C");
    }
}
