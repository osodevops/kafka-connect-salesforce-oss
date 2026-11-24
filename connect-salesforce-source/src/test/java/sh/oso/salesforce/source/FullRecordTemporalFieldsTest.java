package sh.oso.salesforce.source;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.salesforce.testing.FakePubSubServer;
import sh.oso.salesforce.testing.MockSalesforceServer;
import sh.oso.salesforce.testing.TestSchemas;

import java.util.ArrayList;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * With {@code sf.full.record.on.update=true}, an UPDATE event's missing fields are filled from the
 * REST post-image. Change events carry date, datetime and time fields as Avro {@code long}; REST
 * returns them as ISO-8601 text. Each must convert to the representation the rest of the connector
 * uses, and a value that cannot be converted must not fail the task.
 */
class FullRecordTemporalFieldsTest {

    static final String CHANNEL = "/data/Example__ChangeEvent";
    static final String API_NAME = "Example__c";

    MockSalesforceServer sf;
    FakePubSubServer pubsub;
    TaskHarness harness;
    Schema schema;

    @BeforeEach
    void setUp() throws Exception {
        sf = new MockSalesforceServer().start();
        sf.stubDescribe(API_NAME, TestSchemas.accountDescribeJson());
        pubsub = new FakePubSubServer().startOnFreePort();
        schema = exampleChangeEventSchema();
        pubsub.createTopic(CHANNEL, schema);
        harness = new TaskHarness(sf, pubsub)
                .with(SourceConfig.SOBJECTS, API_NAME)
                .with(SourceConfig.FULL_RECORD_ON_UPDATE, "true")
                .with(SourceConfig.SNAPSHOT_ENABLED, "false")
                .with(SourceConfig.EVENT_START, "latest");
    }

    @AfterEach
    void tearDown() {
        harness.close();
        pubsub.close();
        sf.close();
    }

    /** The trimmed Account change event plus the temporal fields of a custom object. */
    private static Schema exampleChangeEventSchema() {
        List<Schema.Field> fields = new ArrayList<>();
        for (Schema.Field field : TestSchemas.accountChangeEventSchema().getFields()) {
            fields.add(new Schema.Field(field, field.schema()));
        }
        Schema optionalLong = SchemaBuilder.unionOf().nullType().and().longType().endUnion();
        for (String name : List.of("Start_Time__c", "End_Time__c", "Due_Date__c", "Approved_At__c")) {
            fields.add(new Schema.Field(name, optionalLong, null, Schema.Field.NULL_DEFAULT_VALUE));
        }
        return Schema.createRecord("Example__ChangeEvent", null, "com.sforce.eventbus", false, fields);
    }

    private void stubFullRecord(String id, String json) {
        sf.wireMock().stubFor(get(urlPathMatching("/services/data/v[^/]+/sobjects/" + API_NAME + "/" + id))
                .willReturn(okJson(json)));
    }

    private void publishUpdate(String id, long commitTimestamp) {
        // Partial event: the temporal fields are absent and are filled from REST.
        pubsub.publishEvent(CHANNEL, TestSchemas.accountChangeEvent(schema, "UPDATE", id, commitTimestamp, "Example"));
    }

    private void startStreaming() throws Exception {
        harness.start();
        harness.poll(); // establishes the LATEST subscription
        Thread.sleep(200);
    }

    @Test
    void updateWithTimeDateAndDatetimeFieldsConvertsThemLikeTheRestOfTheConnector() throws Exception {
        stubFullRecord("a00A", """
                {"Id":"a00A","Name":"Example",
                 "Start_Time__c":"23:00:00.000Z","End_Time__c":"18:55:00.000Z",
                 "Due_Date__c":"2024-01-31","Approved_At__c":"2024-01-31T09:30:00.000+0000"}
                """);
        startStreaming();

        publishUpdate("a00A", 5000);
        List<SourceRecord> records = harness.pollUntil(1, 5000);

        // Before the fix, Start_Time__c reached Instant.parse and killed the task.
        assertThat(records).hasSize(1);
        Struct value = (Struct) records.get(0).value();
        assertThat(value.getInt64("Start_Time__c")).isEqualTo(82_800_000L);   // millis since midnight
        assertThat(value.getInt64("End_Time__c")).isEqualTo(68_100_000L);
        assertThat(value.getInt64("Due_Date__c")).isEqualTo(1_706_659_200_000L); // midnight UTC
        assertThat(value.getInt64("Approved_At__c")).isEqualTo(1_706_693_400_000L);
    }

    @Test
    void unconvertibleValueKeepsTheChangeEventValueAndTheTaskKeepsStreaming() throws Exception {
        stubFullRecord("a00B", """
                {"Id":"a00B","Name":"Example",
                 "Start_Time__c":"25:99:00.000Z","End_Time__c":"18:55:00.000Z"}
                """);
        stubFullRecord("a00C", """
                {"Id":"a00C","Name":"Example","Start_Time__c":"23:00:00.000Z"}
                """);
        startStreaming();

        publishUpdate("a00B", 5000);
        List<SourceRecord> first = harness.pollUntil(1, 5000);

        assertThat(first).hasSize(1);
        Struct value = (Struct) first.get(0).value();
        assertThat(value.get("Start_Time__c")).isNull();                       // as the event had it
        assertThat(value.getInt64("End_Time__c")).isEqualTo(68_100_000L);      // other fields still filled

        publishUpdate("a00C", 6000);
        List<SourceRecord> next = harness.pollUntil(1, 5000);
        assertThat(next).extracting(SourceRecord::key).containsExactly("a00C");
        assertThat(((Struct) next.get(0).value()).getInt64("Start_Time__c")).isEqualTo(82_800_000L);
    }
}
