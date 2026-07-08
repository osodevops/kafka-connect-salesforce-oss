package sh.oso.salesforce.source;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.salesforce.testing.FakePubSubServer;
import sh.oso.salesforce.testing.MockSalesforceServer;
import sh.oso.salesforce.testing.TestSchemas;

import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.assertj.core.api.Assertions.assertThat;

class EventDrivenSourceTaskTest {

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

    @Test
    void streamsCdcEventsWithMetadata() throws Exception {
        harness.start();
        harness.poll(); // establish subscription
        Thread.sleep(200);
        pubsub.publishEvent(TOPIC,
                TestSchemas.accountChangeEvent(schema, "CREATE", "001A", 5000, "Acme"));

        List<SourceRecord> records = harness.pollUntil(1, 5000);
        assertThat(records).hasSize(1);
        SourceRecord record = records.get(0);
        assertThat(record.topic()).isEqualTo("salesforce.Account");
        assertThat(record.key()).isEqualTo("001A");
        Struct value = (Struct) record.value();
        assertThat(value.getString("Name")).isEqualTo("Acme");
        assertThat(value.getString("_EventType")).isEqualTo("created");
        assertThat(value.getString("_ObjectType")).isEqualTo("Account");
        assertThat(record.headers().lastWithName("sf.change.type").value()).isEqualTo("CREATE");
        assertThat(record.headers().lastWithName("sf.entity").value()).isEqualTo("Account");
        assertThat(record.sourceOffset()).containsKey("replayId");
    }

    @Test
    void resumesFromStoredReplayIdAcrossRestart() throws Exception {
        harness.start();
        harness.poll();
        Thread.sleep(200);
        pubsub.publishEvent(TOPIC, TestSchemas.accountChangeEvent(schema, "CREATE", "001A", 5000, "A"));
        assertThat(harness.pollUntil(1, 5000)).hasSize(1);

        harness.restart();
        // Published while "down": must be delivered after restart; the already-consumed
        // event must NOT reappear (resume is exclusive of the stored replayId).
        pubsub.publishEvent(TOPIC, TestSchemas.accountChangeEvent(schema, "UPDATE", "001A", 6000, "A2"));
        List<SourceRecord> records = harness.pollUntil(1, 5000);
        assertThat(records).hasSize(1);
        assertThat(((Struct) records.get(0).value()).getString("_EventType")).isEqualTo("updated");
    }

    @Test
    void emitsTombstoneOnDelete() throws Exception {
        harness.with(SourceConfig.EMIT_TOMBSTONE_ON_DELETE, "true").start();
        harness.poll();
        Thread.sleep(200);
        pubsub.publishEvent(TOPIC, TestSchemas.accountChangeEvent(schema, "DELETE", "001A", 5000, "A"));

        List<SourceRecord> records = harness.pollUntil(2, 5000);
        assertThat(records).hasSize(2);
        assertThat(((Struct) records.get(0).value()).getString("_EventType")).isEqualTo("deleted");
        assertThat(records.get(1).value()).isNull();
        assertThat(records.get(1).key()).isEqualTo("001A");
    }

    @Test
    void gapEventTriggersRestReQuery() throws Exception {
        sf.wireMock().stubFor(get(urlPathMatching("/services/data/v[^/]+/sobjects/Account/001GAP.*"))
                .willReturn(okJson("""
                        {"Id":"001GAP","Name":"Recovered","Industry":"Retail",
                         "NumberOfEmployees":7,"IsDeleted":false}
                        """)));
        harness.start();
        harness.poll();
        Thread.sleep(200);
        pubsub.publishEvent(TOPIC,
                TestSchemas.accountChangeEvent(schema, "GAP_UPDATE", "001GAP", 5000, null));

        List<SourceRecord> records = harness.pollUntil(1, 5000);
        assertThat(records).hasSize(1);
        Struct value = (Struct) records.get(0).value();
        assertThat(value.getString("Name")).isEqualTo("Recovered");
        assertThat(value.getString("_EventType")).isEqualTo("updated");
        assertThat(records.get(0).key()).isEqualTo("001GAP");
    }

    @Test
    void multipleRecordIdsFanOut() throws Exception {
        harness.start();
        harness.poll();
        Thread.sleep(200);
        GenericRecord event = TestSchemas.accountChangeEvent(schema, "UPDATE", "001A", 5000, "Bulkified");
        GenericRecord header = (GenericRecord) event.get("ChangeEventHeader");
        header.put("recordIds", List.of("001A", "001B", "001C"));
        pubsub.publishEvent(TOPIC, event);

        List<SourceRecord> records = harness.pollUntil(3, 5000);
        assertThat(records).extracting(SourceRecord::key).containsExactly("001A", "001B", "001C");
    }
}
