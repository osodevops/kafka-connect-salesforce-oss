package sh.oso.salesforce.pubsub;

import com.google.protobuf.ByteString;
import com.salesforce.eventbus.protobuf.ProducerEvent;
import com.salesforce.eventbus.protobuf.PublishResponse;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.salesforce.testing.FakePubSubServer;
import sh.oso.salesforce.testing.MockSalesforceServer;
import sh.oso.salesforce.testing.TestSchemas;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PubSubClientTest {

    static final String TOPIC = "/data/AccountChangeEvent";

    MockSalesforceServer sf;
    FakePubSubServer pubsub;
    PubSubClient client;
    Schema schema;

    @BeforeEach
    void setUp() throws Exception {
        sf = new MockSalesforceServer().start();
        pubsub = new FakePubSubServer().start();
        schema = TestSchemas.accountChangeEventSchema();
        pubsub.createTopic(TOPIC, schema);
        client = new PubSubClient(sf.sessionSupplier(), pubsub.channel());
    }

    @AfterEach
    void tearDown() {
        client.close();
        pubsub.close();
        sf.close();
    }

    @Test
    void subscribeLatestReceivesOnlyNewEvents() throws Exception {
        pubsub.publishEvent(TOPIC, TestSchemas.accountChangeEvent(schema, "CREATE", "001OLD", 1000, "Old"));

        try (PubSubSubscription subscription = client.subscribe(SubscribeOptions.latest(TOPIC, 10))) {
            assertThat(subscription.awaitEstablished(2000)).isTrue();
            byte[] replayId = pubsub.publishEvent(TOPIC,
                    TestSchemas.accountChangeEvent(schema, "UPDATE", "001NEW", 2000, "New"));

            List<DecodedEvent> events = subscription.poll(10, 2000);
            assertThat(events).hasSize(1);
            DecodedEvent event = events.get(0);
            assertThat(event.replayId()).isEqualTo(replayId);
            assertThat(event.payload().get("Name").toString()).isEqualTo("New");
            GenericRecord header = ChangeEventUtils.changeEventHeader(event.payload()).orElseThrow();
            assertThat(header.get("changeType").toString()).isEqualTo("UPDATE");
        }
    }

    @Test
    void subscribeCustomResumesAfterStoredReplayId() throws Exception {
        byte[] first = pubsub.publishEvent(TOPIC,
                TestSchemas.accountChangeEvent(schema, "CREATE", "001A", 1000, "A"));
        pubsub.publishEvent(TOPIC, TestSchemas.accountChangeEvent(schema, "CREATE", "001B", 2000, "B"));

        try (PubSubSubscription subscription = client.subscribe(SubscribeOptions.custom(TOPIC, first, 10))) {
            List<DecodedEvent> events = subscription.poll(10, 2000);
            assertThat(events).hasSize(1);
            assertThat(events.get(0).payload().get("Name").toString()).isEqualTo("B");
        }
    }

    @Test
    void subscribeEarliestReplaysEverything() throws Exception {
        pubsub.publishEvent(TOPIC, TestSchemas.accountChangeEvent(schema, "CREATE", "001A", 1000, "A"));
        pubsub.publishEvent(TOPIC, TestSchemas.accountChangeEvent(schema, "DELETE", "001A", 2000, "A"));

        try (PubSubSubscription subscription = client.subscribe(SubscribeOptions.earliest(TOPIC, 10))) {
            List<DecodedEvent> events = new java.util.ArrayList<>();
            long deadline = System.currentTimeMillis() + 5000;
            while (events.size() < 2 && System.currentTimeMillis() < deadline) {
                events.addAll(subscription.poll(10, 200));
            }
            assertThat(events).hasSize(2);
        }
    }

    @Test
    void flowControlRequestsMoreThanOneBatch() throws Exception {
        for (int i = 0; i < 7; i++) {
            pubsub.publishEvent(TOPIC,
                    TestSchemas.accountChangeEvent(schema, "CREATE", "001X" + i, 1000 + i, "N" + i));
        }
        try (PubSubSubscription subscription = client.subscribe(SubscribeOptions.earliest(TOPIC, 3))) {
            int total = 0;
            long deadline = System.currentTimeMillis() + 5000;
            while (total < 7 && System.currentTimeMillis() < deadline) {
                total += subscription.poll(10, 200).size();
            }
            assertThat(total).isEqualTo(7);
        }
    }

    @Test
    void publishRpcReturnsPerEventResults() {
        Schema peSchema = TestSchemas.orderShippedEventSchema();
        pubsub.createTopic("/event/Order_Shipped__e", peSchema);

        ProducerEvent event = ProducerEvent.newBuilder()
                .setId("evt-1")
                .setSchemaId(client.getTopic("/event/Order_Shipped__e").getSchemaId())
                .setPayload(ByteString.copyFrom(new byte[]{1}))
                .build();
        PublishResponse response = client.publish("/event/Order_Shipped__e", List.of(event));
        assertThat(response.getResultsList()).hasSize(1);
        assertThat(response.getResults(0).getReplayId().isEmpty()).isFalse();
        assertThat(pubsub.publishedViaRpc("/event/Order_Shipped__e")).hasSize(1);
    }

    @Test
    void getSchemaIsCached() {
        String schemaId = client.getTopic(TOPIC).getSchemaId();
        Schema first = client.getSchema(schemaId);
        Schema second = client.getSchema(schemaId);
        assertThat(second).isSameAs(first);
        assertThat(first.getName()).isEqualTo("AccountChangeEvent");
    }
}
