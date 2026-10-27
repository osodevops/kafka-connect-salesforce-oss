package sh.oso.salesforce.pubsub;

import com.google.protobuf.ByteString;
import com.salesforce.eventbus.protobuf.ProducerEvent;
import com.salesforce.eventbus.protobuf.PublishResponse;
import io.grpc.Status;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.salesforce.common.SalesforceException;
import sh.oso.salesforce.testing.FakePubSubServer;
import sh.oso.salesforce.testing.MockSalesforceServer;
import sh.oso.salesforce.testing.TestSchemas;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void corruptedCustomReplaySurfacesTypedException() {
        try (PubSubSubscription subscription = client.subscribe(
                SubscribeOptions.custom(TOPIC, new byte[3], 10))) {
            assertThatThrownBy(() -> subscription.poll(10, 2000))
                    .isInstanceOfSatisfying(PubSubStreamException.class, e -> {
                        assertThat(e.isInvalidReplay()).isTrue();
                        assertThat(e.errorCode()).isEqualTo(FakePubSubServer.INVALID_REPLAY_ERROR);
                    });
        }
    }

    @Test
    void expiredReplayIsRejected() throws Exception {
        byte[] first = pubsub.publishEvent(TOPIC, TestSchemas.accountChangeEvent(schema, "CREATE", "001A", 1000, "A"));
        byte[] second = pubsub.publishEvent(TOPIC, TestSchemas.accountChangeEvent(schema, "CREATE", "001B", 2000, "B"));
        pubsub.publishEvent(TOPIC, TestSchemas.accountChangeEvent(schema, "CREATE", "001C", 3000, "C"));
        pubsub.expireEvents(TOPIC, 2);

        // Resuming after the first event needs the (expired) second one.
        try (PubSubSubscription subscription = client.subscribe(SubscribeOptions.custom(TOPIC, first, 10))) {
            assertThatThrownBy(() -> subscription.poll(10, 2000))
                    .isInstanceOfSatisfying(PubSubStreamException.class,
                            e -> assertThat(e.isInvalidReplay()).isTrue());
        }
        // Resuming after the last expired event needs nothing that has expired.
        try (PubSubSubscription subscription = client.subscribe(SubscribeOptions.custom(TOPIC, second, 10))) {
            assertThat(subscription.poll(10, 2000)).extracting(e -> e.payload().get("Name").toString())
                    .containsExactly("C");
        }
        try (PubSubSubscription subscription = client.subscribe(SubscribeOptions.earliest(TOPIC, 10))) {
            assertThat(subscription.poll(10, 2000)).hasSize(1);
        }
    }

    @Test
    void subscribeFailureIsTyped() {
        pubsub.failNextSubscribe(TOPIC, Status.UNAVAILABLE, null);

        try (PubSubSubscription subscription = client.subscribe(SubscribeOptions.latest(TOPIC, 10))) {
            assertThatThrownBy(() -> subscription.poll(10, 2000))
                    .isInstanceOfSatisfying(PubSubStreamException.class, e -> {
                        assertThat(e.code()).isEqualTo(Status.Code.UNAVAILABLE);
                        assertThat(e.isRetryable()).isTrue();
                    });
        }
    }

    @Test
    void midStreamFailureIsTyped() throws Exception {
        try (PubSubSubscription subscription = client.subscribe(SubscribeOptions.latest(TOPIC, 10))) {
            assertThat(subscription.awaitEstablished(2000)).isTrue();
            pubsub.breakSubscriptions(TOPIC, Status.UNAVAILABLE, null);

            assertThatThrownBy(() -> subscription.poll(10, 2000))
                    .isInstanceOfSatisfying(PubSubStreamException.class,
                            e -> assertThat(e.isRetryable()).isTrue());
            assertThat(subscription.isBroken()).isTrue();
        }
    }

    @Test
    void awaitEstablishedFailsFastOnStreamError() {
        pubsub.failNextSubscribe(TOPIC, Status.NOT_FOUND, null);

        long started = System.currentTimeMillis();
        try (PubSubSubscription subscription = client.subscribe(SubscribeOptions.latest(TOPIC, 10))) {
            assertThatThrownBy(() -> subscription.awaitEstablished(10_000))
                    .isInstanceOfSatisfying(PubSubStreamException.class,
                            e -> assertThat(e.code()).isEqualTo(Status.Code.NOT_FOUND));
        }
        assertThat(System.currentTimeMillis() - started).isLessThan(2000);
    }

    @Test
    void eventsAfterDecodeFailureAreNeverReturned() throws Exception {
        pubsub.publishEvent(TOPIC, TestSchemas.accountChangeEvent(schema, "CREATE", "001A", 1000, "Good"));
        pubsub.publishRawEvent(TOPIC, new byte[0]);

        try (PubSubSubscription subscription = client.subscribe(SubscribeOptions.earliest(TOPIC, 10))) {
            List<DecodedEvent> first = subscription.poll(10, 2000);
            assertThat(first).extracting(e -> e.payload().get("Name").toString()).containsExactly("Good");
            pubsub.publishEvent(TOPIC, TestSchemas.accountChangeEvent(schema, "CREATE", "001B", 2000, "After"));

            assertThatThrownBy(() -> subscription.poll(10, 2000))
                    .isInstanceOf(SalesforceException.class)
                    .isNotInstanceOf(PubSubStreamException.class)
                    .hasMessageContaining("Avro-decode");
            assertThatThrownBy(() -> subscription.poll(10, 100)).isInstanceOf(SalesforceException.class);
        }
    }

    @Test
    void subscriptionCarriesItsSessionAndRevokedTokensAreUnauthenticated() throws Exception {
        try (PubSubSubscription subscription = client.subscribe(SubscribeOptions.latest(TOPIC, 10))) {
            assertThat(subscription.awaitEstablished(2000)).isTrue();
            assertThat(subscription.session().accessToken()).isEqualTo(pubsub.lastAccessToken());
            pubsub.revokeAccessToken(subscription.session().accessToken());
        }
        try (PubSubSubscription subscription = client.subscribe(SubscribeOptions.latest(TOPIC, 10))) {
            assertThatThrownBy(() -> subscription.poll(10, 2000))
                    .isInstanceOfSatisfying(PubSubStreamException.class,
                            e -> assertThat(e.isUnauthenticated()).isTrue());
        }
    }
}
