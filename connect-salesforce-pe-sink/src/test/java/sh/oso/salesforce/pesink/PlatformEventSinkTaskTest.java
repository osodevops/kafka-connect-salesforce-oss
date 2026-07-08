package sh.oso.salesforce.pesink;

import com.salesforce.eventbus.protobuf.ProducerEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.salesforce.testing.FakePubSubServer;
import sh.oso.salesforce.testing.MockSalesforceServer;
import sh.oso.salesforce.testing.TestSchemas;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PlatformEventSinkTaskTest {

    static final String EVENT_TOPIC = "/event/Order_Shipped__e";

    MockSalesforceServer sf;
    FakePubSubServer pubsub;
    SalesforcePlatformEventSinkTask task;
    ErrantRecordReporter reporter;
    Schema schema;

    @BeforeEach
    void setUp() throws Exception {
        sf = new MockSalesforceServer().start();
        pubsub = new FakePubSubServer().startOnFreePort();
        schema = TestSchemas.orderShippedEventSchema();
        pubsub.createTopic(EVENT_TOPIC, schema);
    }

    @AfterEach
    void tearDown() {
        if (task != null) {
            task.stop();
        }
        pubsub.close();
        sf.close();
    }

    private Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put(PeSinkConfig.AUTH_GRANT_TYPE, "client_credentials");
        props.put(PeSinkConfig.INSTANCE_URL, sf.baseUrl());
        props.put(PeSinkConfig.CONSUMER_KEY, "k");
        props.put(PeSinkConfig.CONSUMER_SECRET, "s");
        props.put(PeSinkConfig.TOKEN_ENDPOINT, sf.baseUrl() + "/services/oauth2/token");
        props.put(PeSinkConfig.PLATFORM_EVENT_NAME, "Order_Shipped__e");
        props.put(PeSinkConfig.PUBSUB_ENDPOINT, pubsub.endpoint());
        props.put(PeSinkConfig.PUBSUB_PLAINTEXT, "true");
        return props;
    }

    private void startTask(Map<String, String> props) {
        task = new SalesforcePlatformEventSinkTask();
        reporter = mock(ErrantRecordReporter.class);
        SinkTaskContext context = mock(SinkTaskContext.class);
        when(context.errantRecordReporter()).thenReturn(reporter);
        task.initialize(context);
        task.start(props);
    }

    private SinkRecord record(Map<String, Object> fields) {
        return new SinkRecord("orders", 0, null, null, null, fields, 0);
    }

    @Test
    void publishesAvroEncodedEventViaPubSub() throws Exception {
        startTask(baseProps());
        task.put(List.of(record(Map.of(
                "OrderNumber__c", "ORD-42", "Carrier__c", "DHL", "ShippedAt__c", 1706693400000L))));

        List<ProducerEvent> published = pubsub.publishedViaRpc(EVENT_TOPIC);
        assertThat(published).hasSize(1);
        GenericRecord decoded = new GenericDatumReader<GenericRecord>(schema)
                .read(null, DecoderFactory.get().binaryDecoder(
                        published.get(0).getPayload().toByteArray(), null));
        assertThat(decoded.get("OrderNumber__c").toString()).isEqualTo("ORD-42");
        assertThat(decoded.get("ShippedAt__c")).isEqualTo(1706693400000L);
        // required fields auto-filled
        assertThat((Long) decoded.get("CreatedDate")).isPositive();
        assertThat(decoded.get("CreatedById")).isNotNull();
    }

    @Test
    void typeMismatchIsReportedNotPublished() {
        Map<String, String> props = baseProps();
        props.put(PeSinkConfig.BEHAVIOR_ON_API_ERRORS, "log");
        startTask(props);
        task.put(List.of(record(Map.of("OrderNumber__c", "OK", "ShippedAt__c", "not-a-long"))));

        assertThat(pubsub.publishedViaRpc(EVENT_TOPIC)).isEmpty();
        verify(reporter).report(any(), any());
    }

    @Test
    void perEventPublishErrorsGoToDlqAndFailBehaviorThrows() {
        pubsub.setPublishFailer(event -> "LIMIT_EXCEEDED: event bus quota");
        startTask(baseProps());
        assertThatThrownBy(() -> task.put(List.of(record(Map.of("OrderNumber__c", "ORD-1")))))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("LIMIT_EXCEEDED");
        verify(reporter).report(any(), any());
    }

    @Test
    void restModePublishesViaComposite() {
        sf.wireMock().stubFor(post(urlPathMatching("/services/data/v[^/]+/composite/sobjects"))
                .willReturn(okJson("[{\"id\":\"e01\",\"success\":true}]")));
        Map<String, String> props = baseProps();
        props.put(PeSinkConfig.PUBLISH_MODE, "rest");
        startTask(props);

        task.put(List.of(record(Map.of("OrderNumber__c", "ORD-9"))));
        sf.wireMock().verify(postRequestedFor(urlPathMatching("/services/data/v[^/]+/composite/sobjects"))
                .withRequestBody(matchingJsonPath("$.records[0].OrderNumber__c",
                        com.github.tomakehurst.wiremock.client.WireMock.equalTo("ORD-9")))
                .withRequestBody(matchingJsonPath("$.records[0].attributes.type",
                        com.github.tomakehurst.wiremock.client.WireMock.equalTo("Order_Shipped__e"))));
    }

    @Test
    void rejectsEventNameWithoutSuffix() {
        Map<String, String> props = baseProps();
        props.put(PeSinkConfig.PLATFORM_EVENT_NAME, "OrderShipped");
        assertThatThrownBy(() -> new PeSinkConfig(props))
                .hasMessageContaining("__e");
    }
}
