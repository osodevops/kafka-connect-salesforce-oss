package sh.oso.salesforce.streaming;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.salesforce.testing.MockSalesforceServer;
import sh.oso.salesforce.testing.TestSchemas;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.assertj.core.api.Assertions.assertThat;

class StreamingSourceTaskTest {

    MockSalesforceServer sf;
    BayeuxStubServer bayeux;
    SalesforceStreamingSourceTask task;
    final Map<Map<String, Object>, Map<String, Object>> committedOffsets = new HashMap<>();

    @BeforeEach
    void setUp() throws Exception {
        sf = new MockSalesforceServer().start();
        bayeux = new BayeuxStubServer();
    }

    @AfterEach
    void tearDown() {
        if (task != null) {
            task.stop();
        }
        bayeux.close();
        sf.close();
    }

    private Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put(StreamingConfig.AUTH_GRANT_TYPE, "client_credentials");
        props.put(StreamingConfig.INSTANCE_URL, sf.baseUrl());
        props.put(StreamingConfig.CONSUMER_KEY, "k");
        props.put(StreamingConfig.CONSUMER_SECRET, "s");
        props.put(StreamingConfig.TOKEN_ENDPOINT, sf.baseUrl() + "/services/oauth2/token");
        props.put(StreamingConfig.COMETD_ENDPOINT, bayeux.endpoint());
        props.put(StreamingConfig.KAFKA_TOPIC, "sf.streaming");
        return props;
    }

    private void startTask(Map<String, String> props) {
        task = new SalesforceStreamingSourceTask();
        task.initialize(new SourceTaskContext() {
            @Override
            public Map<String, String> configs() {
                return props;
            }

            @Override
            public OffsetStorageReader offsetStorageReader() {
                return new OffsetStorageReader() {
                    @Override
                    public <T> Map<String, Object> offset(Map<String, T> partition) {
                        return committedOffsets.get(partition);
                    }

                    @Override
                    public <T> Map<Map<String, T>, Map<String, Object>> offsets(
                            Collection<Map<String, T>> partitions) {
                        Map<Map<String, T>, Map<String, Object>> out = new HashMap<>();
                        partitions.forEach(p -> out.put(p, committedOffsets.get(p)));
                        return out;
                    }
                };
            }
        });
        task.start(props);
    }

    @SuppressWarnings("unchecked")
    private List<SourceRecord> pollUntil(int n, long timeoutMillis) throws InterruptedException {
        List<SourceRecord> out = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (out.size() < n && System.currentTimeMillis() < deadline) {
            List<SourceRecord> polled = task.poll();
            if (polled != null) {
                for (SourceRecord record : polled) {
                    committedOffsets.put((Map<String, Object>) record.sourcePartition(),
                            (Map<String, Object>) record.sourceOffset());
                    out.add(record);
                }
            }
        }
        return out;
    }

    @Test
    void pushTopicAutoCreateAndCrudStreaming() throws Exception {
        sf.stubQuery("SELECT Id FROM PushTopic WHERE Name = 'AccountUpdates'",
                "{\"done\":true,\"totalSize\":0,\"records\":[]}");
        sf.stubDescribe("Account", TestSchemas.accountDescribeJson());
        sf.wireMock().stubFor(post(urlPathMatching("/services/data/v[^/]+/sobjects/PushTopic/"))
                .willReturn(okJson("{\"id\":\"0IF000\",\"success\":true}")));

        Map<String, String> props = baseProps();
        props.put(StreamingConfig.CHANNEL_TYPE, "pushtopic");
        props.put(StreamingConfig.PUSHTOPIC_NAME, "AccountUpdates");
        props.put(StreamingConfig.OBJECT, "Account");
        startTask(props);

        sf.wireMock().verify(postRequestedFor(urlPathMatching("/services/data/v[^/]+/sobjects/PushTopic/"))
                .withRequestBody(matchingJsonPath("$.Name",
                        com.github.tomakehurst.wiremock.client.WireMock.equalTo("AccountUpdates")))
                .withRequestBody(matchingJsonPath("$.NotifyForOperationCreate",
                        com.github.tomakehurst.wiremock.client.WireMock.equalTo("true"))));

        bayeux.publish("/topic/AccountUpdates", Map.of(
                "event", Map.of("type", "updated", "createdDate", "2024-02-01T10:00:00.000Z"),
                "sobject", Map.of("Id", "001A", "Name", "Acme")));

        List<SourceRecord> records = pollUntil(1, 5000);
        assertThat(records).hasSize(1);
        SourceRecord record = records.get(0);
        assertThat(record.topic()).isEqualTo("sf.streaming");
        assertThat(record.key()).isEqualTo("001A");
        Map<String, Object> value = (Map<String, Object>) record.value();
        assertThat(value.get("Name")).isEqualTo("Acme");
        assertThat(value.get("_EventType")).isEqualTo("updated");
        assertThat(value.get("_ObjectType")).isEqualTo("Account");
        assertThat(record.sourceOffset().get("replayId")).isEqualTo(1L);
    }

    @Test
    void cdcChannelMapsChangeEventsAndResumesFromStoredReplay() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StreamingConfig.CHANNEL_TYPE, "cdc");
        props.put(StreamingConfig.CDC_CHANNEL, "ChangeEvents");
        startTask(props);

        bayeux.publish("/data/ChangeEvents", Map.of(
                "payload", Map.of(
                        "ChangeEventHeader", Map.of(
                                "entityName", "Contact",
                                "changeType", "CREATE",
                                "recordIds", List.of("003C")),
                        "LastName", "Smith")));
        List<SourceRecord> first = pollUntil(1, 5000);
        assertThat(first).hasSize(1);
        Map<String, Object> value = (Map<String, Object>) first.get(0).value();
        assertThat(value.get("_EventType")).isEqualTo("created");
        assertThat(value.get("_ObjectType")).isEqualTo("Contact");
        assertThat(value.get("LastName")).isEqualTo("Smith");
        assertThat(value).doesNotContainKey("ChangeEventHeader");

        // Restart: replay extension must resume from the stored replayId, not re-deliver.
        task.stop();
        bayeux.publish("/data/ChangeEvents", Map.of(
                "payload", Map.of(
                        "ChangeEventHeader", Map.of(
                                "entityName", "Contact",
                                "changeType", "UPDATE",
                                "recordIds", List.of("003C")),
                        "LastName", "Smythe")));
        startTask(props);
        assertThat(bayeux.lastRequestedReplay()).isEqualTo(1L);
        List<SourceRecord> second = pollUntil(1, 5000);
        assertThat(second).hasSize(1);
        assertThat(((Map<String, Object>) second.get(0).value()).get("_EventType")).isEqualTo("updated");
    }

    @Test
    void invalidReplayFallsBackPerConfig() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StreamingConfig.CHANNEL_TYPE, "cdc");
        props.put(StreamingConfig.INVALID_REPLAY_BEHAVIOUR, "all");
        // Pretend the org only retains replayIds >= 100; a stored offset of 5 is expired.
        committedOffsets.put(Map.of("channel", "/data/ChangeEvents"), Map.of("replayId", 5L));
        bayeux.setMinValidReplayId(100);
        bayeux.publish("/data/ChangeEvents", Map.of(
                "payload", Map.of(
                        "ChangeEventHeader", Map.of(
                                "entityName", "Lead", "changeType", "CREATE",
                                "recordIds", List.of("00QL")),
                        "Company", "Retained Inc")));

        startTask(props);
        List<SourceRecord> records = pollUntil(1, 5000);
        // fallback ALL (-2) resubscribes and replays the retained event
        assertThat(records).hasSize(1);
        assertThat(((Map<String, Object>) records.get(0).value()).get("Company"))
                .isEqualTo("Retained Inc");
    }

    @Test
    void entityFilterDropsOtherEntities() throws Exception {
        Map<String, String> props = baseProps();
        props.put(StreamingConfig.CHANNEL_TYPE, "cdc");
        props.put(StreamingConfig.CHANNEL_ENTITIES, "Account");
        startTask(props);

        bayeux.publish("/data/ChangeEvents", Map.of(
                "payload", Map.of("ChangeEventHeader", Map.of(
                        "entityName", "Contact", "changeType", "CREATE", "recordIds", List.of("003X")))));
        bayeux.publish("/data/ChangeEvents", Map.of(
                "payload", Map.of("ChangeEventHeader", Map.of(
                        "entityName", "Account", "changeType", "CREATE", "recordIds", List.of("001X")))));

        List<SourceRecord> records = pollUntil(1, 5000);
        assertThat(records).hasSize(1);
        assertThat(((Map<String, Object>) records.get(0).value()).get("_ObjectType")).isEqualTo("Account");
    }
}
