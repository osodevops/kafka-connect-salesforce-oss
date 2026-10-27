package sh.oso.salesforce.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.kafka.KafkaContainer;
import sh.oso.salesforce.testing.FakePubSubServer;
import sh.oso.salesforce.testing.MockSalesforceServer;
import sh.oso.salesforce.testing.TestSchemas;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * True end-to-end test: a real Kafka broker and a real Kafka Connect worker (docker)
 * load the packaged connector plugin directories and run against the fake Salesforce
 * harness on the host (WireMock HTTP surface + gRPC Pub/Sub fake), proving plugin
 * packaging, classloader isolation, config plumbing, and the full data paths.
 */
class ConnectClusterEndToEndIT {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectClusterEndToEndIT.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    // apache/kafka:3.9.0's docker init validates advertised.listeners before Testcontainers'
    // starter script exports them; 3.7.0 is the image the Kafka module supports.
    private static final String KAFKA_IMAGE = "apache/kafka:3.7.0";
    private static final String CDC_TOPIC = "/data/AccountChangeEvent";
    private static final String PLATFORM_EVENT = "Integration_Message__e";
    private static final String PE_CHANNEL = "/event/" + PLATFORM_EVENT;
    private static final String HOST_ALIAS = "host.testcontainers.internal";

    static MockSalesforceServer sf;
    static FakePubSubServer pubsub;
    static Schema cdcSchema;
    static Schema peSchema;
    static String peSchemaId;
    static Network network;
    static KafkaContainer kafka;
    static GenericContainer<?> connect;
    static String connectUrl;
    static final HttpClient http = HttpClient.newHttpClient();

    @BeforeAll
    static void setUp() throws Exception {
        sf = new MockSalesforceServer().start();
        sf.stubDescribe("Account", TestSchemas.accountDescribeJson());
        pubsub = new FakePubSubServer().startOnFreePort();
        cdcSchema = TestSchemas.accountChangeEventSchema();
        pubsub.createTopic(CDC_TOPIC, cdcSchema);
        peSchema = TestSchemas.integrationMessageEventSchema();
        peSchemaId = pubsub.createTopic(PE_CHANNEL, peSchema);

        int pubsubPort = Integer.parseInt(pubsub.endpoint().split(":")[1]);
        Testcontainers.exposeHostPorts(sf.port(), pubsubPort);
        sf.advertise("http://" + HOST_ALIAS + ":" + sf.port());

        network = Network.newNetwork();
        kafka = new KafkaContainer(KAFKA_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("kafka")
                .withListener("kafka:19092")
                .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("kafka"));
        kafka.start();

        String version = System.getProperty("project.version", "0.1.0-SNAPSHOT");
        Path repoRoot = Path.of(System.getProperty("repo.root", "..")).toAbsolutePath().normalize();
        connect = new GenericContainer<>(KAFKA_IMAGE)
                .withNetwork(network)
                .withExposedPorts(8083)
                .withAccessToHost(true)
                .withCreateContainerCmdModifier(cmd ->
                        cmd.withEntrypoint("/opt/kafka/bin/connect-distributed.sh", "/connect.properties"))
                .withCopyToContainer(Transferable.of(workerProperties()), "/connect.properties")
                .waitingFor(Wait.forHttp("/connectors").forPort(8083).withStartupTimeout(Duration.ofMinutes(3)))
                .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("connect"));
        for (String module : List.of("connect-salesforce-source", "connect-salesforce-sink")) {
            Path plugin = repoRoot.resolve(module).resolve("target")
                    .resolve(module + "-" + version + "-kafka-connect-plugin")
                    .resolve(module + "-" + version);
            if (!Files.isDirectory(plugin)) {
                throw new IllegalStateException("Plugin dir missing (run mvn package first): " + plugin);
            }
            // Copy rather than bind-mount: Docker Desktop's host-path cache goes stale when
            // `mvn clean` recreates target directories just before container start.
            connect.withCopyFileToContainer(
                    org.testcontainers.utility.MountableFile.forHostPath(plugin), "/plugins/" + module);
        }
        connect.start();
        connectUrl = "http://" + connect.getHost() + ":" + connect.getMappedPort(8083);
    }

    @AfterAll
    static void tearDown() {
        if (connect != null) {
            connect.stop();
        }
        if (kafka != null) {
            kafka.stop();
        }
        if (network != null) {
            network.close();
        }
        if (pubsub != null) {
            pubsub.close();
        }
        if (sf != null) {
            sf.close();
        }
    }

    private static String workerProperties() {
        return """
                bootstrap.servers=kafka:19092
                group.id=sf-e2e
                key.converter=org.apache.kafka.connect.storage.StringConverter
                value.converter=org.apache.kafka.connect.json.JsonConverter
                value.converter.schemas.enable=false
                config.storage.topic=_connect_configs
                offset.storage.topic=_connect_offsets
                status.storage.topic=_connect_status
                config.storage.replication.factor=1
                offset.storage.replication.factor=1
                status.storage.replication.factor=1
                offset.flush.interval.ms=2000
                plugin.path=/plugins
                listeners=HTTP://0.0.0.0:8083
                """;
    }

    @Test
    void sourceConnectorStreamsCdcFromFakeSalesforceIntoKafka() throws Exception {
        // Retained CDC event published before the connector starts; sf.event.start=all replays it.
        pubsub.publishEvent(CDC_TOPIC, TestSchemas.accountChangeEvent(
                cdcSchema, "CREATE", "001E2E", 7_000, "E2E Streamed"));

        registerConnector("salesforce-source", Map.ofEntries(
                Map.entry("connector.class", "sh.oso.salesforce.source.SalesforceSourceConnector"),
                Map.entry("tasks.max", "1"),
                Map.entry("sf.auth.grant.type", "client_credentials"),
                Map.entry("sf.instance.url", "http://" + HOST_ALIAS + ":" + sf.port()),
                Map.entry("sf.consumer.key", "k"),
                Map.entry("sf.consumer.secret", "s"),
                Map.entry("sf.token.endpoint",
                        "http://" + HOST_ALIAS + ":" + sf.port() + "/services/oauth2/token"),
                Map.entry("sf.pubsub.endpoint", HOST_ALIAS + ":" + pubsub.endpoint().split(":")[1]),
                Map.entry("sf.pubsub.plaintext", "true"),
                Map.entry("sf.sobjects", "Account"),
                Map.entry("sf.topic.prefix", "salesforce"),
                Map.entry("sf.snapshot.enabled", "false"),
                Map.entry("sf.event.start", "all")));
        awaitConnectorRunning("salesforce-source");

        try (KafkaConsumer<String, String> consumer = consumer("salesforce.Account")) {
            ConsumerRecord<String, String> record = awaitOneRecord(consumer);
            assertThat(record.key()).isEqualTo("001E2E");
            JsonNode value = MAPPER.readTree(record.value());
            assertThat(value.path("Name").asText()).isEqualTo("E2E Streamed");
            assertThat(value.path("_EventType").asText()).isEqualTo("created");
            assertThat(value.path("_ObjectType").asText()).isEqualTo("Account");
        }
    }

    /**
     * A platform event connector configured with no SObjects at all: every field of a large
     * event reaches Kafka as published, offsets land in the worker's offset store, and a task
     * restart resumes without duplicates. Writes {@code target/platform-event-e2e-evidence.json}.
     */
    @Test
    void sourceConnectorStreamsPlatformEventsIntoKafka() throws Exception {
        String payload = TestSchemas.samplePayloadJson("006xx000001AbCdEF", 30_000);
        byte[] first = pubsub.publishEvent(PE_CHANNEL,
                TestSchemas.integrationMessage(peSchema, "UpsertOpportunity", 1, payload));

        Map<String, String> config = Map.ofEntries(
                Map.entry("connector.class", "sh.oso.salesforce.source.SalesforceSourceConnector"),
                Map.entry("tasks.max", "1"),
                Map.entry("sf.auth.grant.type", "client_credentials"),
                Map.entry("sf.instance.url", "http://" + HOST_ALIAS + ":" + sf.port()),
                Map.entry("sf.consumer.key", "k"),
                Map.entry("sf.consumer.secret", "s"),
                Map.entry("sf.token.endpoint",
                        "http://" + HOST_ALIAS + ":" + sf.port() + "/services/oauth2/token"),
                Map.entry("sf.pubsub.endpoint", HOST_ALIAS + ":" + pubsub.endpoint().split(":")[1]),
                Map.entry("sf.pubsub.plaintext", "true"),
                Map.entry("sf.platform.events", PLATFORM_EVENT),
                Map.entry("sf.topic.prefix", "salesforce"),
                Map.entry("sf.event.start", "all"));
        registerConnector("salesforce-pe-source", config);
        awaitConnectorRunning("salesforce-pe-source");

        String topic = "salesforce." + PLATFORM_EVENT;
        List<ConsumerRecord<String, String>> delivered = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = consumer(topic)) {
            delivered.addAll(awaitRecords(consumer, 1));
            ConsumerRecord<String, String> record = delivered.get(0);
            assertThat(record.key()).isNull();
            JsonNode value = MAPPER.readTree(record.value());
            assertThat(value.path("CreatedDate").isIntegralNumber()).isTrue();
            assertThat(value.path("CreatedDate").asLong()).isEqualTo(1_767_225_600_000L);
            assertThat(value.path("Payload__c").asText()).isEqualTo(payload);
            assertThat(value.path("ReplayId").asText())
                    .isEqualTo(Long.toUnsignedString(java.nio.ByteBuffer.wrap(first).getLong()));
            assertThat(value.path("EventUuid").asText()).isEqualTo(pubsub.eventId(PE_CHANNEL, first));
            assertThat(value.path("_ObjectType").asText()).isEqualTo(PLATFORM_EVENT);
            assertThat(value.path("_EventType").asText()).isEqualTo(peSchemaId);

            byte[] second = pubsub.publishEvent(PE_CHANNEL,
                    TestSchemas.integrationMessage(peSchema, "UpsertOpportunity", 2, "{}"));
            delivered.addAll(awaitRecords(consumer, 1));
            JsonNode offsets = awaitStoredReplayId("salesforce-pe-source", second);

            // Restart the task on the worker: it must resume from the stored offset.
            restartConnector("salesforce-pe-source");
            awaitConnectorRunning("salesforce-pe-source");
            pubsub.publishEvent(PE_CHANNEL, TestSchemas.integrationMessage(peSchema, "UpsertOpportunity", 3, "{}"));
            delivered.addAll(awaitRecords(consumer, 1));
            delivered.addAll(drain(consumer, Duration.ofSeconds(5)));

            List<Double> sequences = new ArrayList<>();
            for (ConsumerRecord<String, String> r : delivered) {
                sequences.add(MAPPER.readTree(r.value()).path("Sequence__c").asDouble());
            }
            writePlatformEventEvidence(config, delivered, sequences, offsets);
            assertThat(sequences).containsExactly(1.0, 2.0, 3.0);
        }
    }

    @Test
    void sinkConnectorWritesKafkaRecordsToFakeSalesforceViaBulk() throws Exception {
        registerConnector("salesforce-sink", Map.ofEntries(
                Map.entry("connector.class", "sh.oso.salesforce.sink.SalesforceSinkConnector"),
                Map.entry("tasks.max", "1"),
                Map.entry("topics", "e2e.leads"),
                Map.entry("sf.auth.grant.type", "client_credentials"),
                Map.entry("sf.instance.url", "http://" + HOST_ALIAS + ":" + sf.port()),
                Map.entry("sf.consumer.key", "k"),
                Map.entry("sf.consumer.secret", "s"),
                Map.entry("sf.token.endpoint",
                        "http://" + HOST_ALIAS + ":" + sf.port() + "/services/oauth2/token"),
                Map.entry("sf.objects", "Account"),
                Map.entry("sf.Account.topics", "e2e.leads"),
                Map.entry("sf.Account.override.event.type", "true"),
                Map.entry("sf.Account.operation", "insert"),
                Map.entry("sf.write.mode", "bulk2"),
                Map.entry("behavior.on.api.errors", "log")));
        awaitConnectorRunning("salesforce-sink");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>("e2e.leads", null,
                    "{\"Name\":\"E2E Sink Co\",\"Industry\":\"Technology\"}")).get();
        }

        await().atMost(Duration.ofMinutes(2)).untilAsserted(() -> {
            var jobs = sf.bulk().allIngestJobs();
            assertThat(jobs).isNotEmpty();
            assertThat(jobs.get(jobs.size() - 1).uploadedCsv).contains("E2E Sink Co");
        });
    }

    // ---------------------------------------------------------------- helpers

    /** Waits until the worker's offset store holds the given replay position for the event. */
    private static JsonNode awaitStoredReplayId(String connector, byte[] replayId) {
        String expected = Base64.getEncoder().encodeToString(replayId);
        JsonNode[] offsets = new JsonNode[1];
        await().atMost(Duration.ofMinutes(1)).untilAsserted(() -> {
            HttpResponse<String> response = http.send(HttpRequest.newBuilder(
                            URI.create(connectUrl + "/connectors/" + connector + "/offsets")).build(),
                    HttpResponse.BodyHandlers.ofString());
            JsonNode body = MAPPER.readTree(response.body());
            JsonNode entry = body.path("offsets").path(0);
            assertThat(entry.path("partition").path("platformEvent").asText()).isEqualTo(PLATFORM_EVENT);
            assertThat(entry.path("offset").path("replayId").asText()).isEqualTo(expected);
            offsets[0] = body;
        });
        return offsets[0];
    }

    private static void restartConnector(String name) throws Exception {
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(
                        URI.create(connectUrl + "/connectors/" + name + "/restart?includeTasks=true"))
                        .POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isIn(200, 202, 204);
    }

    /** Every record polled until at least {@code count} have arrived. */
    private static List<ConsumerRecord<String, String>> awaitRecords(KafkaConsumer<String, String> consumer,
                                                                    int count) {
        List<ConsumerRecord<String, String>> out = new ArrayList<>();
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
        while (out.size() < count && System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofSeconds(1)).forEach(out::add);
        }
        if (out.size() < count) {
            throw new AssertionError("Only " + out.size() + " of " + count + " records arrived within the deadline");
        }
        return out;
    }

    private static List<ConsumerRecord<String, String>> drain(KafkaConsumer<String, String> consumer,
                                                             Duration window) {
        List<ConsumerRecord<String, String>> out = new ArrayList<>();
        long deadline = System.currentTimeMillis() + window.toMillis();
        while (System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(500)).forEach(out::add);
        }
        return out;
    }

    private static void writePlatformEventEvidence(Map<String, String> config,
                                                   List<ConsumerRecord<String, String>> delivered,
                                                   List<Double> sequences, JsonNode offsets) throws Exception {
        ObjectNode evidence = MAPPER.createObjectNode();
        evidence.put("report", "platform-event-connect-cluster-evidence");
        evidence.put("schemaVersion", 1);
        evidence.put("generatedAt", Instant.now().toString());
        evidence.put("environment", "Kafka broker and Kafka Connect worker " + KAFKA_IMAGE
                + " (docker), packaged source plugin, fake Salesforce Pub/Sub API on the host");
        evidence.put("procedure", "publish event 1 (30k-character JSON payload) before the connector exists; "
                + "register the connector with sf.platform.events and no sf.sobjects; consume; publish event 2; "
                + "wait for the offset in the worker's offset store; restart the connector and its task via REST; "
                + "publish event 3; consume and drain for 5 seconds");
        ObjectNode redacted = evidence.putObject("connectorConfig");
        config.forEach((k, v) -> redacted.put(k, k.equals("sf.consumer.secret") ? "<redacted>" : v));
        ArrayNode records = evidence.putArray("delivered");
        for (ConsumerRecord<String, String> record : delivered) {
            JsonNode value = MAPPER.readTree(record.value());
            ObjectNode entry = records.addObject();
            entry.put("kafkaPartition", record.partition());
            entry.put("kafkaOffset", record.offset());
            entry.put("key", record.key());
            entry.put("Sequence__c", value.path("Sequence__c").asDouble());
            entry.put("ReplayId", value.path("ReplayId").asText());
            entry.put("EventUuid", value.path("EventUuid").asText());
            entry.put("_ObjectType", value.path("_ObjectType").asText());
            entry.put("_EventType", value.path("_EventType").asText());
            entry.put("payloadCharacters", value.path("Payload__c").asText().length());
            ArrayNode fields = entry.putArray("fields");
            value.fieldNames().forEachRemaining(fields::add);
        }
        evidence.set("workerOffsets", offsets);
        boolean pass = sequences.equals(List.of(1.0, 2.0, 3.0));
        evidence.put("verdict", pass ? "PASS: 3 published, 3 delivered, no duplicates across the task restart"
                : "FAIL: delivered sequences " + sequences);
        Path out = Path.of("target", "platform-event-e2e-evidence.json");
        Files.createDirectories(out.getParent());
        Files.writeString(out, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(evidence));
    }

    private static void registerConnector(String name, Map<String, String> config) throws Exception {
        String body = MAPPER.writeValueAsString(Map.of("name", name, "config", config));
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(URI.create(connectUrl + "/connectors"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode())
                .withFailMessage("Connector registration failed: %s", response.body())
                .isIn(200, 201);
    }

    private static void awaitConnectorRunning(String name) {
        await().atMost(Duration.ofMinutes(2)).untilAsserted(() -> {
            HttpResponse<String> response = http.send(
                    HttpRequest.newBuilder(URI.create(connectUrl + "/connectors/" + name + "/status")).build(),
                    HttpResponse.BodyHandlers.ofString());
            JsonNode status = MAPPER.readTree(response.body());
            assertThat(status.path("connector").path("state").asText()).isEqualTo("RUNNING");
            assertThat(status.path("tasks")).isNotEmpty();
            assertThat(status.path("tasks").get(0).path("state").asText())
                    .withFailMessage("task not running: %s", response.body())
                    .isEqualTo("RUNNING");
        });
    }

    private static KafkaConsumer<String, String> consumer(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "e2e-assert-" + topic);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    private static ConsumerRecord<String, String> awaitOneRecord(KafkaConsumer<String, String> consumer) {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (!records.isEmpty()) {
                return records.iterator().next();
            }
        }
        throw new AssertionError("No record arrived on the topic within the deadline");
    }
}
