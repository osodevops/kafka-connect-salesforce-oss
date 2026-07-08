package sh.oso.salesforce.pesink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.salesforce.eventbus.protobuf.ProducerEvent;
import com.salesforce.eventbus.protobuf.PublishResponse;
import com.salesforce.eventbus.protobuf.PublishResult;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.salesforce.auth.SalesforceAuth;
import sh.oso.salesforce.auth.SessionSupplier;
import sh.oso.salesforce.http.SalesforceHttpClient;
import sh.oso.salesforce.pubsub.PubSubClient;
import sh.oso.salesforce.rest.RestClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Publishes Kafka records as Salesforce Platform Events via Pub/Sub (with REST fallback). */
public class SalesforcePlatformEventSinkTask extends SinkTask {

    private static final Logger LOG = LoggerFactory.getLogger(SalesforcePlatformEventSinkTask.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private PeSinkConfig config;
    private PubSubClient pubsub;
    private RestClient rest;
    private EventEncoder encoder;
    private ErrantRecordReporter reporter;

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        config = new PeSinkConfig(props);
        SessionSupplier sessions = new SessionSupplier(
                new SalesforceAuth(config.authConfig(), config.getString(PeSinkConfig.TOKEN_ENDPOINT)));
        if (config.publishMode() == PeSinkConfig.PublishMode.PUBSUB) {
            pubsub = new PubSubClient(sessions, config.getString(PeSinkConfig.PUBSUB_ENDPOINT),
                    !config.getBoolean(PeSinkConfig.PUBSUB_PLAINTEXT));
            String schemaId = pubsub.getTopic(config.eventTopic()).getSchemaId();
            encoder = new EventEncoder(pubsub.getSchema(schemaId), null);
        } else {
            rest = new RestClient(new SalesforceHttpClient(sessions), config.apiVersion());
        }
        try {
            reporter = context.errantRecordReporter();
        } catch (NoSuchMethodError | NoClassDefFoundError e) {
            reporter = null;
        }
        LOG.info("Platform Event sink started for {} via {}", config.eventName(), config.publishMode());
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        List<Failure> failures = config.publishMode() == PeSinkConfig.PublishMode.PUBSUB
                ? publishPubSub(records)
                : publishRest(records);
        handleFailures(failures);
    }

    private record Failure(SinkRecord record, String reason) {
    }

    private List<Failure> publishPubSub(Collection<SinkRecord> records) {
        List<Failure> failures = new ArrayList<>();
        List<ProducerEvent> events = new ArrayList<>();
        List<SinkRecord> encoded = new ArrayList<>();
        String schemaId = pubsub.getTopic(config.eventTopic()).getSchemaId();
        for (SinkRecord record : records) {
            try {
                events.add(ProducerEvent.newBuilder()
                        .setId(UUID.randomUUID().toString())
                        .setSchemaId(schemaId)
                        .setPayload(ByteString.copyFrom(encoder.encode(record)))
                        .build());
                encoded.add(record);
            } catch (ConnectException e) {
                failures.add(new Failure(record, e.getMessage()));
            }
        }
        if (events.isEmpty()) {
            return failures;
        }
        PublishResponse response = pubsub.publish(config.eventTopic(), events);
        List<PublishResult> results = response.getResultsList();
        for (int i = 0; i < results.size() && i < encoded.size(); i++) {
            if (results.get(i).hasError()) {
                failures.add(new Failure(encoded.get(i), results.get(i).getError().getMsg()));
            }
        }
        return failures;
    }

    private List<Failure> publishRest(Collection<SinkRecord> records) {
        List<Failure> failures = new ArrayList<>();
        List<ObjectNode> payload = new ArrayList<>();
        List<SinkRecord> mapped = new ArrayList<>();
        for (SinkRecord record : records) {
            try {
                ObjectNode node = MAPPER.createObjectNode();
                EventEncoder.extract(record).forEach((k, v) -> node.putPOJO(k, v));
                payload.add(node);
                mapped.add(record);
            } catch (ConnectException e) {
                failures.add(new Failure(record, e.getMessage()));
            }
        }
        if (payload.isEmpty()) {
            return failures;
        }
        List<JsonNode> results = rest.compositeCreate(config.eventName(), payload, false);
        for (int i = 0; i < results.size() && i < mapped.size(); i++) {
            if (!results.get(i).path("success").asBoolean(false)) {
                failures.add(new Failure(mapped.get(i), results.get(i).path("errors").toString()));
            }
        }
        return failures;
    }

    private void handleFailures(List<Failure> failures) {
        if (failures.isEmpty()) {
            return;
        }
        for (Failure failure : failures) {
            if (reporter != null) {
                reporter.report(failure.record(), new ConnectException(failure.reason()));
            }
            if (config.errorBehavior() == PeSinkConfig.ErrorBehavior.LOG) {
                LOG.warn("Platform event publish failed: {}", failure.reason());
            }
        }
        if (config.errorBehavior() == PeSinkConfig.ErrorBehavior.FAIL) {
            throw new ConnectException(failures.size() + " platform event(s) failed to publish; first: "
                    + failures.get(0).reason());
        }
    }

    @Override
    public void stop() {
        if (pubsub != null) {
            pubsub.close();
        }
    }
}
