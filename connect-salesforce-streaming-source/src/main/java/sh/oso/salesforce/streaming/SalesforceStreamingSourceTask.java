package sh.oso.salesforce.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.salesforce.auth.SalesforceAuth;
import sh.oso.salesforce.auth.SessionSupplier;
import sh.oso.salesforce.http.SalesforceHttpClient;
import sh.oso.salesforce.rest.RestClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Legacy Streaming API source task: one CometD subscription (PushTopic, CDC channel, or
 * Platform Event) with durable replayId checkpointing in the Connect offset store.
 */
public class SalesforceStreamingSourceTask extends SourceTask {

    static final long REPLAY_LATEST = -1;
    static final long REPLAY_ALL = -2;

    private static final Logger LOG = LoggerFactory.getLogger(SalesforceStreamingSourceTask.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private StreamingConfig config;
    private RestClient rest;
    private StreamingClient client;
    private final BlockingQueue<Map<String, Object>> events = new LinkedBlockingQueue<>();
    private Map<String, Object> partition;
    private volatile long lastReplayId;

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        config = new StreamingConfig(props);
        SessionSupplier sessions = new SessionSupplier(
                new SalesforceAuth(config.authConfig(), config.getString(StreamingConfig.TOKEN_ENDPOINT)));
        rest = new RestClient(new SalesforceHttpClient(sessions), config.apiVersion());
        partition = Map.of("channel", config.bayeuxChannel());

        if (config.channelType() == StreamingConfig.ChannelType.PUSHTOPIC
                && config.getBoolean(StreamingConfig.PUSHTOPIC_CREATE)) {
            ensurePushTopic();
        }
        long replayFrom = resolveReplayStart();
        lastReplayId = replayFrom;
        String endpoint = config.getString(StreamingConfig.COMETD_ENDPOINT) != null
                ? config.getString(StreamingConfig.COMETD_ENDPOINT)
                : sessions.session().instanceUrl() + "/cometd/" + config.apiVersion() + "/";
        client = new StreamingClient(sessions, endpoint, config.bayeuxChannel(), replayFrom,
                config.getLong(StreamingConfig.CONNECTION_TIMEOUT_MS), events::add);
        client.connect();
        LOG.info("Streaming source connected to {} on {}", endpoint, config.bayeuxChannel());
    }

    private long resolveReplayStart() {
        Map<String, Object> stored = context.offsetStorageReader().offset(partition);
        if (stored != null && stored.get("replayId") instanceof Number replayId) {
            return replayId.longValue();
        }
        return config.eventStart() == StreamingConfig.EventStart.ALL ? REPLAY_ALL : REPLAY_LATEST;
    }

    private void ensurePushTopic() {
        String name = config.getString(StreamingConfig.PUSHTOPIC_NAME);
        List<JsonNode> existing = rest.queryAllPages(
                "SELECT Id FROM PushTopic WHERE Name = '" + name + "'", false);
        if (!existing.isEmpty()) {
            return;
        }
        String object = config.getString(StreamingConfig.OBJECT);
        JsonNode describe = rest.describe(object);
        List<String> fields = new ArrayList<>();
        for (JsonNode field : describe.path("fields")) {
            String type = field.path("type").asText();
            if (!"address".equals(type) && !"location".equals(type) && !"base64".equals(type)) {
                fields.add(field.path("name").asText());
            }
        }
        ObjectNode pushTopic = MAPPER.createObjectNode();
        pushTopic.put("Name", name);
        pushTopic.put("Query", "SELECT " + String.join(", ", fields) + " FROM " + object);
        pushTopic.put("ApiVersion", Double.parseDouble(config.apiVersion()));
        pushTopic.put("NotifyForOperationCreate", config.getBoolean(StreamingConfig.PUSHTOPIC_NOTIFY_CREATE));
        pushTopic.put("NotifyForOperationUpdate", config.getBoolean(StreamingConfig.PUSHTOPIC_NOTIFY_UPDATE));
        pushTopic.put("NotifyForOperationDelete", config.getBoolean(StreamingConfig.PUSHTOPIC_NOTIFY_DELETE));
        pushTopic.put("NotifyForOperationUndelete", config.getBoolean(StreamingConfig.PUSHTOPIC_NOTIFY_UNDELETE));
        pushTopic.put("NotifyForFields", "All");
        String id = rest.create("PushTopic", pushTopic);
        LOG.info("Auto-created PushTopic {} ({}) for {}", name, id, object);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (client.isReplayInvalid()) {
            long fallback = config.invalidReplayBehaviour() == StreamingConfig.InvalidReplayBehaviour.ALL
                    ? REPLAY_ALL : REPLAY_LATEST;
            LOG.warn("Stored replayId invalid/expired on {}; resubscribing with {}",
                    config.bayeuxChannel(), fallback == REPLAY_ALL ? "ALL (-2)" : "LATEST (-1)");
            client.resubscribeFrom(fallback);
        }
        Map<String, Object> first = events.poll(500, TimeUnit.MILLISECONDS);
        if (first == null) {
            return null;
        }
        List<Map<String, Object>> batch = new ArrayList<>();
        batch.add(first);
        events.drainTo(batch);
        List<SourceRecord> out = new ArrayList<>();
        for (Map<String, Object> event : batch) {
            SourceRecord record = toRecord(event);
            if (record != null) {
                out.add(record);
            }
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private SourceRecord toRecord(Map<String, Object> data) {
        Map<String, Object> eventMeta = (Map<String, Object>) data.getOrDefault("event", Map.of());
        long replayId = eventMeta.get("replayId") instanceof Number n ? n.longValue() : lastReplayId;
        lastReplayId = replayId;
        client.updateReplayId(replayId);

        Map<String, Object> value = new HashMap<>();
        String eventType;
        String objectType;
        String key = null;
        switch (config.channelType()) {
            case PUSHTOPIC -> {
                Map<String, Object> sobject = (Map<String, Object>) data.getOrDefault("sobject", Map.of());
                value.putAll(sobject);
                eventType = String.valueOf(eventMeta.getOrDefault("type", "created"));
                objectType = config.getString(StreamingConfig.OBJECT) != null
                        ? config.getString(StreamingConfig.OBJECT)
                        : String.valueOf(sobject.getOrDefault("Type", "unknown"));
                key = sobject.get("Id") != null ? sobject.get("Id").toString() : null;
            }
            case CDC -> {
                Map<String, Object> payload = (Map<String, Object>) data.getOrDefault("payload", Map.of());
                Map<String, Object> header =
                        (Map<String, Object>) payload.getOrDefault("ChangeEventHeader", Map.of());
                objectType = String.valueOf(header.getOrDefault("entityName", "unknown"));
                List<String> entities = config.getList(StreamingConfig.CHANNEL_ENTITIES);
                if (!entities.isEmpty() && !entities.contains(objectType)) {
                    return null;
                }
                eventType = changeTypeToEventType(String.valueOf(header.getOrDefault("changeType", "CREATE")));
                payload.forEach((k, v) -> {
                    if (!"ChangeEventHeader".equals(k)) {
                        value.put(k, v);
                    }
                });
                List<Object> recordIds = (List<Object>) header.getOrDefault("recordIds", List.of());
                if (!recordIds.isEmpty()) {
                    key = recordIds.get(0).toString();
                    value.put("Id", key);
                }
            }
            default -> { // PLATFORM_EVENT
                Map<String, Object> payload = (Map<String, Object>) data.getOrDefault("payload", Map.of());
                value.putAll(payload);
                eventType = "created";
                objectType = config.getString(StreamingConfig.PLATFORM_EVENT_NAME);
            }
        }
        value.put("_EventType", eventType);
        value.put("_ObjectType", objectType);
        String topic = config.getString(StreamingConfig.KAFKA_TOPIC)
                .replace("${_EventType}", eventType)
                .replace("${_ObjectType}", objectType);
        Map<String, Object> offset = Map.of("replayId", replayId);
        return new SourceRecord(partition, offset, topic, null,
                key == null ? null : org.apache.kafka.connect.data.Schema.STRING_SCHEMA, key,
                null, value);
    }

    private static String changeTypeToEventType(String changeType) {
        return switch (changeType.toUpperCase(Locale.ROOT)) {
            case "CREATE", "UNDELETE" -> "created";
            case "UPDATE" -> "updated";
            case "DELETE" -> "deleted";
            default -> changeType.toLowerCase(Locale.ROOT);
        };
    }

    @Override
    public void stop() {
        if (client != null) {
            client.close();
        }
    }
}
