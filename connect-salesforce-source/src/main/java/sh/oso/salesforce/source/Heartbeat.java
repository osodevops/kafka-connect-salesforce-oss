package sh.oso.salesforce.source;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import sh.oso.salesforce.pubsub.ReplayIds;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Persists the replay position of an idle stream. Kafka Connect stores a source offset only
 * with a record, so a quiet object's stored position stays at its last event and expires after
 * Salesforce's 72-hour retention. When {@code sf.heartbeat.interval.ms} is set, a small record
 * on the heartbeat topic carries the position from Salesforce's keepalive as its source offset.
 */
final class Heartbeat {

    static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("sh.oso.salesforce.source.Heartbeat")
            .field("name", Schema.STRING_SCHEMA)
            .field("channel", Schema.STRING_SCHEMA)
            .field("replayId", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.INT64_SCHEMA)
            .build();

    private final String topic;
    private final long intervalMs;
    private final String name;
    private final String channel;
    private final Map<String, Object> partition;

    private byte[] persisted;   // position carried by the last record emitted for this stream
    private long lastEmittedAt;

    Heartbeat(SourceConfig config, String name, String channel, Map<String, Object> partition) {
        this.topic = config.heartbeatTopic();
        this.intervalMs = config.heartbeatIntervalMs();
        this.name = name;
        this.channel = channel;
        this.partition = partition;
    }

    /** A data record carried this position to Connect; no heartbeat is needed for it. */
    void persisted(byte[] position) {
        persisted = position;
    }

    /**
     * Returns a heartbeat record carrying {@code offset} when heartbeats are enabled, the idle
     * position has moved since the last record and the interval has elapsed; otherwise nothing.
     */
    List<SourceRecord> maybeEmit(byte[] idlePosition, Map<String, Object> offset) {
        if (intervalMs <= 0 || idlePosition == null || Arrays.equals(idlePosition, persisted)) {
            return List.of();
        }
        long now = System.currentTimeMillis();
        if (now - lastEmittedAt < intervalMs) {
            return List.of();
        }
        lastEmittedAt = now;
        persisted = idlePosition;
        Struct value = new Struct(VALUE_SCHEMA)
                .put("name", name)
                .put("channel", channel)
                .put("replayId", ReplayIds.toDisplayString(idlePosition))
                .put("ts_ms", now);
        return List.of(new SourceRecord(partition, offset, topic, null,
                Schema.STRING_SCHEMA, name, VALUE_SCHEMA, value));
    }
}
