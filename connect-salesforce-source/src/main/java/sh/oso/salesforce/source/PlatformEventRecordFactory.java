package sh.oso.salesforce.source;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import sh.oso.salesforce.pubsub.DecodedEvent;
import sh.oso.salesforce.pubsub.ReplayIds;
import sh.oso.salesforce.schema.AvroToConnect;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Builds SourceRecords from platform events. The value carries every field of the event as
 * published, plus {@code ReplayId} (decimal), {@code EventUuid}, {@code _ObjectType} (the event
 * name) and {@code _EventType} (the Pub/Sub schema ID), the shape consumers of Confluent's
 * Platform Event source expect. The key is null: platform events are not records.
 */
final class PlatformEventRecordFactory {

    static final String REPLAY_ID_FIELD = "ReplayId";
    static final String EVENT_UUID_FIELD = "EventUuid";

    private final String eventName;
    private final String topic;
    private final Map<String, Object> partition;
    /** Value schema per Pub/Sub schema ID: a changed event definition gets a new schema. */
    private final Map<String, Schema> schemaCache = new ConcurrentHashMap<>();

    PlatformEventRecordFactory(String eventName, String topic) {
        this.eventName = eventName;
        this.topic = topic;
        this.partition = PlatformEventOffset.partition(eventName);
    }

    SourceRecord fromEvent(DecodedEvent event, Map<String, Object> offset) {
        org.apache.avro.Schema avroSchema = event.payload().getSchema();
        Schema valueSchema = schemaCache.computeIfAbsent(event.schemaId(), id -> buildValueSchema(avroSchema));
        Struct value = new Struct(valueSchema);
        for (Field field : valueSchema.fields()) {
            if (avroSchema.getField(field.name()) != null) {
                value.put(field, AvroToConnect.toConnectValue(field.schema(), event.payload().get(field.name())));
            }
        }
        if (avroSchema.getField(REPLAY_ID_FIELD) == null) {
            value.put(REPLAY_ID_FIELD, ReplayIds.toDisplayString(event.replayId()));
        }
        if (avroSchema.getField(EVENT_UUID_FIELD) == null) {
            value.put(EVENT_UUID_FIELD, event.eventId());
        }
        value.put(SourceRecordFactory.OBJECT_TYPE_FIELD, eventName);
        value.put(SourceRecordFactory.EVENT_TYPE_FIELD, event.schemaId());
        return new SourceRecord(partition, offset, topic, null, null, null, valueSchema, value);
    }

    private Schema buildValueSchema(org.apache.avro.Schema avroSchema) {
        // Same name across schema versions, so Schema Registry sees an evolution, not a new subject.
        SchemaBuilder builder = SchemaBuilder.struct().name(eventName);
        for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
            builder.field(field.name(), AvroToConnect.toOptionalConnectSchema(field.schema()));
        }
        if (avroSchema.getField(REPLAY_ID_FIELD) == null) {
            builder.field(REPLAY_ID_FIELD, Schema.OPTIONAL_STRING_SCHEMA);
        }
        if (avroSchema.getField(EVENT_UUID_FIELD) == null) {
            builder.field(EVENT_UUID_FIELD, Schema.OPTIONAL_STRING_SCHEMA);
        }
        builder.field(SourceRecordFactory.OBJECT_TYPE_FIELD, Schema.OPTIONAL_STRING_SCHEMA);
        builder.field(SourceRecordFactory.EVENT_TYPE_FIELD, Schema.OPTIONAL_STRING_SCHEMA);
        return builder.build();
    }
}
