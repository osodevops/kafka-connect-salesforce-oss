package sh.oso.salesforce.source;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import sh.oso.salesforce.pubsub.ChangeEventUtils;
import sh.oso.salesforce.pubsub.DecodedEvent;
import sh.oso.salesforce.schema.AvroToConnect;
import sh.oso.salesforce.schema.CsvValueConverter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Builds Kafka Connect SourceRecords from CDC events (Pub/Sub) and Bulk CSV rows.
 * Values carry {@code _ObjectType}/{@code _EventType} fields for Confluent-consumer
 * compatibility; CDC metadata rides in record headers ({@code sf.*}).
 */
final class SourceRecordFactory {

    static final String EVENT_TYPE_FIELD = "_EventType";
    static final String OBJECT_TYPE_FIELD = "_ObjectType";

    private final String sobject;
    private final String topic;
    private final Map<String, Object> partition;
    /** Value schema per Avro/describe schema identity, so schema objects are stable across records. */
    private final Map<Object, Schema> schemaCache = new ConcurrentHashMap<>();

    SourceRecordFactory(String sobject, String topic) {
        this.sobject = sobject;
        this.topic = topic;
        this.partition = SObjectOffset.partition(sobject);
    }

    /** CDC event → SourceRecord. Returns the data record (never the tombstone; see tombstone()). */
    SourceRecord fromChangeEvent(DecodedEvent event, GenericRecord header, String changeType,
                                 String recordId, Map<String, Object> offset) {
        Schema valueSchema = schemaCache.computeIfAbsent(event.payload().getSchema(),
                s -> buildEventValueSchema(event.payload().getSchema()));
        Struct value = new Struct(valueSchema);
        for (Field field : valueSchema.fields()) {
            switch (field.name()) {
                case EVENT_TYPE_FIELD -> value.put(field, eventTypeFor(changeType));
                case OBJECT_TYPE_FIELD -> value.put(field, sobject);
                case "Id" -> value.put(field, recordId);
                default -> {
                    org.apache.avro.Schema.Field avroField =
                            event.payload().getSchema().getField(field.name());
                    if (avroField != null) {
                        value.put(field, AvroToConnect.toConnectValue(
                                field.schema(), event.payload().get(field.name())));
                    }
                }
            }
        }
        ConnectHeaders headers = cdcHeaders(event, header, changeType);
        return new SourceRecord(partition, offset, topic, null,
                Schema.STRING_SCHEMA, recordId, valueSchema, value, null, headers);
    }

    /** Tombstone record (null value) for DELETE events. */
    SourceRecord tombstone(String recordId, Map<String, Object> offset) {
        return new SourceRecord(partition, offset, topic, null,
                Schema.STRING_SCHEMA, recordId, null, null);
    }

    /** Bulk CSV row (snapshot/polling) → SourceRecord. */
    SourceRecord fromBulkRow(Schema describeSchema, Map<String, String> row, String eventType,
                             Map<String, Object> offset) {
        Schema valueSchema = schemaCache.computeIfAbsent(describeSchema,
                s -> buildBulkValueSchema(describeSchema));
        Struct value = new Struct(valueSchema);
        for (Map.Entry<String, String> entry : row.entrySet()) {
            Field field = valueSchema.field(entry.getKey());
            if (field != null) {
                value.put(field, CsvValueConverter.convert(field.schema(), entry.getKey(), entry.getValue()));
            }
        }
        value.put(EVENT_TYPE_FIELD, eventType);
        value.put(OBJECT_TYPE_FIELD, sobject);
        String recordId = row.get("Id");
        return new SourceRecord(partition, offset, topic, null,
                Schema.STRING_SCHEMA, recordId, valueSchema, value);
    }

    private Schema buildEventValueSchema(org.apache.avro.Schema avroSchema) {
        SchemaBuilder builder = SchemaBuilder.struct().name(sobject);
        builder.field("Id", Schema.STRING_SCHEMA);
        for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
            if (ChangeEventUtils.HEADER_FIELD.equals(field.name())) {
                continue;
            }
            Schema fieldSchema = AvroToConnect.toConnectSchema(nullable(field.schema()));
            builder.field(field.name(), fieldSchema);
        }
        builder.field(EVENT_TYPE_FIELD, Schema.OPTIONAL_STRING_SCHEMA);
        builder.field(OBJECT_TYPE_FIELD, Schema.OPTIONAL_STRING_SCHEMA);
        return builder.build();
    }

    private Schema buildBulkValueSchema(Schema describeSchema) {
        SchemaBuilder builder = SchemaBuilder.struct().name(sobject);
        for (Field field : describeSchema.fields()) {
            builder.field(field.name(), field.schema());
        }
        builder.field(EVENT_TYPE_FIELD, Schema.OPTIONAL_STRING_SCHEMA);
        builder.field(OBJECT_TYPE_FIELD, Schema.OPTIONAL_STRING_SCHEMA);
        return builder.build();
    }

    /** CDC events omit unchanged fields; every value field must be optional. */
    private static org.apache.avro.Schema nullable(org.apache.avro.Schema schema) {
        if (schema.getType() == org.apache.avro.Schema.Type.UNION) {
            return schema;
        }
        return org.apache.avro.Schema.createUnion(
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL), schema);
    }

    private ConnectHeaders cdcHeaders(DecodedEvent event, GenericRecord header, String changeType) {
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("sf.change.type", changeType);
        headers.addString("sf.entity", string(header.get("entityName")));
        headers.addLong("sf.commit.timestamp", (Long) header.get("commitTimestamp"));
        headers.addLong("sf.commit.number", (Long) header.get("commitNumber"));
        headers.addString("sf.transaction.key", string(header.get("transactionKey")));
        List<String> changed = ChangeEventUtils.expandBitmapFields(
                event.payload().getSchema(), (List<?>) header.get("changedFields"));
        List<String> nulled = ChangeEventUtils.expandBitmapFields(
                event.payload().getSchema(), (List<?>) header.get("nulledFields"));
        headers.addString("sf.changed.fields", String.join(",", changed));
        headers.addString("sf.nulled.fields", String.join(",", nulled));
        return headers;
    }

    static String eventTypeFor(String changeType) {
        return switch (changeType) {
            case "CREATE", "UNDELETE" -> "created";
            case "UPDATE" -> "updated";
            case "DELETE" -> "deleted";
            default -> changeType.toLowerCase();
        };
    }

    private static String string(Object value) {
        return value == null ? null : value.toString();
    }
}
