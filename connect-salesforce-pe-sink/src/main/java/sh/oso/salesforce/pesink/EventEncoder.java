package sh.oso.salesforce.pesink;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import sh.oso.salesforce.pubsub.ChangeEventUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps Kafka record fields to the platform event's Avro schema by name, validating types,
 * and Avro-binary encodes the result. {@code CreatedDate}/{@code CreatedById} are filled
 * automatically when absent (required in Pub/Sub publish payloads).
 */
final class EventEncoder {

    private final Schema eventSchema;
    private final String createdById;

    EventEncoder(Schema eventSchema, String createdById) {
        this.eventSchema = eventSchema;
        this.createdById = createdById != null ? createdById : "005000000000000AAA";
    }

    byte[] encode(SinkRecord record) {
        Map<String, Object> fields = extract(record);
        GenericRecord event = new GenericData.Record(eventSchema);
        for (Schema.Field schemaField : eventSchema.getFields()) {
            String name = schemaField.name();
            Object value = fields.get(name);
            if (value == null) {
                switch (name) {
                    case "CreatedDate" -> event.put(name, Instant.now().toEpochMilli());
                    case "CreatedById" -> event.put(name, createdById);
                    default -> {
                        if (!isNullable(schemaField.schema())) {
                            throw new ConnectException("Record is missing required event field " + name);
                        }
                        event.put(name, null);
                    }
                }
                continue;
            }
            event.put(name, coerce(name, schemaField.schema(), value));
        }
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            new GenericDatumWriter<GenericRecord>(eventSchema).write(event, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new ConnectException("Failed to Avro-encode platform event", e);
        }
    }

    /** Validates and converts a record value against the event field's Avro type. */
    private Object coerce(String name, Schema fieldSchema, Object value) {
        Schema effective = ChangeEventUtils.unwrapNullable(fieldSchema);
        return switch (effective.getType()) {
            case STRING -> {
                if (value instanceof Number || value instanceof Boolean) {
                    yield value.toString();
                }
                yield requireType(name, value, String.class);
            }
            case BOOLEAN -> requireType(name, value, Boolean.class);
            case LONG -> {
                if (value instanceof Number n) {
                    yield n.longValue();
                }
                throw typeMismatch(name, value, "long");
            }
            case INT -> {
                if (value instanceof Number n) {
                    yield n.intValue();
                }
                throw typeMismatch(name, value, "int");
            }
            case DOUBLE -> {
                if (value instanceof Number n) {
                    yield n.doubleValue();
                }
                throw typeMismatch(name, value, "double");
            }
            case FLOAT -> {
                if (value instanceof Number n) {
                    yield n.floatValue();
                }
                throw typeMismatch(name, value, "float");
            }
            default -> throw new ConnectException("Platform event field " + name
                    + " has unsupported Avro type " + effective.getType());
        };
    }

    private static <T> T requireType(String name, Object value, Class<T> type) {
        if (!type.isInstance(value)) {
            throw typeMismatch(name, value, type.getSimpleName().toLowerCase());
        }
        return type.cast(value);
    }

    private static ConnectException typeMismatch(String name, Object value, String expected) {
        return new ConnectException("Field " + name + " expects " + expected + " but record carries "
                + value.getClass().getSimpleName() + " (" + value + ")");
    }

    private static boolean isNullable(Schema schema) {
        return schema.getType() == Schema.Type.UNION
                && schema.getTypes().stream().anyMatch(t -> t.getType() == Schema.Type.NULL);
    }

    static Map<String, Object> extract(SinkRecord record) {
        Object value = record.value();
        Map<String, Object> out = new LinkedHashMap<>();
        if (value instanceof Struct struct) {
            for (Field field : struct.schema().fields()) {
                out.put(field.name(), struct.get(field));
            }
            return out;
        }
        if (value instanceof Map<?, ?> map) {
            map.forEach((k, v) -> out.put(k.toString(), v));
            return out;
        }
        throw new ConnectException("Platform event sink requires Struct or Map record values; got "
                + (value == null ? "null (tombstone)" : value.getClass().getName()));
    }
}
