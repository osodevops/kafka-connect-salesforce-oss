package sh.oso.salesforce.schema;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import sh.oso.salesforce.common.SalesforceException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps Avro schemas/values (as delivered by the Pub/Sub API) to Kafka Connect schemas/structs.
 * Salesforce event schemas use records, nullable unions, primitives, arrays (bitmap fields),
 * and occasional enums/fixed types.
 */
public final class AvroToConnect {

    private AvroToConnect() {
    }

    public static Schema toConnectSchema(org.apache.avro.Schema avro) {
        return toConnectSchema(avro, false);
    }

    private static Schema toConnectSchema(org.apache.avro.Schema avro, boolean optional) {
        return switch (avro.getType()) {
            case RECORD -> {
                SchemaBuilder builder = SchemaBuilder.struct().name(avro.getFullName());
                if (optional) {
                    builder.optional();
                }
                for (org.apache.avro.Schema.Field field : avro.getFields()) {
                    builder.field(field.name(), toConnectSchema(field.schema(), false));
                }
                yield builder.build();
            }
            case UNION -> unionToConnect(avro);
            case ARRAY -> {
                SchemaBuilder builder = SchemaBuilder.array(toConnectSchema(avro.getElementType(), false));
                if (optional) {
                    builder.optional();
                }
                yield builder.build();
            }
            case MAP -> {
                SchemaBuilder builder = SchemaBuilder.map(Schema.STRING_SCHEMA,
                        toConnectSchema(avro.getValueType(), false));
                if (optional) {
                    builder.optional();
                }
                yield builder.build();
            }
            case ENUM, STRING -> optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
            case BYTES, FIXED -> optional ? Schema.OPTIONAL_BYTES_SCHEMA : Schema.BYTES_SCHEMA;
            case INT -> optional ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA;
            case LONG -> optional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
            case FLOAT -> optional ? Schema.OPTIONAL_FLOAT32_SCHEMA : Schema.FLOAT32_SCHEMA;
            case DOUBLE -> optional ? Schema.OPTIONAL_FLOAT64_SCHEMA : Schema.FLOAT64_SCHEMA;
            case BOOLEAN -> optional ? Schema.OPTIONAL_BOOLEAN_SCHEMA : Schema.BOOLEAN_SCHEMA;
            case NULL -> throw new SalesforceException("Standalone Avro null type is not mappable");
        };
    }

    private static Schema unionToConnect(org.apache.avro.Schema union) {
        List<org.apache.avro.Schema> types = union.getTypes();
        List<org.apache.avro.Schema> nonNull = new ArrayList<>();
        for (org.apache.avro.Schema type : types) {
            if (type.getType() != org.apache.avro.Schema.Type.NULL) {
                nonNull.add(type);
            }
        }
        if (nonNull.size() == 1) {
            return toConnectSchema(nonNull.get(0), true);
        }
        // Multi-type unions (rare in SF events): fall back to optional string representation.
        return Schema.OPTIONAL_STRING_SCHEMA;
    }

    public static Object toConnectValue(Schema connectSchema, Object avroValue) {
        if (avroValue == null) {
            return null;
        }
        return switch (connectSchema.type()) {
            case STRUCT -> {
                GenericRecord record = (GenericRecord) avroValue;
                Struct struct = new Struct(connectSchema);
                for (org.apache.kafka.connect.data.Field field : connectSchema.fields()) {
                    org.apache.avro.Schema.Field avroField = record.getSchema().getField(field.name());
                    if (avroField != null) {
                        struct.put(field, toConnectValue(field.schema(), record.get(field.name())));
                    }
                }
                yield struct;
            }
            case ARRAY -> {
                List<Object> out = new ArrayList<>();
                for (Object element : (List<?>) avroValue) {
                    out.add(toConnectValue(connectSchema.valueSchema(), element));
                }
                yield out;
            }
            case MAP -> {
                Map<Object, Object> out = new HashMap<>();
                ((Map<?, ?>) avroValue).forEach((k, v) ->
                        out.put(k.toString(), toConnectValue(connectSchema.valueSchema(), v)));
                yield out;
            }
            case STRING -> avroValue instanceof Utf8 || avroValue instanceof GenericData.EnumSymbol
                    ? avroValue.toString() : avroValue;
            case BYTES -> avroValue instanceof ByteBuffer bb ? bb : ByteBuffer.wrap(toBytes(avroValue));
            default -> avroValue;
        };
    }

    private static byte[] toBytes(Object avroValue) {
        if (avroValue instanceof GenericData.Fixed fixed) {
            return fixed.bytes();
        }
        if (avroValue instanceof byte[] bytes) {
            return bytes;
        }
        throw new SalesforceException("Cannot convert " + avroValue.getClass() + " to bytes");
    }
}
