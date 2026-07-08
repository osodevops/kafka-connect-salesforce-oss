package sh.oso.salesforce.sink;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import sh.oso.salesforce.bulk.BulkIngestClient;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Converts SinkRecords into Salesforce field maps: resolves the operation from
 * {@code _EventType} (unless overridden), strips metadata/ignored/read-only fields,
 * and renders datetime epoch values as ISO-8601 for the wire.
 */
final class RecordMapper {

    private static final Set<String> METADATA_FIELDS = Set.of("_EventType", "_ObjectType");
    private static final DateTimeFormatter SF_DATETIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(java.time.ZoneOffset.UTC);

    private final SinkConfig.ObjectSpec spec;
    private final Set<String> createable = new HashSet<>();
    private final Set<String> updateable = new HashSet<>();
    private final Map<String, String> fieldTypes = new HashMap<>();

    RecordMapper(SinkConfig.ObjectSpec spec, JsonNode describe) {
        this.spec = spec;
        for (JsonNode field : describe.path("fields")) {
            String name = field.path("name").asText();
            fieldTypes.put(name, field.path("type").asText());
            if (field.path("createable").asBoolean(false)) {
                createable.add(name);
            }
            if (field.path("updateable").asBoolean(false)) {
                updateable.add(name);
            }
        }
    }

    /** Resolves the operation for a record from _EventType or the configured override. */
    BulkIngestClient.Operation operationFor(SinkRecord record) {
        if (spec.overrideEventType()) {
            return spec.operation();
        }
        Object eventType = extractField(record, "_EventType");
        if (eventType == null) {
            return spec.operation();
        }
        return switch (eventType.toString().toLowerCase()) {
            case "created" -> spec.useCustomIdField() ? BulkIngestClient.Operation.UPSERT
                    : BulkIngestClient.Operation.INSERT;
            case "updated" -> spec.useCustomIdField() ? BulkIngestClient.Operation.UPSERT
                    : BulkIngestClient.Operation.UPDATE;
            case "deleted" -> BulkIngestClient.Operation.DELETE;
            default -> spec.operation();
        };
    }

    /**
     * Field map for the write. Read-only Salesforce fields are silently excluded
     * (matching Salesforce describe createable/updateable metadata), as are
     * {@code _EventType}/{@code _ObjectType}, configured ignore fields, and nulls.
     */
    Map<String, Object> toFieldMap(SinkRecord record, BulkIngestClient.Operation operation) {
        Map<String, Object> raw = extractAll(record);
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : raw.entrySet()) {
            String name = entry.getKey();
            if (METADATA_FIELDS.contains(name) || spec.ignoreFields().contains(name)
                    || entry.getValue() == null) {
                continue;
            }
            if (!isWritable(name, operation)) {
                continue;
            }
            out.put(name, wireValue(name, entry.getValue()));
        }
        if (operation == BulkIngestClient.Operation.DELETE) {
            Object id = deleteId(record, raw);
            out.clear();
            if (spec.useCustomIdField()) {
                out.put(spec.customIdFieldName(), raw.get(spec.customIdFieldName()));
            } else {
                out.put("Id", id);
            }
            return out;
        }
        if (operation == BulkIngestClient.Operation.UPSERT && spec.useCustomIdField()) {
            Object extId = raw.get(spec.customIdFieldName());
            if (extId == null) {
                throw new ConnectException("Record has no value for external ID field "
                        + spec.customIdFieldName());
            }
            out.put(spec.customIdFieldName(), extId);
        }
        return out;
    }

    private boolean isWritable(String name, BulkIngestClient.Operation operation) {
        if (name.equals(spec.customIdFieldName())) {
            return true;
        }
        if ("Id".equals(name)) {
            return operation == BulkIngestClient.Operation.UPDATE
                    || operation == BulkIngestClient.Operation.DELETE;
        }
        if (fieldTypes.isEmpty()) {
            return true; // no describe available: pass through
        }
        if (!fieldTypes.containsKey(name)) {
            return false; // unknown field: would fail the whole batch
        }
        return switch (operation) {
            case INSERT -> createable.contains(name);
            case UPDATE -> updateable.contains(name);
            case UPSERT -> createable.contains(name) || updateable.contains(name);
            case DELETE, HARD_DELETE -> true;
        };
    }

    private Object wireValue(String name, Object value) {
        String type = fieldTypes.get(name);
        if (value instanceof Long epoch && type != null) {
            switch (type) {
                case "datetime" -> {
                    return SF_DATETIME.format(Instant.ofEpochMilli(epoch));
                }
                case "date" -> {
                    return DateTimeFormatter.ISO_LOCAL_DATE.withZone(java.time.ZoneOffset.UTC)
                            .format(Instant.ofEpochMilli(epoch));
                }
                default -> {
                    return value;
                }
            }
        }
        return value;
    }

    private Object deleteId(SinkRecord record, Map<String, Object> raw) {
        Object id = raw.get("Id");
        if (id == null && record.key() != null) {
            id = record.key().toString();
        }
        if (id == null && !spec.useCustomIdField()) {
            throw new ConnectException("DELETE record carries no Id field or record key");
        }
        return id;
    }

    static Object extractField(SinkRecord record, String field) {
        Object value = record.value();
        if (value instanceof Struct struct) {
            Field f = struct.schema().field(field);
            return f == null ? null : struct.get(f);
        }
        if (value instanceof Map<?, ?> map) {
            return map.get(field);
        }
        return null;
    }

    static Map<String, Object> extractAll(SinkRecord record) {
        Object value = record.value();
        Map<String, Object> out = new LinkedHashMap<>();
        if (value instanceof Struct struct) {
            for (Field field : struct.schema().fields()) {
                if (field.schema().type() == Schema.Type.STRUCT) {
                    continue; // nested structures are not writable sObject fields
                }
                out.put(field.name(), struct.get(field));
            }
            return out;
        }
        if (value instanceof Map<?, ?> map) {
            map.forEach((k, v) -> {
                if (!(v instanceof Map) && !(v instanceof Struct)) {
                    out.put(k.toString(), v);
                }
            });
            return out;
        }
        throw new ConnectException("Sink requires Struct or Map record values; got "
                + (value == null ? "null (tombstone)" : value.getClass().getName()));
    }
}
