package sh.oso.salesforce.schema;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Set;

/**
 * Builds a Kafka Connect schema from a Salesforce sObject describe result (for Bulk/REST paths).
 *
 * <p>Date/datetime policy: epoch-millis INT64 by default; fields listed in
 * {@code isoStringFields} stay ISO-8601 strings. Compound fields (address, location) are not
 * populated by Bulk queries — they are skipped; their component fields are already present.</p>
 */
public final class DescribeToConnect {

    /** Compound types Bulk 2.0 cannot query; components (e.g. BillingCity) carry the data. */
    private static final Set<String> COMPOUND_TYPES = Set.of("address", "location");

    private DescribeToConnect() {
    }

    public static Schema toConnectSchema(JsonNode describe, Set<String> isoStringFields) {
        String name = describe.path("name").asText();
        SchemaBuilder builder = SchemaBuilder.struct().name(name);
        for (JsonNode field : describe.path("fields")) {
            String fieldName = field.path("name").asText();
            String type = field.path("type").asText();
            if (COMPOUND_TYPES.contains(type)) {
                continue;
            }
            builder.field(fieldName, fieldSchema(field, isoStringFields.contains(fieldName)));
        }
        return builder.build();
    }

    /** All fields optional: Bulk CSV omits/blanks nullable values, and partial queries are common. */
    private static Schema fieldSchema(JsonNode field, boolean keepIsoString) {
        String type = field.path("type").asText();
        return switch (type) {
            case "boolean" -> Schema.OPTIONAL_BOOLEAN_SCHEMA;
            case "int" -> Schema.OPTIONAL_INT32_SCHEMA;
            case "long" -> Schema.OPTIONAL_INT64_SCHEMA;
            case "double", "currency", "percent" -> Schema.OPTIONAL_FLOAT64_SCHEMA;
            case "date", "datetime", "time" ->
                    keepIsoString ? Schema.OPTIONAL_STRING_SCHEMA : Schema.OPTIONAL_INT64_SCHEMA;
            case "base64" -> Schema.OPTIONAL_BYTES_SCHEMA;
            // id, reference, string, textarea, picklist, multipicklist, phone, url, email,
            // encryptedstring, combobox, anyType, json ...
            default -> Schema.OPTIONAL_STRING_SCHEMA;
        };
    }
}
