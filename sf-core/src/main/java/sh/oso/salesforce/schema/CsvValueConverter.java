package sh.oso.salesforce.schema;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import sh.oso.salesforce.common.SalesforceException;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.Map;

/**
 * Converts Bulk 2.0 CSV rows (all strings) into Connect Structs against a describe-derived schema.
 * Datetime fields become epoch millis when the schema says INT64; ISO strings pass through.
 */
public final class CsvValueConverter {

    private static final DateTimeFormatter SF_DATETIME = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private CsvValueConverter() {
    }

    public static Struct toStruct(Schema schema, Map<String, String> csvRow) {
        Struct struct = new Struct(schema);
        for (Map.Entry<String, String> entry : csvRow.entrySet()) {
            Field field = schema.field(entry.getKey());
            if (field == null) {
                continue;
            }
            struct.put(field, convert(field.schema(), entry.getKey(), entry.getValue()));
        }
        return struct;
    }

    public static Object convert(Schema fieldSchema, String fieldName, String raw) {
        if (raw == null || raw.isEmpty()) {
            return null;
        }
        try {
            return switch (fieldSchema.type()) {
                case BOOLEAN -> Boolean.parseBoolean(raw);
                case INT32 -> (int) Double.parseDouble(raw);
                case INT64 -> looksTemporal(raw) ? parseTemporalToEpochMillis(raw) : Long.parseLong(raw);
                case FLOAT64 -> Double.parseDouble(raw);
                case BYTES -> Base64.getDecoder().decode(raw);
                default -> raw;
            };
        } catch (DateTimeParseException | IllegalArgumentException e) {
            throw new SalesforceException("Cannot convert field " + fieldName + " value '" + raw
                    + "' to " + fieldSchema.type(), e);
        }
    }

    private static boolean looksTemporal(String raw) {
        // Salesforce date/datetime/time CSV values: 2024-01-31, 2024-01-31T09:30:00.000+0000, 09:30:00.000Z
        return raw.length() >= 8 && (raw.charAt(4) == '-' || raw.charAt(2) == ':');
    }

    static long parseTemporalToEpochMillis(String raw) {
        if (raw.charAt(2) == ':') {
            // time-only field: millis since midnight UTC
            return LocalTime.parse(raw.replace("Z", "")).toNanoOfDay() / 1_000_000;
        }
        if (raw.length() == 10) {
            return LocalDate.parse(raw).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        }
        // Normalize +0000 to +00:00 for ISO parsing
        String normalized = raw.replaceAll("([+-]\\d{2})(\\d{2})$", "$1:$2");
        return Instant.from(SF_DATETIME.parse(normalized)).toEpochMilli();
    }
}
