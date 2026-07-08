package sh.oso.salesforce.pubsub;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Helpers for Change Data Capture events: extracting the ChangeEventHeader and expanding
 * the hex field bitmaps ({@code changedFields}/{@code nulledFields}/{@code diffFields})
 * into field names against the event's Avro schema.
 */
public final class ChangeEventUtils {

    public static final String HEADER_FIELD = "ChangeEventHeader";

    private ChangeEventUtils() {
    }

    /** Returns the ChangeEventHeader record when the event is a CDC event. */
    public static Optional<GenericRecord> changeEventHeader(GenericRecord event) {
        Schema.Field headerField = event.getSchema().getField(HEADER_FIELD);
        if (headerField == null) {
            return Optional.empty();
        }
        Object header = event.get(HEADER_FIELD);
        return header instanceof GenericRecord record ? Optional.of(record) : Optional.empty();
    }

    /**
     * Expands CDC bitmap entries into field names. Entry formats:
     * <ul>
     *   <li>{@code "0x440000A0"} — bitmap over the event's top-level fields</li>
     *   <li>{@code "5-0x08"} — bitmap over the nested (compound) fields of top-level field 5</li>
     * </ul>
     * Bit {@code i} set means the field at position {@code i} is affected.
     */
    public static List<String> expandBitmapFields(Schema eventSchema, List<?> bitmapEntries) {
        List<String> out = new ArrayList<>();
        if (bitmapEntries == null) {
            return out;
        }
        for (Object entryObj : bitmapEntries) {
            String entry = entryObj.toString();
            int dash = entry.indexOf('-');
            if (dash < 0) {
                appendFields(eventSchema, entry, null, out);
            } else {
                int parentIndex = Integer.parseInt(entry.substring(0, dash));
                Schema.Field parent = eventSchema.getFields().get(parentIndex);
                Schema nested = unwrapNullable(parent.schema());
                appendFields(nested, entry.substring(dash + 1), parent.name(), out);
            }
        }
        return out;
    }

    private static void appendFields(Schema recordSchema, String hexBitmap, String prefix, List<String> out) {
        BigInteger bits = new BigInteger(hexBitmap.replaceFirst("^0x", ""), 16);
        List<Schema.Field> fields = recordSchema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (bits.testBit(i)) {
                String name = fields.get(i).name();
                out.add(prefix == null ? name : prefix + "." + name);
            }
        }
    }

    public static Schema unwrapNullable(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema type : schema.getTypes()) {
                if (type.getType() != Schema.Type.NULL) {
                    return type;
                }
            }
        }
        return schema;
    }
}
