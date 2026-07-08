package sh.oso.salesforce.bulk;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import sh.oso.salesforce.common.SalesforceException;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Builds Bulk API 2.0 ingest CSV payloads (LF line endings, header row from union of keys). */
public final class CsvEncoder {

    private CsvEncoder() {
    }

    /**
     * Encodes records into a single CSV document. Null values are written as the Salesforce
     * {@code #N/A} sentinel when {@code nullsAsSetNull} is true (explicitly clears the field),
     * otherwise as empty (field left unchanged on update).
     */
    public static byte[] encode(List<Map<String, Object>> records, boolean nullsAsSetNull) {
        if (records.isEmpty()) {
            return new byte[0];
        }
        Set<String> columns = new LinkedHashSet<>();
        for (Map<String, Object> record : records) {
            columns.addAll(record.keySet());
        }
        try (StringWriter out = new StringWriter();
             CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT.builder()
                     .setHeader(columns.toArray(new String[0]))
                     .setRecordSeparator('\n')
                     .build())) {
            for (Map<String, Object> record : records) {
                for (String column : columns) {
                    Object value = record.get(column);
                    if (value == null) {
                        printer.print(record.containsKey(column) && nullsAsSetNull ? "#N/A" : "");
                    } else {
                        printer.print(value);
                    }
                }
                printer.println();
            }
            printer.flush();
            return out.toString().getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new SalesforceException("Failed to encode Bulk ingest CSV", e);
        }
    }
}
