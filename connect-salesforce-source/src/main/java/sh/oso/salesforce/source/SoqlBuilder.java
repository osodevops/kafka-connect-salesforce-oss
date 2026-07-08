package sh.oso.salesforce.source;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Builds Bulk 2.0-safe SOQL from describe metadata. */
final class SoqlBuilder {

    /** Field types Bulk 2.0 query jobs cannot select. */
    private static final Set<String> UNQUERYABLE_TYPES = Set.of("address", "location", "base64");

    private SoqlBuilder() {
    }

    static List<String> queryableFields(JsonNode describe) {
        List<String> fields = new ArrayList<>();
        for (JsonNode field : describe.path("fields")) {
            if (!UNQUERYABLE_TYPES.contains(field.path("type").asText())) {
                fields.add(field.path("name").asText());
            }
        }
        return fields;
    }

    static String snapshotQuery(String sobject, JsonNode describe, String createdSince) {
        String soql = "SELECT " + String.join(", ", queryableFields(describe)) + " FROM " + sobject;
        if (createdSince != null && !createdSince.isBlank()) {
            soql += " WHERE CreatedDate >= " + normalizeDatetime(createdSince);
        }
        return soql;
    }

    static String pollingQuery(String sobject, JsonNode describe, String modstampAfter) {
        String soql = "SELECT " + String.join(", ", queryableFields(describe)) + " FROM " + sobject;
        if (modstampAfter != null) {
            soql += " WHERE SystemModstamp > " + modstampAfter;
        }
        return soql;
    }

    static String resyncQuery(String sobject, JsonNode describe, String modstampFrom) {
        return "SELECT " + String.join(", ", queryableFields(describe)) + " FROM " + sobject
                + " WHERE SystemModstamp >= " + modstampFrom;
    }

    /** SOQL datetime literals are unquoted ISO-8601; date-only input gets midnight UTC. */
    private static String normalizeDatetime(String value) {
        return value.length() == 10 ? value + "T00:00:00Z" : value;
    }
}
