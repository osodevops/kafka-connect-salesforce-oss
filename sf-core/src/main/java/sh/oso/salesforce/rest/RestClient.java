package sh.oso.salesforce.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.salesforce.common.SalesforceException;
import sh.oso.salesforce.http.RequestSpec;
import sh.oso.salesforce.http.SalesforceHttpClient;

import java.io.IOException;
import java.net.URLEncoder;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Salesforce REST API client: SOQL query with nextRecordsUrl paging, sObject describe with
 * If-Modified-Since caching, limits, sObject retrieve/create, and composite requests.
 */
public final class RestClient {

    private static final Logger LOG = LoggerFactory.getLogger(RestClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final SalesforceHttpClient http;
    private final String apiVersion;
    private final Map<String, CachedDescribe> describeCache = new ConcurrentHashMap<>();

    public RestClient(SalesforceHttpClient http, String apiVersion) {
        this.http = http;
        this.apiVersion = apiVersion;
    }

    public String apiVersion() {
        return apiVersion;
    }

    private String base() {
        return "/services/data/v" + apiVersion;
    }

    /** Runs a SOQL query, following nextRecordsUrl to exhaustion. */
    public List<JsonNode> queryAllPages(String soql, boolean includeDeleted) {
        List<JsonNode> records = new ArrayList<>();
        queryIterator(soql, includeDeleted).forEachRemaining(records::add);
        return records;
    }

    /** Lazily pages through query results. */
    public Iterator<JsonNode> queryIterator(String soql, boolean includeDeleted) {
        String resource = includeDeleted ? "/queryAll/" : "/query/";
        String first = base() + resource + "?q=" + URLEncoder.encode(soql, StandardCharsets.UTF_8);
        return new Iterator<>() {
            private String nextUrl = first;
            private Iterator<JsonNode> page = null;

            private void advance() {
                while ((page == null || !page.hasNext()) && nextUrl != null) {
                    JsonNode result = http.executeJson(RequestSpec.get(nextUrl));
                    JsonNode recs = result.path("records");
                    page = recs.isArray() ? recs.iterator() : java.util.Collections.emptyIterator();
                    nextUrl = result.path("done").asBoolean(true) ? null : result.path("nextRecordsUrl").asText(null);
                }
            }

            @Override
            public boolean hasNext() {
                advance();
                return page != null && page.hasNext();
            }

            @Override
            public JsonNode next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return page.next();
            }
        };
    }

    /** Describe with If-Modified-Since caching; returns cached copy on 304. */
    public JsonNode describe(String sobject) {
        CachedDescribe cached = describeCache.get(sobject);
        RequestSpec spec = RequestSpec.get(base() + "/sobjects/" + sobject + "/describe/");
        if (cached != null && cached.lastModified != null) {
            spec.header("If-Modified-Since", cached.lastModified);
        }
        HttpResponse<byte[]> response = http.executeRaw(spec);
        if (response.statusCode() == 304 && cached != null) {
            return cached.describe;
        }
        if (response.statusCode() >= 400) {
            throw new SalesforceException("Describe of " + sobject + " failed: HTTP " + response.statusCode()
                    + " " + new String(response.body(), StandardCharsets.UTF_8));
        }
        try {
            JsonNode describe = MAPPER.readTree(response.body());
            String lastModified = response.headers().firstValue("Last-Modified").orElse(null);
            describeCache.put(sobject, new CachedDescribe(describe, lastModified));
            LOG.debug("Refreshed describe for {}", sobject);
            return describe;
        } catch (IOException e) {
            throw new SalesforceException("Failed to parse describe response for " + sobject, e);
        }
    }

    public void evictDescribe(String sobject) {
        describeCache.remove(sobject);
    }

    /** GET /limits/ — live org quota. */
    public JsonNode limits() {
        return http.executeJson(RequestSpec.get(base() + "/limits/"));
    }

    /** Retrieves a record by Id; empty when the record does not exist (404). */
    public Optional<JsonNode> retrieve(String sobject, String id, List<String> fields) {
        String fieldParam = fields == null || fields.isEmpty()
                ? ""
                : "?fields=" + URLEncoder.encode(String.join(",", fields), StandardCharsets.UTF_8);
        HttpResponse<byte[]> response = http.executeRaw(
                RequestSpec.get(base() + "/sobjects/" + sobject + "/" + id + fieldParam));
        if (response.statusCode() == 404) {
            return Optional.empty();
        }
        if (response.statusCode() >= 400) {
            throw new SalesforceException("Retrieve " + sobject + "/" + id + " failed: HTTP " + response.statusCode());
        }
        try {
            return Optional.of(MAPPER.readTree(response.body()));
        } catch (IOException e) {
            throw new SalesforceException("Failed to parse retrieve response", e);
        }
    }

    /** POST /sobjects/<X>/ — returns the created record Id. */
    public String create(String sobject, ObjectNode fields) {
        JsonNode result = http.executeJson(
                RequestSpec.post(base() + "/sobjects/" + sobject + "/").jsonBody(fields.toString()));
        if (!result.path("success").asBoolean(false)) {
            throw new SalesforceException("Create " + sobject + " failed: " + result);
        }
        return result.path("id").asText();
    }

    /**
     * POST /composite/sobjects — up to 200 records per call.
     * Returns one result node per input record (success flag + errors array).
     */
    public List<JsonNode> compositeCreate(String sobject, List<ObjectNode> records, boolean allOrNone) {
        ObjectNode request = MAPPER.createObjectNode();
        request.put("allOrNone", allOrNone);
        ArrayNode arr = request.putArray("records");
        for (ObjectNode record : records) {
            ObjectNode copy = record.deepCopy();
            copy.putObject("attributes").put("type", sobject);
            arr.add(copy);
        }
        JsonNode result = http.executeJson(
                RequestSpec.post(base() + "/composite/sobjects").jsonBody(request.toString()));
        List<JsonNode> out = new ArrayList<>();
        result.forEach(out::add);
        return out;
    }

    /** PATCH /composite/sobjects — batch update; records must include Id. */
    public List<JsonNode> compositeUpdate(String sobject, List<ObjectNode> records, boolean allOrNone) {
        ObjectNode request = MAPPER.createObjectNode();
        request.put("allOrNone", allOrNone);
        ArrayNode arr = request.putArray("records");
        for (ObjectNode record : records) {
            ObjectNode copy = record.deepCopy();
            copy.putObject("attributes").put("type", sobject);
            arr.add(copy);
        }
        JsonNode result = http.executeJson(
                RequestSpec.patch(base() + "/composite/sobjects").jsonBody(request.toString()));
        List<JsonNode> out = new ArrayList<>();
        result.forEach(out::add);
        return out;
    }

    /** PATCH /composite/sobjects/<X>/<extIdField> — batch upsert by external ID. */
    public List<JsonNode> compositeUpsert(String sobject, String externalIdField, List<ObjectNode> records,
                                          boolean allOrNone) {
        ObjectNode request = MAPPER.createObjectNode();
        request.put("allOrNone", allOrNone);
        ArrayNode arr = request.putArray("records");
        for (ObjectNode record : records) {
            ObjectNode copy = record.deepCopy();
            copy.putObject("attributes").put("type", sobject);
            arr.add(copy);
        }
        JsonNode result = http.executeJson(
                RequestSpec.patch(base() + "/composite/sobjects/" + sobject + "/" + externalIdField)
                        .jsonBody(request.toString()));
        List<JsonNode> out = new ArrayList<>();
        result.forEach(out::add);
        return out;
    }

    /** DELETE /composite/sobjects?ids=... — batch delete (up to 200 ids). */
    public List<JsonNode> compositeDelete(List<String> ids, boolean allOrNone) {
        String path = base() + "/composite/sobjects?allOrNone=" + allOrNone
                + "&ids=" + URLEncoder.encode(String.join(",", ids), StandardCharsets.UTF_8);
        JsonNode result = http.executeJson(RequestSpec.delete(path));
        List<JsonNode> out = new ArrayList<>();
        result.forEach(out::add);
        return out;
    }

    private record CachedDescribe(JsonNode describe, String lastModified) {
    }
}
