package sh.oso.salesforce.bulk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.salesforce.common.SalesforceException;
import sh.oso.salesforce.http.RequestSpec;
import sh.oso.salesforce.http.SalesforceHttpClient;

import java.io.IOException;
import java.io.StringReader;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Locale;

/**
 * Bulk API 2.0 query-job client: create job, poll to JobComplete, page results by locator
 * until {@code Sforce-Locator: null}. The same API version must create and read a job.
 */
public final class BulkQueryClient {

    private static final Logger LOG = LoggerFactory.getLogger(BulkQueryClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** SOQL clauses Bulk 2.0 query jobs reject. */
    private static final List<String> FORBIDDEN_CLAUSES =
            List.of("GROUP BY", "OFFSET", "TYPEOF", "ORDER BY");

    private final SalesforceHttpClient http;
    private final String apiVersion;

    public BulkQueryClient(SalesforceHttpClient http, String apiVersion) {
        this.http = http;
        this.apiVersion = apiVersion;
    }

    private String base() {
        return "/services/data/v" + apiVersion + "/jobs/query";
    }

    /** Validates connector-supplied SOQL against Bulk 2.0 query restrictions. */
    public static void validateSoql(String soql) {
        String upper = soql.toUpperCase(Locale.ROOT);
        for (String clause : FORBIDDEN_CLAUSES) {
            if (upper.contains(clause)) {
                throw new SalesforceException("Bulk API 2.0 query does not support " + clause + ": " + soql);
            }
        }
        if (upper.contains("(SELECT")) {
            throw new SalesforceException("Bulk API 2.0 query does not support subqueries: " + soql);
        }
    }

    /** Creates a query job; use operation "queryAll" to include deleted/archived records. */
    public String createJob(String soql, boolean includeDeleted) {
        ObjectNode body = MAPPER.createObjectNode();
        body.put("operation", includeDeleted ? "queryAll" : "query");
        body.put("query", soql);
        JsonNode result = http.executeJson(RequestSpec.post(base()).jsonBody(body.toString()));
        String jobId = result.path("id").asText(null);
        if (jobId == null) {
            throw new SalesforceException("Bulk query job creation returned no id: " + result);
        }
        LOG.info("Created Bulk 2.0 query job {} ({})", jobId, includeDeleted ? "queryAll" : "query");
        return jobId;
    }

    /** Returns current job state, e.g. UploadComplete/InProgress/JobComplete/Failed/Aborted. */
    public JobStatus getStatus(String jobId) {
        JsonNode node = http.executeJson(RequestSpec.get(base() + "/" + jobId));
        return new JobStatus(
                node.path("state").asText(),
                node.path("numberRecordsProcessed").asLong(0),
                node.path("errorMessage").asText(null));
    }

    /**
     * Fetches one page of results. Pass a null locator for the first page.
     * Returns records as ordered field-name→value maps (CSV null → null).
     */
    public ResultPage fetchResults(String jobId, String locator, int maxRecords) {
        StringBuilder path = new StringBuilder(base()).append("/").append(jobId).append("/results?maxRecords=")
                .append(maxRecords);
        if (locator != null) {
            path.append("&locator=").append(locator);
        }
        HttpResponse<byte[]> response = http.execute(
                RequestSpec.get(path.toString()).header("Accept", "text/csv"));
        String nextLocator = response.headers().firstValue("Sforce-Locator").orElse("null");
        if ("null".equals(nextLocator)) {
            nextLocator = null;
        }
        List<Map<String, String>> records = parseCsv(new String(response.body(), StandardCharsets.UTF_8));
        return new ResultPage(records, nextLocator);
    }

    public void abortJob(String jobId) {
        ObjectNode body = MAPPER.createObjectNode();
        body.put("state", "Aborted");
        http.executeJson(RequestSpec.patch(base() + "/" + jobId).jsonBody(body.toString()));
    }

    static List<Map<String, String>> parseCsv(String csv) {
        if (csv.isBlank()) {
            return List.of();
        }
        try (CSVParser parser = CSVParser.parse(new StringReader(csv),
                CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build())) {
            List<Map<String, String>> out = new ArrayList<>();
            List<String> headers = parser.getHeaderNames();
            for (CSVRecord record : parser) {
                Map<String, String> row = new LinkedHashMap<>();
                for (String header : headers) {
                    String value = record.get(header);
                    row.put(header, value == null || value.isEmpty() ? null : value);
                }
                out.add(row);
            }
            return out;
        } catch (IOException e) {
            throw new SalesforceException("Failed to parse Bulk query CSV results", e);
        }
    }

    public record JobStatus(String state, long numberRecordsProcessed, String errorMessage) {
        public boolean isComplete() {
            return "JobComplete".equals(state);
        }

        public boolean isFailed() {
            return "Failed".equals(state) || "Aborted".equals(state);
        }
    }

    public record ResultPage(List<Map<String, String>> records, String nextLocator) {
    }
}
