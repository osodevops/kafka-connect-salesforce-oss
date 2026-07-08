package sh.oso.salesforce.bulk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.salesforce.common.SalesforceException;
import sh.oso.salesforce.http.RequestSpec;
import sh.oso.salesforce.http.SalesforceHttpClient;

import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Bulk API 2.0 ingest-job client: create job (insert/update/upsert/delete),
 * upload CSV, mark UploadComplete, poll, and read successful/failed/unprocessed results.
 */
public final class BulkIngestClient {

    private static final Logger LOG = LoggerFactory.getLogger(BulkIngestClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public enum Operation {
        INSERT, UPDATE, UPSERT, DELETE, HARD_DELETE;

        String wireName() {
            return switch (this) {
                case INSERT -> "insert";
                case UPDATE -> "update";
                case UPSERT -> "upsert";
                case DELETE -> "delete";
                case HARD_DELETE -> "hardDelete";
            };
        }
    }

    private final SalesforceHttpClient http;
    private final String apiVersion;

    public BulkIngestClient(SalesforceHttpClient http, String apiVersion) {
        this.http = http;
        this.apiVersion = apiVersion;
    }

    private String base() {
        return "/services/data/v" + apiVersion + "/jobs/ingest";
    }

    /** Creates an ingest job; externalIdFieldName is required for UPSERT, ignored otherwise. */
    public String createJob(String sobject, Operation operation, String externalIdFieldName) {
        ObjectNode body = MAPPER.createObjectNode();
        body.put("object", sobject);
        body.put("operation", operation.wireName());
        body.put("contentType", "CSV");
        body.put("lineEnding", "LF");
        if (operation == Operation.UPSERT) {
            if (externalIdFieldName == null || externalIdFieldName.isBlank()) {
                throw new SalesforceException("Bulk upsert requires an external ID field name");
            }
            body.put("externalIdFieldName", externalIdFieldName);
        }
        JsonNode result = http.executeJson(RequestSpec.post(base()).jsonBody(body.toString()));
        String jobId = result.path("id").asText(null);
        if (jobId == null) {
            throw new SalesforceException("Bulk ingest job creation returned no id: " + result);
        }
        LOG.info("Created Bulk 2.0 ingest job {} ({} {})", jobId, operation.wireName(), sobject);
        return jobId;
    }

    public void uploadCsv(String jobId, byte[] csv) {
        http.execute(RequestSpec.put(base() + "/" + jobId + "/batches").csvBody(csv));
    }

    public void markUploadComplete(String jobId) {
        ObjectNode body = MAPPER.createObjectNode();
        body.put("state", "UploadComplete");
        http.executeJson(RequestSpec.patch(base() + "/" + jobId).jsonBody(body.toString()));
    }

    public IngestJobStatus getStatus(String jobId) {
        JsonNode node = http.executeJson(RequestSpec.get(base() + "/" + jobId));
        return new IngestJobStatus(
                node.path("state").asText(),
                node.path("numberRecordsProcessed").asLong(0),
                node.path("numberRecordsFailed").asLong(0),
                node.path("errorMessage").asText(null));
    }

    /** CSV of records that succeeded: original columns + sf__Id + sf__Created. */
    public List<Map<String, String>> successfulResults(String jobId) {
        return fetchResultCsv(jobId, "successfulResults");
    }

    /** CSV of records that failed: original columns + sf__Error + sf__Id. */
    public List<Map<String, String>> failedResults(String jobId) {
        return fetchResultCsv(jobId, "failedResults");
    }

    /** CSV of records not processed (job aborted/failed mid-way). */
    public List<Map<String, String>> unprocessedRecords(String jobId) {
        return fetchResultCsv(jobId, "unprocessedrecords");
    }

    public void abortJob(String jobId) {
        ObjectNode body = MAPPER.createObjectNode();
        body.put("state", "Aborted");
        http.executeJson(RequestSpec.patch(base() + "/" + jobId).jsonBody(body.toString()));
    }

    private List<Map<String, String>> fetchResultCsv(String jobId, String resource) {
        HttpResponse<byte[]> response = http.execute(
                RequestSpec.get(base() + "/" + jobId + "/" + resource + "/").header("Accept", "text/csv"));
        return BulkQueryClient.parseCsv(new String(response.body(), StandardCharsets.UTF_8));
    }

    public record IngestJobStatus(String state, long recordsProcessed, long recordsFailed, String errorMessage) {
        public boolean isComplete() {
            return "JobComplete".equals(state);
        }

        public boolean isFailed() {
            return "Failed".equals(state) || "Aborted".equals(state);
        }
    }
}
