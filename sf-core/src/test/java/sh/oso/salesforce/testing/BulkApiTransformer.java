package sh.oso.salesforce.testing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.extension.ResponseDefinitionTransformerV2;
import com.github.tomakehurst.wiremock.http.QueryParameter;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * WireMock transformer implementing the Bulk API 2.0 job lifecycle against {@link BulkJobStore}:
 * query jobs (create → poll → locator-paged CSV results) and ingest jobs
 * (create → CSV upload → UploadComplete → poll → successful/failed/unprocessed results).
 */
final class BulkApiTransformer implements ResponseDefinitionTransformerV2 {

    static final String NAME = "sf-bulk-api";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Pattern QUERY_JOB = Pattern.compile("/services/data/v[^/]+/jobs/query(?:/([^/?]+))?(/results)?/?(?:\\?.*)?");
    private static final Pattern INGEST_JOB = Pattern.compile("/services/data/v[^/]+/jobs/ingest(?:/([^/?]+))?(/batches|/successfulResults|/failedResults|/unprocessedrecords)?/?(?:\\?.*)?");

    private final BulkJobStore store;

    BulkApiTransformer(BulkJobStore store) {
        this.store = store;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean applyGlobally() {
        return false;
    }

    @Override
    public ResponseDefinition transform(ServeEvent serveEvent) {
        Request request = serveEvent.getRequest();
        String url = request.getUrl();
        Matcher query = QUERY_JOB.matcher(url);
        if (query.matches()) {
            return handleQuery(request, query.group(1), query.group(2) != null);
        }
        Matcher ingest = INGEST_JOB.matcher(url);
        if (ingest.matches()) {
            return handleIngest(request, ingest.group(1), ingest.group(2));
        }
        return json(404, "{\"error\":\"no bulk route for " + url + "\"}");
    }

    private ResponseDefinition handleQuery(Request request, String jobId, boolean results) {
        String method = request.getMethod().getName();
        if (jobId == null && "POST".equals(method)) {
            JsonNode body = readJson(request);
            BulkJobStore.QueryJob job = store.createQueryJob(
                    body.path("query").asText(), body.path("operation").asText("query"));
            return json(200, "{\"id\":\"" + job.id + "\",\"state\":\"UploadComplete\"}");
        }
        BulkJobStore.QueryJob job = store.queryJob(jobId);
        if (job == null) {
            return json(404, "[{\"errorCode\":\"NOT_FOUND\",\"message\":\"no such job\"}]");
        }
        if (results) {
            QueryParameter locatorParam = request.queryParameter("locator");
            int page = locatorParam.isPresent() ? Integer.parseInt(locatorParam.firstValue()) : 0;
            String csv = page < job.resultPages.size() ? job.resultPages.get(page) : "";
            String nextLocator = page + 1 < job.resultPages.size() ? String.valueOf(page + 1) : "null";
            long numberOfRecords = Math.max(0, csv.split("\n").length - 1);
            return buildCsv(csv, nextLocator, numberOfRecords);
        }
        if ("GET".equals(method)) {
            if (job.pollsUntilComplete > 0) {
                job.pollsUntilComplete--;
                return json(200, "{\"id\":\"" + job.id + "\",\"state\":\"InProgress\"}");
            }
            if (!"Aborted".equals(job.state) && !"Failed".equals(job.state)) {
                job.state = "JobComplete";
            }
            return json(200, "{\"id\":\"" + job.id + "\",\"state\":\"" + job.state + "\"}");
        }
        if ("PATCH".equals(method)) {
            JsonNode body = readJson(request);
            job.state = body.path("state").asText("Aborted");
            return json(200, "{\"id\":\"" + job.id + "\",\"state\":\"" + job.state + "\"}");
        }
        return json(405, "{}");
    }

    private ResponseDefinition handleIngest(Request request, String jobId, String subResource) {
        String method = request.getMethod().getName();
        if (jobId == null && "POST".equals(method)) {
            JsonNode body = readJson(request);
            BulkJobStore.IngestJob job = store.createIngestJob(
                    body.path("object").asText(),
                    body.path("operation").asText(),
                    body.path("externalIdFieldName").asText(null));
            return json(200, "{\"id\":\"" + job.id + "\",\"state\":\"Open\",\"contentUrl\":\"services/data/v60.0/jobs/ingest/" + job.id + "/batches\"}");
        }
        BulkJobStore.IngestJob job = store.ingestJob(jobId);
        if (job == null) {
            return json(404, "[{\"errorCode\":\"NOT_FOUND\",\"message\":\"no such job\"}]");
        }
        if ("/batches".equals(subResource) && "PUT".equals(method)) {
            job.uploadedCsv = request.getBodyAsString();
            return json(201, "{}");
        }
        if (subResource != null && subResource.length() > "/batches".length()) {
            return resultCsv(job, subResource);
        }
        if ("PATCH".equals(method)) {
            JsonNode body = readJson(request);
            String newState = body.path("state").asText();
            if ("UploadComplete".equals(newState)) {
                job.state = "InProgress";
                job.pollsUntilComplete = 1;
            } else {
                job.state = newState;
            }
            return json(200, "{\"id\":\"" + job.id + "\",\"state\":\"" + job.state + "\"}");
        }
        if ("GET".equals(method)) {
            if (job.pollsUntilComplete > 0) {
                job.pollsUntilComplete--;
                return json(200, "{\"id\":\"" + job.id + "\",\"state\":\"InProgress\"}");
            }
            if ("InProgress".equals(job.state)) {
                job.state = "JobComplete";
            }
            long failed = countFailedRows(job);
            long total = job.uploadedCsv == null ? 0 : BulkJobStore.splitCsv(job.uploadedCsv).rows().size();
            return json(200, "{\"id\":\"" + job.id + "\",\"state\":\"" + job.state
                    + "\",\"numberRecordsProcessed\":" + total
                    + ",\"numberRecordsFailed\":" + failed + "}");
        }
        return json(405, "{}");
    }

    private long countFailedRows(BulkJobStore.IngestJob job) {
        if (job.uploadedCsv == null) {
            return 0;
        }
        return BulkJobStore.splitCsv(job.uploadedCsv).rows().stream()
                .filter(row -> store.ingestRowFailer().apply(row) != null)
                .count();
    }

    private ResponseDefinition resultCsv(BulkJobStore.IngestJob job, String subResource) {
        BulkJobStore.CsvDoc doc = job.uploadedCsv != null
                ? BulkJobStore.splitCsv(job.uploadedCsv)
                : new BulkJobStore.CsvDoc("", List.of());
        List<String> lines = new ArrayList<>();
        int i = 0;
        switch (subResource) {
            case "/successfulResults" -> {
                lines.add("\"sf__Id\",\"sf__Created\"," + doc.header());
                for (String row : doc.rows()) {
                    if (store.ingestRowFailer().apply(row) == null) {
                        lines.add("\"001MOCK" + (i++) + "\",\"true\"," + row);
                    }
                }
            }
            case "/failedResults" -> {
                lines.add("\"sf__Id\",\"sf__Error\"," + doc.header());
                for (String row : doc.rows()) {
                    String error = store.ingestRowFailer().apply(row);
                    if (error != null) {
                        lines.add("\"\",\"" + error + "\"," + row);
                    }
                }
            }
            case "/unprocessedrecords" -> lines.add(doc.header());
            default -> {
                return json(404, "{}");
            }
        }
        return buildCsv(String.join("\n", lines) + "\n", null, lines.size() - 1);
    }

    private static ResponseDefinition buildCsv(String csv, String locator, long numberOfRecords) {
        ResponseDefinitionBuilder builder = ResponseDefinitionBuilder.responseDefinition()
                .withStatus(200)
                .withHeader("Content-Type", "text/csv")
                .withHeader("Sforce-NumberOfRecords", String.valueOf(numberOfRecords))
                .withBody(csv);
        if (locator != null) {
            builder.withHeader("Sforce-Locator", locator);
        }
        return builder.build();
    }

    private static ResponseDefinition json(int status, String body) {
        return ResponseDefinitionBuilder.responseDefinition()
                .withStatus(status)
                .withHeader("Content-Type", "application/json")
                .withBody(body)
                .build();
    }

    private static JsonNode readJson(Request request) {
        try {
            return MAPPER.readTree(request.getBodyAsString());
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid JSON body", e);
        }
    }
}
