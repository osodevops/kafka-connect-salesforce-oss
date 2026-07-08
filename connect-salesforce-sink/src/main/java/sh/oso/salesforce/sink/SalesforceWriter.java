package sh.oso.salesforce.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.salesforce.bulk.BulkIngestClient;
import sh.oso.salesforce.bulk.CsvEncoder;
import sh.oso.salesforce.common.SalesforceException;
import sh.oso.salesforce.rest.RestClient;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Writes batches of mapped records to Salesforce via Bulk API 2.0 ingest jobs or
 * REST Composite calls, returning per-record failures for DLQ routing.
 */
final class SalesforceWriter {

    /** A record failure with the originating SinkRecord for errant-record reporting. */
    record Failure(SinkRecord record, String reason) {
    }

    /** One mapped record ready to write. */
    record Pending(SinkRecord record, Map<String, Object> fields) {
    }

    private static final Logger LOG = LoggerFactory.getLogger(SalesforceWriter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long BULK_POLL_INTERVAL_MS = 500;

    private final SinkConfig config;
    private final RestClient rest;
    private final BulkIngestClient bulkIngest;

    SalesforceWriter(SinkConfig config, RestClient rest, BulkIngestClient bulkIngest) {
        this.config = config;
        this.rest = rest;
        this.bulkIngest = bulkIngest;
    }

    List<Failure> write(SinkConfig.ObjectSpec spec, BulkIngestClient.Operation operation,
                        List<Pending> batch) throws InterruptedException {
        if (batch.isEmpty()) {
            return List.of();
        }
        boolean useRest = switch (config.writeMode()) {
            case REST -> true;
            case BULK2 -> false;
            case AUTO -> batch.size() <= config.getInt(SinkConfig.WRITE_REST_THRESHOLD);
        };
        return useRest ? writeRest(spec, operation, batch) : writeBulk(spec, operation, batch);
    }

    // ---------------------------------------------------------------- REST composite

    private List<Failure> writeRest(SinkConfig.ObjectSpec spec, BulkIngestClient.Operation operation,
                                    List<Pending> batch) {
        List<ObjectNode> payload = new ArrayList<>();
        for (Pending pending : batch) {
            ObjectNode node = MAPPER.createObjectNode();
            pending.fields().forEach((k, v) -> node.putPOJO(k, v));
            payload.add(node);
        }
        List<JsonNode> results = switch (operation) {
            case INSERT -> rest.compositeCreate(spec.name(), payload, false);
            case UPDATE -> rest.compositeUpdate(spec.name(), payload, false);
            case UPSERT -> rest.compositeUpsert(spec.name(), spec.customIdFieldName(), payload, false);
            case DELETE, HARD_DELETE -> rest.compositeDelete(deleteIds(spec, batch), false);
        };
        List<Failure> failures = new ArrayList<>();
        for (int i = 0; i < results.size() && i < batch.size(); i++) {
            JsonNode result = results.get(i);
            if (!result.path("success").asBoolean(false)) {
                failures.add(new Failure(batch.get(i).record(), result.path("errors").toString()));
            }
        }
        return failures;
    }

    private List<String> deleteIds(SinkConfig.ObjectSpec spec, List<Pending> batch) {
        List<String> ids = new ArrayList<>();
        for (Pending pending : batch) {
            Object id = pending.fields().get("Id");
            if (id == null && spec.useCustomIdField()) {
                id = resolveIdByExternalId(spec, pending.fields().get(spec.customIdFieldName()));
            }
            ids.add(Objects.toString(id, ""));
        }
        return ids;
    }

    private String resolveIdByExternalId(SinkConfig.ObjectSpec spec, Object externalId) {
        if (externalId == null) {
            return null;
        }
        List<JsonNode> rows = rest.queryAllPages("SELECT Id FROM " + spec.name() + " WHERE "
                + spec.customIdFieldName() + " = '" + externalId + "'", false);
        return rows.isEmpty() ? null : rows.get(0).path("Id").asText();
    }

    // ---------------------------------------------------------------- Bulk 2.0

    private List<Failure> writeBulk(SinkConfig.ObjectSpec spec, BulkIngestClient.Operation operation,
                                    List<Pending> batch) throws InterruptedException {
        String extId = operation == BulkIngestClient.Operation.UPSERT ? spec.customIdFieldName() : null;
        String jobId = bulkIngest.createJob(spec.name(), operation, extId);
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Pending pending : batch) {
            rows.add(pending.fields());
        }
        bulkIngest.uploadCsv(jobId, CsvEncoder.encode(rows, false));
        bulkIngest.markUploadComplete(jobId);

        BulkIngestClient.IngestJobStatus status;
        while (true) {
            status = bulkIngest.getStatus(jobId);
            if (status.isComplete() || status.isFailed()) {
                break;
            }
            Thread.sleep(BULK_POLL_INTERVAL_MS);
        }
        if (status.isFailed()) {
            throw new SalesforceException("Bulk ingest job " + jobId + " failed: " + status.errorMessage());
        }
        if (status.recordsFailed() == 0) {
            return List.of();
        }
        return matchFailures(spec, batch, bulkIngest.failedResults(jobId));
    }

    /**
     * Bulk failedResults echo the original columns plus sf__Error. Rows are matched back to
     * SinkRecords by identity fields (Id/external id) or, failing that, full field equality.
     */
    private List<Failure> matchFailures(SinkConfig.ObjectSpec spec, List<Pending> batch,
                                        List<Map<String, String>> failedRows) {
        List<Failure> failures = new ArrayList<>();
        List<Pending> unmatched = new ArrayList<>(batch);
        for (Map<String, String> row : failedRows) {
            String reason = row.getOrDefault("sf__Error", "unknown Bulk ingest error");
            Pending match = findMatch(spec, unmatched, row);
            if (match != null) {
                unmatched.remove(match);
                failures.add(new Failure(match.record(), reason));
            } else {
                LOG.warn("Could not correlate failed Bulk row {} to a Kafka record; reporting first unmatched", row);
                if (!unmatched.isEmpty()) {
                    failures.add(new Failure(unmatched.remove(0).record(), reason));
                }
            }
        }
        return failures;
    }

    private Pending findMatch(SinkConfig.ObjectSpec spec, List<Pending> candidates,
                              Map<String, String> failedRow) {
        List<String> keys = new ArrayList<>();
        if (spec.useCustomIdField()) {
            keys.add(spec.customIdFieldName());
        }
        keys.add("Id");
        for (String key : keys) {
            String failedValue = failedRow.get(key);
            if (failedValue == null) {
                continue;
            }
            for (Pending candidate : candidates) {
                if (failedValue.equals(Objects.toString(candidate.fields().get(key), null))) {
                    return candidate;
                }
            }
        }
        for (Pending candidate : candidates) {
            if (fieldsMatch(candidate.fields(), failedRow)) {
                return candidate;
            }
        }
        return null;
    }

    private static boolean fieldsMatch(Map<String, Object> fields, Map<String, String> row) {
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            String rowValue = row.get(entry.getKey());
            if (rowValue != null && !rowValue.equals(Objects.toString(entry.getValue(), null))) {
                return false;
            }
        }
        return true;
    }

    /** Groups mapped records per operation, preserving order within each group. */
    static Map<BulkIngestClient.Operation, List<Pending>> groupByOperation(
            List<Pending> pendings, List<BulkIngestClient.Operation> operations) {
        Map<BulkIngestClient.Operation, List<Pending>> grouped = new LinkedHashMap<>();
        for (int i = 0; i < pendings.size(); i++) {
            grouped.computeIfAbsent(operations.get(i), k -> new ArrayList<>()).add(pendings.get(i));
        }
        return grouped;
    }
}
