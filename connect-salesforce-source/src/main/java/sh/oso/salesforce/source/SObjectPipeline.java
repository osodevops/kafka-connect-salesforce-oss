package sh.oso.salesforce.source;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.salesforce.bulk.BulkQueryClient;
import sh.oso.salesforce.common.SalesforceException;
import sh.oso.salesforce.limits.RateGovernor;
import sh.oso.salesforce.pubsub.ChangeEventUtils;
import sh.oso.salesforce.pubsub.DecodedEvent;
import sh.oso.salesforce.pubsub.PubSubClient;
import sh.oso.salesforce.pubsub.PubSubSubscription;
import sh.oso.salesforce.pubsub.SubscribeOptions;
import sh.oso.salesforce.rest.RestClient;
import sh.oso.salesforce.schema.DescribeToConnect;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Per-SObject ingestion state machine: optional Bulk 2.0 snapshot, then event-driven sync
 * (Pub/Sub CDC) or periodic Bulk polling, with seamless snapshot→stream handoff and
 * gap/overflow recovery. Failures in one pipeline do not affect other SObjects.
 */
final class SObjectPipeline implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SObjectPipeline.class);
    private static final DateTimeFormatter SOQL_DATETIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(java.time.ZoneOffset.UTC);

    private final SourceConfig config;
    private final String sobject;
    private final String cdcTopicName;
    private final RestClient rest;
    private final BulkQueryClient bulkQuery;
    private final PubSubClient pubsub;
    private final RateGovernor governor;
    private final SourceRecordFactory records;
    private final SObjectOffset offset;

    private PubSubSubscription subscription;
    private String snapshotJobId;
    private String snapshotLocator;
    private boolean snapshotJobExhausted;
    private String pendingCursor;          // polling: cursor to store once the job completes
    private String activePollJobId;
    private String activePollLocator;
    private long lastPollStartedAt;
    private Schema describeSchema;

    SObjectPipeline(SourceConfig config, String sobject, RestClient rest, BulkQueryClient bulkQuery,
                    PubSubClient pubsub, RateGovernor governor, Map<String, Object> storedOffset) {
        this.config = config;
        this.sobject = sobject;
        this.cdcTopicName = "/data/" + sobject + "ChangeEvent";
        this.rest = rest;
        this.bulkQuery = bulkQuery;
        this.pubsub = pubsub;
        this.governor = governor;
        this.records = new SourceRecordFactory(sobject, config.topicFor(sobject));
        this.offset = SObjectOffset.fromMap(storedOffset);
        if (offset.mode() == SObjectOffset.Mode.SNAPSHOT
                && (storedOffset == null || storedOffset.isEmpty())) {
            if (!config.getBoolean(SourceConfig.SNAPSHOT_ENABLED)) {
                offset.setMode(realtimeMode());
            }
        }
        LOG.info("Pipeline for {} starting in mode {}", sobject, offset.mode());
    }

    String sobject() {
        return sobject;
    }

    List<SourceRecord> poll() throws InterruptedException {
        return switch (offset.mode()) {
            case SNAPSHOT -> snapshotStep();
            case EVENT_DRIVEN -> eventStep();
            case POLLING -> pollingStep();
        };
    }

    // ---------------------------------------------------------------- snapshot

    private List<SourceRecord> snapshotStep() throws InterruptedException {
        if (snapshotJobId == null) {
            startSnapshot();
            return List.of();
        }
        BulkQueryClient.JobStatus status = bulkQuery.getStatus(snapshotJobId);
        if (status.isFailed()) {
            throw new ConnectException("Snapshot Bulk job " + snapshotJobId + " for " + sobject
                    + " failed: " + status.errorMessage());
        }
        if (!status.isComplete()) {
            return List.of();
        }
        governor.acquire(RateGovernor.DAILY_API_REQUESTS);
        BulkQueryClient.ResultPage page = bulkQuery.fetchResults(snapshotJobId, snapshotLocator,
                config.getInt(SourceConfig.RESULT_MAX_ROWS));
        snapshotLocator = page.nextLocator();
        List<SourceRecord> out = new ArrayList<>();
        for (Map<String, String> row : page.records()) {
            out.add(records.fromBulkRow(describeSchema(), row, "created", offset.toMap()));
        }
        if (snapshotLocator == null) {
            completeSnapshot();
        }
        return out;
    }

    private void startSnapshot() throws InterruptedException {
        if (realtimeMode() == SObjectOffset.Mode.EVENT_DRIVEN && offset.probeReplayId() == null) {
            // Probe the CDC stream's current position before snapshotting; the handoff
            // subscription replays from here so nothing between probe and completion is lost.
            try (PubSubSubscription probe = pubsub.subscribe(
                    SubscribeOptions.latest(cdcTopicName, config.getInt(SourceConfig.PUBSUB_BATCH_SIZE)))) {
                if (!probe.awaitEstablished(30_000)) {
                    throw new SalesforceException("Timed out establishing CDC probe for " + sobject, null, true);
                }
                offset.setProbeReplayId(probe.latestReplayId());
            }
            LOG.info("Recorded CDC probe replayId for {} before snapshot", sobject);
        }
        governor.acquire(RateGovernor.DAILY_BULK_V2_QUERY_JOBS);
        String since = config.getString(SourceConfig.SNAPSHOT_SINCE);
        String soql = SoqlBuilder.snapshotQuery(sobject, describe(), since);
        BulkQueryClient.validateSoql(soql);
        pendingCursor = SOQL_DATETIME.format(Instant.now());
        snapshotJobId = bulkQuery.createJob(soql, false);
        LOG.info("Snapshot for {} started (job {})", sobject, snapshotJobId);
    }

    private void completeSnapshot() {
        offset.setSnapshotCompletionTimestamp(Instant.now().toEpochMilli());
        offset.setLastSystemModstamp(pendingCursor);
        offset.setMode(realtimeMode());
        snapshotJobId = null;
        snapshotLocator = null;
        LOG.info("Snapshot for {} complete; transitioning to {}", sobject, offset.mode());
    }

    // ---------------------------------------------------------------- event-driven

    private List<SourceRecord> eventStep() throws InterruptedException {
        ensureSubscription();
        List<DecodedEvent> events;
        try {
            events = subscription.poll(config.getInt(SourceConfig.PUBSUB_BATCH_SIZE), 100);
        } catch (SalesforceException e) {
            return handleStreamFailure(e);
        }
        List<SourceRecord> out = new ArrayList<>();
        for (DecodedEvent event : events) {
            processEvent(event, out);
        }
        if (subscription != null && subscription.isBroken()) {
            closeSubscription();
        }
        return out;
    }

    private void processEvent(DecodedEvent event, List<SourceRecord> out) throws InterruptedException {
        GenericRecord header = ChangeEventUtils.changeEventHeader(event.payload())
                .orElseThrow(() -> new ConnectException("Event on " + cdcTopicName + " has no ChangeEventHeader"));
        String changeType = header.get("changeType").toString();
        long commitTimestamp = (Long) header.get("commitTimestamp");
        offset.setReplayId(event.replayId());
        offset.setCommitTimestamp(commitTimestamp);

        // Snapshot-handoff dedup: drop changes committed before the snapshot finished —
        // the snapshot already delivered their post-image.
        if (offset.snapshotCompletionTimestamp() != null
                && commitTimestamp < offset.snapshotCompletionTimestamp()) {
            LOG.debug("Dropping {} event for {} committed before snapshot completion", changeType, sobject);
            return;
        }
        if (changeType.startsWith("GAP_")) {
            recoverFromGap(changeType, header, out);
            return;
        }
        @SuppressWarnings("unchecked")
        List<Object> recordIds = (List<Object>) header.get("recordIds");
        for (Object idObj : recordIds) {
            String recordId = idObj.toString();
            DecodedEvent enriched = maybeFetchFullRecord(event, changeType);
            out.add(records.fromChangeEvent(enriched, header, changeType, recordId, offset.toMap()));
            if ("DELETE".equals(changeType) && config.getBoolean(SourceConfig.EMIT_TOMBSTONE_ON_DELETE)) {
                out.add(records.tombstone(recordId, offset.toMap()));
            }
        }
    }

    private DecodedEvent maybeFetchFullRecord(DecodedEvent event, String changeType) {
        if (!"UPDATE".equals(changeType) || !config.getBoolean(SourceConfig.FULL_RECORD_ON_UPDATE)) {
            return event;
        }
        GenericRecord header = ChangeEventUtils.changeEventHeader(event.payload()).orElseThrow();
        @SuppressWarnings("unchecked")
        List<Object> recordIds = (List<Object>) header.get("recordIds");
        if (recordIds.size() != 1) {
            return event;
        }
        governor.acquire(RateGovernor.DAILY_API_REQUESTS);
        return rest.retrieve(sobject, recordIds.get(0).toString(), null)
                .map(json -> mergePostImage(event, json))
                .orElse(event);
    }

    /** Fills event fields that are null (and not explicitly nulled) from the REST post-image. */
    private DecodedEvent mergePostImage(DecodedEvent event, JsonNode json) {
        GenericRecord payload = event.payload();
        for (org.apache.avro.Schema.Field field : payload.getSchema().getFields()) {
            if (ChangeEventUtils.HEADER_FIELD.equals(field.name()) || payload.get(field.name()) != null) {
                continue;
            }
            JsonNode value = json.path(field.name());
            if (value.isMissingNode() || value.isNull()) {
                continue;
            }
            org.apache.avro.Schema fieldSchema = ChangeEventUtils.unwrapNullable(field.schema());
            switch (fieldSchema.getType()) {
                case STRING, ENUM -> payload.put(field.name(), value.asText());
                case LONG -> payload.put(field.name(), value.isNumber() ? value.asLong()
                        : Instant.parse(value.asText().replaceAll("([+-]\\d{2})(\\d{2})$", "$1:$2"))
                                .toEpochMilli());
                case INT -> payload.put(field.name(), value.asInt());
                case DOUBLE -> payload.put(field.name(), value.asDouble());
                case FLOAT -> payload.put(field.name(), (float) value.asDouble());
                case BOOLEAN -> payload.put(field.name(), value.asBoolean());
                default -> {
                    // leave complex types absent
                }
            }
        }
        return event;
    }

    private void recoverFromGap(String changeType, GenericRecord header, List<SourceRecord> out)
            throws InterruptedException {
        LOG.warn("Gap event {} on {}; sf.gap.recovery={}", changeType, sobject, config.gapRecovery());
        if (config.gapRecovery() == SourceConfig.GapRecovery.FAIL) {
            throw new ConnectException("Gap event " + changeType + " on " + sobject
                    + " and sf.gap.recovery=fail");
        }
        if ("GAP_OVERFLOW".equals(changeType)) {
            // Overflow doesn't identify affected records; recover per strategy.
            if (config.gapRecovery() == SourceConfig.GapRecovery.RESYNC) {
                resync(out);
            } else {
                LOG.warn("Skipping overflow gap on {} (sf.gap.recovery=latest); data loss accepted", sobject);
            }
            return;
        }
        // Targeted gap (specific records): reconstruct each by REST re-query.
        @SuppressWarnings("unchecked")
        List<Object> recordIds = (List<Object>) header.get("recordIds");
        String eventType = SourceRecordFactory.eventTypeFor(changeType.substring("GAP_".length()));
        for (Object idObj : recordIds) {
            String recordId = idObj.toString();
            governor.acquire(RateGovernor.DAILY_API_REQUESTS);
            rest.retrieve(sobject, recordId, null).ifPresentOrElse(json -> {
                Map<String, String> row = new java.util.LinkedHashMap<>();
                json.fields().forEachRemaining(e -> {
                    if (!e.getValue().isContainerNode() && !e.getValue().isNull()) {
                        row.put(e.getKey(), e.getValue().asText());
                    }
                });
                row.put("Id", recordId);
                out.add(records.fromBulkRow(describeSchema(), row, eventType, offset.toMap()));
            }, () -> LOG.warn("Gap record {} on {} no longer retrievable", recordId, sobject));
        }
    }

    /** Incremental Bulk resync from the watermark (minus a safety buffer). */
    private void resync(List<SourceRecord> out) throws InterruptedException {
        long watermark = offset.commitTimestamp() != null ? offset.commitTimestamp()
                : offset.snapshotCompletionTimestamp() != null ? offset.snapshotCompletionTimestamp()
                : 0L;
        String from = SOQL_DATETIME.format(Instant.ofEpochMilli(
                Math.max(0, watermark - config.getLong(SourceConfig.GAP_RESYNC_BUFFER_MS))));
        governor.acquire(RateGovernor.DAILY_BULK_V2_QUERY_JOBS);
        String jobId = bulkQuery.createJob(SoqlBuilder.resyncQuery(sobject, describe(), from), false);
        LOG.info("Gap resync for {} from {} (job {})", sobject, from, jobId);
        String locator = null;
        while (true) {
            BulkQueryClient.JobStatus status = bulkQuery.getStatus(jobId);
            if (status.isFailed()) {
                throw new ConnectException("Gap resync job " + jobId + " failed: " + status.errorMessage());
            }
            if (status.isComplete()) {
                break;
            }
            Thread.sleep(200);
        }
        do {
            BulkQueryClient.ResultPage page = bulkQuery.fetchResults(jobId, locator,
                    config.getInt(SourceConfig.RESULT_MAX_ROWS));
            for (Map<String, String> row : page.records()) {
                out.add(records.fromBulkRow(describeSchema(), row, "updated", offset.toMap()));
            }
            locator = page.nextLocator();
        } while (locator != null);
    }

    private void ensureSubscription() throws InterruptedException {
        if (subscription != null && !subscription.isBroken()) {
            return;
        }
        closeSubscription();
        int batchSize = config.getInt(SourceConfig.PUBSUB_BATCH_SIZE);
        SubscribeOptions options;
        if (offset.replayId() != null) {
            options = SubscribeOptions.custom(cdcTopicName, offset.replayId(), batchSize);
        } else if (offset.probeReplayId() != null) {
            options = SubscribeOptions.custom(cdcTopicName, offset.probeReplayId(), batchSize);
        } else if (config.eventStart() == SourceConfig.EventStart.ALL) {
            options = SubscribeOptions.earliest(cdcTopicName, batchSize);
        } else {
            options = SubscribeOptions.latest(cdcTopicName, batchSize);
        }
        subscription = pubsub.subscribe(options);
        LOG.info("Subscribed to {} ({})", cdcTopicName, options.replayPreset());
    }

    private List<SourceRecord> handleStreamFailure(SalesforceException failure) throws InterruptedException {
        closeSubscription();
        String message = failure.getMessage() == null ? "" : failure.getMessage();
        boolean replayInvalid = message.contains("replayid")
                || message.contains("replay id") || message.contains("REPLAY_ID");
        if (replayInvalid || !failure.isRetryable()) {
            LOG.warn("CDC stream for {} lost its replay position ({}); applying {} recovery",
                    sobject, message, config.gapRecovery());
            offset.setReplayId(null);
            offset.setProbeReplayId(null);
            List<SourceRecord> out = new ArrayList<>();
            switch (config.gapRecovery()) {
                case RESYNC -> resync(out);
                case LATEST -> LOG.warn("Resubscribing {} from LATEST; events during downtime lost", sobject);
                case FAIL -> throw new ConnectException("CDC replay position lost for " + sobject, failure);
            }
            return out;
        }
        LOG.warn("Transient CDC stream failure for {}: {}; will resubscribe", sobject, message);
        return List.of();
    }

    private void closeSubscription() {
        if (subscription != null) {
            subscription.close();
            subscription = null;
        }
    }

    // ---------------------------------------------------------------- polling

    private List<SourceRecord> pollingStep() throws InterruptedException {
        if (activePollJobId == null) {
            long interval = config.getInt(SourceConfig.POLL_INTERVAL_MS);
            if (System.currentTimeMillis() - lastPollStartedAt < interval) {
                return List.of();
            }
            lastPollStartedAt = System.currentTimeMillis();
            governor.acquire(RateGovernor.DAILY_BULK_V2_QUERY_JOBS);
            String soql = SoqlBuilder.pollingQuery(sobject, describe(), offset.lastSystemModstamp());
            BulkQueryClient.validateSoql(soql);
            pendingCursor = SOQL_DATETIME.format(Instant.now());
            activePollJobId = bulkQuery.createJob(soql, config.getBoolean(SourceConfig.INCLUDE_DELETED));
            return List.of();
        }
        BulkQueryClient.JobStatus status = bulkQuery.getStatus(activePollJobId);
        if (status.isFailed()) {
            String jobId = activePollJobId;
            activePollJobId = null;
            throw new ConnectException("Polling Bulk job " + jobId + " for " + sobject
                    + " failed: " + status.errorMessage());
        }
        if (!status.isComplete()) {
            return List.of();
        }
        BulkQueryClient.ResultPage page = bulkQuery.fetchResults(activePollJobId, activePollLocator,
                config.getInt(SourceConfig.RESULT_MAX_ROWS));
        activePollLocator = page.nextLocator();
        boolean jobDone = activePollLocator == null;
        if (jobDone) {
            activePollJobId = null;
            // Advance the cursor only once the whole window has been emitted (at-least-once).
            offset.setLastSystemModstamp(pendingCursor);
        }
        List<SourceRecord> out = new ArrayList<>();
        for (Map<String, String> row : page.records()) {
            out.add(records.fromBulkRow(describeSchema(), row, pollEventType(row), offset.toMap()));
        }
        return out;
    }

    private static String pollEventType(Map<String, String> row) {
        if (Boolean.parseBoolean(row.get("IsDeleted"))) {
            return "deleted";
        }
        String created = row.get("CreatedDate");
        String modstamp = row.get("SystemModstamp");
        return created != null && created.equals(modstamp) ? "created" : "updated";
    }

    // ---------------------------------------------------------------- shared

    private SObjectOffset.Mode realtimeMode() {
        return config.realtimeMode() == SourceConfig.RealtimeMode.EVENT_DRIVEN
                ? SObjectOffset.Mode.EVENT_DRIVEN
                : SObjectOffset.Mode.POLLING;
    }

    private JsonNode describe() {
        governor.acquire(RateGovernor.DAILY_API_REQUESTS);
        return rest.describe(sobject);
    }

    private Schema describeSchema() {
        if (describeSchema == null) {
            describeSchema = DescribeToConnect.toConnectSchema(describe(),
                    config.skipEpochConversionFields());
        }
        return describeSchema;
    }

    @Override
    public void close() {
        closeSubscription();
    }
}
