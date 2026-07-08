package sh.oso.salesforce.sink;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.salesforce.auth.SalesforceAuth;
import sh.oso.salesforce.auth.SessionSupplier;
import sh.oso.salesforce.bulk.BulkIngestClient;
import sh.oso.salesforce.http.SalesforceHttpClient;
import sh.oso.salesforce.rest.RestClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Writes Kafka records into Salesforce SObjects (insert/update/upsert/delete + Big Objects). */
public class SalesforceSinkTask extends SinkTask {

    private static final Logger LOG = LoggerFactory.getLogger(SalesforceSinkTask.class);

    private SinkConfig config;
    private RestClient rest;
    private SalesforceWriter writer;
    private ErrantRecordReporter reporter;
    private final Map<String, RecordMapper> mappers = new HashMap<>();

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        config = new SinkConfig(props);
        SessionSupplier sessions = new SessionSupplier(
                new SalesforceAuth(config.authConfig(), config.getString(SinkConfig.TOKEN_ENDPOINT)));
        SalesforceHttpClient http = new SalesforceHttpClient(sessions);
        rest = new RestClient(http, config.apiVersion());
        writer = new SalesforceWriter(config, rest, new BulkIngestClient(http, config.apiVersion()));
        try {
            reporter = context.errantRecordReporter();
        } catch (NoSuchMethodError | NoClassDefFoundError e) {
            reporter = null; // pre-2.6 worker
        }
        LOG.info("Salesforce sink task started for objects {}", config.objects());
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        // Group by target object, preserving per-topic record order.
        Map<SinkConfig.ObjectSpec, List<SinkRecord>> byObject = new LinkedHashMap<>();
        for (SinkRecord record : records) {
            if (record.value() == null) {
                continue; // tombstones carry no writable payload
            }
            byObject.computeIfAbsent(config.specForTopic(record.topic()), k -> new ArrayList<>())
                    .add(record);
        }
        try {
            for (Map.Entry<SinkConfig.ObjectSpec, List<SinkRecord>> entry : byObject.entrySet()) {
                writeObjectBatch(entry.getKey(), entry.getValue());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectException("Interrupted while writing to Salesforce", e);
        }
    }

    private void writeObjectBatch(SinkConfig.ObjectSpec spec, List<SinkRecord> records)
            throws InterruptedException {
        RecordMapper mapper = mappers.computeIfAbsent(spec.name(),
                name -> new RecordMapper(spec, rest.describe(name)));
        Map<BulkIngestClient.Operation, List<SalesforceWriter.Pending>> grouped = new LinkedHashMap<>();
        List<SalesforceWriter.Failure> failures = new ArrayList<>();
        for (SinkRecord record : records) {
            try {
                BulkIngestClient.Operation operation = mapper.operationFor(record);
                if (spec.type() == SinkConfig.ObjectType.BIG_OBJECT
                        && operation != BulkIngestClient.Operation.INSERT) {
                    failures.add(new SalesforceWriter.Failure(record,
                            "Big Objects are insert-only; got " + operation));
                    continue;
                }
                grouped.computeIfAbsent(operation, k -> new ArrayList<>())
                        .add(new SalesforceWriter.Pending(record, mapper.toFieldMap(record, operation)));
            } catch (ConnectException e) {
                failures.add(new SalesforceWriter.Failure(record, e.getMessage()));
            }
        }
        for (Map.Entry<BulkIngestClient.Operation, List<SalesforceWriter.Pending>> group
                : grouped.entrySet()) {
            failures.addAll(writer.write(spec, group.getKey(), group.getValue()));
        }
        handleFailures(spec, failures);
    }

    private void handleFailures(SinkConfig.ObjectSpec spec, List<SalesforceWriter.Failure> failures) {
        if (failures.isEmpty()) {
            return;
        }
        for (SalesforceWriter.Failure failure : failures) {
            if (reporter != null) {
                reporter.report(failure.record(), new ConnectException(failure.reason()));
            }
            if (config.errorBehavior() == SinkConfig.ErrorBehavior.LOG) {
                LOG.warn("Record for {} rejected by Salesforce: {}", spec.name(), failure.reason());
            }
        }
        if (config.errorBehavior() == SinkConfig.ErrorBehavior.FAIL) {
            throw new ConnectException(failures.size() + " record(s) rejected by Salesforce for "
                    + spec.name() + "; first: " + failures.get(0).reason());
        }
    }

    @Override
    public void stop() {
    }
}
