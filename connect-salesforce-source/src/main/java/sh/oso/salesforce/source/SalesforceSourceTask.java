package sh.oso.salesforce.source;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.salesforce.auth.SalesforceAuth;
import sh.oso.salesforce.auth.SessionSupplier;
import sh.oso.salesforce.bulk.BulkQueryClient;
import sh.oso.salesforce.http.SalesforceHttpClient;
import sh.oso.salesforce.limits.RateGovernor;
import sh.oso.salesforce.pubsub.PubSubClient;
import sh.oso.salesforce.rest.RestClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Runs one or more {@link SObjectPipeline}s with independent failure isolation. */
public class SalesforceSourceTask extends SourceTask {

    private static final Logger LOG = LoggerFactory.getLogger(SalesforceSourceTask.class);

    private SourceConfig config;
    private PubSubClient pubsub;
    private final List<SObjectPipeline> pipelines = new ArrayList<>();

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        config = new SourceConfig(props);
        SessionSupplier sessions = new SessionSupplier(
                new SalesforceAuth(config.authConfig(), config.getString(SourceConfig.TOKEN_ENDPOINT)));
        SalesforceHttpClient http = new SalesforceHttpClient(sessions);
        RestClient rest = new RestClient(http, config.apiVersion());
        BulkQueryClient bulkQuery = new BulkQueryClient(http, config.apiVersion());
        RateGovernor governor = RateGovernor.withDefaults(rest);
        if (config.realtimeMode() == SourceConfig.RealtimeMode.EVENT_DRIVEN) {
            pubsub = new PubSubClient(sessions, config.getString(SourceConfig.PUBSUB_ENDPOINT),
                    !config.getBoolean(SourceConfig.PUBSUB_PLAINTEXT));
        }
        for (String sobject : config.taskSobjects()) {
            Map<String, Object> stored = context.offsetStorageReader()
                    .offset(SObjectOffset.partition(sobject));
            pipelines.add(new SObjectPipeline(config, sobject, rest, bulkQuery, pubsub, governor, stored));
        }
        LOG.info("Salesforce source task started for SObjects {}", config.taskSobjects());
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> out = new ArrayList<>();
        ConnectException firstFailure = null;
        for (SObjectPipeline pipeline : pipelines) {
            try {
                out.addAll(pipeline.poll());
            } catch (ConnectException e) {
                // Isolate per-SObject failures: emit what other pipelines produced first.
                LOG.error("Pipeline for {} failed", pipeline.sobject(), e);
                if (firstFailure == null) {
                    firstFailure = e;
                }
            }
        }
        if (out.isEmpty() && firstFailure != null) {
            throw firstFailure;
        }
        if (out.isEmpty()) {
            Thread.sleep(100);
            return null;
        }
        return out;
    }

    @Override
    public void stop() {
        for (SObjectPipeline pipeline : pipelines) {
            try {
                pipeline.close();
            } catch (RuntimeException e) {
                LOG.warn("Error closing pipeline for {}", pipeline.sobject(), e);
            }
        }
        if (pubsub != null) {
            pubsub.close();
        }
    }
}
