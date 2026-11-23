package sh.oso.salesforce.source;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.salesforce.auth.SalesforceAuth;
import sh.oso.salesforce.auth.SessionSupplier;
import sh.oso.salesforce.bulk.BulkQueryClient;
import sh.oso.salesforce.common.SalesforceException;
import sh.oso.salesforce.http.SalesforceHttpClient;
import sh.oso.salesforce.limits.RateGovernor;
import sh.oso.salesforce.pubsub.PubSubClient;
import sh.oso.salesforce.rest.RestClient;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Runs the task's {@link SObjectPipeline}s and {@link PlatformEventPipeline}s with independent
 * failure isolation.
 */
public class SalesforceSourceTask extends SourceTask {

    private static final Logger LOG = LoggerFactory.getLogger(SalesforceSourceTask.class);

    private SourceConfig config;
    private PubSubClient pubsub;
    private final List<SourcePipeline> pipelines = new ArrayList<>();
    private final Map<SourcePipeline, Backoff> backoffs = new IdentityHashMap<>();
    private FatalPipelineException fatal;

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
        List<String> events = config.taskPlatformEvents();
        // Platform events always stream over Pub/Sub, whatever sf.realtime.mode says for SObjects.
        if (config.realtimeMode() == SourceConfig.RealtimeMode.EVENT_DRIVEN || !events.isEmpty()) {
            pubsub = new PubSubClient(sessions, config.getString(SourceConfig.PUBSUB_ENDPOINT),
                    !config.getBoolean(SourceConfig.PUBSUB_PLAINTEXT));
        }
        PubSubClient sobjectPubSub = config.realtimeMode() == SourceConfig.RealtimeMode.EVENT_DRIVEN ? pubsub : null;
        for (String sobject : config.taskSobjects()) {
            Map<String, Object> stored = context.offsetStorageReader()
                    .offset(SObjectOffset.partition(sobject));
            pipelines.add(new SObjectPipeline(config, sobject, rest, bulkQuery, sobjectPubSub, governor, stored));
        }
        for (String event : events) {
            Map<String, Object> stored = context.offsetStorageReader()
                    .offset(PlatformEventOffset.partition(event));
            pipelines.add(new PlatformEventPipeline(config, event, pubsub, stored));
        }
        LOG.info("Salesforce source task started for SObjects {} and platform events {}",
                config.taskSobjects(), events);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (fatal != null) {
            throw fatal;
        }
        List<SourceRecord> out = new ArrayList<>();
        ConnectException firstFailure = null;
        for (SourcePipeline pipeline : pipelines) {
            Backoff backoff = backoffs.computeIfAbsent(pipeline, p -> new Backoff());
            if (!backoff.ready()) {
                continue; // retrying after a transient failure; the other pipelines keep flowing
            }
            try {
                List<SourceRecord> produced = pipeline.poll();
                out.addAll(produced);
                if (!produced.isEmpty()) {
                    backoff.reset();
                }
            } catch (SalesforceException e) {
                // A REST or Bulk call failed inside the pipeline (snapshot, polling, full-record
                // fetch, gap or lost-replay recovery).
                if (e.isRetryable()) {
                    long delay = backoff.failed();
                    LOG.warn("Pipeline for {} hit a transient Salesforce failure ({}); retrying in {} ms. "
                            + "Other pipelines are unaffected", pipeline.name(), e.getMessage(), delay);
                } else {
                    LOG.error("Pipeline for {} failed permanently", pipeline.name(), e);
                    if (fatal == null) {
                        fatal = new FatalPipelineException("Pipeline for " + pipeline.name()
                                + " failed and retrying cannot fix it: " + e.getMessage(), e);
                    }
                }
            } catch (FatalPipelineException e) {
                // Unrecoverable: hand over what the other pipelines produced, then fail the task.
                LOG.error("Pipeline for {} failed permanently", pipeline.name(), e);
                if (fatal == null) {
                    fatal = e;
                }
            } catch (ConnectException e) {
                // Isolate per-pipeline failures: emit what other pipelines produced first.
                LOG.error("Pipeline for {} failed", pipeline.name(), e);
                if (firstFailure == null) {
                    firstFailure = e;
                }
            }
        }
        if (out.isEmpty() && fatal != null) {
            throw fatal;
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
        for (SourcePipeline pipeline : pipelines) {
            try {
                pipeline.close();
            } catch (RuntimeException e) {
                LOG.warn("Error closing pipeline for {}", pipeline.name(), e);
            }
        }
        if (pubsub != null) {
            pubsub.close();
        }
    }
}
