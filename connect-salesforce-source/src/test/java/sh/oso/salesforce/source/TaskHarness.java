package sh.oso.salesforce.source;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import sh.oso.salesforce.testing.FakePubSubServer;
import sh.oso.salesforce.testing.MockSalesforceServer;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Drives a SalesforceSourceTask against the local fakes, simulating Connect's
 * offset storage: offsets from polled records become visible to restarted tasks.
 */
final class TaskHarness implements AutoCloseable {

    final MockSalesforceServer sf;
    final FakePubSubServer pubsub;
    private final Map<String, String> props = new HashMap<>();
    private final Map<Map<String, Object>, Map<String, Object>> committedOffsets = new HashMap<>();
    private SalesforceSourceTask task;

    TaskHarness(MockSalesforceServer sf, FakePubSubServer pubsub) {
        this.sf = sf;
        this.pubsub = pubsub;
        props.put(SourceConfig.AUTH_GRANT_TYPE, "client_credentials");
        props.put(SourceConfig.INSTANCE_URL, sf.baseUrl());
        props.put(SourceConfig.CONSUMER_KEY, "k");
        props.put(SourceConfig.CONSUMER_SECRET, "s");
        props.put(SourceConfig.TOKEN_ENDPOINT, sf.baseUrl() + "/services/oauth2/token");
        props.put(SourceConfig.TOPIC_PREFIX, "salesforce");
        props.put(SourceConfig.SOBJECTS, "Account");
        if (pubsub != null) {
            props.put(SourceConfig.PUBSUB_ENDPOINT, pubsub.endpoint());
            props.put(SourceConfig.PUBSUB_PLAINTEXT, "true");
        }
    }

    TaskHarness with(String key, String value) {
        props.put(key, value);
        return this;
    }

    SalesforceSourceTask start() {
        task = new SalesforceSourceTask();
        task.initialize(new SourceTaskContext() {
            @Override
            public Map<String, String> configs() {
                return props;
            }

            @Override
            public OffsetStorageReader offsetStorageReader() {
                return new OffsetStorageReader() {
                    @Override
                    public <T> Map<String, Object> offset(Map<String, T> partition) {
                        return committedOffsets.get(partition);
                    }

                    @Override
                    public <T> Map<Map<String, T>, Map<String, Object>> offsets(
                            Collection<Map<String, T>> partitions) {
                        Map<Map<String, T>, Map<String, Object>> out = new HashMap<>();
                        for (Map<String, T> partition : partitions) {
                            out.put(partition, committedOffsets.get(partition));
                        }
                        return out;
                    }
                };
            }
        });
        task.start(props);
        return task;
    }

    /** Polls, recording offsets like the Connect framework would on commit. */
    @SuppressWarnings("unchecked")
    List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = task.poll();
        if (records != null) {
            for (SourceRecord record : records) {
                committedOffsets.put((Map<String, Object>) record.sourcePartition(),
                        (Map<String, Object>) record.sourceOffset());
            }
        }
        return records == null ? List.of() : records;
    }

    /** Polls until at least n records accumulate or the deadline passes. */
    List<SourceRecord> pollUntil(int n, long timeoutMillis) throws InterruptedException {
        List<SourceRecord> out = new java.util.ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (out.size() < n && System.currentTimeMillis() < deadline) {
            out.addAll(poll());
        }
        return out;
    }

    void restart() {
        task.stop();
        start();
    }

    SourceTask task() {
        return task;
    }

    @Override
    public void close() {
        if (task != null) {
            task.stop();
        }
    }
}
