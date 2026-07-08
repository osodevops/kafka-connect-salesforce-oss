package sh.oso.salesforce.streaming;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Legacy CometD streaming source connector for orgs without Pub/Sub API access.
 * One subscription per connector: {@code tasks.max} is effectively 1.
 */
public class SalesforceStreamingSourceConnector extends SourceConnector {

    private static final Logger LOG = LoggerFactory.getLogger(SalesforceStreamingSourceConnector.class);

    private Map<String, String> originals;

    @Override
    public void start(Map<String, String> props) {
        this.originals = Map.copyOf(props);
        new StreamingConfig(props); // fail fast
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SalesforceStreamingSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks > 1) {
            LOG.info("Streaming source supports a single subscription; ignoring tasks.max={}", maxTasks);
        }
        return List.of(originals);
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return StreamingConfig.configDef();
    }

    @Override
    public String version() {
        return Version.VERSION;
    }
}
