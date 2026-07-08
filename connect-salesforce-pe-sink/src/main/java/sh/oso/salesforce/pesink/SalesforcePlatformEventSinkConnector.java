package sh.oso.salesforce.pesink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Sink connector publishing Kafka records to a Salesforce Platform Event. */
public class SalesforcePlatformEventSinkConnector extends SinkConnector {

    private Map<String, String> originals;

    @Override
    public void start(Map<String, String> props) {
        this.originals = Map.copyOf(props);
        new PeSinkConfig(props); // fail fast
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SalesforcePlatformEventSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(originals);
        }
        return configs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return PeSinkConfig.configDef();
    }

    @Override
    public String version() {
        return Version.VERSION;
    }
}
