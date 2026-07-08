package sh.oso.salesforce.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unified Salesforce source connector: historical snapshot (Bulk API 2.0) plus event-driven
 * sync (Pub/Sub API) or periodic polling (Bulk API 2.0), per SObject.
 */
public class SalesforceSourceConnector extends SourceConnector {

    private static final Logger LOG = LoggerFactory.getLogger(SalesforceSourceConnector.class);

    private Map<String, String> originals;

    @Override
    public void start(Map<String, String> props) {
        this.originals = Map.copyOf(props);
        new SourceConfig(props); // fail fast on invalid config
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SalesforceSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<String> sobjects = new SourceConfig(originals).sobjects();
        int taskCount = Math.min(maxTasks, sobjects.size());
        if (maxTasks > sobjects.size()) {
            LOG.info("tasks.max={} exceeds SObject count {}; {} task(s) will be created",
                    maxTasks, sobjects.size(), taskCount);
        }
        List<List<String>> assignments = new ArrayList<>();
        for (int i = 0; i < taskCount; i++) {
            assignments.add(new ArrayList<>());
        }
        for (int i = 0; i < sobjects.size(); i++) {
            assignments.get(i % taskCount).add(sobjects.get(i));
        }
        List<Map<String, String>> configs = new ArrayList<>();
        for (List<String> assignment : assignments) {
            Map<String, String> taskConfig = new HashMap<>(originals);
            taskConfig.put(SourceConfig.TASK_SOBJECTS, String.join(",", assignment));
            configs.add(taskConfig);
        }
        return configs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return SourceConfig.configDef();
    }

    @Override
    public String version() {
        return Version.VERSION;
    }
}
