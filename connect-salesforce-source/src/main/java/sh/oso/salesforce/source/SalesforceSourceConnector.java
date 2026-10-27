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
 * sync (Pub/Sub API) or periodic polling (Bulk API 2.0), per SObject; and platform event
 * streams over the Pub/Sub API.
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

    /** Spreads SObjects, then platform events, round-robin across at most one task per stream. */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        SourceConfig config = new SourceConfig(originals);
        List<String> sobjects = config.sobjects();
        List<String> events = config.platformEvents();
        int streams = sobjects.size() + events.size();
        int taskCount = Math.min(maxTasks, streams);
        if (maxTasks > streams) {
            LOG.info("tasks.max={} exceeds the {} SObject(s) and platform event(s); {} task(s) will be created",
                    maxTasks, streams, taskCount);
        }
        List<List<String>> sobjectAssignments = new ArrayList<>();
        List<List<String>> eventAssignments = new ArrayList<>();
        for (int i = 0; i < taskCount; i++) {
            sobjectAssignments.add(new ArrayList<>());
            eventAssignments.add(new ArrayList<>());
        }
        int slot = 0;
        for (String sobject : sobjects) {
            sobjectAssignments.get(slot++ % taskCount).add(sobject);
        }
        for (String event : events) {
            eventAssignments.get(slot++ % taskCount).add(event);
        }
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < taskCount; i++) {
            Map<String, String> taskConfig = new HashMap<>(originals);
            taskConfig.put(SourceConfig.TASK_SOBJECTS, String.join(",", sobjectAssignments.get(i)));
            taskConfig.put(SourceConfig.TASK_PLATFORM_EVENTS, String.join(",", eventAssignments.get(i)));
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
