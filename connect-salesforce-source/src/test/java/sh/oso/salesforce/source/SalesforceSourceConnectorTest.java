package sh.oso.salesforce.source;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SalesforceSourceConnectorTest {

    private Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put(SourceConfig.AUTH_GRANT_TYPE, "client_credentials");
        props.put(SourceConfig.INSTANCE_URL, "https://acme.my.salesforce.com");
        props.put(SourceConfig.CONSUMER_KEY, "k");
        props.put(SourceConfig.CONSUMER_SECRET, "s");
        props.put(SourceConfig.TOPIC_PREFIX, "salesforce");
        return props;
    }

    @Test
    void distributesSObjectsRoundRobinAcrossTasks() {
        Map<String, String> props = baseProps();
        props.put(SourceConfig.SOBJECTS, "Account,Contact,Opportunity");

        SalesforceSourceConnector connector = new SalesforceSourceConnector();
        connector.start(props);
        List<Map<String, String>> configs = connector.taskConfigs(2);
        assertThat(configs).hasSize(2);
        assertThat(configs.get(0).get(SourceConfig.TASK_SOBJECTS)).isEqualTo("Account,Opportunity");
        assertThat(configs.get(1).get(SourceConfig.TASK_SOBJECTS)).isEqualTo("Contact");

        // more tasks than SObjects: capped
        assertThat(connector.taskConfigs(9)).hasSize(3);
    }

    @Test
    void rejectsTooManySObjects() {
        Map<String, String> props = baseProps();
        props.put(SourceConfig.SOBJECTS, "A,B,C,D,E,F");
        assertThatThrownBy(() -> new SalesforceSourceConnector().start(props))
                .isInstanceOf(ConfigException.class);
        props.put(SourceConfig.SOBJECTS_MAX, "6");
        new SalesforceSourceConnector().start(props); // now allowed
    }

    @Test
    void rejectsLoginHostForClientCredentials() {
        Map<String, String> props = baseProps();
        props.put(SourceConfig.SOBJECTS, "Account");
        props.put(SourceConfig.INSTANCE_URL, "https://login.salesforce.com");
        assertThatThrownBy(() -> new SalesforceSourceConnector().start(props))
                .hasMessageContaining("My Domain");
    }
}
