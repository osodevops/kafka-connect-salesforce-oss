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

    @Test
    void distributesSObjectsAndPlatformEventsRoundRobin() {
        Map<String, String> props = baseProps();
        props.put(SourceConfig.SOBJECTS, "Account,Contact");
        props.put(SourceConfig.PLATFORM_EVENTS, "Order_Shipped__e,Integration_Message__e,Invoice_Ready__e");

        SalesforceSourceConnector connector = new SalesforceSourceConnector();
        connector.start(props);
        List<Map<String, String>> configs = connector.taskConfigs(2);
        assertThat(configs).hasSize(2);
        assertThat(configs.get(0).get(SourceConfig.TASK_SOBJECTS)).isEqualTo("Account");
        assertThat(configs.get(0).get(SourceConfig.TASK_PLATFORM_EVENTS))
                .isEqualTo("Order_Shipped__e,Invoice_Ready__e");
        assertThat(configs.get(1).get(SourceConfig.TASK_SOBJECTS)).isEqualTo("Contact");
        assertThat(configs.get(1).get(SourceConfig.TASK_PLATFORM_EVENTS)).isEqualTo("Integration_Message__e");

        // one task per stream at most
        assertThat(connector.taskConfigs(9)).hasSize(5);
    }

    @Test
    void eventOnlyTaskDoesNotInheritAllSObjects() {
        Map<String, String> props = baseProps();
        props.put(SourceConfig.SOBJECTS, "Account");
        props.put(SourceConfig.PLATFORM_EVENTS, "Order_Shipped__e");
        SalesforceSourceConnector connector = new SalesforceSourceConnector();
        connector.start(props);

        SourceConfig eventTask = new SourceConfig(connector.taskConfigs(2).get(1));
        assertThat(eventTask.taskSobjects()).isEmpty();
        assertThat(eventTask.taskPlatformEvents()).containsExactly("Order_Shipped__e");
    }

    @Test
    void platformEventsAloneAreAValidConfig() {
        Map<String, String> props = baseProps();
        props.put(SourceConfig.PLATFORM_EVENTS, "Order_Shipped__e");
        SourceConfig config = new SourceConfig(props);
        assertThat(config.sobjects()).isEmpty();
        assertThat(config.taskPlatformEvents()).containsExactly("Order_Shipped__e");
        assertThat(config.topicFor("Order_Shipped__e")).isEqualTo("salesforce.Order_Shipped__e");
    }

    @Test
    void requiresSObjectsOrPlatformEvents() {
        assertThatThrownBy(() -> new SalesforceSourceConnector().start(baseProps()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(SourceConfig.SOBJECTS)
                .hasMessageContaining(SourceConfig.PLATFORM_EVENTS);
    }

    @Test
    void rejectsPlatformEventWithoutSuffix() {
        Map<String, String> props = baseProps();
        props.put(SourceConfig.PLATFORM_EVENTS, "Order_Shipped");
        assertThatThrownBy(() -> new SourceConfig(props)).hasMessageContaining("__e");
    }

    @Test
    void rejectsPlatformEventListedAsSObject() {
        Map<String, String> props = baseProps();
        props.put(SourceConfig.SOBJECTS, "Account,Order_Shipped__e");
        assertThatThrownBy(() -> new SourceConfig(props))
                .hasMessageContaining("Order_Shipped__e is a platform event")
                .hasMessageContaining(SourceConfig.PLATFORM_EVENTS);
    }

    @Test
    void rejectsDuplicates() {
        Map<String, String> props = baseProps();
        props.put(SourceConfig.PLATFORM_EVENTS, "Order_Shipped__e,Order_Shipped__e");
        assertThatThrownBy(() -> new SourceConfig(props)).hasMessageContaining("more than once");
    }

    @Test
    void sobjectsMaxIgnoresPlatformEvents() {
        Map<String, String> props = baseProps();
        props.put(SourceConfig.SOBJECTS, "A,B,C,D,E");
        props.put(SourceConfig.PLATFORM_EVENTS, "One__e,Two__e,Three__e");
        new SalesforceSourceConnector().start(props); // 5 SObjects is within the default max
    }
}

