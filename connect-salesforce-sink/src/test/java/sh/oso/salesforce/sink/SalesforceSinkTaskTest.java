package sh.oso.salesforce.sink;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import sh.oso.salesforce.testing.MockSalesforceServer;
import sh.oso.salesforce.testing.TestSchemas;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.deleteRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.patch;
import static com.github.tomakehurst.wiremock.client.WireMock.patchRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SalesforceSinkTaskTest {

    MockSalesforceServer sf;
    SalesforceSinkTask task;
    ErrantRecordReporter reporter;

    @BeforeEach
    void setUp() {
        sf = new MockSalesforceServer().start();
        sf.stubDescribe("Account", TestSchemas.accountDescribeJson());
    }

    @AfterEach
    void tearDown() {
        if (task != null) {
            task.stop();
        }
        sf.close();
    }

    private Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put(SinkConfig.AUTH_GRANT_TYPE, "client_credentials");
        props.put(SinkConfig.INSTANCE_URL, sf.baseUrl());
        props.put(SinkConfig.CONSUMER_KEY, "k");
        props.put(SinkConfig.CONSUMER_SECRET, "s");
        props.put(SinkConfig.TOKEN_ENDPOINT, sf.baseUrl() + "/services/oauth2/token");
        props.put(SinkConfig.OBJECTS, "Account");
        props.put("sf.Account.topics", "salesforce.Account");
        return props;
    }

    private void startTask(Map<String, String> props) {
        task = new SalesforceSinkTask();
        reporter = mock(ErrantRecordReporter.class);
        SinkTaskContext context = mock(SinkTaskContext.class);
        when(context.errantRecordReporter()).thenReturn(reporter);
        task.initialize(context);
        task.start(props);
    }

    private SinkRecord record(String eventType, Map<String, Object> fields) {
        Map<String, Object> value = new HashMap<>(fields);
        if (eventType != null) {
            value.put("_EventType", eventType);
            value.put("_ObjectType", "Account");
        }
        return new SinkRecord("salesforce.Account", 0, null, fields.get("Id"), null, value, 0);
    }

    @Test
    void eventTypeDrivesInsertUpdateDeleteViaRest() {
        sf.wireMock().stubFor(post(urlPathMatching("/services/data/v[^/]+/composite/sobjects"))
                .willReturn(okJson("[{\"id\":\"001N\",\"success\":true}]")));
        sf.wireMock().stubFor(patch(urlPathMatching("/services/data/v[^/]+/composite/sobjects"))
                .willReturn(okJson("[{\"id\":\"001U\",\"success\":true}]")));
        sf.wireMock().stubFor(delete(urlPathMatching("/services/data/v[^/]+/composite/sobjects"))
                .willReturn(okJson("[{\"id\":\"001D\",\"success\":true}]")));

        startTask(baseProps());
        task.put(List.of(
                record("created", Map.of("Name", "New Co")),
                record("updated", Map.of("Id", "001U", "Name", "Updated Co")),
                record("deleted", Map.of("Id", "001D"))));

        sf.wireMock().verify(postRequestedFor(urlPathMatching("/services/data/v[^/]+/composite/sobjects"))
                .withRequestBody(matchingJsonPath("$.records[0].Name", com.github.tomakehurst.wiremock.client.WireMock.equalTo("New Co"))));
        sf.wireMock().verify(patchRequestedFor(urlPathMatching("/services/data/v[^/]+/composite/sobjects"))
                .withRequestBody(matchingJsonPath("$.records[0].Id",
                        com.github.tomakehurst.wiremock.client.WireMock.equalTo("001U"))));
        sf.wireMock().verify(deleteRequestedFor(urlPathMatching("/services/data/v[^/]+/composite/sobjects")));
    }

    @Test
    void readOnlyFieldsAreSilentlyExcluded() {
        sf.wireMock().stubFor(post(urlPathMatching("/services/data/v[^/]+/composite/sobjects"))
                .willReturn(okJson("[{\"id\":\"001N\",\"success\":true}]")));
        startTask(baseProps());
        // IsDeleted/CreatedDate/SystemModstamp are createable=false in the describe fixture.
        task.put(List.of(record("created", Map.of(
                "Name", "New Co", "IsDeleted", false, "CreatedDate", 1706693400000L))));

        sf.wireMock().verify(postRequestedFor(urlPathMatching("/services/data/v[^/]+/composite/sobjects"))
                .withRequestBody(matchingJsonPath("$.records[0].Name"))
                .withRequestBody(com.github.tomakehurst.wiremock.client.WireMock.notMatching(".*IsDeleted.*")));
    }

    @Test
    void externalIdUpsertUsesConfiguredField() {
        sf.wireMock().stubFor(patch(urlPathMatching(
                "/services/data/v[^/]+/composite/sobjects/Account/External_Id__c"))
                .willReturn(okJson("[{\"id\":\"001X\",\"success\":true,\"created\":true}]")));
        Map<String, String> props = baseProps();
        props.put("sf.Account.use.custom.id.field", "true");
        props.put("sf.Account.custom.id.field.name", "External_Id__c");
        startTask(props);

        task.put(List.of(record("created", Map.of("Name", "Cross Org", "External_Id__c", "EXT-1"))));
        sf.wireMock().verify(patchRequestedFor(urlPathMatching(
                "/services/data/v[^/]+/composite/sobjects/Account/External_Id__c"))
                .withRequestBody(matchingJsonPath("$.records[0].External_Id__c",
                        com.github.tomakehurst.wiremock.client.WireMock.equalTo("EXT-1"))));
    }

    @Test
    void bulkModeRunsIngestJobAndRoutesFailuresToDlq() {
        sf.bulk().setIngestRowFailer(row -> row.contains("Bad") ? "REQUIRED_FIELD_MISSING" : null);
        Map<String, String> props = baseProps();
        props.put(SinkConfig.WRITE_MODE, "bulk2");
        props.put(SinkConfig.BEHAVIOR_ON_API_ERRORS, "log");
        startTask(props);

        task.put(List.of(
                record("created", Map.of("Name", "Good Co")),
                record("created", Map.of("Name", "Bad")),
                record("created", Map.of("Name", "Also Good"))));

        assertThat(sf.bulk().allIngestJobs()).hasSize(1);
        assertThat(sf.bulk().allIngestJobs().get(0).operation).isEqualTo("insert");
        assertThat(sf.bulk().allIngestJobs().get(0).uploadedCsv).contains("Good Co");

        ArgumentCaptor<SinkRecord> captor = ArgumentCaptor.forClass(SinkRecord.class);
        verify(reporter).report(captor.capture(), any());
        @SuppressWarnings("unchecked")
        Map<String, Object> failedValue = (Map<String, Object>) captor.getValue().value();
        assertThat(failedValue.get("Name")).isEqualTo("Bad");
    }

    @Test
    void behaviorFailThrowsOnRejectedRecords() {
        sf.bulk().setIngestRowFailer(row -> "DUPLICATE_VALUE");
        Map<String, String> props = baseProps();
        props.put(SinkConfig.WRITE_MODE, "bulk2");
        startTask(props);

        assertThatThrownBy(() -> task.put(List.of(record("created", Map.of("Name", "Dup")))))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("rejected");
    }

    @Test
    void bigObjectRejectsNonInsertOperations() {
        sf.stubDescribe("Audit__b", """
                {"name":"Audit__b","fields":[
                  {"name":"Detail__c","type":"string","createable":true,"updateable":false}]}
                """);
        Map<String, String> props = baseProps();
        props.put(SinkConfig.OBJECTS, "Audit__b");
        props.put("sf.Audit__b.topics", "salesforce.Account");
        props.put("sf.Audit__b.type", "big_object");
        props.put(SinkConfig.WRITE_MODE, "bulk2");
        props.put(SinkConfig.BEHAVIOR_ON_API_ERRORS, "log");
        startTask(props);

        task.put(List.of(
                record("created", Map.of("Detail__c", "row1")),
                record("updated", Map.of("Detail__c", "row2"))));

        // insert went through as a Bulk job; the update was reported, not written
        assertThat(sf.bulk().allIngestJobs()).hasSize(1);
        verify(reporter).report(any(), any());
    }
}
