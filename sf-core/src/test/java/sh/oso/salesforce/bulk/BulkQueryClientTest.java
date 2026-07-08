package sh.oso.salesforce.bulk;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.salesforce.common.SalesforceException;
import sh.oso.salesforce.testing.MockSalesforceServer;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BulkQueryClientTest {

    MockSalesforceServer sf;
    BulkQueryClient bulk;

    @BeforeEach
    void setUp() {
        sf = new MockSalesforceServer().start();
        bulk = new BulkQueryClient(sf.httpClient(), "60.0");
    }

    @AfterEach
    void tearDown() {
        sf.close();
    }

    @Test
    void queryJobLifecyclePaginatesUntilLocatorNull() {
        sf.enqueueBulkQueryResultPages(List.of(
                "\"Id\",\"Name\"\n\"001A\",\"Acme\"\n\"001B\",\"Beta\"\n",
                "\"Id\",\"Name\"\n\"001C\",\"Gamma\"\n"));

        String jobId = bulk.createJob("SELECT Id, Name FROM Account", false);
        assertThat(bulk.getStatus(jobId).isComplete()).isTrue();

        BulkQueryClient.ResultPage page1 = bulk.fetchResults(jobId, null, 1000);
        assertThat(page1.records()).hasSize(2);
        assertThat(page1.records().get(0).get("Name")).isEqualTo("Acme");
        assertThat(page1.nextLocator()).isNotNull();

        BulkQueryClient.ResultPage page2 = bulk.fetchResults(jobId, page1.nextLocator(), 1000);
        assertThat(page2.records()).hasSize(1);
        assertThat(page2.nextLocator()).isNull();
    }

    @Test
    void queryAllUsesQueryAllOperation() {
        sf.enqueueBulkQueryResultPages(List.of("\"Id\"\n\"001A\"\n"));
        bulk.createJob("SELECT Id FROM Account", true);
        assertThat(sf.bulk().allQueryJobs().get(0).operation).isEqualTo("queryAll");
    }

    @Test
    void rejectsForbiddenSoql() {
        assertThatThrownBy(() -> BulkQueryClient.validateSoql("SELECT Id FROM Account ORDER BY Name"))
                .isInstanceOf(SalesforceException.class).hasMessageContaining("ORDER BY");
        assertThatThrownBy(() -> BulkQueryClient.validateSoql(
                "SELECT Id, (SELECT Id FROM Contacts) FROM Account"))
                .isInstanceOf(SalesforceException.class).hasMessageContaining("subqueries");
        BulkQueryClient.validateSoql("SELECT Id, Name FROM Account WHERE SystemModstamp > 2024-01-01T00:00:00Z");
    }

    @Test
    void nullCsvValuesParseAsNull() {
        List<java.util.Map<String, String>> rows =
                BulkQueryClient.parseCsv("\"Id\",\"Industry\"\n\"001A\",\"\"\n");
        assertThat(rows.get(0).get("Industry")).isNull();
    }
}
