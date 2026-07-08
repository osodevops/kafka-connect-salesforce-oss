package sh.oso.salesforce.source;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.salesforce.testing.MockSalesforceServer;
import sh.oso.salesforce.testing.TestSchemas;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PollingSourceTaskTest {

    MockSalesforceServer sf;
    TaskHarness harness;

    @BeforeEach
    void setUp() {
        sf = new MockSalesforceServer().start();
        sf.stubDescribe("Account", TestSchemas.accountDescribeJson());
        harness = new TaskHarness(sf, null)
                .with(SourceConfig.REALTIME_MODE, "polling")
                .with(SourceConfig.SNAPSHOT_ENABLED, "false")
                .with(SourceConfig.INCLUDE_DELETED, "true");
    }

    @AfterEach
    void tearDown() {
        harness.close();
        sf.close();
    }

    @Test
    void pollingCapturesInsertsUpdatesAndSoftDeletes() throws Exception {
        sf.enqueueBulkQueryResultPages(List.of(
                "\"Id\",\"Name\",\"CreatedDate\",\"SystemModstamp\",\"IsDeleted\"\n"
                        + "\"001N\",\"New\",\"2024-02-01T10:00:00.000+0000\",\"2024-02-01T10:00:00.000+0000\",\"false\"\n"
                        + "\"001U\",\"Updated\",\"2024-01-01T10:00:00.000+0000\",\"2024-02-01T11:00:00.000+0000\",\"false\"\n"
                        + "\"001D\",\"Gone\",\"2024-01-01T10:00:00.000+0000\",\"2024-02-01T12:00:00.000+0000\",\"true\"\n"));

        harness.start();
        List<SourceRecord> records = harness.pollUntil(3, 5000);
        assertThat(records).hasSize(3);
        assertThat(((Struct) records.get(0).value()).getString("_EventType")).isEqualTo("created");
        assertThat(((Struct) records.get(1).value()).getString("_EventType")).isEqualTo("updated");
        assertThat(((Struct) records.get(2).value()).getString("_EventType")).isEqualTo("deleted");
        // epoch conversion applied to datetime fields
        assertThat(((Struct) records.get(0).value()).getInt64("SystemModstamp")).isNotNull();
        // queryAll used because sf.include.deleted=true
        assertThat(sf.bulk().allQueryJobs().get(0).operation).isEqualTo("queryAll");
        // cursor stored once the job completed
        assertThat(records.get(2).sourceOffset()).containsKey("lastSystemModstamp");
    }

    @Test
    void secondCycleFiltersByModstampCursor() throws Exception {
        sf.enqueueBulkQueryResultPages(List.of(
                "\"Id\",\"Name\",\"CreatedDate\",\"SystemModstamp\",\"IsDeleted\"\n"
                        + "\"001A\",\"A\",\"2024-02-01T10:00:00.000+0000\",\"2024-02-01T10:00:00.000+0000\",\"false\"\n"));
        harness.start();
        assertThat(harness.pollUntil(1, 5000)).hasSize(1);

        // Restarting reads the committed cursor and must filter the next query with it.
        sf.enqueueBulkQueryResultPages(List.of("\"Id\"\n"));
        harness.restart();
        harness.pollUntil(1, 1500);
        List<BulkJobSnapshot> jobs = sf.bulk().allQueryJobs().stream()
                .map(j -> new BulkJobSnapshot(j.soql)).toList();
        assertThat(jobs.get(0).soql).doesNotContain("WHERE SystemModstamp >");
        assertThat(jobs.get(1).soql).contains("WHERE SystemModstamp >");
    }

    private record BulkJobSnapshot(String soql) {
    }
}
