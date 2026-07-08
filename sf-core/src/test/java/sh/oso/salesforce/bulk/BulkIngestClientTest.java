package sh.oso.salesforce.bulk;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.salesforce.common.SalesforceException;
import sh.oso.salesforce.testing.MockSalesforceServer;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BulkIngestClientTest {

    MockSalesforceServer sf;
    BulkIngestClient bulk;

    @BeforeEach
    void setUp() {
        sf = new MockSalesforceServer().start();
        bulk = new BulkIngestClient(sf.httpClient(), "60.0");
    }

    @AfterEach
    void tearDown() {
        sf.close();
    }

    @Test
    void ingestLifecycleReportsSuccessAndFailures() {
        sf.bulk().setIngestRowFailer(row ->
                row.contains("Bad") ? "REQUIRED_FIELD_MISSING:Name is required" : null);

        String jobId = bulk.createJob("Account", BulkIngestClient.Operation.INSERT, null);
        bulk.uploadCsv(jobId, "Name,Industry\nAcme,Tech\nBad,\nBeta,Retail\n".getBytes(StandardCharsets.UTF_8));
        bulk.markUploadComplete(jobId);

        BulkIngestClient.IngestJobStatus status = bulk.getStatus(jobId);
        assertThat(status.state()).isEqualTo("InProgress");
        status = bulk.getStatus(jobId);
        assertThat(status.isComplete()).isTrue();
        assertThat(status.recordsProcessed()).isEqualTo(3);
        assertThat(status.recordsFailed()).isEqualTo(1);

        List<Map<String, String>> ok = bulk.successfulResults(jobId);
        assertThat(ok).hasSize(2);
        assertThat(ok.get(0).get("sf__Id")).isNotBlank();

        List<Map<String, String>> failed = bulk.failedResults(jobId);
        assertThat(failed).hasSize(1);
        assertThat(failed.get(0).get("sf__Error")).contains("REQUIRED_FIELD_MISSING");
    }

    @Test
    void upsertRequiresExternalIdField() {
        assertThatThrownBy(() -> bulk.createJob("Account", BulkIngestClient.Operation.UPSERT, null))
                .isInstanceOf(SalesforceException.class)
                .hasMessageContaining("external ID");
        String jobId = bulk.createJob("Account", BulkIngestClient.Operation.UPSERT, "External_Id__c");
        assertThat(sf.bulk().ingestJob(jobId).externalIdFieldName).isEqualTo("External_Id__c");
    }

    @Test
    void csvEncoderWritesSetNullSentinel() {
        java.util.LinkedHashMap<String, Object> row = new java.util.LinkedHashMap<>();
        row.put("Name", "Acme");
        row.put("Industry", null);
        byte[] csv = CsvEncoder.encode(List.of(row), true);
        // commons-csv quotes the leading '#'; Salesforce unquotes per RFC 4180 before
        // interpreting the set-null sentinel, so the quoted form is equivalent.
        assertThat(new String(csv, StandardCharsets.UTF_8)).isEqualTo("Name,Industry\nAcme,\"#N/A\"\n");
    }
}
