package sh.oso.salesforce.rest;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import sh.oso.salesforce.common.SalesforceException;
import sh.oso.salesforce.testing.MockSalesforceServer;
import sh.oso.salesforce.testing.TestSchemas;

import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RestClientTest {

    MockSalesforceServer sf;
    RestClient rest;

    @BeforeEach
    void setUp() {
        sf = new MockSalesforceServer().start();
        rest = new RestClient(sf.httpClient(), "60.0");
    }

    @AfterEach
    void tearDown() {
        sf.close();
    }

    @Test
    void queryFollowsNextRecordsUrl() {
        String soql = "SELECT Id, Name FROM Account";
        sf.stubQuery(soql, """
                {"done": false, "totalSize": 3,
                 "nextRecordsUrl": "/services/data/v60.0/query/01g-page2",
                 "records": [
                   {"attributes":{"type":"Account"}, "Id":"001A", "Name":"Acme"},
                   {"attributes":{"type":"Account"}, "Id":"001B", "Name":"Beta"}
                 ]}
                """);
        sf.stubQueryPage("/services/data/v60.0/query/01g-page2", """
                {"done": true, "totalSize": 3,
                 "records": [{"attributes":{"type":"Account"}, "Id":"001C", "Name":"Gamma"}]}
                """);

        List<JsonNode> records = rest.queryAllPages(soql, false);
        assertThat(records).hasSize(3);
        assertThat(records.get(2).path("Id").asText()).isEqualTo("001C");
    }

    @Test
    void describeUsesIfModifiedSinceCaching() {
        sf.stubDescribe("Account", TestSchemas.accountDescribeJson());
        JsonNode first = rest.describe("Account");
        assertThat(first.path("name").asText()).isEqualTo("Account");

        sf.stubDescribeNotModified("Account");
        JsonNode second = rest.describe("Account");
        assertThat(second).isSameAs(first);
        sf.wireMock().verify(2, getRequestedFor(
                urlPathMatching("/services/data/v[^/]+/sobjects/Account/describe/")));
    }

    @ParameterizedTest
    @CsvSource({"503,SERVER_UNAVAILABLE,true", "500,UNKNOWN_EXCEPTION,true", "429,TOO_MANY_REQUESTS,true",
            "403,REQUEST_LIMIT_EXCEEDED,true", "403,INSUFFICIENT_ACCESS,false", "404,NOT_FOUND,false",
            "400,INVALID_TYPE,false"})
    void describeFailureIsRetryableOnlyWhenALaterAttemptCouldSucceed(int status, String errorCode,
                                                                    boolean retryable) {
        sf.wireMock().stubFor(get(urlPathMatching("/services/data/v[^/]+/sobjects/Account/describe/"))
                .willReturn(aResponse().withStatus(status)
                        .withBody("[{\"errorCode\":\"" + errorCode + "\",\"message\":\"x\"}]")));

        assertThatThrownBy(() -> rest.describe("Account"))
                .isInstanceOfSatisfying(SalesforceException.class,
                        e -> assertThat(e.isRetryable()).isEqualTo(retryable))
                .hasMessageContaining("HTTP " + status);
    }

    @ParameterizedTest
    @CsvSource({"503,true", "429,true", "400,false"})
    void retrieveFailureIsRetryableOnlyWhenALaterAttemptCouldSucceed(int status, boolean retryable) {
        sf.wireMock().stubFor(get(urlPathMatching("/services/data/v[^/]+/sobjects/Account/001A"))
                .willReturn(aResponse().withStatus(status)));

        assertThatThrownBy(() -> rest.retrieve("Account", "001A", null))
                .isInstanceOfSatisfying(SalesforceException.class,
                        e -> assertThat(e.isRetryable()).isEqualTo(retryable));
    }

    @Test
    void retrieveOfAMissingRecordIsEmptyNotAFailure() {
        sf.wireMock().stubFor(get(urlPathMatching("/services/data/v[^/]+/sobjects/Account/001GONE"))
                .willReturn(aResponse().withStatus(404)));
        assertThat(rest.retrieve("Account", "001GONE", null)).isEmpty();
    }

    @Test
    void limitsReturnsQuota() {
        JsonNode limits = rest.limits();
        assertThat(limits.path("DailyApiRequests").path("Max").asLong()).isEqualTo(100000);
    }
}
