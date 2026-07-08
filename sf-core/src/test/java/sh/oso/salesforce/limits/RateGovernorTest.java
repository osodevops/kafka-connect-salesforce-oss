package sh.oso.salesforce.limits;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.salesforce.common.SalesforceException;
import sh.oso.salesforce.rest.RestClient;
import sh.oso.salesforce.testing.MockSalesforceServer;

import java.time.Duration;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RateGovernorTest {

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
    void allowsWhenQuotaHealthy() {
        RateGovernor governor = new RateGovernor(rest, 0.05, Duration.ZERO);
        assertThatCode(() -> governor.acquire(RateGovernor.DAILY_API_REQUESTS))
                .doesNotThrowAnyException();
    }

    @Test
    void throwsRetryableWhenBelowReserve() {
        sf.wireMock().stubFor(get(urlPathMatching("/services/data/v[^/]+/limits/"))
                .willReturn(okJson("""
                        {"DailyApiRequests": {"Max": 100000, "Remaining": 2000}}
                        """)));
        RateGovernor governor = new RateGovernor(rest, 0.05, Duration.ZERO);
        assertThatThrownBy(() -> governor.acquire(RateGovernor.DAILY_API_REQUESTS))
                .isInstanceOf(SalesforceException.class)
                .hasMessageContaining("nearly exhausted")
                .matches(e -> ((SalesforceException) e).isRetryable());
    }

    @Test
    void tracksLocalConsumptionBetweenRefreshes() {
        sf.wireMock().stubFor(get(urlPathMatching("/services/data/v[^/]+/limits/"))
                .willReturn(okJson("""
                        {"DailyApiRequests": {"Max": 1000, "Remaining": 60}}
                        """)));
        RateGovernor governor = new RateGovernor(rest, 0.05, Duration.ofHours(1));
        governor.acquire(RateGovernor.DAILY_API_REQUESTS); // 6% remaining: OK
        governor.consumed(RateGovernor.DAILY_API_REQUESTS, 15); // now 4.5%
        assertThatThrownBy(() -> governor.acquire(RateGovernor.DAILY_API_REQUESTS))
                .isInstanceOf(SalesforceException.class);
    }
}
