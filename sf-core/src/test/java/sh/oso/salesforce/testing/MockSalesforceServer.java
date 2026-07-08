package sh.oso.salesforce.testing;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import sh.oso.salesforce.auth.AuthConfig;
import sh.oso.salesforce.auth.SalesforceAuth;
import sh.oso.salesforce.auth.SessionSupplier;
import sh.oso.salesforce.http.SalesforceHttpClient;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;

/**
 * A local fake of the Salesforce HTTP surface: OAuth token endpoint, REST (query/describe/
 * limits/composite via per-test stubs), and a stateful Bulk API 2.0 job lifecycle.
 *
 * <p>The token response's {@code instance_url} points back at this server, so a client
 * authenticated against it sends all subsequent traffic here.</p>
 */
public final class MockSalesforceServer implements AutoCloseable {

    public static final String ORG_ID = "00Dxx0000001gPFEAY";

    private final BulkJobStore bulkStore = new BulkJobStore();
    private final AtomicInteger tokenCounter = new AtomicInteger();
    private final WireMockServer server;
    private volatile String advertisedBaseUrl;

    public MockSalesforceServer() {
        this.server = new WireMockServer(WireMockConfiguration.options()
                .dynamicPort()
                .extensions(new BulkApiTransformer(bulkStore)));
    }

    public MockSalesforceServer start() {
        server.start();
        stubTokenEndpoint();
        stubDefaultLimits();
        stubBulkEndpoints();
        return this;
    }

    private void stubTokenEndpoint() {
        String base = advertisedBaseUrl != null ? advertisedBaseUrl : baseUrl();
        server.stubFor(post(urlPathEqualTo("/services/oauth2/token"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"access_token\":\"MOCK_TOKEN_" + tokenCounter.incrementAndGet()
                                + "\",\"instance_url\":\"" + base
                                + "\",\"id\":\"" + base + "/id/" + ORG_ID + "/005xx000001X8UzAAK\","
                                + "\"token_type\":\"Bearer\",\"issued_at\":\"0\"}")));
    }

    /**
     * Advertise a different base URL in token responses (e.g. {@code host.testcontainers.internal})
     * so clients running inside containers route back to this server.
     */
    public void advertise(String externalBaseUrl) {
        this.advertisedBaseUrl = externalBaseUrl;
        stubTokenEndpoint();
    }

    public int port() {
        return server.port();
    }

    private void stubDefaultLimits() {
        server.stubFor(get(urlPathMatching("/services/data/v[^/]+/limits/"))
                .willReturn(okJson("""
                        {
                          "DailyApiRequests": {"Max": 100000, "Remaining": 99000},
                          "DailyBulkV2QueryJobs": {"Max": 10000, "Remaining": 9990},
                          "DailyBulkApiBatches": {"Max": 15000, "Remaining": 14990}
                        }
                        """)));
    }

    private void stubBulkEndpoints() {
        server.stubFor(any(urlMatching("/services/data/v[^/]+/jobs/(query|ingest).*"))
                .willReturn(aResponse().withTransformers(BulkApiTransformer.NAME)));
    }

    /** Stubs a describe response for an SObject. */
    public void stubDescribe(String sobject, String describeJson) {
        server.stubFor(get(urlPathMatching("/services/data/v[^/]+/sobjects/" + sobject + "/describe/"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withHeader("Last-Modified", "Wed, 01 Jan 2026 00:00:00 GMT")
                        .withBody(describeJson)));
    }

    /** After calling this, describe requests carrying If-Modified-Since get a 304. */
    public void stubDescribeNotModified(String sobject) {
        server.stubFor(get(urlPathMatching("/services/data/v[^/]+/sobjects/" + sobject + "/describe/"))
                .withHeader("If-Modified-Since", equalTo("Wed, 01 Jan 2026 00:00:00 GMT"))
                .willReturn(aResponse().withStatus(304)));
    }

    /** Stubs a REST query (exact SOQL match) with a raw JSON response body. */
    public void stubQuery(String soql, String responseJson) {
        server.stubFor(get(urlPathMatching("/services/data/v[^/]+/query/"))
                .withQueryParam("q", equalTo(soql))
                .willReturn(okJson(responseJson)));
    }

    /** Stubs a follow-up page fetched via nextRecordsUrl. */
    public void stubQueryPage(String pagePath, String responseJson) {
        server.stubFor(get(urlPathEqualTo(pagePath)).willReturn(okJson(responseJson)));
    }

    public WireMockServer wireMock() {
        return server;
    }

    public BulkJobStore bulk() {
        return bulkStore;
    }

    public void enqueueBulkQueryResultPages(List<String> pages) {
        bulkStore.enqueueQueryResultPages(pages);
    }

    public String baseUrl() {
        return server.baseUrl();
    }

    /** AuthConfig for the client-credentials flow against this fake (uses a fake My Domain). */
    public AuthConfig authConfig() {
        return AuthConfig.builder()
                .grantType(AuthConfig.GrantType.CLIENT_CREDENTIALS)
                .instanceUrl(baseUrl())
                .consumerKey("mock-consumer-key")
                .consumerSecret("mock-consumer-secret")
                .build();
    }

    public SessionSupplier sessionSupplier() {
        // WireMock listens on localhost, which AuthConfig would reject as non-My-Domain;
        // the token endpoint override keeps validation on the happy path.
        return new SessionSupplier(new SalesforceAuth(authConfig(), baseUrl() + "/services/oauth2/token"));
    }

    public SalesforceHttpClient httpClient() {
        return new SalesforceHttpClient(sessionSupplier());
    }

    @Override
    public void close() {
        server.stop();
    }
}
