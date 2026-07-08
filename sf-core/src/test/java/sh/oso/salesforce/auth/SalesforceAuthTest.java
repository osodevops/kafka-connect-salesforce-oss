package sh.oso.salesforce.auth;

import com.github.tomakehurst.wiremock.client.WireMock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import sh.oso.salesforce.common.SalesforceException;
import sh.oso.salesforce.http.RequestSpec;
import sh.oso.salesforce.http.SalesforceHttpClient;
import sh.oso.salesforce.testing.MockSalesforceServer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Base64;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SalesforceAuthTest {

    static MockSalesforceServer sf;

    @BeforeAll
    static void setUp() {
        sf = new MockSalesforceServer().start();
    }

    @AfterAll
    static void tearDown() {
        sf.close();
    }

    @Test
    void clientCredentialsAuthenticates() {
        Session session = new SalesforceAuth(sf.authConfig(),
                sf.baseUrl() + "/services/oauth2/token").authenticate();
        assertThat(session.accessToken()).startsWith("MOCK_TOKEN");
        assertThat(session.instanceUrl()).isEqualTo(sf.baseUrl());
        assertThat(session.orgId()).isEqualTo(MockSalesforceServer.ORG_ID);
        sf.wireMock().verify(postRequestedFor(urlPathEqualTo("/services/oauth2/token"))
                .withRequestBody(containing("grant_type=client_credentials")));
    }

    @Test
    void clientCredentialsRejectsLoginSalesforceCom() {
        assertThatThrownBy(() -> AuthConfig.builder()
                .grantType(AuthConfig.GrantType.CLIENT_CREDENTIALS)
                .instanceUrl("https://login.salesforce.com")
                .consumerKey("k").consumerSecret("s")
                .build())
                .isInstanceOf(SalesforceException.class)
                .hasMessageContaining("My Domain");
    }

    @Test
    void jwtBearerSendsSignedAssertion(@TempDir Path tempDir) throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keyPair = generator.generateKeyPair();
        Path pem = tempDir.resolve("sf.key");
        Files.writeString(pem, "-----BEGIN PRIVATE KEY-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes())
                        .encodeToString(keyPair.getPrivate().getEncoded())
                + "\n-----END PRIVATE KEY-----\n");

        AuthConfig config = AuthConfig.builder()
                .grantType(AuthConfig.GrantType.JWT_BEARER)
                .instanceUrl(sf.baseUrl())
                .consumerKey("jwt-consumer-key")
                .username("integration@example.org")
                .jwtKeyPath(pem.toString())
                .build();
        Session session = new SalesforceAuth(config,
                sf.baseUrl() + "/services/oauth2/token").authenticate();
        assertThat(session.accessToken()).startsWith("MOCK_TOKEN");
        sf.wireMock().verify(postRequestedFor(urlPathEqualTo("/services/oauth2/token"))
                .withRequestBody(containing("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer"))
                .withRequestBody(containing("assertion=")));
    }

    @Test
    void reAuthenticatesTransparentlyOn401() {
        sf.wireMock().stubFor(get(urlPathMatching("/services/data/v[^/]+/whoami"))
                .inScenario("expired-session").whenScenarioStateIs(STARTED)
                .willReturn(aResponse().withStatus(401)
                        .withBody("[{\"errorCode\":\"INVALID_SESSION_ID\",\"message\":\"expired\"}]"))
                .willSetStateTo("refreshed"));
        sf.wireMock().stubFor(get(urlPathMatching("/services/data/v[^/]+/whoami"))
                .inScenario("expired-session").whenScenarioStateIs("refreshed")
                .willReturn(okJson("{\"ok\":true}")));

        SalesforceHttpClient client = sf.httpClient();
        var result = client.executeJson(RequestSpec.get("/services/data/v60.0/whoami"));
        assertThat(result.path("ok").asBoolean()).isTrue();
        // Two token requests: initial auth + refresh after the 401.
        sf.wireMock().verify(WireMock.moreThanOrExactly(2),
                postRequestedFor(urlPathEqualTo("/services/oauth2/token")));
    }
}
