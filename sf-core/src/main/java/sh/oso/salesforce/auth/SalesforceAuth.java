package sh.oso.salesforce.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.salesforce.common.SalesforceException;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Authenticates against the Salesforce OAuth token endpoint using Client Credentials,
 * JWT Bearer, or (legacy) Username-Password flows.
 */
public final class SalesforceAuth {

    private static final Logger LOG = LoggerFactory.getLogger(SalesforceAuth.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final AuthConfig config;
    private final HttpClient http;
    private final String tokenEndpointOverride;

    public SalesforceAuth(AuthConfig config) {
        this(config, null);
    }

    /** tokenEndpointOverride is for tests; production always derives the endpoint from config. */
    public SalesforceAuth(AuthConfig config, String tokenEndpointOverride) {
        this.config = config;
        this.tokenEndpointOverride = tokenEndpointOverride;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
    }

    public Session authenticate() {
        return switch (config.grantType()) {
            case CLIENT_CREDENTIALS -> clientCredentials();
            case JWT_BEARER -> jwtBearer();
            case PASSWORD -> usernamePassword();
        };
    }

    private Session clientCredentials() {
        Map<String, String> form = new LinkedHashMap<>();
        form.put("grant_type", "client_credentials");
        form.put("client_id", config.consumerKey());
        form.put("client_secret", config.consumerSecret());
        return requestToken(tokenEndpoint(config.instanceUrl()), form);
    }

    private Session jwtBearer() {
        String assertion = buildJwtAssertion();
        Map<String, String> form = new LinkedHashMap<>();
        form.put("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer");
        form.put("assertion", assertion);
        return requestToken(tokenEndpoint(config.instanceUrl()), form);
    }

    private Session usernamePassword() {
        LOG.warn("Username-Password OAuth flow is deprecated by Salesforce and blocked by default in orgs "
                + "created Summer '23 or later. Migrate to client_credentials or jwt_bearer.");
        Map<String, String> form = new LinkedHashMap<>();
        form.put("grant_type", "password");
        form.put("client_id", config.consumerKey());
        form.put("client_secret", config.consumerSecret());
        form.put("username", config.username());
        String password = config.password()
                + (config.securityToken() != null ? config.securityToken() : "");
        form.put("password", password);
        String base = config.instanceUrl() != null ? config.instanceUrl() : "https://login.salesforce.com";
        return requestToken(tokenEndpoint(base), form);
    }

    private String buildJwtAssertion() {
        try {
            RSAPrivateKey key = loadPrivateKey(config.jwtKeyPath(), config.jwtKeyPassword());
            JWTClaimsSet claims = new JWTClaimsSet.Builder()
                    .issuer(config.consumerKey())
                    .subject(config.username())
                    .audience(config.jwtAudience())
                    .expirationTime(Date.from(Instant.now().plus(Duration.ofMinutes(3))))
                    .jwtID(UUID.randomUUID().toString())
                    .build();
            SignedJWT jwt = new SignedJWT(
                    new JWSHeader.Builder(JWSAlgorithm.RS256).type(JOSEObjectType.JWT).build(), claims);
            jwt.sign(new RSASSASigner(key));
            return jwt.serialize();
        } catch (SalesforceException e) {
            throw e;
        } catch (Exception e) {
            throw new SalesforceException("Failed to build JWT bearer assertion: " + e.getMessage(), e);
        }
    }

    static RSAPrivateKey loadPrivateKey(String path, String password) {
        try {
            Path p = Path.of(path);
            String lower = path.toLowerCase();
            if (lower.endsWith(".jks") || lower.endsWith(".p12") || lower.endsWith(".pfx")) {
                KeyStore ks = KeyStore.getInstance(lower.endsWith(".jks") ? "JKS" : "PKCS12");
                char[] pw = password != null ? password.toCharArray() : new char[0];
                try (InputStream in = Files.newInputStream(p)) {
                    ks.load(in, pw);
                }
                Enumeration<String> aliases = ks.aliases();
                while (aliases.hasMoreElements()) {
                    String alias = aliases.nextElement();
                    if (ks.isKeyEntry(alias)) {
                        return (RSAPrivateKey) ks.getKey(alias, pw);
                    }
                }
                throw new SalesforceException("No private key entry found in keystore " + path);
            }
            // PEM (PKCS#8) private key
            String pem = Files.readString(p, StandardCharsets.US_ASCII)
                    .replaceAll("-----BEGIN (RSA )?PRIVATE KEY-----", "")
                    .replaceAll("-----END (RSA )?PRIVATE KEY-----", "")
                    .replaceAll("\\s", "");
            byte[] der = Base64.getDecoder().decode(pem);
            KeyFactory kf = KeyFactory.getInstance("RSA");
            return (RSAPrivateKey) kf.generatePrivate(new PKCS8EncodedKeySpec(der));
        } catch (SalesforceException e) {
            throw e;
        } catch (Exception e) {
            throw new SalesforceException("Failed to load JWT signing key from " + path + ": " + e.getMessage(), e);
        }
    }

    private String tokenEndpoint(String baseUrl) {
        if (tokenEndpointOverride != null) {
            return tokenEndpointOverride;
        }
        String base = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        return base + "/services/oauth2/token";
    }

    private Session requestToken(String endpoint, Map<String, String> form) {
        StringBuilder body = new StringBuilder();
        form.forEach((k, v) -> {
            if (body.length() > 0) {
                body.append('&');
            }
            body.append(URLEncoder.encode(k, StandardCharsets.UTF_8))
                    .append('=')
                    .append(URLEncoder.encode(v, StandardCharsets.UTF_8));
        });
        HttpRequest request = HttpRequest.newBuilder(URI.create(endpoint))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("Accept", "application/json")
                .timeout(Duration.ofSeconds(60))
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .build();
        HttpResponse<String> response;
        try {
            response = http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new SalesforceException("Token request to " + endpoint + " failed: " + e.getMessage(), e, true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SalesforceException("Interrupted during token request", e);
        }
        if (response.statusCode() != 200) {
            String detail = safeErrorDetail(response.body());
            throw new SalesforceException("Authentication failed (HTTP " + response.statusCode() + "): " + detail,
                    null, response.statusCode() >= 500);
        }
        try {
            JsonNode node = MAPPER.readTree(response.body());
            String accessToken = node.path("access_token").asText(null);
            String instanceUrl = node.path("instance_url").asText(null);
            if (accessToken == null || instanceUrl == null) {
                throw new SalesforceException("Token response missing access_token/instance_url");
            }
            String orgId = parseOrgId(node.path("id").asText(null));
            LOG.info("Authenticated to Salesforce org {} at {} via {}", orgId, instanceUrl, config.grantType());
            return new Session(accessToken, instanceUrl, orgId, Instant.now());
        } catch (IOException e) {
            throw new SalesforceException("Failed to parse token response", e);
        }
    }

    /** The {@code id} field is a URL like https://login.salesforce.com/id/{orgId}/{userId}. */
    private static String parseOrgId(String idUrl) {
        if (idUrl == null) {
            return null;
        }
        String[] parts = idUrl.split("/");
        return parts.length >= 2 ? parts[parts.length - 2] : null;
    }

    /** Error responses carry no secrets, but never echo the request (it contains credentials). */
    private static String safeErrorDetail(String responseBody) {
        if (responseBody == null) {
            return "(no body)";
        }
        return responseBody.length() > 300 ? responseBody.substring(0, 300) : responseBody;
    }
}
