package sh.oso.salesforce.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.salesforce.auth.Session;
import sh.oso.salesforce.auth.SessionSupplier;
import sh.oso.salesforce.common.ErrorClassifier;
import sh.oso.salesforce.common.SalesforceApiException;
import sh.oso.salesforce.common.SalesforceException;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * Authenticated HTTP transport for Salesforce REST + Bulk 2.0 endpoints.
 * Injects the bearer token, re-authenticates once on 401/INVALID_SESSION_ID,
 * and maps error responses to {@link SalesforceApiException} with retryability classified.
 */
public final class SalesforceHttpClient {

    private static final Logger LOG = LoggerFactory.getLogger(SalesforceHttpClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final SessionSupplier sessions;
    private final HttpClient http;
    private final Duration requestTimeout;

    public SalesforceHttpClient(SessionSupplier sessions) {
        this(sessions, Duration.ofMinutes(2));
    }

    public SalesforceHttpClient(SessionSupplier sessions, Duration requestTimeout) {
        this.sessions = sessions;
        this.requestTimeout = requestTimeout;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    public Session session() {
        return sessions.session();
    }

    /** Executes a request built against the current session; retries once on session expiry. */
    public HttpResponse<byte[]> execute(RequestSpec spec) {
        Session session = sessions.session();
        HttpResponse<byte[]> response = send(spec, session);
        if (response.statusCode() == 401) {
            sessions.invalidate(session);
            session = sessions.session();
            response = send(spec, session);
        }
        if (response.statusCode() >= 400) {
            throw toApiException(response);
        }
        return response;
    }

    public JsonNode executeJson(RequestSpec spec) {
        HttpResponse<byte[]> response = execute(spec);
        byte[] body = response.body();
        if (body == null || body.length == 0) {
            return MAPPER.nullNode();
        }
        try {
            return MAPPER.readTree(body);
        } catch (IOException e) {
            throw new SalesforceException("Failed to parse Salesforce JSON response from "
                    + spec.method() + " " + spec.path(), e);
        }
    }

    /** Raw send without status handling — for callers that need 304/redirect semantics. */
    public HttpResponse<byte[]> executeRaw(RequestSpec spec) {
        Session session = sessions.session();
        HttpResponse<byte[]> response = send(spec, session);
        if (response.statusCode() == 401) {
            sessions.invalidate(session);
            response = send(spec, sessions.session());
        }
        return response;
    }

    private HttpResponse<byte[]> send(RequestSpec spec, Session session) {
        String url = spec.path().startsWith("http")
                ? spec.path()
                : session.instanceUrl() + spec.path();
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
                .timeout(requestTimeout)
                .header("Authorization", "Bearer " + session.accessToken());
        for (Map.Entry<String, String> h : spec.headers().entrySet()) {
            builder.header(h.getKey(), h.getValue());
        }
        switch (spec.method()) {
            case "GET" -> builder.GET();
            case "DELETE" -> builder.DELETE();
            default -> builder.method(spec.method(), spec.body() != null
                    ? HttpRequest.BodyPublishers.ofByteArray(spec.body())
                    : HttpRequest.BodyPublishers.noBody());
        }
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("{} {}", spec.method(), spec.path());
            }
            return http.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException e) {
            throw new SalesforceException(spec.method() + " " + spec.path() + " failed: " + e.getMessage(), e, true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SalesforceException("Interrupted during " + spec.method() + " " + spec.path(), e);
        }
    }

    private static SalesforceApiException toApiException(HttpResponse<byte[]> response) {
        String body = response.body() != null ? new String(response.body()) : "";
        String errorCode = null;
        try {
            JsonNode node = MAPPER.readTree(body);
            if (node.isArray() && node.size() > 0) {
                errorCode = node.get(0).path("errorCode").asText(null);
            } else if (node.isObject()) {
                errorCode = node.path("errorCode").asText(null);
            }
        } catch (IOException ignored) {
            // non-JSON error body
        }
        boolean retryable = ErrorClassifier.isRetryableHttp(response.statusCode(), errorCode);
        return new SalesforceApiException(response.statusCode(), errorCode, body, retryable);
    }
}
