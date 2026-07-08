package sh.oso.salesforce.http;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/** Immutable-ish description of a Salesforce HTTP request, relative to the instance URL. */
public final class RequestSpec {

    private final String method;
    private final String path;
    private final Map<String, String> headers = new LinkedHashMap<>();
    private byte[] body;

    private RequestSpec(String method, String path) {
        this.method = method;
        this.path = path;
    }

    public static RequestSpec get(String path) {
        return new RequestSpec("GET", path);
    }

    public static RequestSpec post(String path) {
        return new RequestSpec("POST", path);
    }

    public static RequestSpec put(String path) {
        return new RequestSpec("PUT", path);
    }

    public static RequestSpec patch(String path) {
        return new RequestSpec("PATCH", path);
    }

    public static RequestSpec delete(String path) {
        return new RequestSpec("DELETE", path);
    }

    public RequestSpec header(String name, String value) {
        headers.put(name, value);
        return this;
    }

    public RequestSpec jsonBody(String json) {
        this.body = json.getBytes(StandardCharsets.UTF_8);
        return header("Content-Type", "application/json");
    }

    public RequestSpec csvBody(byte[] csv) {
        this.body = csv;
        return header("Content-Type", "text/csv");
    }

    public String method() {
        return method;
    }

    public String path() {
        return path;
    }

    public Map<String, String> headers() {
        return headers;
    }

    public byte[] body() {
        return body;
    }
}
