package sh.oso.salesforce.auth;

import java.time.Instant;
import java.util.Objects;

/** An authenticated Salesforce session. Immutable; never log the access token. */
public final class Session {

    private final String accessToken;
    private final String instanceUrl;
    private final String orgId;
    private final Instant issuedAt;

    public Session(String accessToken, String instanceUrl, String orgId, Instant issuedAt) {
        this.accessToken = Objects.requireNonNull(accessToken, "accessToken");
        this.instanceUrl = stripTrailingSlash(Objects.requireNonNull(instanceUrl, "instanceUrl"));
        this.orgId = orgId;
        this.issuedAt = issuedAt;
    }

    public String accessToken() {
        return accessToken;
    }

    public String instanceUrl() {
        return instanceUrl;
    }

    /** 18-char org ID (tenant ID for Pub/Sub API), parsed from the token response {@code id} URL; may be null. */
    public String orgId() {
        return orgId;
    }

    public Instant issuedAt() {
        return issuedAt;
    }

    private static String stripTrailingSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    @Override
    public String toString() {
        return "Session{instanceUrl=" + instanceUrl + ", orgId=" + orgId + ", issuedAt=" + issuedAt + ", accessToken=***}";
    }
}
