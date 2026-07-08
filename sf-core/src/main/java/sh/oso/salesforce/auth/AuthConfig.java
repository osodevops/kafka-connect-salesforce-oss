package sh.oso.salesforce.auth;

import sh.oso.salesforce.common.SalesforceException;

import java.util.Locale;
import java.util.Objects;

/** Credentials + flow selection for Salesforce OAuth. */
public final class AuthConfig {

    public enum GrantType {
        CLIENT_CREDENTIALS,
        JWT_BEARER,
        PASSWORD;

        public static GrantType fromConfig(String value) {
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "client_credentials" -> CLIENT_CREDENTIALS;
                case "jwt_bearer" -> JWT_BEARER;
                case "password" -> PASSWORD;
                default -> throw new SalesforceException("Unknown sf.auth.grant.type: " + value);
            };
        }
    }

    private final GrantType grantType;
    private final String instanceUrl;
    private final String consumerKey;
    private final String consumerSecret;
    private final String username;
    private final String password;
    private final String securityToken;
    private final String jwtKeyPath;
    private final String jwtKeyPassword;
    private final String jwtAudience;

    private AuthConfig(Builder b) {
        this.grantType = Objects.requireNonNull(b.grantType, "grantType");
        this.instanceUrl = b.instanceUrl;
        this.consumerKey = b.consumerKey;
        this.consumerSecret = b.consumerSecret;
        this.username = b.username;
        this.password = b.password;
        this.securityToken = b.securityToken;
        this.jwtKeyPath = b.jwtKeyPath;
        this.jwtKeyPassword = b.jwtKeyPassword;
        this.jwtAudience = b.jwtAudience;
        validate();
    }

    private void validate() {
        switch (grantType) {
            case CLIENT_CREDENTIALS -> {
                requireMyDomain();
                require(consumerKey, "sf.consumer.key");
                require(consumerSecret, "sf.consumer.secret");
            }
            case JWT_BEARER -> {
                requireMyDomain();
                require(consumerKey, "sf.consumer.key");
                require(username, "sf.username");
                require(jwtKeyPath, "sf.jwt.keystore.path");
            }
            case PASSWORD -> {
                require(consumerKey, "sf.consumer.key");
                require(consumerSecret, "sf.consumer.secret");
                require(username, "sf.username");
                require(password, "sf.password");
            }
        }
    }

    private void requireMyDomain() {
        require(instanceUrl, "sf.instance.url");
        String host = instanceUrl.replaceFirst("^https?://", "").toLowerCase(Locale.ROOT);
        if (host.startsWith("login.salesforce.com") || host.startsWith("test.salesforce.com")) {
            throw new SalesforceException(
                    "sf.instance.url must be the org's My Domain URL; login.salesforce.com / test.salesforce.com "
                            + "are not supported for the " + grantType + " flow");
        }
    }

    private static void require(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new SalesforceException("Missing required auth config: " + name);
        }
    }

    public GrantType grantType() {
        return grantType;
    }

    public String instanceUrl() {
        return instanceUrl;
    }

    public String consumerKey() {
        return consumerKey;
    }

    public String consumerSecret() {
        return consumerSecret;
    }

    public String username() {
        return username;
    }

    public String password() {
        return password;
    }

    public String securityToken() {
        return securityToken;
    }

    public String jwtKeyPath() {
        return jwtKeyPath;
    }

    public String jwtKeyPassword() {
        return jwtKeyPassword;
    }

    /** JWT {@code aud} claim; defaults to https://login.salesforce.com when unset. */
    public String jwtAudience() {
        return jwtAudience != null ? jwtAudience : "https://login.salesforce.com";
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private GrantType grantType;
        private String instanceUrl;
        private String consumerKey;
        private String consumerSecret;
        private String username;
        private String password;
        private String securityToken;
        private String jwtKeyPath;
        private String jwtKeyPassword;
        private String jwtAudience;

        public Builder grantType(GrantType v) {
            this.grantType = v;
            return this;
        }

        public Builder instanceUrl(String v) {
            this.instanceUrl = v;
            return this;
        }

        public Builder consumerKey(String v) {
            this.consumerKey = v;
            return this;
        }

        public Builder consumerSecret(String v) {
            this.consumerSecret = v;
            return this;
        }

        public Builder username(String v) {
            this.username = v;
            return this;
        }

        public Builder password(String v) {
            this.password = v;
            return this;
        }

        public Builder securityToken(String v) {
            this.securityToken = v;
            return this;
        }

        public Builder jwtKeyPath(String v) {
            this.jwtKeyPath = v;
            return this;
        }

        public Builder jwtKeyPassword(String v) {
            this.jwtKeyPassword = v;
            return this;
        }

        public Builder jwtAudience(String v) {
            this.jwtAudience = v;
            return this;
        }

        public AuthConfig build() {
            return new AuthConfig(this);
        }
    }
}
