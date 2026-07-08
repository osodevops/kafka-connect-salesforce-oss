package sh.oso.salesforce.pesink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import sh.oso.salesforce.auth.AuthConfig;

import java.util.Locale;
import java.util.Map;

/** Configuration for the Platform Event sink connector. */
public class PeSinkConfig extends AbstractConfig {

    public static final String AUTH_GRANT_TYPE = "sf.auth.grant.type";
    public static final String INSTANCE_URL = "sf.instance.url";
    public static final String CONSUMER_KEY = "sf.consumer.key";
    public static final String CONSUMER_SECRET = "sf.consumer.secret";
    public static final String USERNAME = "sf.username";
    public static final String PASSWORD = "sf.password";
    public static final String PASSWORD_TOKEN = "sf.password.token";
    public static final String JWT_KEYSTORE_PATH = "sf.jwt.keystore.path";
    public static final String JWT_KEYSTORE_PASSWORD = "sf.jwt.keystore.password";
    public static final String JWT_AUDIENCE = "sf.jwt.audience";
    public static final String API_VERSION = "sf.api.version";
    public static final String PLATFORM_EVENT_NAME = "sf.platform.event.name";
    public static final String PUBLISH_MODE = "sf.publish.mode";
    public static final String BEHAVIOR_ON_API_ERRORS = "behavior.on.api.errors";
    public static final String RETRY_BUDGET_MS = "sf.request.max.retries.time.ms";
    public static final String PUBSUB_ENDPOINT = "sf.pubsub.endpoint";
    public static final String PUBSUB_PLAINTEXT = "sf.pubsub.plaintext";
    public static final String TOKEN_ENDPOINT = "sf.token.endpoint";

    public enum PublishMode {PUBSUB, REST}

    public enum ErrorBehavior {FAIL, LOG, IGNORE}

    public PeSinkConfig(Map<String, String> originals) {
        super(configDef(), originals);
        String eventName = getString(PLATFORM_EVENT_NAME);
        if (!eventName.matches(".*__e$")) {
            throw new ConfigException(PLATFORM_EVENT_NAME, eventName,
                    "Platform event API names end with __e");
        }
        authConfig();
    }

    public static ConfigDef configDef() {
        return new ConfigDef()
                .define(AUTH_GRANT_TYPE, Type.STRING, "client_credentials",
                        ConfigDef.ValidString.in("client_credentials", "jwt_bearer", "password"),
                        Importance.HIGH, "OAuth flow.")
                .define(INSTANCE_URL, Type.STRING, null, Importance.HIGH, "My Domain URL.")
                .define(CONSUMER_KEY, Type.PASSWORD, null, Importance.HIGH, "Consumer key.")
                .define(CONSUMER_SECRET, Type.PASSWORD, null, Importance.HIGH, "Consumer secret.")
                .define(USERNAME, Type.STRING, null, Importance.MEDIUM, "Username (jwt_bearer/password).")
                .define(PASSWORD, Type.PASSWORD, null, Importance.LOW, "Password (legacy flow).")
                .define(PASSWORD_TOKEN, Type.PASSWORD, null, Importance.LOW, "Security token (legacy flow).")
                .define(JWT_KEYSTORE_PATH, Type.STRING, null, Importance.MEDIUM, "RS256 key path.")
                .define(JWT_KEYSTORE_PASSWORD, Type.PASSWORD, null, Importance.MEDIUM, "Keystore password.")
                .define(JWT_AUDIENCE, Type.STRING, null, Importance.LOW, "JWT aud claim.")
                .define(API_VERSION, Type.STRING, "60.0", Importance.MEDIUM, "API version.")
                .define(PLATFORM_EVENT_NAME, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                        "Target platform event API name (…__e); must already exist in Salesforce.")
                .define(PUBLISH_MODE, Type.STRING, "pubsub",
                        ConfigDef.ValidString.in("pubsub", "rest"), Importance.MEDIUM,
                        "Publish via the Pub/Sub API (preferred) or REST.")
                .define(BEHAVIOR_ON_API_ERRORS, Type.STRING, "fail",
                        ConfigDef.ValidString.in("fail", "log", "ignore"), Importance.MEDIUM,
                        "Per-event publish failures: fail the task, log and continue, or ignore.")
                .define(RETRY_BUDGET_MS, Type.LONG, 30_000L, Importance.MEDIUM, "Retry time budget.")
                .define(PUBSUB_ENDPOINT, Type.STRING, "api.pubsub.salesforce.com:443", Importance.LOW,
                        "Pub/Sub API endpoint host:port.")
                .define(PUBSUB_PLAINTEXT, Type.BOOLEAN, false, Importance.LOW,
                        "Use plaintext gRPC (testing only).")
                .define(TOKEN_ENDPOINT, Type.STRING, null, Importance.LOW,
                        "OAuth token endpoint override (testing only).");
    }

    public AuthConfig authConfig() {
        return AuthConfig.builder()
                .grantType(AuthConfig.GrantType.fromConfig(getString(AUTH_GRANT_TYPE)))
                .instanceUrl(getString(INSTANCE_URL))
                .consumerKey(password(CONSUMER_KEY))
                .consumerSecret(password(CONSUMER_SECRET))
                .username(getString(USERNAME))
                .password(password(PASSWORD))
                .securityToken(password(PASSWORD_TOKEN))
                .jwtKeyPath(getString(JWT_KEYSTORE_PATH))
                .jwtKeyPassword(password(JWT_KEYSTORE_PASSWORD))
                .jwtAudience(getString(JWT_AUDIENCE))
                .build();
    }

    private String password(String key) {
        Password value = getPassword(key);
        return value != null ? value.value() : null;
    }

    public PublishMode publishMode() {
        return PublishMode.valueOf(getString(PUBLISH_MODE).toUpperCase(Locale.ROOT));
    }

    public ErrorBehavior errorBehavior() {
        return ErrorBehavior.valueOf(getString(BEHAVIOR_ON_API_ERRORS).toUpperCase(Locale.ROOT));
    }

    public String eventName() {
        return getString(PLATFORM_EVENT_NAME);
    }

    public String eventTopic() {
        return "/event/" + eventName();
    }

    public String apiVersion() {
        return getString(API_VERSION);
    }
}
