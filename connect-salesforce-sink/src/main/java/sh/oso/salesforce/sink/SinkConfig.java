package sh.oso.salesforce.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import sh.oso.salesforce.auth.AuthConfig;
import sh.oso.salesforce.bulk.BulkIngestClient;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Configuration for the SObject sink connector. Global settings use fixed keys; per-object
 * settings use dynamic {@code sf.<object>.*} keys resolved from the raw originals.
 */
public class SinkConfig extends AbstractConfig {

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
    public static final String OBJECTS = "sf.objects";
    public static final String WRITE_MODE = "sf.write.mode";
    public static final String WRITE_REST_THRESHOLD = "sf.write.rest.threshold";
    public static final String BEHAVIOR_ON_API_ERRORS = "behavior.on.api.errors";
    public static final String MAX_BATCH_RECORDS = "sf.max.batch.records";
    public static final String RETRY_BUDGET_MS = "sf.request.max.retries.time.ms";
    public static final String TOKEN_ENDPOINT = "sf.token.endpoint";

    public enum WriteMode {BULK2, REST, AUTO}

    public enum ErrorBehavior {FAIL, LOG, IGNORE}

    public enum ObjectType {STANDARD_CUSTOM, BIG_OBJECT}

    /** Per-SObject settings resolved from dynamic keys. */
    public record ObjectSpec(
            String name,
            Set<String> topics,
            BulkIngestClient.Operation operation,
            boolean overrideEventType,
            boolean useCustomIdField,
            String customIdFieldName,
            ObjectType type,
            Set<String> ignoreFields) {
    }

    private final Map<String, ObjectSpec> specsByObject = new HashMap<>();
    private final Map<String, ObjectSpec> specsByTopic = new HashMap<>();

    public SinkConfig(Map<String, String> originals) {
        super(configDef(), originals);
        for (String object : getList(OBJECTS)) {
            ObjectSpec spec = parseObjectSpec(object, originals);
            specsByObject.put(object, spec);
            for (String topic : spec.topics()) {
                ObjectSpec previous = specsByTopic.put(topic, spec);
                if (previous != null) {
                    throw new ConfigException("Topic " + topic + " is mapped to both "
                            + previous.name() + " and " + object);
                }
            }
        }
        if (specsByObject.isEmpty()) {
            throw new ConfigException(OBJECTS, "", "At least one target SObject is required");
        }
        authConfig();
    }

    private ObjectSpec parseObjectSpec(String object, Map<String, String> originals) {
        String prefix = "sf." + object + ".";
        String topics = originals.getOrDefault(prefix + "topics", "");
        Set<String> topicSet = new HashSet<>();
        for (String topic : topics.split(",")) {
            if (!topic.isBlank()) {
                topicSet.add(topic.trim());
            }
        }
        String operation = originals.getOrDefault(prefix + "operation", "insert");
        String type = originals.getOrDefault(prefix + "type", "standard_custom");
        Set<String> ignore = new HashSet<>();
        for (String field : originals.getOrDefault(prefix + "ignore.fields", "").split(",")) {
            if (!field.isBlank()) {
                ignore.add(field.trim());
            }
        }
        boolean useCustomId = Boolean.parseBoolean(originals.getOrDefault(prefix + "use.custom.id.field", "false"));
        String customIdField = originals.get(prefix + "custom.id.field.name");
        if (useCustomId && (customIdField == null || customIdField.isBlank())) {
            throw new ConfigException(prefix + "custom.id.field.name", customIdField,
                    "Required when " + prefix + "use.custom.id.field=true");
        }
        return new ObjectSpec(
                object,
                topicSet,
                BulkIngestClient.Operation.valueOf(operation.toUpperCase(Locale.ROOT)),
                Boolean.parseBoolean(originals.getOrDefault(prefix + "override.event.type", "false")),
                useCustomId,
                customIdField,
                ObjectType.valueOf(type.toUpperCase(Locale.ROOT)),
                ignore);
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
                .define(OBJECTS, Type.LIST, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                        "Target SObject API names; map topics via sf.<object>.topics.")
                .define(WRITE_MODE, Type.STRING, "auto",
                        ConfigDef.ValidString.in("bulk2", "rest", "auto"), Importance.MEDIUM,
                        "bulk2 (Bulk API 2.0), rest (Composite), or auto (rest under the threshold).")
                .define(WRITE_REST_THRESHOLD, Type.INT, 200, ConfigDef.Range.between(1, 200),
                        Importance.LOW, "Max records for the REST path in auto mode.")
                .define(BEHAVIOR_ON_API_ERRORS, Type.STRING, "fail",
                        ConfigDef.ValidString.in("fail", "log", "ignore"), Importance.MEDIUM,
                        "Per-record API failures: fail the task, log and continue, or ignore.")
                .define(MAX_BATCH_RECORDS, Type.INT, 10_000, ConfigDef.Range.atLeast(1),
                        Importance.LOW, "Records per Bulk ingest job.")
                .define(RETRY_BUDGET_MS, Type.LONG, 30_000L, Importance.MEDIUM, "Retry time budget.")
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

    /** The ObjectSpec a topic feeds; falls back to a single-object config with no topic mapping. */
    public ObjectSpec specForTopic(String topic) {
        ObjectSpec spec = specsByTopic.get(topic);
        if (spec != null) {
            return spec;
        }
        if (specsByObject.size() == 1 && specsByTopic.isEmpty()) {
            return specsByObject.values().iterator().next();
        }
        throw new ConfigException("No sf.<object>.topics mapping covers topic " + topic);
    }

    public List<String> objects() {
        return getList(OBJECTS);
    }

    public WriteMode writeMode() {
        return WriteMode.valueOf(getString(WRITE_MODE).toUpperCase(Locale.ROOT));
    }

    public ErrorBehavior errorBehavior() {
        return ErrorBehavior.valueOf(getString(BEHAVIOR_ON_API_ERRORS).toUpperCase(Locale.ROOT));
    }

    public String apiVersion() {
        return getString(API_VERSION);
    }
}
