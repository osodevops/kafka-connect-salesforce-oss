package sh.oso.salesforce.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import sh.oso.salesforce.auth.AuthConfig;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Configuration for the unified Salesforce source connector ({@code sf.*} namespace). */
public class SourceConfig extends AbstractConfig {

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
    public static final String SOBJECTS = "sf.sobjects";
    public static final String SOBJECTS_MAX = "sf.sobjects.max";
    public static final String TOPIC_PREFIX = "sf.topic.prefix";
    public static final String SNAPSHOT_ENABLED = "sf.snapshot.enabled";
    public static final String SNAPSHOT_SINCE = "sf.snapshot.since";
    public static final String REALTIME_MODE = "sf.realtime.mode";
    public static final String EVENT_START = "sf.event.start";
    public static final String GAP_RECOVERY = "sf.gap.recovery";
    public static final String GAP_RESYNC_BUFFER_MS = "sf.gap.resync.buffer.ms";
    public static final String FULL_RECORD_ON_UPDATE = "sf.full.record.on.update";
    public static final String EMIT_TOMBSTONE_ON_DELETE = "sf.emit.tombstone.on.delete";
    public static final String INCLUDE_DELETED = "sf.include.deleted";
    public static final String POLL_INTERVAL_MS = "sf.poll.interval.ms";
    public static final String RESULT_MAX_ROWS = "sf.result.max.rows";
    public static final String SKIP_EPOCH_CONVERSION_FIELDS = "sf.skip.epoch.conversion.fields";
    public static final String RETRY_BUDGET_MS = "sf.request.max.retries.time.ms";
    public static final String PUBSUB_BATCH_SIZE = "sf.pubsub.batch.size";

    // Endpoint overrides — primarily for testing against a local fake; also useful for proxies.
    public static final String PUBSUB_ENDPOINT = "sf.pubsub.endpoint";
    public static final String PUBSUB_PLAINTEXT = "sf.pubsub.plaintext";
    public static final String TOKEN_ENDPOINT = "sf.token.endpoint";

    // Internal: SObjects assigned to one task (set by the connector, not users).
    public static final String TASK_SOBJECTS = "sf.task.sobjects";

    public enum RealtimeMode {EVENT_DRIVEN, POLLING}

    public enum EventStart {LATEST, ALL}

    public enum GapRecovery {RESYNC, LATEST, FAIL}

    public SourceConfig(Map<String, String> originals) {
        super(configDef(), originals);
        validateCombinations();
    }

    public static ConfigDef configDef() {
        return new ConfigDef()
                .define(AUTH_GRANT_TYPE, Type.STRING, "client_credentials",
                        ConfigDef.ValidString.in("client_credentials", "jwt_bearer", "password"),
                        Importance.HIGH, "OAuth flow: client_credentials, jwt_bearer, or password (legacy).")
                .define(INSTANCE_URL, Type.STRING, null, Importance.HIGH,
                        "The org's My Domain URL, e.g. https://acme.my.salesforce.com.")
                .define(CONSUMER_KEY, Type.PASSWORD, null, Importance.HIGH, "Connected app consumer key.")
                .define(CONSUMER_SECRET, Type.PASSWORD, null, Importance.HIGH, "Connected app consumer secret.")
                .define(USERNAME, Type.STRING, null, Importance.MEDIUM, "Username (jwt_bearer/password flows).")
                .define(PASSWORD, Type.PASSWORD, null, Importance.LOW, "Password (legacy password flow).")
                .define(PASSWORD_TOKEN, Type.PASSWORD, null, Importance.LOW,
                        "Security token appended to the password (legacy password flow).")
                .define(JWT_KEYSTORE_PATH, Type.STRING, null, Importance.MEDIUM,
                        "Path to the RS256 signing key: PEM (PKCS#8), JKS, or PKCS12.")
                .define(JWT_KEYSTORE_PASSWORD, Type.PASSWORD, null, Importance.MEDIUM, "Keystore password.")
                .define(JWT_AUDIENCE, Type.STRING, null, Importance.LOW,
                        "JWT aud claim; defaults to https://login.salesforce.com.")
                .define(API_VERSION, Type.STRING, "60.0", Importance.MEDIUM, "Salesforce API version.")
                .define(SOBJECTS, Type.LIST, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                        "SObject API names to source (exact casing).")
                .define(SOBJECTS_MAX, Type.INT, 5, ConfigDef.Range.atLeast(1), Importance.LOW,
                        "Maximum SObjects per connector instance.")
                .define(TOPIC_PREFIX, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                        "Kafka topics are {prefix}.{SObject}.")
                .define(SNAPSHOT_ENABLED, Type.BOOLEAN, true, Importance.MEDIUM,
                        "Run a historical Bulk 2.0 snapshot before the real-time mode.")
                .define(SNAPSHOT_SINCE, Type.STRING, null, Importance.MEDIUM,
                        "Snapshot lower bound on CreatedDate (ISO-8601 date or datetime). Default: all records.")
                .define(REALTIME_MODE, Type.STRING, "event_driven",
                        ConfigDef.ValidString.in("event_driven", "polling"), Importance.HIGH,
                        "Continuous ingestion mode after the snapshot.")
                .define(EVENT_START, Type.STRING, "latest",
                        ConfigDef.ValidString.in("latest", "all"), Importance.MEDIUM,
                        "Cold-start replay position for event_driven mode.")
                .define(GAP_RECOVERY, Type.STRING, "resync",
                        ConfigDef.ValidString.in("resync", "latest", "fail"), Importance.MEDIUM,
                        "Behaviour on gap/overflow events or replay loss.")
                .define(GAP_RESYNC_BUFFER_MS, Type.LONG, 60_000L, Importance.LOW,
                        "Safety buffer subtracted from the watermark on gap resync.")
                .define(FULL_RECORD_ON_UPDATE, Type.BOOLEAN, false, Importance.LOW,
                        "Fetch the full post-image via REST on every UPDATE (one extra API call per update).")
                .define(EMIT_TOMBSTONE_ON_DELETE, Type.BOOLEAN, false, Importance.MEDIUM,
                        "Emit a tombstone (null value) record on DELETE in event_driven mode.")
                .define(INCLUDE_DELETED, Type.BOOLEAN, false, Importance.LOW,
                        "Polling mode: include soft-deleted records via queryAll.")
                .define(POLL_INTERVAL_MS, Type.INT, 30_000, ConfigDef.Range.atLeast(8_700), Importance.MEDIUM,
                        "Polling-mode interval in milliseconds.")
                .define(RESULT_MAX_ROWS, Type.INT, 1_000, ConfigDef.Range.atLeast(1), Importance.LOW,
                        "Maximum Bulk result rows fetched per request.")
                .define(SKIP_EPOCH_CONVERSION_FIELDS, Type.LIST, "", Importance.LOW,
                        "Date/datetime fields to keep as ISO-8601 strings instead of epoch millis.")
                .define(RETRY_BUDGET_MS, Type.LONG, 30_000L, Importance.MEDIUM,
                        "Total time budget for retrying transient Salesforce failures.")
                .define(PUBSUB_BATCH_SIZE, Type.INT, 100, ConfigDef.Range.between(1, 100), Importance.LOW,
                        "Pub/Sub Subscribe flow-control batch size (num_requested).")
                .define(PUBSUB_ENDPOINT, Type.STRING, "api.pubsub.salesforce.com:443", Importance.LOW,
                        "Pub/Sub API endpoint host:port.")
                .define(PUBSUB_PLAINTEXT, Type.BOOLEAN, false, Importance.LOW,
                        "Use plaintext gRPC (testing only).")
                .define(TOKEN_ENDPOINT, Type.STRING, null, Importance.LOW,
                        "OAuth token endpoint override (testing only).")
                .define(TASK_SOBJECTS, Type.LIST, "", Importance.LOW,
                        "Internal: SObjects assigned to this task.");
    }

    private void validateCombinations() {
        List<String> sobjects = getList(SOBJECTS);
        if (sobjects.isEmpty()) {
            throw new ConfigException(SOBJECTS, sobjects, "At least one SObject is required");
        }
        if (sobjects.size() > getInt(SOBJECTS_MAX)) {
            throw new ConfigException(SOBJECTS, sobjects,
                    "More SObjects than " + SOBJECTS_MAX + "=" + getInt(SOBJECTS_MAX));
        }
        authConfig(); // validates flow-specific requirements
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

    public List<String> sobjects() {
        return getList(SOBJECTS);
    }

    public List<String> taskSobjects() {
        List<String> assigned = getList(TASK_SOBJECTS);
        return assigned.isEmpty() ? sobjects() : assigned;
    }

    public String topicFor(String sobject) {
        return getString(TOPIC_PREFIX) + "." + sobject;
    }

    public RealtimeMode realtimeMode() {
        return RealtimeMode.valueOf(getString(REALTIME_MODE).toUpperCase(Locale.ROOT));
    }

    public EventStart eventStart() {
        return EventStart.valueOf(getString(EVENT_START).toUpperCase(Locale.ROOT));
    }

    public GapRecovery gapRecovery() {
        return GapRecovery.valueOf(getString(GAP_RECOVERY).toUpperCase(Locale.ROOT));
    }

    public Set<String> skipEpochConversionFields() {
        return new HashSet<>(getList(SKIP_EPOCH_CONVERSION_FIELDS));
    }

    public String apiVersion() {
        return getString(API_VERSION);
    }
}
