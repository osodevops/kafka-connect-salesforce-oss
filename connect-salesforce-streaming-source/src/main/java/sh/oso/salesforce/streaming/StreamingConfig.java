package sh.oso.salesforce.streaming;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import sh.oso.salesforce.auth.AuthConfig;

import java.util.Locale;
import java.util.Map;

/** Configuration for the legacy CometD/Bayeux streaming source connector. */
public class StreamingConfig extends AbstractConfig {

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
    public static final String CHANNEL_TYPE = "sf.channel.type";
    public static final String OBJECT = "sf.object";
    public static final String PUSHTOPIC_NAME = "sf.pushtopic.name";
    public static final String PUSHTOPIC_CREATE = "sf.pushtopic.create";
    public static final String PUSHTOPIC_NOTIFY_CREATE = "sf.pushtopic.notify.create";
    public static final String PUSHTOPIC_NOTIFY_UPDATE = "sf.pushtopic.notify.update";
    public static final String PUSHTOPIC_NOTIFY_DELETE = "sf.pushtopic.notify.delete";
    public static final String PUSHTOPIC_NOTIFY_UNDELETE = "sf.pushtopic.notify.undelete";
    public static final String CDC_CHANNEL = "sf.cdc.channel";
    public static final String CHANNEL_ENTITIES = "sf.channel.entities";
    public static final String PLATFORM_EVENT_NAME = "sf.platform.event.name";
    public static final String EVENT_START = "sf.event.start";
    public static final String INVALID_REPLAY_BEHAVIOUR = "sf.invalid.replay.behaviour";
    public static final String KAFKA_TOPIC = "kafka.topic";
    public static final String CONNECTION_TIMEOUT_MS = "sf.connection.timeout.ms";
    public static final String RETRY_BUDGET_MS = "sf.request.max.retries.time.ms";
    public static final String TOKEN_ENDPOINT = "sf.token.endpoint";
    public static final String COMETD_ENDPOINT = "sf.cometd.endpoint";

    public enum ChannelType {PUSHTOPIC, CDC, PLATFORM_EVENT}

    public enum EventStart {LATEST, ALL}

    public enum InvalidReplayBehaviour {ALL, LATEST}

    public StreamingConfig(Map<String, String> originals) {
        super(configDef(), originals);
        switch (channelType()) {
            case PUSHTOPIC -> {
                if (isBlank(getString(PUSHTOPIC_NAME))) {
                    throw new ConfigException(PUSHTOPIC_NAME, null, "Required for channel type pushtopic");
                }
                if (getBoolean(PUSHTOPIC_CREATE) && isBlank(getString(OBJECT))) {
                    throw new ConfigException(OBJECT, null, "Required to auto-create a PushTopic");
                }
            }
            case PLATFORM_EVENT -> {
                String name = getString(PLATFORM_EVENT_NAME);
                if (name == null || !name.matches(".*__e$")) {
                    throw new ConfigException(PLATFORM_EVENT_NAME, name, "Must match .*__e$");
                }
            }
            case CDC -> {
                // sf.cdc.channel default applies
            }
        }
        if (isBlank(getString(KAFKA_TOPIC))) {
            throw new ConfigException(KAFKA_TOPIC, null, "Target Kafka topic is required");
        }
        authConfig();
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
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
                .define(API_VERSION, Type.STRING, "60.0", Importance.MEDIUM,
                        "API version (>= 37.0 for durable replay).")
                .define(CHANNEL_TYPE, Type.STRING, "cdc",
                        ConfigDef.ValidString.in("pushtopic", "cdc", "platform_event"),
                        Importance.HIGH, "Streaming channel type.")
                .define(OBJECT, Type.STRING, null, Importance.MEDIUM,
                        "SObject backing an auto-created PushTopic.")
                .define(PUSHTOPIC_NAME, Type.STRING, null, Importance.MEDIUM, "PushTopic name.")
                .define(PUSHTOPIC_CREATE, Type.BOOLEAN, true, Importance.MEDIUM,
                        "Auto-create the PushTopic from the sf.object describe when missing.")
                .define(PUSHTOPIC_NOTIFY_CREATE, Type.BOOLEAN, true, Importance.LOW, "Notify on create.")
                .define(PUSHTOPIC_NOTIFY_UPDATE, Type.BOOLEAN, true, Importance.LOW, "Notify on update.")
                .define(PUSHTOPIC_NOTIFY_DELETE, Type.BOOLEAN, true, Importance.LOW, "Notify on delete.")
                .define(PUSHTOPIC_NOTIFY_UNDELETE, Type.BOOLEAN, true, Importance.LOW, "Notify on undelete.")
                .define(CDC_CHANNEL, Type.STRING, "ChangeEvents", Importance.MEDIUM,
                        "CDC channel: ChangeEvents, <Entity>ChangeEvent, or a custom __chn channel.")
                .define(CHANNEL_ENTITIES, Type.LIST, "", Importance.LOW,
                        "Entity filter for multi-entity CDC channels.")
                .define(PLATFORM_EVENT_NAME, Type.STRING, null, Importance.MEDIUM,
                        "Platform event API name (…__e).")
                .define(EVENT_START, Type.STRING, "latest",
                        ConfigDef.ValidString.in("latest", "all"), Importance.MEDIUM,
                        "Cold start: latest (replayId -1) or all retained events (replayId -2).")
                .define(INVALID_REPLAY_BEHAVIOUR, Type.STRING, "all",
                        ConfigDef.ValidString.in("all", "latest"), Importance.MEDIUM,
                        "Recovery when a stored replayId has expired or is invalid.")
                .define(KAFKA_TOPIC, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                        "Target Kafka topic; may reference ${_EventType} and ${_ObjectType}.")
                .define(CONNECTION_TIMEOUT_MS, Type.LONG, 30_000L, Importance.LOW, "CometD connect timeout.")
                .define(RETRY_BUDGET_MS, Type.LONG, 900_000L, Importance.MEDIUM, "Retry time budget.")
                .define(TOKEN_ENDPOINT, Type.STRING, null, Importance.LOW,
                        "OAuth token endpoint override (testing only).")
                .define(COMETD_ENDPOINT, Type.STRING, null, Importance.LOW,
                        "CometD endpoint override (testing only); default {instanceUrl}/cometd/{version}/.");
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

    public ChannelType channelType() {
        return ChannelType.valueOf(getString(CHANNEL_TYPE).toUpperCase(Locale.ROOT));
    }

    public EventStart eventStart() {
        return EventStart.valueOf(getString(EVENT_START).toUpperCase(Locale.ROOT));
    }

    public InvalidReplayBehaviour invalidReplayBehaviour() {
        return InvalidReplayBehaviour.valueOf(getString(INVALID_REPLAY_BEHAVIOUR).toUpperCase(Locale.ROOT));
    }

    /** The Bayeux channel to subscribe to. */
    public String bayeuxChannel() {
        return switch (channelType()) {
            case PUSHTOPIC -> "/topic/" + getString(PUSHTOPIC_NAME);
            case CDC -> "/data/" + getString(CDC_CHANNEL);
            case PLATFORM_EVENT -> "/event/" + getString(PLATFORM_EVENT_NAME);
        };
    }

    public String apiVersion() {
        return getString(API_VERSION);
    }
}
