package sh.oso.salesforce.pubsub;

import com.salesforce.eventbus.protobuf.PubSubGrpc;
import com.salesforce.eventbus.protobuf.PublishRequest;
import com.salesforce.eventbus.protobuf.PublishResponse;
import com.salesforce.eventbus.protobuf.ProducerEvent;
import com.salesforce.eventbus.protobuf.SchemaInfo;
import com.salesforce.eventbus.protobuf.SchemaRequest;
import com.salesforce.eventbus.protobuf.TopicInfo;
import com.salesforce.eventbus.protobuf.TopicRequest;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.salesforce.auth.Session;
import sh.oso.salesforce.auth.SessionSupplier;
import sh.oso.salesforce.common.SalesforceException;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Salesforce Pub/Sub API client over gRPC. Owns the channel, attaches session auth as
 * call metadata ({@code accesstoken}/{@code instanceurl}/{@code tenantid}), and exposes
 * GetTopic/GetSchema/Publish plus {@link #subscribe} for the bidirectional Subscribe RPC.
 */
public final class PubSubClient implements AutoCloseable {

    public static final String DEFAULT_ENDPOINT = "api.pubsub.salesforce.com:443";

    private static final Logger LOG = LoggerFactory.getLogger(PubSubClient.class);

    private static final Metadata.Key<String> ACCESS_TOKEN =
            Metadata.Key.of("accesstoken", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> INSTANCE_URL =
            Metadata.Key.of("instanceurl", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> TENANT_ID =
            Metadata.Key.of("tenantid", Metadata.ASCII_STRING_MARSHALLER);

    private final ManagedChannel channel;
    private final SessionSupplier sessions;
    private final ConcurrentHashMap<String, Schema> schemaCache = new ConcurrentHashMap<>();

    public PubSubClient(SessionSupplier sessions) {
        this(sessions, DEFAULT_ENDPOINT, true);
    }

    public PubSubClient(SessionSupplier sessions, String endpoint, boolean useTls) {
        this.sessions = sessions;
        ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(endpoint)
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS);
        if (useTls) {
            builder.useTransportSecurity();
        } else {
            builder.usePlaintext();
        }
        this.channel = builder.build();
    }

    /** For tests: wrap an existing channel (e.g. in-process). Caller keeps channel ownership. */
    public PubSubClient(SessionSupplier sessions, ManagedChannel channel) {
        this.sessions = sessions;
        this.channel = channel;
    }

    public TopicInfo getTopic(String topicName) {
        return blockingStub().getTopic(TopicRequest.newBuilder().setTopicName(topicName).build());
    }

    /** Avro schema by schema ID, cached for the lifetime of the client. */
    public Schema getSchema(String schemaId) {
        return schemaCache.computeIfAbsent(schemaId, id -> {
            SchemaInfo info = blockingStub().getSchema(SchemaRequest.newBuilder().setSchemaId(id).build());
            LOG.debug("Fetched Avro schema {} from Pub/Sub API", id);
            return new Schema.Parser().parse(info.getSchemaJson());
        });
    }

    /** Unary batch publish; returns per-event results (check {@code hasError} on each). */
    public PublishResponse publish(String topicName, List<ProducerEvent> events) {
        return blockingStub().publish(PublishRequest.newBuilder()
                .setTopicName(topicName)
                .addAllEvents(events)
                .build());
    }

    /**
     * Opens a Subscribe stream. The subscription manages pull-based flow control and decodes
     * Avro payloads via this client's schema cache.
     */
    public PubSubSubscription subscribe(SubscribeOptions options) {
        return new PubSubSubscription(asyncStub(), this::getSchema, options);
    }

    PubSubGrpc.PubSubBlockingStub blockingStub() {
        return PubSubGrpc.newBlockingStub(channel).withCallCredentials(sessionCredentials());
    }

    PubSubGrpc.PubSubStub asyncStub() {
        return PubSubGrpc.newStub(channel).withCallCredentials(sessionCredentials());
    }

    private CallCredentials sessionCredentials() {
        return new CallCredentials() {
            @Override
            public void applyRequestMetadata(RequestInfo requestInfo, Executor executor, MetadataApplier applier) {
                executor.execute(() -> {
                    try {
                        Session session = sessions.session();
                        Metadata metadata = new Metadata();
                        metadata.put(ACCESS_TOKEN, session.accessToken());
                        metadata.put(INSTANCE_URL, session.instanceUrl());
                        if (session.orgId() != null) {
                            metadata.put(TENANT_ID, session.orgId());
                        }
                        applier.apply(metadata);
                    } catch (SalesforceException e) {
                        applier.fail(Status.UNAUTHENTICATED.withDescription(e.getMessage()).withCause(e));
                    }
                });
            }
        };
    }

    /** Signals the session may be stale (e.g. UNAUTHENTICATED from a stream). */
    public void invalidateSession() {
        sessions.invalidate(sessions.session());
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(10, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
