package sh.oso.salesforce.testing;

import com.google.protobuf.ByteString;
import com.salesforce.eventbus.protobuf.ConsumerEvent;
import com.salesforce.eventbus.protobuf.FetchRequest;
import com.salesforce.eventbus.protobuf.FetchResponse;
import com.salesforce.eventbus.protobuf.ProducerEvent;
import com.salesforce.eventbus.protobuf.PubSubGrpc;
import com.salesforce.eventbus.protobuf.PublishRequest;
import com.salesforce.eventbus.protobuf.PublishResponse;
import com.salesforce.eventbus.protobuf.PublishResult;
import com.salesforce.eventbus.protobuf.ReplayPreset;
import com.salesforce.eventbus.protobuf.SchemaInfo;
import com.salesforce.eventbus.protobuf.SchemaRequest;
import com.salesforce.eventbus.protobuf.TopicInfo;
import com.salesforce.eventbus.protobuf.TopicRequest;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * In-process fake of the Salesforce Pub/Sub API (eventbus.v1.PubSub): in-memory topics with
 * monotonically increasing (but opaque-to-clients) replayIds, GetTopic/GetSchema/Publish, and
 * Subscribe honoring LATEST/EARLIEST/CUSTOM replay presets with num_requested flow control.
 */
public final class FakePubSubServer implements AutoCloseable {

    public static final Metadata.Key<String> ERROR_CODE_TRAILER =
            Metadata.Key.of("error-code", Metadata.ASCII_STRING_MARSHALLER);
    public static final String INVALID_REPLAY_ERROR =
            "sfdc.platform.eventbus.grpc.subscription.fetch.replayid.corrupted";

    private final String serverName = "fake-pubsub-" + UUID.randomUUID();
    private final Map<String, TopicState> topics = new ConcurrentHashMap<>();
    private final Map<String, Schema> schemasById = new ConcurrentHashMap<>();
    private final AtomicLong replaySequence = new AtomicLong(1000);
    private final AtomicReference<Status> nextSubscribeFailure = new AtomicReference<>();
    private final AtomicReference<String> nextSubscribeFailureCode = new AtomicReference<>();
    private final AtomicReference<java.util.function.Function<ProducerEvent, String>> publishFailer =
            new AtomicReference<>(e -> null);
    private Server server;

    public FakePubSubServer start() throws IOException {
        server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new Impl())
                .build()
                .start();
        return this;
    }

    /** Serves on a real localhost port (plaintext) for connector-level tests. */
    public FakePubSubServer startOnFreePort() throws IOException {
        server = io.grpc.ServerBuilder.forPort(0)
                .addService(new Impl())
                .build()
                .start();
        return this;
    }

    /** host:port endpoint when started with {@link #startOnFreePort()}. */
    public String endpoint() {
        return "localhost:" + server.getPort();
    }

    public ManagedChannel channel() {
        return InProcessChannelBuilder.forName(serverName).directExecutor().build();
    }

    /** Registers a topic with its Avro schema; returns the schema ID. */
    public String createTopic(String topicName, Schema avroSchema) {
        String schemaId = "schema-" + Math.abs(avroSchema.toString().hashCode());
        schemasById.put(schemaId, avroSchema);
        topics.put(topicName, new TopicState(topicName, schemaId, avroSchema));
        return schemaId;
    }

    /** Test-side publish: appends an event to the topic and returns its replayId. */
    public byte[] publishEvent(String topicName, GenericRecord record) {
        TopicState topic = requireTopic(topicName);
        byte[] replayId = nextReplayId();
        StoredEvent event = new StoredEvent(replayId, encode(topic.schema, record),
                topic.schemaId, UUID.randomUUID().toString());
        synchronized (topic) {
            topic.events.add(event);
        }
        topic.subscribers.forEach(Subscriber::deliverPending);
        return replayId;
    }

    /** Events published to a topic via the Publish RPC (for sink tests), decoded by callers. */
    public List<ProducerEvent> publishedViaRpc(String topicName) {
        return List.copyOf(requireTopic(topicName).rpcPublished);
    }

    /** The next Subscribe call fails with the given status + Salesforce error-code trailer. */
    public void failNextSubscribe(Status status, String errorCode) {
        nextSubscribeFailure.set(status);
        nextSubscribeFailureCode.set(errorCode);
    }

    /** Per-event publish failure injection: return an error message to fail an event, null to accept. */
    public void setPublishFailer(java.util.function.Function<ProducerEvent, String> failer) {
        publishFailer.set(failer != null ? failer : e -> null);
    }

    public byte[] latestReplayId(String topicName) {
        TopicState topic = requireTopic(topicName);
        synchronized (topic) {
            return topic.events.isEmpty() ? new byte[]{0}
                    : topic.events.get(topic.events.size() - 1).replayId;
        }
    }

    private byte[] nextReplayId() {
        return ByteBuffer.allocate(8).putLong(replaySequence.incrementAndGet()).array();
    }

    private TopicState requireTopic(String topicName) {
        TopicState topic = topics.get(topicName);
        if (topic == null) {
            throw new IllegalStateException("Fake Pub/Sub has no topic " + topicName);
        }
        return topic;
    }

    private static byte[] encode(Schema schema, GenericRecord record) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        if (server != null) {
            server.shutdownNow();
        }
    }

    private static final class TopicState {
        final String name;
        final String schemaId;
        final Schema schema;
        final List<StoredEvent> events = new ArrayList<>();
        final List<ProducerEvent> rpcPublished = new CopyOnWriteArrayList<>();
        final List<Subscriber> subscribers = new CopyOnWriteArrayList<>();

        TopicState(String name, String schemaId, Schema schema) {
            this.name = name;
            this.schemaId = schemaId;
            this.schema = schema;
        }
    }

    private record StoredEvent(byte[] replayId, byte[] payload, String schemaId, String eventId) {
    }

    private final class Subscriber {
        private final TopicState topic;
        private final StreamObserver<FetchResponse> out;
        private int cursor; // index of next event to deliver
        private int demand;

        Subscriber(TopicState topic, StreamObserver<FetchResponse> out, int startIndex) {
            this.topic = topic;
            this.out = out;
            this.cursor = startIndex;
        }

        synchronized void addDemand(int numRequested) {
            demand += numRequested;
            deliverPending();
        }

        synchronized void deliverPending() {
            List<StoredEvent> batch = new ArrayList<>();
            byte[] latest;
            synchronized (topic) {
                while (demand > 0 && cursor < topic.events.size()) {
                    batch.add(topic.events.get(cursor));
                    cursor++;
                    demand--;
                }
                latest = topic.events.isEmpty() ? new byte[]{0}
                        : topic.events.get(topic.events.size() - 1).replayId;
            }
            if (batch.isEmpty()) {
                return;
            }
            FetchResponse.Builder response = FetchResponse.newBuilder()
                    .setLatestReplayId(ByteString.copyFrom(latest))
                    .setPendingNumRequested(demand);
            for (StoredEvent event : batch) {
                response.addEvents(ConsumerEvent.newBuilder()
                        .setReplayId(ByteString.copyFrom(event.replayId))
                        .setEvent(ProducerEvent.newBuilder()
                                .setId(event.eventId)
                                .setSchemaId(event.schemaId)
                                .setPayload(ByteString.copyFrom(event.payload))));
            }
            out.onNext(response.build());
        }
    }

    private final class Impl extends PubSubGrpc.PubSubImplBase {

        @Override
        public void getTopic(TopicRequest request, StreamObserver<TopicInfo> responseObserver) {
            TopicState topic = topics.get(request.getTopicName());
            if (topic == null) {
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("no topic " + request.getTopicName()).asRuntimeException());
                return;
            }
            responseObserver.onNext(TopicInfo.newBuilder()
                    .setTopicName(topic.name)
                    .setTenantGuid(MockSalesforceServer.ORG_ID)
                    .setCanPublish(true)
                    .setCanSubscribe(true)
                    .setSchemaId(topic.schemaId)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void getSchema(SchemaRequest request, StreamObserver<SchemaInfo> responseObserver) {
            Schema schema = schemasById.get(request.getSchemaId());
            if (schema == null) {
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("no schema " + request.getSchemaId()).asRuntimeException());
                return;
            }
            responseObserver.onNext(SchemaInfo.newBuilder()
                    .setSchemaId(request.getSchemaId())
                    .setSchemaJson(schema.toString())
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void publish(PublishRequest request, StreamObserver<PublishResponse> responseObserver) {
            TopicState topic = topics.get(request.getTopicName());
            if (topic == null) {
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("no topic " + request.getTopicName()).asRuntimeException());
                return;
            }
            PublishResponse.Builder response = PublishResponse.newBuilder().setSchemaId(topic.schemaId);
            for (ProducerEvent event : request.getEventsList()) {
                String error = publishFailer.get().apply(event);
                if (error != null) {
                    response.addResults(PublishResult.newBuilder()
                            .setCorrelationKey(event.getId())
                            .setError(com.salesforce.eventbus.protobuf.Error.newBuilder()
                                    .setCode(com.salesforce.eventbus.protobuf.ErrorCode.PUBLISH)
                                    .setMsg(error)));
                    continue;
                }
                topic.rpcPublished.add(event);
                byte[] replayId = nextReplayId();
                synchronized (topic) {
                    topic.events.add(new StoredEvent(replayId, event.getPayload().toByteArray(),
                            topic.schemaId, event.getId()));
                }
                response.addResults(PublishResult.newBuilder()
                        .setReplayId(ByteString.copyFrom(replayId))
                        .setCorrelationKey(event.getId()));
            }
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
            topic.subscribers.forEach(Subscriber::deliverPending);
        }

        @Override
        public StreamObserver<FetchRequest> subscribe(StreamObserver<FetchResponse> responseObserver) {
            Status failure = nextSubscribeFailure.getAndSet(null);
            if (failure != null) {
                Metadata trailers = new Metadata();
                String code = nextSubscribeFailureCode.getAndSet(null);
                if (code != null) {
                    trailers.put(ERROR_CODE_TRAILER, code);
                }
                return new StreamObserver<>() {
                    @Override
                    public void onNext(FetchRequest value) {
                        responseObserver.onError(failure.asRuntimeException(trailers));
                    }

                    @Override
                    public void onError(Throwable t) {
                    }

                    @Override
                    public void onCompleted() {
                    }
                };
            }
            return new StreamObserver<>() {
                private Subscriber subscriber;

                @Override
                public void onNext(FetchRequest request) {
                    if (subscriber == null) {
                        TopicState topic = topics.get(request.getTopicName());
                        if (topic == null) {
                            responseObserver.onError(Status.NOT_FOUND
                                    .withDescription("no topic " + request.getTopicName())
                                    .asRuntimeException());
                            return;
                        }
                        Integer start = resolveStartIndex(topic, request);
                        if (start == null) {
                            Metadata trailers = new Metadata();
                            trailers.put(ERROR_CODE_TRAILER, INVALID_REPLAY_ERROR);
                            responseObserver.onError(Status.INVALID_ARGUMENT
                                    .withDescription("corrupted replay id").asRuntimeException(trailers));
                            return;
                        }
                        subscriber = new Subscriber(topic, responseObserver, start);
                        topic.subscribers.add(subscriber);
                        // Keepalive acknowledging the subscription, like the real service:
                        // an empty FetchResponse carrying the latest replayId.
                        byte[] latest;
                        synchronized (topic) {
                            latest = topic.events.isEmpty() ? new byte[]{0}
                                    : topic.events.get(topic.events.size() - 1).replayId();
                        }
                        responseObserver.onNext(FetchResponse.newBuilder()
                                .setLatestReplayId(ByteString.copyFrom(latest))
                                .build());
                    }
                    subscriber.addDemand(request.getNumRequested());
                }

                @Override
                public void onError(Throwable t) {
                    removeSubscriber();
                }

                @Override
                public void onCompleted() {
                    removeSubscriber();
                    responseObserver.onCompleted();
                }

                private void removeSubscriber() {
                    if (subscriber != null) {
                        subscriber.topic.subscribers.remove(subscriber);
                    }
                }
            };
        }

        /** Returns the event index to start at, or null for an invalid CUSTOM replayId. */
        private Integer resolveStartIndex(TopicState topic, FetchRequest request) {
            synchronized (topic) {
                if (request.getReplayPreset() == ReplayPreset.EARLIEST) {
                    return 0;
                }
                if (request.getReplayPreset() == ReplayPreset.CUSTOM) {
                    byte[] wanted = request.getReplayId().toByteArray();
                    if (wanted.length == 0 || wanted.length > 8) {
                        return null; // corrupted
                    }
                    // Positional semantics like the real service: resume strictly after the
                    // given position. Keepalive replayIds (not tied to an event) are valid.
                    java.math.BigInteger position = new java.math.BigInteger(1, wanted);
                    for (int i = 0; i < topic.events.size(); i++) {
                        java.math.BigInteger eventPosition =
                                new java.math.BigInteger(1, topic.events.get(i).replayId);
                        if (eventPosition.compareTo(position) > 0) {
                            return i;
                        }
                    }
                    return topic.events.size();
                }
                return topic.events.size(); // LATEST
            }
        }
    }
}
