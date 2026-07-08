package sh.oso.salesforce.pubsub;

import com.google.protobuf.ByteString;
import com.salesforce.eventbus.protobuf.ConsumerEvent;
import com.salesforce.eventbus.protobuf.FetchRequest;
import com.salesforce.eventbus.protobuf.FetchResponse;
import com.salesforce.eventbus.protobuf.PubSubGrpc;
import com.salesforce.eventbus.protobuf.ReplayPreset;
import io.grpc.stub.StreamObserver;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.salesforce.common.SalesforceException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * A live Pub/Sub Subscribe stream with pull-based flow control.
 *
 * <p>Decoded events are buffered internally; callers drain them with {@link #poll}.
 * The subscription tops up {@code num_requested} whenever outstanding demand drops below
 * half the configured batch size. Stream errors surface on the next {@code poll} call.</p>
 */
public final class PubSubSubscription implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubSubscription.class);

    private final SubscribeOptions options;
    private final Function<String, Schema> schemaProvider;
    private final BlockingQueue<DecodedEvent> buffer = new LinkedBlockingQueue<>();
    private final AtomicInteger outstanding = new AtomicInteger();
    private final AtomicReference<Throwable> streamError = new AtomicReference<>();
    private final AtomicReference<byte[]> latestReplayId = new AtomicReference<>();
    private volatile boolean completed;
    private volatile boolean closed;
    private final java.util.concurrent.CountDownLatch firstResponse = new java.util.concurrent.CountDownLatch(1);
    private final StreamObserver<FetchRequest> requestObserver;

    PubSubSubscription(PubSubGrpc.PubSubStub stub, Function<String, Schema> schemaProvider,
                       SubscribeOptions options) {
        this.options = options;
        this.schemaProvider = schemaProvider;
        this.requestObserver = stub.subscribe(new StreamObserver<>() {
            @Override
            public void onNext(FetchResponse response) {
                handleResponse(response);
            }

            @Override
            public void onError(Throwable t) {
                LOG.warn("Pub/Sub subscribe stream error on {}: {}", options.topicName(), t.getMessage());
                streamError.set(t);
            }

            @Override
            public void onCompleted() {
                LOG.info("Pub/Sub subscribe stream completed on {}", options.topicName());
                completed = true;
            }
        });
        sendInitialFetch();
    }

    private void sendInitialFetch() {
        FetchRequest.Builder request = FetchRequest.newBuilder()
                .setTopicName(options.topicName())
                .setReplayPreset(options.replayPreset())
                .setNumRequested(options.batchSize());
        if (options.replayPreset() == ReplayPreset.CUSTOM) {
            request.setReplayId(ByteString.copyFrom(options.replayId()));
        }
        outstanding.set(options.batchSize());
        requestObserver.onNext(request.build());
    }

    private void handleResponse(FetchResponse response) {
        if (!response.getLatestReplayId().isEmpty()) {
            latestReplayId.set(response.getLatestReplayId().toByteArray());
        }
        firstResponse.countDown();
        for (ConsumerEvent event : response.getEventsList()) {
            outstanding.decrementAndGet();
            try {
                buffer.add(decode(event));
            } catch (RuntimeException e) {
                streamError.set(e);
                return;
            }
        }
        maybeRequestMore();
    }

    private DecodedEvent decode(ConsumerEvent event) {
        String schemaId = event.getEvent().getSchemaId();
        Schema schema = schemaProvider.apply(schemaId);
        try {
            BinaryDecoder decoder = DecoderFactory.get()
                    .binaryDecoder(event.getEvent().getPayload().toByteArray(), null);
            GenericRecord record = new GenericDatumReader<GenericRecord>(schema).read(null, decoder);
            return new DecodedEvent(
                    options.topicName(),
                    event.getEvent().getId(),
                    schemaId,
                    record,
                    event.getReplayId().toByteArray());
        } catch (IOException e) {
            throw new SalesforceException("Failed to Avro-decode event " + event.getEvent().getId()
                    + " on " + options.topicName() + " (schema " + schemaId + ")", e);
        }
    }

    private void maybeRequestMore() {
        if (closed || completed) {
            return;
        }
        int current = outstanding.get();
        // Keep demand topped up once we've consumed half a batch, but never exceed
        // batchSize outstanding + buffered (bounds memory if the caller stops polling).
        if (current <= options.batchSize() / 2 && buffer.size() < options.batchSize()) {
            int topUp = options.batchSize() - current;
            if (topUp > 0 && outstanding.compareAndSet(current, current + topUp)) {
                requestObserver.onNext(FetchRequest.newBuilder()
                        .setTopicName(options.topicName())
                        .setNumRequested(topUp)
                        .build());
            }
        }
    }

    /**
     * Drains up to {@code maxEvents} decoded events, waiting up to the timeout for the first one.
     * Throws when the underlying stream has failed.
     */
    public List<DecodedEvent> poll(int maxEvents, long timeoutMillis) throws InterruptedException {
        throwIfFailed();
        List<DecodedEvent> out = new ArrayList<>();
        DecodedEvent first = buffer.poll(timeoutMillis, TimeUnit.MILLISECONDS);
        if (first != null) {
            out.add(first);
            buffer.drainTo(out, maxEvents - 1);
        }
        throwIfFailed();
        maybeRequestMore();
        return out;
    }

    private void throwIfFailed() {
        Throwable t = streamError.get();
        if (t != null && buffer.isEmpty()) {
            if (t instanceof RuntimeException re) {
                throw re;
            }
            throw new SalesforceException("Pub/Sub subscribe stream failed: " + t.getMessage(), t, true);
        }
    }

    /**
     * Waits until the server has acknowledged the subscription with a first response
     * (Salesforce sends keepalive FetchResponses carrying the latest replayId).
     * Needed for the snapshot-handoff LATEST probe.
     */
    public boolean awaitEstablished(long timeoutMillis) throws InterruptedException {
        return firstResponse.await(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    /** True when the server ended the stream or an error was recorded. */
    public boolean isBroken() {
        return completed || streamError.get() != null;
    }

    /** Latest replayId reported by the server (for LATEST-probe during snapshot handoff). */
    public byte[] latestReplayId() {
        return latestReplayId.get();
    }

    @Override
    public void close() {
        closed = true;
        try {
            requestObserver.onCompleted();
        } catch (RuntimeException ignored) {
            // stream may already be dead
        }
    }
}
