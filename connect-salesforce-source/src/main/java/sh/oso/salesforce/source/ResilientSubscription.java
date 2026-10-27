package sh.oso.salesforce.source;

import com.salesforce.eventbus.protobuf.ReplayPreset;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.salesforce.auth.Session;
import sh.oso.salesforce.common.SalesforceException;
import sh.oso.salesforce.pubsub.DecodedEvent;
import sh.oso.salesforce.pubsub.PubSubClient;
import sh.oso.salesforce.pubsub.PubSubStreamException;
import sh.oso.salesforce.pubsub.PubSubSubscription;
import sh.oso.salesforce.pubsub.SubscribeOptions;

import java.util.List;
import java.util.function.Supplier;

/**
 * A Pub/Sub subscription that survives stream failures, shared by the source pipelines.
 *
 * <ul>
 *   <li>Transient failures (UNAVAILABLE, stream resets, a server-completed stream) close the
 *       stream and resubscribe from the pipeline's position after a non-blocking backoff.</li>
 *   <li>UNAUTHENTICATED invalidates the session that opened the stream and resubscribes with a
 *       fresh one; repeated rejections are fatal.</li>
 *   <li>An expired or corrupted replay position is rethrown as {@link PubSubStreamException}
 *       for the pipeline to recover according to {@code sf.gap.recovery}.</li>
 *   <li>Anything else (missing topic, missing permission, an undecodable event) is a
 *       {@link FatalPipelineException}: retrying cannot fix it.</li>
 * </ul>
 */
final class ResilientSubscription implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ResilientSubscription.class);
    private static final long POLL_TIMEOUT_MS = 100;
    // Salesforce acknowledges an idle subscription only with its keepalive, within about 270 s.
    private static final long PROBE_TIMEOUT_MS = 330_000;
    private static final int MAX_CONSECUTIVE_AUTH_FAILURES = 3;

    private final PubSubClient pubsub;
    private final String topicName;
    private final String description;
    private final String notFoundHint;
    private final int batchSize;
    private final Backoff backoff = new Backoff();

    private PubSubSubscription subscription;
    private PubSubSubscription probe;
    private long probeStartedAt;
    private boolean delivered;
    private byte[] knownPosition;
    private int authFailures;

    ResilientSubscription(PubSubClient pubsub, String topicName, String description,
                          String notFoundHint, int batchSize) {
        this.pubsub = pubsub;
        this.topicName = topicName;
        this.description = description;
        this.notFoundHint = notFoundHint;
        this.batchSize = batchSize;
    }

    String topicName() {
        return topicName;
    }

    /**
     * Returns the next batch of events, (re)subscribing from {@code resumeFrom} when there is no
     * live stream. Returns an empty list while backing off after a transient failure.
     *
     * @throws PubSubStreamException when the replay position is expired or corrupted
     * @throws FatalPipelineException when the failure cannot be fixed by retrying
     */
    List<DecodedEvent> poll(Supplier<SubscribeOptions> resumeFrom) throws InterruptedException {
        if (subscription == null) {
            if (!backoff.ready()) {
                return List.of();
            }
            try {
                open(resumeFrom.get());
            } catch (SalesforceException e) {
                return streamFailed(e, null);
            }
        }
        List<DecodedEvent> events;
        try {
            events = subscription.poll(batchSize, POLL_TIMEOUT_MS);
        } catch (SalesforceException e) {
            return streamFailed(e, subscription.session());
        }
        if (!events.isEmpty()) {
            delivered = true;
        }
        if (subscription.isEstablished()) {
            backoff.reset();
            authFailures = 0;
        }
        if (events.isEmpty() && subscription.isBroken()) {
            // Completed by the server without an error: resubscribe from the current position.
            LOG.info("{} stream on {} was closed by the server; resubscribing", description, topicName);
            closeStream();
            backoff.failed();
        }
        return events;
    }

    /**
     * Returns the topic's current replay position once a LATEST probe subscription has been
     * acknowledged, or null while it is pending or after a transient failure. Never blocks:
     * Salesforce answers an idle subscription only when an event arrives or with its keepalive
     * (within about 270 s), so the caller asks again on later polls.
     */
    byte[] probeLatestReplayId() throws InterruptedException {
        if (probe == null) {
            if (!backoff.ready()) {
                return null;
            }
            try {
                probe = pubsub.subscribe(SubscribeOptions.latest(topicName, batchSize));
            } catch (SalesforceException e) {
                retryOrFail(e, null);
                return null;
            }
            probeStartedAt = System.currentTimeMillis();
            LOG.info("Probing the current position of {}; Salesforce answers when an event arrives "
                    + "or with its keepalive, within about 270 s", topicName);
        }
        try {
            if (probe.awaitEstablished(0) && probe.latestReplayId() != null) {
                byte[] position = probe.latestReplayId();
                closeProbe();
                backoff.reset();
                authFailures = 0;
                return position;
            }
        } catch (SalesforceException e) {
            Session session = probe.session();
            closeProbe();
            retryOrFail(e, session);
            return null;
        }
        if (probe.isBroken() || System.currentTimeMillis() - probeStartedAt > PROBE_TIMEOUT_MS) {
            LOG.warn("No position from {} after {} s; probing again", topicName,
                    (System.currentTimeMillis() - probeStartedAt) / 1000);
            closeProbe();
            backoff.failed();
        }
        return null;
    }

    private void closeProbe() {
        if (probe != null) {
            probe.close();
            probe = null;
        }
    }

    /** Forgets any position learned from a stream; the next subscription uses the caller's options. */
    void reset() {
        closeStream();
        knownPosition = null;
    }

    private void open(SubscribeOptions requested) {
        SubscribeOptions options = requested;
        if (requested.replayPreset() == ReplayPreset.LATEST && knownPosition != null) {
            // A LATEST stream broke before delivering anything: resume from the position it
            // reported rather than LATEST again, so events published meanwhile are not skipped.
            options = SubscribeOptions.custom(topicName, knownPosition, batchSize);
        }
        subscription = pubsub.subscribe(options);
        delivered = false;
        LOG.info("Subscribed to {} ({})", topicName, options.replayPreset());
    }

    private List<DecodedEvent> streamFailed(SalesforceException failure, Session session) {
        rememberPosition();
        closeStream();
        if (failure instanceof PubSubStreamException stream && stream.isInvalidReplay()) {
            knownPosition = null;
            throw stream;
        }
        retryOrFail(failure, session);
        return List.of();
    }

    /** Schedules a retry for a recoverable failure; throws {@link FatalPipelineException} otherwise. */
    private void retryOrFail(SalesforceException failure, Session session) {
        if (failure instanceof PubSubStreamException stream) {
            if (stream.isUnauthenticated()) {
                if (++authFailures >= MAX_CONSECUTIVE_AUTH_FAILURES) {
                    throw new FatalPipelineException(description + " on " + topicName
                            + " was rejected as unauthenticated " + authFailures
                            + " times in a row; check the connected app and integration user", failure);
                }
                if (session != null) {
                    pubsub.invalidateSession(session);
                }
                long delay = backoff.failed();
                LOG.warn("{} on {} was rejected as unauthenticated; re-authenticating and resubscribing in {} ms",
                        description, topicName, delay);
                return;
            }
        }
        if (failure.isRetryable()) {
            long delay = backoff.failed();
            LOG.warn("Transient failure on {} ({}); retrying in {} ms", topicName, failure.getMessage(), delay);
            return;
        }
        throw new FatalPipelineException(description + " on " + topicName + " failed: "
                + failure.getMessage() + hint(failure), failure);
    }

    private String hint(SalesforceException failure) {
        if (failure instanceof PubSubStreamException stream) {
            if (stream.code() == Status.Code.NOT_FOUND) {
                return ". " + notFoundHint;
            }
            if (stream.code() == Status.Code.PERMISSION_DENIED) {
                return ". Check that the integration user has permission to subscribe to " + topicName;
            }
        }
        return "";
    }

    private void rememberPosition() {
        if (subscription != null && subscription.replayPreset() == ReplayPreset.LATEST && !delivered
                && subscription.latestReplayId() != null) {
            knownPosition = subscription.latestReplayId();
        }
    }

    private void closeStream() {
        if (subscription != null) {
            subscription.close();
            subscription = null;
        }
    }

    @Override
    public void close() {
        closeProbe();
        closeStream();
    }
}
