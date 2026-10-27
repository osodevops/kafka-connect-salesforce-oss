package sh.oso.salesforce.source;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.salesforce.pubsub.DecodedEvent;
import sh.oso.salesforce.pubsub.PlatformEvents;
import sh.oso.salesforce.pubsub.PubSubClient;
import sh.oso.salesforce.pubsub.PubSubStreamException;
import sh.oso.salesforce.pubsub.SubscribeOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Streams one platform event channel ({@code /event/<name>__e}) into Kafka. Each event becomes
 * one record carrying the event's fields as published; no lookups are made back to Salesforce.
 */
final class PlatformEventPipeline implements SourcePipeline {

    private static final Logger LOG = LoggerFactory.getLogger(PlatformEventPipeline.class);

    private final SourceConfig config;
    private final String eventName;
    private final String channel;
    private final ResilientSubscription stream;
    private final PlatformEventRecordFactory records;
    private final PlatformEventOffset offset;

    private SubscribeOptions recoveryStart; // set after the stored position was rejected

    PlatformEventPipeline(SourceConfig config, String eventName, PubSubClient pubsub,
                          Map<String, Object> storedOffset) {
        this.config = config;
        this.eventName = eventName;
        this.channel = PlatformEvents.channel(eventName);
        this.stream = new ResilientSubscription(pubsub, channel, "Platform event stream for " + eventName,
                "Check that " + eventName + " exists (API name, exact casing) and that the integration user "
                        + "has Read access to it",
                config.getInt(SourceConfig.PUBSUB_BATCH_SIZE));
        this.records = new PlatformEventRecordFactory(eventName, config.topicFor(eventName));
        this.offset = PlatformEventOffset.fromMap(storedOffset);
        LOG.info("Pipeline for platform event {} starting ({})", eventName,
                offset.replayId() != null ? "resuming from stored position" : "sf.event.start=" + config.eventStart());
    }

    @Override
    public String name() {
        return eventName;
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<DecodedEvent> events;
        try {
            events = stream.poll(this::resumeOptions);
        } catch (PubSubStreamException e) {
            return recoverLostReplay(e);
        }
        List<SourceRecord> out = new ArrayList<>(events.size());
        for (DecodedEvent event : events) {
            offset.setReplayId(event.replayId());
            out.add(records.fromEvent(event, offset.toMap()));
        }
        return out;
    }

    private SubscribeOptions resumeOptions() {
        int batchSize = config.getInt(SourceConfig.PUBSUB_BATCH_SIZE);
        if (offset.replayId() != null) {
            return SubscribeOptions.custom(channel, offset.replayId(), batchSize);
        }
        if (recoveryStart != null) {
            return recoveryStart;
        }
        return config.eventStart() == SourceConfig.EventStart.ALL
                ? SubscribeOptions.earliest(channel, batchSize)
                : SubscribeOptions.latest(channel, batchSize);
    }

    /**
     * The stored position was rejected (typically more than 72 hours of downtime). Platform
     * events cannot be re-queried, so {@code resync} replays everything Salesforce still retains.
     */
    private List<SourceRecord> recoverLostReplay(PubSubStreamException lost) {
        SourceConfig.GapRecovery strategy = config.gapRecovery();
        if (strategy == SourceConfig.GapRecovery.FAIL) {
            // Keep the stored offset: a restarted task fails the same way until an operator acts.
            throw new FatalPipelineException("Replay position lost for platform event " + eventName
                    + " and sf.gap.recovery=fail", lost);
        }
        int batchSize = config.getInt(SourceConfig.PUBSUB_BATCH_SIZE);
        stream.reset();
        offset.setReplayId(null);
        if (strategy == SourceConfig.GapRecovery.LATEST) {
            recoveryStart = SubscribeOptions.latest(channel, batchSize);
            LOG.warn("Replay position lost for {} ({}); resubscribing from the newest event, events during "
                    + "the downtime are lost", eventName, lost.getMessage());
        } else {
            recoveryStart = SubscribeOptions.earliest(channel, batchSize);
            LOG.warn("Replay position lost for {} ({}); resubscribing from the earliest retained event",
                    eventName, lost.getMessage());
        }
        return List.of();
    }

    @Override
    public void close() {
        stream.close();
    }
}
