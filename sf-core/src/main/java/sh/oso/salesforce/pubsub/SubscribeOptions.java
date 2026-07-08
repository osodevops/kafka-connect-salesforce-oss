package sh.oso.salesforce.pubsub;

import com.salesforce.eventbus.protobuf.ReplayPreset;

import java.util.Objects;

/** Options for a Pub/Sub Subscribe stream. */
public final class SubscribeOptions {

    private final String topicName;
    private final ReplayPreset replayPreset;
    private final byte[] replayId;
    private final int batchSize;

    private SubscribeOptions(String topicName, ReplayPreset replayPreset, byte[] replayId, int batchSize) {
        this.topicName = Objects.requireNonNull(topicName, "topicName");
        this.replayPreset = replayPreset;
        this.replayId = replayId;
        if (batchSize < 1 || batchSize > 100) {
            throw new IllegalArgumentException("Pub/Sub num_requested must be 1..100, got " + batchSize);
        }
        this.batchSize = batchSize;
    }

    public static SubscribeOptions latest(String topicName, int batchSize) {
        return new SubscribeOptions(topicName, ReplayPreset.LATEST, null, batchSize);
    }

    public static SubscribeOptions earliest(String topicName, int batchSize) {
        return new SubscribeOptions(topicName, ReplayPreset.EARLIEST, null, batchSize);
    }

    /** Resume after a stored replayId (exclusive). */
    public static SubscribeOptions custom(String topicName, byte[] replayId, int batchSize) {
        return new SubscribeOptions(topicName, ReplayPreset.CUSTOM,
                Objects.requireNonNull(replayId, "replayId"), batchSize);
    }

    public String topicName() {
        return topicName;
    }

    public ReplayPreset replayPreset() {
        return replayPreset;
    }

    public byte[] replayId() {
        return replayId;
    }

    public int batchSize() {
        return batchSize;
    }
}
