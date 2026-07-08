package sh.oso.salesforce.source;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Per-SObject source offset. Serialized into Kafka Connect's {@code sourceOffset} map;
 * the partition is {@code {"sobject": <name>}}.
 */
public final class SObjectOffset {

    public static final String VERSION = "1";

    public enum Mode {SNAPSHOT, EVENT_DRIVEN, POLLING}

    private Mode mode = Mode.SNAPSHOT;
    private byte[] replayId;
    private byte[] probeReplayId;
    private Long snapshotCompletionTimestamp;
    private String lastSystemModstamp;
    private Long commitTimestamp;

    public static Map<String, Object> partition(String sobject) {
        return Map.of("sobject", sobject);
    }

    public static SObjectOffset fromMap(Map<String, Object> map) {
        SObjectOffset offset = new SObjectOffset();
        if (map == null || map.isEmpty()) {
            return offset;
        }
        String mode = (String) map.get("mode");
        if (mode != null) {
            offset.mode = Mode.valueOf(mode);
        }
        String replayId = (String) map.get("replayId");
        if (replayId != null) {
            offset.replayId = Base64.getDecoder().decode(replayId);
        }
        String probe = (String) map.get("probeReplayId");
        if (probe != null) {
            offset.probeReplayId = Base64.getDecoder().decode(probe);
        }
        offset.snapshotCompletionTimestamp = (Long) map.get("snapshotCompletionTimestamp");
        offset.lastSystemModstamp = (String) map.get("lastSystemModstamp");
        offset.commitTimestamp = (Long) map.get("commitTimestamp");
        return offset;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("offsetVersion", VERSION);
        map.put("mode", mode.name());
        if (replayId != null) {
            map.put("replayId", Base64.getEncoder().encodeToString(replayId));
        }
        if (probeReplayId != null) {
            map.put("probeReplayId", Base64.getEncoder().encodeToString(probeReplayId));
        }
        if (snapshotCompletionTimestamp != null) {
            map.put("snapshotCompletionTimestamp", snapshotCompletionTimestamp);
        }
        if (lastSystemModstamp != null) {
            map.put("lastSystemModstamp", lastSystemModstamp);
        }
        if (commitTimestamp != null) {
            map.put("commitTimestamp", commitTimestamp);
        }
        return map;
    }

    public Mode mode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public byte[] replayId() {
        return replayId;
    }

    public void setReplayId(byte[] replayId) {
        this.replayId = replayId;
    }

    public byte[] probeReplayId() {
        return probeReplayId;
    }

    public void setProbeReplayId(byte[] probeReplayId) {
        this.probeReplayId = probeReplayId;
    }

    public Long snapshotCompletionTimestamp() {
        return snapshotCompletionTimestamp;
    }

    public void setSnapshotCompletionTimestamp(Long ts) {
        this.snapshotCompletionTimestamp = ts;
    }

    public String lastSystemModstamp() {
        return lastSystemModstamp;
    }

    public void setLastSystemModstamp(String lastSystemModstamp) {
        this.lastSystemModstamp = lastSystemModstamp;
    }

    public Long commitTimestamp() {
        return commitTimestamp;
    }

    public void setCommitTimestamp(Long commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }
}
