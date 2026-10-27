package sh.oso.salesforce.source;

import org.apache.kafka.connect.errors.ConnectException;
import sh.oso.salesforce.pubsub.ReplayIds;

import java.util.HashMap;
import java.util.Map;

/**
 * Per-platform-event source offset: the Pub/Sub replay position (opaque bytes, base64) of the
 * last emitted event. The partition is {@code {"platformEvent": <name>}}.
 */
final class PlatformEventOffset {

    static final String VERSION = "1";

    private byte[] replayId;

    static Map<String, Object> partition(String eventName) {
        return Map.of("platformEvent", eventName);
    }

    static PlatformEventOffset fromMap(Map<String, Object> map) {
        PlatformEventOffset offset = new PlatformEventOffset();
        if (map == null || map.isEmpty()) {
            return offset;
        }
        Object version = map.get("offsetVersion");
        if (version != null && !VERSION.equals(version)) {
            throw new ConnectException("Unsupported platform event offset version " + version
                    + "; this connector version writes " + VERSION);
        }
        String replayId = (String) map.get("replayId");
        if (replayId != null) {
            offset.replayId = ReplayIds.fromBase64(replayId);
        }
        return offset;
    }

    Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("offsetVersion", VERSION);
        if (replayId != null) {
            map.put("replayId", ReplayIds.toBase64(replayId));
        }
        return map;
    }

    byte[] replayId() {
        return replayId;
    }

    void setReplayId(byte[] replayId) {
        this.replayId = replayId;
    }
}
