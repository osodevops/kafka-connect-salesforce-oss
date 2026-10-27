package sh.oso.salesforce.pubsub;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Base64;

/**
 * Representations of Pub/Sub replay IDs. A replay ID is opaque bytes and must be stored and
 * sent back verbatim; the decimal form is for display only.
 *
 * <p>Salesforce's replay IDs start with the event's 8-byte big-endian position, followed by
 * bytes identifying the event bus partition (for example {@code ...core1.sfdc-...}). The
 * position is the numeric ReplayId the Streaming API reports for the same event.</p>
 */
public final class ReplayIds {

    private ReplayIds() {
    }

    public static String toBase64(byte[] replayId) {
        return Base64.getEncoder().encodeToString(replayId);
    }

    public static byte[] fromBase64(String encoded) {
        return Base64.getDecoder().decode(encoded);
    }

    /**
     * The numeric ReplayId Salesforce reports for the event (such as {@code 4093842}): the first
     * 8 bytes as an unsigned big-endian number. Shorter values are read whole; an empty value
     * renders as an empty string.
     */
    public static String toDisplayString(byte[] replayId) {
        if (replayId.length == 0) {
            return "";
        }
        return new BigInteger(1, Arrays.copyOf(replayId, Math.min(replayId.length, 8))).toString();
    }
}
