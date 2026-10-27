package sh.oso.salesforce.pubsub;

import java.math.BigInteger;
import java.util.Base64;

/**
 * Representations of Pub/Sub replay IDs. A replay ID is opaque bytes and must be stored and
 * sent back verbatim; the decimal form is for display only.
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
     * The replay ID as an unsigned big-endian decimal (the numeric form Salesforce shows, such
     * as {@code 4093842}), or base64 when it is not 1 to 8 bytes long.
     */
    public static String toDisplayString(byte[] replayId) {
        if (replayId.length >= 1 && replayId.length <= 8) {
            return new BigInteger(1, replayId).toString();
        }
        return toBase64(replayId);
    }
}
