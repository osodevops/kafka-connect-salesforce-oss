package sh.oso.salesforce.pubsub;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

class ReplayIdsTest {

    @Test
    void eightByteReplayIdRendersAsUnsignedDecimal() {
        byte[] replayId = ByteBuffer.allocate(8).putLong(4_093_842L).array();
        assertThat(ReplayIds.toDisplayString(replayId)).isEqualTo("4093842");
        assertThat(ReplayIds.toDisplayString(new byte[]{(byte) 0xFF})).isEqualTo("255");
        assertThat(ReplayIds.toDisplayString(ByteBuffer.allocate(8).putLong(-1L).array()))
                .isEqualTo("18446744073709551615");
    }

    @Test
    void salesforceShapedReplayIdRendersItsPosition() {
        // As delivered by the Pub/Sub API: 8-byte position, then the event bus partition.
        byte[] replayId = ByteBuffer.allocate(29).putLong(4_093_842L)
                .put(new byte[]{0, 0x13}).put("N-core9.sfdc-test".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
                .array();
        assertThat(ReplayIds.toDisplayString(replayId)).isEqualTo("4093842");
        assertThat(ReplayIds.toDisplayString(new byte[0])).isEmpty();
    }

    @Test
    void base64RoundTripsOpaqueBytes() {
        byte[] replayId = {0, 1, 2, (byte) 0xFE};
        assertThat(ReplayIds.fromBase64(ReplayIds.toBase64(replayId))).isEqualTo(replayId);
    }
}
