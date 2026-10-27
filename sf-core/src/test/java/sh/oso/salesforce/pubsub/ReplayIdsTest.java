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
    void longerReplayIdFallsBackToBase64() {
        byte[] replayId = new byte[9];
        replayId[8] = 1;
        assertThat(ReplayIds.toDisplayString(replayId)).isEqualTo(ReplayIds.toBase64(replayId));
    }

    @Test
    void base64RoundTripsOpaqueBytes() {
        byte[] replayId = {0, 1, 2, (byte) 0xFE};
        assertThat(ReplayIds.fromBase64(ReplayIds.toBase64(replayId))).isEqualTo(replayId);
    }
}
