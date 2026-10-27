package sh.oso.salesforce.pubsub;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PubSubStreamExceptionTest {

    static final String TOPIC = "/event/Order_Shipped__e";

    static StatusRuntimeException grpc(Status status, String errorCode) {
        Metadata trailers = new Metadata();
        if (errorCode != null) {
            trailers.put(PubSubStreamException.ERROR_CODE_TRAILER, errorCode);
        }
        return status.asRuntimeException(trailers);
    }

    @Test
    void invalidArgumentWithReplayErrorCodeIsInvalidReplay() {
        PubSubStreamException e = PubSubStreamException.from(grpc(Status.INVALID_ARGUMENT,
                "sfdc.platform.eventbus.grpc.subscription.fetch.replayid.corrupted"), TOPIC);

        assertThat(e.isInvalidReplay()).isTrue();
        assertThat(e.isRetryable()).isFalse();
        assertThat(e.code()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    }

    @Test
    void replayDescriptionWithoutErrorCodeIsInvalidReplay() {
        PubSubStreamException e = PubSubStreamException.from(
                grpc(Status.INVALID_ARGUMENT.withDescription("Replay ID validation failed"), null), TOPIC);

        assertThat(e.isInvalidReplay()).isTrue();
    }

    @Test
    void invalidArgumentWithoutReplayHintIsNotInvalidReplay() {
        PubSubStreamException e = PubSubStreamException.from(
                grpc(Status.INVALID_ARGUMENT.withDescription("num_requested must be positive"), null), TOPIC);

        assertThat(e.isInvalidReplay()).isFalse();
        assertThat(e.isRetryable()).isFalse();
    }

    @Test
    void unavailableAndStreamResetsAreRetryable() {
        assertThat(PubSubStreamException.from(grpc(Status.UNAVAILABLE, null), TOPIC).isRetryable()).isTrue();
        assertThat(PubSubStreamException.from(grpc(Status.INTERNAL, null), TOPIC).isRetryable()).isTrue();
        assertThat(PubSubStreamException.from(grpc(Status.RESOURCE_EXHAUSTED, null), TOPIC).isRetryable())
                .isTrue();
    }

    @Test
    void unauthenticatedIsFlagged() {
        PubSubStreamException e = PubSubStreamException.from(grpc(Status.UNAUTHENTICATED,
                "sfdc.platform.eventbus.grpc.service.auth.error"), TOPIC);

        assertThat(e.isUnauthenticated()).isTrue();
        assertThat(e.isInvalidReplay()).isFalse();
    }

    @Test
    void notFoundAndPermissionDeniedAreFatal() {
        assertThat(PubSubStreamException.from(grpc(Status.NOT_FOUND, null), TOPIC).isRetryable()).isFalse();
        assertThat(PubSubStreamException.from(grpc(Status.PERMISSION_DENIED, null), TOPIC).isRetryable())
                .isFalse();
    }

    @Test
    void messageCarriesCodeErrorCodeAndTopic() {
        PubSubStreamException e = PubSubStreamException.from(grpc(
                Status.INVALID_ARGUMENT.withDescription("corrupted replay id"), "some.error.code"), TOPIC);

        assertThat(e.getMessage()).contains(TOPIC, "INVALID_ARGUMENT", "corrupted replay id", "some.error.code");
        assertThat(e.errorCode()).isEqualTo("some.error.code");
        assertThat(e.topicName()).isEqualTo(TOPIC);
    }

    @Test
    void detectsStatusInCauseChain() {
        RuntimeException wrapped = new RuntimeException("outer", grpc(Status.UNAVAILABLE, null));

        assertThat(PubSubStreamException.hasGrpcStatus(wrapped)).isTrue();
        assertThat(PubSubStreamException.hasGrpcStatus(new IllegalStateException())).isFalse();
        assertThat(PubSubStreamException.from(wrapped, TOPIC).code()).isEqualTo(Status.Code.UNAVAILABLE);
    }
}
