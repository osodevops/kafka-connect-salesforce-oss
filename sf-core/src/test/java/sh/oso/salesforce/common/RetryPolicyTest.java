package sh.oso.salesforce.common;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryPolicyTest {

    @Test
    void retriesRetryableFailuresUntilSuccess() {
        RetryPolicy policy = new RetryPolicy(Duration.ofMillis(1), Duration.ofMillis(5), Duration.ofSeconds(5));
        AtomicInteger attempts = new AtomicInteger();
        String result = policy.execute("op", () -> {
            if (attempts.incrementAndGet() < 3) {
                throw new SalesforceException("transient", null, true);
            }
            return "ok";
        });
        assertThat(result).isEqualTo("ok");
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void failsFastOnNonRetryable() {
        RetryPolicy policy = new RetryPolicy(Duration.ofMillis(1), Duration.ofMillis(5), Duration.ofSeconds(5));
        AtomicInteger attempts = new AtomicInteger();
        assertThatThrownBy(() -> policy.execute("op", () -> {
            attempts.incrementAndGet();
            throw new SalesforceApiException(400, "INVALID_FIELD", "bad field", false);
        })).isInstanceOf(SalesforceApiException.class);
        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    void exhaustsBudget() {
        RetryPolicy policy = new RetryPolicy(Duration.ofMillis(5), Duration.ofMillis(10), Duration.ofMillis(50));
        assertThatThrownBy(() -> policy.execute("op", () -> {
            throw new SalesforceException("always transient", null, true);
        })).isInstanceOf(SalesforceException.class)
                .hasMessageContaining("Retry budget");
    }

    @Test
    void classifiesHttpAndGrpcErrors() {
        assertThat(ErrorClassifier.isRetryableHttp(429, null)).isTrue();
        assertThat(ErrorClassifier.isRetryableHttp(503, null)).isTrue();
        assertThat(ErrorClassifier.isRetryableHttp(403, "REQUEST_LIMIT_EXCEEDED")).isTrue();
        assertThat(ErrorClassifier.isRetryableHttp(403, "API_DISABLED_FOR_ORG")).isFalse();
        assertThat(ErrorClassifier.isRetryableHttp(400, null)).isFalse();
        assertThat(ErrorClassifier.isRetryableGrpc(io.grpc.Status.UNAVAILABLE)).isTrue();
        assertThat(ErrorClassifier.isRetryableGrpc(io.grpc.Status.RESOURCE_EXHAUSTED)).isTrue();
        assertThat(ErrorClassifier.isRetryableGrpc(io.grpc.Status.INVALID_ARGUMENT)).isFalse();
    }
}
