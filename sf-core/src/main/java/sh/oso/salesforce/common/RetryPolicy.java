package sh.oso.salesforce.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with jitter, bounded by a total retry-time budget.
 * Only retries failures {@link ErrorClassifier#isRetryable(Throwable) classified as retryable}.
 */
public final class RetryPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(RetryPolicy.class);

    private final Duration initialBackoff;
    private final Duration maxBackoff;
    private final Duration budget;

    public RetryPolicy(Duration initialBackoff, Duration maxBackoff, Duration budget) {
        this.initialBackoff = initialBackoff;
        this.maxBackoff = maxBackoff;
        this.budget = budget;
    }

    /** Default: 500ms initial, 30s max backoff, budget supplied by caller config. */
    public static RetryPolicy withBudget(Duration budget) {
        return new RetryPolicy(Duration.ofMillis(500), Duration.ofSeconds(30), budget);
    }

    public <T> T execute(String opName, Callable<T> op) {
        long deadline = System.nanoTime() + budget.toNanos();
        long backoffMillis = initialBackoff.toMillis();
        int attempt = 0;
        while (true) {
            attempt++;
            try {
                return op.call();
            } catch (Exception e) {
                if (!ErrorClassifier.isRetryable(e)) {
                    throw toRuntime(e);
                }
                long remainingNanos = deadline - System.nanoTime();
                if (remainingNanos <= 0) {
                    throw new SalesforceException(
                            "Retry budget (" + budget.toMillis() + " ms) exhausted after " + attempt
                                    + " attempts for " + opName, e, false);
                }
                long jittered = ThreadLocalRandom.current().nextLong(backoffMillis / 2, backoffMillis + 1);
                long sleepMillis = Math.min(jittered, Duration.ofNanos(remainingNanos).toMillis());
                LOG.warn("Retryable failure on {} (attempt {}), backing off {} ms: {}",
                        opName, attempt, sleepMillis, e.getMessage());
                sleep(sleepMillis);
                backoffMillis = Math.min(backoffMillis * 2, maxBackoff.toMillis());
            }
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new SalesforceException("Interrupted during retry backoff", ie, false);
        }
    }

    private static RuntimeException toRuntime(Exception e) {
        return e instanceof RuntimeException re ? re : new SalesforceException(e.getMessage(), e, false);
    }
}
