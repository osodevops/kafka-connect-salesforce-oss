package sh.oso.salesforce.source;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Non-blocking exponential backoff with jitter. A pipeline skips its retry until
 * {@link #ready()} instead of sleeping, so other pipelines in the task keep flowing.
 */
final class Backoff {

    static final long INITIAL_MS = 500;
    static final long MAX_MS = 60_000;

    private long nextDelayMs = INITIAL_MS;
    private long notBefore;

    boolean ready() {
        return System.currentTimeMillis() >= notBefore;
    }

    /** Records a failure; returns the delay before the next attempt. */
    long failed() {
        long delay = (long) (nextDelayMs * ThreadLocalRandom.current().nextDouble(0.8, 1.2));
        notBefore = System.currentTimeMillis() + delay;
        nextDelayMs = Math.min(nextDelayMs * 2, MAX_MS);
        return delay;
    }

    void reset() {
        nextDelayMs = INITIAL_MS;
        notBefore = 0;
    }
}
