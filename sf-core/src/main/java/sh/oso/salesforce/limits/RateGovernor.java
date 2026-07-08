package sh.oso.salesforce.limits;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.salesforce.common.SalesforceException;
import sh.oso.salesforce.rest.RestClient;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Proactive API-quota governance. Refreshes {@code GET /limits/} at a bounded cadence and
 * fails fast (with a retryable error, so callers back off) when the remaining fraction of a
 * tracked limit drops below the configured reserve.
 */
public final class RateGovernor {

    public static final String DAILY_API_REQUESTS = "DailyApiRequests";
    public static final String DAILY_BULK_V2_QUERY_JOBS = "DailyBulkV2QueryJobs";
    public static final String DAILY_BULK_API_BATCHES = "DailyBulkApiBatches";

    private static final Logger LOG = LoggerFactory.getLogger(RateGovernor.class);

    private final RestClient rest;
    private final double reserveFraction;
    private final Duration refreshInterval;
    private final Map<String, Snapshot> snapshots = new ConcurrentHashMap<>();
    private volatile Instant lastRefresh = Instant.EPOCH;

    public RateGovernor(RestClient rest, double reserveFraction, Duration refreshInterval) {
        if (reserveFraction < 0 || reserveFraction >= 1) {
            throw new IllegalArgumentException("reserveFraction must be in [0,1): " + reserveFraction);
        }
        this.rest = rest;
        this.reserveFraction = reserveFraction;
        this.refreshInterval = refreshInterval;
    }

    /** Default: keep 5% of every pool in reserve; refresh limits at most every 5 minutes. */
    public static RateGovernor withDefaults(RestClient rest) {
        return new RateGovernor(rest, 0.05, Duration.ofMinutes(5));
    }

    /**
     * Call before consuming quota of the given type. Throws a retryable exception when the
     * remaining fraction is at or below the reserve so the caller's retry policy backs off.
     */
    public void acquire(String limitName) {
        refreshIfStale();
        Snapshot snapshot = snapshots.get(limitName);
        if (snapshot == null || snapshot.max <= 0) {
            return; // unknown limit: do not block progress
        }
        double remainingFraction = (double) snapshot.remaining / snapshot.max;
        if (remainingFraction <= reserveFraction) {
            throw new SalesforceException(
                    "Salesforce quota " + limitName + " nearly exhausted: " + snapshot.remaining + "/"
                            + snapshot.max + " remaining (reserve " + (reserveFraction * 100) + "%)",
                    null, true);
        }
    }

    /** Records local consumption between refreshes to keep the estimate honest. */
    public void consumed(String limitName, long amount) {
        snapshots.computeIfPresent(limitName, (k, s) ->
                new Snapshot(Math.max(0, s.remaining - amount), s.max));
    }

    private void refreshIfStale() {
        Instant now = Instant.now();
        if (Duration.between(lastRefresh, now).compareTo(refreshInterval) < 0) {
            return;
        }
        synchronized (this) {
            if (Duration.between(lastRefresh, Instant.now()).compareTo(refreshInterval) < 0) {
                return;
            }
            try {
                JsonNode limits = rest.limits();
                for (String name : new String[]{DAILY_API_REQUESTS, DAILY_BULK_V2_QUERY_JOBS,
                        DAILY_BULK_API_BATCHES}) {
                    JsonNode limit = limits.path(name);
                    if (!limit.isMissingNode()) {
                        snapshots.put(name, new Snapshot(
                                limit.path("Remaining").asLong(), limit.path("Max").asLong()));
                    }
                }
                lastRefresh = Instant.now();
                Snapshot api = snapshots.get(DAILY_API_REQUESTS);
                if (api != null) {
                    LOG.debug("Salesforce API quota: {}/{} remaining", api.remaining, api.max);
                }
            } catch (RuntimeException e) {
                // Limits polling must never take the pipeline down; keep last snapshot.
                LOG.warn("Failed to refresh /limits/ ({}); using last known quota", e.getMessage());
                lastRefresh = Instant.now();
            }
        }
    }

    private record Snapshot(long remaining, long max) {
    }
}
