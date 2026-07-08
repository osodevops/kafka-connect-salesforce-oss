package sh.oso.salesforce.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-safe cached session with transparent re-authentication.
 * Callers use {@link #session()} for the current token and {@link #invalidate(Session)}
 * when a request fails with 401/INVALID_SESSION_ID, then retry with a fresh session.
 */
public final class SessionSupplier {

    private static final Logger LOG = LoggerFactory.getLogger(SessionSupplier.class);

    private final SalesforceAuth auth;
    private volatile Session current;

    public SessionSupplier(SalesforceAuth auth) {
        this.auth = auth;
    }

    public Session session() {
        Session s = current;
        if (s != null) {
            return s;
        }
        synchronized (this) {
            if (current == null) {
                current = auth.authenticate();
            }
            return current;
        }
    }

    /**
     * Drops the cached session if it is still the one the caller saw fail,
     * so concurrent callers don't trigger redundant re-auths.
     */
    public synchronized void invalidate(Session failed) {
        if (current == failed) {
            LOG.info("Salesforce session expired; re-authenticating");
            current = null;
        }
    }
}
