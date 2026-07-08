package sh.oso.salesforce.common;

/** Base exception for all Salesforce client failures. */
public class SalesforceException extends RuntimeException {

    private final boolean retryable;

    public SalesforceException(String message) {
        this(message, null, false);
    }

    public SalesforceException(String message, Throwable cause) {
        this(message, cause, false);
    }

    public SalesforceException(String message, Throwable cause, boolean retryable) {
        super(message, cause);
        this.retryable = retryable;
    }

    /** True when the failure is transient and the operation may succeed on retry. */
    public boolean isRetryable() {
        return retryable;
    }
}
