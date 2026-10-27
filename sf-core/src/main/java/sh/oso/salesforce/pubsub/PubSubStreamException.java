package sh.oso.salesforce.pubsub;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import sh.oso.salesforce.common.ErrorClassifier;
import sh.oso.salesforce.common.SalesforceException;

import java.util.Locale;

/**
 * A failed Pub/Sub stream, carrying the gRPC status and the Salesforce {@code error-code}
 * trailer so callers can tell an expired or corrupted replay position from a transient
 * outage, an expired session or a configuration error.
 */
public final class PubSubStreamException extends SalesforceException {

    /** Trailer in which the Pub/Sub API reports its error code (e.g. {@code sfdc.platform.eventbus...}). */
    public static final Metadata.Key<String> ERROR_CODE_TRAILER =
            Metadata.Key.of("error-code", Metadata.ASCII_STRING_MARSHALLER);

    private final Status.Code code;
    private final String description;
    private final String errorCode;
    private final String topicName;

    private PubSubStreamException(String message, Throwable cause, boolean retryable,
                                  Status.Code code, String description, String errorCode, String topicName) {
        super(message, cause, retryable);
        this.code = code;
        this.description = description;
        this.errorCode = errorCode;
        this.topicName = topicName;
    }

    /** True when the throwable (or a cause) carries a gRPC status. */
    public static boolean hasGrpcStatus(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof StatusRuntimeException || c instanceof StatusException) {
                return true;
            }
        }
        return false;
    }

    public static PubSubStreamException from(Throwable t, String topicName) {
        Status status = Status.fromThrowable(t);
        Metadata trailers = Status.trailersFromThrowable(t);
        String errorCode = trailers == null ? null : trailers.get(ERROR_CODE_TRAILER);
        Status.Code code = status.getCode();
        // INTERNAL and UNKNOWN surface for HTTP/2 stream resets and proxy hiccups; worth a retry.
        boolean retryable = ErrorClassifier.isRetryableGrpc(status)
                || code == Status.Code.INTERNAL || code == Status.Code.UNKNOWN;
        StringBuilder message = new StringBuilder("Pub/Sub stream on ").append(topicName)
                .append(" failed: ").append(code);
        if (status.getDescription() != null) {
            message.append(" (").append(status.getDescription()).append(')');
        }
        if (errorCode != null) {
            message.append(" [").append(errorCode).append(']');
        }
        return new PubSubStreamException(message.toString(), t, retryable, code,
                status.getDescription(), errorCode, topicName);
    }

    public Status.Code code() {
        return code;
    }

    /** The Salesforce {@code error-code} trailer, or null when absent. */
    public String errorCode() {
        return errorCode;
    }

    public String topicName() {
        return topicName;
    }

    /** The stored replay position is expired (outside retention) or corrupted. */
    public boolean isInvalidReplay() {
        if (errorCode != null && errorCode.toLowerCase(Locale.ROOT).contains("replayid")) {
            return true;
        }
        return code == Status.Code.INVALID_ARGUMENT && description != null
                && description.toLowerCase(Locale.ROOT).contains("replay");
    }

    /** The session token was rejected; re-authenticate before resubscribing. */
    public boolean isUnauthenticated() {
        return code == Status.Code.UNAUTHENTICATED;
    }
}
