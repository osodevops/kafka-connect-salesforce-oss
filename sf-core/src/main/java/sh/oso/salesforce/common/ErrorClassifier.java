package sh.oso.salesforce.common;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;

import java.io.IOException;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpTimeoutException;

/**
 * Classifies failures as retryable or fatal, per the connector retry contract:
 * HTTP 429/5xx, 403 REQUEST_LIMIT_EXCEEDED, gRPC UNAVAILABLE/DEADLINE_EXCEEDED/RESOURCE_EXHAUSTED,
 * and network timeouts are retryable.
 */
public final class ErrorClassifier {

    private ErrorClassifier() {
    }

    public static boolean isRetryableHttp(int statusCode, String errorCode) {
        if (statusCode == 429 || statusCode >= 500) {
            return true;
        }
        return statusCode == 403 && "REQUEST_LIMIT_EXCEEDED".equals(errorCode);
    }

    public static boolean isRetryable(Throwable t) {
        if (t instanceof SalesforceException se) {
            return se.isRetryable();
        }
        if (t instanceof StatusRuntimeException sre) {
            return isRetryableGrpc(sre.getStatus());
        }
        if (t instanceof StatusException se) {
            return isRetryableGrpc(se.getStatus());
        }
        if (t instanceof HttpTimeoutException || t instanceof HttpConnectTimeoutException) {
            return true;
        }
        if (t instanceof IOException) {
            // Connection resets, broken pipes, DNS blips: worth retrying.
            return true;
        }
        return false;
    }

    public static boolean isRetryableGrpc(Status status) {
        return switch (status.getCode()) {
            case UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED -> true;
            default -> false;
        };
    }
}
