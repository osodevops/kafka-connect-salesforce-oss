package sh.oso.salesforce.common;

/** HTTP-level failure from a Salesforce REST/Bulk endpoint. */
public class SalesforceApiException extends SalesforceException {

    private final int statusCode;
    private final String errorCode;
    private final String responseBody;

    public SalesforceApiException(int statusCode, String errorCode, String responseBody, boolean retryable) {
        super("Salesforce API error: HTTP " + statusCode
                + (errorCode != null ? " [" + errorCode + "]" : "")
                + (responseBody != null && !responseBody.isBlank() ? " " + truncate(responseBody) : ""),
                null, retryable);
        this.statusCode = statusCode;
        this.errorCode = errorCode;
        this.responseBody = responseBody;
    }

    public int statusCode() {
        return statusCode;
    }

    /** Salesforce errorCode, e.g. {@code REQUEST_LIMIT_EXCEEDED}, {@code INVALID_SESSION_ID}; may be null. */
    public String errorCode() {
        return errorCode;
    }

    public String responseBody() {
        return responseBody;
    }

    public boolean isSessionExpired() {
        return statusCode == 401 || "INVALID_SESSION_ID".equals(errorCode);
    }

    private static String truncate(String s) {
        return s.length() > 500 ? s.substring(0, 500) + "..." : s;
    }
}
