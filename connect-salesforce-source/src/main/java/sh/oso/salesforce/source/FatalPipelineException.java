package sh.oso.salesforce.source;

import org.apache.kafka.connect.errors.ConnectException;

/**
 * A pipeline failure that retrying cannot fix (missing topic, missing permission, undecodable
 * event, or a lost replay position with {@code sf.gap.recovery=fail}). The task emits what the
 * other pipelines produced in the same poll, then fails.
 */
final class FatalPipelineException extends ConnectException {

    FatalPipelineException(String message, Throwable cause) {
        super(message, cause);
    }
}
