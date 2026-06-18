package com.gotocompany.firehose.exception;

/**
 * Unchecked exception representing a failure that occurs while a sink processes messages.
 *
 * <p>Examples include a fail-fast handler that deliberately aborts the pipeline on a configured
 * error type, or a sink that cannot deserialize a record before writing it. The original cause is
 * preserved so it can be logged and surfaced through Firehose's error handling.
 */
public class SinkException extends RuntimeException {
    /**
     * Creates the exception with a detail message and the underlying cause.
     *
     * @param message human-readable description of the sink failure
     * @param cause   the original error that triggered the failure
     */
    public SinkException(String message, Throwable cause) {
        super(message, cause);
    }
}
