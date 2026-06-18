package com.gotocompany.firehose.error;

/**
 * Categories that classify how a message error should be handled.
 *
 * <p>The {@link ErrorHandler} matches a message's error type against the configured set for each
 * scope to decide its fate: route to the dead-letter queue, retry, or fail the consumer.
 */
public enum ErrorScope {
    /** Errors that should send the message to the dead-letter queue. */
    DLQ,
    /** Errors that should trigger a retry. */
    RETRY,
    /** Errors that should fail the consumer. */
    FAIL
}
