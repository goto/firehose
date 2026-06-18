package com.gotocompany.firehose.filter;

/**
 * Checked exception thrown when a {@link Filter} cannot evaluate a batch of messages.
 *
 * <p>Causes include a malformed filter expression, a deserialization failure while reading a record,
 * or an unexpected expression result. It propagates up to the consumer, which wraps it in a
 * {@code FirehoseConsumerFailedException} to stop processing.
 */
public class FilterException extends Exception {

    /**
     * Creates the exception with a detail message and the underlying cause.
     *
     * @param message description of the filtering failure
     * @param e       the original exception that caused the failure
     */
    public FilterException(String message, Exception e) {
        super(message, e);
    }

    /**
     * Creates the exception with a detail message.
     *
     * @param message description of the filtering failure
     */
    public FilterException(String message) {
        super(message);
    }
}
