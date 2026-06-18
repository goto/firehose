package com.gotocompany.firehose.exception;

import lombok.EqualsAndHashCode;

/**
 * Generic checked exception used as a lightweight, named carrier for error details.
 *
 * <p>Firehose attaches it to a message's error info when a more specific exception type is not
 * required, for example as the default error placed on a freshly consumed
 * {@link com.gotocompany.firehose.message.Message} or to tag retryable versus non-retryable gRPC
 * failures. {@link #toString()} returns only the detail message so the value reads cleanly in logs
 * and error metadata. The Lombok {@code @EqualsAndHashCode(callSuper = false)} annotation makes two
 * instances equal when their messages match.
 */
@EqualsAndHashCode(callSuper = false)
public class DefaultException extends Exception {
    /**
     * Creates the exception with the given detail message.
     *
     * @param message human-readable description of the error
     */
    public DefaultException(String message) {
        super(message);
    }

    /**
     * Returns the detail message only, omitting the class name.
     *
     * @return the exception's detail message
     */
    @Override
    public String toString() {
        return getMessage();
    }
}
